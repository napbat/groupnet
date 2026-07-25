//! Publisher half: [`WriteFeed`] and its ring of recent writes.

use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use groupnet_runtime::Group;

use crate::token::WriteToken;
use crate::wire::{Frame, entry_key};

/// Attempts before giving up on advertising a frame under inbox
/// backpressure (the ring keeps the write; the next publish re-carries it).
const PUBLISH_RETRIES: usize = 8;

type EncodeFn<K> = dyn Fn(&K) -> Vec<u8> + Send + Sync;

/// Ring of the last N encoded writes; all mutation keeps `first_seq` equal
/// to the sequence number of the front element.
struct Ring {
    epoch: u64,
    first_seq: u64,
    keys: VecDeque<Vec<u8>>,
    capacity: usize,
}

impl Ring {
    fn push(&mut self, key: Vec<u8>) {
        self.keys.push_back(key);
        if self.keys.len() > self.capacity {
            self.keys.pop_front();
            self.first_seq += 1;
        }
    }

    fn frame(&self) -> Frame {
        Frame {
            epoch: self.epoch,
            first_seq: self.first_seq,
            keys: self.keys.iter().cloned().collect(),
        }
    }
}

/// The wall clock as a feed epoch: strictly increasing across restarts
/// unless the clock steps backwards (prefer [`WriteFeed::with_epoch`] with a
/// durable counter when that matters).
fn wall_clock_epoch() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Publisher half: advertises this node's writes to the group.
///
/// Call [`WriteFeed::publish`] after every local durable write. The feed is
/// best-effort under actor-inbox backpressure — a dropped advertisement is
/// re-carried by the next publish (the ring is state, not a log); call
/// [`WriteFeed::republish`] at quiescence points if the last write must be
/// advertised promptly.
pub struct WriteFeed<K> {
    group: Group,
    key: String,
    ring: Mutex<Ring>,
    encode: Box<EncodeFn<K>>,
}

impl<K> fmt::Debug for WriteFeed<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WriteFeed")
            .field("group", &self.group.id())
            .field("key", &self.key)
            .field("epoch", &self.epoch())
            .finish_non_exhaustive()
    }
}

impl<K> WriteFeed<K> {
    /// Creates the default feed over `group`, remembering the last
    /// `capacity` writes, with a wall-clock epoch.
    ///
    /// Size `capacity` for the write rate: peers that fall further behind
    /// than the ring holds receive a [`PeerWrite::Gap`](crate::PeerWrite::Gap) instead of the
    /// individual keys.
    pub fn new(
        group: Group,
        capacity: NonZeroUsize,
        encode: impl Fn(&K) -> Vec<u8> + Send + Sync + 'static,
    ) -> Self {
        Self::named("", group, capacity, encode)
    }

    /// Creates a named feed — independent subsystems sharing one group must
    /// name their feeds so they occupy distinct entries (and pair each with
    /// [`PeerWrites::named`](crate::PeerWrites::named) under the same name).
    pub fn named(
        name: &str,
        group: Group,
        capacity: NonZeroUsize,
        encode: impl Fn(&K) -> Vec<u8> + Send + Sync + 'static,
    ) -> Self {
        Self {
            group,
            key: entry_key(name),
            ring: Mutex::new(Ring {
                epoch: wall_clock_epoch(),
                first_seq: 1,
                keys: VecDeque::new(),
                capacity: capacity.get(),
            }),
            encode: Box::new(encode),
        }
    }

    /// Replaces the epoch — call before the first [`publish`](Self::publish).
    /// Use a durable, strictly-increasing per-writer counter (a WAL
    /// generation, a boot counter) when the wall-clock default is not
    /// trustworthy across restarts.
    #[must_use]
    pub fn with_epoch(self, epoch: u64) -> Self {
        {
            let mut ring = self
                .ring
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            ring.epoch = epoch;
        }
        self
    }

    /// This feed life's epoch (the `epoch` half of every token it issues).
    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.ring
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .epoch
    }

    /// The token of the most recent publish this life, or `None` before the
    /// first — observability for "how far has this writer written".
    #[must_use]
    pub fn last_token(&self) -> Option<WriteToken> {
        let ring = self
            .ring
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let len = ring.keys.len() as u64;
        (len > 0).then(|| WriteToken {
            epoch: ring.epoch,
            seq: ring.first_seq + len - 1,
        })
    }

    /// Records `key` as written and advertises the updated feed, resolving
    /// to the write's [`WriteToken`] — the second half of a
    /// `(writer, token)` read-your-writes session token.
    ///
    /// The write is recorded in the ring synchronously (before the returned
    /// future is polled), so even a dropped future is re-carried by the
    /// next publish.
    pub fn publish(&self, key: &K) -> impl Future<Output = WriteToken> + Send + '_ {
        let (token, frame) = {
            let mut ring = self
                .ring
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let token = WriteToken {
                epoch: ring.epoch,
                seq: ring.first_seq + ring.keys.len() as u64,
            };
            ring.push((self.encode)(key));
            (token, ring.frame().encode())
        };
        async move {
            self.advertise(frame).await;
            token
        }
    }

    /// Re-advertises the current feed without recording a new write —
    /// useful at quiescence points after a `publish` hit backpressure.
    pub fn republish(&self) -> impl Future<Output = ()> + Send + '_ {
        let frame = {
            let ring = self
                .ring
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            ring.frame().encode()
        };
        self.advertise(frame)
    }

    async fn advertise(&self, frame: Vec<u8>) {
        for _ in 0..PUBLISH_RETRIES {
            if self
                .group
                .set_entry(self.key.clone(), frame.clone(), None)
                .is_ok()
            {
                return;
            }
            // Inbox backpressure: yield and retry; on sustained pressure the
            // ring re-carries this write on the next publish.
            tokio::task::yield_now().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::Ring;

    #[test]
    fn ring_overflow_advances_first_seq() {
        let mut ring = Ring {
            epoch: 1,
            first_seq: 1,
            keys: VecDeque::new(),
            capacity: 2,
        };
        for key in [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()] {
            ring.push(key);
        }
        let frame = ring.frame();
        assert_eq!(frame.first_seq, 2);
        assert_eq!(frame.keys, vec![b"b".to_vec(), b"c".to_vec()]);
    }
}
