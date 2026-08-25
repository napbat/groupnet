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
    max_frame_bytes: usize,
    encoded_keys_bytes: usize,
}

impl Ring {
    fn push(&mut self, key: Vec<u8>) {
        self.encoded_keys_bytes = self
            .encoded_keys_bytes
            .saturating_add(4_usize.saturating_add(key.len()));
        self.keys.push_back(key);
        while self.keys.len() > self.capacity
            || (self.keys.len() > 1 && self.encoded_len() > self.max_frame_bytes)
        {
            let Some(removed) = self.keys.pop_front() else {
                break;
            };
            self.encoded_keys_bytes = self
                .encoded_keys_bytes
                .saturating_sub(4_usize.saturating_add(removed.len()));
            self.first_seq += 1;
        }
    }

    fn encoded_len(&self) -> usize {
        20_usize.saturating_add(self.encoded_keys_bytes)
    }

    /// Retires the acknowledged prefix while keeping the current head as a
    /// sequence anchor for subscribers reconciling this state-based feed.
    fn retire_through(&mut self, token: WriteToken) -> bool {
        if token.epoch != self.epoch || self.keys.len() <= 1 || token.seq < self.first_seq {
            return false;
        }

        let mut changed = false;
        while self.keys.len() > 1 && self.first_seq <= token.seq {
            let removed = self
                .keys
                .pop_front()
                .expect("a non-anchor entry exists while retiring");
            self.encoded_keys_bytes = self
                .encoded_keys_bytes
                .saturating_sub(4_usize.saturating_add(removed.len()));
            self.first_seq += 1;
            changed = true;
        }
        changed
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
    ///
    /// `capacity` is an upper bound, not a promise that every slot remains
    /// visible: the ring is also trimmed to the group's
    /// [`Config::max_delta_frame_bytes`](groupnet_core::Config::max_delta_frame_bytes)
    /// budget. Large encoded keys therefore shorten the replay window and make a
    /// lagging subscriber receive an explicit [`PeerWrite::Gap`](crate::PeerWrite::Gap)
    /// instead of growing this single gossiped entry past the transport envelope.
    pub fn named(
        name: &str,
        group: Group,
        capacity: NonZeroUsize,
        encode: impl Fn(&K) -> Vec<u8> + Send + Sync + 'static,
    ) -> Self {
        let max_frame_bytes = group.config().max_delta_frame_bytes;
        Self {
            group,
            key: entry_key(name),
            ring: Mutex::new(Ring {
                epoch: wall_clock_epoch(),
                first_seq: 1,
                keys: VecDeque::new(),
                capacity: capacity.get(),
                max_frame_bytes,
                encoded_keys_bytes: 0,
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
        let token = {
            let mut ring = self
                .ring
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let token = WriteToken {
                epoch: ring.epoch,
                seq: ring.first_seq + ring.keys.len() as u64,
            };
            ring.push((self.encode)(key));
            token
        };
        async move {
            self.advertise_current().await;
            token
        }
    }

    /// Retires writes through `token` from the advertised history and
    /// re-advertises the shortened feed best-effort.
    ///
    /// Call this only after every reader that may still serve has applied
    /// through `token`, or after those readers have lost serving authority.
    /// Retirement is irreversible: a lagging subscriber behind the shortened
    /// window receives [`PeerWrite::Gap`](crate::PeerWrite::Gap). The newest
    /// current-epoch write is always retained as the advertised-head anchor,
    /// even when `token` is at or beyond that head.
    ///
    /// Tokens from another epoch, tokens older than the retained window, an
    /// empty feed, and acknowledgements already reflected in the window are
    /// no-ops. Concurrent acknowledgements compact monotonically: a late older
    /// token cannot restore retired entries or move sequence identity backward.
    ///
    /// The ring is shortened synchronously before the returned future is
    /// polled. If its advertisement meets sustained inbox backpressure, the
    /// next [`publish`](Self::publish) re-carries the compacted window.
    pub fn retire_through(&self, token: WriteToken) -> impl Future<Output = ()> + Send + '_ {
        let changed = {
            let mut ring = self
                .ring
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            ring.retire_through(token)
        };
        async move {
            if changed {
                self.advertise_current().await;
            }
        }
    }

    /// Re-advertises the current feed without recording a new write —
    /// useful at quiescence points after a `publish` hit backpressure.
    pub fn republish(&self) -> impl Future<Output = ()> + Send + '_ {
        self.advertise_current()
    }

    async fn advertise_current(&self) {
        for _ in 0..PUBLISH_RETRIES {
            // Enqueue while holding the ring lock: a concurrent publish or
            // retirement can only mutate after this frame is ordered into the
            // actor inbox, so delayed/out-of-order futures cannot advertise an
            // older window over a newer one.
            let advertised = {
                let ring = self
                    .ring
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                self.group
                    .set_entry(self.key.clone(), ring.frame().encode(), None)
                    .is_ok()
            };
            if advertised {
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
            max_frame_bytes: usize::MAX,
            encoded_keys_bytes: 0,
        };
        for key in [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()] {
            ring.push(key);
        }
        let frame = ring.frame();
        assert_eq!(frame.first_seq, 2);
        assert_eq!(frame.keys, vec![b"b".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn ring_byte_budget_keeps_the_newest_write_and_advances_first_seq() {
        let mut ring = Ring {
            epoch: 1,
            first_seq: 1,
            keys: VecDeque::new(),
            capacity: 32,
            max_frame_bytes: 40,
            encoded_keys_bytes: 0,
        };
        for key in [
            b"aaaaaaaa".to_vec(),
            b"bbbbbbbb".to_vec(),
            b"cccccccc".to_vec(),
        ] {
            ring.push(key);
        }
        let frame = ring.frame();
        assert_eq!(frame.first_seq, 3);
        assert_eq!(frame.keys, vec![b"cccccccc".to_vec()]);
        assert!(frame.encode().len() <= ring.max_frame_bytes);
    }

    #[test]
    fn retirement_keeps_the_head_anchor_and_exact_byte_accounting() {
        let mut ring = Ring {
            epoch: 7,
            first_seq: 1,
            keys: VecDeque::new(),
            capacity: 8,
            max_frame_bytes: usize::MAX,
            encoded_keys_bytes: 0,
        };
        for key in [b"a".to_vec(), b"bb".to_vec(), b"ccc".to_vec()] {
            ring.push(key);
        }

        assert!(ring.retire_through(crate::WriteToken {
            epoch: 7,
            seq: u64::MAX,
        }));
        let frame = ring.frame();
        assert_eq!(frame.first_seq, 3);
        assert_eq!(frame.keys, vec![b"ccc".to_vec()]);
        assert_eq!(ring.encoded_keys_bytes, 4 + b"ccc".len());
        assert_eq!(ring.encoded_len(), frame.encode().len());
    }

    #[test]
    fn retirement_ignores_wrong_and_out_of_order_tokens() {
        let mut empty = Ring {
            epoch: 11,
            first_seq: 1,
            keys: VecDeque::new(),
            capacity: 8,
            max_frame_bytes: usize::MAX,
            encoded_keys_bytes: 0,
        };
        assert!(!empty.retire_through(crate::WriteToken { epoch: 11, seq: 1 }));

        let mut ring = Ring {
            epoch: 11,
            first_seq: 1,
            keys: VecDeque::new(),
            capacity: 8,
            max_frame_bytes: usize::MAX,
            encoded_keys_bytes: 0,
        };
        for key in [
            b"w1".to_vec(),
            b"w2".to_vec(),
            b"w3".to_vec(),
            b"w4".to_vec(),
        ] {
            ring.push(key);
        }

        assert!(!ring.retire_through(crate::WriteToken { epoch: 10, seq: 4 }));
        assert_eq!(ring.frame().first_seq, 1);

        assert!(ring.retire_through(crate::WriteToken { epoch: 11, seq: 2 }));
        assert_eq!(ring.frame().first_seq, 3);
        assert!(!ring.retire_through(crate::WriteToken { epoch: 11, seq: 1 }));
        assert!(!ring.retire_through(crate::WriteToken { epoch: 11, seq: 2 }));

        assert!(ring.retire_through(crate::WriteToken { epoch: 11, seq: 3 }));
        assert_eq!(ring.frame().first_seq, 4);
        assert!(!ring.retire_through(crate::WriteToken { epoch: 11, seq: 2 }));
        assert!(!ring.retire_through(crate::WriteToken { epoch: 11, seq: 4 }));
        assert_eq!(ring.frame().keys, vec![b"w4".to_vec()]);
    }
}
