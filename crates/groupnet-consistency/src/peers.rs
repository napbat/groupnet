//! Subscriber half: [`PeerWrites`] turning peers' feed changes into ordered
//! [`PeerWrite`] events.

use std::collections::{HashMap, VecDeque};
use std::fmt;

use groupnet_core::NodeId;
use groupnet_runtime::{Group, GroupEvent};
use tokio::sync::broadcast::error::RecvError;

use crate::token::WriteToken;
use crate::wire::{Frame, entry_key};

type DecodeFn<K> = dyn Fn(&[u8]) -> Option<K> + Send + Sync;

/// A present feed entry could not be decoded into a valid head.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InvalidFeedHead;

/// The head [`WriteToken`] `peer`'s default feed currently advertises in
/// `group` — its most recently published write, as gossip shows it right
/// now — or `None` when the peer has no decodable feed (or none yet).
///
/// This is what a freshness barrier compares a [`Frontier`](crate::Frontier)
/// against: once `reached(peer, head)` holds for the head observed at some
/// instant, every write the peer had advertised as of that instant has been
/// applied locally. The head itself is only as fresh as propagation, so the
/// barrier bounds staleness at roughly one push/gossip hop — a session
/// guarantee, not a global order.
#[must_use]
pub fn advertised_head(group: &Group, peer: &NodeId) -> Option<WriteToken> {
    checked_advertised_head(group, peer).ok().flatten()
}

/// Reads the default feed head, distinguishing an absent entry from a
/// malformed present entry. Recovery protocols should use this form so an
/// unreadable source observation cannot be mistaken for a quiet writer.
///
/// # Errors
/// Returns [`InvalidFeedHead`] when the peer has a present invalid frame.
pub fn checked_advertised_head(
    group: &Group,
    peer: &NodeId,
) -> Result<Option<WriteToken>, InvalidFeedHead> {
    checked_advertised_head_named("", group, peer)
}

/// [`advertised_head`] for a named feed (see [`WriteFeed::named`](crate::WriteFeed::named)).
#[must_use]
pub fn advertised_head_named(name: &str, group: &Group, peer: &NodeId) -> Option<WriteToken> {
    checked_advertised_head_named(name, group, peer)
        .ok()
        .flatten()
}

/// Fallible [`advertised_head_named`] for source-backed recovery observers.
///
/// # Errors
/// Returns [`InvalidFeedHead`] when the named entry is present but invalid.
pub fn checked_advertised_head_named(
    name: &str,
    group: &Group,
    peer: &NodeId,
) -> Result<Option<WriteToken>, InvalidFeedHead> {
    let Some(bytes) = group.node_entry(peer, &entry_key(name)) else {
        return Ok(None);
    };
    let frame = Frame::decode(&bytes).ok_or(InvalidFeedHead)?;
    let end = frame
        .first_seq
        .checked_add(u64::try_from(frame.keys.len()).map_err(|_| InvalidFeedHead)?)
        .ok_or(InvalidFeedHead)?;
    let Some(head) = end.checked_sub(1).filter(|head| *head >= frame.first_seq) else {
        return Ok(None);
    };
    Ok(Some(WriteToken {
        epoch: frame.epoch,
        seq: head,
    }))
}

/// One peer-write notification from [`PeerWrites::next`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerWrite<K> {
    /// `peer` wrote `key` at `token`. Apply it (drop the stale copy, refresh
    /// the index entry, …), then advance the [`Frontier`](crate::Frontier) to `token`.
    Wrote {
        /// The node that performed the write.
        peer: NodeId,
        /// The write's position in `peer`'s feed.
        token: WriteToken,
        /// The written key.
        key: K,
    },
    /// Writes of `peer` up to `missed_through` were provably missed — its
    /// ring advanced past this subscriber's cursor, or it restarted into a
    /// new epoch without a seal this subscriber delivered (epoch-major
    /// ordering makes `missed_through` cover the whole previous life).
    /// Remediate coarsely (flush, rebuild, refetch), then advance the
    /// [`Frontier`](crate::Frontier) to `missed_through`.
    Gap {
        /// The node whose writes were missed.
        peer: NodeId,
        /// After remediating, every write of `peer` up to and including
        /// this token is covered.
        missed_through: WriteToken,
    },
    /// `peer` sealed its current life at `token`, the position after its
    /// last write ([`WriteFeed::seal`](crate::WriteFeed::seal)), and every
    /// write before it has been delivered. Nothing changes locally; advance
    /// the [`Frontier`](crate::Frontier) to `token` and acknowledge it as a
    /// write, which is how the writer learns its seal was observed.
    Sealed {
        /// The node that sealed its feed.
        peer: NodeId,
        /// The seal's own position.
        token: WriteToken,
    },
    /// `peer` restarted into `epoch` after a life that this subscriber
    /// delivered through its seal at `sealed`. The previous life ended
    /// there, so no write is missing and there is nothing to remediate:
    /// advance the [`Frontier`](crate::Frontier) to
    /// `WriteToken { epoch, seq: 0 }` and carry on with the new life's writes.
    Renewed {
        /// The node that restarted.
        peer: NodeId,
        /// The previous life's seal, already delivered.
        sealed: WriteToken,
        /// The new life's epoch.
        epoch: u64,
    },
}

/// A subscriber's position in one peer's feed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Cursor {
    epoch: u64,
    /// Next unseen sequence number within `epoch`.
    next: u64,
    /// This life's seal, at `next - 1`, has been delivered.
    sealed: bool,
}

impl Cursor {
    /// Where a subscriber attaching to `frame` starts: at its current end,
    /// or past its seal. History is not replayed, and a life that has
    /// already ended leaves nothing to deliver.
    fn attached(frame: &Frame) -> Self {
        Self {
            epoch: frame.epoch,
            next: frame.end() + u64::from(frame.sealed),
            sealed: frame.sealed,
        }
    }
}

/// Reconcile one peer's current frame against its cursor, queueing events.
/// Returns the number of gaps queued. A frame from an older epoch is a stale
/// copy of a previous life, a late seal included, and changes nothing.
fn reconcile<K>(
    node: &NodeId,
    cursor: &mut Cursor,
    frame: &Frame,
    decode: &DecodeFn<K>,
    pending: &mut VecDeque<PeerWrite<K>>,
) -> u64 {
    let mut gaps = 0;
    if frame.epoch < cursor.epoch {
        return gaps;
    }
    if frame.epoch > cursor.epoch {
        // The writer restarted. A seal this subscriber delivered, followed by
        // a new life whose first write is still visible, misses nothing.
        // Otherwise epoch-major token ordering makes the gap cover every
        // write of the previous life as well.
        if cursor.sealed && frame.first_seq == 1 {
            pending.push_back(PeerWrite::Renewed {
                peer: node.clone(),
                sealed: WriteToken {
                    epoch: cursor.epoch,
                    seq: cursor.next - 1,
                },
                epoch: frame.epoch,
            });
        } else {
            pending.push_back(PeerWrite::Gap {
                peer: node.clone(),
                missed_through: WriteToken {
                    epoch: frame.epoch,
                    seq: frame.first_seq.saturating_sub(1),
                },
            });
            gaps += 1;
        }
        *cursor = Cursor {
            epoch: frame.epoch,
            next: frame.first_seq,
            sealed: false,
        };
    } else if cursor.sealed {
        // The life ended at its seal; the frame can only repeat it.
        return gaps;
    } else if cursor.next < frame.first_seq {
        // The ring advanced past us: writes were provably missed.
        pending.push_back(PeerWrite::Gap {
            peer: node.clone(),
            missed_through: WriteToken {
                epoch: frame.epoch,
                seq: frame.first_seq.saturating_sub(1),
            },
        });
        gaps += 1;
        cursor.next = frame.first_seq;
    }
    while cursor.next < frame.end() {
        let Ok(index) = usize::try_from(cursor.next - frame.first_seq) else {
            break;
        };
        if let Some(key) = decode(&frame.keys[index]) {
            pending.push_back(PeerWrite::Wrote {
                peer: node.clone(),
                token: WriteToken {
                    epoch: frame.epoch,
                    seq: cursor.next,
                },
                key,
            });
        }
        cursor.next += 1;
    }
    if frame.sealed && cursor.next == frame.end() {
        pending.push_back(PeerWrite::Sealed {
            peer: node.clone(),
            token: WriteToken {
                epoch: frame.epoch,
                seq: cursor.next,
            },
        });
        cursor.next += 1;
        cursor.sealed = true;
    }
    gaps
}

/// Subscriber half: turns peers' feed changes into [`PeerWrite`] events.
///
/// Drive it from a task: `while let Some(event) = peers.next().await { … }`.
/// Event-stream lag is handled internally by re-reading the always-current
/// entry snapshots — no write is ever silently skipped.
pub struct PeerWrites<K> {
    group: Group,
    me: NodeId,
    key: String,
    events: tokio::sync::broadcast::Receiver<GroupEvent>,
    cursors: HashMap<NodeId, Cursor>,
    pending: VecDeque<PeerWrite<K>>,
    gaps_seen: u64,
    decode: Box<DecodeFn<K>>,
}

impl<K> fmt::Debug for PeerWrites<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerWrites")
            .field("group", &self.group.id())
            .field("me", &self.me)
            .field("key", &self.key)
            .field("peers", &self.cursors.len())
            .field("gaps_seen", &self.gaps_seen)
            .finish_non_exhaustive()
    }
}

impl<K> PeerWrites<K> {
    /// Subscribes to the default feed in `group`. `me` is this node's id
    /// (its own feed is ignored). Existing peer feeds start at their current
    /// end: history is not replayed.
    pub fn new(
        group: Group,
        me: NodeId,
        decode: impl Fn(&[u8]) -> Option<K> + Send + Sync + 'static,
    ) -> Self {
        Self::named("", group, me, decode)
    }

    /// Subscribes to the feed named `name` (the counterpart of
    /// [`WriteFeed::named`](crate::WriteFeed::named)).
    pub fn named(
        name: &str,
        group: Group,
        me: NodeId,
        decode: impl Fn(&[u8]) -> Option<K> + Send + Sync + 'static,
    ) -> Self {
        let key = entry_key(name);
        let events = group.events();
        let mut cursors = HashMap::new();
        for (node, entries) in group.all_entries().iter() {
            if *node == me {
                continue;
            }
            if let Some(bytes) = entries.get(&key) {
                if let Some(frame) = Frame::decode(bytes) {
                    cursors.insert(node.clone(), Cursor::attached(&frame));
                }
            }
        }
        Self {
            group,
            me,
            key,
            events,
            cursors,
            pending: VecDeque::new(),
            gaps_seen: 0,
            decode: Box::new(decode),
        }
    }

    /// How many [`PeerWrite::Gap`]s this subscriber has emitted — a rising
    /// count means the ring is undersized for the write rate (or writers
    /// keep restarting without sealing).
    #[must_use]
    pub fn gaps_seen(&self) -> u64 {
        self.gaps_seen
    }

    /// How far this subscriber currently lags behind `peer`'s advertised
    /// feed, in writes (`None` if the peer has no decodable feed). An epoch
    /// this subscriber has not entered yet counts as the peer's whole
    /// visible window.
    #[must_use]
    pub fn lag(&self, peer: &NodeId) -> Option<u64> {
        let frame = Frame::decode(&self.group.node_entry(peer, &self.key)?)?;
        let lag = match self.cursors.get(peer) {
            Some(c) if c.epoch == frame.epoch => frame.end().saturating_sub(c.next),
            // Behind by a whole life (or never seen): the visible window.
            _ => frame.end().saturating_sub(frame.first_seq),
        };
        Some(lag)
    }

    /// The next peer write, or `None` once the group is gone.
    pub async fn next(&mut self) -> Option<PeerWrite<K>> {
        loop {
            if let Some(event) = self.pending.pop_front() {
                return Some(event);
            }
            match self.events.recv().await {
                Ok(GroupEvent::NodeStateChanged { node, key })
                    if key == self.key && node != self.me =>
                {
                    self.scan(&node);
                }
                // Lag means missed edge triggers, never missed state: the
                // entry snapshots are current, so a full re-scan recovers.
                Err(RecvError::Lagged(_)) | Ok(GroupEvent::MembershipChanged) => self.scan_all(),
                Ok(_) => {}
                Err(RecvError::Closed) => return None,
            }
        }
    }

    fn scan_all(&mut self) {
        for node in self.group.members() {
            if node != self.me {
                self.scan(&node);
            }
        }
    }

    /// Reconciles one peer's feed against our cursor, queueing events. A
    /// peer first seen here replays its visible window.
    fn scan(&mut self, node: &NodeId) {
        let Some(bytes) = self.group.node_entry(node, &self.key) else {
            return;
        };
        let Some(frame) = Frame::decode(&bytes) else {
            return;
        };
        let cursor = self.cursors.entry(node.clone()).or_insert(Cursor {
            epoch: frame.epoch,
            next: frame.first_seq,
            sealed: false,
        });
        self.gaps_seen += reconcile(node, cursor, &frame, &*self.decode, &mut self.pending);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use groupnet_core::NodeId;

    use super::{Cursor, PeerWrite, reconcile};
    use crate::token::WriteToken;
    use crate::wire::Frame;

    fn frame(epoch: u64, first_seq: u64, keys: &[&str], sealed: bool) -> Frame {
        Frame {
            epoch,
            first_seq,
            keys: keys.iter().map(|key| key.as_bytes().to_vec()).collect(),
            sealed,
        }
    }

    fn decode(bytes: &[u8]) -> Option<String> {
        String::from_utf8(bytes.to_vec()).ok()
    }

    /// Feed `frames` in order to a subscriber attached at `start`.
    fn deliver(start: Cursor, frames: &[Frame]) -> (Cursor, Vec<PeerWrite<String>>) {
        let node = NodeId::from("w");
        let mut cursor = start;
        let mut pending = VecDeque::new();
        for frame in frames {
            reconcile(&node, &mut cursor, frame, &decode, &mut pending);
        }
        (cursor, pending.into())
    }

    fn at(epoch: u64, next: u64) -> Cursor {
        Cursor {
            epoch,
            next,
            sealed: false,
        }
    }

    fn token(epoch: u64, seq: u64) -> WriteToken {
        WriteToken { epoch, seq }
    }

    fn wrote(epoch: u64, seq: u64, key: &str) -> PeerWrite<String> {
        PeerWrite::Wrote {
            peer: NodeId::from("w"),
            token: token(epoch, seq),
            key: key.to_owned(),
        }
    }

    fn gap(epoch: u64, seq: u64) -> PeerWrite<String> {
        PeerWrite::Gap {
            peer: NodeId::from("w"),
            missed_through: token(epoch, seq),
        }
    }

    /// A life delivered through its seal crosses into the next life with no
    /// gap, whether the next life announces itself empty or with its first
    /// write already visible.
    #[test]
    fn a_delivered_seal_crosses_into_the_next_life_without_a_gap() {
        for first_write in [&[][..], &["n1"][..]] {
            let (cursor, events) = deliver(
                at(7, 1),
                &[
                    frame(7, 1, &["a", "b"], false),
                    frame(7, 1, &["a", "b"], true),
                    frame(9, 1, first_write, false),
                ],
            );
            let mut expected = vec![
                wrote(7, 1, "a"),
                wrote(7, 2, "b"),
                PeerWrite::Sealed {
                    peer: NodeId::from("w"),
                    token: token(7, 3),
                },
                PeerWrite::Renewed {
                    peer: NodeId::from("w"),
                    sealed: token(7, 3),
                    epoch: 9,
                },
            ];
            if !first_write.is_empty() {
                expected.push(wrote(9, 1, "n1"));
            }
            assert_eq!(events, expected);
            assert_eq!(cursor.epoch, 9);
            assert!(!cursor.sealed);
        }
    }

    /// A restart without a seal, or one whose sealed frame this subscriber
    /// never saw because the next life's frame replaced it first, keeps the
    /// restart gap over the whole previous life.
    #[test]
    fn an_unsealed_or_unseen_seal_restart_gaps() {
        let unsealed = deliver(
            at(7, 1),
            &[frame(7, 1, &["a"], false), frame(9, 1, &["n1"], false)],
        );
        assert_eq!(unsealed.1, [wrote(7, 1, "a"), gap(9, 0), wrote(9, 1, "n1")]);
        // The seal was advertised but lost: the next frame seen is the new
        // life's.
        let lost = deliver(at(7, 1), &[frame(9, 1, &[], false)]);
        assert_eq!(lost.1, [gap(9, 0)]);
    }

    /// A seal that arrives after this subscriber already crossed into the
    /// next life is a stale frame of a previous life and changes nothing:
    /// it cannot undo the gap. A new life whose first writes already left
    /// the ring gaps even after a delivered seal.
    #[test]
    fn a_late_seal_or_an_overflowed_new_life_still_gaps() {
        let late = deliver(
            at(7, 1),
            &[
                frame(7, 1, &["a"], false),
                frame(9, 1, &["n1"], false),
                frame(7, 1, &["a"], true),
            ],
        );
        assert_eq!(late.1, [wrote(7, 1, "a"), gap(9, 0), wrote(9, 1, "n1")]);
        assert_eq!(late.0, at(9, 2));

        let overflowed = deliver(
            at(7, 1),
            &[frame(7, 1, &["a"], true), frame(9, 4, &["n4"], false)],
        );
        assert_eq!(
            overflowed.1,
            [
                wrote(7, 1, "a"),
                PeerWrite::Sealed {
                    peer: NodeId::from("w"),
                    token: token(7, 2),
                },
                gap(9, 3),
                wrote(9, 4, "n4"),
            ]
        );
    }

    /// A subscriber that lagged past the ring before the seal takes the ring
    /// gap first; once that gap is remediated the life is complete through
    /// its seal, so the restart that follows needs no second gap. A repeated
    /// sealed frame delivers nothing again.
    #[test]
    fn a_ring_gap_before_the_seal_is_remediated_once() {
        let (cursor, events) = deliver(
            at(7, 1),
            &[
                frame(7, 3, &["c"], true),
                frame(7, 3, &["c"], true),
                frame(9, 1, &[], false),
            ],
        );
        assert_eq!(
            events,
            [
                gap(7, 2),
                wrote(7, 3, "c"),
                PeerWrite::Sealed {
                    peer: NodeId::from("w"),
                    token: token(7, 4),
                },
                PeerWrite::Renewed {
                    peer: NodeId::from("w"),
                    sealed: token(7, 4),
                    epoch: 9,
                },
            ]
        );
        assert_eq!(cursor, at(9, 1));
    }

    /// A subscriber attaching to a life that already ended starts past its
    /// seal, delivers nothing of it, and crosses into the next life without
    /// a gap: it attached after the last write, so it missed none.
    #[test]
    fn attaching_to_a_sealed_life_starts_past_the_seal() {
        let start = Cursor::attached(&frame(7, 1, &["a", "b"], true));
        assert_eq!(
            start,
            Cursor {
                epoch: 7,
                next: 4,
                sealed: true,
            }
        );
        let (_, events) = deliver(start, &[frame(9, 1, &["n1"], false)]);
        assert_eq!(
            events,
            [
                PeerWrite::Renewed {
                    peer: NodeId::from("w"),
                    sealed: token(7, 3),
                    epoch: 9,
                },
                wrote(9, 1, "n1"),
            ]
        );
    }
}
