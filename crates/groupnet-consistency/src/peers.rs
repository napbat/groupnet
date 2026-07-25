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
    advertised_head_named("", group, peer)
}

/// [`advertised_head`] for a named feed (see [`WriteFeed::named`](crate::WriteFeed::named)).
#[must_use]
pub fn advertised_head_named(name: &str, group: &Group, peer: &NodeId) -> Option<WriteToken> {
    let frame = Frame::decode(&group.node_entry(peer, &entry_key(name))?)?;
    let head = frame.end().checked_sub(1)?;
    (head >= frame.first_seq).then_some(WriteToken {
        epoch: frame.epoch,
        seq: head,
    })
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
    /// new epoch (epoch-major ordering makes `missed_through` cover the
    /// whole previous life). Remediate coarsely (flush, rebuild, refetch),
    /// then advance the [`Frontier`](crate::Frontier) to `missed_through`.
    Gap {
        /// The node whose writes were missed.
        peer: NodeId,
        /// After remediating, every write of `peer` up to and including
        /// this token is covered.
        missed_through: WriteToken,
    },
}

/// A subscriber's position in one peer's feed.
#[derive(Clone, Copy)]
struct Cursor {
    epoch: u64,
    /// Next unseen sequence number within `epoch`.
    next: u64,
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
                    cursors.insert(
                        node.clone(),
                        Cursor {
                            epoch: frame.epoch,
                            next: frame.end(),
                        },
                    );
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
    /// keep restarting).
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

    /// Reconciles one peer's feed against our cursor, queueing events.
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
        });
        if frame.epoch < cursor.epoch {
            return; // a stale frame from a previous life — ignore
        }
        if frame.epoch > cursor.epoch {
            // The writer restarted. Epoch-major token ordering makes this
            // gap cover every write of the previous life as well.
            self.pending.push_back(PeerWrite::Gap {
                peer: node.clone(),
                missed_through: WriteToken {
                    epoch: frame.epoch,
                    seq: frame.first_seq.saturating_sub(1),
                },
            });
            self.gaps_seen += 1;
            *cursor = Cursor {
                epoch: frame.epoch,
                next: frame.first_seq,
            };
        } else if cursor.next < frame.first_seq {
            // The ring advanced past us: writes were provably missed.
            self.pending.push_back(PeerWrite::Gap {
                peer: node.clone(),
                missed_through: WriteToken {
                    epoch: frame.epoch,
                    seq: frame.first_seq.saturating_sub(1),
                },
            });
            self.gaps_seen += 1;
            cursor.next = frame.first_seq;
        }
        while cursor.next < frame.end() {
            let Ok(index) = usize::try_from(cursor.next - frame.first_seq) else {
                break;
            };
            if let Some(key) = (self.decode)(&frame.keys[index]) {
                self.pending.push_back(PeerWrite::Wrote {
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
    }
}
