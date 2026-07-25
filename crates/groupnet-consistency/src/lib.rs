//! # groupnet-consistency
//!
//! Session-consistency primitives over the Groupnet fabric — the reusable
//! layer for cross-node write propagation with honest guarantees.
//!
//! Each node publishes its recent writes as a compact ring inside one
//! versioned, gossiped group entry ([`WriteFeed`]); every peer turns entry
//! changes into typed events ([`PeerWrites`]): [`PeerWrite::Wrote`] for each
//! new write, or [`PeerWrite::Gap`] when writes were provably missed (the
//! peer's ring advanced past this subscriber's cursor, or the peer
//! restarted). The application applies each event — a cache invalidates the
//! key, an index refreshes the id, a replica schedules a refetch — and
//! advances a [`Frontier`], so readers can barrier on *applied* state with
//! [`FrontierView::reached`].
//!
//! # What you get — and what you deliberately don't
//!
//! Provided, and safe to rely on:
//!
//! - **Per-writer total order.** Each node's feed is totally ordered by
//!   [`WriteToken`] (epoch-major, then sequence), and subscribers observe
//!   its writes in publication order.
//! - **Loss is detected, never silent.** Ring overflow past a slow
//!   subscriber — and a writer restart — degrade to an explicit
//!   [`PeerWrite::Gap`], not a skip.
//! - **Read-your-writes and monotonic reads, per writer.**
//!   [`WriteFeed::publish`] resolves to the write's [`WriteToken`]; hand
//!   `(writer, token)` to the client as a session token, and any node
//!   serving that client barriers with [`FrontierView::reached`] before
//!   reading locally. Because the frontier is advanced by *your* apply
//!   loop, "reached" means applied — not merely delivered.
//!
//! Not provided, by design:
//!
//! - **Cross-writer ordering, consensus, fencing.** Two nodes' feeds have no
//!   mutual order, and groupnet's coordinator is derived, not fenced. For
//!   "exactly one writer may proceed", fence at an external authority (a
//!   store with conditional writes) or run a consensus log — this crate will
//!   not pretend to do it with gossip.
//!
//! # Writer restarts (why tokens carry an epoch)
//!
//! A restarted writer starts a fresh ring at sequence 1. Bare sequence
//! numbers would then lie twice: a subscriber's cursor from the old life
//! would sit past the new ring (deaf forever), and an old high watermark
//! would satisfy new-life barriers instantly (stale reads passed off as
//! fresh). So every feed life carries an **epoch**, tokens are
//! `(epoch, seq)`, and ordering is epoch-major:
//!
//! - By default the epoch is the wall-clock time at [`WriteFeed::new`] —
//!   strictly increasing across restarts unless the clock steps backwards.
//!   Applications with a durable counter (a WAL generation, a boot counter)
//!   should prefer [`WriteFeed::with_epoch`].
//! - Subscribers surface an epoch change as a [`PeerWrite::Gap`] covering
//!   the entire previous life (epoch-major ordering makes the new-life
//!   token compare above every old-life token), then resume normal
//!   delivery.
//! - The [`Frontier`] stores one [`WriteToken`] watermark per writer; after
//!   the gap remediation advances it into the new epoch, old-life barriers
//!   remain satisfied (the remediation covered them) and new-life barriers
//!   behave normally.
//!
//! # Semantics (read this once, rely on it forever)
//!
//! - **State-based, not a log.** The feed entry always carries the last N
//!   writes; gossip loss, event lag, and duplication are all safe because
//!   subscribers reconcile against the current entry, and applying a write
//!   notification must be idempotent. Missing writes are *detected*, never
//!   silently dropped.
//! - **Eventual, bounded by propagation latency.** A peer observes a write
//!   after roughly one gossip round (or one round trip, when the engine's
//!   eager delta push is enabled). The barrier for "has it landed?" is the
//!   [`Frontier`], never wall-clock time.
//! - **Keys travel by your codec — and a "key" is any datum.** Provide
//!   encode/decode closures (no forced serde). Encoding `(key, version)`
//!   pairs, or a key plus a small value, turns the feed from invalidation
//!   into write-through propagation with zero API changes; keep the datum
//!   small (it rides the gossiped ring) and keep codecs in lockstep across
//!   nodes. A datum that fails to decode is skipped.
//! - **History is not replayed.** A subscriber starts at each existing peer
//!   feed's current end (a fresh node has nothing stale to fix up). Feeds
//!   appearing later replay their visible window — those writes are
//!   genuinely new to this subscriber.
//! - **One feed per name per group.** The default feed occupies one
//!   reserved entry; independent subsystems sharing a group must use
//!   [`WriteFeed::named`] / [`PeerWrites::named`] to keep their feeds
//!   apart.
//! - **The frontier remembers every writer it has seen.** One token per
//!   peer, bounded by group membership — departed peers' watermarks linger
//!   harmlessly.
//!
//! # Example
//!
//! ```no_run
//! use std::collections::HashSet;
//! use std::num::NonZeroUsize;
//! use std::sync::{Arc, Mutex};
//!
//! use groupnet_consistency::{Frontier, PeerWrite, PeerWrites, WriteFeed};
//! use groupnet_core::NodeId;
//! use groupnet_runtime::Node;
//! use groupnet_transport_mem::Network;
//!
//! # async fn demo() {
//! let net = Network::new();
//! let me = NodeId::new("node-a");
//! let node = Node::builder(me.clone(), net.endpoint(me.clone())).spawn();
//! let group = node.join_group("stores");
//!
//! let feed = WriteFeed::new(
//!     group.clone(),
//!     NonZeroUsize::new(128).unwrap(),
//!     |key: &String| key.clone().into_bytes(),
//! );
//! let mut peers = PeerWrites::new(group, me, |bytes| {
//!     String::from_utf8(bytes.to_vec()).ok()
//! });
//! let (frontier, view) = Frontier::new();
//!
//! // The node-local state kept coherent — a cache, an index, a replica.
//! let fresh: Arc<Mutex<HashSet<String>>> = Arc::default();
//!
//! // After every local durable write (the token is the client's RYW token):
//! let token = feed.publish(&"user:1".to_owned()).await;
//!
//! // Apply peer writes, advancing the frontier only once applied:
//! let local = Arc::clone(&fresh);
//! tokio::spawn(async move {
//!     while let Some(event) = peers.next().await {
//!         match event {
//!             PeerWrite::Wrote { peer, token, key } => {
//!                 local.lock().unwrap().remove(&key); // drop the stale copy
//!                 frontier.advance(&peer, token);
//!             }
//!             PeerWrite::Gap {
//!                 peer,
//!                 missed_through,
//!             } => {
//!                 local.lock().unwrap().clear(); // coarse remediation
//!                 frontier.advance(&peer, missed_through);
//!             }
//!         }
//!     }
//! });
//!
//! // Serving a client that carries a token (writer, token): barrier first.
//! # let writer = NodeId::new("node-b");
//! if view.reached(&writer, token).await {
//!     // local state now reflects that write — serve the read
//! }
//! # }
//! ```

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use groupnet_core::NodeId;
use groupnet_runtime::{Group, GroupEvent};
use tokio::sync::broadcast::error::RecvError;

/// The group entry key under which a node's default write feed is gossiped
/// (`~`-prefixed like the runtime's reserved entries). Named feeds append
/// `:<name>`.
const ENTRY_KEY: &str = "~writes";

/// Attempts before giving up on advertising a frame under inbox
/// backpressure (the ring keeps the write; the next publish re-carries it).
const PUBLISH_RETRIES: usize = 8;

type EncodeFn<K> = dyn Fn(&K) -> Vec<u8> + Send + Sync;
type DecodeFn<K> = dyn Fn(&[u8]) -> Option<K> + Send + Sync;

/// The entry key for a feed name: the reserved default, or `~writes:<name>`.
fn entry_key(name: &str) -> String {
    if name.is_empty() {
        ENTRY_KEY.to_owned()
    } else {
        format!("{ENTRY_KEY}:{name}")
    }
}

/// A write's position in one writer's feed: the feed life (`epoch`) and the
/// sequence number within it. The derived ordering is epoch-major, so any
/// token from a newer life compares above every token from an older one —
/// exactly the comparison [`Frontier`] watermarks need.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WriteToken {
    /// The feed life this write belongs to (see the crate docs on writer
    /// restarts).
    pub epoch: u64,
    /// The write's sequence number within the epoch, starting at 1.
    pub seq: u64,
}

/// One peer-write notification from [`PeerWrites::next`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerWrite<K> {
    /// `peer` wrote `key` at `token`. Apply it (drop the stale copy, refresh
    /// the index entry, …), then advance the [`Frontier`] to `token`.
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
    /// then advance the [`Frontier`] to `missed_through`.
    Gap {
        /// The node whose writes were missed.
        peer: NodeId,
        /// After remediating, every write of `peer` up to and including
        /// this token is covered.
        missed_through: WriteToken,
    },
}

/// The wire frame: the feed epoch, `first_seq`, and the encoded keys of the
/// last N writes, sequential from `first_seq`.
struct Frame {
    epoch: u64,
    first_seq: u64,
    keys: Vec<Vec<u8>>,
}

impl Frame {
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(20 + self.keys.iter().map(|k| 4 + k.len()).sum::<usize>());
        out.extend_from_slice(&self.epoch.to_le_bytes());
        out.extend_from_slice(&self.first_seq.to_le_bytes());
        out.extend_from_slice(
            &u32::try_from(self.keys.len())
                .unwrap_or(u32::MAX)
                .to_le_bytes(),
        );
        for key in &self.keys {
            out.extend_from_slice(&u32::try_from(key.len()).unwrap_or(u32::MAX).to_le_bytes());
            out.extend_from_slice(key);
        }
        out
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        let epoch = u64::from_le_bytes(bytes.get(0..8)?.try_into().ok()?);
        let first_seq = u64::from_le_bytes(bytes.get(8..16)?.try_into().ok()?);
        let count = u32::from_le_bytes(bytes.get(16..20)?.try_into().ok()?);
        let mut offset = 20_usize;
        let mut keys = Vec::with_capacity(usize::try_from(count).ok()?.min(4096));
        for _ in 0..count {
            let len = usize::try_from(u32::from_le_bytes(
                bytes.get(offset..offset + 4)?.try_into().ok()?,
            ))
            .ok()?;
            offset += 4;
            keys.push(bytes.get(offset..offset + len)?.to_vec());
            offset += len;
        }
        Some(Self {
            epoch,
            first_seq,
            keys,
        })
    }

    fn end(&self) -> u64 {
        self.first_seq + self.keys.len() as u64
    }
}

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
    /// than the ring holds receive a [`PeerWrite::Gap`] instead of the
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
    /// [`PeerWrites::named`] under the same name).
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
    /// [`WriteFeed::named`]).
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

/// Applied-write watermarks per peer, advanced by the application's apply
/// loop — see [`Frontier`].
type Applied = HashMap<NodeId, WriteToken>;

/// The writer half of the applied-write frontier.
///
/// The apply loop calls [`Frontier::advance`] after each peer write has
/// actually been applied (the stale copy dropped, or the gap remediation
/// finished). Barriers on the matching [`FrontierView`] then mean *applied*,
/// not merely delivered.
#[derive(Debug)]
pub struct Frontier {
    tx: tokio::sync::watch::Sender<Applied>,
}

/// The reader half: cheap to clone, held wherever reads need a
/// read-your-writes barrier.
#[derive(Debug, Clone)]
pub struct FrontierView {
    rx: tokio::sync::watch::Receiver<Applied>,
}

impl Frontier {
    /// A fresh frontier (nothing applied) and its reader view.
    #[must_use]
    pub fn new() -> (Self, FrontierView) {
        let (tx, rx) = tokio::sync::watch::channel(Applied::new());
        (Self { tx }, FrontierView { rx })
    }

    /// Marks `peer`'s writes as applied through `token` (monotonic in
    /// epoch-major order: lower tokens are ignored).
    pub fn advance(&self, peer: &NodeId, token: WriteToken) {
        self.tx.send_modify(|applied| {
            let entry = applied
                .entry(peer.clone())
                .or_insert(WriteToken { epoch: 0, seq: 0 });
            if *entry < token {
                *entry = token;
            }
        });
    }
}

impl FrontierView {
    /// Waits until `peer`'s writes through `token` have been applied
    /// locally. A watermark from a newer epoch also satisfies older-epoch
    /// tokens: the frontier only enters a new epoch through gap
    /// remediation, which covered the previous life.
    ///
    /// Returns `false` if the [`Frontier`] was dropped first (the apply
    /// loop is gone — do not serve reads assuming freshness). Combine with
    /// a caller-side timeout for bounded waiting.
    pub async fn reached(&self, peer: &NodeId, token: WriteToken) -> bool {
        let mut rx = self.rx.clone();
        rx.wait_for(|applied| applied.get(peer).is_some_and(|&t| t >= token))
            .await
            .is_ok()
    }
}

#[cfg(test)]
mod frame_tests {
    use super::*;

    #[test]
    fn frame_round_trips() {
        let frame = Frame {
            epoch: 7,
            first_seq: 41,
            keys: vec![b"alpha".to_vec(), Vec::new(), b"c".to_vec()],
        };
        let decoded = Frame::decode(&frame.encode()).expect("decode");
        assert_eq!(decoded.epoch, 7);
        assert_eq!(decoded.first_seq, 41);
        assert_eq!(decoded.keys, frame.keys);
        assert_eq!(decoded.end(), 44);
    }

    #[test]
    fn truncated_frames_are_rejected() {
        let bytes = Frame {
            epoch: 3,
            first_seq: 1,
            keys: vec![b"key".to_vec()],
        }
        .encode();
        for cut in 0..bytes.len() {
            assert!(Frame::decode(&bytes[..cut]).is_none(), "cut at {cut}");
        }
    }

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

    #[test]
    fn tokens_order_epoch_major() {
        let old_life = WriteToken { epoch: 1, seq: 500 };
        let new_life = WriteToken { epoch: 2, seq: 1 };
        assert!(new_life > old_life, "any new-life token beats old-life");
    }

    #[test]
    fn feed_names_map_to_distinct_entries() {
        assert_eq!(entry_key(""), "~writes");
        assert_eq!(entry_key("docs"), "~writes:docs");
        assert_ne!(entry_key("docs"), entry_key("index"));
    }
}
