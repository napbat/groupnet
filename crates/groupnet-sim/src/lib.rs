//! # groupnet-sim
//!
//! A **deterministic, single-threaded** driver for [`GroupEngine`]s. It owns a
//! virtual clock and an in-memory network with a configurable (but *fixed*,
//! never random) latency and drop schedule, then steps every engine in strict
//! time order.
//!
//! This is the same core that [`groupnet-runtime`] runs across threads in
//! production — here it runs in a plain event loop so an entire cluster's
//! behaviour is reproducible bit-for-bit. No async runtime, no real sockets, no
//! wall clock.
//!
//! ```
//! use groupnet_core::{Config, GroupEngine, GroupId, NodeId, Time};
//! use groupnet_sim::Simulation;
//!
//! let group = GroupId::new("shard-42");
//! let ids: Vec<NodeId> = ["a", "b", "c"].iter().map(|s| NodeId::new(*s)).collect();
//! let mut sim = Simulation::new(10); // 10ms link latency
//! for id in &ids {
//!     let seeds = ids.iter().filter(|x| *x != id).cloned();
//!     sim.add(GroupEngine::new(group.clone(), id.clone(), seeds, Config::default()));
//! }
//! sim.run_until(Time(5_000));
//! assert!(sim.all_agree_on_coordinator());
//! ```
//!
//! [`groupnet-runtime`]: https://docs.rs/groupnet-runtime

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};

use groupnet_core::{Command, Effect, GroupEngine, NodeId, Status, Time};

mod rng;

pub use rng::SplitMix64;

/// One scheduled future event in the simulation.
struct Event {
    at: Time,
    seq: u64,
    kind: Kind,
}

enum Kind {
    /// Deliver a frame to a node.
    Deliver {
        to: NodeId,
        from: NodeId,
        wire: Vec<u8>,
    },
    /// Fire a timer tick on a node.
    Tick { node: NodeId },
}

// The heap is a max-heap; we invert the ordering so `pop` yields the *earliest*
// `(at, seq)`. `seq` breaks ties so delivery order is fully deterministic.
impl Ord for Event {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .at
            .cmp(&self.at)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}
impl PartialOrd for Event {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for Event {
    fn eq(&self, other: &Self) -> bool {
        self.at == other.at && self.seq == other.seq
    }
}
impl Eq for Event {}

/// A deterministic in-memory cluster of engines.
#[derive(Debug)]
pub struct Simulation {
    now: Time,
    latency_ms: u64,
    engines: BTreeMap<NodeId, GroupEngine>,
    queue: BinaryHeap<Event>,
    seq: u64,
    /// Deterministic per-message loss probability, 0..=100 percent.
    loss_percent: u8,
    /// Deterministic per-message extra latency, 0..=`jitter_ms`, so messages on a
    /// link can arrive out of send order (models reorder). 0 disables it.
    jitter_ms: u64,
    /// Directed links that drop every message (a one-way partition).
    blocked: BTreeSet<(NodeId, NodeId)>,
    /// Largest encoded frame (`Effect::Send` payload) the sim has carried — lets a
    /// test assert every emitted frame stayed within the configured cap.
    max_frame_bytes: usize,
    /// Observed coordinator transitions, in the order the sim saw them.
    pub coordinator_log: Vec<(NodeId, Option<NodeId>)>,
}

impl std::fmt::Debug for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Event").field("at", &self.at).finish()
    }
}

impl Simulation {
    /// Creates a simulation where every link delivers after `latency_ms` of
    /// logical time and no messages are dropped.
    #[must_use]
    pub fn new(latency_ms: u64) -> Self {
        Self {
            now: Time::ZERO,
            latency_ms,
            engines: BTreeMap::new(),
            queue: BinaryHeap::new(),
            seq: 0,
            loss_percent: 0,
            jitter_ms: 0,
            blocked: BTreeSet::new(),
            max_frame_bytes: 0,
            coordinator_log: Vec::new(),
        }
    }

    /// Drops every message on the directed link `from -> to`, modelling a
    /// one-way partition. Call twice (both directions) for a full partition.
    pub fn block(&mut self, from: &NodeId, to: &NodeId) {
        self.blocked.insert((from.clone(), to.clone()));
    }

    /// Restores the directed link `from -> to`.
    pub fn heal(&mut self, from: &NodeId, to: &NodeId) {
        self.blocked.remove(&(from.clone(), to.clone()));
    }

    /// Restores every partitioned link.
    pub fn heal_all(&mut self) {
        self.blocked.clear();
    }

    /// Changes the per-message loss probability mid-run (0..=100).
    pub fn set_loss(&mut self, percent: u8) {
        self.loss_percent = percent.min(100);
    }

    /// Sets the maximum deterministic per-message extra latency. With a non-zero
    /// jitter, messages on the same link can arrive out of send order, exercising
    /// the protocol's tolerance of reordering. Reproducible run-to-run.
    pub fn set_jitter(&mut self, ms: u64) {
        self.jitter_ms = ms;
    }

    /// The largest encoded frame the sim has carried so far. A test asserts this
    /// stays within the engine's `max_delta_frame_bytes` cap across a whole run.
    #[must_use]
    pub fn max_frame_bytes(&self) -> usize {
        self.max_frame_bytes
    }

    /// Sets deterministic per-message packet loss (`percent` of 0..=100).
    /// Reproducible run-to-run; no randomness is used anywhere. Each message is
    /// dropped independently based on a hash of its scheduling sequence.
    #[must_use]
    pub fn with_loss(mut self, percent: u8) -> Self {
        self.loss_percent = percent.min(100);
        self
    }

    /// Adds an engine to the cluster and primes it (calls `start`).
    pub fn add(&mut self, mut engine: GroupEngine) {
        let id = engine.local().clone();
        let effects = engine.start(self.now);
        self.engines.insert(id.clone(), engine);
        self.dispatch(&id, effects);
    }

    /// Runs the event loop until logical time reaches `max` (or the queue
    /// empties, which won't happen while periodic gossip is armed).
    pub fn run_until(&mut self, max: Time) {
        while let Some(event) = self.queue.peek() {
            if event.at > max {
                break;
            }
            let event = self.queue.pop().expect("peeked");
            self.now = event.at;
            let now = self.now;
            let seq = event.seq;
            match event.kind {
                Kind::Deliver { to, from, wire } => {
                    if self.loss_percent != 0
                        && SplitMix64::hash(seq) % 100 < u64::from(self.loss_percent)
                    {
                        continue; // deterministic per-message drop
                    }
                    if self.blocked.contains(&(from.clone(), to.clone())) {
                        continue; // partitioned link
                    }
                    let effects = match self.engines.get_mut(&to) {
                        Some(engine) => engine.on_message(from, &wire, now),
                        None => continue,
                    };
                    self.dispatch(&to, effects);
                }
                Kind::Tick { node } => {
                    let effects = match self.engines.get_mut(&node) {
                        Some(engine) => engine.on_tick(self.now),
                        None => continue,
                    };
                    self.dispatch(&node, effects);
                }
            }
        }
        self.now = max;
    }

    /// The coordinator a given node currently believes in.
    #[must_use]
    pub fn coordinator_of(&self, node: &NodeId) -> Option<NodeId> {
        self.engines
            .get(node)
            .and_then(|e| e.coordinator().cloned())
    }

    /// How many members `node` currently knows about.
    #[must_use]
    pub fn member_count(&self, node: &NodeId) -> usize {
        self.engines.get(node).map_or(0, |e| e.members().count())
    }

    /// Applies a local command at `node` and schedules its effects.
    pub fn command(&mut self, node: &NodeId, cmd: Command) {
        let effects = match self.engines.get_mut(node) {
            Some(engine) => engine.apply(cmd),
            None => return,
        };
        self.dispatch(node, effects);
    }

    /// Reads a metadata value as `node` currently sees it.
    #[must_use]
    pub fn metadata_of(&self, node: &NodeId, key: &str) -> Option<String> {
        self.engines
            .get(node)
            .and_then(|e| e.metadata(key).map(str::to_owned))
    }

    /// Abruptly removes `node` from the simulation — it stops sending acks and
    /// gossip, modelling a crash. Survivors must detect it via failure
    /// detection.
    pub fn crash(&mut self, node: &NodeId) {
        self.engines.remove(node);
    }

    /// Whether `observer` currently considers `node` a live member (present and
    /// not `Dead`).
    #[must_use]
    pub fn is_member(&self, observer: &NodeId, node: &NodeId) -> bool {
        self.engines
            .get(observer)
            .is_some_and(|e| e.members().any(|n| n == node))
    }

    /// The status `observer` holds for `node`, if any (including `Dead`).
    #[must_use]
    pub fn status_of(&self, observer: &NodeId, node: &NodeId) -> Option<Status> {
        self.engines
            .get(observer)
            .and_then(|e| e.member_status(node))
    }

    /// The app-defined per-node state that `observer` currently holds for
    /// `node`.
    #[must_use]
    pub fn state_of(&self, observer: &NodeId, node: &NodeId) -> Option<Vec<u8>> {
        self.engines
            .get(observer)
            .and_then(|e| e.node_state(node).map(<[u8]>::to_vec))
    }

    /// The ids of every engine currently in the simulation.
    #[must_use]
    pub fn nodes(&self) -> Vec<NodeId> {
        self.engines.keys().cloned().collect()
    }

    /// The live members (not `Dead`) that `observer` currently sees.
    #[must_use]
    pub fn members_of(&self, observer: &NodeId) -> BTreeSet<NodeId> {
        self.engines
            .get(observer)
            .map(|e| e.members().cloned().collect())
            .unwrap_or_default()
    }

    /// `observer`'s full metadata view.
    #[must_use]
    pub fn metadata_snapshot(&self, observer: &NodeId) -> BTreeMap<String, String> {
        self.engines.get(observer).map_or_else(BTreeMap::new, |e| {
            e.metadata_iter()
                .map(|(k, v)| (k.to_owned(), v.to_owned()))
                .collect()
        })
    }

    /// `observer`'s full per-node app-state view.
    #[must_use]
    pub fn state_snapshot(&self, observer: &NodeId) -> BTreeMap<NodeId, Vec<u8>> {
        self.engines.get(observer).map_or_else(BTreeMap::new, |e| {
            e.node_states_iter()
                .map(|(n, s)| (n.clone(), s.to_vec()))
                .collect()
        })
    }

    /// `observer`'s full per-node **keyed-entry** view (live entries only —
    /// tombstoned/expired keys are absent), so a test can assert every node
    /// converges on every node's whole keyed map, not just the `~blob` shim.
    #[must_use]
    pub fn entries_snapshot(
        &self,
        observer: &NodeId,
    ) -> BTreeMap<NodeId, BTreeMap<String, Vec<u8>>> {
        self.engines.get(observer).map_or_else(BTreeMap::new, |e| {
            let nodes: Vec<NodeId> = e.member_statuses().map(|(n, _)| n.clone()).collect();
            let mut out = BTreeMap::new();
            for node in nodes {
                let entries: BTreeMap<String, Vec<u8>> = e
                    .node_entries(&node)
                    .map(|(k, v)| (k.to_owned(), v.to_vec()))
                    .collect();
                if !entries.is_empty() {
                    out.insert(node, entries);
                }
            }
            out
        })
    }

    /// Whether every node has converged on the same (non-`None`) coordinator.
    #[must_use]
    pub fn all_agree_on_coordinator(&self) -> bool {
        let mut coords = self.engines.values().map(|e| e.coordinator().cloned());
        let Some(Some(first)) = coords.next() else {
            return false; // empty cluster, or someone has no coordinator yet
        };
        coords.all(|c| c == Some(first.clone()))
    }

    fn dispatch(&mut self, node: &NodeId, effects: Vec<Effect>) {
        for effect in effects {
            match effect {
                Effect::Send { to, wire } => {
                    self.max_frame_bytes = self.max_frame_bytes.max(wire.len());
                    // Deterministic per-message jitter keyed on the sequence number
                    // this delivery is about to take, so links can reorder.
                    let extra = if self.jitter_ms == 0 {
                        0
                    } else {
                        SplitMix64::hash(self.seq.wrapping_add(1)) % (self.jitter_ms + 1)
                    };
                    let at = self
                        .now
                        .saturating_add(self.latency_ms)
                        .saturating_add(extra);
                    self.schedule(
                        at,
                        Kind::Deliver {
                            to,
                            from: node.clone(),
                            wire,
                        },
                    );
                }
                Effect::ArmTimer { at } => {
                    // `at` is absolute; never schedule in the past.
                    let at = at.max(self.now);
                    self.schedule(at, Kind::Tick { node: node.clone() });
                }
                Effect::CoordinatorChanged { coordinator } => {
                    self.coordinator_log.push((node.clone(), coordinator));
                }
                Effect::MembershipChanged
                | Effect::NodeStateChanged { .. }
                | Effect::MetadataChanged { .. } => {}
            }
        }
    }

    fn schedule(&mut self, at: Time, kind: Kind) {
        self.seq += 1;
        self.queue.push(Event {
            at,
            seq: self.seq,
            kind,
        });
    }
}
