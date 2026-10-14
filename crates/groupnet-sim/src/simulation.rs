//! The deterministic event loop: virtual clock, lossy/partitionable
//! in-memory network, and the [`Simulation`] driver over real engines.
//!
//! The probe/liveness dispatch (frame delivery, timer ticks, the frame probes
//! and the liveness observers) lives in [`liveness`]; the external CAS anchor's
//! driver and knobs live in [`external`].

mod external;
mod liveness;

use crate::anchor::{AnchorEvent, AnchorModel};
use crate::error::SimError;
use crate::percent::Percent;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};

use groupnet_core::anchor::AnchorRecord;
use groupnet_core::{Command, Effect, GroupEngine, NodeId, Role, Time};

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
    /// A driver's external-anchor round, landing at the store one anchor
    /// latency after the [`Effect::AnchorClaimDue`] that prompted it. The
    /// whole round — load, plan, conditional write — happens at this one
    /// instant, which is what makes the register linearizable by construction.
    AnchorRound {
        node: NodeId,
        epoch_hint: u64,
        /// When the round *began*: the instant `lease_until` is anchored at,
        /// deliberately not the instant the write landed. See
        /// [`Command::AnchorActivated`].
        started_at: Time,
    },
    /// The read-back that resolves a conditional write the store reported
    /// `Unknown` for — a separate instant, because that is the only way the
    /// record can have moved on underneath it.
    AnchorReadBack {
        node: NodeId,
        attempted: AnchorRecord,
        started_at: Time,
    },
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

impl std::fmt::Debug for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Hand-written because `Kind` carries whole encoded frames; the schedule
        // key is what a trace needs to read.
        f.debug_struct("Event")
            .field("at", &self.at)
            .field("seq", &self.seq)
            .finish_non_exhaustive()
    }
}

/// A deterministic in-memory cluster of engines — a **one-engine-per-node
/// protocol harness**.
///
/// Each [`NodeId`] hosts exactly one [`GroupEngine`], and so takes part in
/// exactly one group. This is not a simulator of a multi-group node: a real
/// node hosting several groups over one transport is groupnet-runtime's job,
/// and that behaviour is tested over the runtime with groupnet-testkit's
/// in-memory cluster instead.
///
/// A call that targets one node by id either reports a missing node in its
/// return type or refuses it: mutations return [`SimError::UnknownNode`],
/// read-only queries return `None`, and [`add`](Self::add) refuses a second
/// engine for an id with [`SimError::DuplicateNode`]. The link and anchor
/// knobs ([`block`](Self::block), [`block_anchor`](Self::block_anchor),
/// [`set_anchor_skew`](Self::set_anchor_skew), …) are the exception by
/// design: they model the node's host and network, not its engine, so they
/// accept any id and outlive [`crash`](Self::crash).
#[derive(Debug)]
pub struct Simulation {
    now: Time,
    latency_ms: u64,
    engines: BTreeMap<NodeId, GroupEngine>,
    queue: BinaryHeap<Event>,
    seq: u64,
    /// Deterministic per-message loss probability.
    loss: Percent,
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
    /// Observed leadership transitions — `(observer, epoch, host)` — in the
    /// order the sim saw them. The Hosted-mode counterpart of
    /// [`coordinator_log`](Self::coordinator_log); an `Eventual` run leaves it
    /// empty, which is itself the assertion that mode invariance holds.
    pub leadership_log: Vec<(NodeId, u64, Option<NodeId>)>,
    /// How many delivered frames carried an election kind. See
    /// [`election_frames_seen`](Self::election_frames_seen).
    election_frames: u64,
    /// How many `LeadClaim` frames the sim has seen **issued**. See
    /// [`claim_frames_seen`](Self::claim_frames_seen).
    claim_frames: u64,
    /// How many `LeadGrant` frames the sim has seen **issued**. See
    /// [`grant_frames_seen`](Self::grant_frames_seen).
    grant_frames: u64,
    /// The **durable** voter ledger this sim keeps on each node's behalf,
    /// written from [`Effect::PersistGrant`] — the store a driver with voter
    /// durability would provide. Keyed by granter, and deliberately *not* held
    /// inside the engine, so it survives [`crash`](Self::crash) and can be fed
    /// back through
    /// [`GroupEngine::with_recovered`](groupnet_core::GroupEngine::with_recovered).
    persisted_grants: BTreeMap<NodeId, (u64, NodeId)>,
    /// Every grant frame this sim has seen **issued** — `(when, granter,
    /// epoch, claimant)`, appended at dispatch rather than at delivery, so a
    /// grant that is lost or partitioned away still appears here. That is what
    /// makes it usable as the one-grant-per-epoch-per-voter probe: the property
    /// is about what a voter *said*, not about what anyone heard.
    pub grant_log: Vec<(Time, NodeId, u64, NodeId)>,
    /// The external CAS anchor: the register, the per-node driver state around
    /// it, and its fault knobs. Inert until
    /// [`enable_anchor`](Self::enable_anchor) arms it.
    anchor: AnchorModel,
    /// Every anchor round that touched the register — `(when, node, what)`, in
    /// schedule order. The register's whole history, and the probe the steal /
    /// renewal / yield floors are read off.
    ///
    /// A round that never ran — a crashed node, or one that cannot reach the
    /// anchor — leaves nothing here, which is itself the fail-closed signal:
    /// the silence *is* the reason that node's lease is about to lapse.
    ///
    /// One caveat, and only under
    /// [`set_anchor_unknown_lost_percent`](Self::set_anchor_unknown_lost_percent):
    /// an entry records the round the driver *attempted*, which under that knob
    /// is a write the store swallowed. [`anchor_record`](Self::anchor_record) is
    /// the ground truth for what actually landed, and the two deliberately
    /// disagree there.
    pub anchor_log: Vec<(Time, NodeId, AnchorEvent)>,
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
            loss: Percent::ZERO,
            jitter_ms: 0,
            blocked: BTreeSet::new(),
            max_frame_bytes: 0,
            coordinator_log: Vec::new(),
            leadership_log: Vec::new(),
            election_frames: 0,
            claim_frames: 0,
            grant_frames: 0,
            persisted_grants: BTreeMap::new(),
            grant_log: Vec::new(),
            anchor: AnchorModel::default(),
            anchor_log: Vec::new(),
        }
    }

    /// Drops every message on the directed link `from -> to`, modelling a
    /// one-way partition. Call twice (both directions) for a full partition.
    ///
    /// A link is network state, not engine state: it accepts any pair of ids,
    /// may be cut before either end is [`add`](Self::add)ed, and stays cut
    /// across [`crash`](Self::crash) and a restart until healed.
    pub fn block(&mut self, from: &NodeId, to: &NodeId) {
        self.blocked.insert((from.clone(), to.clone()));
    }

    /// Restores the directed link `from -> to`. Network state, like
    /// [`block`](Self::block): accepts any pair of ids.
    pub fn heal(&mut self, from: &NodeId, to: &NodeId) {
        self.blocked.remove(&(from.clone(), to.clone()));
    }

    /// Restores every partitioned link.
    pub fn heal_all(&mut self) {
        self.blocked.clear();
    }

    /// Changes the per-message loss probability mid-run.
    pub fn set_loss(&mut self, loss: Percent) {
        self.loss = loss;
    }

    /// Sets the maximum deterministic per-message extra latency. With a non-zero
    /// jitter, messages on the same link can arrive out of send order, exercising
    /// the protocol's tolerance of reordering. Reproducible run-to-run.
    pub fn set_jitter(&mut self, ms: u64) {
        self.jitter_ms = ms;
    }

    /// Sets deterministic per-message packet loss.
    /// Reproducible run-to-run; no randomness is used anywhere. Each message is
    /// dropped independently based on a hash of its scheduling sequence.
    #[must_use]
    pub fn with_loss(mut self, loss: Percent) -> Self {
        self.loss = loss;
        self
    }

    /// Adds an engine to the cluster under its local id and primes it (calls
    /// `start`).
    ///
    /// One engine per node: an id hosts at most one engine at a time. To
    /// restart a node, [`crash`](Self::crash) it first and add the new engine.
    ///
    /// # Errors
    /// [`SimError::DuplicateNode`] if an engine with the same local id is
    /// already in the simulation. The engine in place keeps running untouched,
    /// and the rejected one is dropped without being started.
    pub fn add(&mut self, mut engine: GroupEngine) -> Result<(), SimError> {
        let id = engine.local().clone();
        if self.engines.contains_key(&id) {
            return Err(SimError::DuplicateNode(id));
        }
        let effects = engine.start(self.now);
        self.engines.insert(id.clone(), engine);
        self.dispatch(&id, effects);
        Ok(())
    }

    /// Runs the event loop until logical time reaches `max` (or the queue
    /// empties, which won't happen while periodic gossip is armed).
    pub fn run_until(&mut self, max: Time) {
        while self.step_until(max).is_some() {}
    }

    /// Processes **exactly one** scheduled event, if the next one is at or
    /// before `max`, and returns the instant it fired at. `None` — with the
    /// clock advanced to `max`, exactly as [`run_until`](Self::run_until)
    /// leaves it — once nothing is due.
    ///
    /// Every state change in the simulation happens inside one of these steps:
    /// an engine only moves when it takes a frame, a tick, or an anchor
    /// round's command. So a test that samples between steps samples
    /// *everything* — no property can slip through the gap between two
    /// coarser samples, which is what lets a safety claim be stated as
    /// absolute rather than as "not observed at this cadence".
    ///
    /// # Panics
    /// Never in practice: it only pops an event it has just peeked, and the
    /// simulation is single-threaded.
    pub fn step_until(&mut self, max: Time) -> Option<Time> {
        match self.queue.peek() {
            Some(event) if event.at <= max => {}
            _ => {
                self.now = max;
                return None;
            }
        }
        let event = self.queue.pop().expect("peeked");
        self.now = event.at;
        let seq = event.seq;
        match event.kind {
            Kind::Deliver { to, from, wire } => self.deliver(&to, from, &wire, seq),
            Kind::Tick { node } => self.tick(&node),
            Kind::AnchorRound {
                node,
                epoch_hint,
                started_at,
            } => self.anchor_round(&node, epoch_hint, started_at),
            Kind::AnchorReadBack {
                node,
                attempted,
                started_at,
            } => self.anchor_read_back(&node, &attempted, started_at),
        }
        Some(self.now)
    }

    /// The `(epoch, claimant)` pair `node` has durably granted as a voter, as
    /// the sim's stand-in store recorded it from
    /// [`Effect::PersistGrant`].
    ///
    /// Survives [`crash`](Self::crash) — that is the whole point of it — so a
    /// restart test reads it here and hands it back through
    /// [`GroupEngine::with_recovered`](groupnet_core::GroupEngine::with_recovered).
    #[must_use]
    pub fn persisted_grant_of(&self, node: &NodeId) -> Option<(u64, NodeId)> {
        self.persisted_grants.get(node).cloned()
    }

    /// The coordinator a given node currently believes in.
    #[must_use]
    pub fn coordinator_of(&self, node: &NodeId) -> Option<NodeId> {
        self.engines
            .get(node)
            .and_then(|e| e.coordinator().cloned())
    }

    /// The `(epoch, host)` pair `node` has adopted, or `None` if it is not in
    /// the simulation. `(0, None)` for a node whose group is
    /// [`Eventual`](groupnet_core::GroupMode::Eventual) or which has not
    /// adopted anything yet.
    #[must_use]
    pub fn leadership_of(&self, node: &NodeId) -> Option<(u64, Option<NodeId>)> {
        self.engines.get(node).map(|e| {
            let (epoch, host) = e.leadership();
            (epoch, host.cloned())
        })
    }

    /// The election role `node` currently plays, or `None` if it is not in the
    /// simulation.
    #[must_use]
    pub fn role_of(&self, node: &NodeId) -> Option<Role> {
        self.engines.get(node).map(GroupEngine::role)
    }

    /// Every node that currently believes *itself* to be the host, in id
    /// order — the split-brain probe. More than one entry means two nodes hold
    /// the group at once, which `Settle` permits only inside the lease window;
    /// a test asserts on the count and on how long it lasts.
    #[must_use]
    pub fn hosts(&self) -> Vec<NodeId> {
        self.engines
            .iter()
            .filter(|(_, e)| e.role() == Role::Host)
            .map(|(node, _)| node.clone())
            .collect()
    }

    /// The highest epoch `node` has observed from any source, or `None` if it
    /// is not in the simulation.
    #[must_use]
    pub fn observed_epoch_of(&self, node: &NodeId) -> Option<u64> {
        self.engines.get(node).map(GroupEngine::observed_epoch)
    }

    /// When `node`'s hostship expires if it stops renewing — `None` unless it
    /// is in the simulation *and* currently a host. Read in exact virtual
    /// time, which is what lets a test check lease disjointness precisely.
    #[must_use]
    pub fn lease_until_of(&self, node: &NodeId) -> Option<Time> {
        self.engines
            .get(node)
            .and_then(GroupEngine::host_lease_until)
    }

    /// Applies a local command at `node` and schedules its effects.
    ///
    /// # Errors
    /// [`SimError::UnknownNode`] if `node` is not in the simulation (never
    /// added, or crashed); nothing is applied or scheduled.
    pub fn command(&mut self, node: &NodeId, cmd: Command) -> Result<(), SimError> {
        let engine = self
            .engines
            .get_mut(node)
            .ok_or_else(|| SimError::UnknownNode(node.clone()))?;
        let effects = engine.apply(cmd);
        self.dispatch(node, effects);
        Ok(())
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
    ///
    /// The node's **anchor driver dies with it**: the etag it was carrying is
    /// forgotten, exactly as a real process's would be. That is what makes a
    /// restart re-win the group through
    /// [`AnchorTerms::plan_claim`](groupnet_core::anchor::AnchorTerms::plan_claim) at a strictly higher
    /// epoch instead of resuming the epoch the record still names it at — the
    /// tier keeps no node-local storage, and neither does this simulation.
    ///
    /// # Errors
    /// [`SimError::UnknownNode`] if `node` is not in the simulation (never
    /// added, or already crashed); nothing changes.
    pub fn crash(&mut self, node: &NodeId) -> Result<(), SimError> {
        if self.engines.remove(node).is_none() {
            return Err(SimError::UnknownNode(node.clone()));
        }
        self.anchor.forget(node);
        Ok(())
    }

    /// The app-defined per-node state that `observer` currently holds for
    /// `node`.
    #[must_use]
    pub fn state_of(&self, observer: &NodeId, node: &NodeId) -> Option<Vec<u8>> {
        self.engines
            .get(observer)
            .and_then(|e| e.node_state(node).map(<[u8]>::to_vec))
    }

    /// When `observer` will expire its copy of `node`'s `key` — the
    /// lease-lapse instant, in exact virtual time, which is what lets a test
    /// check the coherence-lease tier's bound precisely (a writer waits out
    /// either an ack or this instant).
    ///
    /// `None` if `observer` is not in the simulation, or holds no lapsing copy
    /// of the key: absent, tombstoned, reaped, or authored without a TTL. The
    /// stamp is armed at **adoption** on `observer`'s own timeline, so two
    /// observers of the same write legitimately report different instants —
    /// see
    /// [`GroupEngine::node_entry_expires_at`](groupnet_core::GroupEngine::node_entry_expires_at).
    #[must_use]
    pub fn entry_expires_at_of(&self, observer: &NodeId, node: &NodeId, key: &str) -> Option<Time> {
        self.engines
            .get(observer)
            .and_then(|e| e.node_entry_expires_at(node, key))
    }

    /// The ids of every engine currently in the simulation.
    #[must_use]
    pub fn nodes(&self) -> Vec<NodeId> {
        self.engines.keys().cloned().collect()
    }

    /// `observer`'s full metadata view, or `None` if `observer` is not in the
    /// simulation.
    #[must_use]
    pub fn metadata_snapshot(&self, observer: &NodeId) -> Option<BTreeMap<String, String>> {
        self.engines.get(observer).map(|e| {
            e.metadata_iter()
                .map(|(k, v)| (k.to_owned(), v.to_owned()))
                .collect()
        })
    }

    /// `observer`'s full per-node app-state view, or `None` if `observer` is
    /// not in the simulation.
    #[must_use]
    pub fn state_snapshot(&self, observer: &NodeId) -> Option<BTreeMap<NodeId, Vec<u8>>> {
        self.engines.get(observer).map(|e| {
            e.node_states_iter()
                .map(|(n, s)| (n.clone(), s.to_vec()))
                .collect()
        })
    }

    /// `observer`'s full per-node **keyed-entry** view (live entries only —
    /// tombstoned/expired keys are absent), so a test can assert every node
    /// converges on every node's whole keyed map, not just the `~blob` shim.
    /// `None` if `observer` is not in the simulation.
    #[must_use]
    pub fn entries_snapshot(
        &self,
        observer: &NodeId,
    ) -> Option<BTreeMap<NodeId, BTreeMap<String, Vec<u8>>>> {
        self.engines.get(observer).map(|e| {
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
                Effect::Send { to, wire } => self.send(node, to, wire),
                Effect::ArmTimer { at } => {
                    // `at` is absolute; never schedule in the past.
                    let at = at.max(self.now);
                    self.schedule(at, Kind::Tick { node: node.clone() });
                }
                Effect::CoordinatorChanged { coordinator } => {
                    self.coordinator_log.push((node.clone(), coordinator));
                }
                Effect::LeadershipChanged { epoch, host } => {
                    self.leadership_log.push((node.clone(), epoch, host));
                }
                Effect::PersistGrant { epoch, claimant } => {
                    // The store a durable voter would have. Written before the
                    // grant frame is dispatched, because the effect precedes
                    // the `Send` in the batch — which is the whole contract.
                    self.persisted_grants
                        .insert(node.clone(), (epoch, claimant));
                }
                Effect::AnchorClaimDue { epoch_hint } => self.prompt_anchor(node, epoch_hint),
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
