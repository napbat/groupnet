//! The deterministic event loop: virtual clock, lossy/partitionable
//! in-memory network, and the [`Simulation`] driver over real engines.

use crate::anchor::{AnchorEvent, AnchorModel, RoundReport};
use crate::rng::SplitMix64;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};

use groupnet_core::anchor::AnchorRecord;
use groupnet_core::{Command, Effect, GroupEngine, NodeId, Role, Status, Time, wire};

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

/// Whether an encoded frame's kind tag names an election frame — wire kinds
/// `8..=10` (`LeadClaim`, `LeadGrant`, `LeadState`). Read straight off the
/// byte after the version, the same cheap peek `wire::peek_group` does, so
/// counting costs nothing on the delivery path.
fn is_election_frame(wire: &[u8]) -> bool {
    matches!(wire.get(1).copied(), Some(8..=10))
}

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

    // -- The external CAS anchor -------------------------------------------
    //
    // Everything below drives `Activation::External`. A simulation that never
    // calls `enable_anchor` has no anchor, so `Effect::AnchorClaimDue` is
    // dropped and no group ever activates a host — the same fail-safe posture a
    // driver with no anchor configured has, and the reason every `Settle` and
    // `Quorum` suite in this crate is unaffected by any of it.

    /// Arms the external anchor for this simulation.
    ///
    /// `ttl_ms` is the anchor record's TTL and `steal_margin_ms` the steal
    /// margin — respectively
    /// [`HostedConfig::lease_ms`](groupnet_core::HostedConfig::lease_ms) and
    /// [`Activation::External`](groupnet_core::Activation::External)'s field,
    /// which is the one configuration the tier has. Pass anything else and you
    /// are simulating a deployment that cannot be configured.
    pub fn enable_anchor(&mut self, ttl_ms: u64, steal_margin_ms: u64) {
        self.anchor.enable(ttl_ms, steal_margin_ms);
    }

    /// How long a store round trip takes, in virtual milliseconds: prompt to
    /// write, and (for an ambiguous write) write to read-back. A knob of its
    /// own because it is not a fabric latency — the anchor is not on the
    /// cluster's network, and a deployment where the store is far and the peers
    /// are near is the normal one.
    pub fn set_anchor_latency(&mut self, ms: u64) {
        self.anchor.set_latency(ms);
    }

    /// This node's wall-clock offset from virtual time, in milliseconds, which
    /// may be negative. Zero for every node unless set.
    ///
    /// This is the *only* clock the External tier consults, and it consults it
    /// in exactly one place ([`AnchorRecord::stealable`]). An offset here is
    /// therefore the complete model of the assumption `Activation::External`
    /// states — and of violating it.
    ///
    /// [`AnchorRecord::stealable`]: groupnet_core::anchor::AnchorRecord::stealable
    pub fn set_anchor_skew(&mut self, node: &NodeId, ms: i64) {
        self.anchor.set_skew(node, ms);
    }

    /// Cuts `node` off from the anchor. **Orthogonal to
    /// [`block`](Self::block)**: this is the availability axis under
    /// `External`, and the CP inversion is only testable because the two are
    /// separate. A node blocked here keeps gossiping, keeps its rank and keeps
    /// its peers — and cannot renew, so its lease lapses and it demotes.
    pub fn block_anchor(&mut self, node: &NodeId) {
        self.anchor.block(node);
    }

    /// Restores `node`'s access to the anchor.
    pub fn heal_anchor(&mut self, node: &NodeId) {
        self.anchor.heal(node);
    }

    /// Restores every node's access to the anchor.
    pub fn heal_anchor_all(&mut self) {
        self.anchor.heal_all();
    }

    /// What share of conditional writes **apply and report `Unknown`**
    /// (0..=100) — the timed-out `PUT` every object store can produce. The
    /// driver resolves each one by reading the record back an anchor latency
    /// later and judging it with
    /// [`ambiguous_applied`](groupnet_core::anchor::ambiguous_applied).
    ///
    /// Deterministic: the schedule is keyed on the round counter through the
    /// same [`SplitMix64`] the loss schedule uses, so a
    /// failing seed replays exactly.
    pub fn set_anchor_unknown_percent(&mut self, percent: u8) {
        self.anchor.set_unknown_percent(percent);
    }

    /// What share of conditional writes report `Unknown` **without applying**
    /// (0..=100) — the other half of the same timeout, and the shape a
    /// write-throttled, read-only or expired-credential store produces
    /// indefinitely rather than transiently.
    ///
    /// It is the half a **renewal** makes dangerous: an attempted renewal's
    /// `(epoch, host)` is identical to the record it means to replace, so a
    /// read-back that judged the pair alone would call every failed renewal a
    /// win and extend a lease off a record that is quietly ageing out.
    /// [`ambiguous_applied`](groupnet_core::anchor::ambiguous_applied) compares
    /// the whole record, which is what makes this knob resolve as *lost*.
    ///
    /// Independent of [`set_anchor_unknown_percent`](Self::set_anchor_unknown_percent)
    /// — a separate deterministic draw on the same round counter — and decided
    /// first, so a write it takes never reaches the register.
    pub fn set_anchor_unknown_lost_percent(&mut self, percent: u8) {
        self.anchor.set_unknown_lost_percent(percent);
    }

    /// Writes a record into the anchor directly, as if some earlier incarnation
    /// of the cluster — or a node that is not in this simulation at all — had
    /// won it. The etag moves, so nobody holds one for it.
    pub fn seed_anchor(&mut self, record: AnchorRecord) {
        self.anchor.seed(record);
    }

    /// The record the anchor currently holds — the ground truth every observer
    /// is converging on, readable without asking any node.
    #[must_use]
    pub fn anchor_record(&self) -> Option<AnchorRecord> {
        self.anchor.record()
    }

    /// How many conditional writes reported `Unknown` and had to be resolved by
    /// a read-back. The floor that keeps an ambiguity schedule from passing
    /// vacuously.
    #[must_use]
    pub fn anchor_unknown_rounds(&self) -> u64 {
        self.anchor.unknown_rounds()
    }

    /// How many of those never applied — the half of
    /// [`anchor_unknown_rounds`](Self::anchor_unknown_rounds) a read-back has
    /// to resolve as *lost*, and the floor that keeps a
    /// [`set_anchor_unknown_lost_percent`](Self::set_anchor_unknown_lost_percent)
    /// schedule from passing vacuously.
    #[must_use]
    pub fn anchor_unknown_lost_rounds(&self) -> u64 {
        self.anchor.unknown_lost_rounds()
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
            Kind::Tick { node } => {
                let now = self.now;
                if let Some(engine) = self.engines.get_mut(&node) {
                    let effects = engine.on_tick(now);
                    self.dispatch(&node, effects);
                }
            }
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

    /// One frame arriving, subject to the loss schedule and the partition set.
    fn deliver(&mut self, to: &NodeId, from: NodeId, wire: &[u8], seq: u64) {
        if self.loss_percent != 0 && SplitMix64::hash(seq) % 100 < u64::from(self.loss_percent) {
            return; // deterministic per-message drop
        }
        if self.blocked.contains(&(from.clone(), to.clone())) {
            return; // partitioned link
        }
        let now = self.now;
        let effects = match self.engines.get_mut(to) {
            Some(engine) => engine.on_message(from, wire, now),
            None => return, // a crashed node: nothing consumed it
        };
        // Counted only once an engine has actually taken it — the probe is
        // "delivered to an engine", not "scheduled and not dropped", so a frame
        // still in flight to a node that has since crashed is not election
        // traffic anyone observed.
        if is_election_frame(wire) {
            self.election_frames += 1;
        }
        self.dispatch(to, effects);
    }

    /// How many election frames (wire kinds `LeadClaim`, `LeadGrant`,
    /// `LeadState`) the sim has actually delivered to an engine — frames lost
    /// or blocked by a partition are not counted.
    ///
    /// This is the mode-invariance probe: a run of purely
    /// [`GroupMode::Eventual`](groupnet_core::GroupMode) groups must end with
    /// zero, proving the metadata-only contract really does cost no election
    /// traffic.
    #[must_use]
    pub fn election_frames_seen(&self) -> u64 {
        self.election_frames
    }

    /// How many `LeadClaim` frames the run has **issued** — counted at
    /// dispatch, off the decoded frame.
    ///
    /// Issuance rather than delivery, unlike
    /// [`election_frames_seen`](Self::election_frames_seen), and deliberately:
    /// this is the **X-purity** probe, and the claim it pins is that an
    /// [`Activation::External`](groupnet_core::Activation::External) group
    /// never *builds* a bid. A frame that was built and then lost, dropped by
    /// the loss schedule or blocked by a partition would satisfy a delivery
    /// counter while violating the property.
    #[must_use]
    pub fn claim_frames_seen(&self) -> u64 {
        self.claim_frames
    }

    /// How many `LeadGrant` frames the run has **issued** — the other half of
    /// the X-purity probe. See [`claim_frames_seen`](Self::claim_frames_seen)
    /// for why both are counted at issuance.
    #[must_use]
    pub fn grant_frames_seen(&self) -> u64 {
        self.grant_frames
    }

    /// The `(epoch, claimant)` pair `node` has durably granted as a voter, as
    /// the sim's stand-in store recorded it from
    /// [`Effect::PersistGrant`](groupnet_core::Effect::PersistGrant).
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
    ///
    /// The node's **anchor driver dies with it**: the etag it was carrying is
    /// forgotten, exactly as a real process's would be. That is what makes a
    /// restart re-win the group through
    /// [`plan_claim`](groupnet_core::anchor::plan_claim) at a strictly higher
    /// epoch instead of resuming the epoch the record still names it at — the
    /// tier keeps no node-local storage, and neither does this simulation.
    pub fn crash(&mut self, node: &NodeId) {
        self.engines.remove(node);
        self.anchor.forget(node);
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

    /// The status `observer` holds for `node` **and the virtual instant it has
    /// held it continuously since** — the fencing-verdict roster, readable in
    /// exact virtual time.
    ///
    /// `None` once the observer has reaped the member's tombstone
    /// (`2×dead_timeout` after death), or if it never knew the node.
    #[must_use]
    pub fn status_since_of(&self, observer: &NodeId, node: &NodeId) -> Option<(Status, Time)> {
        self.engines
            .get(observer)
            .and_then(|e| e.member_status_since(node))
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
                    self.note_lead_frame(node, &wire);
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
                // The External tier's prompt to a driver: run an anchor round
                // now. It is a *level* signal on the anti-entropy cadence, so
                // the model debounces it against this node's own in-flight
                // round exactly as the effect's contract requires a driver to
                // — without that, a store round trip longer than one
                // anti-entropy interval would stack rounds and burn epochs.
                Effect::AnchorClaimDue { epoch_hint } => {
                    if self.anchor.accept_prompt(node) {
                        let at = self.now.saturating_add(self.anchor.latency_ms());
                        self.schedule(
                            at,
                            Kind::AnchorRound {
                                node: node.clone(),
                                epoch_hint,
                                started_at: self.now,
                            },
                        );
                    }
                }
                Effect::MembershipChanged
                | Effect::NodeStateChanged { .. }
                | Effect::MetadataChanged { .. } => {}
            }
        }
    }

    /// One anchor round: what the driver would do between the prompt and the
    /// command it reports back.
    ///
    /// Two ways a round simply does not happen, and both are load-bearing:
    ///
    /// * **The process crashed.** Nothing performs the round, and the record it
    ///   holds ages out on its own.
    /// * **The node cannot reach the anchor.** Also nothing — and *this* is the
    ///   fail-closed posture the tier is chosen for: the engine is untouched,
    ///   keeps its rank, keeps its peers, and demotes anyway when row 6 finds
    ///   the lease lapsed. A node never hosts on its own say-so here.
    fn anchor_round(&mut self, node: &NodeId, epoch_hint: u64, started_at: Time) {
        if !self.engines.contains_key(node) || self.anchor.is_blocked(node) {
            self.anchor.finish(node);
            return;
        }
        // Renew or claim is the engine's answer, not the model's: the driver
        // renews only while the node it is driving still believes it hosts the
        // epoch its etag was won for.
        let hosting = self
            .engines
            .get(node)
            .filter(|e| e.role() == Role::Host)
            .map(|e| e.leadership().0);
        let (event, report) = self.anchor.round(node, epoch_hint, self.now, hosting);
        self.anchor_log.push((self.now, node.clone(), event));
        self.report_anchor(node, report, started_at);
    }

    /// The read-back an ambiguous write is resolved by. A node that crashed or
    /// lost the anchor in the meantime never learns: its etag stays dropped and
    /// its next round re-plans from whatever the record shows, which is the
    /// conservative direction.
    fn anchor_read_back(&mut self, node: &NodeId, attempted: &AnchorRecord, started_at: Time) {
        if !self.engines.contains_key(node) || self.anchor.is_blocked(node) {
            self.anchor.finish(node);
            return;
        }
        let report = self.anchor.read_back(node, attempted);
        self.report_anchor(node, report, started_at);
    }

    /// Hands a finished round's verdict to the engine as the command a real
    /// driver would send, and releases the round's debounce.
    ///
    /// `lease_until` is `started_at + ttl`, anchored at the instant the round
    /// **began** rather than the instant the write landed — the rule
    /// [`Command::AnchorActivated`] states. The record's own
    /// `expires_at_wall_ms` was stamped one anchor latency later, so the engine
    /// lease always runs out first and a host never overhangs the record it is
    /// hosting on.
    fn report_anchor(&mut self, node: &NodeId, report: RoundReport, started_at: Time) {
        match report {
            RoundReport::Won { epoch } => {
                self.anchor.finish(node);
                let lease_until = started_at.saturating_add(self.anchor.ttl_ms());
                self.command(node, Command::AnchorActivated { epoch, lease_until });
            }
            RoundReport::Observed { epoch, host } => {
                self.anchor.finish(node);
                self.command(node, Command::AnchorObserved { epoch, host });
            }
            RoundReport::Silent => self.anchor.finish(node),
            // Still in flight: an ambiguous write is not over until the
            // read-back, so the debounce deliberately stays held.
            RoundReport::Ambiguous { attempted } => {
                let at = self.now.saturating_add(self.anchor.latency_ms());
                self.schedule(
                    at,
                    Kind::AnchorReadBack {
                        node: node.clone(),
                        attempted,
                        started_at,
                    },
                );
            }
        }
    }

    /// Counts and logs one dispatched election frame, reading its body off the
    /// encoded bytes rather than off the effect: issuance is a wire fact.
    ///
    /// Every dispatched frame is decoded to find out — the codec is the only
    /// authority on what a frame is, and a byte-offset pre-filter would be a
    /// second, silently drifting copy of the wire format. The cost is test-only
    /// and lands on a path that has already allocated the encoded frame. The
    /// one decode feeds three probes: [`grant_log`](Self::grant_log),
    /// [`claim_frames_seen`](Self::claim_frames_seen) and
    /// [`grant_frames_seen`](Self::grant_frames_seen).
    fn note_lead_frame(&mut self, from: &NodeId, wire: &[u8]) {
        let Some(frame) = wire::decode(wire) else {
            return;
        };
        match frame.lead {
            Some(wire::LeadBody::Claim { .. }) => self.claim_frames += 1,
            Some(wire::LeadBody::Grant {
                epoch, claimant, ..
            }) => {
                self.grant_frames += 1;
                self.grant_log
                    .push((self.now, from.clone(), epoch, claimant));
            }
            Some(wire::LeadBody::State { .. }) | None => {}
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
