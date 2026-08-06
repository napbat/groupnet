//! Deterministic Simulation Testing for `Activation::External` — the tier that
//! closes an epoch at a **linearizable CAS register outside the cluster**, over
//! a real (lossy, partitionable) network and against a real anchor model.
//!
//! The simulator owns one register per run: an `Option<AnchorRecord>` plus an
//! etag counter, written only by rounds that are scheduled like every other
//! event. Every *verdict* in those rounds is core's — `plan_claim`,
//! `renewal_record`, `ambiguous_applied` — so a counterexample here is a
//! counterexample against the shipped decision rules, not against a paraphrase
//! of them.
//!
//! # The suites, and exactly what each one is allowed to claim
//!
//! * Three deterministic pins first: the happy path (elect, renew, converge); a
//!   **pre-seeded record naming a node that is not in the cluster** — nobody
//!   activates until its expiry plus the steal margin, and the steal is at
//!   exactly one epoch above it; and one long schedule carrying every
//!   transition the tier has, for the purity claim to be made over.
//! * **X-S1 — [`dst_external_epoch_is_globally_unique_without_storage`].** The
//!   headline. 128 seeds of the full fault menu — crashes, *amnesiac* restarts,
//!   partitions, heals, loss, reorder, anchor outages, clock skew — and **no
//!   epoch is ever activated by two nodes**, across the whole run and both
//!   sides of every partition. No `with_recovered`, no persisted anything: the
//!   property is unconditional because the anchor allocates the epoch, so there
//!   is nothing for a restart to forget. That is strictly stronger than
//!   `Quorum`'s S1-strict, which is storage-conditional (see
//!   `election_quorum.rs`'s matrix), and it is the whole reason this tier
//!   exists.
//! * **X-purity — [`assert_pure`], asserted at the end of *every* run in this
//!   file** and in the two sibling files. Zero `LeadClaim` frames, zero
//!   `LeadGrant` frames, zero `PersistGrant` effects, over every schedule.
//!   Counted at **issuance**, so a bid that was built and then lost would still
//!   fail it.
//! * **X-S2 and S4b**, sampled every round mid-chaos: an observer's epochs
//!   never run backwards, and a node playing `Role::Host` is always inside its
//!   lease.
//!
//! What this file deliberately does *not* assert is cluster-wide lease
//! disjointness. That property is real, but it is the only one in the tier that
//! consults a clock, so it is stated — and its documented failure pinned — in
//! `election_external_skew.rs` where the clocks are controlled. The chaos here
//! runs with arbitrary skew precisely to show X-S1 does not depend on one.
//!
//! The shaped failover, partition-irrelevance and fail-closed scenarios live in
//! `election_external_failover.rs`, the same way `election_quorum_failover.rs`
//! is split out of `election_quorum.rs`.
//!
//! A failing seed is a reproducible counterexample, not a flake.

use std::collections::{BTreeMap, BTreeSet};

use groupnet_core::anchor::AnchorRecord;
use groupnet_core::{
    Activation, Command, Config, GroupEngine, GroupId, GroupMode, HostedConfig, NodeId, Role, Time,
    placement,
};
use groupnet_sim::{AnchorEvent, Simulation, SplitMix64};

/// How long a host's authority survives its last successful anchor round —
/// which is also the anchor record's TTL and the boot guard. One number, as
/// `HostedConfig::lease_ms` documents: the record and the engine lease describe
/// the same authority from the two clocks.
const LEASE_MS: u64 = 400;
/// The anti-entropy cadence, which is also the cadence the anchor prompt rides.
const GOSSIP_INTERVAL_MS: u64 = 60;
/// How far past a record's expiry a claimant must wait before it may steal.
const STEAL_MARGIN_MS: u64 = 150;
/// One store round trip. Deliberately not a link latency: the anchor is not on
/// the cluster's network.
const ANCHOR_LATENCY_MS: u64 = 15;

/// The same detector timings the Settle and Quorum suites run on, with an
/// External activation in place of the settle window or the roster.
fn cfg() -> Config {
    Config {
        gossip_interval_ms: GOSSIP_INTERVAL_MS,
        probe_interval_ms: 50,
        probe_timeout_ms: 40,
        suspect_timeout_ms: 120,
        dead_timeout_ms: 1_000,
        indirect_probes: 2,
        fanout: 4,
        anti_entropy_interval_ms: GOSSIP_INTERVAL_MS,
        anti_entropy_fanout: 2,
        eager_push: true,
        full_digest_every: 4,
        max_delta_frame_bytes: 4_096,
        mode: GroupMode::Hosted(HostedConfig {
            activation: Activation::External {
                steal_margin_ms: STEAL_MARGIN_MS,
            },
            lease_ms: LEASE_MS,
        }),
    }
}

fn nodes(ids: &[&str]) -> BTreeSet<NodeId> {
    ids.iter().map(|id| NodeId::new(*id)).collect()
}

/// A cluster of `members` bootstrapped all-to-all, with the anchor armed at the
/// group's own configuration.
fn cluster(group: &str, members: &BTreeSet<NodeId>) -> Simulation {
    let group = GroupId::new(group);
    let mut sim = Simulation::new(10);
    sim.enable_anchor(LEASE_MS, STEAL_MARGIN_MS);
    sim.set_anchor_latency(ANCHOR_LATENCY_MS);
    for id in members {
        let seeds = members.iter().filter(|x| *x != id).cloned();
        sim.add(GroupEngine::new(group.clone(), id.clone(), seeds, cfg()));
    }
    sim
}

/// An `(epoch, host)` pair: the unit that names a serializer.
type Pair = (u64, Option<NodeId>);

/// The pair `node` has adopted. Every caller here reads a node it knows is in
/// the simulation.
fn pair_of(sim: &Simulation, node: &NodeId) -> Pair {
    sim.leadership_of(node)
        .expect("a live node is in the simulation")
}

/// The single node that believes it holds the group, asserting there is exactly
/// one.
fn sole_host(sim: &Simulation, label: &str) -> NodeId {
    let hosts = sim.hosts();
    assert_eq!(
        hosts.len(),
        1,
        "{label}: expected exactly one host: {hosts:?}"
    );
    hosts.into_iter().next().expect("length asserted above")
}

/// The pair the anchor itself holds — ground truth, readable without asking any
/// node.
fn anchor_pair(sim: &Simulation, label: &str) -> (u64, NodeId) {
    let record = sim
        .anchor_record()
        .unwrap_or_else(|| panic!("{label}: the anchor is still empty"));
    (record.epoch, record.host)
}

/// **X-purity.** An `External` group has no bid to stand and no endorsement to
/// collect, so over any schedule whatsoever it builds **zero** `LeadClaim` and
/// `LeadGrant` frames and asks for **zero** grant persists. Its only election
/// frame is `LeadState`.
///
/// Counted at issuance rather than delivery: a bid that was built and then lost
/// to the loss schedule, or blocked by a partition, would satisfy a delivery
/// counter while violating the property this pins.
///
/// The `PersistGrant` half is the storage claim in its falsifiable form. There
/// is no `GrantStore` analogue under `External` and none is coming — the anchor
/// *is* the ledger — so a run that asked for a persist would mean the tier had
/// quietly grown a durability requirement.
fn assert_pure(sim: &Simulation, all: &BTreeSet<NodeId>, label: &str) {
    assert_eq!(
        sim.claim_frames_seen(),
        0,
        "{label}: an External group put a claim on the wire"
    );
    assert_eq!(
        sim.grant_frames_seen(),
        0,
        "{label}: an External group put a grant on the wire"
    );
    assert!(
        sim.grant_log.is_empty(),
        "{label}: an External group issued grants: {:?}",
        sim.grant_log
    );
    for node in all {
        assert_eq!(
            sim.persisted_grant_of(node),
            None,
            "{label}: {node} persisted a grant under External"
        );
    }
}

/// **X-S1.** Across the whole run, no two nodes ever activated the same epoch.
/// An activation is the one leadership transition a node logs naming *itself*.
///
/// Unconditional here, and that is the point: the anchor allocated every epoch,
/// so a restart has nothing to forget and no storage stands behind the claim.
fn assert_sole_activator_per_epoch(log: &[(NodeId, u64, Option<NodeId>)], label: &str) {
    let mut by_epoch: BTreeMap<u64, NodeId> = BTreeMap::new();
    for (observer, epoch, host) in log {
        if host.as_ref() != Some(observer) {
            continue; // not an activation — an adoption or a step down
        }
        if let Some(first) = by_epoch.insert(*epoch, observer.clone()) {
            assert_eq!(
                &first, observer,
                "{label}: epoch {epoch} was activated by both {first} and {observer}"
            );
        }
    }
}

/// How many times each kind of round the run performed — the floors, and the
/// shape a drifted schedule reports about itself.
fn tally(sim: &Simulation) -> BTreeMap<AnchorEvent, u64> {
    let mut counts = BTreeMap::new();
    for (_, _, event) in &sim.anchor_log {
        *counts.entry(*event).or_default() += 1;
    }
    counts
}

fn count(counts: &BTreeMap<AnchorEvent, u64>, what: AnchorEvent) -> u64 {
    counts.get(&what).copied().unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Pins.
// ---------------------------------------------------------------------------

/// The happy path, end to end: the top-ranked node wins the register, keeps it
/// by renewing on the anti-entropy cadence, and every node converges on the
/// pair the register holds.
///
/// Four things are asserted that only a real anchor can show:
///
/// * the epoch came from the **register**, not from a settle window — the
///   record's `(epoch, host)` is exactly the pair the cluster adopted;
/// * the record was created **once** and renewed many times, so a renewal
///   really does allocate nothing;
/// * the host's engine lease is still ahead of the clock 4 seconds in, which
///   under `External` is only possible by winning rounds (row 5's re-rank
///   renewal is gated to `Settle`);
/// * X-purity, over the whole run.
#[test]
fn an_external_group_elects_the_top_ranked_node_and_keeps_renewing_it() {
    let members = nodes(&["n1", "n2", "n3"]);
    let mut sim = cluster("x-happy", &members);
    sim.run_until(Time(4_000));
    let label = "X-happy";

    let host = sole_host(&sim, label);
    assert_eq!(
        host,
        placement::owner("x-happy", &members).expect("a non-empty cluster"),
        "the anchor does not change who is a *candidate* — rank still decides that"
    );
    let (epoch, holder) = anchor_pair(&sim, label);
    assert_eq!(
        holder, host,
        "the register and the cluster name the same node"
    );
    for node in &members {
        assert_eq!(
            pair_of(&sim, node),
            (epoch, Some(host.clone())),
            "{node} has not converged on the register's pair"
        );
    }

    let lease = sim.lease_until_of(&host).expect("the host holds a lease");
    assert!(
        lease > Time(4_000),
        "the host's lease lapsed rather than being renewed by an anchor round: {lease:?}"
    );

    let counts = tally(&sim);
    assert_eq!(
        count(&counts, AnchorEvent::Create),
        1,
        "the register is created exactly once: {counts:?}"
    );
    assert_eq!(
        count(&counts, AnchorEvent::Steal),
        0,
        "nothing was stolen on a healthy run: {counts:?}"
    );
    assert!(
        count(&counts, AnchorEvent::Renew) > 10,
        "a host four seconds in has renewed many times over: {counts:?}"
    );
    assert!(
        sim.anchor_log.iter().all(|(_, node, _)| *node == host),
        "a node that is not top-ranked ran an anchor round: {:?}",
        sim.anchor_log
    );
    assert!(
        sim.election_frames_seen() > 0,
        "the group ran an election without a single LeadState"
    );
    assert_pure(&sim, &members, label);
}

/// A record left behind by a node that is **not in this cluster at all** — a
/// previous incarnation, a member that has not booted, an operator's fixture.
///
/// Three things follow, and the third is the tier's succession rule in exact
/// virtual time:
///
/// * the group does **not** elect over it. The pair `(5, outsider)` outranks
///   `(0, None)`, so the top-ranked node adopts it — *through the anchor*,
///   having heard no gossip about it from anyone.
/// * **nobody activates before `expires_at_wall_ms + steal_margin_ms`.**
///   Sampled every millisecond across the boundary, so an activation one tick
///   early would be caught.
/// * the steal is at **exactly one epoch above** the record it superseded, and
///   the cluster converges on the new pair.
///
/// The followers' silence is the documented shape, asserted rather than
/// tolerated: adoption is observer-local and only a *host* beacons a pair, so
/// `n2` and `n3` learn nothing until the steal broadcasts `(6, n1)`. Under this
/// tier that costs nothing — the anchor, not the fabric, is what decides.
#[test]
fn a_record_naming_an_outsider_is_adopted_and_stolen_only_at_expiry_plus_margin() {
    let members = nodes(&["n1", "n2", "n3"]);
    let group = "x-outsider";
    let top = placement::owner(group, &members).expect("a non-empty cluster");
    let mut sim = cluster(group, &members);
    let expires_at = 2_000;
    sim.seed_anchor(AnchorRecord {
        epoch: 5,
        host: NodeId::new("outsider"),
        expires_at_wall_ms: expires_at,
    });

    // Before the boundary: adopted, never activated.
    sim.run_until(Time(1_500));
    assert_eq!(
        pair_of(&sim, &top),
        (5, Some(NodeId::new("outsider"))),
        "the top-ranked node did not adopt the record it read"
    );
    for other in members.iter().filter(|n| **n != top) {
        assert_eq!(
            pair_of(&sim, other),
            (0, None),
            "{other} learned a pair nobody ever gossiped it"
        );
    }

    let entitled_at = expires_at + STEAL_MARGIN_MS;
    for at in (1_500..=entitled_at).step_by(1) {
        sim.run_until(Time(at));
        assert!(
            sim.hosts().is_empty(),
            "somebody activated at {at}, before the record was stealable at {entitled_at}"
        );
    }
    assert!(
        count(&tally(&sim), AnchorEvent::Yield) > 0,
        "the top-ranked node never even asked the anchor"
    );

    // And then, within one prompt cadence plus a round trip, it takes it.
    sim.run_until(Time(entitled_at + GOSSIP_INTERVAL_MS + ANCHOR_LATENCY_MS));
    let host = sole_host(&sim, "X-outsider");
    assert_eq!(host, top);
    assert_eq!(
        anchor_pair(&sim, "X-outsider"),
        (6, top.clone()),
        "a steal allocates exactly one epoch above the record it superseded"
    );

    sim.run_until(Time(entitled_at + 2_000));
    for node in &members {
        assert_eq!(
            pair_of(&sim, node),
            (6, Some(top.clone())),
            "{node} did not converge on the stolen pair"
        );
    }
    assert_eq!(count(&tally(&sim), AnchorEvent::Steal), 1);
    assert_pure(&sim, &members, "X-outsider");
}

/// The whole transition alphabet in one schedule — elect, renew, crash the
/// host, let a successor steal, restart the dead node, partition, heal — with
/// the purity claim over all of it and `Role::Claimant` never entered.
///
/// The complement of the purity assertion matters as much as the assertion:
/// `LeadState` traffic is *expected* and asserted non-zero. The tier does not
/// buy its purity by going quiet; it buys it by having nothing to bid.
#[test]
fn a_long_external_run_never_bids_never_grants_and_never_persists() {
    let members = nodes(&["n1", "n2", "n3", "n4"]);
    let group = "x-pure";
    let mut sim = cluster(group, &members);
    sim.run_until(Time(3_000));
    let first = sole_host(&sim, "X-pure");

    sim.crash(&first);
    sim.run_until(Time(6_000));
    let second = sole_host(&sim, "X-pure (after the crash)");
    assert_ne!(second, first);

    // The dead node comes back with no memory of anything — including the
    // record that still carries its name in the log.
    let seeds = members.iter().filter(|x| **x != first).cloned();
    sim.add(GroupEngine::new(
        GroupId::new(group),
        first.clone(),
        seeds,
        cfg(),
    ));
    sim.run_until(Time(9_000));

    for node in members.iter().filter(|n| **n != second) {
        sim.block(&second, node);
        sim.block(node, &second);
    }
    sim.run_until(Time(13_000));
    sim.heal_all();
    sim.run_until(Time(20_000));

    for node in &members {
        assert_ne!(
            sim.role_of(node),
            Some(Role::Claimant),
            "{node} entered Role::Claimant under External"
        );
    }
    assert!(sim.election_frames_seen() > 0, "the run went silent");
    assert_pure(&sim, &members, "X-pure");
    assert_sole_activator_per_epoch(&sim.leadership_log, "X-pure");
}

// ---------------------------------------------------------------------------
// Shared DST scaffolding.
// ---------------------------------------------------------------------------

/// Seeds the shared deterministic PRNG so each schedule is reproducible. Each
/// suite salts the seed so they explore independent streams.
fn rng(seed: u64) -> SplitMix64 {
    SplitMix64::new(seed ^ 0x9e37_79b9_7f4a_7c15)
}

fn pick(set: &BTreeSet<NodeId>, rng: &mut SplitMix64) -> NodeId {
    let v: Vec<&NodeId> = set.iter().collect();
    let n = u32::try_from(v.len()).expect("these clusters are a handful of nodes");
    let i = usize::try_from(rng.below(n)).expect("bounded by the set size");
    v[i].clone()
}

/// One seed's cluster. No roster, no store, no recovery constructor — the whole
/// topology of this tier is "who booted", which is itself the point.
#[derive(Debug)]
struct Topology {
    group: GroupId,
    booted: BTreeSet<NodeId>,
}

impl Topology {
    fn engine(&self, id: &NodeId, peers: &BTreeSet<NodeId>) -> GroupEngine {
        let seeds = peers.iter().filter(|x| *x != id).cloned();
        GroupEngine::new(self.group.clone(), id.clone(), seeds, cfg())
    }

    fn boot(&self, sim: &mut Simulation) {
        for id in &self.booted {
            sim.add(self.engine(id, &self.booted));
        }
    }
}

/// Draws a cluster of 3..=6 nodes.
fn draw_topology(rng: &mut SplitMix64, name: &str) -> Topology {
    let n = 3 + usize::try_from(rng.below(4)).expect("a 0..4 draw");
    let booted: BTreeSet<NodeId> = (0..n).map(|i| NodeId::new(format!("n{i}"))).collect();
    Topology {
        group: GroupId::new(name),
        booted,
    }
}

/// What one observer was last seen believing — the history X-S2 is read
/// against.
#[derive(Debug, Default)]
struct Beliefs {
    observed: BTreeMap<NodeId, u64>,
    adopted: BTreeMap<NodeId, u64>,
}

impl Beliefs {
    /// A restarted node is a *new* observer on a fresh timeline: its election
    /// state was in memory only, so it legitimately comes back a Follower at
    /// epoch 0 and nothing recorded about it applies. Under this tier that is
    /// not even a weakening — the anchor still refuses to award it a spent
    /// epoch.
    fn forget(&mut self, node: &NodeId) {
        self.observed.remove(node);
        self.adopted.remove(node);
    }
}

/// Applies one fault. Returns the node id if the fault was a **restart**, whose
/// fresh engine invalidates everything recorded about that observer.
fn inject_fault(
    sim: &mut Simulation,
    rng: &mut SplitMix64,
    topo: &Topology,
    alive: &mut BTreeSet<NodeId>,
    now: u64,
) -> Option<NodeId> {
    match rng.below(11) {
        0 if alive.len() > 2 => {
            let victim = pick(alive, rng);
            sim.crash(&victim);
            alive.remove(&victim);
        }
        1 if alive.len() < topo.booted.len() => {
            // A restart is a *fresh* engine and a *fresh driver*: incarnation 0,
            // epoch 0, no etag, and nothing persisted anywhere. The anchor is
            // the only memory in the system.
            let down: BTreeSet<NodeId> = topo
                .booted
                .iter()
                .filter(|x| !alive.contains(*x))
                .cloned()
                .collect();
            let node = pick(&down, rng);
            alive.insert(node.clone());
            sim.add(topo.engine(&node, alive));
            return Some(node);
        }
        2 if alive.len() > 1 => {
            let a = pick(alive, rng);
            let b = pick(alive, rng);
            if a != b {
                sim.block(&a, &b);
                sim.block(&b, &a);
            }
        }
        3 => sim.heal_all(),
        // The availability axis of this tier, and the one the fabric knows
        // nothing about: a node that keeps every peer and loses the store.
        4 => {
            let node = pick(alive, rng);
            sim.block_anchor(&node);
        }
        5 => sim.heal_anchor_all(),
        6 | 7 => {
            let node = pick(alive, rng);
            sim.command(
                &node,
                Command::SetLocalEntry {
                    key: "kv".into(),
                    value: format!("v{now}").into_bytes(),
                    ttl_ms: None,
                },
            );
        }
        _ => {
            let node = pick(alive, rng);
            sim.command(
                &node,
                Command::UpdateMetadata {
                    key: "k".into(),
                    value: format!("v{now}"),
                },
            );
        }
    }
    None
}

// ---------------------------------------------------------------------------
// X-S1 — absolute epoch uniqueness, with no storage anywhere.
// ---------------------------------------------------------------------------

/// **X-S1.** The full fault menu, 128 seeds: crashes, amnesiac restarts,
/// partitions, heals, loss, reorder, anchor outages, writes — and arbitrary
/// clock skew underneath all of it.
///
/// **No epoch is ever activated by two nodes**, over the whole run and both
/// sides of every partition. Nothing is recovered, nothing is persisted, and no
/// node remembers anything across a restart: the anchor allocated every epoch,
/// so the property has nothing to be conditional on. That is what
/// `election_quorum.rs`'s matrix cannot say — there S1-strict needs a
/// `GrantStore`, and an amnesiac voter re-issues an epoch.
///
/// The skew is drawn deliberately wide (up to a whole margin either way, so
/// pairs disagree by up to two margins) to make one thing explicit: **X-S1 does
/// not consult a clock.** A steal that a broken clock made premature is still a
/// steal at a strictly higher epoch. What a clock can cost is succession
/// timing, and that is pinned in `election_external_skew.rs`.
///
/// Sampled every round: X-S2 (an observer's observed and adopted epochs never
/// run backwards) and S4b (a `Role::Host` is always inside its lease). Folded
/// over the whole run: X-S1 and X-purity. After the heal, a long settle and
/// **L1-external** — every live node on the register's own pair, and that pair
/// naming the rendezvous owner of the live set.
#[test]
fn dst_external_epoch_is_globally_unique_without_storage() {
    let mut floors: BTreeMap<AnchorEvent, u64> = BTreeMap::new();
    let mut activations = 0u64;
    for seed in 0..128u64 {
        let (counts, acts) = external_chaos(seed);
        for (event, n) in counts {
            *floors.entry(event).or_default() += n;
        }
        activations += acts;
    }
    for event in [
        AnchorEvent::Create,
        AnchorEvent::Supersede,
        AnchorEvent::Steal,
        AnchorEvent::Renew,
        AnchorEvent::Yield,
    ] {
        assert!(
            count(&floors, event) > 0,
            "vacuous: the corpus never saw a {event:?} round — {floors:?}"
        );
    }
    assert!(activations > 128, "vacuous: barely anything was elected");
    // Printed on success too, so the floors are self-evidencing: a schedule
    // that has drifted towards the floor reports it here rather than the first
    // time it drifts past.
    println!("X-S1: activations {activations}, rounds {floors:?}");
}

/// Returns the seed's round tally and how many activations it saw.
fn external_chaos(seed: u64) -> (BTreeMap<AnchorEvent, u64>, u64) {
    let mut rng = rng(seed ^ 0x7c31);
    let topo = draw_topology(&mut rng, &format!("x-chaos-{seed}"));
    let mut sim = Simulation::new(u64::from(3 + rng.below(8))); // 3..=10ms links
    sim.enable_anchor(LEASE_MS, STEAL_MARGIN_MS);
    sim.set_anchor_latency(u64::from(5 + rng.below(26))); // 5..=30ms store trips
    sim.set_loss(u8::try_from(rng.below(25)).expect("below(25) is 0..25"));
    sim.set_jitter(u64::from(rng.below(9))); // up to 8ms reorder
    for node in &topo.booted {
        let margin = i64::try_from(STEAL_MARGIN_MS).expect("a small margin");
        let skew = i64::from(rng.below(u32::try_from(2 * margin + 1).expect("small"))) - margin;
        sim.set_anchor_skew(node, skew);
    }

    let mut alive: BTreeSet<NodeId> = topo.booted.clone();
    topo.boot(&mut sim);

    let label = format!("X-S1 seed {seed}");
    let mut beliefs = Beliefs::default();
    let mut now = 0u64;
    for _round in 0..40 {
        now += u64::from(30 + rng.below(120));
        sim.run_until(Time(now));
        sample(&sim, &alive, &mut beliefs, (now, &label));
        if let Some(node) = inject_fault(&mut sim, &mut rng, &topo, &mut alive, now) {
            beliefs.forget(&node);
        }
    }

    sim.heal_all();
    sim.heal_anchor_all();
    sim.set_loss(0);
    sim.set_jitter(0);
    now += 30_000;
    sim.run_until(Time(now));

    assert_sole_activator_per_epoch(&sim.leadership_log, &label);
    assert_pure(&sim, &topo.booted, &label);
    assert_settled_on_the_register(&sim, &alive, topo.group.as_str(), &label);

    let activations = sim
        .leadership_log
        .iter()
        .filter(|(observer, _, host)| host.as_ref() == Some(observer))
        .count();
    (
        tally(&sim),
        u64::try_from(activations).expect("a bounded run"),
    )
}

/// The per-round mid-chaos sample: X-S2 and S4b, for every node still running.
fn sample(sim: &Simulation, alive: &BTreeSet<NodeId>, beliefs: &mut Beliefs, clock: (u64, &str)) {
    let (now, label) = clock;
    for node in alive {
        let role = sim.role_of(node).expect("a live node is in the simulation");
        let pair = pair_of(sim, node);

        if role == Role::Host {
            // S4b. Under External this is a strictly stronger statement than it
            // is under Settle: the lease was moved by an anchor round, so a
            // host still inside it has *won* something recently.
            let lease = sim
                .lease_until_of(node)
                .expect("a host always has a lease to read");
            assert!(
                Time(now) <= lease,
                "{label}: {node} still plays Host at {now} on a lease that ran out at {lease:?}"
            );
            assert_eq!(
                pair.1.as_ref(),
                Some(node),
                "{label}: {node} plays Host at {now} but its adopted pair is {pair:?}"
            );
        } else {
            assert_ne!(
                pair.1.as_ref(),
                Some(node),
                "{label}: {node} plays {role:?} at {now} yet its adopted pair still names it"
            );
        }
        assert_ne!(
            role,
            Role::Claimant,
            "{label}: {node} entered Role::Claimant at {now} — External has no bid to stand"
        );

        // X-S2, on both epochs an observer keeps.
        let observed = sim
            .observed_epoch_of(node)
            .expect("a live node is in the simulation");
        if let Some(was) = beliefs.observed.insert(node.clone(), observed) {
            assert!(
                observed >= was,
                "{label}: {node}'s highest observed epoch fell from {was} to {observed} by {now}"
            );
        }
        if let Some(was) = beliefs.adopted.insert(node.clone(), pair.0) {
            assert!(
                pair.0 >= was,
                "{label}: {node}'s adopted epoch fell from {was} to {} by {now}",
                pair.0
            );
        }
    }
}

/// **L1-external**, asserted once the fabric and the anchor are both fair again
/// and have had a long time to settle: exactly one host, it is **the register's
/// own holder**, it is the **rendezvous owner of the live set**, and every live
/// node has adopted that pair.
///
/// Two differences from `Quorum`'s L1, both of them the tier speaking rather
/// than the assertion being weakened:
///
/// * **There is no "or nobody hosts" arm.** There is no roster to lose: as long
///   as *someone* is alive and can reach the anchor, a dead holder's record
///   ages out and the top-ranked survivor takes it.
/// * **The register's holder is asserted as well as the ranking**, which
///   `Quorum`'s L1 has no analogue for at all. It is the stronger of the two —
///   ground truth read without asking any node — and the tier's whole claim is
///   that the two agree.
///
/// The rendezvous half is sound here for the same reason it is under `Settle`
/// and `Quorum`: **renewal is rank-gated under every activation**, row X7
/// included. An incumbent that a restart or a heal has outranked stops being
/// prompted, lets its record age out, and is superseded by the top-ranked
/// candidate — so once the fabric is fair and the anchor reachable, the settled
/// host converges on the ranking rather than merely on whoever last held the
/// record. The hand-back that gets it there is pinned step by step in
/// `election_external_failover.rs`.
fn assert_settled_on_the_register(
    sim: &Simulation,
    alive: &BTreeSet<NodeId>,
    group: &str,
    label: &str,
) {
    for node in alive {
        assert_eq!(
            sim.members_of(node),
            *alive,
            "{label}: {node} did not converge on the live set"
        );
    }
    let host = sole_host(sim, label);
    let (epoch, holder) = anchor_pair(sim, label);
    assert_eq!(
        holder, host,
        "{label}: the cluster hosts on {host} while the register holds {holder}"
    );
    assert_eq!(
        Some(&host),
        placement::owner(group, alive).as_ref(),
        "{label}: the settled host is not the rendezvous owner of the live set"
    );
    for node in alive {
        assert_eq!(
            pair_of(sim, node),
            (epoch, Some(host.clone())),
            "{label}: {node} disagrees with the register's pair"
        );
    }
}
