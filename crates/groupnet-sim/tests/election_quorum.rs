//! Deterministic Simulation Testing for `Activation::Quorum` — the CP tier,
//! over a real (lossy, partitionable) network rather than one engine at a time.
//!
//! Two properties the engine suite cannot reach on its own: that the rows
//! *compose* into an election across a cluster, and that a partition side
//! without a voter majority is starved of a host rather than served a second
//! one. The one-grant-per-epoch-per-voter invariant is read off the sim's
//! `grant_log`, which records grants at **issuance**, so a grant lost or
//! partitioned away still counts against the voter that made it.
//!
//! # The suites, and exactly what each one is allowed to claim
//!
//! * Three deterministic pins first — the happy path, the stranded incumbent,
//!   and the roster voter that never boots.
//! * **Q-chaos — [`dst_quorum_chaos_holds_global_safety`].** The `election.rs`
//!   fault schedule with a roster underneath it: global lease disjointness and
//!   belief monotonicity every round, and L1-quorum after the heal.
//! * **Q-S1 — [`dst_quorum_epoch_is_globally_unique`].** Chaos in which voters
//!   may die but never come back amnesiac: then, and only then, an epoch names
//!   at most one serializer for the whole run.
//!
//! The two *shaped* scenarios — the minority freeze (Q-S3) and the failover
//! budget (Q-budget) — live in `election_quorum_failover.rs`, the same way
//! `election_failover.rs` is split out of `election.rs`. Voter durability under
//! restarts is `election_quorum_durability.rs`.
//!
//! The honest matrix, stated once so no suite has to hedge:
//!
//! | property | never restarts | `with_recovered` | amnesiac restart |
//! |---|---|---|---|
//! | S3 minority freeze | ✓ | ✓ | ✓ |
//! | S4c-global (≤1 unexpired lease, cluster-wide, across partitions) | ✓ | ✓ | ✓ |
//! | S1-strict global epoch uniqueness (= E4-impossibility) | ✓ | ✓ | **✗** |
//!
//! The blackout is what keeps S4c-global true through an amnesiac restart: a
//! voter that reboots refuses every *new* claimant for a lease measured from
//! boot, and boot is at or after the grant it forgot. What the blackout cannot
//! restore is the *ledger*, so a voter with no store may re-grant an epoch
//! number it already spent in a previous life — which is why S1-strict is a
//! `with_recovered` property and is asserted in
//! `election_quorum_durability.rs`, not here. Nothing in this file asserts it
//! under amnesia.
//!
//! A failing seed is a reproducible counterexample, not a flake.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use groupnet_core::{
    Activation, Command, Config, GroupEngine, GroupId, GroupMode, HostedConfig, NodeId, Role, Time,
    VoterRoster, placement,
};
use groupnet_sim::{Simulation, SplitMix64};

/// How long a host's authority survives its last successful renewal — and,
/// under Quorum, the claim window, the boot guard and the grant blackout too.
const LEASE_MS: u64 = 400;
/// The anti-entropy / gossip cadence, which is also the renewal-round cadence.
const GOSSIP_INTERVAL_MS: u64 = 60;
const GROUP: &str = "shard-q";

/// The same detector timings the Settle suites run on, with a Quorum activation
/// over `voters` in place of the settle window.
fn cfg(voters: &BTreeSet<NodeId>) -> Config {
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
            activation: Activation::Quorum {
                voters: VoterRoster::new(voters.iter().cloned()),
            },
            lease_ms: LEASE_MS,
        }),
    }
}

fn nodes(ids: &[&str]) -> BTreeSet<NodeId> {
    ids.iter().map(|id| NodeId::new(*id)).collect()
}

/// A cluster of `members`, every one of them a voter, bootstrapped all-to-all.
fn cluster(members: &BTreeSet<NodeId>) -> Simulation {
    let group = GroupId::new(GROUP);
    let mut sim = Simulation::new(10);
    for id in members {
        let seeds = members.iter().filter(|x| *x != id).cloned();
        sim.add(GroupEngine::new(
            group.clone(),
            id.clone(),
            seeds,
            cfg(members),
        ));
    }
    sim
}

/// How many times `granter` had restarted by `at` — the index of the voter
/// **lifetime** a grant belongs to. A fresh engine forgets the ledger, so
/// one-grant-per-epoch is a per-lifetime statement unless storage carries it
/// across; that stronger form is `election_quorum_durability.rs`'s subject.
fn lifetime(restarts: &BTreeMap<NodeId, Vec<Time>>, granter: &NodeId, at: Time) -> usize {
    restarts
        .get(granter)
        .map_or(0, |ats| ats.iter().filter(|t| **t <= at).count())
}

/// The one-grant-per-epoch-per-voter invariant, read off every grant the run
/// ever *issued*: within one voter lifetime, no voter ever named two different
/// claimants for one epoch. Violating it is how two majorities of one roster
/// could both be collected.
///
/// An empty `restarts` map makes it the **unconditioned** statement — no voter
/// ever named two claimants for one epoch, full stop — which is what a run
/// where the voters never reboot amnesiac is entitled to assert.
fn assert_grants_single_valued(
    sim: &Simulation,
    restarts: &BTreeMap<NodeId, Vec<Time>>,
    label: &str,
) {
    let mut seen: BTreeMap<(NodeId, usize, u64), NodeId> = BTreeMap::new();
    for (at, granter, epoch, claimant) in &sim.grant_log {
        let key = (granter.clone(), lifetime(restarts, granter, *at), *epoch);
        if let Some(first) = seen.insert(key, claimant.clone()) {
            assert_eq!(
                &first, claimant,
                "{label}: {granter} granted epoch {epoch} to both {first} and {claimant} \
                 inside one lifetime (second at {at:?})"
            );
        }
    }
}

/// A quorum group elects exactly one host, keeps renewing it through the
/// grant round, and every voter's grants stay single-valued per epoch.
#[test]
fn a_quorum_group_elects_one_host_and_keeps_renewing_it() {
    let members = nodes(&["n1", "n2", "n3"]);
    let mut sim = cluster(&members);
    sim.run_until(Time(4_000));

    let hosts = sim.hosts();
    assert_eq!(hosts.len(), 1, "expected exactly one host, got {hosts:?}");
    let host = &hosts[0];

    // Renewal is a round trip here, so a host still standing at 4s has been
    // re-granted by a majority many times over — a Settle host would have
    // renewed on its own say-so and proved nothing.
    let lease = sim.lease_until_of(host).expect("the host holds a lease");
    assert!(
        lease > Time(4_000),
        "the host's lease lapsed rather than being renewed: {lease:?}"
    );
    assert!(
        !sim.grant_log.is_empty(),
        "an epoch was closed without a single grant being issued"
    );
    assert_grants_single_valued(&sim, &BTreeMap::new(), "quorum-happy-path");

    // Every voter durably wrote down what it granted, and never below the
    // epoch the host actually holds.
    let (epoch, adopted) = sim.leadership_of(host).expect("a live node");
    assert_eq!(adopted.as_ref(), Some(host));
    for voter in &members {
        let (granted_epoch, _) = sim
            .persisted_grant_of(voter)
            .unwrap_or_else(|| panic!("{voter:?} never persisted a grant"));
        assert!(
            granted_epoch >= epoch,
            "{voter:?} persisted epoch {granted_epoch}, below the adopted {epoch}"
        );
    }
}

/// The CP posture itself, in its hardest shape: the partition strands the
/// **sitting host** on the side that cannot reach a majority.
///
/// Three things must hold, and the third is the one Settle cannot promise:
///
/// * the stranded incumbent stops hosting when its lease lapses, because it
///   can no longer collect a renewal majority (row 6 via row Q7/Q8);
/// * the majority side elects a successor;
/// * **no instant of the run has two hosts** — not even during the handover.
///   That is the send-instant anchoring paying off: the successor needs three
///   grants, and each granter is promised to the incumbent until a lease after
///   its last renewal grant, which is at or after the instant the incumbent's
///   own lease is anchored to. The successor's majority therefore cannot form
///   until the incumbent's lease has expired.
#[test]
fn a_stranded_host_lapses_and_the_majority_side_elects_without_overlap() {
    let members = nodes(&["n1", "n2", "n3", "n4", "n5"]);
    let mut sim = cluster(&members);
    sim.run_until(Time(2_000));
    let hosts = sim.hosts();
    assert_eq!(hosts.len(), 1, "one host before the partition: {hosts:?}");
    let incumbent = hosts.into_iter().next().expect("length asserted above");

    // 2 | 3, with the incumbent in the pair. It can never collect the three
    // grants a five-voter roster asks for again.
    let companion = members
        .iter()
        .find(|n| **n != incumbent)
        .expect("five members")
        .clone();
    let minority: BTreeSet<NodeId> = [incumbent.clone(), companion].into_iter().collect();
    let majority: BTreeSet<NodeId> = members.difference(&minority).cloned().collect();
    for a in &minority {
        for b in &majority {
            sim.block(a, b);
            sim.block(b, a);
        }
    }

    // Sampled finely across the whole handover, not just at the end.
    let starved_from = 2_000 + LEASE_MS + GOSSIP_INTERVAL_MS;
    for at in (2_010..=8_000).step_by(10) {
        sim.run_until(Time(at));
        let hosting = sim.hosts();
        assert!(
            hosting.len() <= 1,
            "two nodes held the group at once at {at}: {hosting:?}"
        );
        if at >= starved_from {
            for node in &minority {
                assert_ne!(
                    sim.role_of(node),
                    Some(Role::Host),
                    "{node:?} still hosted from the minority side at {at}"
                );
            }
        }
    }
    assert_grants_single_valued(&sim, &BTreeMap::new(), "quorum-minority");

    let hosts = sim.hosts();
    assert_eq!(
        hosts.len(),
        1,
        "the majority side must have elected a successor, got {hosts:?}"
    );
    assert!(
        majority.contains(&hosts[0]),
        "the surviving host {:?} is not on the majority side",
        hosts[0]
    );
    assert_ne!(hosts[0], incumbent, "the stranded incumbent must be gone");
}

/// A roster names **voters, not members**: a node in the roster that never
/// boots still counts toward `majority`, and 3-of-5 is still a majority when
/// only four of the five exist. Pinned on its own because every seeded suite
/// below draws this shape and would otherwise only prove it by accident.
///
/// The ghost is not merely tolerated — it is *asked*: row Q4 broadcasts a claim
/// to every voter, live or not, precisely so a roster member gossip has never
/// shown alive is not silently skipped.
#[test]
fn a_roster_voter_that_never_boots_still_leaves_a_majority() {
    let booted = nodes(&["n1", "n2", "n3", "n4"]);
    let mut voters = booted.clone();
    voters.insert(NodeId::new("ghost"));
    assert_eq!(VoterRoster::new(voters.iter().cloned()).majority(), 3);

    let group = GroupId::new(GROUP);
    let mut sim = Simulation::new(10);
    for id in &booted {
        let seeds = booted.iter().filter(|x| *x != id).cloned();
        sim.add(GroupEngine::new(
            group.clone(),
            id.clone(),
            seeds,
            cfg(&voters),
        ));
    }
    sim.run_until(Time(4_000));

    let hosts = sim.hosts();
    assert_eq!(
        hosts.len(),
        1,
        "3-of-5 is a majority of the booted voters, so the group must host: {hosts:?}"
    );
    assert!(
        sim.lease_until_of(&hosts[0])
            .is_some_and(|u| u > Time(4_000)),
        "the host was elected but could not keep renewing without the fifth voter"
    );
    assert!(
        sim.grant_log.iter().all(|(_, g, _, _)| booted.contains(g)),
        "a node that never booted somehow granted"
    );
    assert_grants_single_valued(&sim, &BTreeMap::new(), "quorum-absent-voter");
}

// ---------------------------------------------------------------------------
// Shared DST scaffolding.
// ---------------------------------------------------------------------------

/// Seeds the shared deterministic PRNG so each schedule is reproducible. Each
/// suite salts the seed so they explore independent streams.
fn rng(seed: u64) -> SplitMix64 {
    SplitMix64::new(seed ^ 0x9e37_79b9_7f4a_7c15)
}

/// One seed's cluster: the engines that actually boot, and the static roster
/// whose majority closes an epoch.
///
/// The two are deliberately allowed to differ in both directions. A roster may
/// name a node that never boots — the **non-member voter**, which still counts
/// toward `majority` and is still sent every claim — and a booted node may not
/// vote at all, which is how a *non-voter* comes to host (nothing about the
/// claim guard is roster-gated; only granting is, by row Q0).
#[derive(Debug)]
struct Topology {
    group: GroupId,
    /// The engines the simulation runs.
    booted: BTreeSet<NodeId>,
    /// The static roster — a superset, a subset, or neither, of `booted`.
    voters: BTreeSet<NodeId>,
    /// `voters.len() / 2 + 1`, recomputed here rather than read off the engine.
    majority: usize,
}

impl Topology {
    fn cfg(&self) -> Config {
        cfg(&self.voters)
    }

    /// An engine for `id` bootstrapped against every other node in `peers`. The
    /// seed set is what lets two sides of a hard partition find each other
    /// after a heal.
    fn engine(&self, id: &NodeId, peers: &BTreeSet<NodeId>) -> GroupEngine {
        let seeds = peers.iter().filter(|x| *x != id).cloned();
        GroupEngine::new(self.group.clone(), id.clone(), seeds, self.cfg())
    }

    fn boot(&self, sim: &mut Simulation) {
        for id in &self.booted {
            sim.add(self.engine(id, &self.booted));
        }
    }

    /// How many roster members `set` holds — the only nodes in it that can ever
    /// contribute a grant.
    fn voters_in(&self, set: &BTreeSet<NodeId>) -> usize {
        self.voters.intersection(set).count()
    }

    /// Whether `set` can close an epoch at all.
    fn has_majority(&self, set: &BTreeSet<NodeId>) -> bool {
        self.voters_in(set) >= self.majority
    }
}

/// Draws a cluster of 3..=6 nodes and a roster of **3 or 5** over it, in every
/// arrangement the tier has to survive: the roster may be all of the cluster, a
/// strict subset of it (so a non-voter can be top-ranked), or five voters only
/// four of which ever boot.
fn draw_topology(rng: &mut SplitMix64, name: &str) -> Topology {
    let n = 3 + usize::try_from(rng.below(4)).expect("a 0..4 draw"); // 3..=6 nodes
    let ids: Vec<NodeId> = (0..n).map(|i| NodeId::new(format!("n{i}"))).collect();
    let booted: BTreeSet<NodeId> = ids.iter().cloned().collect();

    // One seed in four (where four voters can be found) puts a fifth voter in
    // the roster that is never booted at all.
    let absent = n >= 4 && rng.below(4) == 0;
    let booted_voters = if absent {
        4
    } else if n >= 5 && rng.below(2) == 0 {
        5
    } else {
        3
    };
    // Rotate which nodes vote, so the roster is not always the low ids and the
    // rendezvous top of a run is sometimes outside it.
    let off = usize::try_from(rng.below(u32::try_from(n).expect("a handful of nodes")))
        .expect("bounded by n");
    let mut voters: BTreeSet<NodeId> = (0..booted_voters)
        .map(|k| ids[(off + k) % n].clone())
        .collect();
    if absent {
        voters.insert(NodeId::new("ghost"));
    }
    let majority = voters.len() / 2 + 1;
    Topology {
        group: GroupId::new(name),
        booted,
        voters,
        majority,
    }
}

fn pick(set: &BTreeSet<NodeId>, rng: &mut SplitMix64) -> NodeId {
    let v: Vec<&NodeId> = set.iter().collect();
    let n = u32::try_from(v.len()).expect("these clusters are a handful of nodes");
    let i = usize::try_from(rng.below(n)).expect("bounded by the set size");
    v[i].clone()
}

/// An `(epoch, host)` pair: the unit that names a serializer.
type Pair = (u64, Option<NodeId>);

/// The pair `node` has adopted. Every caller here reads a node it knows is in
/// the simulation.
fn pair_of(sim: &Simulation, node: &NodeId) -> Pair {
    sim.leadership_of(node)
        .expect("a live node is in the simulation")
}

/// The fencing order over pairs, **recomputed here** from [`placement`] and
/// nothing else, so a suite that pins the order is not checking the engine
/// against its own arithmetic.
fn cmp_pair(group: &str, a: &Pair, b: &Pair) -> Ordering {
    match a.0.cmp(&b.0) {
        Ordering::Equal => {}
        by_epoch => return by_epoch,
    }
    match (&a.1, &b.1) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(x), Some(y)) if x == y => Ordering::Equal,
        (Some(x), Some(y)) => {
            let two: BTreeSet<NodeId> = [x.clone(), y.clone()].into_iter().collect();
            if placement::owner(group, &two).as_ref() == Some(x) {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
    }
}

/// **S4c-global.** Every node in the simulation holding an unexpired lease at
/// `now` — *across partitions*, because `Simulation::nodes` sees every engine
/// whatever the network is doing to it.
///
/// This is the whole CP claim in one read, and it is exact in virtual time:
/// `lease_until_of` is `Some` only for a node currently playing `Role::Host`,
/// and the instant it returns is the deadline that armed that node's driver
/// timer. Two entries means two nodes could both have served the group at the
/// same instant — which `Settle` permits between partition sides and `Quorum`
/// must not, ever.
fn unexpired_leases(sim: &Simulation, now: u64) -> Vec<NodeId> {
    sim.nodes()
        .into_iter()
        .filter(|n| sim.lease_until_of(n).is_some_and(|until| until > Time(now)))
        .collect()
}

fn assert_lease_disjoint(sim: &Simulation, now: u64, label: &str) {
    let leased = unexpired_leases(sim, now);
    assert!(
        leased.len() <= 1,
        "{label}: two unexpired leases at {now}, cluster-wide: {leased:?}"
    );
}

/// The single node that believes it holds the group, asserting there is exactly
/// one. `Simulation::hosts` yields ids in order, so failure text is stable.
fn sole_host(sim: &Simulation, label: &str) -> NodeId {
    let hosts = sim.hosts();
    assert_eq!(
        hosts.len(),
        1,
        "{label}: expected exactly one host: {hosts:?}"
    );
    hosts.into_iter().next().expect("length asserted above")
}

/// Which nodes a restart may bring back. Q-chaos restarts anyone; Q-S1 restarts
/// only non-voters, because an amnesiac *voter* is exactly the case that costs
/// the run its global epoch uniqueness.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Restarts {
    Anyone,
    NonVotersOnly,
}

/// Applies one fault — crash, restart, two-way partition, heal, or one of two
/// write shapes that keep anti-entropy busy underneath the election. Returns
/// the node id if the fault was a **restart**, whose fresh engine invalidates
/// everything recorded about that observer.
fn inject_fault(
    sim: &mut Simulation,
    rng: &mut SplitMix64,
    topo: &Topology,
    alive: &mut BTreeSet<NodeId>,
    clock: (u64, Restarts),
) -> Option<NodeId> {
    let (now, policy) = clock;
    match rng.below(8) {
        0 if alive.len() > 2 => {
            let victim = pick(alive, rng);
            sim.crash(&victim);
            alive.remove(&victim);
        }
        1 if alive.len() < topo.booted.len() => {
            // A restart is a *fresh* engine: incarnation 0, epoch 0, and — with
            // no store behind it — an empty grant ledger guarded only by the
            // boot blackout.
            let down: BTreeSet<NodeId> = topo
                .booted
                .iter()
                .filter(|x| !alive.contains(*x))
                .filter(|x| policy == Restarts::Anyone || !topo.voters.contains(*x))
                .cloned()
                .collect();
            if down.is_empty() {
                return None;
            }
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
        4 | 5 => {
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
// Q-chaos — global safety every round, L1-quorum at the end.
// ---------------------------------------------------------------------------

/// **Q-chaos.** The `election.rs` fault schedule — crashes, amnesiac restarts,
/// partitions, heals, loss, reorder and writes — run over a voter roster.
///
/// Sampled every round, mid-chaos:
///
/// * **S4c-global.** At most one unexpired lease *anywhere*, across partitions.
///   This is the statement `Settle` cannot make: E3 has both sides of a split
///   hosting on purpose. Here the blackout and the promise make it hold even
///   through amnesiac voter restarts.
/// * **S4b.** A node playing `Role::Host` is always inside its lease.
/// * **S2.** Beliefs do not run backwards: the highest epoch seen, the adopted
///   epoch, and the adopted pair in the fencing order — with the one carve-out
///   [`check_monotone`] names. A restart forgets the observer entirely.
/// * **Role/pair self-consistency**, in both directions.
///
/// Folded over the whole run: one claimant per `(granter, epoch)` **per granter
/// lifetime**. Not unconditioned — an amnesiac voter is a new voter, and the
/// blackout rather than the ledger is what covers the seam. Q-S1 asserts the
/// unconditioned form on runs that earn it.
///
/// After heal, loss 0, jitter 0 and a long settle, **L1-quorum**: see
/// [`assert_one_fenced_host`] for why "no host at all" is one of its two legal
/// answers.
#[test]
fn dst_quorum_chaos_holds_global_safety() {
    for seed in 0..128u64 {
        quorum_chaos(seed);
    }
}

/// What one observer was last seen believing — the history S2 is read against.
#[derive(Debug, Default)]
struct Beliefs {
    epochs: BTreeMap<NodeId, u64>,
    pairs: BTreeMap<NodeId, Pair>,
}

impl Beliefs {
    /// A restarted node is a *new* observer on a fresh logical timeline: its
    /// election state is in memory only, so it legitimately comes back a
    /// Follower at epoch 0 and nothing recorded about it applies.
    fn forget(&mut self, node: &NodeId) {
        self.epochs.remove(node);
        self.pairs.remove(node);
    }
}

fn quorum_chaos(seed: u64) {
    let mut rng = rng(seed ^ 0x90c7);
    let topo = draw_topology(&mut rng, &format!("chaos-{seed}"));
    let mut sim = Simulation::new(u64::from(3 + rng.below(8))); // 3..=10ms links
    sim.set_loss(u8::try_from(rng.below(25)).expect("below(25) is 0..25"));
    sim.set_jitter(u64::from(rng.below(9))); // up to 8ms reorder

    let mut alive: BTreeSet<NodeId> = topo.booted.clone();
    topo.boot(&mut sim);

    let label = format!("Q-chaos seed {seed}");
    let mut beliefs = Beliefs::default();
    let mut restarts: BTreeMap<NodeId, Vec<Time>> = BTreeMap::new();
    let mut now = 0u64;
    // The sampling division of labour, stated once so no suite has to pretend
    // its own cadence is continuous. This loop hops ~30–150ms between samples:
    // it buys **breadth** — 128 seeds × 30 rounds of drawn faults — and a
    // finer step would spend the whole budget on one seed. Fine-grained
    // S4c-global coverage is bought by the *shaped* scenarios instead:
    // `election_quorum_failover.rs`'s Q-S3 walks a partitioned cluster in 5ms
    // steps for the whole minority hold, and the stranded-incumbent pin above
    // walks a handover in 10ms steps. A two-host instant would therefore have to
    // fit inside 5ms of a shaped run *and* dodge every sample of the seeded ones.
    for _round in 0..30 {
        now += u64::from(30 + rng.below(120));
        sim.run_until(Time(now));
        assert_lease_disjoint(&sim, now, &label);
        sample(&sim, &topo, &alive, &mut beliefs, (now, &label));

        if let Some(node) = inject_fault(
            &mut sim,
            &mut rng,
            &topo,
            &mut alive,
            (now, Restarts::Anyone),
        ) {
            beliefs.forget(&node);
            restarts.entry(node).or_default().push(Time(now));
        }
    }

    sim.heal_all();
    sim.set_loss(0);
    sim.set_jitter(0);
    now += 30_000;
    sim.run_until(Time(now));

    assert_lease_disjoint(&sim, now, &label);
    assert_grants_single_valued(&sim, &restarts, &label);
    assert_one_fenced_host(&sim, &topo, &alive, (now, &label));
}

/// The per-round mid-chaos sample: S4b, S2 and role/pair self-consistency, for
/// every node still running.
fn sample(
    sim: &Simulation,
    topo: &Topology,
    alive: &BTreeSet<NodeId>,
    beliefs: &mut Beliefs,
    clock: (u64, &str),
) {
    let (now, label) = clock;
    for node in alive {
        let role = sim.role_of(node).expect("a live node is in the simulation");
        let pair = pair_of(sim, node);

        if role == Role::Host {
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

        check_monotone(sim, topo, node, beliefs, clock);
    }
}

/// S2 for one observer, against what it was last seen believing.
///
/// The pair may regress in the fencing order in exactly one situation, and the
/// assertion names it rather than tolerating it generally: a node giving up
/// **its own** hostship. Row 6 (the lease lapsed) and row 15 (a voluntary
/// leave) step `(e, Some(self))` down to `(e, None)`, which *is* lower in the
/// order — deliberately, since keeping the epoch is what leaves the stepped-down
/// node fenced against by any later pair. From `(e, None)` it may then adopt any
/// `(e, Some(x))`, so a sample straddling both moves can land on a pair that
/// loses the equal-epoch tiebreak to the one it held.
fn check_monotone(
    sim: &Simulation,
    topo: &Topology,
    node: &NodeId,
    beliefs: &mut Beliefs,
    clock: (u64, &str),
) {
    let (now, label) = clock;
    let observed = sim
        .observed_epoch_of(node)
        .expect("a live node is in the simulation");
    if let Some(was) = beliefs.epochs.insert(node.clone(), observed) {
        assert!(
            observed >= was,
            "{label}: {node}'s highest observed epoch fell from {was} to {observed} by {now}"
        );
    }

    let pair = pair_of(sim, node);
    let Some(was) = beliefs.pairs.insert(node.clone(), pair.clone()) else {
        return; // first sighting — nothing to compare against
    };
    assert!(
        pair.0 >= was.0,
        "{label}: {node}'s adopted epoch fell from {} to {} by {now}",
        was.0,
        pair.0
    );
    if cmp_pair(topo.group.as_str(), &pair, &was) == Ordering::Less {
        assert_eq!(
            was.1.as_ref(),
            Some(node),
            "{label}: {node} regressed from {was:?} to {pair:?} by {now} \
             without giving up hostship of its own"
        );
    }
}

/// **L1-quorum**, asserted once the fabric is fair again and has had a long
/// time to settle. Two outcomes are legal, and the *roster* decides which:
///
/// * **A majority of the roster is alive.** Exactly one node hosts, it is the
///   rendezvous top of the live set — voter or not, which is the point of
///   drawing rosters that are strict subsets — every live node names the same
///   pair, and its lease is the only one in the cluster.
/// * **The roster majority is gone.** *No* node hosts, however healthy the
///   survivors look to each other. That is the other half of the CP posture,
///   and asserting it is what stops "converges on one host" from being
///   satisfied by a group that quietly stopped consulting its roster.
fn assert_one_fenced_host(
    sim: &Simulation,
    topo: &Topology,
    alive: &BTreeSet<NodeId>,
    clock: (u64, &str),
) {
    let (now, label) = clock;
    if !topo.has_majority(alive) {
        assert!(
            sim.hosts().is_empty(),
            "{label}: {} of {} voters are alive, below the majority of {}, \
             yet the group still hosts: {:?}",
            topo.voters_in(alive),
            topo.voters.len(),
            topo.majority,
            sim.hosts()
        );
        return;
    }

    for node in alive {
        assert_eq!(
            sim.members_of(node),
            *alive,
            "{label}: {node} did not converge on the live set"
        );
    }
    let expected = placement::owner(topo.group.as_str(), alive).expect("a non-empty live set");
    let host = sole_host(sim, label);
    assert_eq!(
        host, expected,
        "{label}: the group settled on {host}, not the rendezvous owner {expected}"
    );

    let settled = pair_of(sim, &host);
    assert_eq!(
        settled.1.as_ref(),
        Some(&host),
        "{label}: {host} hosts but names {settled:?}"
    );
    for node in alive {
        assert_eq!(
            pair_of(sim, node),
            settled,
            "{label}: {node} disagrees on the pair the cluster settled"
        );
    }
    assert_eq!(
        unexpired_leases(sim, now),
        vec![expected],
        "{label}: the settled group does not hold exactly one lease"
    );
}

// ---------------------------------------------------------------------------
// Q-S1 — global epoch uniqueness, on the runs that earn it.
// ---------------------------------------------------------------------------

/// **Q-S1.** The same chaos, with one rule changed: a voter that crashes stays
/// dead. Then — and, per the matrix in the module doc, *only* then without a
/// store — the run is entitled to the strongest statement the tier makes:
///
/// * **No epoch was ever activated by two distinct nodes**, over the whole run
///   and both sides of every partition. This is E4-impossibility: the
///   same-epoch collision `election_failover.rs` pins a *tiebreak* for cannot
///   occur at all here, because the two sides' majorities intersect in a voter
///   that already spent that epoch.
/// * **Unconditioned `(granter, epoch)` uniqueness** in the whole grant log —
///   the mechanism the first bullet rests on, asserted directly rather than
///   inferred.
///
/// Non-voters still restart freely, which is what keeps this a chaos run rather
/// than a calm one; a restarted non-voter comes back at epoch 0 and cannot
/// re-open a spent epoch, because every voter's ledger refuses an epoch at or
/// below the one it has already granted.
#[test]
fn dst_quorum_epoch_is_globally_unique() {
    for seed in 0..64u64 {
        epoch_uniqueness(seed);
    }
}

fn epoch_uniqueness(seed: u64) {
    let mut rng = rng(seed ^ 0x51c1);
    let topo = draw_topology(&mut rng, &format!("unique-{seed}"));
    let mut sim = Simulation::new(u64::from(3 + rng.below(8)));
    sim.set_loss(u8::try_from(rng.below(25)).expect("below(25) is 0..25"));
    sim.set_jitter(u64::from(rng.below(9)));

    let mut alive: BTreeSet<NodeId> = topo.booted.clone();
    topo.boot(&mut sim);

    let label = format!("Q-S1 seed {seed}");
    let mut now = 0u64;
    for _round in 0..30 {
        now += u64::from(30 + rng.below(120));
        sim.run_until(Time(now));
        assert_lease_disjoint(&sim, now, &label);
        inject_fault(
            &mut sim,
            &mut rng,
            &topo,
            &mut alive,
            (now, Restarts::NonVotersOnly),
        );
    }
    sim.heal_all();
    sim.set_loss(0);
    sim.set_jitter(0);
    now += 20_000;
    sim.run_until(Time(now));
    assert_lease_disjoint(&sim, now, &label);

    // No voter restarted, so the ledger is unbroken and the fold is
    // unconditioned: an empty restart map.
    assert_grants_single_valued(&sim, &BTreeMap::new(), &label);
    assert_sole_activator_per_epoch(&sim.leadership_log, &label);
}

/// **S1-strict**: across the whole run, no two nodes ever activated the same
/// epoch. An activation is the one leadership transition a node logs naming
/// *itself*.
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
