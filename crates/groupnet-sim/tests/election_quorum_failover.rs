//! Deterministic Simulation Testing for **`Activation::Quorum` failover and
//! the minority freeze** — what the CP tier does when the host goes away or the
//! cluster is cut in two.
//!
//! Split out of `election_quorum.rs` the same way `election_failover.rs` is
//! split out of `election.rs`: the chaos suites and the roster's global safety
//! properties live there, the two *shaped* scenarios live here.
//!
//! * **Q-S3 — [`dst_quorum_minority_never_activates`].** The property the whole
//!   tier is chosen for: a partition side holding fewer than `majority` of the
//!   roster never gets a host, however healthy it looks to itself — and a
//!   *sitting* host stranded on such a side lapses on schedule rather than
//!   serving a second copy of the group. Where `election_failover.rs`'s E3
//!   asserts that both sides of a split host (the AP posture, deliberately),
//!   this asserts that exactly one side ever can.
//! * **Q-budget — [`dst_quorum_failover_lands_inside_its_budget`].** What the
//!   grant round trip costs when the host crashes, with every millisecond of
//!   the budget accounted for and no fudge term.
//!
//! A failing seed is a reproducible counterexample, not a flake.

use std::collections::{BTreeMap, BTreeSet};

use groupnet_core::{
    Activation, Config, GroupEngine, GroupId, GroupMode, HostedConfig, NodeId, Role, Time,
    VoterRoster, placement,
};
use groupnet_sim::{Simulation, SplitMix64};

/// How long a host's authority survives its last successful renewal — and,
/// under Quorum, the claim window, the boot guard and the grant blackout too.
const LEASE_MS: u64 = 400;
/// The anti-entropy / gossip cadence, which is also the renewal-round cadence.
const GOSSIP_INTERVAL_MS: u64 = 60;

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

    fn boot(&self, sim: &mut Simulation) {
        for id in &self.booted {
            let seeds = self.booted.iter().filter(|x| *x != id).cloned();
            sim.add(GroupEngine::new(
                self.group.clone(),
                id.clone(),
                seeds,
                self.cfg(),
            ));
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

/// [`pick`], removing the drawn node — a draw without replacement.
fn take(set: &mut BTreeSet<NodeId>, rng: &mut SplitMix64) -> NodeId {
    let node = pick(set, rng);
    set.remove(&node);
    node
}

/// An `(epoch, host)` pair: the unit that names a serializer.
type Pair = (u64, Option<NodeId>);

/// The pair `node` has adopted. Every caller here reads a node it knows is in
/// the simulation.
fn pair_of(sim: &Simulation, node: &NodeId) -> Pair {
    sim.leadership_of(node)
        .expect("a live node is in the simulation")
}

/// **S4c-global.** Every node in the simulation holding an unexpired lease at
/// `now` — *across partitions*, because `Simulation::nodes` sees every engine
/// whatever the network is doing to it.
///
/// Exact in virtual time: `lease_until_of` is `Some` only for a node currently
/// playing `Role::Host`, and the instant it returns is the deadline that armed
/// that node's driver timer. Two entries means two nodes could both have served
/// the group at the same instant — which `Settle` permits between partition
/// sides and `Quorum` must not, ever.
fn assert_lease_disjoint(sim: &Simulation, now: u64, label: &str) {
    let leased: Vec<NodeId> = sim
        .nodes()
        .into_iter()
        .filter(|n| sim.lease_until_of(n).is_some_and(|until| until > Time(now)))
        .collect();
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

/// The one-grant-per-epoch-per-voter invariant, read off every grant the run
/// ever *issued*. Neither suite here restarts anything, so the fold is
/// unconditioned: no voter ever named two claimants for one epoch, full stop.
fn assert_grants_single_valued(sim: &Simulation, label: &str) {
    let mut seen: BTreeMap<(NodeId, u64), NodeId> = BTreeMap::new();
    for (at, granter, epoch, claimant) in &sim.grant_log {
        if let Some(first) = seen.insert((granter.clone(), *epoch), claimant.clone()) {
            assert_eq!(
                &first, claimant,
                "{label}: {granter} granted epoch {epoch} to both {first} and \
                 {claimant} (second at {at:?})"
            );
        }
    }
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

// ---------------------------------------------------------------------------
// Q-S3 — a voter minority is starved, not served.
// ---------------------------------------------------------------------------

/// How long past the split the suite keeps sampling: long enough for the served
/// side to bury everything it lost, wait out the promises the roster made to
/// the incumbent, and burn a claim window if it needs one.
const MINORITY_HOLD_MS: u64 = 3 * LEASE_MS + 2_000;

/// **Q-S3.** A partition side holding fewer than `majority` of the roster never
/// activates.
///
/// The split is drawn per seed with the incumbent on either side, and the
/// starved side is a genuine voter minority (0..`majority`-1 roster members)
/// plus any number of non-voters. Asserted:
///
/// * **S4c-global, sampled every 5ms** across the whole handover — never two
///   unexpired leases anywhere in the cluster, partitions included.
/// * **A stranded incumbent demotes by `split + LEASE_MS + GOSSIP_INTERVAL_MS`,
///   exactly.** Its lease is anchored to the send instant of its last renewal
///   round, which is at or before the split, so `split + LEASE_MS` bounds it,
///   and the engine arms that deadline on the driver timer — the tick term is
///   rounding, not slack. It also *announces* the step-down, which an
///   application reconciling against `LeadershipChanged` has to be told.
/// * **No starved node activates anything after the split**, read off the tail
///   of `leadership_log` rather than off a role sample, so a hostship that
///   flickered between two samples would still be caught.
/// * **The served side ends with exactly one host**, and it is a different node
///   exactly when the incumbent was the one stranded.
#[test]
fn dst_quorum_minority_never_activates() {
    for seed in 0..48u64 {
        minority_split(seed);
    }
}

fn minority_split(seed: u64) {
    let mut rng = rng(seed ^ 0x53a0);
    let topo = draw_topology(&mut rng, &format!("minority-{seed}"));
    let mut sim = Simulation::new(2 + u64::from(rng.below(5)));
    sim.set_jitter(u64::from(rng.below(6)));
    topo.boot(&mut sim);

    let split_at = 3_000 + u64::from(rng.below(500));
    sim.run_until(Time(split_at));
    let label = format!("Q-S3 seed {seed}");
    let incumbent = sole_host(&sim, &format!("{label} (before the split)"));

    let starved = draw_starved_side(&mut rng, &topo, &incumbent);
    let served: BTreeSet<NodeId> = topo.booted.difference(&starved).cloned().collect();
    assert!(
        !topo.has_majority(&starved) && topo.has_majority(&served),
        "{label}: the draw did not produce a voter minority: {starved:?} | {served:?}"
    );
    let mark = sim.leadership_log.len();
    for a in &starved {
        for b in &served {
            sim.block(a, b);
            sim.block(b, a);
        }
    }

    let stranded = starved.contains(&incumbent);
    let demote_by = split_at + LEASE_MS + GOSSIP_INTERVAL_MS;
    let demoted_at = hold_the_split(&mut sim, (&starved, &incumbent), (split_at, &label));

    if stranded {
        let at = demoted_at.unwrap_or_else(|| panic!("{label}: {incumbent} never stopped hosting"));
        assert!(
            at <= demote_by,
            "{label}: {incumbent} held the group until {at}, past the {demote_by} \
             its last renewal round bounds it to"
        );
        assert!(
            sim.leadership_log[mark..]
                .iter()
                .any(|(observer, _, host)| observer == &incumbent && host.is_none()),
            "{label}: {incumbent} went quiet instead of announcing its step down"
        );
    }
    for (observer, epoch, host) in &sim.leadership_log[mark..] {
        assert!(
            !(host.as_ref() == Some(observer) && starved.contains(observer)),
            "{label}: {observer} activated epoch {epoch} from the starved side"
        );
    }

    let host = sole_host(&sim, &format!("{label} (after the hold)"));
    assert!(
        served.contains(&host),
        "{label}: {host} hosts but is not on the side that holds the roster majority"
    );
    assert_eq!(
        host != incumbent,
        stranded,
        "{label}: the group changed hands exactly when the incumbent was stranded"
    );
    assert_grants_single_valued(&sim, &label);
}

/// Runs the split out, sampling S4c-global and the starved side's roles every
/// 5ms. Returns the first instant the incumbent was seen not hosting.
fn hold_the_split(
    sim: &mut Simulation,
    side: (&BTreeSet<NodeId>, &NodeId),
    clock: (u64, &str),
) -> Option<u64> {
    let (starved, incumbent) = side;
    let (split_at, label) = clock;
    let demote_by = split_at + LEASE_MS + GOSSIP_INTERVAL_MS;
    let mut demoted_at = None;
    for at in (split_at + 5..=split_at + MINORITY_HOLD_MS).step_by(5) {
        sim.run_until(Time(at));
        assert_lease_disjoint(sim, at, label);
        for node in starved {
            let hosting = sim.role_of(node) == Some(Role::Host);
            assert!(
                !hosting || at <= demote_by,
                "{label}: {node} still hosted from the starved side at {at}, \
                 past the {demote_by} its lease bounds it to"
            );
            if node == incumbent && !hosting && demoted_at.is_none() {
                demoted_at = Some(at);
            }
        }
    }
    demoted_at
}

/// One side of a two-way split, drawn so it is always a genuine voter minority
/// and the other side always keeps one: the incumbent is placed first (both
/// placements appear across the seeds), then as many further voters as the
/// roster can spare, then any number of non-voters.
fn draw_starved_side(
    rng: &mut SplitMix64,
    topo: &Topology,
    incumbent: &NodeId,
) -> BTreeSet<NodeId> {
    let booted_voters = topo.voters_in(&topo.booted);
    // What the served side can give up and still close an epoch, capped by what
    // "minority" means at all.
    let spare = (booted_voters - topo.majority).min(topo.majority - 1);
    let mut starved = BTreeSet::new();
    if rng.below(2) == 0 {
        starved.insert(incumbent.clone());
    }
    let mut voter_pool: BTreeSet<NodeId> = topo
        .voters
        .intersection(&topo.booted)
        .filter(|v| !starved.contains(*v))
        .cloned()
        .collect();
    let room = spare - topo.voters_in(&starved);
    let wanted = rng.below(u32::try_from(room).expect("a handful of voters") + 1);
    for _ in 0..wanted {
        let node = take(&mut voter_pool, rng);
        starved.insert(node);
    }
    let mut other_pool: BTreeSet<NodeId> = topo
        .booted
        .difference(&topo.voters)
        .filter(|v| !starved.contains(*v))
        .cloned()
        .collect();
    while !other_pool.is_empty() && rng.below(2) == 0 {
        let node = take(&mut other_pool, rng);
        starved.insert(node);
    }
    if starved.is_empty() {
        // A degenerate draw would assert nothing; give it a node the served
        // side can still spare.
        let mut pool = if voter_pool.is_empty() {
            other_pool
        } else {
            voter_pool
        };
        let node = take(&mut pool, rng);
        starved.insert(node);
    }
    starved
}

// ---------------------------------------------------------------------------
// Q-budget — what the grant round trip costs on failover.
// ---------------------------------------------------------------------------

/// The largest link latency this suite configures.
const MAX_LATENCY_MS: u64 = 6;
/// The largest per-message reorder jitter this suite configures.
const MAX_JITTER_MS: u64 = 5;
/// One wire hop, worst case.
const HOP_MS: u64 = MAX_LATENCY_MS + MAX_JITTER_MS;

/// **Q-budget.** After the host crashes a successor is serving inside a budget
/// this suite states in full — every millisecond of it accounted for, with no
/// fudge term.
///
/// ```text
/// budget = detection_window_ms(n).max(LEASE_MS)   // detect, and outlive the promise
///        + HOP_MS                                 // a renewal claim in flight at the crash
///        + 2 · HOP_MS                             // the claim out, the grants back
///        + GOSSIP_INTERVAL_MS                     // the re-offer cadence / tick rounding
///        + HOP_MS                                 // the activation reaching the survivors
/// ```
///
/// * **`detection_window_ms(n).max(LEASE_MS)`** — two waits that *overlap*
///   rather than add, so the longer one binds. The successor cannot **claim**
///   until it has buried the dead host (`detection_window_ms`), and no voter
///   will **grant** it until the promise made to the dead host expires, which
///   is at most one `LEASE_MS` after the crash (a promise is renewed on every
///   grant, and the last one a dead host collected was at or before it died).
///   **The `max` is load-bearing at n = 3**, where `detection_window_ms(3)` is
///   `2·(50 + 2·40) + 120` = **380ms — under the 400ms lease**. A budget stated
///   as detection alone would be short there, and short by a *promise*, which
///   is the one wait a Settle failover does not pay at all.
/// * **`HOP_MS` — the renewal claim already in flight.** The promise term above
///   is anchored at the crash only if nothing the dead host sent outlives it,
///   and something does: [`Simulation::crash`] removes the engine but does not
///   purge the queues, exactly like a real process dying with packets on the
///   wire. A renewal claim broadcast in the instant before the crash is still
///   delivered, up to one hop later, and every voter that answers it (row Q2)
///   slides its promise to *that arrival* + `LEASE_MS`. So the last promise the
///   successor must outlive is anchored at most `HOP_MS` after the crash, not at
///   it. Carried as its own term rather than folded into the `max`: it applies
///   to the promise side alone, and adding it outside keeps the arithmetic both
///   conservative and readable.
/// * **`2 · HOP_MS`** — one claim round trip. Under `Settle` nothing crosses
///   the wire between the window shutting and the activation; here the epoch is
///   closed by grants, so the round trip is on the critical path.
/// * **`GOSSIP_INTERVAL_MS`** — a claim broadcast before the promises lapse is
///   refused *in silence*, and the next thing to ask the voters again is row
///   3's re-offer on the anti-entropy round. The same term covers the tick
///   rounding on the burial itself: the engine only observes time when ticked.
/// * **`HOP_MS`** — the activation's `LeadState` reaching the other survivors,
///   because the assertion is that they have all adopted the new pair.
///
/// # No term for the successor that needs its own vote
///
/// There used to be one — a whole extra `LEASE_MS`, on every shape where the
/// successor's **own** grant was part of the majority it needed. A claimant's
/// self-grant was attempted once, when row Q4 opened the round, and at that
/// instant the successor is normally still promised to the host it just buried,
/// so Q3 refused it. Row 3's re-offer re-asked the *peers* every round and
/// nobody re-asked the claimant, so such a round could not close at all and the
/// claim window (one `LEASE_MS`) was burnt before the guard re-bid one epoch
/// higher with a fresh round.
///
/// Row **Q4b** — the self-grant re-attempted on every tick the round is still
/// open — removed that term. The promise lapses mid-window, the retry's verdict
/// flips to a grant, and the round closes on the same anchor it opened with, so
/// this shape now lands inside the same budget every other one does. The
/// activation is still anchored to `round_sent_at`, so nothing here bought its
/// speed with a longer lease.
#[test]
fn dst_quorum_failover_lands_inside_its_budget() {
    for seed in 0..32u64 {
        failover_budget(seed);
    }
}

fn failover_budget(seed: u64) {
    let mut rng = rng(seed ^ 0xe2b0);
    let topo = draw_topology(&mut rng, &format!("budget-{seed}"));
    // A healthy fabric: no loss, but jittered, so links reorder.
    let latency = 2 + u64::from(rng.below(u32::try_from(MAX_LATENCY_MS).expect("small") - 1));
    let mut sim = Simulation::new(latency);
    sim.set_jitter(u64::from(
        rng.below(u32::try_from(MAX_JITTER_MS).expect("small") + 1),
    ));
    topo.boot(&mut sim);

    // Converge first: the budget bounds *failover*, not bootstrap. The crash
    // instant is drawn too, so the schedule samples its phase against each
    // survivor's probe cursor and the incumbent's renewal round.
    let crash_at = 3_000 + u64::from(rng.below(500));
    sim.run_until(Time(crash_at));
    let label = format!("Q-budget seed {seed}");
    let incumbent = sole_host(&sim, &format!("{label} (before the crash)"));

    let survivors: BTreeSet<NodeId> = topo
        .booted
        .iter()
        .filter(|x| **x != incumbent)
        .cloned()
        .collect();
    let (epoch, _) = pair_of(&sim, survivors.iter().next().expect("a survivor"));
    sim.crash(&incumbent);
    if !topo.has_majority(&survivors) {
        // The crash took the roster majority with it. The CP answer is no host
        // at all, for ever — asserted here rather than skipped.
        sim.run_until(Time(crash_at + 20_000));
        assert!(
            sim.hosts().is_empty(),
            "{label}: the survivors hold {} of {} voters, below the majority of {}, \
             yet elected {:?}",
            topo.voters_in(&survivors),
            topo.voters.len(),
            topo.majority,
            sim.hosts()
        );
        return;
    }

    let expected =
        placement::owner(topo.group.as_str(), &survivors).expect("at least one survivor");
    let budget = topo
        .cfg()
        .detection_window_ms(topo.booted.len())
        .max(LEASE_MS)
        + 4 * HOP_MS
        + GOSSIP_INTERVAL_MS;
    sim.run_until(Time(crash_at + budget));

    let successor = sole_host(&sim, &format!("{label} (after {budget}ms)"));
    assert_eq!(
        successor, expected,
        "{label}: {successor} took the group {budget}ms after the crash, \
         not the survivors' rendezvous owner {expected}"
    );
    let (next, _) = pair_of(&sim, &successor);
    assert!(
        next > epoch,
        "{label}: {successor} activated epoch {next}, which does not fence epoch {epoch}"
    );
    for id in &survivors {
        assert_eq!(
            pair_of(&sim, id),
            (next, Some(successor.clone())),
            "{label}: {id} had not adopted the new pair within the {budget}ms budget"
        );
    }
    assert_lease_disjoint(&sim, crash_at + budget, &label);
    assert_grants_single_valued(&sim, &label);
    assert_sole_activator_per_epoch(&sim.leadership_log, &label);
}
