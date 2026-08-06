//! Deterministic Simulation Testing for **voter durability** under
//! `Activation::Quorum` — what a restarting voter is allowed to forget, and
//! what it must not.
//!
//! Two suites run the *same* restart-heavy fault schedule, seed for seed, and
//! differ in exactly one line: how a rebooting engine is built.
//!
//! * **Q-DUR-blackout —
//!   [`dst_quorum_blackout_survives_amnesiac_voter_restarts`].** Every restart
//!   is a [`GroupEngine::new`]: incarnation 0, epoch 0, and an **empty grant
//!   ledger**. All that stands between that and two hosts is the boot
//!   blackout — a freshly started voter refuses every *new* claimant for one
//!   `lease_ms` measured from boot. This suite is the proof that the timing
//!   rule really does stand in for the durability one, in exact virtual time.
//! * **Q-DUR-recovered —
//!   [`dst_quorum_recovered_grants_restore_global_epoch_uniqueness`].** Every
//!   restart is a [`GroupEngine::with_recovered`] fed the pair the simulation's
//!   stand-in store wrote from [`Effect::PersistGrant`]. That buys back the two
//!   things the blackout cannot: an epoch is globally unique again, and the
//!   sitting host does not have to wait out a lease to be re-granted.
//!
//! # What each suite may claim, and what it must not
//!
//! | property | blackout | recovered |
//! |---|---|---|
//! | S4c-global (≤1 unexpired lease, cluster-wide, across partitions) | ✓ | ✓ |
//! | one claimant per `(granter, epoch)` **per lifetime** | ✓ | ✓ |
//! | one claimant per `(granter, epoch)` **across lifetimes** | ✗ | ✓ |
//! | per-granter epoch monotonicity across lifetimes | ✗ | ✓ |
//! | S1-strict global epoch uniqueness (= E4-impossibility) | ✗ | ✓ |
//!
//! The blackout suite does **not** assert the rows marked ✗, and it does not
//! quietly hope they hold either: it asserts positively that cross-lifetime
//! epoch monotonicity *does* break somewhere across its seeds, which is what
//! keeps the recovered suite's version of that row from being a tautology
//! nobody could have failed.
//!
//! Both boot blackouts and recovered ledgers are conservative about *time*: the
//! store records what was granted, never when, so a recovered voter still
//! blacks out new claimants from boot. What recovery changes is the ledger
//! floor and the incumbent's exemption from it.
//!
//! A failing seed is a reproducible counterexample, not a flake.
//!
//! [`Effect::PersistGrant`]: groupnet_core::Effect::PersistGrant

use std::collections::{BTreeMap, BTreeSet};

use groupnet_core::{
    Activation, Command, Config, GroupEngine, GroupId, GroupMode, HostedConfig, NodeId,
    RecoveredGrant, Time, VoterRoster,
};
use groupnet_sim::{Simulation, SplitMix64};

/// How long a host's authority survives its last successful renewal — and,
/// under Quorum, the claim window, the boot guard and the grant blackout too.
const LEASE_MS: u64 = 400;
/// The anti-entropy / gossip cadence, which is also the renewal-round cadence.
const GOSSIP_INTERVAL_MS: u64 = 60;
/// How many rounds of chaos each seed runs before the final settle.
const ROUNDS: usize = 40;
/// How long the healed, loss-free fabric is given to quiesce.
const SETTLE_MS: u64 = 20_000;

/// The election-suite timings, with a Quorum activation over `voters`.
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

/// Seeds the shared deterministic PRNG so each schedule is reproducible.
fn rng(seed: u64) -> SplitMix64 {
    SplitMix64::new(seed ^ 0x9e37_79b9_7f4a_7c15)
}

fn pick(set: &BTreeSet<NodeId>, rng: &mut SplitMix64) -> NodeId {
    let v: Vec<&NodeId> = set.iter().collect();
    let n = u32::try_from(v.len()).expect("these clusters are a handful of nodes");
    let i = usize::try_from(rng.below(n)).expect("bounded by the set size");
    v[i].clone()
}

/// One seed's cluster: the engines that boot, and the static roster whose
/// majority closes an epoch. A roster may name a node that never boots.
#[derive(Debug)]
struct Topology {
    group: GroupId,
    booted: BTreeSet<NodeId>,
    voters: BTreeSet<NodeId>,
}

/// Draws 3..=6 nodes and a roster of **3 or 5** over them — sometimes all of
/// the cluster, sometimes a strict subset, and one seed in four a five-voter
/// roster whose fifth member never boots at all.
fn draw_topology(rng: &mut SplitMix64, name: &str) -> Topology {
    let n = 3 + usize::try_from(rng.below(4)).expect("a 0..4 draw");
    let ids: Vec<NodeId> = (0..n).map(|i| NodeId::new(format!("n{i}"))).collect();
    let booted: BTreeSet<NodeId> = ids.iter().cloned().collect();

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
    Topology {
        group: GroupId::new(name),
        booted,
        voters,
    }
}

/// How a rebooting engine is built — the one line the two suites differ in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Durability {
    /// [`GroupEngine::new`]: no store, so nothing but the boot blackout.
    Blackout,
    /// [`GroupEngine::with_recovered`], fed whatever the sim's stand-in store
    /// holds for the node — [`RecoveredGrant::none`] when it holds nothing,
    /// which is storage *attesting* the voter never granted rather than a
    /// driver guessing it.
    Recovered,
}

/// One restart: the node, the instant its fresh engine was started, and what
/// storage had for it at that instant.
#[derive(Debug)]
struct Reboot {
    node: NodeId,
    at: Time,
    recovered: Option<(u64, NodeId)>,
}

/// Everything one seed produced.
struct Run {
    topo: Topology,
    sim: Simulation,
    reboots: Vec<Reboot>,
    /// Restart instants per node, for the per-lifetime grant fold.
    restarts: BTreeMap<NodeId, Vec<Time>>,
    label: String,
}

/// Drives one seed's fault schedule. The draws are identical under both
/// [`Durability`] modes — only the constructor on the restart arm changes — so
/// the two suites are comparing the same schedule rather than two of them.
///
/// **S4c-global is asserted here**, every round, for both modes: it is the one
/// property that must hold whichever way a voter comes back, and asserting it
/// inside the shared driver is what makes that literal.
fn durability_run(seed: u64, mode: Durability) -> Run {
    let mut rng = rng(seed ^ 0xd0b1);
    let topo = draw_topology(&mut rng, &format!("durable-{seed}"));
    let label = format!(
        "{} seed {seed}",
        match mode {
            Durability::Blackout => "Q-DUR-blackout",
            Durability::Recovered => "Q-DUR-recovered",
        }
    );

    let mut sim = Simulation::new(u64::from(3 + rng.below(8)));
    sim.set_loss(u8::try_from(rng.below(20)).expect("below(20) is 0..20"));
    sim.set_jitter(u64::from(rng.below(9)));
    let mut alive: BTreeSet<NodeId> = topo.booted.clone();
    for id in &topo.booted {
        let seeds = topo.booted.iter().filter(|x| *x != id).cloned();
        sim.add(GroupEngine::new(
            topo.group.clone(),
            id.clone(),
            seeds,
            cfg(&topo.voters),
        ));
    }

    let mut reboots = Vec::new();
    let mut restarts: BTreeMap<NodeId, Vec<Time>> = BTreeMap::new();
    let mut now = 0u64;
    for _round in 0..ROUNDS {
        now += u64::from(30 + rng.below(120));
        sim.run_until(Time(now));
        assert_lease_disjoint(&sim, now, &label);

        match rng.below(7) {
            0 | 1 if alive.len() > 1 => {
                let victim = pick(&alive, &mut rng);
                sim.crash(&victim);
                alive.remove(&victim);
            }
            2 | 3 if alive.len() < topo.booted.len() => {
                let down: BTreeSet<NodeId> = topo
                    .booted
                    .iter()
                    .filter(|x| !alive.contains(*x))
                    .cloned()
                    .collect();
                let node = pick(&down, &mut rng);
                alive.insert(node.clone());
                let recovered = sim.persisted_grant_of(&node);
                sim.add(reboot(&topo, &node, &alive, mode, recovered.clone()));
                restarts.entry(node.clone()).or_default().push(Time(now));
                reboots.push(Reboot {
                    node,
                    at: Time(now),
                    recovered,
                });
            }
            4 if alive.len() > 1 => {
                let a = pick(&alive, &mut rng);
                let b = pick(&alive, &mut rng);
                if a != b {
                    sim.block(&a, &b);
                    sim.block(&b, &a);
                }
            }
            5 => sim.heal_all(),
            _ => {
                let node = pick(&alive, &mut rng);
                sim.command(
                    &node,
                    Command::SetLocalEntry {
                        key: "kv".into(),
                        value: format!("v{now}").into_bytes(),
                        ttl_ms: None,
                    },
                );
            }
        }
    }

    sim.heal_all();
    sim.set_loss(0);
    sim.set_jitter(0);
    now += SETTLE_MS;
    sim.run_until(Time(now));
    assert_lease_disjoint(&sim, now, &label);

    Run {
        topo,
        sim,
        reboots,
        restarts,
        label,
    }
}

/// The one line the two suites differ in.
fn reboot(
    topo: &Topology,
    node: &NodeId,
    peers: &BTreeSet<NodeId>,
    mode: Durability,
    recovered: Option<(u64, NodeId)>,
) -> GroupEngine {
    let seeds: Vec<NodeId> = peers.iter().filter(|x| *x != node).cloned().collect();
    let config = cfg(&topo.voters);
    match mode {
        Durability::Blackout => GroupEngine::new(topo.group.clone(), node.clone(), seeds, config),
        Durability::Recovered => GroupEngine::with_recovered(
            topo.group.clone(),
            node.clone(),
            seeds,
            config,
            match recovered {
                None => RecoveredGrant::none(),
                Some((epoch, claimant)) => RecoveredGrant::granted(epoch, claimant),
            },
        ),
    }
}

// ---------------------------------------------------------------------------
// Shared probes.
// ---------------------------------------------------------------------------

/// **S4c-global.** Every node in the simulation holding an unexpired lease at
/// `now` — *across partitions*, since `Simulation::nodes` sees every engine
/// whatever the network is doing to it. Exact in virtual time: `lease_until_of`
/// is `Some` only for a node currently playing host, and the instant it returns
/// is the deadline that armed that node's driver timer.
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

/// How many times `granter` had restarted by `at`.
fn lifetime(restarts: &BTreeMap<NodeId, Vec<Time>>, granter: &NodeId, at: Time) -> usize {
    restarts
        .get(granter)
        .map_or(0, |ats| ats.iter().filter(|t| **t <= at).count())
}

/// One claimant per `(granter, epoch)`, scoped to a voter lifetime when
/// `restarts` is populated and to the whole run when it is empty.
fn assert_grants_single_valued(run: &Run, restarts: &BTreeMap<NodeId, Vec<Time>>, scope: &str) {
    let mut seen: BTreeMap<(NodeId, usize, u64), NodeId> = BTreeMap::new();
    for (at, granter, epoch, claimant) in &run.sim.grant_log {
        let key = (granter.clone(), lifetime(restarts, granter, *at), *epoch);
        if let Some(first) = seen.insert(key, claimant.clone()) {
            assert_eq!(
                &first, claimant,
                "{}: {granter} granted epoch {epoch} to both {first} and {claimant} \
                 ({scope}; second at {at:?})",
                run.label
            );
        }
    }
}

/// **Row Q0**, folded over the whole run: only a roster member ever granted.
/// A node outside the roster never grants, whatever it is asked and however it
/// rebooted — and neither does the roster member that never booted at all.
fn assert_only_voters_granted(run: &Run) {
    for (at, granter, epoch, claimant) in &run.sim.grant_log {
        assert!(
            run.topo.voters.contains(granter),
            "{}: {granter} granted ({epoch}, {claimant}) at {at:?} without being in the roster",
            run.label
        );
    }
}

/// **S1-strict / E4-impossibility.** No epoch was ever activated by two
/// distinct nodes, over the whole run and both sides of every partition.
fn assert_sole_activator_per_epoch(run: &Run) {
    let mut by_epoch: BTreeMap<u64, NodeId> = BTreeMap::new();
    for (observer, epoch, host) in &run.sim.leadership_log {
        if host.as_ref() != Some(observer) {
            continue; // not an activation — an adoption or a step down
        }
        if let Some(first) = by_epoch.insert(*epoch, observer.clone()) {
            assert_eq!(
                &first, observer,
                "{}: epoch {epoch} was activated by both {first} and {observer}",
                run.label
            );
        }
    }
}

/// How many grants in the run name an epoch **below** one the same granter had
/// already issued. Zero is the durable posture; a positive count is amnesia
/// showing, and is what [`Durability::Blackout`] is entitled to.
fn epoch_regressions(run: &Run) -> usize {
    let mut high: BTreeMap<NodeId, u64> = BTreeMap::new();
    let mut regressions = 0;
    for (_, granter, epoch, _) in &run.sim.grant_log {
        let seen = high.entry(granter.clone()).or_insert(*epoch);
        if epoch < seen {
            regressions += 1;
        } else {
            *seen = *epoch;
        }
    }
    regressions
}

/// Restarts whose **first** grant back is the incumbent named in the recovered
/// pair, issued *inside* the blackout window a storage-free voter would have
/// been serving out. This is the liveness recovery buys — see
/// [`dst_quorum_recovered_grants_restore_global_epoch_uniqueness`].
fn fast_regrants(run: &Run) -> usize {
    run.reboots
        .iter()
        .filter(|reboot| {
            let Some((epoch, claimant)) = reboot.recovered.as_ref() else {
                return false;
            };
            run.sim
                .grant_log
                .iter()
                .find(|(at, granter, _, _)| granter == &reboot.node && *at >= reboot.at)
                .is_some_and(|(at, _, granted, to)| {
                    to == claimant && granted >= epoch && at.0 < reboot.at.0 + LEASE_MS
                })
        })
        .count()
}

// ---------------------------------------------------------------------------
// Q-DUR-blackout — the timing rule standing in for the durability one.
// ---------------------------------------------------------------------------

/// **Q-DUR-blackout.** Voters crash and come back **amnesiac** — a fresh
/// engine, an empty ledger, nothing but the boot blackout — and the tier's
/// global safety property survives it.
///
/// * **S4c-global, every round** (in [`durability_run`]): never two unexpired
///   leases anywhere in the cluster. *This is the proof that the blackout
///   suffices.* The argument it checks: a voter that granted at `t` promised
///   until `t + lease_ms`; it crashes at some `c ≥ t` and boots at some
///   `b ≥ c`, arming a fresh blackout to `b + lease_ms ≥ t + lease_ms`. The
///   forgotten promise is therefore *covered* by the one the reboot arms, and
///   two majorities of one roster always intersect.
/// * **Every post-restart grant is stamped at or after
///   `restart + LEASE_MS`** — the blackout observed directly, in exact virtual
///   time, rather than inferred from its consequences. It holds for *every*
///   grant of the lifetime and not merely the first, because a fresh ledger is
///   empty, so the blackout is the only gate the first grant can pass, and
///   every later one is later still.
/// * **One claimant per `(granter, epoch)` per lifetime** — the strongest form
///   of the ledger rule an amnesiac voter can honour.
///
/// Deliberately **not** asserted: S1-strict. A voter with no store may re-grant
/// an epoch number it already spent in a previous life, and if enough of a
/// roster does so at once an epoch can be closed twice. Instead of pretending
/// otherwise, the suite pins the *mechanism* of that hole positively — see the
/// aggregate assertion at the end.
#[test]
fn dst_quorum_blackout_survives_amnesiac_voter_restarts() {
    let mut regressions = 0;
    let mut early = 0;
    let mut reboots = 0;
    for seed in 0..64u64 {
        let run = durability_run(seed, Durability::Blackout);
        assert_only_voters_granted(&run);
        assert_blackout_covers_every_grant(&run);
        assert_grants_single_valued(&run, &run.restarts, "within one lifetime");
        regressions += epoch_regressions(&run);
        early += fast_regrants(&run);
        reboots += run.reboots.len();
    }

    assert!(reboots > 0, "the schedule never restarted anything");
    // The hole the matrix admits, pinned as a fact rather than a caveat: an
    // amnesiac voter really does grant epochs below ones it has already
    // granted. Without this, the recovered suite's monotonicity assertion
    // would be a statement nobody could have failed.
    assert!(
        regressions > 0,
        "no amnesiac voter ever re-granted a spent epoch across {reboots} restarts — \
         either the schedule stopped exercising restarts, or the ledger is surviving \
         them, in which case Q-DUR-recovered is now asserting nothing"
    );
    // And the contrast the recovered suite is measured against: with no store,
    // a rebooted voter is of no use to the sitting host for a whole lease.
    assert_eq!(
        early, 0,
        "a storage-free voter re-granted an incumbent inside its own boot blackout"
    );
}

/// The blackout, read off the grant log: for every grant, the most recent
/// restart of its granter is at least one `LEASE_MS` earlier.
fn assert_blackout_covers_every_grant(run: &Run) {
    for (at, granter, epoch, claimant) in &run.sim.grant_log {
        let Some(booted) = run
            .restarts
            .get(granter)
            .and_then(|ats| ats.iter().rfind(|t| **t <= *at))
        else {
            continue; // this granter has not restarted yet
        };
        assert!(
            at.0 >= booted.0 + LEASE_MS,
            "{}: {granter} granted ({epoch}, {claimant}) at {at:?}, only {}ms after \
             rebooting at {booted:?} — inside the {LEASE_MS}ms blackout",
            run.label,
            at.0 - booted.0
        );
    }
}

// ---------------------------------------------------------------------------
// Q-DUR-recovered — what the store buys back.
// ---------------------------------------------------------------------------

/// **Q-DUR-recovered.** The same schedule, with every restart replaying the
/// pair the simulation's stand-in store recorded from `Effect::PersistGrant`
/// into [`GroupEngine::with_recovered`]. Two things come back that the blackout
/// alone cannot give:
///
/// * **Global epoch uniqueness — S1-strict, and with it E4-impossibility.** No
///   epoch is ever activated by two distinct nodes, across the whole run and
///   both sides of every partition. The same-epoch collision that
///   `election_failover.rs` has to break a *tiebreak* for cannot arise: two
///   majorities of one roster intersect, and the voter in the intersection
///   remembers, across the reboot, that it already spent that epoch. Asserted
///   alongside its mechanism — **unconditioned** one-claimant-per
///   `(granter, epoch)`, folded over every lifetime as one — and its
///   consequence, **per-granter epoch monotonicity across lifetimes**: a
///   recovered ledger is a floor, so a voter never grants below where it left
///   off. Q-DUR-blackout proves that last row genuinely fails without a store.
/// * **Liveness: the incumbent is not made to wait.** The claimant named in a
///   recovered pair is exempt from the promise — a claimant may always advance
///   its own epoch — so a restarted voter re-grants the *sitting* host at once
///   instead of starving it for a lease. The aggregate assertion at the end
///   counts those, and Q-DUR-blackout asserts the same count is **zero**
///   without a store. That pair of assertions is the whole difference between
///   recovery and the blackout, stated in the only terms a test can see.
///
/// S4c-global holds here too, asserted in [`durability_run`] alongside the
/// blackout run's — recovery is strictly stronger, never a trade.
#[test]
fn dst_quorum_recovered_grants_restore_global_epoch_uniqueness() {
    let mut recovered_pairs = 0;
    let mut fast = 0;
    for seed in 0..64u64 {
        let run = durability_run(seed, Durability::Recovered);
        assert_only_voters_granted(&run);
        assert_sole_activator_per_epoch(&run);
        // Unconditioned: the ledger crosses every reboot, so the fold does too.
        assert_grants_single_valued(&run, &BTreeMap::new(), "across every lifetime");
        assert_eq!(
            epoch_regressions(&run),
            0,
            "{}: a recovered voter granted below the epoch its ledger restored",
            run.label
        );
        assert_persisted_floor(&run);
        recovered_pairs += run
            .reboots
            .iter()
            .filter(|reboot| reboot.recovered.is_some())
            .count();
        fast += fast_regrants(&run);
    }

    assert!(
        recovered_pairs > 0,
        "no restart ever had a pair to recover, so the suite proved nothing"
    );
    assert!(
        fast > 0,
        "no recovered voter re-granted its incumbent inside the blackout window \
         across {recovered_pairs} recovered restarts — recovery bought no liveness \
         over the storage-free posture"
    );
}

/// The recovered pair is a **floor**: after a restart that replayed
/// `(epoch, claimant)`, this voter never issues a grant below `epoch`, and
/// never names a different claimant at `epoch` itself.
fn assert_persisted_floor(run: &Run) {
    for reboot in &run.reboots {
        let Some((floor, claimant)) = reboot.recovered.as_ref() else {
            continue;
        };
        for (at, granter, epoch, to) in &run.sim.grant_log {
            if granter != &reboot.node || *at < reboot.at {
                continue;
            }
            assert!(
                epoch >= floor,
                "{}: {granter} recovered epoch {floor} at {:?} then granted {epoch} at {at:?}",
                run.label,
                reboot.at
            );
            assert!(
                epoch > floor || to == claimant,
                "{}: {granter} recovered ({floor}, {claimant}) then granted the same \
                 epoch to {to} at {at:?}",
                run.label
            );
        }
    }
}

/// The topology draw really does produce the three roster shapes the suites
/// claim to cover — all-of-cluster, strict-subset, and a five-voter roster
/// whose fifth member never boots. A silent drift here would leave every seeded
/// assertion above running on one shape.
#[test]
fn the_topology_draw_covers_every_roster_shape() {
    let (mut whole, mut subset, mut ghosted) = (0, 0, 0);
    for seed in 0..64u64 {
        let topo = draw_topology(&mut rng(seed ^ 0xd0b1), "shapes");
        let booted_voters = topo.voters.intersection(&topo.booted).count();
        assert!(
            matches!(topo.voters.len(), 3 | 5),
            "seed {seed}: a roster of {} is neither 3 nor 5",
            topo.voters.len()
        );
        if booted_voters < topo.voters.len() {
            ghosted += 1;
        } else if booted_voters == topo.booted.len() {
            whole += 1;
        } else {
            subset += 1;
        }
    }
    assert!(
        whole > 0 && subset > 0 && ghosted > 0,
        "{whole}/{subset}/{ghosted}"
    );
}
