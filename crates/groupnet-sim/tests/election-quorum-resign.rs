//! Deterministic Simulation Testing for **row Q16, the resign of a leaving
//! host** under `Activation::Quorum` (napbat/groupnet#13).
//!
//! The clock is the simulation's virtual clock, so each instant below is
//! exact and no test waits on wall time.
//!
//! * **Q-RES-fast — [`dst_quorum_a_resign_elects_before_the_lease_ends`].**
//!   The host leaves. A successor hosts before the lease of the old host
//!   ends. Without the resign it cannot: each voter that renewed the old host
//!   refuses every other claimant until its promise ends, and that promise
//!   ends at or after the old host's lease.
//! * **Q-RES-fence — [`dst_quorum_resigns_keep_every_fence`].** Hosts leave
//!   one after another on a lossy, jittery fabric. At most one unexpired
//!   lease exists at any instant, no two nodes activate one epoch, no voter
//!   grants two claimants one epoch, and the remaining majority elects a host
//!   after each leave.
//! * **Q-RES-minority — [`dst_quorum_a_resign_that_misses_two_voters_waits_for_their_promise`].**
//!   The resign and each copy reach only two voters of five. The successor
//!   needs one of the two others, so it hosts only after the promise of one
//!   of them ends.
//! * **Q-RES-again — [`dst_quorum_a_lost_resign_goes_again`].** The first
//!   resign to two voters is lost. The node that left sends it again to the
//!   voters that did not answer (row Q16d), so the successor hosts before
//!   their promise ends.
//! * **Q-RES-restart — [`dst_quorum_a_voter_that_restarts_after_the_resign_keeps_its_blackout`].**
//!   A voter crashes right after it released its promise and restarts from
//!   its stored grant. It applies the boot blackout and never grants the
//!   resigned pair again.
//!
//! Each run also checks at every step that no node that left hosts again.
//!
//! A failing seed is a reproducible counterexample, not a flake.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;

use groupnet_core::{
    Activation, Command, Config, GroupEngine, GroupId, GroupMode, HostedConfig, NodeId,
    RecoveredGrant, Time, VoterRoster,
};
use groupnet_sim::{Percent, Simulation, SplitMix64};

/// The lease — under Quorum also the claim window, the boot guard and the
/// grant blackout.
const LEASE_MS: u64 = 400;
/// The anti-entropy / gossip cadence, which is also the renewal cadence.
const GOSSIP_INTERVAL_MS: u64 = 60;
/// The poll step of the runs, in virtual milliseconds.
const STEP_MS: u64 = 5;
/// How long a healthy cluster may take to elect its first host.
const FIRST_HOST_MS: u64 = 4 * LEASE_MS;
/// How long Q-RES-fence waits for a host after a leave. A lost resign falls
/// back to the end of the promise, so this is several leases.
const SETTLE_MS: u64 = 20_000;

/// The election-suite timings, with a Quorum activation over `voters`.
fn cfg(voters: &[NodeId]) -> Config {
    Config {
        gossip_interval_ms: GOSSIP_INTERVAL_MS,
        probe_interval_ms: 50,
        probe_timeout_ms: 40,
        suspect_timeout_ms: 120,
        dead_timeout_ms: 1_000,
        max_delta_frame_bytes: NonZeroUsize::new(4_096).expect("nonzero"),
        mode: GroupMode::Hosted(HostedConfig {
            activation: Activation::Quorum {
                voters: VoterRoster::new(voters.iter().cloned()),
            },
            lease_ms: LEASE_MS,
        }),
        ..Config::default()
    }
}

/// Boots `size` voters named `v0..` in group `group`, each seeded with all
/// the others.
fn boot(sim: &mut Simulation, group: &str, size: usize) -> Vec<NodeId> {
    let voters: Vec<NodeId> = (0..size).map(|i| NodeId::new(format!("v{i}"))).collect();
    for voter in &voters {
        let seeds = voters.iter().filter(|v| *v != voter).cloned();
        sim.add(
            GroupEngine::new(GroupId::new(group), voter.clone(), seeds, cfg(&voters))
                .expect("valid config"),
        )
        .expect("distinct node");
    }
    voters
}

/// **S4c-global.** At most one node holds an unexpired lease at `now`.
fn assert_lease_disjoint(sim: &Simulation, now: u64, label: &str) {
    let leased: Vec<NodeId> = sim
        .nodes()
        .into_iter()
        .filter(|n| sim.lease_until_of(n).is_some_and(|until| until > Time(now)))
        .collect();
    assert!(
        leased.len() <= 1,
        "{label}: two leases at {now}: {leased:?}"
    );
}

/// **S1-strict.** No two nodes activated one epoch: an activation is the one
/// leadership transition a node logs that names itself.
fn assert_sole_activator_per_epoch(sim: &Simulation, label: &str) {
    let mut by_epoch: BTreeMap<u64, NodeId> = BTreeMap::new();
    for (observer, epoch, host) in &sim.leadership_log {
        if host.as_ref() != Some(observer) {
            continue;
        }
        if let Some(first) = by_epoch.insert(*epoch, observer.clone()) {
            assert_eq!(
                &first, observer,
                "{label}: epoch {epoch} was activated by both {first} and {observer}"
            );
        }
    }
}

/// One grant per epoch per voter, read off every grant the run issued.
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

/// Panics if a node in `gone` hosts at `now`: a node that left never hosts
/// again.
fn assert_gone_never_host(sim: &Simulation, now: u64, gone: &BTreeSet<NodeId>, label: &str) {
    let back: Vec<NodeId> = sim
        .hosts()
        .into_iter()
        .filter(|h| gone.contains(h))
        .collect();
    assert!(back.is_empty(), "{label}: {back:?} left and host at {now}");
}

/// Runs `sim` in [`STEP_MS`] steps from `from` to `to`, checking S4c and
/// that no node in `gone` hosts at every step.
fn run_checked(sim: &mut Simulation, from: u64, to: u64, gone: &BTreeSet<NodeId>, label: &str) {
    let mut now = from;
    while now < to {
        now = (now + STEP_MS).min(to);
        sim.run_until(Time(now));
        assert_lease_disjoint(sim, now, label);
        assert_gone_never_host(sim, now, gone, label);
    }
}

/// Runs `sim` in [`STEP_MS`] steps from `now` until a node outside `gone`
/// hosts, checking S4c and that no node in `gone` hosts at every step.
/// Returns the host and the instant.
fn next_host(
    sim: &mut Simulation,
    mut now: u64,
    within: u64,
    gone: &BTreeSet<NodeId>,
    label: &str,
) -> (NodeId, u64) {
    let until = now + within;
    loop {
        assert_gone_never_host(sim, now, gone, label);
        if let Some(host) = sim.hosts().into_iter().find(|h| !gone.contains(h)) {
            return (host, now);
        }
        assert!(now < until, "{label}: no host by {until}");
        now += STEP_MS;
        sim.run_until(Time(now));
        assert_lease_disjoint(sim, now, label);
    }
}

/// **Q-RES-fast.** A host that leaves resigns, and a successor hosts before
/// the old host's lease would have ended. Rosters of 3 and 5, over seeds that
/// vary the ranking, the latency and the instant of the leave.
#[test]
fn dst_quorum_a_resign_elects_before_the_lease_ends() {
    for seed in 0..32u64 {
        let mut rng = SplitMix64::new(seed ^ 0x9e37_79b9_7f4a_7c15);
        let size = if seed % 2 == 0 { 3 } else { 5 };
        let label = format!("Q-RES-fast seed {seed}");
        let mut sim = Simulation::new(1 + u64::from(rng.up_to(5)));
        boot(&mut sim, &format!("resign-{seed}"), size);

        let (host, mut now) = next_host(&mut sim, 0, FIRST_HOST_MS, &BTreeSet::new(), &label);
        // Let the host renew for a while, then leave at a drawn instant.
        now += 3 * LEASE_MS + u64::from(rng.up_to(u32::try_from(LEASE_MS).expect("small")));
        sim.run_until(Time(now));
        assert!(
            sim.hosts() == vec![host.clone()],
            "{label}: {host} still hosts"
        );
        let lease_end = sim.lease_until_of(&host).expect("a host has a lease");
        let (old_epoch, _) = sim.leadership_of(&host).expect("a live node");

        sim.command(&host, Command::Leave)
            .expect("the host is in the sim");
        assert!(
            sim.lease_until_of(&host).is_none(),
            "{label}: the leave demotes at once"
        );
        let gone = BTreeSet::from([host.clone()]);
        let (successor, at) = next_host(&mut sim, now, LEASE_MS, &gone, &label);

        assert!(
            Time(at) < lease_end,
            "{label}: {successor} hosts at {at}, not before the lease of {host} ends \
             at {lease_end:?}"
        );
        let (epoch, _) = sim.leadership_of(&successor).expect("a live node");
        assert!(
            epoch > old_epoch,
            "{label}: the successor bid above {old_epoch}"
        );
        assert!(sim.resign_frames_seen() > 0, "{label}: no resign was sent");
        assert_sole_activator_per_epoch(&sim, &label);
        assert_grants_single_valued(&sim, &label);
    }
}

/// **Q-RES-fence.** Two hosts leave one after the other on a 5-voter roster,
/// with loss and jitter, so some resigns are lost and some arrive late. The
/// fences hold at every step and the remaining majority elects a host after
/// each leave.
#[test]
fn dst_quorum_resigns_keep_every_fence() {
    for seed in 0..32u64 {
        let mut rng = SplitMix64::new(seed ^ 0x5e51_6e00);
        let label = format!("Q-RES-fence seed {seed}");
        let mut sim = Simulation::new(u64::from(2 + rng.up_to(6)));
        sim.set_loss(Percent::try_from(rng.up_to(15)).expect("at most 15%"));
        sim.set_jitter(u64::from(rng.up_to(8)));
        boot(&mut sim, &format!("fence-{seed}"), 5);

        let mut gone = BTreeSet::new();
        let (_, mut now) = next_host(&mut sim, 0, SETTLE_MS, &gone, &label);
        for _ in 0..2 {
            let to = now + LEASE_MS + u64::from(rng.up_to(u32::try_from(LEASE_MS).expect("small")));
            run_checked(&mut sim, now, to, &gone, &label);
            now = to;
            // The host can have changed while the run went on.
            let Some(host) = sim.hosts().into_iter().find(|h| !gone.contains(h)) else {
                (_, now) = next_host(&mut sim, now, SETTLE_MS, &gone, &label);
                continue;
            };
            sim.command(&host, Command::Leave)
                .expect("the host is in the sim");
            gone.insert(host);
            (_, now) = next_host(&mut sim, now, SETTLE_MS, &gone, &label);
        }
        // The nodes that left stay out while the run goes on.
        run_checked(&mut sim, now, now + 4 * LEASE_MS, &gone, &label);
        assert_sole_activator_per_epoch(&sim, &label);
        assert_grants_single_valued(&sim, &label);
    }
}

/// The link latency of the two shaped cases below.
const LATENCY_MS: u64 = 5;

/// The host of a fresh cluster of `size` voters, after it renewed for a few
/// leases: the cluster, the voters, the host, its epoch, and the instant.
fn settled(group: &str, size: usize, label: &str) -> (Simulation, Vec<NodeId>, NodeId, u64, u64) {
    let mut sim = Simulation::new(LATENCY_MS);
    let voters = boot(&mut sim, group, size);
    let none = BTreeSet::new();
    let (host, at) = next_host(&mut sim, 0, FIRST_HOST_MS, &none, label);
    let now = at + 3 * LEASE_MS;
    run_checked(&mut sim, at, now, &none, label);
    assert_eq!(sim.hosts(), vec![host.clone()], "{label}: the host stays");
    let (epoch, _) = sim.leadership_of(&host).expect("a live node");
    (sim, voters, host, epoch, now)
}

/// A cluster of five voters whose host left while the links from the host
/// to two other voters dropped every frame. The links heal two latencies
/// after the leave when `heal` is set, and stay cut otherwise. Returns the
/// cluster, the host that left, its successor, the instant it hosts, and the
/// earliest end of the promise of the two cut voters to the old host.
fn missed_resign(label: &str, heal: bool) -> (Simulation, NodeId, NodeId, u64, Time) {
    let (mut sim, voters, host, epoch, now) = settled(label, 5, label);
    let missed: Vec<NodeId> = voters
        .iter()
        .filter(|v| **v != host)
        .take(2)
        .cloned()
        .collect();
    for voter in &missed {
        sim.block(&host, voter);
    }
    let resigns = sim.resign_frames_seen();
    sim.command(&host, Command::Leave)
        .expect("the host is in the sim");
    assert_eq!(
        sim.resign_frames_seen() - resigns,
        4,
        "{label}: one resign to each other voter"
    );
    let gone = BTreeSet::from([host.clone()]);
    let healed = now + 2 * LATENCY_MS;
    run_checked(&mut sim, now, healed, &gone, label);
    if heal {
        sim.heal_all();
    }

    // A voter's promise ends one lease after its last grant to the host.
    let promise_end = |voter: &NodeId| {
        sim.grant_log
            .iter()
            .filter(|(_, granter, granted, claimant)| {
                granter == voter && *granted == epoch && *claimant == host
            })
            .map(|(at, ..)| *at)
            .max()
            .expect("the voter renewed the host")
            .saturating_add(LEASE_MS)
    };
    let earliest = missed.iter().map(promise_end).min().expect("two voters");
    let (successor, at) = next_host(&mut sim, healed, SETTLE_MS, &gone, label);
    sim.heal_all();
    run_checked(&mut sim, at, at + 2 * LEASE_MS, &gone, label);
    assert_sole_activator_per_epoch(&sim, label);
    assert_grants_single_valued(&sim, label);
    (sim, host, successor, at, earliest)
}

/// **Q-RES-minority.** The resign, and each copy of it, reaches only two
/// voters of five: the links from the host to the two others stay cut. A
/// successor needs a third grant, and each of the two others refuses every
/// other claimant until its promise to the old host ends. So the successor
/// hosts only after the earliest of those two promises ends, and the fences
/// hold.
#[test]
fn dst_quorum_a_resign_that_misses_two_voters_waits_for_their_promise() {
    let label = "Q-RES-minority";
    let (_, _, successor, at, earliest) = missed_resign(label, false);
    assert!(
        Time(at) >= earliest,
        "{label}: {successor} hosts at {at}, before the promise of the cut voters ends \
         at {earliest:?}"
    );
}

/// **Q-RES-again.** The links to two voters drop the first resign and then
/// heal (as a resign that the stop of the node dropped from its queue). The
/// node that left sends the resign again to the voters that did not answer
/// (row Q16d), so the successor hosts before the promise of those voters
/// ends, and the node that left ends with no voter pending.
#[test]
fn dst_quorum_a_lost_resign_goes_again() {
    let label = "Q-RES-again";
    let (sim, host, successor, at, earliest) = missed_resign(label, true);
    assert!(
        Time(at) < earliest,
        "{label}: {successor} hosts at {at}, not before the promise of the cut voters \
         ends at {earliest:?}"
    );
    assert_eq!(
        sim.resign_pending_of(&host),
        Some(0),
        "{label}: every voter answered"
    );
}

/// **Q-RES-restart.** A voter crashes at the instant it released its promise
/// and restarts from its stored grant, the resigned pair. Nothing about the
/// release is stored, so the restarted voter applies the boot blackout: the
/// successor, which needs its grant, hosts no earlier than one lease after
/// the restart. The restarted voter never grants the resigned pair again, and
/// the fences hold.
#[test]
fn dst_quorum_a_voter_that_restarts_after_the_resign_keeps_its_blackout() {
    let label = "Q-RES-restart";
    let group = "resign-restart";
    let (mut sim, voters, host, epoch, now) = settled(group, 3, label);
    let voter = voters
        .iter()
        .find(|v| **v != host)
        .expect("another voter")
        .clone();
    sim.command(&host, Command::Leave)
        .expect("the host is in the sim");
    let gone = BTreeSet::from([host.clone()]);

    // One event at a time, until the voter took the resign.
    let released = loop {
        let at = sim
            .step_until(Time(now + LEASE_MS))
            .expect("the resign arrives within a lease");
        assert_lease_disjoint(&sim, at.0, label);
        assert_gone_never_host(&sim, at.0, &gone, label);
        if sim.leadership_of(&voter) == Some((epoch, None)) {
            break at;
        }
    };
    assert_eq!(
        sim.persisted_grant_of(&voter),
        Some((epoch, host.clone())),
        "{label}: the stored grant is the resigned pair"
    );
    // The grants that the voter issued before the crash, also a renewal of
    // the old host in the same instant as the resign, stay out of the check.
    let restarted = sim.grant_log.len();
    sim.crash(&voter).expect("the voter runs");
    let seeds = voters.iter().filter(|v| **v != voter).cloned();
    let engine = GroupEngine::recovering(GroupId::new(group), voter.clone(), seeds, cfg(&voters))
        .expect("valid config")
        .restore(RecoveredGrant::granted(epoch, host.clone()));
    sim.add(engine).expect("the voter restarts");

    let (successor, at) = next_host(&mut sim, released.0, SETTLE_MS, &gone, label);
    assert!(
        Time(at) >= released.saturating_add(LEASE_MS),
        "{label}: {successor} hosts at {at}, inside the blackout of {voter} that \
         restarted at {released:?}"
    );
    run_checked(&mut sim, at, at + 2 * LEASE_MS, &gone, label);
    assert!(
        !sim.grant_log[restarted..]
            .iter()
            .any(|(_, granter, granted, claimant)| {
                *granter == voter && *granted == epoch && *claimant == host
            }),
        "{label}: {voter} granted the resigned pair again after its restart"
    );
    assert_sole_activator_per_epoch(&sim, label);
    assert_grants_single_valued(&sim, label);
}
