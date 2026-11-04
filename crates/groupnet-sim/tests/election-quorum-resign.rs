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
//!
//! A failing seed is a reproducible counterexample, not a flake.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;

use groupnet_core::{
    Activation, Command, Config, GroupEngine, GroupId, GroupMode, HostedConfig, NodeId, Time,
    VoterRoster,
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

/// Runs `sim` in [`STEP_MS`] steps from `now` until a node outside `gone`
/// hosts, checking S4c at every step. Returns the host and the instant.
fn next_host(
    sim: &mut Simulation,
    mut now: u64,
    within: u64,
    gone: &BTreeSet<NodeId>,
    label: &str,
) -> (NodeId, u64) {
    let until = now + within;
    loop {
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
        let (mut host, mut now) = next_host(&mut sim, 0, SETTLE_MS, &gone, &label);
        for _ in 0..2 {
            now += LEASE_MS + u64::from(rng.up_to(u32::try_from(LEASE_MS).expect("small")));
            sim.run_until(Time(now));
            assert_lease_disjoint(&sim, now, &label);
            // The host can have changed while the run went on.
            let Some(current) = sim.hosts().into_iter().find(|h| !gone.contains(h)) else {
                (host, now) = next_host(&mut sim, now, SETTLE_MS, &gone, &label);
                continue;
            };
            host = current;
            sim.command(&host, Command::Leave)
                .expect("the host is in the sim");
            gone.insert(host.clone());
            (host, now) = next_host(&mut sim, now, SETTLE_MS, &gone, &label);
        }
        assert!(!gone.contains(&host), "{label}: {host} left and hosts");
        assert_sole_activator_per_epoch(&sim, &label);
        assert_grants_single_valued(&sim, &label);
    }
}
