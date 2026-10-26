//! Deterministic Simulation Testing for the first election after a restart of
//! **every** voter under `Activation::Quorum` (napbat/groupnet#8).
//!
//! Every voter of the roster boots at once from a recovered grant, as a
//! cluster does after a full outage. The recovered epochs are high, as they
//! are in a cluster that changed host often, and they differ from voter to
//! voter, as they do after a voter claimed alone without a majority. Before
//! the fix, a restored claimant bid from epoch 1, its own ledger and each
//! voter that granted higher refused each bid without an answer, and each
//! refused round cost a whole claim window: the election took about one lease
//! per epoch up to the highest grant.
//!
//! * **Q-RST-bound — [`dst_quorum_full_restart_elects_within_a_bounded_time`].**
//!   On a loss-free fabric, a host exists within [`ELECT_WITHIN_MS`] of the
//!   boot, whatever the recovered epochs. The restore raises the observed
//!   epoch to the recovered grant (the first claim bids above it), and a
//!   voter that refuses a claim at or below its grant answers with that grant
//!   (row Q3a), so the claimant bids above it at once (row Q5b).
//! * **Q-RST-fence — [`dst_quorum_full_restart_keeps_every_floor`].** The same
//!   restart with loss, jitter and partitions. Fencing does not weaken: at most
//!   one unexpired lease at any instant, no voter endorses a claim at or below
//!   its recovered grant (other than the recovered pair), and a majority of
//!   the roster can have granted the epoch of each host.
//!
//! A failing seed is a reproducible counterexample, not a flake.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;

use groupnet_core::{
    Activation, Config, GroupEngine, GroupId, GroupMode, HostedConfig, NodeId, RecoveredGrant,
    Time, VoterRoster,
};
use groupnet_sim::{Percent, Simulation, SplitMix64};

/// The lease — under Quorum also the claim window, the boot guard and the
/// grant blackout.
const LEASE_MS: u64 = 400;
/// The anti-entropy / gossip cadence.
const GOSSIP_INTERVAL_MS: u64 = 60;
/// The bound of Q-RST-bound: the boot guard and two claim windows. Before the
/// fix the first host came after about one window per recovered epoch, and
/// every recovered epoch here is at least [`FLOOR_MIN`].
const ELECT_WITHIN_MS: u64 = 3 * LEASE_MS;
/// The lowest recovered epoch a seed draws.
const FLOOR_MIN: u64 = 20;
/// How far above [`FLOOR_MIN`] a recovered epoch can be.
const FLOOR_SPREAD: u32 = 180;
/// How long Q-RST-fence runs its faults before the final heal.
const CHAOS_MS: u64 = 6_000;
/// How long the healed fabric of Q-RST-fence has to elect a host.
const SETTLE_MS: u64 = 20_000;
/// The poll step of the runs, in virtual milliseconds.
const STEP_MS: u64 = 10;

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

/// One seed's restart: the roster (every voter boots) and the grant that each
/// voter's store holds.
#[derive(Debug)]
struct Restart {
    group: GroupId,
    voters: Vec<NodeId>,
    floors: BTreeMap<NodeId, (u64, NodeId)>,
}

impl Restart {
    /// Draws a roster of 2, 3 or 5 voters and a recovered grant for each, at
    /// an epoch of at least [`FLOOR_MIN`] naming any voter of the roster.
    fn draw(seed: u64, name: &str) -> Self {
        let mut rng = SplitMix64::new(seed ^ 0x9e37_79b9_7f4a_7c15);
        let size = [2usize, 3, 5][usize::try_from(rng.up_to(2)).expect("a 0..=2 draw")];
        let voters: Vec<NodeId> = (0..size).map(|i| NodeId::new(format!("v{i}"))).collect();
        let last = u32::try_from(size - 1).expect("a handful of voters");
        let floors = voters
            .iter()
            .map(|voter| {
                let epoch = FLOOR_MIN + u64::from(rng.up_to(FLOOR_SPREAD));
                let claimant = voters[usize::try_from(rng.up_to(last)).expect("bounded")].clone();
                (voter.clone(), (epoch, claimant))
            })
            .collect();
        Self {
            group: GroupId::new(format!("{name}-{seed}")),
            voters,
            floors,
        }
    }

    /// Boots every voter at once on `sim`, each restored with its grant.
    fn boot(&self, sim: &mut Simulation) {
        for voter in &self.voters {
            let seeds = self.voters.iter().filter(|v| *v != voter).cloned();
            let (epoch, claimant) = self.floors[voter].clone();
            let engine = GroupEngine::recovering(
                self.group.clone(),
                voter.clone(),
                seeds,
                cfg(&self.voters),
            )
            .expect("valid config")
            .restore(RecoveredGrant::granted(epoch, claimant));
            sim.add(engine).expect("distinct node");
        }
    }

    /// How many voters can have granted `epoch` to `host`: those whose
    /// recovered grant is below `epoch`, and the one whose recovered pair is
    /// `(epoch, host)` itself (row Q2 re-grants it).
    fn can_grant(&self, epoch: u64, host: &NodeId) -> usize {
        self.floors
            .values()
            .filter(|(floor, claimant)| *floor < epoch || (*floor == epoch && claimant == host))
            .count()
    }

    fn majority(&self) -> usize {
        self.voters.len() / 2 + 1
    }
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

/// The fence of each host the run has seen: a majority of the roster can have
/// granted its epoch to it — each of them recovered a grant below that epoch,
/// or recovered that very pair.
fn assert_host_fenced(sim: &Simulation, restart: &Restart, label: &str) {
    for host in sim.hosts() {
        let (epoch, _) = sim.leadership_of(&host).expect("a live host");
        assert!(
            restart.can_grant(epoch, &host) >= restart.majority(),
            "{label}: {host} hosts epoch {epoch}, which too few voters can have granted \
             it: {:?}",
            restart.floors
        );
    }
}

/// No voter endorsed a claim at or below its recovered grant, other than the
/// recovered pair itself, and no voter's store went below its recovered grant.
fn assert_floors_kept(sim: &Simulation, restart: &Restart, label: &str) {
    for (at, granter, epoch, claimant) in &sim.grant_log {
        let (floor, recovered) = &restart.floors[granter];
        assert!(
            *epoch > *floor || (epoch == floor && claimant == recovered),
            "{label}: {granter} recovered ({floor}, {recovered}) and granted \
             ({epoch}, {claimant}) at {at:?}"
        );
    }
    for voter in &restart.voters {
        let (floor, recovered) = &restart.floors[voter];
        let (epoch, claimant) = sim
            .persisted_grant_of(voter)
            .unwrap_or_else(|| (*floor, recovered.clone()));
        assert!(
            epoch > *floor || (epoch == *floor && claimant == *recovered),
            "{label}: {voter} recovered ({floor}, {recovered}) and its store went to \
             ({epoch}, {claimant})"
        );
    }
}

/// **Q-RST-bound.** After a restart of every voter on a loss-free fabric, a
/// host exists within [`ELECT_WITHIN_MS`] of the boot, whatever epoch the
/// cluster had reached and however far apart the recovered grants are. The
/// suite also proves that the floor answer of row Q3a was exercised: with
/// recovered grants this far apart, a silent refusal is the only other way a
/// seed could pass.
#[test]
fn dst_quorum_full_restart_elects_within_a_bounded_time() {
    let mut floor_answers = 0;
    let mut sizes = BTreeSet::new();
    for seed in 0..64u64 {
        let restart = Restart::draw(seed, "restart");
        let label = format!("Q-RST-bound seed {seed}");
        sizes.insert(restart.voters.len());
        let mut sim = Simulation::new(5);
        restart.boot(&mut sim);

        let mut now = 0;
        while sim.hosts().is_empty() {
            assert!(
                now <= ELECT_WITHIN_MS,
                "{label}: no host {now} ms after a restart of every voter \
                 (lease {LEASE_MS} ms, recovered {:?})",
                restart.floors
            );
            now += STEP_MS;
            sim.run_until(Time(now));
            assert_lease_disjoint(&sim, now, &label);
        }
        assert_host_fenced(&sim, &restart, &label);
        assert_floors_kept(&sim, &restart, &label);
        floor_answers += sim.floor_answers_seen();
    }
    assert_eq!(sizes.len(), 3, "the draw covers rosters of 2, 3 and 5");
    assert!(
        floor_answers > 0,
        "no voter ever answered a claim below its floor"
    );
}

/// **Q-RST-fence.** The same restart of every voter with message loss, jitter
/// and partitions, then a heal. Fencing holds at every step — one lease at
/// most, each host above the recovered grant of a majority, no endorsement
/// at or below a recovered grant — and the healed fabric elects a host.
#[test]
fn dst_quorum_full_restart_keeps_every_floor() {
    for seed in 0..64u64 {
        let restart = Restart::draw(seed, "fence");
        let label = format!("Q-RST-fence seed {seed}");
        let mut rng = SplitMix64::new(seed ^ 0xfe9c_e000);
        let mut sim = Simulation::new(u64::from(3 + rng.up_to(7)));
        sim.set_loss(Percent::try_from(rng.up_to(19)).expect("at most 19%"));
        sim.set_jitter(u64::from(rng.up_to(8)));
        restart.boot(&mut sim);

        let last = u32::try_from(restart.voters.len() - 1).expect("a handful of voters");
        let mut now = 0;
        while now < CHAOS_MS {
            now += STEP_MS;
            sim.run_until(Time(now));
            assert_lease_disjoint(&sim, now, &label);
            assert_host_fenced(&sim, &restart, &label);
            match rng.up_to(40) {
                0 => {
                    let a = &restart.voters[usize::try_from(rng.up_to(last)).expect("bounded")];
                    let b = &restart.voters[usize::try_from(rng.up_to(last)).expect("bounded")];
                    if a != b {
                        sim.block(a, b);
                        sim.block(b, a);
                    }
                }
                1 => sim.heal_all(),
                _ => {}
            }
        }

        sim.heal_all();
        sim.set_loss(Percent::ZERO);
        sim.set_jitter(0);
        let settle = now + SETTLE_MS;
        while sim.hosts().is_empty() {
            assert!(now < settle, "{label}: no host after the heal");
            now += STEP_MS;
            sim.run_until(Time(now));
            assert_lease_disjoint(&sim, now, &label);
        }
        assert_host_fenced(&sim, &restart, &label);
        assert_floors_kept(&sim, &restart, &label);
    }
}
