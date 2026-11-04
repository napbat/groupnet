//! A digest lists at most `Config::digest_window` members, so its cost stays
//! flat as the group grows — and membership and entries still converge.

use std::num::NonZeroUsize;

use groupnet_core::{Command, Config, GroupEngine, GroupId, NodeId, Time};
use groupnet_sim::Simulation;

const N: usize = 300;
const WINDOW: usize = 64;

/// The encoded size of one member summary for the ids this test uses
/// (`node-000`): id (4 + 8) + incarnation (8) + status (1) + high-water (8) +
/// content hash (8).
const SUMMARY_BYTES: u64 = 37;

/// A digest frame's own bytes besides its summaries — version, kind, group,
/// target flag, the two counts — with room to spare for the group name.
const FRAME_OVERHEAD: u64 = 64;

/// Whether every node knows all `N` members.
fn membership_converged(sim: &Simulation, ids: &[NodeId]) -> bool {
    ids.iter().all(|id| sim.member_count(id) == Some(N))
}

/// Whether every node holds `writer`'s entry `key`.
fn entry_converged(sim: &Simulation, ids: &[NodeId], writer: &NodeId, key: &str) -> bool {
    ids.iter().all(|id| {
        sim.entries_snapshot(id)
            .is_some_and(|all| all.get(writer).is_some_and(|e| e.contains_key(key)))
    })
}

/// Runs whole gossip rounds from `*now` until `done` holds, at most `limit`
/// of them; whether it held. `*now` follows the virtual clock.
fn within_rounds(
    sim: &mut Simulation,
    now: &mut u64,
    round_ms: u64,
    limit: u64,
    mut done: impl FnMut(&Simulation) -> bool,
) -> bool {
    for _ in 0..limit {
        *now += round_ms;
        sim.run_until(Time(*now));
        if done(sim) {
            return true;
        }
    }
    false
}

/// 300 nodes boot against three seeds with a 64-member digest window.
///
/// * Every digest any node builds lists at most 64 summaries.
/// * Membership converges everywhere within 40 gossip rounds (measured: 34),
///   and one node's entry write then reaches all 300 within 8 rounds
///   (measured: 5) — fresh changes lead every full digest.
/// * Digest bytes per node per digest round stay under `gossip_fanout`
///   window-sized digests — a bound that does not mention N (measured: 4694
///   bytes at most, against a bound of 4864).
///
/// The same run with the window at or above N — the full digest before the
/// window, which listed every member — has its largest digest list all 300
/// summaries and costs up to 19820 digest bytes per node per round;
/// membership converges in 7 rounds and the entry in 4. The window trades
/// cold-boot convergence (one 64-member slice per digest) for a digest cost
/// that stays flat as the group grows.
#[test]
fn digests_stay_within_the_window_and_still_converge() {
    let config = Config {
        digest_window: NonZeroUsize::new(WINDOW).expect("nonzero"),
        ..Config::default()
    };
    let round_ms = config.gossip_interval_ms;
    let fanout = u64::try_from(config.gossip_fanout.get()).expect("small fanout");

    let group = GroupId::new("digest-window");
    let ids: Vec<NodeId> = (0..N)
        .map(|i| NodeId::new(format!("node-{i:03}")))
        .collect();
    let seeds: Vec<NodeId> = ids[..3].to_vec();
    let mut sim = Simulation::new(10);
    for id in &ids {
        let seeds = seeds.iter().filter(|s| *s != id).cloned();
        let engine = GroupEngine::new(group.clone(), id.clone(), seeds, config.clone())
            .expect("valid config");
        sim.add(engine).expect("distinct node");
    }

    let mut now = 0;
    assert!(
        within_rounds(&mut sim, &mut now, round_ms, 40, |sim| {
            membership_converged(sim, &ids)
        }),
        "membership did not converge within 40 rounds"
    );

    let writer = ids[N / 2].clone();
    sim.command(
        &writer,
        Command::SetLocalEntry {
            key: "k".into(),
            value: b"v".to_vec(),
            ttl_ms: None,
        },
    )
    .expect("live node");
    assert!(
        within_rounds(&mut sim, &mut now, round_ms, 8, |sim| {
            entry_converged(sim, &ids, &writer, "k")
        }),
        "the entry did not reach every node within 8 rounds"
    );

    // A node's digest rounds are its digests over the fanout: membership
    // changes bring rounds forward, so the clock undercounts them at boot.
    let window = u64::try_from(WINDOW).expect("small window");
    let per_round_bound = fanout * (window * SUMMARY_BYTES + FRAME_OVERHEAD);
    let mut largest = 0;
    let mut busiest = 0;
    for id in &ids {
        let stats = sim.net_stats_of(id).expect("live node");
        largest = largest.max(stats.max_digest_summaries);
        let rounds = (stats.digests_built / fanout).max(1);
        busiest = busiest.max(stats.digest_bytes_sent / rounds);
    }
    assert!(
        largest <= window,
        "a digest listed {largest} summaries, above the {WINDOW}-member window"
    );
    assert!(
        busiest <= per_round_bound,
        "a node sent {busiest} digest bytes per round, above {per_round_bound}"
    );
}
