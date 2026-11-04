//! Probe load spreads over the group: a node's indirect probers are drawn at
//! random from its live peers, and every node's probe ring starts at its own
//! offset, so no member carries the whole group's probe traffic.

use groupnet_core::{Command, Config, GroupEngine, GroupId, NodeId, Time, wire};
use groupnet_sim::{Percent, Simulation};

/// A group of `n` nodes that all know each other from the start, so every
/// probe ring is full at the first probe round.
fn full_mesh(n: usize, loss: Percent, config: &Config) -> (Simulation, Vec<NodeId>) {
    let group = GroupId::new("probe-spread");
    let ids: Vec<NodeId> = (0..n)
        .map(|i| NodeId::new(format!("node-{i:02}")))
        .collect();
    let mut sim = Simulation::new(10).with_loss(loss);
    for id in &ids {
        let engine =
            GroupEngine::new(group.clone(), id.clone(), [], config.clone()).expect("valid config");
        sim.add(engine).expect("distinct node");
    }
    for id in &ids {
        for peer in ids.iter().filter(|p| *p != id) {
            sim.command(id, Command::AddPeer(peer.clone()))
                .expect("live node");
        }
    }
    (sim, ids)
}

/// With 20% loss a direct probe round often misses, so every node keeps
/// enlisting indirect probers. Those are drawn at random among the live peers
/// other than the target, so the `PingReq` load is even: the busiest node
/// receives at most three times the mean.
///
/// Before random probers every node enlisted the `indirect_probes` (2)
/// lowest-id peers other than the target, so in this same run node-00 and
/// node-01 took 4447 and 4453 of the 9226 requests (node-02 313, node-03 13,
/// everyone else none): max/mean 30.9. Measured after: 169 at most of 9120,
/// max/mean 1.19.
#[test]
fn indirect_probe_load_is_even_across_the_group() {
    const N: usize = 64;
    let config = Config {
        // Nobody dies, so every ring stays full for the whole run.
        suspect_timeout_ms: 10_000_000,
        ..Config::default()
    };
    let (mut sim, ids) = full_mesh(N, Percent::of(20), &config);
    sim.run_until(Time(20_000));

    let load: Vec<u64> = ids
        .iter()
        .map(|id| sim.frames_issued_to(id, wire::Kind::PingReq))
        .collect();
    let total: u64 = load.iter().sum();
    let max = load.iter().copied().max().expect("nonempty group");
    assert!(total >= 5_000, "too few indirect probes to judge: {total}");
    // max / mean <= 3, in integers: max * N <= 3 * total.
    let n = u64::try_from(N).expect("small group");
    assert!(
        max * n <= 3 * total,
        "indirect probe load concentrated: max {max} of {total} over {N} nodes \
         (max/mean {:.2})",
        ratio(max * n, total)
    );
}

/// Nodes that start together with the same membership begin their probe
/// rings at offsets derived from their own ids, so the first probe round
/// spreads over the group instead of every node pinging the same member.
///
/// Before the offsets every ring started at 0, so all 64 nodes probed the
/// lowest-id peer first: 2 distinct first targets (63 pings to node-00, one
/// to node-01). Measured after: 40 distinct first targets.
#[test]
fn nodes_started_together_probe_different_first_targets() {
    const N: usize = 64;
    let config = Config::default();
    let (mut sim, ids) = full_mesh(N, Percent::ZERO, &config);
    // The first probe round fires at `probe_interval_ms`; stop before the
    // second, and before any direct miss could relay a probe.
    sim.run_until(Time(config.probe_interval_ms + config.probe_timeout_ms - 1));

    let pings: Vec<u64> = ids
        .iter()
        .map(|id| sim.frames_issued_to(id, wire::Kind::Ping))
        .collect();
    assert_eq!(
        pings.iter().sum::<u64>(),
        u64::try_from(N).expect("small group"),
        "every node probes exactly once in the first round"
    );
    let distinct = pings.iter().filter(|&&p| p > 0).count();
    assert!(
        distinct >= N / 2,
        "first probe round hit only {distinct} distinct targets of {N}"
    );
}

/// `num / den` as a float, for an assertion message only.
#[expect(
    clippy::cast_precision_loss,
    reason = "a two-decimal ratio in a failure message"
)]
fn ratio(num: u64, den: u64) -> f64 {
    num as f64 / den as f64
}
