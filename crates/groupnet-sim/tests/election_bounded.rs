//! Bounded-exhaustive schedules for the Hosted/Settle election.
//!
//! The seeded chaos suite samples long traces. This checker complements it with
//! a closed finite matrix: every directed three-node link topology, every
//! zero-or-one crash choice, and timing cuts on both sides of the settle and
//! lease boundaries. Every case drives the production `GroupEngine` through
//! `Simulation`; a failing `(mask, crash, dwell_ms)` tuple is a replayable trace.

use std::collections::{BTreeMap, BTreeSet};

use groupnet_core::{
    Activation, Config, GroupEngine, GroupId, GroupMode, HostedConfig, NodeId, Role, Time,
    placement,
};
use groupnet_sim::Simulation;

const INITIAL_SETTLE_MS: u64 = 1_500;
const FINAL_SETTLE_MS: u64 = 3_000;
const CLAIM_SETTLE_MS: u64 = 200;
const LEASE_MS: u64 = 400;
const DWELL_MS: [u64; 7] = [0, 199, 200, 399, 400, 401, 900];

#[derive(Clone, Copy, Debug)]
struct Case {
    /// Six bits, one for every directed edge between three nodes. A set bit
    /// blocks that edge for the fault window.
    blocked_mask: u8,
    /// `None` or one of the three nodes, crashed at the start of the window and
    /// restarted after the fabric heals.
    crashed: Option<usize>,
    dwell_ms: u64,
}

fn config() -> Config {
    Config {
        gossip_interval_ms: 60,
        probe_interval_ms: 50,
        probe_timeout_ms: 40,
        suspect_timeout_ms: 1_000,
        indirect_probes: 2,
        fanout: 4,
        anti_entropy_interval_ms: 60,
        anti_entropy_fanout: 2,
        eager_push: true,
        full_digest_every: 4,
        max_delta_frame_bytes: 4_096,
        mode: GroupMode::Hosted(HostedConfig {
            activation: Activation::Settle {
                claim_settle_ms: CLAIM_SETTLE_MS,
            },
            lease_ms: LEASE_MS,
        }),
        ..Config::default()
    }
}

fn nodes() -> [NodeId; 3] {
    [NodeId::new("n0"), NodeId::new("n1"), NodeId::new("n2")]
}

fn engine(group: &GroupId, node: &NodeId, nodes: &[NodeId; 3]) -> GroupEngine {
    let seeds = nodes.iter().filter(|candidate| *candidate != node).cloned();
    GroupEngine::new(group.clone(), node.clone(), seeds, config())
}

fn block_mask(sim: &mut Simulation, nodes: &[NodeId; 3], mask: u8) {
    let mut bit = 0u32;
    for from in nodes {
        for to in nodes {
            if from == to {
                continue;
            }
            if mask & (1u8 << bit) != 0 {
                sim.block(from, to);
            }
            bit += 1;
        }
    }
    assert_eq!(bit, 6, "three nodes have six directed links");
}

fn pair_of(sim: &Simulation, node: &NodeId) -> (u64, Option<NodeId>) {
    sim.leadership_of(node)
        .expect("a checked node is present in the simulation")
}

fn assert_local_invariants(
    sim: &Simulation,
    alive: &BTreeSet<NodeId>,
    baseline_epochs: &BTreeMap<NodeId, u64>,
    now: u64,
    case: Case,
) {
    for node in alive {
        let role = sim.role_of(node).expect("an alive node has an engine");
        let pair = pair_of(sim, node);
        let observed = sim
            .observed_epoch_of(node)
            .expect("an alive node exposes its observed epoch");

        assert!(
            observed >= pair.0,
            "{case:?}: {node} adopted epoch {} above highest-seen {observed}",
            pair.0
        );
        assert!(
            observed >= baseline_epochs[node],
            "{case:?}: {node}'s observed epoch regressed from {} to {observed}",
            baseline_epochs[node]
        );

        if role == Role::Host {
            assert_eq!(
                pair.1.as_ref(),
                Some(node),
                "{case:?}: {node} is Host but adopted {pair:?}"
            );
            let lease = sim
                .lease_until_of(node)
                .expect("a host has a lease deadline");
            assert!(
                Time(now) <= lease,
                "{case:?}: {node} remained Host at {now} past lease {lease:?}"
            );
        } else {
            assert_ne!(
                pair.1.as_ref(),
                Some(node),
                "{case:?}: {node} is {role:?} but still names itself in {pair:?}"
            );
            assert_eq!(
                sim.lease_until_of(node),
                None,
                "{case:?}: non-host {node} retained a host lease"
            );
        }
    }
}

fn run_case(case: Case) {
    let group = GroupId::new("bounded-election");
    let nodes = nodes();
    let all: BTreeSet<NodeId> = nodes.iter().cloned().collect();
    let mut alive = all.clone();
    let mut sim = Simulation::new(7);
    for node in &nodes {
        sim.add(engine(&group, node, &nodes));
    }

    sim.run_until(Time(INITIAL_SETTLE_MS));
    assert_eq!(
        sim.hosts().len(),
        1,
        "{case:?}: precondition did not reach one host"
    );
    let baseline_epochs: BTreeMap<NodeId, u64> = nodes
        .iter()
        .map(|node| {
            (
                node.clone(),
                sim.observed_epoch_of(node)
                    .expect("all initial engines are present"),
            )
        })
        .collect();

    block_mask(&mut sim, &nodes, case.blocked_mask);
    if let Some(crashed) = case.crashed {
        sim.crash(&nodes[crashed]);
        alive.remove(&nodes[crashed]);
    }

    let fault_end = INITIAL_SETTLE_MS + case.dwell_ms;
    sim.run_until(Time(fault_end));
    assert_local_invariants(&sim, &alive, &baseline_epochs, fault_end, case);

    sim.heal_all();
    if let Some(crashed) = case.crashed {
        let node = &nodes[crashed];
        sim.add(engine(&group, node, &nodes));
        alive.insert(node.clone());
    }
    assert_eq!(alive, all, "{case:?}: recovery did not restore every node");

    let final_time = fault_end + FINAL_SETTLE_MS;
    sim.run_until(Time(final_time));
    assert_local_invariants(&sim, &alive, &baseline_epochs, final_time, case);

    for node in &nodes {
        assert_eq!(
            sim.members_of(node),
            all,
            "{case:?}: {node} did not converge on full membership"
        );
    }

    let expected = placement::owner(group.as_str(), &all).expect("nonempty membership");
    assert_eq!(
        sim.hosts(),
        vec![expected.clone()],
        "{case:?}: healed cluster did not converge on the rendezvous owner"
    );
    let settled = pair_of(&sim, &expected);
    assert_eq!(
        settled.1.as_ref(),
        Some(&expected),
        "{case:?}: settled host does not name itself"
    );
    for node in &nodes {
        assert_eq!(
            pair_of(&sim, node),
            settled,
            "{case:?}: {node} disagrees on the fenced pair"
        );
    }
}

/// BOUNDED-EXHAUSTIVE: $2^6$ directed link topologies × four crash choices ×
/// seven timing cuts = 1,792 replayable schedules through the real engine.
#[test]
fn every_bounded_topology_crash_and_boundary_schedule_recovers() {
    let mut cases = 0usize;
    for blocked_mask in 0u8..64 {
        for crashed in [None, Some(0), Some(1), Some(2)] {
            for dwell_ms in DWELL_MS {
                run_case(Case {
                    blocked_mask,
                    crashed,
                    dwell_ms,
                });
                cases += 1;
            }
        }
    }
    assert_eq!(cases, 1_792, "the declared finite matrix changed");
}
