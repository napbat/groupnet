//! End-to-end determinism tests: the same core, driven by a virtual clock,
//! must converge every node on one coordinator — reproducibly.

use groupnet_core::{Command, Config, GroupEngine, GroupId, NodeId, Time};
use groupnet_sim::Simulation;

fn node_ids(names: &[&str]) -> Vec<NodeId> {
    names.iter().map(|s| NodeId::new(*s)).collect()
}

fn build(group: &GroupId, ids: &[NodeId], latency: u64, loss_percent: u8) -> Simulation {
    let mut sim = Simulation::new(latency).with_loss(loss_percent);
    for id in ids {
        let seeds = ids.iter().filter(|x| *x != id).cloned();
        sim.add(GroupEngine::new(
            group.clone(),
            id.clone(),
            seeds,
            Config::default(),
        ));
    }
    sim
}

#[test]
fn three_nodes_agree_on_coordinator() {
    let group = GroupId::new("shard-42");
    let ids = node_ids(&["node-a", "node-b", "node-c"]);

    let mut sim = build(&group, &ids, 10, 0);
    sim.run_until(Time(5_000));

    assert!(sim.all_agree_on_coordinator(), "nodes did not converge");
    for id in &ids {
        assert_eq!(sim.member_count(id), 3, "{id} missing members");
    }
}

#[test]
fn converges_despite_deterministic_loss() {
    let group = GroupId::new("shard-42");
    let ids = node_ids(&["node-a", "node-b", "node-c", "node-d"]);

    // Drop a third of all messages: gossip's redundancy must still converge.
    let mut sim = build(&group, &ids, 10, 33);
    sim.run_until(Time(10_000));

    assert!(sim.all_agree_on_coordinator());
    for id in &ids {
        assert_eq!(sim.member_count(id), 4);
    }
}

#[test]
fn run_is_bit_for_bit_reproducible() {
    let group = GroupId::new("shard-42");
    let ids = node_ids(&["node-a", "node-b", "node-c"]);

    let coord_after_run = || {
        let mut sim = build(&group, &ids, 7, 0);
        sim.run_until(Time(5_000));
        ids.iter()
            .map(|id| sim.coordinator_of(id))
            .collect::<Vec<_>>()
    };

    assert_eq!(
        coord_after_run(),
        coord_after_run(),
        "sim was not deterministic"
    );
}

#[test]
fn metadata_propagates_and_converges() {
    let group = GroupId::new("shard-42");
    let ids = node_ids(&["node-a", "node-b", "node-c"]);

    let mut sim = build(&group, &ids, 10, 0);
    // Let membership settle, then write metadata on one node.
    sim.run_until(Time(1_000));
    sim.command(
        &ids[0],
        Command::UpdateMetadata {
            key: "routing".into(),
            value: "v3".into(),
        },
    );
    sim.run_until(Time(5_000));

    for id in &ids {
        assert_eq!(
            sim.metadata_of(id, "routing").as_deref(),
            Some("v3"),
            "{id} did not receive the metadata"
        );
    }
}

#[test]
fn latest_write_wins_across_nodes() {
    let group = GroupId::new("shard-42");
    let ids = node_ids(&["node-a", "node-b", "node-c"]);

    let mut sim = build(&group, &ids, 10, 0);
    sim.run_until(Time(1_000));

    // node-a writes v1; later node-b overwrites with v2. The later (higher
    // version) write must win everywhere.
    sim.command(
        &ids[0],
        Command::UpdateMetadata {
            key: "routing".into(),
            value: "v1".into(),
        },
    );
    sim.run_until(Time(2_000));
    sim.command(
        &ids[1],
        Command::UpdateMetadata {
            key: "routing".into(),
            value: "v2".into(),
        },
    );
    sim.run_until(Time(6_000));

    for id in &ids {
        assert_eq!(sim.metadata_of(id, "routing").as_deref(), Some("v2"));
    }
}
