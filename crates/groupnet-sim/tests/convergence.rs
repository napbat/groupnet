//! End-to-end determinism tests: the same core, driven by a virtual clock,
//! must converge every node on one coordinator — reproducibly.

use groupnet_core::{Command, Config, GroupEngine, GroupId, NodeId, Status, Time};
use groupnet_sim::Simulation;

fn node_ids(names: &[&str]) -> Vec<NodeId> {
    names.iter().map(|s| NodeId::new(*s)).collect()
}

fn build(group: &GroupId, ids: &[NodeId], latency: u64, loss_percent: u8) -> Simulation {
    build_with(group, ids, latency, loss_percent, &Config::default())
}

fn build_with(
    group: &GroupId,
    ids: &[NodeId],
    latency: u64,
    loss_percent: u8,
    config: &Config,
) -> Simulation {
    let mut sim = Simulation::new(latency).with_loss(loss_percent);
    for id in ids {
        let seeds = ids.iter().filter(|x| *x != id).cloned();
        sim.add(GroupEngine::new(
            group.clone(),
            id.clone(),
            seeds,
            config.clone(),
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

    // This test isolates *dissemination* under loss, so we disable failure
    // detection: with only direct probes, 33% loss would produce false
    // positives (the exact problem indirect probes / ping-req solve). Gossip's
    // redundancy must still carry membership to convergence.
    let cfg = Config {
        probe_timeout_ms: 10_000_000,
        suspect_timeout_ms: 10_000_000,
        ..Config::default()
    };
    let mut sim = build_with(&group, &ids, 10, 33, &cfg);
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

#[test]
fn detects_and_removes_a_crashed_node() {
    let group = GroupId::new("shard-42");
    let ids = node_ids(&["node-a", "node-b", "node-c", "node-d"]);

    let mut sim = build(&group, &ids, 10, 0);
    sim.run_until(Time(2_000));
    for id in &ids {
        assert_eq!(sim.member_count(id), 4, "did not converge before crash");
    }

    // node-d crashes: it stops acking probes and gossiping.
    let dead = ids[3].clone();
    sim.crash(&dead);
    sim.run_until(Time(14_000));

    // Survivors detect it, mark it Dead, and drop it from the live set.
    for id in &ids[..3] {
        assert_eq!(
            sim.status_of(id, &dead),
            Some(Status::Dead),
            "{id} still trusts crashed node"
        );
        assert!(!sim.is_member(id, &dead));
        assert_eq!(sim.member_count(id), 3);
    }
    // The coordinator must be a survivor, and all survivors agree.
    assert!(sim.all_agree_on_coordinator());
    assert_ne!(sim.coordinator_of(&ids[0]), Some(dead));
}

#[test]
fn indirect_probe_prevents_false_positive_under_partition() {
    let group = GroupId::new("shard-42");
    let ids = node_ids(&["node-a", "node-b", "node-c"]);

    let mut sim = build(&group, &ids, 10, 0);
    sim.run_until(Time(2_000));

    // Sever the *direct* link between a and c in both directions. They can still
    // reach each other indirectly through b, so ping-req must keep both alive —
    // no node should ever be marked Dead.
    sim.block(&ids[0], &ids[2]);
    sim.block(&ids[2], &ids[0]);
    sim.run_until(Time(20_000));

    for observer in &ids {
        for node in &ids {
            assert_ne!(
                sim.status_of(observer, node),
                Some(Status::Dead),
                "{observer} wrongly killed {node} despite indirect reachability"
            );
        }
        assert_eq!(sim.member_count(observer), 3);
    }
}

#[test]
fn dead_tombstones_are_reaped() {
    let group = GroupId::new("shard-42");
    let ids = node_ids(&["node-a", "node-b", "node-c", "node-d"]);

    // Small dead_timeout so the reap window is reached quickly in the sim.
    let cfg = Config {
        dead_timeout_ms: 1_000,
        ..Config::default()
    };
    let mut sim = build_with(&group, &ids, 10, 0, &cfg);
    sim.run_until(Time(2_000));

    let dead = ids[3].clone();
    sim.crash(&dead);
    // Detected + declared Dead, then gossiped for dead_timeout, then reaped at
    // 2×dead_timeout — with no peer re-teaching it.
    sim.run_until(Time(12_000));

    for id in &ids[..3] {
        assert_eq!(
            sim.status_of(id, &dead),
            None,
            "{id} never reaped the dead tombstone"
        );
    }
}

#[test]
fn voluntary_leave_removes_node_and_sticks() {
    let group = GroupId::new("shard-42");
    let ids = node_ids(&["node-a", "node-b", "node-c"]);

    let mut sim = build(&group, &ids, 10, 0);
    sim.run_until(Time(2_000));

    // node-c leaves gracefully; the leave must disseminate and not be refuted
    // (a grow-set could never express this).
    let gone = ids[2].clone();
    sim.command(&gone, Command::Leave);
    sim.run_until(Time(6_000));

    for id in &ids[..2] {
        assert_eq!(sim.status_of(id, &gone), Some(Status::Dead));
        assert_eq!(sim.member_count(id), 2);
    }
}

#[test]
fn per_node_state_converges_cluster_wide() {
    let group = GroupId::new("shard-42");
    let ids = node_ids(&["node-a", "node-b", "node-c"]);

    let mut sim = build(&group, &ids, 10, 0);
    sim.run_until(Time(1_000));

    // Each node advertises its own app-defined state (e.g. its capacity weight).
    for (i, id) in ids.iter().enumerate() {
        sim.command(
            id,
            Command::SetLocalState(format!("weight={i}").into_bytes()),
        );
    }
    sim.run_until(Time(5_000));

    // Every node converges on every node's self-authored state.
    for observer in &ids {
        for (i, node) in ids.iter().enumerate() {
            assert_eq!(
                sim.state_of(observer, node).as_deref(),
                Some(format!("weight={i}").as_bytes()),
                "{observer} did not converge on {node}'s state"
            );
        }
    }
}

#[test]
fn per_node_state_update_supersedes_cluster_wide() {
    let group = GroupId::new("shard-42");
    let ids = node_ids(&["node-a", "node-b", "node-c"]);

    let mut sim = build(&group, &ids, 10, 0);
    sim.run_until(Time(1_000));

    sim.command(&ids[0], Command::SetLocalState(b"ready=false".to_vec()));
    sim.run_until(Time(2_000));
    sim.command(&ids[0], Command::SetLocalState(b"ready=true".to_vec()));
    sim.run_until(Time(5_000));

    for observer in &ids {
        assert_eq!(
            sim.state_of(observer, &ids[0]).as_deref(),
            Some(&b"ready=true"[..])
        );
    }
}
