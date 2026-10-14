//! The harness's one-engine-per-node contract: a duplicate [`Simulation::add`]
//! is refused without disturbing the engine in place, every mutation that
//! targets a missing node reports [`SimError::UnknownNode`], and every
//! read-only per-node query reports a missing observer as `None`.

use std::collections::{BTreeMap, BTreeSet};

use groupnet_core::{Command, Config, GroupEngine, GroupId, NodeId, Time};
use groupnet_sim::{SimError, Simulation};

fn engine(group: &str, local: &NodeId, seeds: &[NodeId]) -> GroupEngine {
    GroupEngine::new(
        GroupId::new(group),
        local.clone(),
        seeds.iter().filter(|s| *s != local).cloned(),
        Config::default(),
    )
    .expect("valid config")
}

/// Two converged nodes, `a` and `b`, in group `g`.
fn pair() -> (Simulation, NodeId, NodeId) {
    let a = NodeId::new("a");
    let b = NodeId::new("b");
    let ids = [a.clone(), b.clone()];
    let mut sim = Simulation::new(10);
    sim.add(engine("g", &a, &ids)).expect("a is new");
    sim.add(engine("g", &b, &ids)).expect("b is new");
    sim.run_until(Time(3_000));
    (sim, a, b)
}

#[test]
fn duplicate_add_is_refused_and_keeps_the_original_engine() {
    let (mut sim, a, b) = pair();
    sim.command(
        &a,
        Command::UpdateMetadata {
            key: "owner".into(),
            value: "original".into(),
        },
    )
    .expect("a is present");
    sim.run_until(Time(4_000));

    // Same id, different group, no seeds: if this engine replaced the original,
    // `a` would forget both `b` and the metadata it wrote.
    let err = sim
        .add(engine("other", &a, &[]))
        .expect_err("a second engine for `a` must be refused");
    assert_eq!(err, SimError::DuplicateNode(a.clone()));
    assert_eq!(
        err.to_string(),
        "node a already hosts an engine in this simulation"
    );

    sim.run_until(Time(8_000));
    assert_eq!(sim.nodes(), vec![a.clone(), b.clone()]);
    assert_eq!(
        sim.members_of(&a),
        Some(BTreeSet::from([a.clone(), b.clone()])),
        "the original engine keeps its view"
    );
    assert_eq!(sim.metadata_of(&a, "owner").as_deref(), Some("original"));
    assert_eq!(sim.metadata_of(&b, "owner").as_deref(), Some("original"));
}

#[test]
fn command_to_a_missing_node_is_unknown() {
    let (mut sim, a, _) = pair();
    let ghost = NodeId::new("ghost");
    let err = sim
        .command(&ghost, Command::AddPeer(a.clone()))
        .expect_err("ghost was never added");
    assert_eq!(err, SimError::UnknownNode(ghost));
    assert_eq!(err.to_string(), "node ghost is not in this simulation");

    sim.crash(&a).expect("a is present");
    assert_eq!(
        sim.command(&a, Command::Leave),
        Err(SimError::UnknownNode(a.clone())),
        "a crashed node takes no commands"
    );
}

#[test]
fn crash_of_a_missing_node_is_unknown() {
    let (mut sim, a, b) = pair();
    let ghost = NodeId::new("ghost");
    assert_eq!(sim.crash(&ghost), Err(SimError::UnknownNode(ghost)));
    assert_eq!(
        sim.nodes(),
        vec![a.clone(), b.clone()],
        "nothing was removed"
    );

    sim.crash(&a).expect("a is present");
    assert_eq!(
        sim.crash(&a),
        Err(SimError::UnknownNode(a.clone())),
        "a node crashes once"
    );
    assert_eq!(sim.nodes(), vec![b]);
}

#[test]
fn a_crashed_id_can_be_added_again() {
    let (mut sim, a, b) = pair();
    sim.crash(&a).expect("a is present");
    sim.add(engine("g", &a, std::slice::from_ref(&b)))
        .expect("a crashed id is free again");
    sim.run_until(Time(20_000));
    assert_eq!(sim.member_count(&a), Some(2));
}

#[test]
fn per_node_queries_report_a_missing_observer_as_none() {
    let (mut sim, a, b) = pair();
    sim.crash(&a).expect("a is present");
    for missing in [a, NodeId::new("ghost")] {
        assert_eq!(sim.member_count(&missing), None, "{missing}");
        assert_eq!(sim.is_member(&missing, &b), None, "{missing}");
        assert_eq!(sim.members_of(&missing), None, "{missing}");
        assert_eq!(sim.metadata_snapshot(&missing), None, "{missing}");
        assert_eq!(sim.state_snapshot(&missing), None, "{missing}");
        assert_eq!(sim.entries_snapshot(&missing), None, "{missing}");
    }
    // A present observer still answers, empty views included.
    assert_eq!(
        sim.member_count(&b),
        Some(2),
        "b has not detected the crash yet"
    );
    assert_eq!(sim.metadata_snapshot(&b), Some(BTreeMap::new()));
}

#[test]
fn sim_error_is_a_std_error() {
    let err: Box<dyn std::error::Error> = Box::new(SimError::UnknownNode(NodeId::new("x")));
    assert!(err.source().is_none());
    assert_eq!(err.to_string(), "node x is not in this simulation");
}
