//! Probe, suspicion, refutation, and membership lifecycle tests.

use crate::config::Config;
use crate::membership::Status;
use std::collections::BTreeSet;

use crate::{GroupId, NodeId, Time, placement, wire};

use super::super::{Command, Effect, GroupEngine};
use super::test_support::*;

#[test]
fn start_announces_to_seeds() {
    // Fanout defaults to 2, so a round reaches up to two seeds.
    let mut e = engine("a", &["b", "c"]);
    let sends = e
        .start(Time::ZERO)
        .iter()
        .filter(|e| matches!(e, Effect::Send { .. }))
        .count();
    assert_eq!(sends, 2, "should announce to two seeds via digest");
}

#[test]
fn learns_members_and_recomputes_coordinator() {
    let mut a = engine("a", &["b"]);
    a.on_message(
        NodeId::new("b"),
        &digest_frame(vec![ndigest("b", 0, Status::Alive, 0)], vec![]),
        Time(1),
    );
    assert_eq!(a.members().count(), 2);
    let set: BTreeSet<NodeId> = [NodeId::new("a"), NodeId::new("b")].into_iter().collect();
    assert_eq!(a.coordinator().cloned(), placement::owner("g", &set));
}

#[test]
fn two_node_probe_leads_to_suspect_then_dead() {
    let cfg = Config {
        probe_interval_ms: 100,
        probe_timeout_ms: 50,
        suspect_timeout_ms: 200,
        gossip_interval_ms: 100,
        anti_entropy_interval_ms: 100,
        ..Config::default()
    };
    let mut a = GroupEngine::new(GroupId::new("g"), NodeId::new("a"), [NodeId::new("b")], cfg);
    a.on_message(
        NodeId::new("b"),
        &digest_frame(vec![ndigest("b", 0, Status::Alive, 0)], vec![]),
        Time(1),
    );
    a.start(Time(1));

    // With no third node to relay, a direct miss falls straight through to
    // suspicion.
    a.on_tick(Time(101)); // sends the direct probe (deadline 151)
    a.on_tick(Time(160)); // window elapsed, no probers -> suspect
    assert_eq!(a.member_status(&NodeId::new("b")), Some(Status::Suspect));

    a.on_tick(Time(400)); // suspicion window elapsed -> dead
    assert_eq!(a.member_status(&NodeId::new("b")), Some(Status::Dead));
    assert!(!a.members().any(|n| *n == NodeId::new("b")));
}

#[test]
fn direct_miss_escalates_to_indirect_before_suspecting() {
    let cfg = Config {
        probe_interval_ms: 1000,
        probe_timeout_ms: 50,
        ..Config::default()
    };
    let mut a = GroupEngine::new(GroupId::new("g"), NodeId::new("a"), [], cfg);
    // Learn b and c.
    a.on_message(
        NodeId::new("b"),
        &digest_frame(
            vec![
                ndigest("b", 0, Status::Alive, 0),
                ndigest("c", 0, Status::Alive, 0),
            ],
            vec![],
        ),
        Time(1),
    );
    a.start(Time(1));

    a.on_tick(Time(1001)); // direct probe to first candidate (b)
    let effects = a.on_tick(Time(1100)); // direct miss -> ping-req, NOT suspect
    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::Send { to, .. } if *to == NodeId::new("c"))),
        "should ask c to probe b indirectly"
    );
    assert_eq!(a.member_status(&NodeId::new("b")), Some(Status::Alive));

    // An indirect ack keeps b alive.
    a.on_message(
        NodeId::new("c"),
        &probe_frame(wire::Kind::IndirectAck, Some(NodeId::new("b"))),
        Time(1120),
    );
    a.on_tick(Time(2000));
    assert_eq!(a.member_status(&NodeId::new("b")), Some(Status::Alive));
}

#[test]
fn ping_req_makes_us_probe_and_relay() {
    let mut p = engine("p", &[]);
    // origin o asks p to probe t.
    let effects = p.on_message(
        NodeId::new("o"),
        &probe_frame(wire::Kind::PingReq, Some(NodeId::new("t"))),
        Time(1),
    );
    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::Send { to, .. } if *to == NodeId::new("t"))),
        "prober should ping the target"
    );
    // When t acks, we relay an IndirectAck back to the origin o.
    let ack = p.on_message(
        NodeId::new("t"),
        &probe_frame(wire::Kind::Ack, None),
        Time(3),
    );
    assert!(
        ack.iter()
            .any(|e| matches!(e, Effect::Send { to, .. } if *to == NodeId::new("o"))),
        "should relay an indirect ack to the origin"
    );
}

#[test]
fn ping_is_answered_with_a_bare_ack() {
    let mut a = engine("a", &[]);
    let effects = a.on_message(
        NodeId::new("b"),
        &probe_frame(wire::Kind::Ping, None),
        Time(1),
    );
    let ack = effects
        .iter()
        .find_map(|e| match e {
            Effect::Send { to, wire } if *to == NodeId::new("b") => wire::decode(wire),
            _ => None,
        })
        .expect("an ack send");
    assert_eq!(ack.kind, wire::Kind::Ack);
    assert!(ack.digest.is_empty() && ack.members.is_empty() && ack.metadata.is_empty());
}

#[test]
fn refutes_false_suspicion_about_self() {
    let mut a = engine("a", &["b"]);
    a.on_message(
        NodeId::new("b"),
        &digest_frame(vec![ndigest("a", 0, Status::Suspect, 0)], vec![]),
        Time(1),
    );
    assert_eq!(a.member_status(&NodeId::new("a")), Some(Status::Alive));
    // Our next digest advertises ourselves Alive at a bumped incarnation.
    let frame = decode_one_digest(&a.on_tick(Time(2)));
    let self_d = frame
        .digest
        .iter()
        .find(|d| d.node == NodeId::new("a"))
        .expect("self in digest");
    assert_eq!(self_d.status, Status::Alive.to_wire());
    assert!(
        self_d.incarnation >= 1,
        "refutation should bump incarnation"
    );
}

#[test]
fn voluntary_leave_is_not_refuted() {
    let mut a = engine("a", &["b"]);
    a.apply(Command::Leave);
    assert_eq!(a.member_status(&NodeId::new("a")), Some(Status::Dead));
    a.on_message(
        NodeId::new("b"),
        &digest_frame(vec![ndigest("a", 0, Status::Dead, 0)], vec![]),
        Time(1),
    );
    assert_eq!(a.member_status(&NodeId::new("a")), Some(Status::Dead));
}
