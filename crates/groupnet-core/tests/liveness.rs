//! Probe, suspicion, refutation, and membership lifecycle tests.

use std::collections::BTreeSet;

use groupnet_core::{
    Command, Config, Effect, GroupEngine, GroupId, NodeId, Status, Time, placement, wire,
};
use groupnet_testkit::frames::*;

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

/// Timings for the status-since tests: one peer, so a direct probe miss falls
/// straight through to suspicion and every transition lands on an instant the
/// config alone dictates.
fn since_cfg() -> Config {
    Config {
        probe_interval_ms: 100,
        probe_timeout_ms: 50,
        suspect_timeout_ms: 200,
        gossip_interval_ms: 100,
        anti_entropy_interval_ms: 100,
        ..Config::default()
    }
}

/// An engine for `a` that has just learned `b` (Alive, incarnation 0) at
/// `Time(1)` and been started, with [`since_cfg`] timings.
fn engine_knowing_b() -> GroupEngine {
    let mut a = GroupEngine::new(
        GroupId::new("g"),
        NodeId::new("a"),
        [NodeId::new("b")],
        since_cfg(),
    );
    a.on_message(
        NodeId::new("b"),
        &digest_frame(vec![ndigest("b", 0, Status::Alive, 0)], vec![]),
        Time(1),
    );
    a.start(Time(1));
    a
}

/// A silent peer's status stamps land on exactly the instants the detector's
/// timeouts dictate: adoption at first sight, `Suspect` when the probe window
/// closes, `Dead` one suspicion window after that.
#[test]
fn status_since_lands_on_the_instants_the_timeouts_dictate() {
    let b = NodeId::new("b");
    let mut a = engine_knowing_b();
    assert_eq!(
        a.member_status_since(&b),
        Some((Status::Alive, Time(1))),
        "first adoption stamps the moment we learned the member"
    );

    a.on_tick(Time(101)); // probe slot: direct probe out, deadline 151
    assert_eq!(
        a.member_status_since(&b),
        Some((Status::Alive, Time(1))),
        "an outstanding probe is not a status change"
    );

    // 101 + probe_timeout: the window closes and, with no third node to relay,
    // suspicion is immediate.
    a.on_tick(Time(151));
    assert_eq!(
        a.member_status_since(&b),
        Some((Status::Suspect, Time(151)))
    );

    // 151 + suspect_timeout: the refutation window closes.
    a.on_tick(Time(350));
    assert_eq!(
        a.member_status_since(&b),
        Some((Status::Suspect, Time(151))),
        "one millisecond early is still Suspect"
    );
    a.on_tick(Time(351));
    assert_eq!(a.member_status_since(&b), Some((Status::Dead, Time(351))));

    // The roster iterator agrees with the point lookup, self included.
    let roster: Vec<(NodeId, Status, Time)> = a
        .member_statuses_since()
        .map(|(n, s, t)| (n.clone(), s, t))
        .collect();
    assert_eq!(
        roster,
        vec![
            (NodeId::new("a"), Status::Alive, Time::ZERO),
            (NodeId::new("b"), Status::Dead, Time(351)),
        ]
    );
}

/// Re-merging the *same* status — even at a higher incarnation, which does
/// supersede and does re-arm the suspicion timer — must not move
/// `status_since`. The stamp measures how long the value has stood, and the
/// value stood still.
#[test]
fn same_status_re_merge_at_a_higher_incarnation_holds_the_stamp() {
    let b = NodeId::new("b");
    let mut a = engine_knowing_b();

    // A higher-incarnation *Alive* claim supersedes without changing the value.
    a.on_message(
        NodeId::new("b"),
        &digest_frame(vec![ndigest("b", 5, Status::Alive, 0)], vec![]),
        Time(60),
    );
    assert_eq!(a.member_status_since(&b), Some((Status::Alive, Time(1))));

    // Detect it locally: Alive -> Suspect at 151.
    a.on_tick(Time(101));
    a.on_tick(Time(151));
    assert_eq!(
        a.member_status_since(&b),
        Some((Status::Suspect, Time(151)))
    );

    // A peer re-asserts Suspect at a higher incarnation at t=200. The stamp
    // holds; the suspicion *timer* re-arms, which death timing must reflect.
    a.on_message(
        NodeId::new("c"),
        &digest_frame(vec![ndigest("b", 6, Status::Suspect, 0)], vec![]),
        Time(200),
    );
    assert_eq!(
        a.member_status_since(&b),
        Some((Status::Suspect, Time(151))),
        "a same-status re-merge must not restart the duration"
    );

    a.on_tick(Time(351)); // would have been death from the ORIGINAL stamp
    assert_eq!(
        a.member_status_since(&b),
        Some((Status::Suspect, Time(151))),
        "the re-armed suspicion timer, not status_since, decides death"
    );
    a.on_tick(Time(400)); // 200 + suspect_timeout
    assert_eq!(a.member_status_since(&b), Some((Status::Dead, Time(400))));
}

/// A refutation observed from outside — the suspected node out-incarnating our
/// suspicion — flips the record back to `Alive` with a *fresh* stamp: the
/// member has been alive, as far as this observer is concerned, only since the
/// refutation landed.
#[test]
fn a_refutation_flips_the_record_to_alive_with_a_fresh_stamp() {
    let b = NodeId::new("b");
    let mut a = engine_knowing_b();
    a.on_tick(Time(101));
    a.on_tick(Time(151));
    assert_eq!(
        a.member_status_since(&b),
        Some((Status::Suspect, Time(151)))
    );

    // b refutes by out-incarnating the suspicion.
    a.on_message(
        NodeId::new("b"),
        &digest_frame(vec![ndigest("b", 1, Status::Alive, 0)], vec![]),
        Time(180),
    );
    assert_eq!(a.member_status_since(&b), Some((Status::Alive, Time(180))));

    // And the fresh stamp holds while the refuted status keeps being gossiped.
    a.on_message(
        NodeId::new("c"),
        &digest_frame(vec![ndigest("b", 2, Status::Alive, 0)], vec![]),
        Time(240),
    );
    assert_eq!(a.member_status_since(&b), Some((Status::Alive, Time(180))));
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

/// The command path carries no clock, so its status writes stamp from the
/// freshest time the engine has been *told* about — `now_hint`, one event-loop
/// turn stale at worst. Both command-path status sites are pinned here; the
/// simulator's stamp suite deliberately leaves them out for exactly this
/// reason.
#[test]
fn command_path_status_writes_stamp_from_the_latest_observed_time() {
    let mut a = engine("a", &["b"]);
    assert_eq!(
        a.member_status_since(&NodeId::new("a")),
        Some((Status::Alive, Time::ZERO)),
        "a fresh engine has been alive since the origin of its timeline"
    );

    // Advance the engine's notion of now, then introduce a peer out-of-band.
    a.on_tick(Time(500));
    a.apply(Command::AddPeer(NodeId::new("c")));
    assert_eq!(
        a.member_status_since(&NodeId::new("c")),
        Some((Status::Alive, Time(500)))
    );

    // Leaving flips our own record to Dead, stamped the same way.
    a.on_tick(Time(900));
    a.apply(Command::Leave);
    assert_eq!(
        a.member_status_since(&NodeId::new("a")),
        Some((Status::Dead, Time(900)))
    );
}
