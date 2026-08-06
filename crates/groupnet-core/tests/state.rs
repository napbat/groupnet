//! State dissemination tests: digests, deltas, entries, eager push.

use groupnet_core::{Command, Config, Effect, GroupEngine, GroupId, NodeId, Status, Time, wire};
use groupnet_testkit::frames::*;

/// Delta digests list only members changed since the last digest built
/// for the peer; a quiet round emits nothing at all; every Nth digest is
/// full again. The counters expose exactly that shape.
#[test]
fn delta_digests_list_only_changed_members_with_periodic_full() {
    let config = Config {
        anti_entropy_fanout: 1,
        full_digest_every: 3,
        // Keep probes out of the timeline: no suspicion stamps.
        probe_interval_ms: 1_000_000,
        ..Config::default()
    };
    let mut a = GroupEngine::new(
        GroupId::new("g"),
        NodeId::new("a"),
        [NodeId::new("b")],
        config,
    );

    // Visit 1 (full): only ourselves exist — one summary.
    let effects = a.start(Time(0));
    assert_eq!(digest_summaries(&effects), vec![NodeId::new("a")]);

    // b joins (stamped): the next digest is a delta listing exactly b.
    let _ = a.apply(Command::AddPeer(NodeId::new("b")));
    let effects = a.on_tick(Time(200));
    assert_eq!(digest_summaries(&effects), vec![NodeId::new("b")]);

    // Nothing changed since: a quiet delta round sends no digest at all.
    let effects = a.on_tick(Time(400));
    assert_eq!(digest_summaries(&effects), Vec::<NodeId>::new());

    // A local write stamps us; visit 4 is the periodic FULL digest, so it
    // lists everyone — the repair bound for anything a dropped frame lost.
    let _ = a.apply(Command::SetLocalEntry {
        key: "k".into(),
        value: b"v".to_vec(),
        ttl_ms: None,
    });
    let effects = a.on_tick(Time(600));
    assert_eq!(
        digest_summaries(&effects),
        vec![NodeId::new("a"), NodeId::new("b")],
        "every full_digest_every-th digest lists all members"
    );

    // Quiet again: back to zero-cost rounds.
    let effects = a.on_tick(Time(800));
    assert_eq!(digest_summaries(&effects), Vec::<NodeId>::new());

    let stats = a.net_stats();
    assert_eq!(stats.digests_built, 5);
    assert_eq!(stats.full_digests_built, 2, "visit 1 and visit 4");
    assert_eq!(
        stats.digest_summaries_listed, 4,
        "1 (boot) + 1 (b joined) + 0 + 2 (full) + 0"
    );
    assert!(stats.anti_entropy_bytes_sent > 0);
}

#[test]
fn metadata_merges_by_last_writer_wins() {
    let mut a = engine("a", &["b"]);
    let meta = |ver, writer: &str, val: &str| {
        digest_frame(
            vec![],
            vec![wire::MetaDelta {
                key: "k".into(),
                version: ver,
                writer: NodeId::new(writer),
                value: val.into(),
            }],
        )
    };

    a.on_message(NodeId::new("z"), &meta(2, "z", "remote"), Time(1));
    assert_eq!(a.metadata("k"), Some("remote"));

    a.apply(Command::UpdateMetadata {
        key: "k".into(),
        value: "local".into(),
    });
    assert_eq!(a.metadata("k"), Some("local")); // 2 -> 3 beats remote

    a.on_message(NodeId::new("z"), &meta(1, "z", "stale"), Time(2));
    assert_eq!(a.metadata("k"), Some("local")); // stale ignored
}

#[test]
fn per_node_state_merges_by_last_writer_wins() {
    let mut a = engine("a", &["b"]);

    // Learn b's blob state at version 2.
    a.on_message(
        NodeId::new("b"),
        &delta_frame(vec![member_delta(
            "b",
            vec![entry(GroupEngine::BLOB_KEY, 2, 0, false, b"v2")],
        )]),
        Time(1),
    );
    assert_eq!(a.node_state(&NodeId::new("b")), Some(&b"v2"[..]));

    // A newer version wins; a stale one is ignored.
    a.on_message(
        NodeId::new("b"),
        &delta_frame(vec![member_delta(
            "b",
            vec![entry(GroupEngine::BLOB_KEY, 3, 0, false, b"v3")],
        )]),
        Time(2),
    );
    assert_eq!(a.node_state(&NodeId::new("b")), Some(&b"v3"[..]));
    a.on_message(
        NodeId::new("b"),
        &delta_frame(vec![member_delta(
            "b",
            vec![entry(GroupEngine::BLOB_KEY, 1, 0, false, b"old")],
        )]),
        Time(3),
    );
    assert_eq!(a.node_state(&NodeId::new("b")), Some(&b"v3"[..]));
}

#[test]
fn a_node_authors_only_its_own_state() {
    let mut a = engine("a", &["b"]);
    a.apply(Command::SetLocalState(b"mine".to_vec()));
    assert_eq!(a.local_state(), b"mine");

    // A peer's delta claiming *our* state is out-versioned — we're the sole
    // author.
    a.on_message(
        NodeId::new("b"),
        &delta_frame(vec![member_delta(
            "a",
            vec![entry(GroupEngine::BLOB_KEY, 999, 0, false, b"forged")],
        )]),
        Time(1),
    );
    assert_eq!(a.local_state(), b"mine");
}

#[test]
fn state_and_liveness_merge_independently() {
    let mut a = engine("a", &["b"]);
    // Learn b alive with blob state at version 1.
    a.on_message(
        NodeId::new("b"),
        &delta_frame(vec![member_delta(
            "b",
            vec![entry(GroupEngine::BLOB_KEY, 1, 0, false, b"s1")],
        )]),
        Time(1),
    );
    // A pure liveness digest (suspect, same state version) must not wipe state.
    a.on_message(
        NodeId::new("c"),
        &digest_frame(vec![ndigest("b", 0, Status::Suspect, 1)], vec![]),
        Time(2),
    );
    assert_eq!(a.member_status(&NodeId::new("b")), Some(Status::Suspect));
    assert_eq!(
        a.node_state(&NodeId::new("b")),
        Some(&b"s1"[..]),
        "state survived a status change"
    );
}

#[test]
fn keys_version_independently() {
    let mut a = engine("a", &["b"]);
    a.on_message(
        NodeId::new("b"),
        &delta_frame(vec![member_delta(
            "b",
            vec![
                entry("x", 5, 0, false, b"x5"),
                entry("y", 1, 0, false, b"y1"),
            ],
        )]),
        Time(1),
    );
    // A fresher y does not disturb x; a stale x is ignored.
    a.on_message(
        NodeId::new("b"),
        &delta_frame(vec![member_delta(
            "b",
            vec![
                entry("x", 4, 0, false, b"stale"),
                entry("y", 2, 0, false, b"y2"),
            ],
        )]),
        Time(2),
    );
    assert_eq!(a.node_entry(&NodeId::new("b"), "x"), Some(&b"x5"[..]));
    assert_eq!(a.node_entry(&NodeId::new("b"), "y"), Some(&b"y2"[..]));
    let keys: Vec<&str> = a.node_entries(&NodeId::new("b")).map(|(k, _)| k).collect();
    assert_eq!(keys, ["x", "y"]);
}

#[test]
fn ttl_entries_expire_and_a_refresh_rearms() {
    let mut a = engine("a", &["b"]);
    a.on_message(
        NodeId::new("b"),
        &delta_frame(vec![member_delta(
            "b",
            vec![entry("hot", 1, 100, false, b"v1")],
        )]),
        Time(0),
    );
    assert!(a.node_entry(&NodeId::new("b"), "hot").is_some());
    // A fresher version at t=60 re-arms the expiry to t=160.
    a.on_message(
        NodeId::new("b"),
        &delta_frame(vec![member_delta(
            "b",
            vec![entry("hot", 2, 100, false, b"v2")],
        )]),
        Time(60),
    );
    a.on_tick(Time(120)); // old deadline passed, refreshed one hasn't
    assert_eq!(a.node_entry(&NodeId::new("b"), "hot"), Some(&b"v2"[..]));
    a.on_tick(Time(161));
    assert_eq!(
        a.node_entry(&NodeId::new("b"), "hot"),
        None,
        "expired after ttl"
    );
}

// ---------------------------------------------------------------------------
// Entry-TTL semantics, pinned.
//
// The coherence-lease tier (T3) reads a TTL'd entry as a *lease to serve*, and
// its safety argument is Gray–Cheriton: a writer invalidating a key waits for
// an ack from a reachable holder, or — for a holder that has gone silent — for
// that holder's lease to LAPSE. The second branch is a real bound only because
// the lapse is ended by the holder's own clock rather than by the holder
// learning anything, which makes these four properties of the existing entry
// machinery load-bearing rather than incidental:
//
//   * `membership.rs` `StateEntry::adopted` arms `expires_at = now + ttl_ms`
//     (`ttl_ms == 0` => `Time::MAX`, never) — so the TTL is a *duration* on
//     the wire and the deadline is the ADOPTING observer's, never the author's.
//   * `merge.rs` `merge_peer_delta`'s wins-check adopts an entry only when
//     `(version, tombstone, value)` is STRICTLY greater than what is held — so
//     re-gossip of a version already held cannot re-arm a lapse instant, and a
//     writer's bound cannot be pushed out by chatter it did not cause.
//   * the same check re-adopts on a strictly newer version, which re-arms —
//     that is how an author renews a lease.
//   * `liveness.rs` `reap_entries` drops an entry once `now >= expires_at` on
//     EVERY engine holding it, the author's own included — so the author is
//     not a privileged replica that keeps serving past its own lease.
//
// A regression in any of them silently converts the lease tier's bound back
// into a hope, so they are pinned here at the engine level.
// ---------------------------------------------------------------------------

/// (a) Adoption arms the lapse instant at `receiver_now + ttl_ms`, on the
/// receiver's clock — and the three no-lapse cases read `None`
/// (`StateEntry::adopted`: `ttl_ms == 0` arms `Time::MAX`; a tombstone never
/// expires on a TTL; an absent key has nothing to expire).
#[test]
fn adoption_arms_the_lapse_instant_on_the_receivers_clock() {
    let b = NodeId::new("b");
    let mut a = engine("a", &["b"]);

    // One delta, three shapes of entry, delivered at a known logical now.
    a.on_message(
        b.clone(),
        &delta_frame(vec![member_delta(
            "b",
            vec![
                entry("leased", 1, 100, false, b"v1"),
                entry("forever", 2, 0, false, b"static"),
                entry("gone", 3, 100, true, b""),
            ],
        )]),
        Time(40),
    );

    assert_eq!(
        a.node_entry_expires_at(&b, "leased"),
        Some(Time(140)),
        "the TTL arms against the ADOPTING node's clock (40 + 100)"
    );
    assert_eq!(
        a.node_entry_expires_at(&b, "forever"),
        None,
        "ttl_ms == 0 never expires, so there is no lapse instant to wait for"
    );
    assert_eq!(
        a.node_entry_expires_at(&b, "gone"),
        None,
        "a tombstoned key holds no servable copy"
    );
    assert_eq!(a.node_entry_expires_at(&b, "absent"), None);
    assert_eq!(a.node_entry_expires_at(&NodeId::new("zz"), "leased"), None);

    // The same write adopted LATER lapses later: the instant is the adopter's,
    // not the author's. This is the skew a writer must budget for.
    let mut c = engine("c", &["b"]);
    c.on_message(
        b.clone(),
        &delta_frame(vec![member_delta(
            "b",
            vec![entry("leased", 1, 100, false, b"v1")],
        )]),
        Time(90),
    );
    assert_eq!(c.node_entry_expires_at(&b, "leased"), Some(Time(190)));
}

/// (b) Re-gossip of a version already held does NOT re-arm the lapse instant.
///
/// `merge_peer_delta`'s wins-check is strictly-greater on
/// `(version, tombstone, value)`, so an identical entry is skipped before
/// `StateEntry::adopted` is ever called. Without this the anti-entropy round
/// itself would renew every lease it touched, and a writer waiting out a
/// silent holder would wait forever — the bound would not exist.
#[test]
fn a_same_version_regossip_does_not_rearm_the_lapse_instant() {
    let b = NodeId::new("b");
    let mut a = engine("a", &["b"]);
    let frame = delta_frame(vec![member_delta(
        "b",
        vec![entry("leased", 1, 100, false, b"v1")],
    )]);

    a.on_message(b.clone(), &frame, Time(40));
    assert_eq!(a.node_entry_expires_at(&b, "leased"), Some(Time(140)));

    // The identical entry arrives again, twice, well after the first adoption.
    a.on_message(b.clone(), &frame, Time(90));
    a.on_message(b.clone(), &frame, Time(130));
    assert_eq!(
        a.node_entry_expires_at(&b, "leased"),
        Some(Time(140)),
        "re-gossip of a held version must not push the lapse out"
    );

    // And the entry really does lapse on the original schedule — `expired` is
    // `now >= expires_at`, so the deadline instant itself is past the lease.
    a.on_tick(Time(140));
    assert_eq!(a.node_entry(&b, "leased"), None, "lapsed on schedule");
    assert_eq!(
        a.node_entry_expires_at(&b, "leased"),
        None,
        "a reaped entry has no lapse instant"
    );
}

/// (c) A strictly newer version DOES re-arm — how an author renews a lease.
///
/// The same wins-check in `merge_peer_delta` re-adopts on a higher version,
/// and `StateEntry::adopted` re-arms `expires_at` from the merge instant. A
/// stale version, arriving late, must not move it in either direction.
#[test]
fn a_newer_version_rearms_the_lapse_instant() {
    let b = NodeId::new("b");
    let mut a = engine("a", &["b"]);
    a.on_message(
        b.clone(),
        &delta_frame(vec![member_delta(
            "b",
            vec![entry("leased", 1, 100, false, b"v1")],
        )]),
        Time(40),
    );
    assert_eq!(a.node_entry_expires_at(&b, "leased"), Some(Time(140)));

    // A renewal at t=90 re-arms to 190 — strictly forward.
    a.on_message(
        b.clone(),
        &delta_frame(vec![member_delta(
            "b",
            vec![entry("leased", 2, 100, false, b"v2")],
        )]),
        Time(90),
    );
    assert_eq!(
        a.node_entry_expires_at(&b, "leased"),
        Some(Time(190)),
        "a strictly newer version re-arms from the adoption instant"
    );

    // A stale version arriving late is ignored, lapse instant included.
    a.on_message(
        b.clone(),
        &delta_frame(vec![member_delta(
            "b",
            vec![entry("leased", 1, 100, false, b"v1")],
        )]),
        Time(150),
    );
    assert_eq!(a.node_entry_expires_at(&b, "leased"), Some(Time(190)));

    // The old deadline passes with the renewed entry intact; the new one ends
    // it.
    a.on_tick(Time(140));
    assert_eq!(a.node_entry(&b, "leased"), Some(&b"v2"[..]));
    a.on_tick(Time(190));
    assert_eq!(a.node_entry(&b, "leased"), None);
    assert_eq!(a.node_entry_expires_at(&b, "leased"), None);
}

/// (d) The author's own copy expires on the author's engine.
///
/// `set_local_entry` goes through the same `StateEntry::adopted`, and
/// `reap_entries` walks every member including self — so the author is not a
/// privileged replica that keeps serving a key past its own lease. If it were,
/// a writer's lapse bound would hold for every node except the one most likely
/// to be asked.
#[test]
fn an_authors_own_entry_lapses_on_its_own_engine() {
    let a_id = NodeId::new("a");
    let mut a = engine("a", &["b"]);
    // A command carries no clock of its own; it stamps from `now_hint`, the
    // freshest time the engine has been told about.
    let _ = a.on_tick(Time(1_000));

    let _ = a.apply(Command::SetLocalEntry {
        key: "leased".into(),
        value: b"mine".to_vec(),
        ttl_ms: Some(200),
    });
    assert_eq!(a.node_entry(&a_id, "leased"), Some(&b"mine"[..]));
    assert_eq!(
        a.node_entry_expires_at(&a_id, "leased"),
        Some(Time(1_200)),
        "the author arms its own copy on its own clock too"
    );

    // Short of the deadline it still holds.
    a.on_tick(Time(1_199));
    assert_eq!(a.node_entry(&a_id, "leased"), Some(&b"mine"[..]));

    // At the deadline the author reaps its own entry, and says so.
    let effects = a.on_tick(Time(1_200));
    assert!(
        effects.iter().any(|e| matches!(
            e,
            Effect::NodeStateChanged { node, key } if *node == a_id && key == "leased"
        )),
        "a lapse is an observable state change on the author too"
    );
    assert_eq!(
        a.node_entry(&a_id, "leased"),
        None,
        "the author's copy lapsed"
    );
    assert_eq!(a.node_entry_expires_at(&a_id, "leased"), None);

    // A no-TTL local write has no lapse instant at all.
    let _ = a.apply(Command::SetLocalEntry {
        key: "forever".into(),
        value: b"mine".to_vec(),
        ttl_ms: None,
    });
    assert_eq!(a.node_entry_expires_at(&a_id, "forever"), None);
}

#[test]
fn a_truncated_delta_triggers_a_continuation_request() {
    let mut a = engine("a", &["b"]);
    // An eager frame teaches `a` that b's high-water is 3 while carrying
    // only the newest entry — holes below it.
    a.on_message(
        NodeId::new("b"),
        &delta_frame(vec![member_delta(
            "b",
            vec![entry("k3", 3, 0, false, b"v3")],
        )]),
        Time(0),
    );
    // A backfill arrives truncated: entries through v1, advertised max 1,
    // below our stored high-water of 3 — the merge must ask for the rest.
    let effects = a.on_message(
        NodeId::new("b"),
        &delta_frame(vec![member_delta(
            "b",
            vec![entry("k1", 1, 0, false, b"v1")],
        )]),
        Time(1),
    );
    let request = effects
        .iter()
        .find_map(|e| match e {
            Effect::Send { wire, .. } => wire::decode(wire),
            _ => None,
        })
        .expect("a continuation frame");
    assert!(matches!(request.kind, wire::Kind::DeltaRequest));
    assert_eq!(request.wants.len(), 1);
    assert_eq!(request.wants[0].node, NodeId::new("b"));
    assert_eq!(request.wants[0].have_version, 1);

    // A frame that matches our stored high-water requests nothing.
    let effects = a.on_message(
        NodeId::new("b"),
        &delta_frame(vec![member_delta(
            "b",
            vec![
                entry("k2", 2, 0, false, b"v2"),
                entry("k3", 3, 0, false, b"v3"),
            ],
        )]),
        Time(2),
    );
    assert!(
        !effects.iter().any(|e| matches!(e, Effect::Send { .. })),
        "no continuation once holdings match the advertised high-water"
    );
}

#[test]
fn a_local_write_eagerly_pushes_a_delta_to_fanout_peers() {
    let mut a = engine("a", &["b"]);
    let effects = a.apply(Command::SetLocalEntry {
        key: "k".into(),
        value: b"v1".to_vec(),
        ttl_ms: None,
    });
    let wires: Vec<Vec<u8>> = effects
        .iter()
        .filter_map(|e| match e {
            Effect::Send { wire, .. } => Some(wire.clone()),
            _ => None,
        })
        .collect();
    assert!(!wires.is_empty(), "the write must emit eager delta frames");
    let frame = wire::decode(&wires[0]).expect("decodes");
    assert!(matches!(frame.kind, wire::Kind::Delta));
    let m = frame
        .members
        .iter()
        .find(|m| m.node.as_str() == "a")
        .expect("self delta");
    assert!(m.entries.iter().any(|e| e.key == "k" && e.value == b"v1"));

    // A peer adopts it with no tick and no digest exchange: the write
    // travels at network latency, not gossip cadence.
    let mut b = engine("b", &["a"]);
    b.on_message(NodeId::new("a"), &wires[0], Time(1));
    assert_eq!(b.node_entry(&NodeId::new("a"), "k"), Some(&b"v1"[..]));
}

#[test]
fn eager_push_carries_only_the_newest_change_including_tombstones() {
    let mut a = engine("a", &["b"]);
    a.apply(Command::SetLocalEntry {
        key: "old".into(),
        value: b"x".to_vec(),
        ttl_ms: None,
    });
    let effects = a.apply(Command::DeleteLocalEntry { key: "old".into() });
    let bytes = effects
        .iter()
        .find_map(|e| match e {
            Effect::Send { wire, .. } => Some(wire.clone()),
            _ => None,
        })
        .expect("eager frame");
    let frame = wire::decode(&bytes).expect("decodes");
    let m = frame
        .members
        .iter()
        .find(|m| m.node.as_str() == "a")
        .expect("self delta");
    assert_eq!(
        m.entries.len(),
        1,
        "exactly the newest change rides the eager frame"
    );
    assert!(m.entries[0].tombstone && m.entries[0].key == "old");
}

#[test]
fn eager_push_can_be_disabled() {
    let mut a = GroupEngine::new(
        GroupId::new("g"),
        NodeId::new("a"),
        [NodeId::new("b")],
        Config {
            eager_push: false,
            ..Config::default()
        },
    );
    let effects = a.apply(Command::SetLocalEntry {
        key: "k".into(),
        value: b"v".to_vec(),
        ttl_ms: None,
    });
    assert!(
        !effects.iter().any(|e| matches!(e, Effect::Send { .. })),
        "no unsolicited frames when disabled"
    );
}

#[test]
fn delete_offers_a_tombstone_in_a_delta_then_reaps_it_without_resurrection() {
    let mut a = engine("a", &["b"]);
    a.apply(Command::SetLocalEntry {
        key: "k".into(),
        value: b"v".to_vec(),
        ttl_ms: None,
    });
    a.apply(Command::DeleteLocalEntry { key: "k".into() });
    assert_eq!(
        a.node_entry(&NodeId::new("a"), "k"),
        None,
        "deleted locally"
    );

    // A peer requesting our full state gets the tombstone in a delta (so it
    // drops the key too).
    let req = wire::encode(&wire::Frame {
        kind: wire::Kind::DeltaRequest,
        group: GroupId::new("g"),
        target: None,
        digest: vec![],
        wants: vec![wire::NodeWant {
            node: NodeId::new("a"),
            have_version: 0,
        }],
        members: vec![],
        metadata: vec![],
        lead: None,
    });
    let delta = wire::decode(
        &a.on_message(NodeId::new("b"), &req, Time(1_000))
            .iter()
            .find_map(|e| match e {
                Effect::Send { wire, .. } => Some(wire.clone()),
                _ => None,
            })
            .expect("a delta response"),
    )
    .expect("decodes");
    let m = delta
        .members
        .iter()
        .find(|m| m.node.as_str() == "a")
        .expect("self member");
    assert!(
        m.entries.iter().any(|e| e.key == "k" && e.tombstone),
        "tombstone offered"
    );
    let hwm = m.max_version;

    // After 2× dead_timeout the tombstone is reaped and no longer offered,
    // but the high-water mark is preserved — so a request can never resurrect
    // it.
    let far = Time(1_000 + Config::default().dead_timeout_ms * 2 + 1);
    a.on_tick(far);
    let delta = wire::decode(
        &a.on_message(NodeId::new("b"), &req, far.saturating_add(1))
            .iter()
            .find_map(|e| match e {
                Effect::Send { wire, .. } => Some(wire.clone()),
                _ => None,
            })
            .expect("a delta response"),
    )
    .expect("decodes");
    let m = delta
        .members
        .iter()
        .find(|m| m.node.as_str() == "a")
        .expect("self member");
    assert!(!m.entries.iter().any(|e| e.key == "k"), "tombstone reaped");
    assert!(
        m.max_version >= hwm,
        "high-water preserved across reap (no resurrection)"
    );
}

/// The equal-version restart hazard: both lives author a key at version 1
/// with different values (a reboot reusing its version clock, e.g. `~addr`).
/// The new life must OUT-VERSION the echoed old value — never ignore it —
/// or receivers tiebreak arbitrarily and can wedge on the dead value.
#[test]
fn restart_out_versions_an_equal_version_echo_with_a_different_value() {
    let mut a = engine("a", &["b"]);
    let _ = a.start(Time(0));
    // This boot authors ~addr at version 1 (fresh clock).
    let _ = a.apply(Command::SetLocalEntry {
        key: "~addr".into(),
        value: b"10.0.0.9:7946".to_vec(),
        ttl_ms: None,
    });

    // A peer echoes the PREVIOUS life's ~addr — same version, dead value.
    let echo = delta_frame(vec![member_delta(
        "a",
        vec![entry("~addr", 1, 0, false, b"10.0.0.4:7946")],
    )]);
    let _ = a.on_message(NodeId::new("b"), &echo, Time(10));

    let value = a
        .node_entry(&NodeId::new("a"), "~addr")
        .expect("entry present");
    assert_eq!(value, b"10.0.0.9:7946", "our value survives the echo");
    // The real assertion: the digest-visible version must now EXCEED the
    // echo, so every receiver's LWW converges on this life's value.
    let effects = a.on_tick(Time(200));
    let digest = decode_one_digest(&effects);
    let summary = digest
        .digest
        .iter()
        .find(|d| d.node == NodeId::new("a"))
        .expect("self summary");
    assert!(
        summary.max_version >= 2,
        "authored key must be out-versioned past the equal-version echo (got {})",
        summary.max_version
    );
}

#[test]
fn restart_adopts_echoed_entries_for_unauthored_keys() {
    // Fresh engine (a restart): a peer echoes entries we authored last boot.
    let mut a = engine("a", &["b"]);
    let effects = a.on_message(
        NodeId::new("b"),
        &delta_frame(vec![member_delta(
            "a",
            vec![entry("addr", 7, 0, false, b"10.0.0.1")],
        )]),
        Time(1),
    );
    // Adopted verbatim (NOT wiped by out-versioning with emptiness)...
    assert_eq!(
        a.node_entry(&NodeId::new("a"), "addr"),
        Some(&b"10.0.0.1"[..])
    );
    // ...with a change event so the app can re-author.
    assert!(effects.iter().any(|e| matches!(
        e,
        Effect::NodeStateChanged { node, key } if node.as_str() == "a" && key == "addr"
    )));
    // A post-restart local write supersedes the adopted version everywhere.
    a.apply(Command::SetLocalEntry {
        key: "addr".into(),
        value: b"10.0.0.2".to_vec(),
        ttl_ms: None,
    });
    assert_eq!(
        a.node_entry(&NodeId::new("a"), "addr"),
        Some(&b"10.0.0.2"[..])
    );

    // And once authored this boot, echoes can never replace it (sole-author
    // rule): the forged/echoed 999 only bumps our version past it.
    a.on_message(
        NodeId::new("b"),
        &delta_frame(vec![member_delta(
            "a",
            vec![entry("addr", 999, 0, false, b"forged")],
        )]),
        Time(2),
    );
    assert_eq!(
        a.node_entry(&NodeId::new("a"), "addr"),
        Some(&b"10.0.0.2"[..])
    );
}
