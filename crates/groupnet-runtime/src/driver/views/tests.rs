//! What a publish costs, and that it publishes what a full rebuild would.

use std::collections::BTreeMap;
use std::sync::Arc;

use groupnet_core::{Command, Config, Effect, GroupEngine, GroupId, NodeId, Status, Time, wire};
use tokio::sync::{broadcast, watch};

use super::super::{Publishers, publish_actor_views};
use super::Views;
use crate::group::Leadership;
use crate::pmap::NODES_BUILT;

const N: usize = 1_000;

fn id(i: usize) -> NodeId {
    NodeId::new(format!("n{i:04}"))
}

/// An engine that knows `N - 1` live peers, added out of band.
fn crowded() -> GroupEngine {
    let mut e =
        GroupEngine::new(GroupId::new("g"), id(0), [], Config::default()).expect("valid config");
    for i in 1..N {
        e.apply(Command::AddPeer(id(i)));
    }
    e
}

/// Publishers seeded from `views`, with the receivers that watch them.
struct Wiring {
    publishers: Publishers,
    members: watch::Receiver<super::MembersSnapshot>,
    statuses: watch::Receiver<super::StatusesSnapshot>,
    entries: watch::Receiver<super::NodeEntriesSnapshot>,
}

fn wiring(engine: &GroupEngine, views: &Views) -> Wiring {
    let (members_tx, members) = watch::channel(views.members());
    let (statuses_tx, statuses) = watch::channel(views.statuses());
    let (entries_tx, entries) = watch::channel(views.entries());
    let (epoch, host) = engine.leadership();
    Wiring {
        publishers: Publishers {
            coordinator: watch::channel(engine.coordinator().cloned()).0,
            leadership: watch::channel(Leadership::observed(epoch, host.cloned(), engine.local()))
                .0,
            metadata: watch::channel(Arc::new(BTreeMap::new())).0,
            members: members_tx,
            statuses: statuses_tx,
            entries: entries_tx,
            net_stats: watch::channel(engine.net_stats()).0,
            resign_pending: watch::channel(0).0,
            events: broadcast::channel(16).0,
        },
        members,
        statuses,
        entries,
    }
}

/// One digest line claiming `node` holds `status` at incarnation 0.
fn claim(node: &NodeId, status: Status) -> Vec<u8> {
    wire::encode(&wire::Frame {
        kind: wire::Kind::Digest,
        group: GroupId::new("g"),
        target: None,
        digest: vec![wire::NodeDigest {
            node: node.clone(),
            incarnation: 0,
            status: status.to_wire(),
            max_version: 0,
            content_hash: 0,
        }],
        wants: Vec::new(),
        members: Vec::new(),
        metadata: Vec::new(),
        lead: None,
    })
}

/// Publishes `effects` and returns how many member records it wrote.
fn publish(engine: &GroupEngine, wiring: &Wiring, views: &mut Views, effects: &[Effect]) -> u64 {
    let before = NODES_BUILT.with(std::cell::Cell::get);
    publish_actor_views(engine, &wiring.publishers, None, &mut None, views, effects);
    NODES_BUILT.with(std::cell::Cell::get) - before
}

/// The published views equal the engine's state, read in full.
fn assert_published(engine: &GroupEngine, wiring: &Wiring) {
    assert!(wiring.members.borrow().iter().eq(engine.members()));
    assert!(
        wiring
            .statuses
            .borrow()
            .iter()
            .map(|(node, (status, since))| (node, *status, *since))
            .eq(engine.member_statuses_since())
    );
    let entries = wiring.entries.borrow();
    for (node, _) in engine.member_statuses() {
        let held: Vec<(&str, &[u8])> = engine.node_entries(node).collect();
        let published: Vec<(&str, &[u8])> = entries
            .get(node)
            .map(|kv| kv.iter().map(|(k, v)| (k.as_str(), v.as_slice())).collect())
            .unwrap_or_default();
        assert_eq!(published, held, "{node}'s entries");
    }
}

/// In a 1,000-member group, one member's status change republishes by
/// copying only the path to it — the status map's nodes on the way to that
/// member — and a batch that changes nothing sends nothing.
///
/// Measured (member records written or copied): at f71405f, which rebuilt
/// the status map, the live-member slice and a set of every member on each
/// membership change, 3,000 for a suspicion and 2,999 for a death. Now 10
/// and 19: the status map's path, plus the live map's for the death.
#[test]
fn one_status_change_writes_only_its_path() {
    let mut engine = crowded();
    let mut views = Views::new(&engine);
    let wiring = wiring(&engine, &views);
    let subject = id(N / 2);

    let effects = engine.on_message(id(1), &claim(&subject, Status::Suspect), Time(10));
    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::MembershipChanged))
    );
    let written = publish(&engine, &wiring, &mut views, &effects);
    assert!(wiring.statuses.has_changed().expect("open"));
    assert!(
        !wiring.members.has_changed().expect("open"),
        "a suspect stays live"
    );
    assert_published(&engine, &wiring);
    let bound = 2 * u64::from(N.bit_width());
    assert!(
        written <= bound,
        "one status change wrote {written} member records, above {bound}"
    );

    // A death leaves the live set: the live map's path is copied too.
    let effects = engine.on_message(id(1), &claim(&subject, Status::Dead), Time(10));
    let written = publish(&engine, &wiring, &mut views, &effects);
    assert!(wiring.members.has_changed().expect("open"));
    assert_published(&engine, &wiring);
    assert!(
        written <= 2 * bound,
        "one death wrote {written} member records"
    );

    // The tombstone's reap is learned from the engine's forgotten members.
    let reap = Time(10 + 2 * Config::default().dead_timeout_ms);
    let effects = engine.on_tick(reap);
    assert_eq!(engine.member_status(&subject), None, "reaped");
    publish(&engine, &wiring, &mut views, &effects);
    assert!(wiring.statuses.borrow().get(&subject).is_none());
    assert_published(&engine, &wiring);

    // An entry write touches that node's entries alone.
    let effects = engine.apply(Command::SetLocalEntry {
        key: "k".into(),
        value: vec![1],
        ttl_ms: None,
    });
    let written = publish(&engine, &wiring, &mut views, &effects);
    assert!(wiring.entries.has_changed().expect("open"));
    assert_published(&engine, &wiring);
    assert!(
        written <= 2 * bound,
        "one entry write wrote {written} member records"
    );

    // Nothing changed: nothing written, nothing sent.
    let mut wiring = wiring;
    wiring.members.mark_unchanged();
    wiring.statuses.mark_unchanged();
    wiring.entries.mark_unchanged();
    let written = publish(&engine, &wiring, &mut views, &[]);
    assert_eq!(written, 0);
    assert!(!wiring.members.has_changed().expect("open"));
    assert!(!wiring.statuses.has_changed().expect("open"));
    assert!(!wiring.entries.has_changed().expect("open"));
}
