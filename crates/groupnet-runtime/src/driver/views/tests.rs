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
    assert!(
        entries
            .iter()
            .all(|(node, kv)| !kv.is_empty() && engine.member_status(node).is_some()),
        "entries published for a node that has none, or is forgotten"
    );
    assert!(wiring.members.borrow().roster().iter().eq(engine.members()));
    assert!(
        entries
            .all()
            .keys()
            .eq(entries.iter().map(|(node, _)| node))
    );
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

/// A splitmix64 stream for the random histories.
struct Draws(u64);

impl Draws {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let z = (self.0 ^ (self.0 >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        let z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn pick<T: Copy>(&mut self, of: &[T]) -> T {
        of[usize::try_from(self.below(of.len() as u64)).expect("small")]
    }
}

const PEERS: u64 = 12;

fn peer(i: u64) -> NodeId {
    NodeId::new(format!("p{i:02}"))
}

fn frame(
    kind: wire::Kind,
    digest: Vec<wire::NodeDigest>,
    members: Vec<wire::MemberDelta>,
) -> Vec<u8> {
    wire::encode(&wire::Frame {
        kind,
        group: GroupId::new("g"),
        target: None,
        digest,
        wants: Vec::new(),
        members,
        metadata: Vec::new(),
        lead: None,
    })
}

/// One random step that is not a tick — the operation mix of
/// `groupnet-core`'s `engine::index::tests::step`, through the public API:
/// liveness claims (suspicions, refutations, deaths, rejoins, about a peer or
/// about us), peer entries with and without TTLs and tombstones, local
/// writes with TTLs and deletes, `AddPeer`, contacts, and an answer to the
/// last probe.
fn step(e: &mut GroupEngine, draws: &mut Draws, now: Time, probed: Option<&NodeId>) -> Vec<Effect> {
    let statuses = [Status::Alive, Status::Suspect, Status::Dead];
    match draws.below(10) {
        0..=2 => {
            let digest = (0..=draws.below(3))
                .map(|_| {
                    let i = draws.below(PEERS + 1);
                    wire::NodeDigest {
                        node: if i == PEERS {
                            NodeId::new("a")
                        } else {
                            peer(i)
                        },
                        incarnation: draws.below(4),
                        status: draws.pick(&statuses).to_wire(),
                        max_version: 0,
                        content_hash: 0,
                    }
                })
                .collect();
            let from = peer(draws.below(PEERS));
            e.on_message(from, &frame(wire::Kind::Digest, digest, Vec::new()), now)
        }
        3 | 4 => {
            let entries: Vec<wire::EntryDelta> = (0..=draws.below(2))
                .map(|_| wire::EntryDelta {
                    key: format!("k{}", draws.below(5)),
                    version: draws.below(20) + 1,
                    ttl_ms: draws.pick(&[0, 100, 700, 3_000]),
                    tombstone: draws.below(4) == 0,
                    value: vec![u8::try_from(draws.below(3)).expect("small")],
                })
                .collect();
            let delta = wire::MemberDelta {
                node: peer(draws.below(PEERS)),
                incarnation: draws.below(4),
                status: draws.pick(&statuses).to_wire(),
                max_version: entries.iter().map(|e| e.version).max().unwrap_or(0),
                entries,
            };
            let from = peer(draws.below(PEERS));
            e.on_message(
                from,
                &frame(wire::Kind::Delta, Vec::new(), vec![delta]),
                now,
            )
        }
        5 => e.apply(Command::SetLocalEntry {
            key: format!("k{}", draws.below(5)),
            value: vec![1],
            ttl_ms: draws.pick(&[None, Some(150), Some(900)]),
        }),
        6 => e.apply(Command::DeleteLocalEntry {
            key: format!("k{}", draws.below(5)),
        }),
        7 => e.apply(Command::AddPeer(peer(draws.below(PEERS)))),
        8 => {
            let contacts = (0..draws.below(3))
                .map(|_| peer(draws.below(PEERS) + PEERS))
                .collect();
            e.apply(Command::SetBootstrapContacts(contacts))
        }
        _ => probed.map_or_else(Vec::new, |target| {
            e.on_message(
                target.clone(),
                &frame(wire::Kind::Ack, Vec::new(), Vec::new()),
                now,
            )
        }),
    }
}

/// The last member `effects` sent a direct probe to, if any.
fn pinged(effects: &[Effect]) -> Option<NodeId> {
    effects.iter().rev().find_map(|effect| match effect {
        Effect::Send { to, wire }
            if wire::decode(wire).is_some_and(|f| f.kind == wire::Kind::Ping) =>
        {
            Some(to.clone())
        }
        _ => None,
    })
}

/// Over random histories — every liveness change, entry write, TTL expiry,
/// tombstone reap, member reaped and later learned again, and clock jump the
/// step mix makes — the views published after every step equal the
/// engine's state read in full.
#[test]
fn published_views_follow_every_random_history() {
    let config = Config {
        probe_interval_ms: 100,
        probe_timeout_ms: 50,
        suspect_timeout_ms: 300,
        dead_timeout_ms: 1_000,
        gossip_interval_ms: 200,
        ..Config::default()
    };
    let mut reaped = 0;
    for seed in 0..32 {
        let mut draws = Draws(seed);
        let mut engine = GroupEngine::new(
            GroupId::new("g"),
            NodeId::new("a"),
            [peer(0)],
            config.clone(),
        )
        .expect("valid config");
        let mut views = Views::new(&engine);
        let wiring = wiring(&engine, &views);
        let mut now = Time(1);
        let mut probed = pinged(&engine.start(now));
        for _ in 0..600 {
            let effects = if draws.below(3) == 0 {
                let jump = if draws.below(8) == 0 {
                    3_000 + draws.below(27_000)
                } else {
                    1 + draws.below(400)
                };
                now = Time(now.0 + jump);
                let before = engine.member_statuses().count();
                let effects = engine.on_tick(now);
                reaped += before.saturating_sub(engine.member_statuses().count());
                effects
            } else {
                step(&mut engine, &mut draws, now, probed.as_ref())
            };
            probed = pinged(&effects).or(probed);
            publish(&engine, &wiring, &mut views, &effects);
            assert_published(&engine, &wiring);
        }
    }
    assert!(
        reaped > 100,
        "too few members reaped to exercise forgetting: {reaped}"
    );
}

/// More members forgotten between two publishes than the engine records:
/// the views are rebuilt from the engine, all three republished.
#[test]
fn a_publish_after_more_reaps_than_recorded_rebuilds() {
    let mut engine = crowded();
    let mut views = Views::new(&engine);
    let mut wiring = wiring(&engine, &views);
    let dead: Vec<wire::NodeDigest> = (0..5_000)
        .map(|i| wire::NodeDigest {
            node: NodeId::new(format!("d{i:05}")),
            incarnation: 0,
            status: Status::Dead.to_wire(),
            max_version: 0,
            content_hash: 0,
        })
        .collect();
    let effects = engine.on_message(
        id(1),
        &frame(wire::Kind::Digest, dead, Vec::new()),
        Time(10),
    );
    publish(&engine, &wiring, &mut views, &effects);
    assert_published(&engine, &wiring);
    let since = engine.change_clock();
    wiring.members.mark_unchanged();
    wiring.statuses.mark_unchanged();
    wiring.entries.mark_unchanged();

    let effects = engine.on_tick(Time(10 + 2 * Config::default().dead_timeout_ms));
    assert!(
        engine.members_forgotten_since(since).is_none(),
        "the record ran out"
    );
    publish(&engine, &wiring, &mut views, &effects);
    assert!(wiring.members.has_changed().expect("open"));
    assert!(wiring.statuses.has_changed().expect("open"));
    assert!(wiring.entries.has_changed().expect("open"));
    assert_published(&engine, &wiring);
    assert_eq!(wiring.statuses.borrow().len(), N, "every tombstone reaped");
}
