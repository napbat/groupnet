//! The index against the full walks it replaced: random sequences of
//! liveness claims, entry writes and deletes, local commands and clock jumps,
//! with every tick's reaps checked against a walk of every member and entry.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Excluded, Unbounded};

use crate::membership::Member;
use crate::{Command, Config, Effect, GroupEngine, GroupId, NodeId, Status, Time, wire};

use super::super::spread;
use super::super::state::PeerDigest;

/// A splitmix64 stream for the schedules.
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

    fn status(&mut self) -> Status {
        [Status::Alive, Status::Suspect, Status::Dead]
            [usize::try_from(self.below(3)).expect("small")]
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

/// What a tick at `now` reaps, found the way 7ee9940 found it: a walk of
/// every member and entry.
#[derive(Debug, Default)]
struct Walk {
    /// Suspects past their window, in id order.
    suspects: Vec<NodeId>,
    /// `Dead` tombstones past their reap, in id order.
    dead: Vec<NodeId>,
    /// Every entry due (expired, or a tombstone past its reap) of a member
    /// that stays.
    entries: BTreeSet<(NodeId, String)>,
    /// The expired live entries among them, in id and key order.
    expired: Vec<(NodeId, String)>,
}

fn walk(e: &GroupEngine, now: Time) -> Walk {
    let reap_after = e.reap_after();
    let mut out = Walk::default();
    for (node, m) in &e.members {
        if *node != e.local {
            match m.status {
                Status::Suspect
                    if now >= m.suspect_since.saturating_add(e.config.suspect_timeout_ms) =>
                {
                    out.suspects.push(node.clone());
                }
                Status::Dead if now >= m.dead_since.saturating_add(reap_after) => {
                    out.dead.push(node.clone());
                    continue;
                }
                _ => {}
            }
        }
        for (key, entry) in &m.entries {
            if now >= entry.reap_due(reap_after) {
                out.entries.insert((node.clone(), key.clone()));
                if !entry.tombstone {
                    out.expired.push((node.clone(), key.clone()));
                }
            }
        }
    }
    out
}

/// Every member with the keys it holds.
fn holdings(e: &GroupEngine) -> BTreeMap<NodeId, BTreeSet<String>> {
    e.members
        .iter()
        .map(|(node, m)| (node.clone(), m.entries.keys().cloned().collect()))
        .collect()
}

/// One random step that is not a tick.
fn step(e: &mut GroupEngine, draws: &mut Draws, now: Time) {
    let about = |draws: &mut Draws| {
        let i = draws.below(PEERS + 1);
        if i == PEERS {
            NodeId::new("a")
        } else {
            peer(i)
        }
    };
    match draws.below(10) {
        // Liveness claims: suspicions, refutations, deaths, rejoins at a
        // higher incarnation — about a peer or about us.
        0..=2 => {
            let digest = (0..=draws.below(3))
                .map(|_| wire::NodeDigest {
                    node: about(draws),
                    incarnation: draws.below(4),
                    status: draws.status().to_wire(),
                    max_version: 0,
                    content_hash: 0,
                })
                .collect();
            e.on_message(
                peer(draws.below(PEERS)),
                &frame(wire::Kind::Digest, digest, Vec::new()),
                now,
            );
        }
        // A peer's entries, with and without a TTL, and tombstones.
        3 | 4 => {
            let entries: Vec<wire::EntryDelta> = (0..=draws.below(2))
                .map(|_| wire::EntryDelta {
                    key: format!("k{}", draws.below(5)),
                    version: draws.below(20) + 1,
                    ttl_ms: [0, 100, 700, 3_000][usize::try_from(draws.below(4)).expect("small")],
                    tombstone: draws.below(4) == 0,
                    value: vec![u8::try_from(draws.below(3)).expect("small")],
                })
                .collect();
            let delta = wire::MemberDelta {
                node: peer(draws.below(PEERS)),
                incarnation: draws.below(4),
                status: draws.status().to_wire(),
                max_version: entries.iter().map(|e| e.version).max().unwrap_or(0),
                entries,
            };
            e.on_message(
                peer(draws.below(PEERS)),
                &frame(wire::Kind::Delta, Vec::new(), vec![delta]),
                now,
            );
        }
        5 => {
            let ttl_ms =
                [None, Some(150), Some(900)][usize::try_from(draws.below(3)).expect("small")];
            e.apply(Command::SetLocalEntry {
                key: format!("k{}", draws.below(5)),
                value: vec![1],
                ttl_ms,
            });
        }
        6 => {
            e.apply(Command::DeleteLocalEntry {
                key: format!("k{}", draws.below(5)),
            });
        }
        7 => {
            e.apply(Command::AddPeer(peer(draws.below(PEERS))));
        }
        8 => {
            let contacts = (0..draws.below(3))
                .map(|_| peer(draws.below(PEERS) + PEERS))
                .collect();
            e.apply(Command::SetBootstrapContacts(contacts));
        }
        _ => {
            // The probed member answers, so not every probe ends a suspicion.
            if let Some(target) = e.pending.as_ref().map(|p| p.target.clone()) {
                e.on_message(target, &frame(wire::Kind::Ack, Vec::new(), Vec::new()), now);
            }
        }
    }
}

/// A tick at `now` reaps exactly what a full walk finds due — the same
/// suspects declared `Dead`, the same tombstones forgotten, the same
/// entries dropped and the same expiries announced in the same order — and
/// leaves the index equal to a fresh scan, its earliest deadline after `now`.
fn tick_matches_the_walk(e: &mut GroupEngine, now: Time, seed: u64, step: u64) {
    let expect = walk(e, now);
    let before = holdings(e);
    let effects = e.on_tick(now);
    let expired: Vec<(NodeId, String)> = effects
        .iter()
        .filter_map(|effect| match effect {
            Effect::NodeStateChanged { node, key } => Some((node.clone(), key.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(expired, expect.expired, "seed {seed} step {step}: expiries");
    for node in &expect.suspects {
        assert_eq!(
            e.member_status_since(node).map(|(status, _)| status),
            Some(Status::Dead),
            "seed {seed} step {step}: suspect {node} outlived its window"
        );
        assert_eq!(
            e.members[node].dead_since, now,
            "seed {seed} step {step}: {node}"
        );
    }
    let mut left = before;
    for node in &expect.dead {
        left.remove(node);
    }
    for (node, key) in &expect.entries {
        if let Some(keys) = left.get_mut(node) {
            keys.remove(key);
        }
    }
    assert_eq!(
        holdings(e),
        left,
        "seed {seed} step {step}: members and entries kept"
    );
    e.assert_index();
    assert!(
        e.slow_deadline() > now,
        "seed {seed} step {step}: a reap still due after the tick"
    );
}

/// The index-driven reaps are the full walks' over random histories, and the
/// index equals a fresh scan after every step (`next_deadline` asserts it in
/// this crate's test builds; this test also checks it explicitly).
#[test]
fn indexed_reaps_match_a_full_walk() {
    let config = Config {
        probe_interval_ms: 100,
        probe_timeout_ms: 50,
        suspect_timeout_ms: 300,
        dead_timeout_ms: 1_000,
        gossip_interval_ms: 200,
        ..Config::default()
    };
    for seed in 0..32 {
        let mut draws = Draws(seed);
        let mut e = GroupEngine::new(
            GroupId::new("g"),
            NodeId::new("a"),
            [peer(0)],
            config.clone(),
        )
        .expect("valid config");
        let mut now = Time(1);
        e.start(now);
        for i in 0..600 {
            match draws.below(16) {
                0..=9 => step(&mut e, &mut draws, now),
                // Now and then the node leaves: it is `Dead` to itself, and
                // the live set and the coordinator follow.
                10 if draws.below(40) == 0 => {
                    e.apply(Command::Leave);
                }
                _ => {
                    let jump = if draws.below(8) == 0 {
                        3_000 + draws.below(27_000)
                    } else {
                        1 + draws.below(400)
                    };
                    now = Time(now.0 + jump);
                    tick_matches_the_walk(&mut e, now, seed, i);
                }
            }
            e.assert_index();
        }
    }
}

/// The digest builder as f71405f had it: collect every member changed since
/// the peer's cursor, select the lead from all of them, then walk the
/// window. The oracle the index-driven builder is checked against.
fn old_digest_for(e: &GroupEngine, to: &NodeId, now: Time) -> (Vec<NodeId>, bool, PeerDigest) {
    let window = e.config.digest_window.get();
    let prev = e.digest_peers.get(to);
    let visits = prev.map_or(0, |p| p.visits);
    let cursor = prev.map_or(0, |p| p.cursor);
    let mut peer = PeerDigest {
        visits: visits + 1,
        cursor: e.change_clock,
        resume: prev.and_then(|p| p.resume.clone()),
    };
    let gossipable = || e.members.iter().filter(|(_, m)| e.should_gossip(m, now));
    let mut listed: Vec<(&NodeId, &Member)> = gossipable()
        .filter(|(_, m)| m.changed_at > cursor)
        .collect();
    let ids =
        |listed: Vec<(&NodeId, &Member)>| listed.into_iter().map(|(n, _)| n.clone()).collect();
    if !visits.is_multiple_of(e.config.full_digest_every.get()) && listed.len() <= window {
        return (ids(listed), false, peer);
    }
    let hot_ms = e.liveness_hot_ms();
    let rank = |entry: &(&NodeId, &Member)| {
        let (node, m) = *entry;
        let hot = now.0.saturating_sub(m.liveness_since.0) < hot_ms;
        (
            *node != e.local,
            !hot,
            Reverse(if hot { m.liveness_since } else { Time::ZERO }),
            Reverse(m.changed_at),
        )
    };
    let lead_cap = window / 2;
    if lead_cap == 0 {
        listed.clear();
    } else if listed.len() > lead_cap {
        listed.select_nth_unstable_by_key(lead_cap - 1, rank);
        listed.truncate(lead_cap);
    }
    listed.sort_unstable_by_key(rank);
    let lead = listed.len();
    if let Some(after) = peer.resume.clone() {
        let walk = e
            .members
            .range((Excluded(&after), Unbounded))
            .chain(e.members.range(..=&after))
            .filter(|(_, m)| e.should_gossip(m, now));
        old_fill(&mut listed, lead, window, walk, &mut peer.resume);
    } else {
        let len = gossipable().count();
        if len > 0 {
            let start = spread::index(spread::pair_hash(&e.local, to), len);
            let walk = gossipable().skip(start).chain(gossipable().take(start));
            old_fill(&mut listed, lead, window, walk, &mut peer.resume);
        }
    }
    (ids(listed), true, peer)
}

/// The old builder's window fill: a linear scan of the lead per member.
fn old_fill<'a>(
    listed: &mut Vec<(&'a NodeId, &'a Member)>,
    lead: usize,
    window: usize,
    walk: impl Iterator<Item = (&'a NodeId, &'a Member)>,
    resume: &mut Option<NodeId>,
) {
    for (node, m) in walk {
        if listed.len() >= window {
            break;
        }
        *resume = Some(node.clone());
        if listed[..lead].iter().any(|(led, _)| *led == node) {
            continue;
        }
        listed.push((node, m));
    }
}

/// The index-driven digest builder lists exactly what the old one did — the
/// same members in the same order, full or delta, with the same peer
/// bookkeeping — for every peer, after every step of random histories. A
/// window of 4 keeps the lead, the window and the delta limit all binding
/// with a dozen peers.
#[test]
fn indexed_digests_match_the_old_builder() {
    let config = Config {
        probe_interval_ms: 100,
        probe_timeout_ms: 50,
        suspect_timeout_ms: 300,
        dead_timeout_ms: 1_000,
        gossip_interval_ms: 200,
        digest_window: std::num::NonZeroUsize::new(4).expect("nonzero"),
        ..Config::default()
    };
    let mut compared = (0, 0);
    for seed in 0..32 {
        let mut draws = Draws(seed ^ 0xd16e);
        let mut e = GroupEngine::new(
            GroupId::new("g"),
            NodeId::new("a"),
            [peer(0)],
            config.clone(),
        )
        .expect("valid config");
        let mut now = Time(1);
        e.start(now);
        for i in 0..600 {
            if draws.below(3) == 0 {
                now = Time(now.0 + 1 + draws.below(400));
                e.on_tick(now);
            } else {
                step(&mut e, &mut draws, now);
            }
            for to in (0..2 * PEERS).map(peer) {
                let built = e.digest_for(&to, now);
                assert_eq!(
                    built,
                    old_digest_for(&e, &to, now),
                    "seed {seed} step {i} to {to}"
                );
                if built.1 {
                    compared.1 += 1;
                } else {
                    compared.0 += 1;
                }
            }
        }
    }
    assert!(
        compared.0 > 1_000 && compared.1 > 1_000,
        "too few digests compared: {compared:?}"
    );
}

/// A reader of the change clock learns every member that changed and every
/// one forgotten after its clock — until it falls more than the kept
/// forgotten members behind, which it is told instead of missing them.
#[test]
fn forgotten_members_are_reported_until_the_record_runs_out() {
    let config = Config::default();
    let reap = 2 * config.dead_timeout_ms;
    let mut e =
        GroupEngine::new(GroupId::new("g"), NodeId::new("a"), [], config).expect("valid config");
    let dead = |count: u64| -> Vec<wire::NodeDigest> {
        (0..count)
            .map(|i| wire::NodeDigest {
                node: NodeId::new(format!("d{i:05}")),
                incarnation: 0,
                status: Status::Dead.to_wire(),
                max_version: 0,
                content_hash: 0,
            })
            .collect()
    };
    let before = e.change_clock();
    e.merge_digest_liveness(&dead(10), Time(1));
    assert_eq!(e.members_changed_since(before).count(), 10);
    let learned = e.change_clock();
    e.on_tick(Time(1 + reap));
    assert_eq!(
        e.members_changed_since(learned).count(),
        0,
        "a reap is no change"
    );
    let gone: Vec<&NodeId> = e
        .members_forgotten_since(learned)
        .expect("within the record")
        .collect();
    assert_eq!(gone, dead(10).iter().map(|d| &d.node).collect::<Vec<_>>());

    let behind = e.change_clock();
    e.merge_digest_liveness(&dead(5_000), Time(2 + reap));
    e.on_tick(Time(2 + 2 * reap));
    assert_eq!(e.members().count(), 1, "all reaped");
    assert!(
        e.members_forgotten_since(behind).is_none(),
        "the record ran out"
    );
    let now = e.change_clock();
    assert_eq!(e.members_forgotten_since(now).map(Iterator::count), Some(0));
}
