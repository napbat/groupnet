//! What one idle tick costs in a large group: the members and entries the
//! engine examines, counted by the test-only `touched` counter.

use std::num::NonZeroU64;

use crate::{Config, Effect, GroupEngine, GroupId, NodeId, Status, Time, wire};

/// Members the engine knows, itself included.
const N: usize = 1_000;
/// Entries each member holds; every other one has a TTL longer than the run.
const ENTRIES: u64 = 4;
/// Long enough for every peer to have had its first digest (the one walk
/// that counts the members): `N / gossip_fanout` rounds of 200 ms.
const WARM_MS: u64 = 110_000;
/// The idle window measured after the warm-up.
const MEASURE_MS: u64 = 20_000;

fn id(i: usize) -> NodeId {
    NodeId::new(format!("n{i:04}"))
}

/// An engine that has learned `N - 1` live peers, each with `ENTRIES`
/// entries. Every digest is full, so each gossip round walks a whole
/// window per fanout target: the most a quiet round can cost.
fn crowded(now: Time) -> GroupEngine {
    let config = Config {
        full_digest_every: NonZeroU64::MIN,
        ..Config::default()
    };
    let mut e = GroupEngine::new(GroupId::new("g"), id(0), [], config).expect("valid config");
    e.start(now);
    let deltas = (1..N)
        .map(|i| wire::MemberDelta {
            node: id(i),
            incarnation: 0,
            status: Status::Alive.to_wire(),
            max_version: ENTRIES,
            entries: (1..=ENTRIES)
                .map(|k| wire::EntryDelta {
                    key: format!("k{k}"),
                    version: k,
                    ttl_ms: if k % 2 == 0 { 3_600_000 } else { 0 },
                    tombstone: false,
                    value: vec![7; 16],
                })
                .collect(),
        })
        .collect();
    e.merge_members(deltas, now);
    e
}

/// A bare `Ack`, as a live peer answers a probe.
fn ack(group: &GroupId) -> Vec<u8> {
    wire::encode(&wire::Frame {
        kind: wire::Kind::Ack,
        group: group.clone(),
        target: None,
        digest: Vec::new(),
        wants: Vec::new(),
        members: Vec::new(),
        metadata: Vec::new(),
        lead: None,
    })
}

/// In a quiet group of 1,000 members with 4,000 entries, an idle tick examines
/// only what a probe and a gossip round need — at most a digest window per
/// fanout target — not every member and entry. Every probe is answered at
/// once, so nothing is suspected, nothing expires and nothing changes.
///
/// Measured over 201 idle ticks (max / mean members and entries examined):
/// at 7ee9940, which walked every member and entry for each reap, the
/// deadline and the content hashes, and every member for the probe ring and
/// each digest's delta filter, 21,115 / 19,543. With the index, 128 / 63:
/// the two digest windows of a gossip round, and nothing on a probe tick.
#[test]
fn an_idle_tick_examines_only_what_is_due() {
    let mut now = Time(1);
    let mut e = crowded(now);
    let ack = ack(e.group());
    let mut costs: Vec<u64> = Vec::new();
    while now.0 < WARM_MS + MEASURE_MS {
        now = e.next_deadline().max(Time(now.0 + 1));
        let before = e.touched.get();
        let effects = e.on_tick(now);
        let cost = e.touched.get() - before;
        for effect in &effects {
            if let Effect::Send { to, wire } = effect
                && wire::decode(wire).is_some_and(|f| f.kind == wire::Kind::Ping)
            {
                e.on_message(to.clone(), &ack, now);
            }
        }
        if now.0 >= WARM_MS {
            costs.push(cost);
        }
    }
    assert_eq!(e.members().count(), N, "a member was lost while idle");
    let max = costs.iter().copied().max().expect("ticks were measured");
    let mean = costs.iter().sum::<u64>() / costs.len() as u64;
    let fanout = e.config.gossip_fanout.get() as u64;
    let window = e.config.digest_window.get() as u64;
    let bound = fanout * (window + 1) + 8;
    assert!(
        max <= bound,
        "an idle tick examined {max} members and entries (mean {mean}), above {bound}"
    );
}
