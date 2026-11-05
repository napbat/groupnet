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
        answer_pings(&mut e, &effects, &ack, now);
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

/// Every ping answered at once, as live peers would.
fn answer_pings(e: &mut GroupEngine, effects: &[Effect], ack: &[u8], now: Time) {
    for effect in effects {
        if let Effect::Send { to, wire } = effect
            && wire::decode(wire).is_some_and(|f| f.kind == wire::Kind::Ping)
        {
            e.on_message(to.clone(), ack, now);
        }
    }
}

/// Under a liveness storm — every member's liveness changed before every
/// round, so every member is hot news and changed since every peer's cursor
/// — a full digest examines its lead and its window, not every member.
///
/// Measured over 100 rounds of 2 full digests (max / mean members examined
/// per full digest): at f71405f, which collected every changed member to
/// select the lead, 1,031 / 1,031 (999 changed members, then 32 walked).
/// Reading the lead off the top of the news index, 64 / 64: 32 led, 32
/// walked.
#[test]
fn a_full_digest_in_a_storm_examines_its_lead_and_window() {
    let mut now = Time(1);
    let mut e = crowded(now);
    let ack = ack(e.group());
    while now.0 < WARM_MS {
        now = e.next_deadline().max(Time(now.0 + 1));
        let effects = e.on_tick(now);
        answer_pings(&mut e, &effects, &ack, now);
    }
    let fanout = e.config.gossip_fanout.get() as u64;
    let mut costs: Vec<u64> = Vec::new();
    for incarnation in 1..=100 {
        now = Time(now.0 + e.config.gossip_interval_ms);
        // Every peer re-asserts itself at a new incarnation: all of them
        // changed, all of them hot.
        let claims: Vec<wire::NodeDigest> = (1..N)
            .map(|i| wire::NodeDigest {
                node: id(i),
                incarnation,
                status: Status::Alive.to_wire(),
                max_version: ENTRIES,
                content_hash: 0,
            })
            .collect();
        e.merge_digest_liveness(&claims, now);
        let before = e.touched.get();
        let effects = e.disseminate_digest(now);
        costs.push((e.touched.get() - before) / fanout);
        answer_pings(&mut e, &effects, &ack, now);
    }
    assert_eq!(e.members().count(), N, "a member was lost in the storm");
    assert_eq!(
        e.net_stats().digests_built,
        e.net_stats().full_digests_built
    );
    let max = costs.iter().copied().max().expect("digests were measured");
    let mean = costs.iter().sum::<u64>() / costs.len() as u64;
    let window = e.config.digest_window.get() as u64;
    let bound = window + window / 2 + 8;
    assert!(
        max <= bound,
        "a full digest examined {max} members (mean {mean}), above {bound}"
    );
}
