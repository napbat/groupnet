//! Codec throughput for the two frames that carry the fabric's steady-state
//! traffic: the anti-entropy `Digest` and the `Delta` that answers it.
//!
//! Sizes are the ones that matter for the scaling envelope in the README — 5,
//! 50, and 500 member summaries per frame. A full digest is the O(N) term in a
//! gossip round, so its encode/decode cost per member is the number to watch;
//! the delta cases carry entry payloads on top and are bounded in practice by
//! `Config::max_delta_frame_bytes`.
//!
//! ```text
//! cargo bench -p groupnet-core --bench wire
//! ```

use divan::Bencher;
use divan::counter::BytesCount;

use groupnet_core::wire::{
    self, EntryDelta, Frame, Kind, MemberDelta, MetaDelta, NodeDigest, NodeWant,
};
use groupnet_core::{GroupId, NodeId};

/// Members summarized per frame — the axis the fabric actually scales along.
const SIZES: [usize; 3] = [5, 50, 500];

fn main() {
    divan::main();
}

fn group() -> GroupId {
    GroupId::new("shard-42")
}

fn node(i: usize) -> NodeId {
    NodeId::new(format!("node-{i:03}"))
}

/// A full digest over `members`, plus the small metadata register set that
/// rides along on every one (a routing table's worth).
fn digest_frame(members: usize) -> Frame {
    Frame {
        kind: Kind::Digest,
        group: group(),
        target: None,
        digest: (0..members)
            .map(|i| NodeDigest {
                node: node(i),
                incarnation: (i as u64) % 7,
                status: u8::try_from(i % 3).expect("0..3"),
                max_version: (i as u64) * 13,
                content_hash: (i as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15),
            })
            .collect(),
        wants: Vec::new(),
        members: Vec::new(),
        metadata: (0..4)
            .map(|i| MetaDelta {
                key: format!("route/range-{i}"),
                version: i,
                writer: node(0),
                value: format!("shard-{i}"),
            })
            .collect(),
    }
}

/// A delta carrying `members` member records, each with the two entries a real
/// node advertises: its address and one hot application key.
fn delta_frame(members: usize) -> Frame {
    Frame {
        kind: Kind::Delta,
        group: group(),
        target: None,
        digest: Vec::new(),
        wants: Vec::new(),
        members: (0..members)
            .map(|i| MemberDelta {
                node: node(i),
                incarnation: (i as u64) % 7,
                status: 0,
                max_version: (i as u64) * 13,
                entries: vec![
                    EntryDelta {
                        key: "~addr".to_owned(),
                        version: i as u64,
                        ttl_ms: 0,
                        tombstone: false,
                        value: format!("10.0.{}.{}:7000", i / 256, i % 256).into_bytes(),
                    },
                    EntryDelta {
                        key: format!("shard/{i}"),
                        version: (i as u64) * 13,
                        ttl_ms: 120_000,
                        tombstone: false,
                        value: vec![0xab; 32],
                    },
                ],
            })
            .collect(),
        metadata: Vec::new(),
    }
}

/// The gaps a receiver asks for after comparing a digest — one want per member.
fn request_frame(members: usize) -> Frame {
    Frame {
        kind: Kind::DeltaRequest,
        group: group(),
        target: None,
        digest: Vec::new(),
        wants: (0..members)
            .map(|i| NodeWant {
                node: node(i),
                have_version: (i as u64) * 11,
            })
            .collect(),
        members: Vec::new(),
        metadata: Vec::new(),
    }
}

#[divan::bench(args = SIZES)]
fn encode_digest(bencher: Bencher, members: usize) {
    let frame = digest_frame(members);
    bencher
        .counter(BytesCount::new(wire::encode(&frame).len()))
        .bench(|| wire::encode(divan::black_box(&frame)));
}

#[divan::bench(args = SIZES)]
fn decode_digest(bencher: Bencher, members: usize) {
    let bytes = wire::encode(&digest_frame(members));
    bencher
        .counter(BytesCount::of_slice(&bytes))
        .bench(|| wire::decode(divan::black_box(&bytes)));
}

#[divan::bench(args = SIZES)]
fn encode_delta(bencher: Bencher, members: usize) {
    let frame = delta_frame(members);
    bencher
        .counter(BytesCount::new(wire::encode(&frame).len()))
        .bench(|| wire::encode(divan::black_box(&frame)));
}

#[divan::bench(args = SIZES)]
fn decode_delta(bencher: Bencher, members: usize) {
    let bytes = wire::encode(&delta_frame(members));
    bencher
        .counter(BytesCount::of_slice(&bytes))
        .bench(|| wire::decode(divan::black_box(&bytes)));
}

#[divan::bench(args = SIZES)]
fn encode_delta_request(bencher: Bencher, members: usize) {
    let frame = request_frame(members);
    bencher
        .counter(BytesCount::new(wire::encode(&frame).len()))
        .bench(|| wire::encode(divan::black_box(&frame)));
}

/// Demuxing an inbound frame to its group must not depend on frame size — this
/// is the one that would show it if it ever did.
#[divan::bench(args = SIZES)]
fn peek_group(bencher: Bencher, members: usize) {
    let bytes = wire::encode(&digest_frame(members));
    bencher.bench(|| wire::peek_group(divan::black_box(&bytes)));
}
