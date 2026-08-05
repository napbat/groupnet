//! Shared fixtures for the sans-IO engine tests: build an engine, hand-assemble
//! wire frames, and read summaries back out of the effects it returns.
//!
//! Dependency-free by design — these helpers touch nothing but
//! [`groupnet_core`], so any crate can use them without dragging an async
//! runtime into its test graph.

use groupnet_core::{Config, Effect, GroupEngine, GroupId, NodeId, Status, wire};

/// The group every fixture frame and fixture engine belongs to. Tests are
/// single-group, so one well-known id keeps senders and receivers agreeing.
pub const TEST_GROUP: &str = "g";

/// An engine for node `id` seeded with `seeds`, in [`TEST_GROUP`] at the
/// default [`Config`].
pub fn engine(id: &str, seeds: &[&str]) -> GroupEngine {
    GroupEngine::new(
        GroupId::new(TEST_GROUP),
        NodeId::new(id),
        seeds.iter().map(|s| NodeId::new(*s)),
        Config::default(),
    )
}

/// The member summaries listed across all digest frames in `effects`.
pub fn digest_summaries(effects: &[Effect]) -> Vec<NodeId> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::Send { wire, .. } => wire::decode(wire),
            _ => None,
        })
        .filter(|f| f.kind == wire::Kind::Digest)
        .flat_map(|f| f.digest.into_iter().map(|d| d.node))
        .collect()
}

/// A digest frame (liveness summaries + metadata) — how liveness and
/// metadata now disseminate.
pub fn digest_frame(digest: Vec<wire::NodeDigest>, metadata: Vec<wire::MetaDelta>) -> Vec<u8> {
    wire::encode(&wire::Frame {
        kind: wire::Kind::Digest,
        group: GroupId::new(TEST_GROUP),
        target: None,
        digest,
        wants: Vec::new(),
        members: Vec::new(),
        metadata,
    })
}

/// A delta frame (member entries) — how per-node state now disseminates.
pub fn delta_frame(members: Vec<wire::MemberDelta>) -> Vec<u8> {
    wire::encode(&wire::Frame {
        kind: wire::Kind::Delta,
        group: GroupId::new(TEST_GROUP),
        target: None,
        digest: Vec::new(),
        wants: Vec::new(),
        members,
        metadata: Vec::new(),
    })
}

/// A bare probe frame of `kind` (`Ping`, `PingReq`, `Ack`, `IndirectAck`),
/// optionally naming the probe's `target`.
pub fn probe_frame(kind: wire::Kind, target: Option<NodeId>) -> Vec<u8> {
    wire::encode(&wire::Frame {
        kind,
        group: GroupId::new(TEST_GROUP),
        target,
        digest: Vec::new(),
        wants: Vec::new(),
        members: Vec::new(),
        metadata: Vec::new(),
    })
}

/// One liveness-only digest summary for `node`.
pub fn ndigest(node: &str, inc: u64, status: Status, max_version: u64) -> wire::NodeDigest {
    wire::NodeDigest {
        node: NodeId::new(node),
        incarnation: inc,
        status: status.to_wire(),
        max_version,
        // Empty holdings hash to zero; these liveness-only digests advertise
        // no entries, so a zero here matches an empty receiver.
        content_hash: 0,
    }
}

/// One keyed state entry, as it rides a delta frame.
pub fn entry(
    key: &str,
    version: u64,
    ttl_ms: u64,
    tombstone: bool,
    value: &[u8],
) -> wire::EntryDelta {
    wire::EntryDelta {
        key: key.to_owned(),
        version,
        ttl_ms,
        tombstone,
        value: value.to_vec(),
    }
}

/// A member delta carrying `entries` (a well-formed delta sets its
/// high-water to the max entry version).
pub fn member_delta(node: &str, entries: Vec<wire::EntryDelta>) -> wire::MemberDelta {
    let max_version = entries.iter().map(|e| e.version).max().unwrap_or(0);
    wire::MemberDelta {
        node: NodeId::new(node),
        incarnation: 0,
        status: Status::Alive.to_wire(),
        max_version,
        entries,
    }
}

/// Decodes the single digest frame a round emits (all chunks in one, at
/// these small sizes), returning the sender's own summaries and metadata.
pub fn decode_one_digest(effects: &[Effect]) -> wire::Frame {
    let bytes = effects
        .iter()
        .find_map(|e| match e {
            Effect::Send { wire, .. } => {
                let f = wire::decode(wire)?;
                (f.kind == wire::Kind::Digest).then_some(wire.clone())
            }
            _ => None,
        })
        .expect("a digest send");
    wire::decode(&bytes).expect("decodes")
}
