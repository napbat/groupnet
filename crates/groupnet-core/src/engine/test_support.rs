//! Shared fixtures for the engine test modules.

use crate::config::Config;
use crate::membership::Status;
use crate::{GroupId, NodeId, wire};

use super::{Effect, GroupEngine};

pub(super) fn engine(id: &str, seeds: &[&str]) -> GroupEngine {
    GroupEngine::new(
        GroupId::new("g"),
        NodeId::new(id),
        seeds.iter().map(|s| NodeId::new(*s)),
        Config::default(),
    )
}

/// The member summaries listed across all digest frames in `effects`.
pub(super) fn digest_summaries(effects: &[Effect]) -> Vec<NodeId> {
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
pub(super) fn digest_frame(
    digest: Vec<wire::NodeDigest>,
    metadata: Vec<wire::MetaDelta>,
) -> Vec<u8> {
    wire::encode(&wire::Frame {
        kind: wire::Kind::Digest,
        group: GroupId::new("g"),
        target: None,
        digest,
        wants: Vec::new(),
        members: Vec::new(),
        metadata,
    })
}

/// A delta frame (member entries) — how per-node state now disseminates.
pub(super) fn delta_frame(members: Vec<wire::MemberDelta>) -> Vec<u8> {
    wire::encode(&wire::Frame {
        kind: wire::Kind::Delta,
        group: GroupId::new("g"),
        target: None,
        digest: Vec::new(),
        wants: Vec::new(),
        members,
        metadata: Vec::new(),
    })
}

pub(super) fn probe_frame(kind: wire::Kind, target: Option<NodeId>) -> Vec<u8> {
    wire::encode(&wire::Frame {
        kind,
        group: GroupId::new("g"),
        target,
        digest: Vec::new(),
        wants: Vec::new(),
        members: Vec::new(),
        metadata: Vec::new(),
    })
}

pub(super) fn ndigest(node: &str, inc: u64, status: Status, max_version: u64) -> wire::NodeDigest {
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

pub(super) fn entry(
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
pub(super) fn member_delta(node: &str, entries: Vec<wire::EntryDelta>) -> wire::MemberDelta {
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
pub(super) fn decode_one_digest(effects: &[Effect]) -> wire::Frame {
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
