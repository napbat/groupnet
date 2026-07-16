//! The gossip wire protocol and its codec.
//!
//! The engine owns serialization so that [`Transport`] implementations only
//! ever move opaque `&[u8]` — a TCP, UDP, IPC, or shared-memory binding never
//! re-encodes the protocol and so can never drift out of sync with it.
//!
//! Every message is a [`Frame`] with a [`Kind`] plus a kind-specific body. Since
//! **v3** the protocol is digest/delta anti-entropy rather than full-view
//! piggybacking:
//!
//! * [`Kind::Digest`] — a compact per-node `(incarnation, status, max_version)`
//!   summary ([`NodeDigest`]) plus the small metadata register set. No entry
//!   values. This is what the periodic anti-entropy round emits; from one digest
//!   a receiver knows exactly what to request and what to send.
//! * [`Kind::DeltaRequest`] — the gaps: per node, "send me entries newer than
//!   this version" ([`NodeWant`]).
//! * [`Kind::Delta`] — the targeted entry payload: only the [`MemberDelta`]s /
//!   [`EntryDelta`]s newer than the recipient's digest, bounded per frame.
//! * [`Kind::Ping`] / [`Kind::Ack`] / [`Kind::PingReq`] / [`Kind::IndirectAck`] —
//!   SWIM liveness probes. Since v3 these carry **no** piggybacked view; they are
//!   tiny.
//!
//! The codec is a small hand-rolled length-prefixed format (little-endian),
//! deliberately dependency-free.
//!
//! **Invariant:** a frame encodes its [`GroupId`] immediately after the
//! one-byte kind tag, so [`peek_group`] can demux an inbound frame to the right
//! group without fully decoding it.
//!
//! Member status is carried as a raw `u8`; the engine owns the `Status` enum
//! and its mapping, keeping this module pure serialization.
//!
//! [`Transport`]: https://docs.rs/groupnet-transport

use crate::{GroupId, NodeId};

/// What a [`Frame`] is for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// Periodic anti-entropy: a compact per-node version [`NodeDigest`] vector
    /// plus the metadata register set. Carries no entry values; a receiver
    /// compares it against its own view to decide what to request and send.
    Digest,
    /// A request for the entries a peer is missing — a vector of [`NodeWant`]s,
    /// each "send me this node's entries newer than `have_version`".
    DeltaRequest,
    /// The targeted entry payload answering a [`Kind::Digest`] or a
    /// [`Kind::DeltaRequest`]: only the entries newer than the recipient's
    /// digest, bounded per frame (successive rounds converge the rest).
    Delta,
    /// A liveness probe; the receiver must reply with [`Kind::Ack`]. Carries no
    /// view.
    Ping,
    /// A reply to a [`Kind::Ping`], proving the sender is alive. Carries no view.
    Ack,
    /// "Please probe [`Frame::target`] on my behalf" — sent to indirect probers
    /// after a direct probe goes unanswered. Carries no view.
    PingReq,
    /// "[`Frame::target`] answered my relayed probe" — an indirect prober's
    /// report back to the origin that the target is alive. Carries no view.
    IndirectAck,
}

/// One key of a member's app-defined state, as the sender holds it. Each key
/// is independently mergeable by its owning node's per-node version clock;
/// `ttl_ms` lets receivers arm their own expiry, and `tombstone` disseminates an
/// explicit delete.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EntryDelta {
    /// State key (application-defined; keys starting `~` are reserved for
    /// groupnet itself, e.g. `~addr`, `~blob`).
    pub key: String,
    /// The owning node's per-node version clock at the moment it authored this
    /// key. Monotonic across *all* of that node's keys, so a scalar per-node
    /// maximum ([`NodeDigest::max_version`]) summarizes the whole map.
    pub version: u64,
    /// TTL in ms (0 = none); receivers expire the entry `ttl_ms` after the
    /// merge that adopted it, unless a fresher version refreshes it.
    pub ttl_ms: u64,
    /// Deletion marker.
    pub tombstone: bool,
    /// The value (empty when `tombstone`). Groupnet disseminates and
    /// version-orders it but never interprets it.
    pub value: Vec<u8>,
}

/// One member's status, high-water mark, and (in a [`Kind::Delta`]) app-defined
/// entries, as the sender sees it. Liveness (`incarnation`/`status`) and per-key
/// app state are merged independently.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemberDelta {
    /// The node this entry describes.
    pub node: NodeId,
    /// The node's incarnation number (bumped by the node itself to refute).
    pub incarnation: u64,
    /// Status code (engine-defined; `0 = alive, 1 = suspect, 2 = dead`).
    pub status: u8,
    /// The sender's high-water mark over this node's entry versions. When a
    /// delta carries every entry the recipient asked for, the recipient can
    /// advance its own summary straight to this value; when the delta was
    /// truncated to fit the frame budget it equals the highest version actually
    /// included, so the recipient re-requests the remainder next round.
    pub max_version: u64,
    /// The node's keyed state entries the sender is delivering (empty outside a
    /// [`Kind::Delta`], or when the delta only advances the high-water mark).
    pub entries: Vec<EntryDelta>,
}

/// A compact per-node summary in a [`Kind::Digest`]: liveness plus a scalar
/// state high-water mark, from which the receiver computes exactly which
/// entries it must request or send. No values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeDigest {
    /// The node this digest entry summarizes.
    pub node: NodeId,
    /// The node's incarnation, as the sender holds it (merged by SWIM
    /// precedence straight from the digest — liveness needs no delta round-trip).
    pub incarnation: u64,
    /// Status code (`0 = alive, 1 = suspect, 2 = dead`).
    pub status: u8,
    /// The sender's high-water mark over this node's entry versions (0 if the
    /// sender holds none of its state). A receiver behind this asks for more; a
    /// receiver ahead of it offers the difference.
    pub max_version: u64,
    /// A hash of the sender's *held* entries for this node (keys, versions,
    /// tombstones, values). Two summaries can share a `max_version` yet hold
    /// different entries — a version clock resets on restart, so a fresh boot can
    /// reuse a version a peer already bound to another key. When the high-water
    /// marks match but the hashes differ, the receiver falls back to a full
    /// per-key exchange, which converges by last-writer-wins where the scalar
    /// comparison alone could not.
    pub content_hash: u64,
}

/// One entry of a [`Kind::DeltaRequest`]: "send me `node`'s entries with version
/// strictly greater than `have_version`". `have_version` is 0 to request a
/// node's entire state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeWant {
    /// The node whose entries are wanted.
    pub node: NodeId,
    /// The requester's current high-water mark for that node.
    pub have_version: u64,
}

/// One metadata key's value plus its last-writer-wins timestamp.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetaDelta {
    /// Metadata key.
    pub key: String,
    /// Monotonic version at the writing node.
    pub version: u64,
    /// The node that produced this version (LWW tiebreaker).
    pub writer: NodeId,
    /// The value.
    pub value: String,
}

/// A protocol frame: a [`Kind`] plus its kind-specific body. Only the fields a
/// kind uses are populated (and encoded); the rest are empty.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    /// What this frame is for.
    pub kind: Kind,
    /// The group it concerns.
    pub group: GroupId,
    /// The subject of a [`Kind::PingReq`] / [`Kind::IndirectAck`]; `None`
    /// otherwise.
    pub target: Option<NodeId>,
    /// Per-node version summaries — populated on a [`Kind::Digest`].
    pub digest: Vec<NodeDigest>,
    /// Per-node gap requests — populated on a [`Kind::DeltaRequest`].
    pub wants: Vec<NodeWant>,
    /// Member states with entries — populated on a [`Kind::Delta`].
    pub members: Vec<MemberDelta>,
    /// Metadata register set — populated on a [`Kind::Digest`] (small, bounded).
    pub metadata: Vec<MetaDelta>,
}

/// Protocol version, the first byte of every frame. Bumped to **3** for
/// digest/delta anti-entropy (v2 frames piggybacked the full view). As with the
/// v1→v2 bump this is a hard cut: a v2 frame is simply undecodable to a v3 node
/// and vice versa, so a mixed-version cluster degrades to two disjoint gossip
/// meshes rather than misreading each other — gossip is loss-tolerant, so this
/// is the safe failure mode, and a rolling deployment converges once the last v2
/// node is upgraded.
const FRAME_VERSION: u8 = 3;

const KIND_DIGEST: u8 = 1;
const KIND_PING: u8 = 2;
const KIND_ACK: u8 = 3;
const KIND_PING_REQ: u8 = 4;
const KIND_INDIRECT_ACK: u8 = 5;
const KIND_DELTA_REQUEST: u8 = 6;
const KIND_DELTA: u8 = 7;

/// Caps how much a decoder pre-allocates from a frame's self-declared element
/// count. A corrupt or hostile frame can claim a huge count, so we reserve at
/// most this many slots up front and let the `Vec` grow as elements actually
/// arrive — never trusting the claim enough to allocate on it.
const MAX_PREALLOC: usize = 1024;

fn kind_to_u8(k: Kind) -> u8 {
    match k {
        Kind::Digest => KIND_DIGEST,
        Kind::Ping => KIND_PING,
        Kind::Ack => KIND_ACK,
        Kind::PingReq => KIND_PING_REQ,
        Kind::IndirectAck => KIND_INDIRECT_ACK,
        Kind::DeltaRequest => KIND_DELTA_REQUEST,
        Kind::Delta => KIND_DELTA,
    }
}

fn kind_from_u8(b: u8) -> Option<Kind> {
    match b {
        KIND_DIGEST => Some(Kind::Digest),
        KIND_PING => Some(Kind::Ping),
        KIND_ACK => Some(Kind::Ack),
        KIND_PING_REQ => Some(Kind::PingReq),
        KIND_INDIRECT_ACK => Some(Kind::IndirectAck),
        KIND_DELTA_REQUEST => Some(Kind::DeltaRequest),
        KIND_DELTA => Some(Kind::Delta),
        _ => None,
    }
}

/// Encodes a frame to bytes for a transport to ship.
#[must_use]
pub fn encode(frame: &Frame) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(FRAME_VERSION);
    out.push(kind_to_u8(frame.kind));
    put_str(&mut out, frame.group.as_str());

    match &frame.target {
        Some(t) => {
            out.push(1);
            put_str(&mut out, t.as_str());
        }
        None => out.push(0),
    }

    match frame.kind {
        Kind::Digest => {
            put_u32(&mut out, frame.digest.len() as u32);
            for d in &frame.digest {
                put_str(&mut out, d.node.as_str());
                put_u64(&mut out, d.incarnation);
                out.push(d.status);
                put_u64(&mut out, d.max_version);
                put_u64(&mut out, d.content_hash);
            }
            put_metadata(&mut out, &frame.metadata);
        }
        Kind::DeltaRequest => {
            put_u32(&mut out, frame.wants.len() as u32);
            for w in &frame.wants {
                put_str(&mut out, w.node.as_str());
                put_u64(&mut out, w.have_version);
            }
        }
        Kind::Delta => {
            put_u32(&mut out, frame.members.len() as u32);
            for m in &frame.members {
                put_str(&mut out, m.node.as_str());
                put_u64(&mut out, m.incarnation);
                out.push(m.status);
                put_u64(&mut out, m.max_version);
                put_u32(&mut out, m.entries.len() as u32);
                for e in &m.entries {
                    put_str(&mut out, &e.key);
                    put_u64(&mut out, e.version);
                    put_u64(&mut out, e.ttl_ms);
                    out.push(u8::from(e.tombstone));
                    put_bytes(&mut out, &e.value);
                }
            }
        }
        Kind::Ping | Kind::Ack | Kind::PingReq | Kind::IndirectAck => {}
    }
    out
}

fn put_metadata(out: &mut Vec<u8>, metadata: &[MetaDelta]) {
    put_u32(out, metadata.len() as u32);
    for d in metadata {
        put_str(out, &d.key);
        put_u64(out, d.version);
        put_str(out, d.writer.as_str());
        put_str(out, &d.value);
    }
}

/// Decodes a frame produced by [`encode`]. Returns `None` on any malformed or
/// truncated input — the engine treats undecodable frames as dropped, which is
/// safe because the transport contract is best-effort anyway.
#[must_use]
pub fn decode(bytes: &[u8]) -> Option<Frame> {
    let mut cur = bytes;
    if take_u8(&mut cur)? != FRAME_VERSION {
        return None; // other protocol version — dropped, gossip is loss-tolerant
    }
    let kind = kind_from_u8(take_u8(&mut cur)?)?;
    let group = GroupId::new(get_str(&mut cur)?);

    let target = match take_u8(&mut cur)? {
        0 => None,
        1 => Some(NodeId::new(get_str(&mut cur)?)),
        _ => return None,
    };

    let mut frame = Frame {
        kind,
        group,
        target,
        digest: Vec::new(),
        wants: Vec::new(),
        members: Vec::new(),
        metadata: Vec::new(),
    };

    match kind {
        Kind::Digest => {
            let n = get_u32(&mut cur)? as usize;
            frame.digest = Vec::with_capacity(n.min(MAX_PREALLOC));
            for _ in 0..n {
                let node = NodeId::new(get_str(&mut cur)?);
                let incarnation = get_u64(&mut cur)?;
                let status = take_u8(&mut cur)?;
                let max_version = get_u64(&mut cur)?;
                let content_hash = get_u64(&mut cur)?;
                frame.digest.push(NodeDigest {
                    node,
                    incarnation,
                    status,
                    max_version,
                    content_hash,
                });
            }
            frame.metadata = get_metadata(&mut cur)?;
        }
        Kind::DeltaRequest => {
            let n = get_u32(&mut cur)? as usize;
            frame.wants = Vec::with_capacity(n.min(MAX_PREALLOC));
            for _ in 0..n {
                let node = NodeId::new(get_str(&mut cur)?);
                let have_version = get_u64(&mut cur)?;
                frame.wants.push(NodeWant { node, have_version });
            }
        }
        Kind::Delta => {
            let n = get_u32(&mut cur)? as usize;
            frame.members = Vec::with_capacity(n.min(MAX_PREALLOC));
            for _ in 0..n {
                let node = NodeId::new(get_str(&mut cur)?);
                let incarnation = get_u64(&mut cur)?;
                let status = take_u8(&mut cur)?;
                let max_version = get_u64(&mut cur)?;
                let k = get_u32(&mut cur)? as usize;
                let mut entries = Vec::with_capacity(k.min(MAX_PREALLOC));
                for _ in 0..k {
                    let key = get_str(&mut cur)?;
                    let version = get_u64(&mut cur)?;
                    let ttl_ms = get_u64(&mut cur)?;
                    let tombstone = match take_u8(&mut cur)? {
                        0 => false,
                        1 => true,
                        _ => return None,
                    };
                    let value = get_bytes(&mut cur)?;
                    entries.push(EntryDelta {
                        key,
                        version,
                        ttl_ms,
                        tombstone,
                        value,
                    });
                }
                frame.members.push(MemberDelta {
                    node,
                    incarnation,
                    status,
                    max_version,
                    entries,
                });
            }
        }
        Kind::Ping | Kind::Ack | Kind::PingReq | Kind::IndirectAck => {}
    }

    Some(frame)
}

fn get_metadata(cur: &mut &[u8]) -> Option<Vec<MetaDelta>> {
    let m = get_u32(cur)? as usize;
    let mut metadata = Vec::with_capacity(m.min(MAX_PREALLOC));
    for _ in 0..m {
        let key = get_str(cur)?;
        let version = get_u64(cur)?;
        let writer = NodeId::new(get_str(cur)?);
        let value = get_str(cur)?;
        metadata.push(MetaDelta {
            key,
            version,
            writer,
            value,
        });
    }
    Some(metadata)
}

/// Cheaply reads just the [`GroupId`] from an encoded frame without decoding the
/// body, so a driver can route an inbound message to the correct group actor.
#[must_use]
pub fn peek_group(bytes: &[u8]) -> Option<GroupId> {
    let mut cur = bytes;
    if take_u8(&mut cur)? != FRAME_VERSION {
        return None;
    }
    let _kind = take_u8(&mut cur)?;
    Some(GroupId::new(get_str(&mut cur)?))
}

// ---- encoded-size accounting -------------------------------------------------
//
// The engine packs deltas up to a byte budget (`max_delta_frame_bytes`) without
// repeatedly re-encoding a growing frame, so the size of each piece lives here
// next to `encode` — the one source of truth for the layout.

fn str_len(s: &str) -> usize {
    4 + s.len()
}

/// Encoded byte length a single [`EntryDelta`] contributes to a `Delta` frame.
pub(crate) fn entry_len(e: &EntryDelta) -> usize {
    str_len(&e.key) + 8 /* version */ + 8 /* ttl */ + 1 /* tombstone */ + 4 + e.value.len()
}

/// Encoded byte length a member header (before its entries) contributes to a
/// `Delta` frame: node id, incarnation, status, high-water, entry count.
pub(crate) fn member_header_len(node: &NodeId) -> usize {
    str_len(node.as_str()) + 8 /* inc */ + 1 /* status */ + 8 /* max_version */ + 4 /* entry count */
}

/// Encoded byte length of a `Delta` frame's fixed prefix (version, kind, group,
/// absent target, member count) — the floor a delta occupies before any member.
pub(crate) fn delta_frame_overhead(group: &GroupId) -> usize {
    1 /* version */ + 1 /* kind */ + str_len(group.as_str()) + 1 /* target = none */ + 4 /* member count */
}

// Minimal little-endian codec primitives.

fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_str(out: &mut Vec<u8>, s: &str) {
    put_bytes(out, s.as_bytes());
}

fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    put_u32(out, b.len() as u32);
    out.extend_from_slice(b);
}

fn get_bytes(cur: &mut &[u8]) -> Option<Vec<u8>> {
    let len = get_u32(cur)? as usize;
    if cur.len() < len {
        return None;
    }
    let (head, rest) = cur.split_at(len);
    *cur = rest;
    Some(head.to_vec())
}

fn take_u8(cur: &mut &[u8]) -> Option<u8> {
    let (&b, rest) = cur.split_first()?;
    *cur = rest;
    Some(b)
}

fn get_u32(cur: &mut &[u8]) -> Option<u32> {
    if cur.len() < 4 {
        return None;
    }
    let (head, rest) = cur.split_at(4);
    *cur = rest;
    Some(u32::from_le_bytes(head.try_into().ok()?))
}

fn get_u64(cur: &mut &[u8]) -> Option<u64> {
    if cur.len() < 8 {
        return None;
    }
    let (head, rest) = cur.split_at(8);
    *cur = rest;
    Some(u64::from_le_bytes(head.try_into().ok()?))
}

fn get_str(cur: &mut &[u8]) -> Option<String> {
    let len = get_u32(cur)? as usize;
    if cur.len() < len {
        return None;
    }
    let (head, rest) = cur.split_at(len);
    *cur = rest;
    std::str::from_utf8(head).ok().map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest_sample() -> Frame {
        Frame {
            kind: Kind::Digest,
            group: GroupId::new("shard-42"),
            target: None,
            digest: vec![
                NodeDigest {
                    node: NodeId::new("node-a"),
                    incarnation: 2,
                    status: 0,
                    max_version: 12,
                    content_hash: 0x1234_5678_9abc_def0,
                },
                NodeDigest {
                    node: NodeId::new("node-b"),
                    incarnation: 5,
                    status: 1,
                    max_version: 0,
                    content_hash: 0,
                },
            ],
            wants: vec![],
            members: vec![],
            metadata: vec![MetaDelta {
                key: "routing".into(),
                version: 3,
                writer: NodeId::new("node-a"),
                value: "v3".into(),
            }],
        }
    }

    fn delta_sample() -> Frame {
        Frame {
            kind: Kind::Delta,
            group: GroupId::new("shard-42"),
            target: None,
            digest: vec![],
            wants: vec![],
            members: vec![MemberDelta {
                node: NodeId::new("node-a"),
                incarnation: 2,
                status: 0,
                max_version: 12,
                entries: vec![
                    EntryDelta {
                        key: "~addr".into(),
                        version: 7,
                        ttl_ms: 0,
                        tombstone: false,
                        value: vec![1, 2, 3],
                    },
                    EntryDelta {
                        key: "hot/3".into(),
                        version: 12,
                        ttl_ms: 120_000,
                        tombstone: false,
                        value: vec![9; 40],
                    },
                    EntryDelta {
                        key: "old".into(),
                        version: 4,
                        ttl_ms: 0,
                        tombstone: true,
                        value: vec![],
                    },
                ],
            }],
            metadata: vec![],
        }
    }

    fn request_sample() -> Frame {
        Frame {
            kind: Kind::DeltaRequest,
            group: GroupId::new("shard-42"),
            target: None,
            digest: vec![],
            wants: vec![
                NodeWant {
                    node: NodeId::new("node-a"),
                    have_version: 4,
                },
                NodeWant {
                    node: NodeId::new("node-c"),
                    have_version: 0,
                },
            ],
            members: vec![],
            metadata: vec![],
        }
    }

    fn ping_req_sample() -> Frame {
        Frame {
            kind: Kind::PingReq,
            group: GroupId::new("shard-42"),
            target: Some(NodeId::new("node-c")),
            digest: vec![],
            wants: vec![],
            members: vec![],
            metadata: vec![],
        }
    }

    #[test]
    fn round_trips_every_kind() {
        for frame in [
            digest_sample(),
            delta_sample(),
            request_sample(),
            ping_req_sample(),
        ] {
            let bytes = encode(&frame);
            assert_eq!(decode(&bytes), Some(frame.clone()));
            assert_eq!(peek_group(&bytes), Some(GroupId::new("shard-42")));
        }
    }

    #[test]
    fn probes_carry_no_view() {
        // A bare probe encodes only version, kind, group, and the target flag —
        // never a piggybacked digest or delta.
        let ping = Frame {
            kind: Kind::Ping,
            group: GroupId::new("g"),
            target: None,
            digest: vec![],
            wants: vec![],
            members: vec![],
            metadata: vec![],
        };
        assert_eq!(encode(&ping).len(), 1 + 1 + (4 + 1) + 1);
    }

    #[test]
    fn entry_and_member_lengths_match_the_encoding() {
        // The size accounting the engine budgets against must equal what `encode`
        // actually produces.
        let frame = delta_sample();
        let member = &frame.members[0];
        let predicted = delta_frame_overhead(&frame.group)
            + member_header_len(&member.node)
            + member.entries.iter().map(entry_len).sum::<usize>();
        assert_eq!(predicted, encode(&frame).len());
    }

    #[test]
    fn truncated_input_decodes_to_none_not_panic() {
        for frame in [digest_sample(), delta_sample(), request_sample()] {
            let bytes = encode(&frame);
            for cut in 0..bytes.len() {
                let _ = decode(&bytes[..cut]); // must never panic
            }
        }
        assert_eq!(decode(&[]), None);
    }

    #[test]
    fn a_v2_framed_byte_stream_is_rejected() {
        // Hard cut: a frame stamped with the old version byte does not decode.
        let mut bytes = encode(&digest_sample());
        bytes[0] = 2;
        assert_eq!(decode(&bytes), None);
        assert_eq!(peek_group(&bytes), None);
    }
}
