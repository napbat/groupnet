//! The gossip wire protocol and its codec.
//!
//! The engine owns serialization so that [`Transport`] implementations only
//! ever move opaque `&[u8]` — a TCP, UDP, IPC, or shared-memory binding never
//! re-encodes the protocol and so can never drift out of sync with it.
//!
//! Every message is a [`Frame`] with a [`Kind`] (gossip / ping / ack) plus the
//! sender's current membership and metadata view, piggybacked for
//! infection-style dissemination. The codec is a small hand-rolled
//! length-prefixed format (little-endian), deliberately dependency-free.
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
    /// Periodic anti-entropy dissemination.
    Gossip,
    /// A liveness probe; the receiver must reply with [`Kind::Ack`].
    Ping,
    /// A reply to a [`Kind::Ping`], proving the sender is alive.
    Ack,
    /// "Please probe [`Frame::target`] on my behalf" — sent to indirect probers
    /// after a direct probe goes unanswered.
    PingReq,
    /// "[`Frame::target`] answered my relayed probe" — an indirect prober's
    /// report back to the origin that the target is alive.
    IndirectAck,
}

/// One member's status as the sender sees it, for last-writer-wins merge by
/// `(incarnation, status)` (see the engine's SWIM merge rules).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemberDelta {
    /// The node this entry describes.
    pub node: NodeId,
    /// The node's incarnation number (bumped by the node itself to refute).
    pub incarnation: u64,
    /// Status code (engine-defined; `0 = alive, 1 = suspect, 2 = dead`).
    pub status: u8,
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

/// A protocol frame: a kind plus the sender's piggybacked membership and
/// metadata view.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    /// What this frame is for.
    pub kind: Kind,
    /// The group it concerns.
    pub group: GroupId,
    /// The subject of a [`Kind::PingReq`] / [`Kind::IndirectAck`]; `None`
    /// otherwise.
    pub target: Option<NodeId>,
    /// Member states the sender currently holds.
    pub members: Vec<MemberDelta>,
    /// Metadata entries the sender currently holds.
    pub metadata: Vec<MetaDelta>,
}

const KIND_GOSSIP: u8 = 1;
const KIND_PING: u8 = 2;
const KIND_ACK: u8 = 3;
const KIND_PING_REQ: u8 = 4;
const KIND_INDIRECT_ACK: u8 = 5;

fn kind_to_u8(k: Kind) -> u8 {
    match k {
        Kind::Gossip => KIND_GOSSIP,
        Kind::Ping => KIND_PING,
        Kind::Ack => KIND_ACK,
        Kind::PingReq => KIND_PING_REQ,
        Kind::IndirectAck => KIND_INDIRECT_ACK,
    }
}

fn kind_from_u8(b: u8) -> Option<Kind> {
    match b {
        KIND_GOSSIP => Some(Kind::Gossip),
        KIND_PING => Some(Kind::Ping),
        KIND_ACK => Some(Kind::Ack),
        KIND_PING_REQ => Some(Kind::PingReq),
        KIND_INDIRECT_ACK => Some(Kind::IndirectAck),
        _ => None,
    }
}

/// Encodes a frame to bytes for a transport to ship.
#[must_use]
pub fn encode(frame: &Frame) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(kind_to_u8(frame.kind));
    put_str(&mut out, frame.group.as_str());

    match &frame.target {
        Some(t) => {
            out.push(1);
            put_str(&mut out, t.as_str());
        }
        None => out.push(0),
    }

    put_u32(&mut out, frame.members.len() as u32);
    for m in &frame.members {
        put_str(&mut out, m.node.as_str());
        put_u64(&mut out, m.incarnation);
        out.push(m.status);
    }

    put_u32(&mut out, frame.metadata.len() as u32);
    for d in &frame.metadata {
        put_str(&mut out, &d.key);
        put_u64(&mut out, d.version);
        put_str(&mut out, d.writer.as_str());
        put_str(&mut out, &d.value);
    }
    out
}

/// Decodes a frame produced by [`encode`]. Returns `None` on any malformed or
/// truncated input — the engine treats undecodable frames as dropped, which is
/// safe because the transport contract is best-effort anyway.
#[must_use]
pub fn decode(bytes: &[u8]) -> Option<Frame> {
    let mut cur = bytes;
    let kind = kind_from_u8(take_u8(&mut cur)?)?;
    let group = GroupId::new(get_str(&mut cur)?);

    let target = match take_u8(&mut cur)? {
        0 => None,
        1 => Some(NodeId::new(get_str(&mut cur)?)),
        _ => return None,
    };

    let n = get_u32(&mut cur)? as usize;
    let mut members = Vec::with_capacity(n.min(1024));
    for _ in 0..n {
        let node = NodeId::new(get_str(&mut cur)?);
        let incarnation = get_u64(&mut cur)?;
        let status = take_u8(&mut cur)?;
        members.push(MemberDelta {
            node,
            incarnation,
            status,
        });
    }

    let m = get_u32(&mut cur)? as usize;
    let mut metadata = Vec::with_capacity(m.min(1024));
    for _ in 0..m {
        let key = get_str(&mut cur)?;
        let version = get_u64(&mut cur)?;
        let writer = NodeId::new(get_str(&mut cur)?);
        let value = get_str(&mut cur)?;
        metadata.push(MetaDelta {
            key,
            version,
            writer,
            value,
        });
    }

    Some(Frame {
        kind,
        group,
        target,
        members,
        metadata,
    })
}

/// Cheaply reads just the [`GroupId`] from an encoded frame without decoding the
/// body, so a driver can route an inbound message to the correct group actor.
#[must_use]
pub fn peek_group(bytes: &[u8]) -> Option<GroupId> {
    let mut cur = bytes;
    let _kind = take_u8(&mut cur)?;
    Some(GroupId::new(get_str(&mut cur)?))
}

// ---- minimal little-endian codec helpers ---------------------------------

fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_str(out: &mut Vec<u8>, s: &str) {
    put_u32(out, s.len() as u32);
    out.extend_from_slice(s.as_bytes());
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

    fn sample() -> Frame {
        Frame {
            kind: Kind::PingReq,
            group: GroupId::new("shard-42"),
            target: Some(NodeId::new("node-c")),
            members: vec![
                MemberDelta {
                    node: NodeId::new("node-a"),
                    incarnation: 2,
                    status: 0,
                },
                MemberDelta {
                    node: NodeId::new("node-b"),
                    incarnation: 5,
                    status: 1,
                },
            ],
            metadata: vec![MetaDelta {
                key: "routing".into(),
                version: 3,
                writer: NodeId::new("node-a"),
                value: "v3".into(),
            }],
        }
    }

    #[test]
    fn round_trips() {
        let frame = sample();
        let bytes = encode(&frame);
        assert_eq!(decode(&bytes), Some(frame));
        assert_eq!(peek_group(&bytes), Some(GroupId::new("shard-42")));
    }

    #[test]
    fn truncated_input_decodes_to_none_not_panic() {
        let bytes = encode(&sample());
        for cut in 0..bytes.len() {
            let _ = decode(&bytes[..cut]); // must never panic
        }
        assert_eq!(decode(&[]), None);
    }
}
