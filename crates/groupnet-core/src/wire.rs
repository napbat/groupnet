//! The gossip wire protocol and its codec.
//!
//! The engine owns serialization so that [`Transport`] implementations only
//! ever move opaque `&[u8]` — a TCP, UDP, IPC, or shared-memory binding never
//! re-encodes the protocol and so can never drift out of sync with it.
//!
//! The codec is a small hand-rolled length-prefixed format (little-endian).
//! It's deliberately dependency-free; swap it for `postcard`/`bincode` later if
//! the protocol grows, but keep this crate dep-free by default.
//!
//! **Invariant:** every message encodes its [`GroupId`] immediately after the
//! one-byte tag. [`peek_group`] relies on this so a driver can demux an inbound
//! frame to the right group without fully decoding it.
//!
//! [`Transport`]: https://docs.rs/groupnet-transport

use crate::{GroupId, NodeId};

const TAG_GOSSIP: u8 = 1;

/// A single metadata key's value plus its last-writer-wins timestamp. Receivers
/// keep the entry with the greater `(version, writer)` — a per-key LWW-register.
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

/// A protocol message exchanged between nodes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Msg {
    /// A gossip round carrying the sender's current membership view and its
    /// metadata entries. Receivers merge membership by set union (grow-set in
    /// this scaffold) and metadata by last-writer-wins.
    Gossip {
        /// The group this delta concerns.
        group: GroupId,
        /// Members the sender currently knows about.
        members: Vec<NodeId>,
        /// Metadata entries the sender currently holds.
        metadata: Vec<MetaDelta>,
    },
}

/// Encodes a message to bytes for a transport to ship.
#[must_use]
pub fn encode(msg: &Msg) -> Vec<u8> {
    let mut out = Vec::new();
    match msg {
        Msg::Gossip {
            group,
            members,
            metadata,
        } => {
            out.push(TAG_GOSSIP);
            put_str(&mut out, group.as_str());
            put_u32(&mut out, members.len() as u32);
            for m in members {
                put_str(&mut out, m.as_str());
            }
            put_u32(&mut out, metadata.len() as u32);
            for d in metadata {
                put_str(&mut out, &d.key);
                put_u64(&mut out, d.version);
                put_str(&mut out, d.writer.as_str());
                put_str(&mut out, &d.value);
            }
        }
    }
    out
}

/// Decodes a message previously produced by [`encode`]. Returns `None` on any
/// malformed or truncated input — the engine treats undecodable frames as
/// dropped, which is safe because the transport contract is best-effort anyway.
#[must_use]
pub fn decode(bytes: &[u8]) -> Option<Msg> {
    let mut cur = bytes;
    let tag = take_u8(&mut cur)?;
    match tag {
        TAG_GOSSIP => {
            let group = GroupId::new(get_str(&mut cur)?);
            let n = get_u32(&mut cur)? as usize;
            let mut members = Vec::with_capacity(n.min(1024));
            for _ in 0..n {
                members.push(NodeId::new(get_str(&mut cur)?));
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
            Some(Msg::Gossip {
                group,
                members,
                metadata,
            })
        }
        _ => None,
    }
}

/// Cheaply reads just the [`GroupId`] from an encoded frame without decoding the
/// body, so a driver can route an inbound message to the correct group actor.
#[must_use]
pub fn peek_group(bytes: &[u8]) -> Option<GroupId> {
    let mut cur = bytes;
    let _tag = take_u8(&mut cur)?;
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

    #[test]
    fn round_trips() {
        let msg = Msg::Gossip {
            group: GroupId::new("shard-42"),
            members: vec![NodeId::new("node-a"), NodeId::new("node-b")],
            metadata: vec![
                MetaDelta {
                    key: "routing".into(),
                    version: 3,
                    writer: NodeId::new("node-a"),
                    value: "v3".into(),
                },
                MetaDelta {
                    key: "shards".into(),
                    version: 1,
                    writer: NodeId::new("node-b"),
                    value: "16".into(),
                },
            ],
        };
        let bytes = encode(&msg);
        assert_eq!(decode(&bytes), Some(msg));
        assert_eq!(peek_group(&bytes), Some(GroupId::new("shard-42")));
    }

    #[test]
    fn truncated_input_decodes_to_none_not_panic() {
        let bytes = encode(&Msg::Gossip {
            group: GroupId::new("g"),
            members: vec![NodeId::new("n")],
            metadata: vec![MetaDelta {
                key: "k".into(),
                version: 7,
                writer: NodeId::new("n"),
                value: "v".into(),
            }],
        });
        for cut in 0..bytes.len() {
            let _ = decode(&bytes[..cut]); // must never panic
        }
        assert_eq!(decode(&[]), None);
    }
}
