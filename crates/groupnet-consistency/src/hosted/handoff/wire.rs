//! The handoff protocol's codec: five frames, one stream, no allocation for the
//! payload that matters.
//!
//! # Stream-embedded, not gossiped
//!
//! Every other codec in this crate encodes a *group entry* — a small value that
//! crosses the fabric whole or not at all, framed and length-checked by the
//! core's wire protocol long before it arrives. This one does not. These frames
//! ride a data-plane stream ([`DataStream`](groupnet_transport::bulk::DataStream)
//! and its length-delimited framing), which is a different threat model in two
//! ways worth naming:
//!
//! * **The peer chose to open this stream, and may be speaking anything.** A
//!   stream accepted on the bulk transport carries no guarantee of being a
//!   handoff at all, so every frame begins with a magic and a version, and
//!   [`is_request`] exists to demux the first frame cheaply before anything
//!   heavier runs.
//! * **A stream ends at an EOF, which the framing reports as a clean `None`.**
//!   A donor that dies mid-snapshot is indistinguishable, at the framing layer,
//!   from one that finished — so the protocol carries its own terminator, the
//!   `Done` frame with the counts
//!   [`HandoffCore::done_consistent`](super::HandoffCore::done_consistent)
//!   checks. Silence is never success here.
//!
//! # The layout
//!
//! Every frame opens with the same six bytes:
//!
//! ```text
//! (magic: b"GNHO") (version: u8 = 1) (kind: u8)
//! ```
//!
//! and continues, little-endian throughout, per kind:
//!
//! ```text
//! 1 Request { (group_len: u32) group (name_len: u32) name (need: record-map) }
//! 2 Offer   { (fence_epoch: u64) (host_len: u32) host (covers: record-map) }
//! 3 Refuse  { (code: u8) [ (have: record-map) — code 1 only ] }
//! 4 Chunk   { opaque bytes to the end of the frame }
//! 5 Done    { (chunks: u64) (bytes: u64) (final_epoch: u64) (host_len: u32) host }
//! ```
//!
//! The **record map** is the commit ledger's, byte for byte and by the same
//! code: `(count: u32) (writer_len: u32, writer, token_epoch: u64,
//! token_seq: u64)*`, count-checked and distinct-writer-checked. Sharing it is
//! not tidiness — a watermark map that parsed differently in two places would be
//! a silent divergence between what a donor claims to cover and what a ledger
//! says a voter applied, and the whole handoff turns on those two numbers being
//! the same kind of number.
//!
//! A **host** is length-prefixed with `0` meaning *hostless* — the honest
//! `Option<NodeId>` a follower publishes before it has learned who won an epoch.
//! An empty node id would encode identically, which is a non-problem because an
//! empty node id is not a legal identity; the wire simply has no way to spell
//! one, and does not need one.
//!
//! # All or nothing, in three answers
//!
//! [`decode`] separates two failures that look alike and must not be treated
//! alike:
//!
//! * `Err(`[`HandoffError::Protocol`]`)` — **the peer is speaking something
//!   else**: a foreign magic, a version this build does not know, a frame kind
//!   it does not define, a refusal code it cannot name. Loud, and terminal: the
//!   alternative is waiting forever on a stream that will never say `Done`.
//! * `Ok(None)` — **these bytes are not a whole frame of that kind**: truncated
//!   anywhere, or carrying a tail behind a complete body. The framing layer
//!   delivers whole frames, so a driver turns this into
//!   [`HandoffError::Truncated`] rather than reading more; the distinction is
//!   kept here because the codec's job is to describe the bytes, not to decide
//!   what the caller does about them.
//! * `Ok(Some(frame))` — the whole frame, and every byte of it accounted for.
//!
//! There is no fourth answer and in particular no partial one: a half-read
//! `covers` map that decoded to a smaller honest-looking map is exactly the
//! failure the ledger's record count was added to prevent, and it would be worse
//! here, where the map is a claim about state a node is about to adopt.

use bytes::{Bytes, BytesMut};
use groupnet_core::{GroupId, NodeId};

use super::{HandoffError, RefusalCode};
use crate::hosted::Watermarks;
use crate::hosted::ledger::{decode_records, encode_records};

/// The four bytes that say "this stream is a groupnet handoff".
const MAGIC: [u8; 4] = *b"GNHO";

/// The protocol version. A peer that sends anything else is refused loudly —
/// there is no negotiation here, and [`RefusalCode::Version`] is how a donor
/// says so in its own words.
const VERSION: u8 = 1;

const KIND_REQUEST: u8 = 1;
const KIND_OFFER: u8 = 2;
const KIND_REFUSE: u8 = 3;
const KIND_CHUNK: u8 = 4;
const KIND_DONE: u8 = 5;

/// Magic + version + kind: the fixed prefix of every frame.
const PREFIX: usize = MAGIC.len() + 2;

/// One frame of the handoff protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Frame {
    /// The requester opens with this: which write path, and how far it needs a
    /// donor to have got.
    Request {
        /// The group whose write path is being recovered.
        group: GroupId,
        /// The write path's name (empty for the default one).
        name: String,
        /// The per-writer watermarks the requester must reach.
        need: Watermarks,
    },
    /// The donor's acceptance: its fence stamp, and what its snapshot covers.
    Offer {
        /// The leadership epoch the donor had adopted when it opened.
        fence_epoch: u64,
        /// Who it believed held that epoch; `None` is hostless and honest.
        host: Option<NodeId>,
        /// The watermarks the snapshot is at or above.
        covers: Watermarks,
    },
    /// The donor's refusal.
    Refuse {
        /// Why.
        code: RefusalCode,
        /// For [`RefusalCode::NotCovered`], what the donor does cover; empty
        /// for every other code, which carries no map on the wire.
        have: Watermarks,
    },
    /// One opaque piece of the snapshot.
    Chunk(Bytes),
    /// The terminator, and the counts that prove the stream was whole.
    Done {
        /// Chunks sent.
        chunks: u64,
        /// Payload bytes sent, framing excluded.
        bytes: u64,
        /// The donor's fence epoch at the end of the transfer.
        final_epoch: u64,
        /// The host it believed held that epoch at the end of the transfer.
        final_host: Option<NodeId>,
    },
}

/// Whether `payload` looks like a handoff `Request` — magic, version, kind, and
/// nothing more.
///
/// The demux test a responder runs on the first frame of an accepted bulk
/// stream, before it decides whether this stream is its business. A cheap
/// prefix test and **not** a validity claim: a `true` still goes through
/// [`decode`], which is what refuses a truncated or malformed request.
pub(crate) fn is_request(payload: &[u8]) -> bool {
    payload.len() >= PREFIX
        && payload[..MAGIC.len()] == MAGIC
        && payload[MAGIC.len()] == VERSION
        && payload[MAGIC.len() + 1] == KIND_REQUEST
}

/// `frame` on the wire.
///
/// A `Chunk` copies its payload once, to sit behind the six-byte prefix in one
/// buffer — the framing layer writes a single [`Bytes`] per frame, so the
/// alternative is a second frame kind for headers and a re-assembly rule to go
/// with it. The decode side is copy-free, which is the side that runs per chunk
/// on the node doing the recovering.
pub(crate) fn encode(frame: &Frame) -> Bytes {
    let mut out = Vec::new();
    match frame {
        Frame::Request { group, name, need } => {
            put_prefix(&mut out, KIND_REQUEST);
            put_str(&mut out, group.as_str());
            put_str(&mut out, name);
            encode_records(need, &mut out);
        }
        Frame::Offer {
            fence_epoch,
            host,
            covers,
        } => {
            put_prefix(&mut out, KIND_OFFER);
            out.extend_from_slice(&fence_epoch.to_le_bytes());
            put_host(&mut out, host.as_ref());
            encode_records(covers, &mut out);
        }
        Frame::Refuse { code, have } => {
            put_prefix(&mut out, KIND_REFUSE);
            out.push(code.code());
            // Only `NotCovered` has anything to say beyond its name. A `have`
            // map handed in with any other code is dropped here rather than
            // travelling as a field nobody may read.
            if *code == RefusalCode::NotCovered {
                encode_records(have, &mut out);
            }
        }
        Frame::Chunk(data) => {
            let mut buf = BytesMut::with_capacity(PREFIX + data.len());
            buf.extend_from_slice(&MAGIC);
            buf.extend_from_slice(&[VERSION, KIND_CHUNK]);
            buf.extend_from_slice(data);
            return buf.freeze();
        }
        Frame::Done {
            chunks,
            bytes,
            final_epoch,
            final_host,
        } => {
            put_prefix(&mut out, KIND_DONE);
            out.extend_from_slice(&chunks.to_le_bytes());
            out.extend_from_slice(&bytes.to_le_bytes());
            out.extend_from_slice(&final_epoch.to_le_bytes());
            put_host(&mut out, final_host.as_ref());
        }
    }
    Bytes::from(out)
}

/// The whole frame in `payload`, `Ok(None)` if it is not a whole frame, or
/// `Err` if it is not this protocol at all. See the module docs for why those
/// are three answers and not two.
///
/// A `Chunk`'s payload is a zero-copy slice of `payload` — one refcount bump per
/// chunk, no matter how large the snapshot.
///
/// # Errors
/// [`HandoffError::Protocol`] for a foreign magic, an unknown version, an
/// unknown frame kind, or an undefined refusal code.
pub(crate) fn decode(payload: &Bytes) -> Result<Option<Frame>, HandoffError> {
    // A prefix shorter than the fixed header is a truncation, never a foreign
    // magic: two bytes of `GN` prove nothing either way.
    if payload.len() < PREFIX {
        return Ok(None);
    }
    if payload[..MAGIC.len()] != MAGIC {
        return Err(HandoffError::Protocol("not a handoff frame"));
    }
    if payload[MAGIC.len()] != VERSION {
        return Err(HandoffError::Protocol("unknown handoff protocol version"));
    }
    let kind = payload[MAGIC.len() + 1];
    // The one kind whose body is the rest of the frame, and the only one that
    // does not end in an exact-length check.
    if kind == KIND_CHUNK {
        return Ok(Some(Frame::Chunk(payload.slice(PREFIX..))));
    }
    let bytes = &payload[..];
    let mut at = PREFIX;
    let frame = match kind {
        KIND_REQUEST => {
            let Some(group) = take_str(bytes, &mut at) else {
                return Ok(None);
            };
            let group = GroupId::new(group);
            let Some(name) = take_str(bytes, &mut at) else {
                return Ok(None);
            };
            let name = name.to_owned();
            let Some(need) = decode_records(bytes, &mut at) else {
                return Ok(None);
            };
            Frame::Request { group, name, need }
        }
        KIND_OFFER => {
            let (Some(fence_epoch), Some(host)) =
                (take_u64(bytes, &mut at), take_str(bytes, &mut at))
            else {
                return Ok(None);
            };
            let host = host_id(host);
            let Some(covers) = decode_records(bytes, &mut at) else {
                return Ok(None);
            };
            Frame::Offer {
                fence_epoch,
                host,
                covers,
            }
        }
        KIND_REFUSE => {
            let Some(byte) = bytes.get(at).copied() else {
                return Ok(None);
            };
            at += 1;
            let Some(code) = RefusalCode::from_code(byte) else {
                return Err(HandoffError::Protocol("unknown handoff refusal code"));
            };
            let have = if code == RefusalCode::NotCovered {
                match decode_records(bytes, &mut at) {
                    Some(have) => have,
                    None => return Ok(None),
                }
            } else {
                Watermarks::new()
            };
            Frame::Refuse { code, have }
        }
        KIND_DONE => {
            let (Some(chunks), Some(count), Some(final_epoch), Some(final_host)) = (
                take_u64(bytes, &mut at),
                take_u64(bytes, &mut at),
                take_u64(bytes, &mut at),
                take_str(bytes, &mut at),
            ) else {
                return Ok(None);
            };
            Frame::Done {
                chunks,
                bytes: count,
                final_epoch,
                final_host: host_id(final_host),
            }
        }
        _ => return Err(HandoffError::Protocol("unknown handoff frame kind")),
    };
    // Nothing left over. A tail behind a complete body is a re-framing attempt,
    // and it is not a frame.
    if at == bytes.len() {
        Ok(Some(frame))
    } else {
        Ok(None)
    }
}

fn put_prefix(out: &mut Vec<u8>, kind: u8) {
    out.extend_from_slice(&MAGIC);
    out.push(VERSION);
    out.push(kind);
}

/// `(u32 len) utf-8` — the same length-prefixed shape the record map's writer
/// ids use.
fn put_str(out: &mut Vec<u8>, s: &str) {
    let raw = s.as_bytes();
    debug_assert!(
        u32::try_from(raw.len()).is_ok(),
        "identifier longer than u32::MAX bytes"
    );
    out.extend_from_slice(&u32::try_from(raw.len()).unwrap_or(u32::MAX).to_le_bytes());
    out.extend_from_slice(raw);
}

/// A host, with `0` length meaning hostless.
fn put_host(out: &mut Vec<u8>, host: Option<&NodeId>) {
    put_str(out, host.map_or("", NodeId::as_str));
}

fn take_u64(bytes: &[u8], at: &mut usize) -> Option<u64> {
    let end = at.checked_add(8)?;
    let value = u64::from_le_bytes(bytes.get(*at..end)?.try_into().ok()?);
    *at = end;
    Some(value)
}

fn take_str<'a>(bytes: &'a [u8], at: &mut usize) -> Option<&'a str> {
    let len_end = at.checked_add(4)?;
    let len = usize::try_from(u32::from_le_bytes(
        bytes.get(*at..len_end)?.try_into().ok()?,
    ))
    .ok()?;
    let end = len_end.checked_add(len)?;
    let text = std::str::from_utf8(bytes.get(len_end..end)?).ok()?;
    *at = end;
    Some(text)
}

/// A decoded host name as the `Option<NodeId>` it means: the zero-length marker
/// is *hostless*, and an empty node id — which the wire cannot spell apart from
/// it — is not a legal identity.
fn host_id(name: &str) -> Option<NodeId> {
    (!name.is_empty()).then(|| NodeId::new(name))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use groupnet_core::{GroupId, NodeId};

    use super::{
        Frame, KIND_CHUNK, KIND_DONE, KIND_OFFER, KIND_REFUSE, KIND_REQUEST, MAGIC, PREFIX,
        VERSION, decode, encode, is_request,
    };
    use crate::hosted::Watermarks;
    use crate::hosted::handoff::{HandoffError, RefusalCode};
    use crate::token::WriteToken;

    fn marks(pairs: &[(&str, u64, u64)]) -> Watermarks {
        pairs
            .iter()
            .map(|(writer, epoch, seq)| {
                (
                    NodeId::new(*writer),
                    WriteToken {
                        epoch: *epoch,
                        seq: *seq,
                    },
                )
            })
            .collect()
    }

    /// Every kind, with the awkward shapes of each: an empty map, a wide map, a
    /// unicode id, a hostless stamp, an empty chunk.
    fn table() -> Vec<Frame> {
        vec![
            Frame::Request {
                group: GroupId::new("shard-42"),
                name: String::new(),
                need: Watermarks::new(),
            },
            Frame::Request {
                group: GroupId::new("ünïcøde-group"),
                name: "docs".to_owned(),
                need: marks(&[("h1", 5, 9), ("h2", 2, 4), ("ünïcøde-node", 9, 9)]),
            },
            Frame::Offer {
                fence_epoch: 6,
                host: Some(NodeId::new("host-b")),
                covers: marks(&[("h1", 5, 9)]),
            },
            Frame::Offer {
                fence_epoch: 0,
                host: None,
                covers: Watermarks::new(),
            },
            Frame::Refuse {
                code: RefusalCode::NotCovered,
                have: marks(&[("h1", 5, 2)]),
            },
            Frame::Refuse {
                code: RefusalCode::NotCovered,
                have: Watermarks::new(),
            },
            Frame::Refuse {
                code: RefusalCode::Unavailable,
                have: Watermarks::new(),
            },
            Frame::Refuse {
                code: RefusalCode::BadRequest,
                have: Watermarks::new(),
            },
            Frame::Refuse {
                code: RefusalCode::Version,
                have: Watermarks::new(),
            },
            Frame::Chunk(Bytes::from_static(b"")),
            Frame::Chunk(Bytes::from_static(b"an opaque piece of somebody's state")),
            Frame::Done {
                chunks: 12,
                bytes: 4096,
                final_epoch: 6,
                final_host: Some(NodeId::new("host-b")),
            },
            Frame::Done {
                chunks: 0,
                bytes: 0,
                final_epoch: u64::MAX,
                final_host: None,
            },
        ]
    }

    #[test]
    fn every_kind_round_trips() {
        for frame in table() {
            let bytes = encode(&frame);
            assert_eq!(
                decode(&bytes).expect("well-formed"),
                Some(frame.clone()),
                "{frame:?}"
            );
            assert!(bytes.len() >= PREFIX);
            assert_eq!(bytes[..MAGIC.len()], MAGIC);
            assert_eq!(bytes[MAGIC.len()], VERSION);
        }
    }

    #[test]
    fn every_prefix_of_every_frame_decodes_to_nothing() {
        for frame in table() {
            let bytes = encode(&frame);
            for cut in 0..bytes.len() {
                let prefix = bytes.slice(..cut);
                // A chunk is the one kind whose body has no length of its own,
                // so a truncated chunk decodes to a shorter chunk — the framing
                // layer below owns that length, and this codec would be lying
                // to claim otherwise.
                if matches!(frame, Frame::Chunk(_)) && cut >= PREFIX {
                    assert!(matches!(decode(&prefix), Ok(Some(Frame::Chunk(_)))));
                    continue;
                }
                assert_eq!(
                    decode(&prefix).expect("a prefix is never a protocol error"),
                    None,
                    "prefix at {cut} of {frame:?}"
                );
            }
        }
    }

    #[test]
    fn a_tail_behind_a_whole_frame_is_not_a_frame() {
        for frame in table() {
            if matches!(frame, Frame::Chunk(_)) {
                continue; // A chunk's body *is* the tail.
            }
            let mut bytes = encode(&frame).to_vec();
            bytes.extend_from_slice(&[0x09, 0x00, 0x00, 0x00, 0x01]);
            assert_eq!(
                decode(&Bytes::from(bytes)).expect("still this protocol"),
                None,
                "{frame:?}"
            );
        }
    }

    #[test]
    fn a_foreign_magic_or_version_or_kind_is_a_protocol_error() {
        let whole = encode(&Frame::Offer {
            fence_epoch: 6,
            host: None,
            covers: Watermarks::new(),
        });
        let mangled = |at: usize, byte: u8| {
            let mut bytes = whole.to_vec();
            bytes[at] = byte;
            decode(&Bytes::from(bytes))
        };
        for at in 0..MAGIC.len() {
            assert!(
                matches!(mangled(at, b'X'), Err(HandoffError::Protocol(_))),
                "magic byte {at}"
            );
        }
        assert!(matches!(
            mangled(MAGIC.len(), VERSION + 1),
            Err(HandoffError::Protocol(_))
        ));
        assert!(matches!(
            mangled(MAGIC.len(), 0),
            Err(HandoffError::Protocol(_))
        ));
        for kind in [0u8, 6, 7, 200, u8::MAX] {
            assert!(
                matches!(
                    mangled(MAGIC.len() + 1, kind),
                    Err(HandoffError::Protocol(_))
                ),
                "kind {kind}"
            );
        }
        // …and every kind this version defines is one the codec knows, whatever
        // it then makes of a body that was written for another kind.
        for kind in [KIND_REQUEST, KIND_OFFER, KIND_REFUSE, KIND_CHUNK, KIND_DONE] {
            assert!(
                !matches!(
                    mangled(MAGIC.len() + 1, kind),
                    Err(HandoffError::Protocol(what)) if what.contains("frame kind")
                ),
                "kind {kind} is defined by this version"
            );
        }
    }

    #[test]
    fn an_undefined_refusal_code_is_a_protocol_error() {
        let mut bytes = encode(&Frame::Refuse {
            code: RefusalCode::Unavailable,
            have: Watermarks::new(),
        })
        .to_vec();
        let code_at = PREFIX;
        for byte in [0u8, 5, 99, u8::MAX] {
            bytes[code_at] = byte;
            assert!(
                matches!(
                    decode(&Bytes::from(bytes.clone())),
                    Err(HandoffError::Protocol(_))
                ),
                "code {byte}"
            );
        }
    }

    #[test]
    fn only_a_not_covered_refusal_carries_a_map() {
        // The `have` field is dropped for every other code rather than
        // travelling as something nobody may read — so this is deliberately not
        // a round trip, and the assertion says so.
        let sent = Frame::Refuse {
            code: RefusalCode::Unavailable,
            have: marks(&[("h1", 5, 9)]),
        };
        assert_eq!(
            decode(&encode(&sent)).expect("well-formed"),
            Some(Frame::Refuse {
                code: RefusalCode::Unavailable,
                have: Watermarks::new(),
            })
        );
        // A bare code and nothing else: three bytes past the prefix would be a
        // map header this frame must not have.
        assert_eq!(encode(&sent).len(), PREFIX + 1);
        // …while `NotCovered` carries its count even when empty.
        let empty = Frame::Refuse {
            code: RefusalCode::NotCovered,
            have: Watermarks::new(),
        };
        assert_eq!(encode(&empty).len(), PREFIX + 1 + 4);
    }

    #[test]
    fn a_record_count_that_disagrees_decodes_to_nothing() {
        // The shared ledger check, exercised through a frame: the count is the
        // thing that stops a truncated map from becoming a smaller honest one.
        let whole = encode(&Frame::Offer {
            fence_epoch: 6,
            host: Some(NodeId::new("host-b")),
            covers: marks(&[("h1", 5, 9), ("h2", 2, 4)]),
        });
        let map_at = PREFIX + 8 + 4 + "host-b".len();
        assert_eq!(&whole[map_at..map_at + 4], &2u32.to_le_bytes());
        // Over-counted: three claimed, two present — the third runs off the end.
        let mut over = whole.to_vec();
        over[map_at..map_at + 4].copy_from_slice(&3u32.to_le_bytes());
        assert_eq!(decode(&Bytes::from(over)).expect("this protocol"), None);
        // Under-counted: one claimed, two present — a tail is left.
        let mut under = whole.to_vec();
        under[map_at..map_at + 4].copy_from_slice(&1u32.to_le_bytes());
        assert_eq!(decode(&Bytes::from(under)).expect("this protocol"), None);
        // Collapsed: two records, one writer. The bytes frame perfectly and end
        // exactly; only the distinct-writer check catches it.
        let duplicated = encode(&Frame::Offer {
            fence_epoch: 6,
            host: Some(NodeId::new("host-b")),
            covers: marks(&[("h1", 5, 9)]),
        });
        let mut collapsed = duplicated.to_vec();
        collapsed[map_at..map_at + 4].copy_from_slice(&2u32.to_le_bytes());
        let record = &duplicated[map_at + 4..];
        collapsed.extend_from_slice(record);
        assert_eq!(
            decode(&Bytes::from(collapsed)).expect("this protocol"),
            None,
            "a duplicated writer id is a map shorter than its own header"
        );
    }

    #[test]
    fn a_hostless_fence_survives_the_wire_as_hostless() {
        // The whole point of the zero-length host: a donor that has adopted an
        // epoch but not yet learned who won it must be able to say so, because
        // `HandoffCore::staleness` treats that stamp differently from a named
        // one — and identically to a named one it happens to agree with.
        for frame in [
            Frame::Offer {
                fence_epoch: 6,
                host: None,
                covers: marks(&[("h1", 5, 9)]),
            },
            Frame::Done {
                chunks: 1,
                bytes: 2,
                final_epoch: 6,
                final_host: None,
            },
        ] {
            let decoded = decode(&encode(&frame))
                .expect("well-formed")
                .expect("whole");
            assert_eq!(decoded, frame);
            match decoded {
                Frame::Offer { host, .. } => assert_eq!(host, None),
                Frame::Done { final_host, .. } => assert_eq!(final_host, None),
                other => panic!("unexpected {other:?}"),
            }
        }
    }

    #[test]
    fn a_chunk_is_a_zero_copy_slice_of_its_frame() {
        let payload = Bytes::from(vec![7u8; 4096]);
        let framed = encode(&Frame::Chunk(payload.clone()));
        let Some(Frame::Chunk(out)) = decode(&framed).expect("well-formed") else {
            panic!("a chunk");
        };
        assert_eq!(out, payload);
        // Same allocation as the frame it came out of, not a copy of it.
        assert_eq!(out.as_ptr(), framed[PREFIX..].as_ptr());
    }

    #[test]
    fn only_a_request_demuxes_as_one() {
        for frame in table() {
            let bytes = encode(&frame);
            assert_eq!(
                is_request(&bytes),
                matches!(frame, Frame::Request { .. }),
                "{frame:?}"
            );
        }
        // Cheap and prefix-only: it says nothing about the body, which is
        // exactly why a `true` still has to go through `decode`.
        let mut stub = MAGIC.to_vec();
        stub.extend_from_slice(&[VERSION, KIND_REQUEST]);
        assert!(is_request(&stub));
        assert_eq!(
            decode(&Bytes::from(stub.clone())).expect("this protocol"),
            None
        );
        // Everything that is not this protocol is not a request.
        assert!(!is_request(b""));
        assert!(!is_request(b"GNH"));
        assert!(!is_request(&stub[..PREFIX - 1]));
        assert!(!is_request(b"GNHX\x01\x01"));
        assert!(!is_request(b"GNHO\x02\x01"));
    }
}
