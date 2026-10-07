//! The RPC frame codec: what one [`DataStream`](groupnet_transport::bulk::DataStream)
//! payload carries.
//!
//! Every frame starts with the codec version byte and a kind byte; all
//! integers are big-endian.
//!
//! | Kind | Layout after `version`, `kind` |
//! |------|--------------------------------|
//! | `0` Request | `id: u64`, `deadline_ms: u32` (nonzero), payload (rest of frame) |
//! | `1` Response | `id: u64`, payload (rest of frame) |
//! | `2` Error | `id: u64`, `code: u16`, UTF-8 message (rest of frame) |
//!
//! `deadline_ms` is the caller's *remaining* budget when the request was
//! sent, so the server measures it from receipt and never compares clocks.
//! Decoding fails closed: a wrong version, an unknown kind, a short head, a
//! zero deadline or a non-UTF-8 message is malformed, and the connection that
//! carried it is dropped.

use std::fmt;

use bytes::Bytes;
use zerocopy::byteorder::big_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::RpcStatus;

/// The codec version byte. Peers upgrade together, so this only rejects a
/// mis-deployed node's frames; bump it whenever a kind or layout changes.
pub(crate) const VERSION: u8 = 1;

const KIND_REQUEST: u8 = 0;
const KIND_RESPONSE: u8 = 1;
const KIND_ERROR: u8 = 2;

/// The fields every kind starts with; alone, a response's whole head.
#[derive(Clone, Copy, Debug, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct Prefix {
    version: u8,
    kind: u8,
    id: U64,
}

/// A request's head: the prefix and the caller's remaining budget.
#[derive(Clone, Copy, Debug, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct RequestHead {
    prefix: Prefix,
    deadline_ms: U32,
}

/// An error's head: the prefix and the status code.
#[derive(Clone, Copy, Debug, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct ErrorHead {
    prefix: Prefix,
    code: U16,
}

/// Head bytes of a request: version, kind, id, deadline.
pub(crate) const REQUEST_HEAD: usize = size_of::<RequestHead>();
/// Head bytes of a response: version, kind, id.
pub(crate) const RESPONSE_HEAD: usize = size_of::<Prefix>();
/// Head bytes of an error: version, kind, id, code.
pub(crate) const ERROR_HEAD: usize = size_of::<ErrorHead>();

/// One decoded RPC frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Frame {
    /// A call: run the handler on `payload` within `deadline_ms` of receipt.
    Request {
        /// Correlates the answer; unique per connection.
        id: u64,
        /// The caller's remaining budget, in milliseconds (never zero).
        deadline_ms: u32,
        /// The opaque request body.
        payload: Bytes,
    },
    /// A handler's success.
    Response {
        /// The request this answers.
        id: u64,
        /// The opaque response body.
        payload: Bytes,
    },
    /// A handler's (or the server's) failure.
    Error {
        /// The request this answers.
        id: u64,
        /// What went wrong.
        status: RpcStatus,
    },
}

/// A frame ready to write: a typed head followed by a body that is written
/// from its own buffer, so a payload is never copied into a frame buffer.
#[derive(Debug)]
pub(crate) struct Encoded {
    head: Head,
    body: Bytes,
}

/// The fixed fields of one encoded frame, by kind.
#[derive(Debug)]
enum Head {
    Request(RequestHead),
    Response(Prefix),
    Error(ErrorHead),
}

impl Encoded {
    /// The version, kind and fixed fields.
    pub(crate) fn head(&self) -> &[u8] {
        match &self.head {
            Head::Request(head) => head.as_bytes(),
            Head::Response(head) => head.as_bytes(),
            Head::Error(head) => head.as_bytes(),
        }
    }

    /// The payload or error message.
    pub(crate) fn body(&self) -> &[u8] {
        &self.body
    }

    /// The data-plane frame length this encodes to.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.head().len() + self.body.len()
    }
}

/// Why a frame failed to decode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Malformed(&'static str);

impl fmt::Display for Malformed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "malformed rpc frame: {}", self.0)
    }
}

impl Prefix {
    fn new(kind: u8, id: u64) -> Self {
        Self {
            version: VERSION,
            kind,
            id: U64::new(id),
        }
    }
}

impl Frame {
    /// Encodes this frame without copying its payload.
    pub(crate) fn encode(self) -> Encoded {
        let (head, body) = match self {
            Self::Request {
                id,
                deadline_ms,
                payload,
            } => (
                Head::Request(RequestHead {
                    prefix: Prefix::new(KIND_REQUEST, id),
                    deadline_ms: U32::new(deadline_ms),
                }),
                payload,
            ),
            Self::Response { id, payload } => {
                (Head::Response(Prefix::new(KIND_RESPONSE, id)), payload)
            }
            Self::Error { id, status } => (
                Head::Error(ErrorHead {
                    prefix: Prefix::new(KIND_ERROR, id),
                    code: U16::new(status.code),
                }),
                Bytes::from(status.message),
            ),
        };
        Encoded { head, body }
    }

    /// Decodes one data-plane frame. The payload is a zero-copy slice of
    /// `frame`.
    ///
    /// # Errors
    /// [`Malformed`] for anything but an exact frame of a known kind.
    pub(crate) fn decode(frame: &Bytes) -> Result<Self, Malformed> {
        let bytes = frame.as_ref();
        let (prefix, _) = Prefix::ref_from_prefix(bytes).map_err(|_| Malformed("short head"))?;
        if prefix.version != VERSION {
            return Err(Malformed("unknown codec version"));
        }
        let id = prefix.id.get();
        match prefix.kind {
            KIND_REQUEST => {
                let (head, _) = RequestHead::ref_from_prefix(bytes)
                    .map_err(|_| Malformed("short request head"))?;
                let deadline_ms = head.deadline_ms.get();
                if deadline_ms == 0 {
                    return Err(Malformed("zero deadline"));
                }
                Ok(Self::Request {
                    id,
                    deadline_ms,
                    payload: frame.slice(REQUEST_HEAD..),
                })
            }
            KIND_RESPONSE => Ok(Self::Response {
                id,
                payload: frame.slice(RESPONSE_HEAD..),
            }),
            KIND_ERROR => {
                let (head, message) =
                    ErrorHead::ref_from_prefix(bytes).map_err(|_| Malformed("short error head"))?;
                let message = std::str::from_utf8(message)
                    .map_err(|_| Malformed("non-UTF-8 error message"))?;
                Ok(Self::Error {
                    id,
                    status: RpcStatus::new(head.code.get(), message),
                })
            }
            _ => Err(Malformed("unknown kind")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Encodes `frame` into the contiguous bytes a data-plane frame carries.
    fn wire(frame: Frame) -> Bytes {
        let encoded = frame.encode();
        let mut bytes = encoded.head().to_vec();
        bytes.extend_from_slice(encoded.body());
        assert_eq!(bytes.len(), encoded.len());
        Bytes::from(bytes)
    }

    fn samples() -> Vec<Frame> {
        vec![
            Frame::Request {
                id: 0,
                deadline_ms: 1,
                payload: Bytes::new(),
            },
            Frame::Request {
                id: u64::MAX,
                deadline_ms: u32::MAX,
                payload: Bytes::from_static(b"request body"),
            },
            Frame::Response {
                id: 7,
                payload: Bytes::from_static(b"\x00\xffbinary"),
            },
            Frame::Response {
                id: 8,
                payload: Bytes::new(),
            },
            Frame::Error {
                id: 9,
                status: RpcStatus::new(404, "no such key: caf\u{e9}"),
            },
            Frame::Error {
                id: 10,
                status: RpcStatus::new(RpcStatus::DEADLINE_EXCEEDED, ""),
            },
        ]
    }

    #[test]
    fn every_kind_round_trips() {
        for frame in samples() {
            assert_eq!(Frame::decode(&wire(frame.clone())), Ok(frame));
        }
    }

    #[test]
    fn the_layout_is_the_documented_one() {
        // The typed heads are unaligned and padding-free: 1 + 1 + 8 (+ 4 | + 2).
        assert_eq!((REQUEST_HEAD, RESPONSE_HEAD, ERROR_HEAD), (14, 10, 12));
        let bytes = wire(Frame::Request {
            id: 0x0102_0304_0506_0708,
            deadline_ms: 0x0A0B_0C0D,
            payload: Bytes::from_static(b"p"),
        });
        assert_eq!(
            &bytes[..],
            &[
                VERSION, 0, 1, 2, 3, 4, 5, 6, 7, 8, 0x0A, 0x0B, 0x0C, 0x0D, b'p'
            ]
        );
        let bytes = wire(Frame::Error {
            id: 1,
            status: RpcStatus::new(0x0203, "m"),
        });
        assert_eq!(
            &bytes[..],
            &[VERSION, 2, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, b'm']
        );
    }

    #[test]
    fn every_truncated_head_is_rejected() {
        for frame in samples() {
            let bytes = wire(frame.clone());
            let head = match frame {
                Frame::Request { .. } => REQUEST_HEAD,
                Frame::Response { .. } => RESPONSE_HEAD,
                Frame::Error { .. } => ERROR_HEAD,
            };
            for cut in 0..head {
                assert!(
                    Frame::decode(&bytes.slice(..cut)).is_err(),
                    "{frame:?} cut at {cut} decoded"
                );
            }
        }
    }

    #[test]
    fn a_foreign_version_or_kind_is_rejected() {
        let good = wire(Frame::Response {
            id: 1,
            payload: Bytes::from_static(b"x"),
        });
        for (index, value) in [(0, VERSION + 1), (0, 0), (1, 3), (1, u8::MAX)] {
            let mut bad = good.to_vec();
            bad[index] = value;
            assert!(
                Frame::decode(&Bytes::from(bad)).is_err(),
                "byte {index} = {value}"
            );
        }
    }

    #[test]
    fn a_zero_deadline_is_rejected() {
        let mut bytes = wire(Frame::Request {
            id: 1,
            deadline_ms: 1,
            payload: Bytes::new(),
        })
        .to_vec();
        bytes[13] = 0;
        assert_eq!(
            Frame::decode(&Bytes::from(bytes)),
            Err(Malformed("zero deadline"))
        );
    }

    #[test]
    fn a_non_utf8_error_message_is_rejected() {
        let mut bytes = wire(Frame::Error {
            id: 1,
            status: RpcStatus::new(1, "ok"),
        })
        .to_vec();
        bytes.push(0xFF);
        assert!(Frame::decode(&Bytes::from(bytes)).is_err());
    }

    #[test]
    fn a_decoded_payload_shares_the_frame_buffer() {
        let bytes = wire(Frame::Response {
            id: 1,
            payload: Bytes::from_static(b"shared"),
        });
        let Ok(Frame::Response { payload, .. }) = Frame::decode(&bytes) else {
            panic!("decodes");
        };
        assert_eq!(payload.as_ptr(), bytes[RESPONSE_HEAD..].as_ptr());
    }
}
