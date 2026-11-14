//! The RPC frame codec: what one [`DataStream`](groupnet_transport::bulk::DataStream)
//! payload carries.
//!
//! Every frame starts with the codec version byte and a kind byte; all
//! integers are big-endian.
//!
//! | Kind | Direction | Layout after `version`, `kind` |
//! |------|-----------|--------------------------------|
//! | `0` Request | client → server | `id: u64`, `deadline_ms: u32` (nonzero), payload (rest of frame) |
//! | `1` Response | server → client | `id: u64`, payload (rest of frame) |
//! | `2` Error | server → client | `id: u64`, `code: u16`, UTF-8 message (rest of frame) |
//! | `3` Subscribe | client → server | `id: u64`, `window: u32` (nonzero), payload (rest of frame) |
//! | `4` Item | server → client | `id: u64`, payload (rest of frame) |
//! | `5` End | server → client | `id: u64` |
//! | `6` Credit | client → server | `id: u64`, `grant: u32` (nonzero) |
//! | `7` Cancel | client → server | `id: u64` |
//!
//! `deadline_ms` is the caller's *remaining* budget when the request was
//! sent, so the server measures it from receipt and never compares clocks.
//! A subscription's `window` is how many items the server may send before
//! its first Credit; each Credit adds `grant`. Calls and subscriptions share
//! one id space per connection; Error also ends a subscription.
//! Decoding fails closed: a wrong version, an unknown kind, a short head, a
//! zero deadline, window or grant, bytes after an End, Credit or Cancel, or
//! a non-UTF-8 message is malformed, and the connection that carried it is
//! dropped.

use std::fmt;
use std::num::NonZeroU32;

use bytes::Bytes;
use zerocopy::byteorder::big_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::RpcStatus;

/// The codec version byte. Peers upgrade together, so this only rejects a
/// mis-deployed node's frames; it stays 1 until the first deployment.
pub(crate) const VERSION: u8 = 1;

const KIND_REQUEST: u8 = 0;
const KIND_RESPONSE: u8 = 1;
const KIND_ERROR: u8 = 2;
const KIND_SUBSCRIBE: u8 = 3;
const KIND_ITEM: u8 = 4;
const KIND_END: u8 = 5;
const KIND_CREDIT: u8 = 6;
const KIND_CANCEL: u8 = 7;

/// The fields every kind starts with; alone, the whole head of a response,
/// an item, an end and a cancel.
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

/// A subscribe's head: the prefix and the initial credit window.
#[derive(Clone, Copy, Debug, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct SubscribeHead {
    prefix: Prefix,
    window: U32,
}

/// A credit frame, whole: the prefix and the items granted.
#[derive(Clone, Copy, Debug, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct CreditHead {
    prefix: Prefix,
    grant: U32,
}

/// Head bytes of a request: version, kind, id, deadline.
pub(crate) const REQUEST_HEAD: usize = size_of::<RequestHead>();
/// Head bytes of a response: version, kind, id.
pub(crate) const RESPONSE_HEAD: usize = size_of::<Prefix>();
/// Head bytes of an error: version, kind, id, code.
pub(crate) const ERROR_HEAD: usize = size_of::<ErrorHead>();
/// Head bytes of a subscribe: version, kind, id, window.
pub(crate) const SUBSCRIBE_HEAD: usize = size_of::<SubscribeHead>();
/// Head bytes of an item: version, kind, id.
pub(crate) const ITEM_HEAD: usize = size_of::<Prefix>();
/// Bytes of a whole end or cancel frame: version, kind, id.
const BARE_FRAME: usize = size_of::<Prefix>();
/// Bytes of a whole credit frame: version, kind, id, grant.
const CREDIT_FRAME: usize = size_of::<CreditHead>();

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
    /// A handler's (or the server's) failure; ends a call or a subscription.
    Error {
        /// The request or subscription this answers.
        id: u64,
        /// What went wrong.
        status: RpcStatus,
    },
    /// Opens a subscription: run the subscribe handler on `payload`, sending
    /// at most `window` items before the first credit.
    Subscribe {
        /// Names the subscription; unique per connection, shared with calls.
        id: u64,
        /// Items the server may send before it receives credit.
        window: NonZeroU32,
        /// The opaque subscribe body.
        payload: Bytes,
    },
    /// One item of a subscription.
    Item {
        /// The subscription it belongs to.
        id: u64,
        /// The opaque item body.
        payload: Bytes,
    },
    /// A subscription's handler finished: no more items follow.
    End {
        /// The subscription that ended.
        id: u64,
    },
    /// The client consumed items: the server may send `grant` more.
    Credit {
        /// The subscription credited.
        id: u64,
        /// Further items allowed.
        grant: NonZeroU32,
    },
    /// The client no longer wants the subscription.
    Cancel {
        /// The subscription cancelled.
        id: u64,
    },
}

/// A frame ready to write: a typed head followed by a body that is written
/// from its own buffer, so a payload is never copied into a frame buffer.
#[derive(Debug)]
pub(crate) struct Encoded {
    head: Head,
    body: Bytes,
}

/// The fixed fields of one encoded frame, by layout.
#[derive(Debug)]
enum Head {
    /// Response, item, end and cancel.
    Bare(Prefix),
    Request(RequestHead),
    Error(ErrorHead),
    Subscribe(SubscribeHead),
    Credit(CreditHead),
}

impl Encoded {
    /// The version, kind and fixed fields.
    pub(crate) fn head(&self) -> &[u8] {
        match &self.head {
            Head::Bare(head) => head.as_bytes(),
            Head::Request(head) => head.as_bytes(),
            Head::Error(head) => head.as_bytes(),
            Head::Subscribe(head) => head.as_bytes(),
            Head::Credit(head) => head.as_bytes(),
        }
    }

    /// The payload or error message; empty for end, credit and cancel.
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
            Self::Response { id, payload } => (Head::Bare(Prefix::new(KIND_RESPONSE, id)), payload),
            Self::Error { id, status } => (
                Head::Error(ErrorHead {
                    prefix: Prefix::new(KIND_ERROR, id),
                    code: U16::new(status.code),
                }),
                Bytes::from(status.message),
            ),
            Self::Subscribe {
                id,
                window,
                payload,
            } => (
                Head::Subscribe(SubscribeHead {
                    prefix: Prefix::new(KIND_SUBSCRIBE, id),
                    window: U32::new(window.get()),
                }),
                payload,
            ),
            Self::Item { id, payload } => (Head::Bare(Prefix::new(KIND_ITEM, id)), payload),
            Self::End { id } => (Head::Bare(Prefix::new(KIND_END, id)), Bytes::new()),
            Self::Credit { id, grant } => (
                Head::Credit(CreditHead {
                    prefix: Prefix::new(KIND_CREDIT, id),
                    grant: U32::new(grant.get()),
                }),
                Bytes::new(),
            ),
            Self::Cancel { id } => (Head::Bare(Prefix::new(KIND_CANCEL, id)), Bytes::new()),
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
            KIND_SUBSCRIBE => {
                let (head, _) = SubscribeHead::ref_from_prefix(bytes)
                    .map_err(|_| Malformed("short subscribe head"))?;
                let window = NonZeroU32::new(head.window.get()).ok_or(Malformed("zero window"))?;
                Ok(Self::Subscribe {
                    id,
                    window,
                    payload: frame.slice(SUBSCRIBE_HEAD..),
                })
            }
            KIND_ITEM => Ok(Self::Item {
                id,
                payload: frame.slice(ITEM_HEAD..),
            }),
            KIND_END => exact(bytes, BARE_FRAME).map(|()| Self::End { id }),
            KIND_CREDIT => {
                exact(bytes, CREDIT_FRAME)?;
                let (head, _) = CreditHead::ref_from_prefix(bytes)
                    .map_err(|_| Malformed("short credit frame"))?;
                let grant = NonZeroU32::new(head.grant.get()).ok_or(Malformed("zero grant"))?;
                Ok(Self::Credit { id, grant })
            }
            KIND_CANCEL => exact(bytes, BARE_FRAME).map(|()| Self::Cancel { id }),
            _ => Err(Malformed("unknown kind")),
        }
    }
}

/// Fails unless a fixed-size frame is exactly `len` bytes.
fn exact(bytes: &[u8], len: usize) -> Result<(), Malformed> {
    match bytes.len().cmp(&len) {
        std::cmp::Ordering::Equal => Ok(()),
        std::cmp::Ordering::Less => Err(Malformed("short fixed-size frame")),
        std::cmp::Ordering::Greater => Err(Malformed("bytes after a fixed-size frame")),
    }
}

#[cfg(test)]
mod tests;
