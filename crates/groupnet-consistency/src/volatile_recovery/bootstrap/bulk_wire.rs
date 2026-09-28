//! Bounded, versioned correlation envelope for one donor bulk exchange.
//!
//! This public opt-in peer data-plane API carries only the nine typed donor
//! phases. Its codec frames are transport data, never source authority.

use groupnet_core::NodeId;
use groupnet_core::volatile_bootstrap::{
    BootId, BootstrapOperation, BootstrapScope, ClaimIdentity,
};

mod phase;
mod transport;
pub use phase::{
    PhaseLimits, WireReply, decode_reply, decode_request, encode_reply, encode_request,
};
pub use transport::{BootstrapBulkClient, BootstrapBulkListener, BulkError, BulkLimits};

const MAGIC: [u8; 4] = *b"GBST";
const VERSION: u8 = 1;
// Magic/version/kinds, four string lengths, two claim IDs, two operations,
// and the payload length. Variable strings and payload are charged separately.
const FIXED_BYTES: usize = 8 + 4 * 2 + 2 * (16 + 8 + 8) + 2 * (16 + 8 + 8 + 8) + 4;

/// One of the nine donor operations; no application-defined wire kind exists.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExchangeKind {
    /// Fetch one bounded image offer.
    Offer,
    /// Reserve the donor suffix at C.
    Reserve,
    /// Fetch one image chunk.
    Chunk,
    /// Attach the donor's live stream.
    Attach,
    /// Sample B with exact native cuts.
    Barrier,
    /// Sample a later B after acknowledging the prior one.
    AdvanceBarrier,
    /// Fetch one bounded suffix batch.
    Batch,
    /// Confirm one exact batch acknowledgment.
    Ack,
    /// Retire one exact reservation.
    Release,
}

impl ExchangeKind {
    fn code(self) -> u8 {
        match self {
            Self::Offer => 1,
            Self::Reserve => 2,
            Self::Chunk => 3,
            Self::Attach => 4,
            Self::Barrier => 5,
            Self::AdvanceBarrier => 6,
            Self::Batch => 7,
            Self::Ack => 8,
            Self::Release => 9,
        }
    }

    fn from_code(value: u8) -> Result<Self, WireError> {
        Ok(match value {
            1 => Self::Offer,
            2 => Self::Reserve,
            3 => Self::Chunk,
            4 => Self::Attach,
            5 => Self::Barrier,
            6 => Self::AdvanceBarrier,
            7 => Self::Batch,
            8 => Self::Ack,
            9 => Self::Release,
            _ => return Err(WireError::Invalid),
        })
    }
}

/// Exact identity repeated on each frame of an exchange.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Correlation {
    /// Complete application partition represented by the donor image.
    pub scope: BootstrapScope,
    /// Exact selected donor claim incarnation.
    pub donor: ClaimIdentity,
    /// Exact receiving follower claim incarnation.
    pub follower: ClaimIdentity,
    /// Original claim operation whose deadline cannot be extended.
    pub parent: BootstrapOperation,
    /// Current transfer child operation.
    pub child: BootstrapOperation,
}

/// Terminal refusal; it cannot be mistaken for a successful data reply.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// The requested identity or resource is no longer current.
    Stale,
    /// The donor's finite capture or request deadline expired.
    Expired,
    /// Finite queue, byte, or event admission was exhausted.
    Capacity,
    /// Image or protocol schema is unsupported.
    Schema,
    /// Donor continuity or native coverage cannot be proved.
    Continuity,
}

impl Refusal {
    fn code(self) -> u8 {
        match self {
            Self::Stale => 1,
            Self::Expired => 2,
            Self::Capacity => 3,
            Self::Schema => 4,
            Self::Continuity => 5,
        }
    }

    fn from_code(value: u8) -> Result<Self, WireError> {
        Ok(match value {
            1 => Self::Stale,
            2 => Self::Expired,
            3 => Self::Capacity,
            4 => Self::Schema,
            5 => Self::Continuity,
            _ => return Err(WireError::Invalid),
        })
    }
}

/// A single bounded frame inside the request/reply stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    /// Exactly one bounded request body.
    Request(Vec<u8>),
    /// One bounded response data body.
    Reply(Vec<u8>),
    /// An exact refusal body, followed by a terminator.
    Refused(Refusal),
    /// Counts data frames and their encoded payload bytes, excluding itself.
    Terminator {
        /// Number of response data frames before this terminator.
        frames: u32,
        /// Combined response data payload bytes before this terminator.
        bytes: u64,
    },
}

/// Exact frame whose identity must match the active donor and child operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Envelope {
    /// One named donor phase.
    pub exchange: ExchangeKind,
    /// Exact scope, peer, and operation binding.
    pub correlation: Correlation,
    /// Request, response, refusal, or terminator payload.
    pub message: Message,
}

/// Caller-chosen finite pre-allocation wire limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WireLimits {
    /// Maximum complete encoded bootstrap body, excluding `DataStream` header.
    pub max_frame_bytes: usize,
    /// Maximum combined scope name bytes.
    pub max_scope_bytes: usize,
    /// Maximum bytes in one node identity.
    pub max_node_bytes: usize,
    /// Maximum bytes in the frame's operation payload.
    pub max_payload_bytes: usize,
}

/// Invalid, unsupported, or excessive wire input.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireError {
    /// Malformed, inconsistent, or unsupported encoded input.
    Invalid,
    /// A finite length or allocation bound was exceeded.
    Capacity,
}

impl WireLimits {
    fn valid(self) -> bool {
        self.max_frame_bytes > 0
            && self.max_scope_bytes > 0
            && self.max_node_bytes > 0
            && self.max_payload_bytes > 0
    }
}

fn identity_size(identity: &ClaimIdentity, limits: WireLimits) -> Result<usize, WireError> {
    let node = identity.node.as_str().len();
    if node == 0 || node > limits.max_node_bytes || node > usize::from(u16::MAX) {
        return Err(WireError::Capacity);
    }
    Ok(node)
}

fn validate_correlation(value: &Correlation, limits: WireLimits) -> Result<usize, WireError> {
    let scope = value
        .scope
        .domain
        .len()
        .checked_add(value.scope.partition.len())
        .ok_or(WireError::Capacity)?;
    if scope == 0
        || value.scope.domain.is_empty()
        || value.scope.partition.is_empty()
        || scope > limits.max_scope_bytes
        || value.scope.domain.len() > usize::from(u16::MAX)
        || value.scope.partition.len() > usize::from(u16::MAX)
        || value.donor.incarnation.0 == 0
        || value.follower.incarnation.0 == 0
        || value.donor.session == 0
        || value.follower.session == 0
        || value.donor.attempt == 0
        || value.follower.attempt == 0
        || value.parent.incarnation.0 == 0
        || value.child.incarnation.0 == 0
        || value.parent.session == 0
        || value.child.session == 0
        || value.parent.generation == 0
        || value.child.generation == 0
        || value.parent.token == 0
        || value.child.token == 0
    {
        return Err(WireError::Invalid);
    }
    if value.parent.incarnation != value.follower.incarnation
        || value.child.incarnation != value.follower.incarnation
        || value.parent.session != value.follower.session
        || value.child.session != value.follower.session
        || value.child.generation != value.parent.generation
    {
        return Err(WireError::Invalid);
    }
    let donor = identity_size(&value.donor, limits)?;
    let follower = identity_size(&value.follower, limits)?;
    scope
        .checked_add(donor)
        .and_then(|size| size.checked_add(follower))
        .ok_or(WireError::Capacity)
}

/// Encodes one exact frame after validating every declared byte bound.
///
/// # Errors
/// Rejects invalid identities, inconsistent operation binding, or a cap.
pub fn encode(value: &Envelope, limits: WireLimits) -> Result<Vec<u8>, WireError> {
    if !limits.valid() {
        return Err(WireError::Capacity);
    }
    let variable = validate_correlation(&value.correlation, limits)?;
    let payload_bytes = match &value.message {
        Message::Request(bytes) | Message::Reply(bytes) => bytes.len(),
        Message::Refused(_) => 1,
        Message::Terminator { .. } => 12,
    };
    if payload_bytes > limits.max_payload_bytes {
        return Err(WireError::Capacity);
    }
    let payload_len = u32::try_from(payload_bytes).map_err(|_| WireError::Capacity)?;
    let length = FIXED_BYTES
        .checked_add(variable)
        .and_then(|size| size.checked_add(payload_bytes))
        .ok_or(WireError::Capacity)?;
    if length > limits.max_frame_bytes {
        return Err(WireError::Capacity);
    }
    let mut out = Vec::new();
    out.try_reserve_exact(length)
        .map_err(|_| WireError::Capacity)?;
    out.extend_from_slice(&MAGIC);
    out.push(VERSION);
    out.push(value.exchange.code());
    out.push(match &value.message {
        Message::Request(_) => 1,
        Message::Reply(_) => 2,
        Message::Refused(_) => 3,
        Message::Terminator { .. } => 4,
    });
    out.push(0);
    write_str(&mut out, &value.correlation.scope.domain)?;
    write_str(&mut out, &value.correlation.scope.partition)?;
    write_identity(&mut out, &value.correlation.donor)?;
    write_identity(&mut out, &value.correlation.follower)?;
    write_operation(&mut out, value.correlation.parent);
    write_operation(&mut out, value.correlation.child);
    out.extend_from_slice(&payload_len.to_be_bytes());
    match &value.message {
        Message::Request(bytes) | Message::Reply(bytes) => out.extend_from_slice(bytes),
        Message::Refused(reason) => out.push(reason.code()),
        Message::Terminator { frames, bytes } => {
            out.extend_from_slice(&frames.to_be_bytes());
            out.extend_from_slice(&bytes.to_be_bytes());
        }
    }
    debug_assert_eq!(out.len(), length);
    Ok(out)
}

fn write_str(out: &mut Vec<u8>, value: &str) -> Result<(), WireError> {
    let length = u16::try_from(value.len()).map_err(|_| WireError::Capacity)?;
    out.extend_from_slice(&length.to_be_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn write_identity(out: &mut Vec<u8>, value: &ClaimIdentity) -> Result<(), WireError> {
    write_str(out, value.node.as_str())?;
    out.extend_from_slice(&value.incarnation.0.to_be_bytes());
    out.extend_from_slice(&value.session.to_be_bytes());
    out.extend_from_slice(&value.attempt.to_be_bytes());
    Ok(())
}

fn write_operation(out: &mut Vec<u8>, value: BootstrapOperation) {
    out.extend_from_slice(&value.incarnation.0.to_be_bytes());
    out.extend_from_slice(&value.session.to_be_bytes());
    out.extend_from_slice(&value.generation.to_be_bytes());
    out.extend_from_slice(&value.token.to_be_bytes());
}

struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, length: usize) -> Result<&'a [u8], WireError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(WireError::Invalid)?;
        let result = self
            .bytes
            .get(self.position..end)
            .ok_or(WireError::Invalid)?;
        self.position = end;
        Ok(result)
    }

    fn number<const N: usize>(&mut self) -> Result<[u8; N], WireError> {
        self.take(N)?.try_into().map_err(|_| WireError::Invalid)
    }

    fn string(&mut self, cap: usize) -> Result<String, WireError> {
        let length = usize::from(u16::from_be_bytes(self.number()?));
        if length == 0 || length > cap {
            return Err(WireError::Capacity);
        }
        let text = std::str::from_utf8(self.take(length)?).map_err(|_| WireError::Invalid)?;
        let mut owned = String::new();
        owned
            .try_reserve_exact(length)
            .map_err(|_| WireError::Capacity)?;
        owned.push_str(text);
        Ok(owned)
    }

    fn identity(&mut self, cap: usize) -> Result<ClaimIdentity, WireError> {
        Ok(ClaimIdentity {
            node: NodeId::new(self.string(cap)?),
            incarnation: BootId(u128::from_be_bytes(self.number()?)),
            session: u64::from_be_bytes(self.number()?),
            attempt: u64::from_be_bytes(self.number()?),
        })
    }

    fn operation(&mut self) -> Result<BootstrapOperation, WireError> {
        Ok(BootstrapOperation {
            incarnation: BootId(u128::from_be_bytes(self.number()?)),
            session: u64::from_be_bytes(self.number()?),
            generation: u64::from_be_bytes(self.number()?),
            token: u64::from_be_bytes(self.number()?),
        })
    }
}

/// Decodes one bounded frame, rejecting trailing bytes and inconsistent IDs.
///
/// # Errors
/// Rejects malformed, unsupported, excessive, or inconsistent input.
pub fn decode(bytes: &[u8], limits: WireLimits) -> Result<Envelope, WireError> {
    if !limits.valid() || bytes.len() > limits.max_frame_bytes || bytes.len() < FIXED_BYTES {
        return Err(WireError::Capacity);
    }
    let mut reader = Reader { bytes, position: 0 };
    if reader.number::<4>()? != MAGIC || reader.number::<1>()?[0] != VERSION {
        return Err(WireError::Invalid);
    }
    let exchange = ExchangeKind::from_code(reader.number::<1>()?[0])?;
    let kind = reader.number::<1>()?[0];
    if reader.number::<1>()?[0] != 0 {
        return Err(WireError::Invalid);
    }
    let domain = reader.string(limits.max_scope_bytes)?;
    let partition = reader.string(limits.max_scope_bytes)?;
    let donor = reader.identity(limits.max_node_bytes)?;
    let follower = reader.identity(limits.max_node_bytes)?;
    let parent = reader.operation()?;
    let child = reader.operation()?;
    let correlation = Correlation {
        scope: BootstrapScope { domain, partition },
        donor,
        follower,
        parent,
        child,
    };
    validate_correlation(&correlation, limits)?;
    let payload_bytes =
        usize::try_from(u32::from_be_bytes(reader.number()?)).map_err(|_| WireError::Capacity)?;
    if payload_bytes > limits.max_payload_bytes {
        return Err(WireError::Capacity);
    }
    let payload = reader.take(payload_bytes)?;
    if reader.position != bytes.len() {
        return Err(WireError::Invalid);
    }
    let message = match kind {
        1 | 2 => {
            let mut owned = Vec::new();
            owned
                .try_reserve_exact(payload.len())
                .map_err(|_| WireError::Capacity)?;
            owned.extend_from_slice(payload);
            if kind == 1 {
                Message::Request(owned)
            } else {
                Message::Reply(owned)
            }
        }
        3 if payload.len() == 1 => Message::Refused(Refusal::from_code(payload[0])?),
        4 if payload.len() == 12 => Message::Terminator {
            frames: u32::from_be_bytes(payload[..4].try_into().map_err(|_| WireError::Invalid)?),
            bytes: u64::from_be_bytes(payload[4..].try_into().map_err(|_| WireError::Invalid)?),
        },
        _ => return Err(WireError::Invalid),
    };
    Ok(Envelope {
        exchange,
        correlation,
        message,
    })
}

/// Streaming reply check; a terminal record alone is insufficient until EOF.
#[derive(Clone, Debug)]
pub struct ReplyTracker {
    correlation: Correlation,
    exchange: ExchangeKind,
    max_frames: u32,
    max_bytes: u64,
    frames: u32,
    bytes: u64,
    refusal: Option<Refusal>,
    terminated: bool,
}

impl ReplyTracker {
    /// Pins the exact request and finite reply shape before reading frames.
    ///
    /// # Errors
    /// Rejects a non-request frame or invalid finite response bounds.
    pub fn new(request: &Envelope, max_frames: u32, max_bytes: u64) -> Result<Self, WireError> {
        if !matches!(&request.message, Message::Request(_)) || max_frames == 0 || max_bytes == 0 {
            return Err(WireError::Invalid);
        }
        Ok(Self {
            correlation: request.correlation.clone(),
            exchange: request.exchange,
            max_frames,
            max_bytes,
            frames: 0,
            bytes: 0,
            refusal: None,
            terminated: false,
        })
    }

    /// Accounts one decoded frame before any payload is applied to a stage.
    ///
    /// # Errors
    /// Rejects stale correlation, duplicate terminals, invalid order, or caps.
    pub fn accept(&mut self, frame: &Envelope) -> Result<(), WireError> {
        if self.terminated
            || frame.exchange != self.exchange
            || frame.correlation != self.correlation
        {
            return Err(WireError::Invalid);
        }
        match &frame.message {
            Message::Reply(payload) if self.refusal.is_none() => {
                let next_frames = self.frames.checked_add(1).ok_or(WireError::Capacity)?;
                let next_bytes = self
                    .bytes
                    .checked_add(u64::try_from(payload.len()).map_err(|_| WireError::Capacity)?)
                    .ok_or(WireError::Capacity)?;
                if next_frames > self.max_frames || next_bytes > self.max_bytes {
                    return Err(WireError::Capacity);
                }
                self.frames = next_frames;
                self.bytes = next_bytes;
                Ok(())
            }
            Message::Refused(reason) if self.frames == 0 && self.refusal.is_none() => {
                self.refusal = Some(*reason);
                Ok(())
            }
            Message::Terminator { frames, bytes }
                if *frames == self.frames
                    && *bytes == self.bytes
                    && (self.frames > 0 || self.refusal.is_some()) =>
            {
                self.terminated = true;
                Ok(())
            }
            _ => Err(WireError::Invalid),
        }
    }

    /// Succeeds only after a matching in-band terminator and clean EOF.
    ///
    /// # Errors
    /// EOF without the terminator is truncation.
    pub fn finish_eof(self) -> Result<Option<Refusal>, WireError> {
        if self.terminated {
            Ok(self.refusal)
        } else {
            Err(WireError::Invalid)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> WireLimits {
        WireLimits {
            max_frame_bytes: 512,
            max_scope_bytes: 64,
            max_node_bytes: 32,
            max_payload_bytes: 128,
        }
    }

    fn fixture(message: Message) -> Envelope {
        let follower = ClaimIdentity {
            node: NodeId::from("follower"),
            incarnation: BootId(7),
            session: 8,
            attempt: 9,
        };
        Envelope {
            exchange: ExchangeKind::Batch,
            correlation: Correlation {
                scope: BootstrapScope {
                    domain: "origin".into(),
                    partition: "bucket".into(),
                },
                donor: ClaimIdentity {
                    node: NodeId::from("donor"),
                    incarnation: BootId(11),
                    session: 12,
                    attempt: 13,
                },
                follower,
                parent: BootstrapOperation {
                    incarnation: BootId(7),
                    session: 8,
                    generation: 14,
                    token: 15,
                },
                child: BootstrapOperation {
                    incarnation: BootId(7),
                    session: 8,
                    generation: 14,
                    token: 16,
                },
            },
            message,
        }
    }

    #[test]
    fn each_frame_kind_round_trips_exact_identity_and_counts() {
        for message in [
            Message::Request(vec![0, 1, 2]),
            Message::Reply(vec![3, 4]),
            Message::Refused(Refusal::Capacity),
            Message::Terminator {
                frames: 1,
                bytes: 2,
            },
        ] {
            let original = fixture(message);
            let encoded = encode(&original, limits()).unwrap();
            assert_eq!(
                encoded.len(),
                FIXED_BYTES
                    + 6
                    + 6
                    + 5
                    + 8
                    + match &original.message {
                        Message::Request(bytes) | Message::Reply(bytes) => bytes.len(),
                        Message::Refused(_) => 1,
                        Message::Terminator { .. } => 12,
                    }
            );
            assert_eq!(decode(&encoded, limits()).unwrap(), original);
        }
    }

    #[test]
    fn bad_version_reserved_byte_and_trailing_bytes_fail_closed() {
        let mut frame = encode(&fixture(Message::Request(vec![1])), limits()).unwrap();
        frame[4] = VERSION + 1;
        assert_eq!(decode(&frame, limits()), Err(WireError::Invalid));
        frame[4] = VERSION;
        frame[7] = 1;
        assert_eq!(decode(&frame, limits()), Err(WireError::Invalid));
        frame[7] = 0;
        frame.push(0);
        assert_eq!(decode(&frame, limits()), Err(WireError::Invalid));
    }

    #[test]
    fn mismatched_follower_operation_and_caps_fail_before_encoding() {
        let mut frame = fixture(Message::Reply(vec![1, 2, 3]));
        frame.correlation.child.session += 1;
        assert_eq!(encode(&frame, limits()), Err(WireError::Invalid));
        frame.correlation.child.session -= 1;
        let tight = WireLimits {
            max_frame_bytes: FIXED_BYTES,
            ..limits()
        };
        assert_eq!(encode(&frame, tight), Err(WireError::Capacity));
        let encoded = encode(&frame, limits()).unwrap();
        assert_eq!(decode(&encoded, tight), Err(WireError::Capacity));
    }

    #[test]
    fn reply_requires_exact_terminator_and_eof_without_trailing_frame() {
        let request = fixture(Message::Request(vec![1]));
        let reply = fixture(Message::Reply(vec![2, 3]));
        let terminal = fixture(Message::Terminator {
            frames: 1,
            bytes: 2,
        });
        let mut tracker = ReplyTracker::new(&request, 1, 2).unwrap();
        assert_eq!(tracker.clone().finish_eof(), Err(WireError::Invalid));
        tracker.accept(&reply).unwrap();
        assert_eq!(tracker.clone().finish_eof(), Err(WireError::Invalid));
        tracker.accept(&terminal).unwrap();
        assert_eq!(tracker.accept(&terminal), Err(WireError::Invalid));
        assert_eq!(tracker.finish_eof(), Ok(None));
    }

    #[test]
    fn reply_rejects_wrong_correlation_counts_and_over_budget_payload() {
        let request = fixture(Message::Request(vec![]));
        let mut tracker = ReplyTracker::new(&request, 1, 2).unwrap();
        let mut wrong = fixture(Message::Reply(vec![1]));
        wrong.correlation.child.token += 1;
        assert_eq!(tracker.accept(&wrong), Err(WireError::Invalid));
        assert_eq!(
            tracker.accept(&fixture(Message::Reply(vec![1, 2, 3]))),
            Err(WireError::Capacity)
        );
        tracker
            .accept(&fixture(Message::Reply(vec![1, 2])))
            .unwrap();
        assert_eq!(
            tracker.accept(&fixture(Message::Terminator {
                frames: 1,
                bytes: 1,
            })),
            Err(WireError::Invalid)
        );
        assert_eq!(
            tracker.accept(&fixture(Message::Refused(Refusal::Stale))),
            Err(WireError::Invalid)
        );
    }

    #[test]
    fn refusal_needs_matching_zero_count_terminal() {
        let request = fixture(Message::Request(vec![]));
        let mut tracker = ReplyTracker::new(&request, 1, 1).unwrap();
        tracker
            .accept(&fixture(Message::Refused(Refusal::Expired)))
            .unwrap();
        assert_eq!(
            tracker.accept(&fixture(Message::Terminator {
                frames: 1,
                bytes: 0,
            })),
            Err(WireError::Invalid)
        );
        tracker
            .accept(&fixture(Message::Terminator {
                frames: 0,
                bytes: 0,
            }))
            .unwrap();
        assert_eq!(tracker.finish_eof(), Ok(Some(Refusal::Expired)));
    }
}
