//! Sans-IO application message identities, receipt transitions and bounded codec.
//!
//! These acknowledgements are queue/application outcomes, not durable commits.

use std::{fmt, str::Utf8Error};

use groupnet_core::{GroupId, NodeId};
use groupnet_transport::NodeIdError;
use zerocopy::byteorder::network_endian::U64;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

/// Default configured opaque application payload bound.
pub const DEFAULT_MAX_MESSAGE_BYTES: usize = 60_000;
/// Default configured acknowledged retry window in milliseconds.
pub const DEFAULT_MAX_TIMEOUT_MS: u64 = 30_000;
/// Default terminal duplicate retention, longer than the default retry window.
pub const DEFAULT_DEDUP_RETENTION_MS: u64 = 60_000;

const MAGIC: [u8; 4] = *b"GNA2";

#[derive(Clone, Copy, Debug, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct Header {
    magic: [u8; 4],
    kind: u8,
    id: [u8; 16],
}

#[derive(Clone, Copy, Debug, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct DataMetadata {
    delivery: u8,
    grouped: u8,
    retry_horizon_ms: U64,
}

#[derive(Clone, Copy, Debug, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct AckStatus {
    outcome: u8,
}

/// Process nonce plus monotonically allocated sequence number.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct MessageId(
    #[doc = "Nonce and counter encoded as sixteen network-order bytes."] pub [u8; 16],
);

/// Requested receiver acknowledgement boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Delivery {
    /// Local dispatch only; no receiver acknowledgement or retry.
    BestEffort,
    /// Receiver reserved its application queue.
    Delivered,
    /// Application explicitly completed the frame successfully.
    Applied,
}

/// Portable rejection reasons understood by all peers in the same build.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rejection {
    /// Application queue is full.
    Full,
    /// Application receiver is closed.
    Closed,
    /// Group or sender is not admitted by the runtime.
    Permission,
    /// Invalid application input.
    Invalid,
    /// Application operation was interrupted.
    Interrupted,
    /// Application failed for another reason.
    Other,
}

/// Receipt state carried in a terminal or progress acknowledgement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Receiver reserved its queue; not necessarily applied.
    Accepted,
    /// Application successfully completed the operation.
    Applied,
    /// Receiver/application rejected the operation.
    Rejected(Rejection),
}

/// Pure first-terminal-outcome receipt state machine.
#[derive(Clone, Copy, Debug)]
pub struct ReceiptState {
    delivery: Delivery,
    outcome: Option<Outcome>,
    retain_until: u64,
    retention_ms: u64,
}

impl ReceiptState {
    /// Creates a pending record with its endpoint's validated retention horizon.
    /// Pending records must not be evicted.
    #[must_use]
    pub fn new(delivery: Delivery, now_ms: u64, retention_ms: u64) -> Self {
        Self {
            delivery,
            outcome: None,
            retain_until: now_ms.saturating_add(retention_ms),
            retention_ms,
        }
    }

    /// Applies a receipt action, preserving the first terminal outcome.
    /// Returns the acknowledgement to send, if any.
    pub fn act(&mut self, action: Outcome, now_ms: u64) -> Option<Outcome> {
        if self.delivery == Delivery::BestEffort {
            return None;
        }
        self.retire(now_ms);
        self.retain_until = now_ms.saturating_add(self.retention_ms);
        if !self.terminal() {
            self.outcome = Some(action);
        }
        self.outcome
    }

    /// Replays current progress without delivering the application again.
    #[must_use]
    pub fn replay(&mut self, now_ms: u64) -> Option<Outcome> {
        self.retire(now_ms);
        self.retain_until = now_ms.saturating_add(self.retention_ms);
        self.outcome
    }

    /// True only for a terminal record past its safe retry retention window.
    #[must_use]
    pub fn expired(&self, now_ms: u64) -> bool {
        self.terminal() && now_ms >= self.retain_until
    }

    /// Retires abandoned pending work after an entire inactive retention window.
    /// Retirement records a terminal interruption before permitting later eviction;
    /// even an old held receipt cannot revive or acknowledge the abandoned operation.
    pub fn retire(&mut self, now_ms: u64) {
        if !self.terminal() && now_ms >= self.retain_until {
            self.outcome = Some(Outcome::Rejected(Rejection::Interrupted));
            self.retain_until = now_ms.saturating_add(self.retention_ms);
        }
    }

    /// Whether this receipt has reached its requested terminal boundary.
    #[must_use]
    pub fn terminal(&self) -> bool {
        matches!(self.outcome, Some(Outcome::Applied | Outcome::Rejected(_)))
            || (self.delivery == Delivery::Delivered && self.outcome == Some(Outcome::Accepted))
    }

    /// The millisecond instant at which this record next retires (pending) or
    /// expires (terminal); refreshed by every action and replay.
    #[must_use]
    pub fn retain_until(&self) -> u64 {
        self.retain_until
    }
}

/// Returns whether a sender may resolve success/error for a matched receipt.
#[must_use]
pub fn completes(delivery: Delivery, outcome: Outcome) -> bool {
    delivery == Delivery::Delivered || !matches!(outcome, Outcome::Accepted)
}

/// A strictly decoded application packet, borrowing its opaque payload.
#[derive(Debug, PartialEq, Eq)]
pub enum Packet<'a> {
    /// Application data, sharing one identity across retries.
    Data {
        /// Application identity.
        id: MessageId,
        /// Receiver acknowledgement boundary.
        delivery: Delivery,
        /// Finite sender retry horizon, rounded up to milliseconds; zero for best effort.
        retry_horizon_ms: u64,
        /// Optional group destination.
        group: Option<GroupId>,
        /// Opaque data; never interpreted by coordination.
        payload: &'a [u8],
    },
    /// Receiver progress or terminal outcome.
    Ack {
        /// Application identity being acknowledged.
        id: MessageId,
        /// Queue/application outcome.
        outcome: Outcome,
    },
}

/// Why bytes are not a well-formed application packet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecodeError {
    /// The packet ends before a complete header, field or group name.
    Truncated,
    /// The packet does not start with this codec's magic and version.
    UnknownVersion,
    /// The packet kind is neither data nor acknowledgement.
    UnknownKind(u8),
    /// The data packet's delivery boundary is unknown.
    UnknownDelivery(u8),
    /// The retry horizon is zero for an acknowledged delivery or nonzero for
    /// best effort.
    RetryHorizonMismatch,
    /// The group-presence flag is neither 0 nor 1.
    UnknownGroupFlag(u8),
    /// A present group name is empty.
    EmptyGroup,
    /// A present group name is not UTF-8.
    GroupNotUtf8(Utf8Error),
    /// The payload exceeds the `u32` wire length.
    PayloadTooLarge,
    /// The acknowledgement outcome is unknown.
    UnknownOutcome(u8),
    /// The rejection reason is unknown.
    UnknownRejection(u8),
    /// An acknowledgement carries bytes after its outcome.
    TrailingBytes,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => f.write_str("application packet is truncated"),
            Self::UnknownVersion => f.write_str("application packet has an unknown version"),
            Self::UnknownKind(kind) => write!(f, "application packet kind {kind} is unknown"),
            Self::UnknownDelivery(delivery) => {
                write!(f, "application delivery {delivery} is unknown")
            }
            Self::RetryHorizonMismatch => {
                f.write_str("application retry horizon does not match its delivery")
            }
            Self::UnknownGroupFlag(flag) => write!(f, "application group flag {flag} is unknown"),
            Self::EmptyGroup => f.write_str("application group name is empty"),
            Self::GroupNotUtf8(_) => f.write_str("application group name is not UTF-8"),
            Self::PayloadTooLarge => f.write_str("application payload exceeds the u32 wire length"),
            Self::UnknownOutcome(outcome) => {
                write!(f, "application outcome {outcome} is unknown")
            }
            Self::UnknownRejection(reason) => {
                write!(f, "application rejection {reason} is unknown")
            }
            Self::TrailingBytes => f.write_str("application acknowledgement has trailing bytes"),
        }
    }
}

impl std::error::Error for DecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::GroupNotUtf8(error) => Some(error),
            _ => None,
        }
    }
}

impl From<DecodeError> for std::io::Error {
    fn from(error: DecodeError) -> Self {
        Self::new(std::io::ErrorKind::InvalidData, error)
    }
}

fn take<'a>(body: &mut &'a [u8], count: usize) -> Result<&'a [u8], DecodeError> {
    let (part, rest) = body.split_at_checked(count).ok_or(DecodeError::Truncated)?;
    *body = rest;
    Ok(part)
}

fn byte(body: &mut &[u8]) -> Result<u8, DecodeError> {
    Ok(take(body, 1)?[0])
}

/// Rejects unknown versions, kinds, statuses, truncated IDs and trailing ACK bytes.
/// # Errors
/// Returns the [`DecodeError`] naming the first malformed field.
pub fn decode(bytes: &[u8]) -> Result<Packet<'_>, DecodeError> {
    let (header, mut body) = Header::ref_from_prefix(bytes).map_err(|_| DecodeError::Truncated)?;
    if header.magic != MAGIC {
        return Err(DecodeError::UnknownVersion);
    }
    let id = MessageId(header.id);
    match header.kind {
        0 => {
            let (metadata, rest) =
                DataMetadata::ref_from_prefix(body).map_err(|_| DecodeError::Truncated)?;
            body = rest;
            let delivery = match metadata.delivery {
                0 => Delivery::BestEffort,
                1 => Delivery::Delivered,
                2 => Delivery::Applied,
                other => return Err(DecodeError::UnknownDelivery(other)),
            };
            let retry_horizon_ms = metadata.retry_horizon_ms.get();
            if (delivery == Delivery::BestEffort) != (retry_horizon_ms == 0) {
                return Err(DecodeError::RetryHorizonMismatch);
            }
            let group = match metadata.grouped {
                0 => None,
                1 => {
                    let length = usize::from(byte(&mut body)?);
                    if length == 0 {
                        return Err(DecodeError::EmptyGroup);
                    }
                    let name = std::str::from_utf8(take(&mut body, length)?)
                        .map_err(DecodeError::GroupNotUtf8)?;
                    Some(GroupId::new(name))
                }
                other => return Err(DecodeError::UnknownGroupFlag(other)),
            };
            if u32::try_from(body.len()).is_err() {
                return Err(DecodeError::PayloadTooLarge);
            }
            Ok(Packet::Data {
                id,
                delivery,
                retry_horizon_ms,
                group,
                payload: body,
            })
        }
        1 => {
            let (status, rest) =
                AckStatus::ref_from_prefix(body).map_err(|_| DecodeError::Truncated)?;
            body = rest;
            let outcome = match status.outcome {
                0 => Outcome::Accepted,
                1 => Outcome::Applied,
                2 => Outcome::Rejected(match byte(&mut body)? {
                    0 => Rejection::Full,
                    1 => Rejection::Closed,
                    2 => Rejection::Permission,
                    3 => Rejection::Invalid,
                    4 => Rejection::Interrupted,
                    5 => Rejection::Other,
                    other => return Err(DecodeError::UnknownRejection(other)),
                }),
                other => return Err(DecodeError::UnknownOutcome(other)),
            };
            if !body.is_empty() {
                return Err(DecodeError::TrailingBytes);
            }
            Ok(Packet::Ack { id, outcome })
        }
        other => Err(DecodeError::UnknownKind(other)),
    }
}

/// The fields of one application data packet, encoded by [`data`].
#[derive(Clone, Copy, Debug)]
pub struct DataPacket<'a> {
    /// Application identity, shared by every retry.
    pub id: MessageId,
    /// Receiver acknowledgement boundary.
    pub delivery: Delivery,
    /// The sender's complete retry deadline rounded up to milliseconds, or zero
    /// for best effort. Every retry must preserve it.
    pub retry_horizon_ms: u64,
    /// Optional group destination; its name is 1 to 255 bytes.
    pub group: Option<&'a GroupId>,
    /// Opaque data, at most `u32::MAX` bytes.
    pub payload: &'a [u8],
}

/// Why a [`DataPacket`] cannot be encoded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EncodeError {
    /// `payload` exceeds the `u32` wire length.
    PayloadTooLarge,
    /// `group` names an empty or longer-than-255-byte group.
    GroupNameLength,
    /// `retry_horizon_ms` is zero for an acknowledged delivery or nonzero for
    /// best effort.
    RetryHorizonMismatch,
}

impl fmt::Display for EncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::PayloadTooLarge => "application payload exceeds the u32 wire length",
            Self::GroupNameLength => "application group name must be 1 to 255 bytes",
            Self::RetryHorizonMismatch => {
                "application retry horizon must be zero exactly for best effort"
            }
        })
    }
}

impl std::error::Error for EncodeError {}

impl From<EncodeError> for std::io::Error {
    fn from(error: EncodeError) -> Self {
        Self::new(std::io::ErrorKind::InvalidInput, error)
    }
}

/// Encodes application data after validating group, buffer and retry-horizon bounds.
///
/// # Errors
/// Returns the [`EncodeError`] naming an unrepresentable payload, group or horizon.
pub fn data(packet: &DataPacket<'_>) -> Result<Vec<u8>, EncodeError> {
    let frame = DataFrame::new(*packet)?;
    let mut bytes = Vec::with_capacity(frame.len());
    frame.encode(|part| bytes.extend_from_slice(part));
    Ok(bytes)
}

/// One application data packet, validated once and re-encoded for each retry
/// without repeating its group, buffer and retry-horizon checks.
#[derive(Debug)]
pub(crate) struct DataFrame<'a> {
    header: Header,
    metadata: DataMetadata,
    /// Validated nonempty group name and its one-byte wire length.
    group: Option<(u8, &'a str)>,
    payload: &'a [u8],
    len: usize,
}

impl<'a> DataFrame<'a> {
    /// Validates group, buffer and retry-horizon representability.
    pub(crate) fn new(packet: DataPacket<'a>) -> Result<Self, EncodeError> {
        let DataPacket {
            id,
            delivery,
            retry_horizon_ms,
            group,
            payload,
        } = packet;
        if u32::try_from(payload.len()).is_err() {
            return Err(EncodeError::PayloadTooLarge);
        }
        if (delivery == Delivery::BestEffort) != (retry_horizon_ms == 0) {
            return Err(EncodeError::RetryHorizonMismatch);
        }
        let group = group
            .map(|group| {
                let name = group.as_str();
                match u8::try_from(name.len()) {
                    Ok(length) if length != 0 => Ok((length, name)),
                    _ => Err(EncodeError::GroupNameLength),
                }
            })
            .transpose()?;
        let len = (size_of::<Header>() + size_of::<DataMetadata>())
            .checked_add(group.map_or(0, |(length, _)| 1 + usize::from(length)))
            .and_then(|length| length.checked_add(payload.len()))
            .ok_or(EncodeError::PayloadTooLarge)?;
        Ok(Self {
            header: Header {
                magic: MAGIC,
                kind: 0,
                id: id.0,
            },
            metadata: DataMetadata {
                delivery: match delivery {
                    Delivery::BestEffort => 0,
                    Delivery::Delivered => 1,
                    Delivery::Applied => 2,
                },
                grouped: u8::from(group.is_some()),
                retry_horizon_ms: U64::new(retry_horizon_ms),
            },
            group,
            payload,
            len,
        })
    }

    /// Exact encoded length, checked once at construction.
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// Appends the packet's parts in wire order.
    pub(crate) fn encode(&self, mut append: impl FnMut(&[u8])) {
        append(self.header.as_bytes());
        append(self.metadata.as_bytes());
        if let Some((length, name)) = self.group {
            append(&[length]);
            append(name.as_bytes());
        }
        append(self.payload);
    }
}

/// Encodes a bounded progress/terminal acknowledgement.
#[must_use]
pub fn ack(id: MessageId, outcome: Outcome) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(ack_len(outcome));
    encode_ack(id, outcome, |part| bytes.extend_from_slice(part));
    bytes
}

pub(crate) fn ack_len(outcome: Outcome) -> usize {
    size_of::<Header>()
        + size_of::<AckStatus>()
        + usize::from(matches!(outcome, Outcome::Rejected(_)))
}

pub(crate) fn encode_ack(id: MessageId, outcome: Outcome, mut append: impl FnMut(&[u8])) {
    append(
        Header {
            magic: MAGIC,
            kind: 1,
            id: id.0,
        }
        .as_bytes(),
    );
    append(
        AckStatus {
            outcome: match outcome {
                Outcome::Accepted => 0,
                Outcome::Applied => 1,
                Outcome::Rejected(_) => 2,
            },
        }
        .as_bytes(),
    );
    if let Outcome::Rejected(reason) = outcome {
        append(&[match reason {
            Rejection::Full => 0,
            Rejection::Closed => 1,
            Rejection::Permission => 2,
            Rejection::Invalid => 3,
            Rejection::Interrupted => 4,
            Rejection::Other => 5,
        }]);
    }
}

/// Validates a destination without imposing application authorization.
///
/// # Errors
/// Returns the [`NodeIdError`] for an empty identity or one longer than
/// [`MAX_NODE_ID_BYTES`](groupnet_transport::MAX_NODE_ID_BYTES).
pub fn validate_destination(node: &NodeId) -> Result<(), NodeIdError> {
    groupnet_transport::validate_node_id(node, groupnet_transport::MAX_NODE_ID_BYTES)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn receipts_distinguish_queue_application_and_first_terminal_failure() {
        let mut applied = ReceiptState::new(Delivery::Applied, 0, DEFAULT_DEDUP_RETENTION_MS);
        assert_eq!(applied.act(Outcome::Accepted, 10), Some(Outcome::Accepted));
        assert!(!applied.terminal());
        assert!(!applied.expired(u64::MAX));
        assert!(!completes(Delivery::Applied, Outcome::Accepted));
        assert_eq!(
            applied.act(Outcome::Rejected(Rejection::Other), 20),
            Some(Outcome::Rejected(Rejection::Other))
        );
        assert_eq!(
            applied.act(Outcome::Applied, 30),
            Some(Outcome::Rejected(Rejection::Other))
        );
        assert!(!applied.expired(60_029));
        assert!(applied.expired(60_030));
        let mut delivered = ReceiptState::new(Delivery::Delivered, 0, DEFAULT_DEDUP_RETENTION_MS);
        delivered.act(Outcome::Accepted, 1);
        assert_eq!(
            delivered.act(Outcome::Rejected(Rejection::Other), 2),
            Some(Outcome::Accepted)
        );

        let mut abandoned = ReceiptState::new(Delivery::Applied, 0, DEFAULT_DEDUP_RETENTION_MS);
        abandoned.retire(DEFAULT_DEDUP_RETENTION_MS);
        assert_eq!(
            abandoned.act(Outcome::Applied, 60_001),
            Some(Outcome::Rejected(Rejection::Interrupted))
        );
        assert!(!abandoned.expired(120_000));
        assert!(abandoned.expired(120_001));
    }

    #[test]
    fn duplicate_retention_refreshes_and_pending_records_never_expire() {
        let mut receipt = ReceiptState::new(Delivery::Applied, 0, DEFAULT_DEDUP_RETENTION_MS);
        assert_eq!(receipt.replay(59_999), None);
        assert!(!receipt.expired(u64::MAX));
        receipt.act(Outcome::Applied, 90_000);
        assert_eq!(receipt.retain_until(), 150_000);
        assert_eq!(receipt.replay(119_999), Some(Outcome::Applied));
        assert_eq!(receipt.retain_until(), 179_999);
        assert!(!receipt.expired(179_998));
        assert!(receipt.expired(179_999));
    }

    fn packet<'a>(
        delivery: Delivery,
        retry_horizon_ms: u64,
        group: Option<&'a GroupId>,
        payload: &'a [u8],
    ) -> DataPacket<'a> {
        DataPacket {
            id: MessageId([9; 16]),
            delivery,
            retry_horizon_ms,
            group,
            payload,
        }
    }

    #[test]
    fn codec_preserves_binary_data_and_rejects_malformed_packets() {
        let id = MessageId([9; 16]);
        let group = GroupId::new("unicode-λ");
        let payload = [0, 255, 0, 42];
        let bytes = data(&packet(
            Delivery::Applied,
            DEFAULT_MAX_TIMEOUT_MS,
            Some(&group),
            &payload,
        ))
        .unwrap();
        assert_eq!(
            decode(&bytes),
            Ok(Packet::Data {
                id,
                delivery: Delivery::Applied,
                retry_horizon_ms: DEFAULT_MAX_TIMEOUT_MS,
                group: Some(group),
                payload: &payload
            })
        );
        for end in 0..size_of::<Header>() + size_of::<DataMetadata>() + 1 + "unicode-λ".len() {
            assert_eq!(decode(&bytes[..end]), Err(DecodeError::Truncated));
        }
        let mut bad = bytes.clone();
        bad[21] = 99;
        assert_eq!(decode(&bad), Err(DecodeError::UnknownDelivery(99)));
        bad.clone_from(&bytes);
        bad[4] = 99;
        assert_eq!(decode(&bad), Err(DecodeError::UnknownKind(99)));
        bad.clone_from(&bytes);
        bad[size_of::<Header>() + 1] = 7;
        assert_eq!(decode(&bad), Err(DecodeError::UnknownGroupFlag(7)));
        bad.clone_from(&bytes);
        bad[size_of::<Header>() + size_of::<DataMetadata>() + 1] = 0xff;
        let not_utf8 = decode(&bad).unwrap_err();
        assert!(matches!(not_utf8, DecodeError::GroupNotUtf8(_)));
        assert!(std::error::Error::source(&not_utf8).is_some());
        for outcome in [
            Outcome::Accepted,
            Outcome::Applied,
            Outcome::Rejected(Rejection::Permission),
        ] {
            let mut bytes = ack(id, outcome);
            assert_eq!(decode(&bytes), Ok(Packet::Ack { id, outcome }));
            bytes.push(0);
            assert_eq!(decode(&bytes), Err(DecodeError::TrailingBytes));
        }
        let mut unknown = ack(id, Outcome::Rejected(Rejection::Other));
        *unknown.last_mut().unwrap() = 6;
        assert_eq!(decode(&unknown), Err(DecodeError::UnknownRejection(6)));
        unknown.pop();
        *unknown.last_mut().unwrap() = 3;
        assert_eq!(decode(&unknown), Err(DecodeError::UnknownOutcome(3)));
        let extended = vec![0; DEFAULT_MAX_MESSAGE_BYTES + 1];
        let encoded = data(&packet(
            Delivery::Delivered,
            DEFAULT_MAX_TIMEOUT_MS,
            None,
            &extended,
        ))
        .unwrap();
        assert!(
            matches!(decode(&encoded), Ok(Packet::Data { payload, .. }) if payload == extended)
        );
        assert_eq!(
            data(&packet(
                Delivery::Delivered,
                DEFAULT_MAX_TIMEOUT_MS,
                Some(&GroupId::new("")),
                &[]
            )),
            Err(EncodeError::GroupNameLength)
        );
    }

    #[test]
    fn wire_bounds_and_retry_horizon_are_explicit_and_fail_closed() {
        let boundary = GroupId::new("g".repeat(usize::from(u8::MAX)));
        let bytes = data(&packet(Delivery::Applied, 1, Some(&boundary), b"body")).unwrap();
        assert!(
            matches!(decode(&bytes), Ok(Packet::Data { group: Some(group), retry_horizon_ms: 1, .. }) if group == boundary)
        );
        let oversized = GroupId::new("g".repeat(usize::from(u8::MAX) + 1));
        assert_eq!(
            data(&packet(Delivery::Applied, 1, Some(&oversized), b"")),
            Err(EncodeError::GroupNameLength)
        );
        assert_eq!(
            data(&packet(Delivery::Applied, 0, None, b"")),
            Err(EncodeError::RetryHorizonMismatch)
        );
        assert_eq!(
            data(&packet(Delivery::BestEffort, 1, None, b"")),
            Err(EncodeError::RetryHorizonMismatch)
        );
        let mut old_version = data(&packet(Delivery::BestEffort, 0, None, b"")).unwrap();
        old_version[..4].copy_from_slice(b"GNA1");
        assert_eq!(decode(&old_version), Err(DecodeError::UnknownVersion));
        let mut malformed = data(&packet(Delivery::Applied, 1, None, b"")).unwrap();
        let offset = size_of::<Header>() + 2;
        malformed[offset..offset + size_of::<U64>()].fill(0);
        assert_eq!(decode(&malformed), Err(DecodeError::RetryHorizonMismatch));
        let mut empty_group = data(&packet(Delivery::Applied, 1, Some(&boundary), b"")).unwrap();
        empty_group[size_of::<Header>() + size_of::<DataMetadata>()] = 0;
        assert_eq!(decode(&empty_group), Err(DecodeError::EmptyGroup));
        for error in [DecodeError::Truncated, DecodeError::UnknownKind(9)] {
            assert_eq!(
                std::io::Error::from(error).kind(),
                std::io::ErrorKind::InvalidData
            );
        }
    }
}
