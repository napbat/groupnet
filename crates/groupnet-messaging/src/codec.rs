//! Sans-IO application message identities, receipt transitions and bounded codec.
//!
//! These acknowledgements are queue/application outcomes, not durable commits.

use groupnet_core::{GroupId, NodeId};
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

/// A malformed or out-of-bounds packet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecodeError;

fn take<'a>(body: &mut &'a [u8], count: usize) -> Result<&'a [u8], DecodeError> {
    let (part, rest) = body.split_at_checked(count).ok_or(DecodeError)?;
    *body = rest;
    Ok(part)
}

fn byte(body: &mut &[u8]) -> Result<u8, DecodeError> {
    Ok(take(body, 1)?[0])
}

/// Rejects unknown versions, kinds, statuses, truncated IDs and trailing ACK bytes.
/// # Errors
/// Returns `DecodeError` for any malformed application packet.
pub fn decode(bytes: &[u8]) -> Result<Packet<'_>, DecodeError> {
    let (header, mut body) = Header::ref_from_prefix(bytes).map_err(|_| DecodeError)?;
    if header.magic != MAGIC {
        return Err(DecodeError);
    }
    let id = MessageId(header.id);
    match header.kind {
        0 => {
            let (metadata, rest) = DataMetadata::ref_from_prefix(body).map_err(|_| DecodeError)?;
            body = rest;
            let delivery = match metadata.delivery {
                0 => Delivery::BestEffort,
                1 => Delivery::Delivered,
                2 => Delivery::Applied,
                _ => return Err(DecodeError),
            };
            let retry_horizon_ms = metadata.retry_horizon_ms.get();
            if (delivery == Delivery::BestEffort) != (retry_horizon_ms == 0) {
                return Err(DecodeError);
            }
            let group = match metadata.grouped {
                0 => None,
                1 => {
                    let length = usize::from(byte(&mut body)?);
                    if length == 0 {
                        return Err(DecodeError);
                    }
                    let name =
                        std::str::from_utf8(take(&mut body, length)?).map_err(|_| DecodeError)?;
                    Some(GroupId::new(name))
                }
                _ => return Err(DecodeError),
            };
            if u32::try_from(body.len()).is_err() {
                return Err(DecodeError);
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
            let (status, rest) = AckStatus::ref_from_prefix(body).map_err(|_| DecodeError)?;
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
                    _ => return Err(DecodeError),
                }),
                _ => return Err(DecodeError),
            };
            if !body.is_empty() {
                return Err(DecodeError);
            }
            Ok(Packet::Ack { id, outcome })
        }
        _ => Err(DecodeError),
    }
}

/// Encodes application data after validating group, buffer and retry-horizon bounds.
/// `retry_horizon_ms` is the sender's complete retry deadline rounded up to
/// milliseconds, or zero for best effort. Every retry must preserve it.
///
/// # Errors
/// Returns `DecodeError` for unrepresentable buffers/groups or an invalid horizon.
pub fn data(
    id: MessageId,
    delivery: Delivery,
    retry_horizon_ms: u64,
    group: Option<&GroupId>,
    payload: &[u8],
) -> Result<Vec<u8>, DecodeError> {
    let frame = DataFrame::new(id, delivery, retry_horizon_ms, group, payload)?;
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
    pub(crate) fn new(
        id: MessageId,
        delivery: Delivery,
        retry_horizon_ms: u64,
        group: Option<&'a GroupId>,
        payload: &'a [u8],
    ) -> Result<Self, DecodeError> {
        if u32::try_from(payload.len()).is_err()
            || (delivery == Delivery::BestEffort) != (retry_horizon_ms == 0)
        {
            return Err(DecodeError);
        }
        let group = group
            .map(|group| {
                let name = group.as_str();
                match u8::try_from(name.len()) {
                    Ok(length) if length != 0 => Ok((length, name)),
                    _ => Err(DecodeError),
                }
            })
            .transpose()?;
        let len = (size_of::<Header>() + size_of::<DataMetadata>())
            .checked_add(group.map_or(0, |(length, _)| 1 + usize::from(length)))
            .and_then(|length| length.checked_add(payload.len()))
            .ok_or(DecodeError)?;
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
#[must_use]
pub fn destination_valid(node: &NodeId) -> bool {
    !node.as_str().is_empty() && node.as_str().len() <= 255
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

    #[test]
    fn codec_preserves_binary_data_and_rejects_malformed_packets() {
        let id = MessageId([9; 16]);
        let group = GroupId::new("unicode-λ");
        let payload = [0, 255, 0, 42];
        let bytes = data(
            id,
            Delivery::Applied,
            DEFAULT_MAX_TIMEOUT_MS,
            Some(&group),
            &payload,
        )
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
            assert!(decode(&bytes[..end]).is_err());
        }
        let mut bad = bytes.clone();
        bad[21] = 99;
        assert!(decode(&bad).is_err());
        bad = bytes;
        bad[4] = 99;
        assert!(decode(&bad).is_err());
        for outcome in [
            Outcome::Accepted,
            Outcome::Applied,
            Outcome::Rejected(Rejection::Permission),
        ] {
            let mut bytes = ack(id, outcome);
            assert_eq!(decode(&bytes), Ok(Packet::Ack { id, outcome }));
            bytes.push(0);
            assert!(decode(&bytes).is_err());
        }
        let extended = vec![0; DEFAULT_MAX_MESSAGE_BYTES + 1];
        let encoded = data(
            id,
            Delivery::Delivered,
            DEFAULT_MAX_TIMEOUT_MS,
            None,
            &extended,
        )
        .unwrap();
        assert!(
            matches!(decode(&encoded), Ok(Packet::Data { payload, .. }) if payload == extended)
        );
        assert!(
            data(
                id,
                Delivery::Delivered,
                DEFAULT_MAX_TIMEOUT_MS,
                Some(&GroupId::new("")),
                &[]
            )
            .is_err()
        );
    }

    #[test]
    fn wire_bounds_and_retry_horizon_are_explicit_and_fail_closed() {
        let id = MessageId([1; 16]);
        let boundary = GroupId::new("g".repeat(usize::from(u8::MAX)));
        let bytes = data(id, Delivery::Applied, 1, Some(&boundary), b"body").unwrap();
        assert!(
            matches!(decode(&bytes), Ok(Packet::Data { group: Some(group), retry_horizon_ms: 1, .. }) if group == boundary)
        );
        let oversized = GroupId::new("g".repeat(usize::from(u8::MAX) + 1));
        assert!(data(id, Delivery::Applied, 1, Some(&oversized), b"").is_err());
        assert!(data(id, Delivery::Applied, 0, None, b"").is_err());
        assert!(data(id, Delivery::BestEffort, 1, None, b"").is_err());
        let mut old_version = data(id, Delivery::BestEffort, 0, None, b"").unwrap();
        old_version[..4].copy_from_slice(b"GNA1");
        assert!(decode(&old_version).is_err());
        let mut malformed = data(id, Delivery::Applied, 1, None, b"").unwrap();
        let offset = size_of::<Header>() + 2;
        malformed[offset..offset + size_of::<U64>()].fill(0);
        assert!(decode(&malformed).is_err());
    }
}
