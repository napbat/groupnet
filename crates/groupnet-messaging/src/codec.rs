//! Sans-IO application message identities, receipt transitions and bounded codec.
//!
//! These acknowledgements are queue/application outcomes, not durable commits.

use groupnet_core::{GroupId, NodeId};

/// Maximum opaque application payload, below the routing envelope limit.
pub const MAX_MESSAGE_BYTES: usize = 60_000;
/// Maximum acknowledged retry window in milliseconds.
pub const MAX_TIMEOUT_MS: u64 = 30_000;
/// Minimum terminal duplicate retention, longer than the retry window.
pub const DEDUP_RETENTION_MS: u64 = 60_000;

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
}

impl ReceiptState {
    /// Creates a pending record; pending records must not be evicted.
    #[must_use]
    pub fn new(delivery: Delivery, now_ms: u64) -> Self {
        Self {
            delivery,
            outcome: None,
            retain_until: now_ms.saturating_add(DEDUP_RETENTION_MS),
        }
    }

    /// Applies a receipt action, preserving the first terminal outcome.
    /// Returns the acknowledgement to send, if any.
    pub fn act(&mut self, action: Outcome, now_ms: u64) -> Option<Outcome> {
        if self.delivery == Delivery::BestEffort {
            return None;
        }
        self.retire(now_ms);
        self.retain_until = now_ms.saturating_add(DEDUP_RETENTION_MS);
        if !self.terminal() {
            self.outcome = Some(action);
        }
        self.outcome
    }

    /// Replays current progress without delivering the application again.
    #[must_use]
    pub fn replay(&mut self, now_ms: u64) -> Option<Outcome> {
        self.retire(now_ms);
        self.retain_until = now_ms.saturating_add(DEDUP_RETENTION_MS);
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
            self.retain_until = now_ms.saturating_add(DEDUP_RETENTION_MS);
        }
    }

    /// Whether this receipt has reached its requested terminal boundary.
    #[must_use]
    pub fn terminal(&self) -> bool {
        matches!(self.outcome, Some(Outcome::Applied | Outcome::Rejected(_)))
            || (self.delivery == Delivery::Delivered && self.outcome == Some(Outcome::Accepted))
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
    let mut body = bytes;
    if take(&mut body, 4)? != b"GNA1" {
        return Err(DecodeError);
    }
    let kind = byte(&mut body)?;
    let id = MessageId(take(&mut body, 16)?.try_into().map_err(|_| DecodeError)?);
    match kind {
        0 => {
            let delivery = match byte(&mut body)? {
                0 => Delivery::BestEffort,
                1 => Delivery::Delivered,
                2 => Delivery::Applied,
                _ => return Err(DecodeError),
            };
            let group = match byte(&mut body)? {
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
            if body.len() > MAX_MESSAGE_BYTES {
                return Err(DecodeError);
            }
            Ok(Packet::Data {
                id,
                delivery,
                group,
                payload: body,
            })
        }
        1 => {
            let outcome = match byte(&mut body)? {
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

/// Encodes application data after validating the group and buffer bounds.
/// # Errors
/// Returns `DecodeError` for oversized buffers or empty/oversized group IDs.
pub fn data(
    id: MessageId,
    delivery: Delivery,
    group: Option<&GroupId>,
    payload: &[u8],
) -> Result<Vec<u8>, DecodeError> {
    if payload.len() > MAX_MESSAGE_BYTES
        || group.is_some_and(|g| g.as_str().is_empty() || g.as_str().len() > 255)
    {
        return Err(DecodeError);
    }
    let mut bytes = Vec::with_capacity(24 + group.map_or(0, |g| g.as_str().len()) + payload.len());
    bytes.extend_from_slice(b"GNA1\0");
    bytes.extend_from_slice(&id.0);
    bytes.push(match delivery {
        Delivery::BestEffort => 0,
        Delivery::Delivered => 1,
        Delivery::Applied => 2,
    });
    if let Some(group) = group {
        bytes.push(1);
        bytes.push(u8::try_from(group.as_str().len()).map_err(|_| DecodeError)?);
        bytes.extend_from_slice(group.as_str().as_bytes());
    } else {
        bytes.push(0);
    }
    bytes.extend_from_slice(payload);
    Ok(bytes)
}

/// Encodes a bounded progress/terminal acknowledgement.
#[must_use]
pub fn ack(id: MessageId, outcome: Outcome) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(23);
    bytes.extend_from_slice(b"GNA1\x01");
    bytes.extend_from_slice(&id.0);
    match outcome {
        Outcome::Accepted => bytes.push(0),
        Outcome::Applied => bytes.push(1),
        Outcome::Rejected(reason) => {
            bytes.push(2);
            bytes.push(match reason {
                Rejection::Full => 0,
                Rejection::Closed => 1,
                Rejection::Permission => 2,
                Rejection::Invalid => 3,
                Rejection::Interrupted => 4,
                Rejection::Other => 5,
            });
        }
    }
    bytes
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
        let mut applied = ReceiptState::new(Delivery::Applied, 0);
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
        let mut delivered = ReceiptState::new(Delivery::Delivered, 0);
        delivered.act(Outcome::Accepted, 1);
        assert_eq!(
            delivered.act(Outcome::Rejected(Rejection::Other), 2),
            Some(Outcome::Accepted)
        );

        let mut abandoned = ReceiptState::new(Delivery::Applied, 0);
        abandoned.retire(DEDUP_RETENTION_MS);
        assert_eq!(
            abandoned.act(Outcome::Applied, 60_001),
            Some(Outcome::Rejected(Rejection::Interrupted))
        );
        assert!(!abandoned.expired(120_000));
        assert!(abandoned.expired(120_001));
    }

    #[test]
    fn duplicate_retention_refreshes_and_pending_records_never_expire() {
        let mut receipt = ReceiptState::new(Delivery::Applied, 0);
        assert_eq!(receipt.replay(59_999), None);
        assert!(!receipt.expired(u64::MAX));
        receipt.act(Outcome::Applied, 90_000);
        assert_eq!(receipt.replay(119_999), Some(Outcome::Applied));
        assert!(!receipt.expired(179_998));
        assert!(receipt.expired(179_999));
    }

    #[test]
    fn codec_preserves_binary_data_and_rejects_malformed_packets() {
        let id = MessageId([9; 16]);
        let group = GroupId::new("unicode-λ");
        let payload = [0, 255, 0, 42];
        let bytes = data(id, Delivery::Applied, Some(&group), &payload).unwrap();
        assert_eq!(
            decode(&bytes),
            Ok(Packet::Data {
                id,
                delivery: Delivery::Applied,
                group: Some(group),
                payload: &payload
            })
        );
        for end in 0..24 {
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
        assert!(
            data(
                id,
                Delivery::Delivered,
                None,
                &vec![0; MAX_MESSAGE_BYTES + 1]
            )
            .is_err()
        );
        assert!(data(id, Delivery::Delivered, Some(&GroupId::new("")), &[]).is_err());
    }
}
