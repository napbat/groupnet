//! Exact durable sink effects and conditional source acknowledgement receipts.

use super::{
    Cursor, RegisterReceipt, SubscriberKey, SubscriptionEpoch, SubscriptionError,
    SubscriptionLimits,
};
use crate::replication::{Batch, Operation, ProofId};

/// Durable sink transaction for one source-contiguous whole batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DurableDeliveryReceipt {
    /// Exact core-issued application operation.
    pub operation: Operation,
    /// Stable subscriber key whose sink transaction committed.
    pub key: SubscriberKey,
    /// Source-registered sink fence, including monotonic ordinal.
    pub epoch: SubscriptionEpoch,
    /// Prior durable sink cursor used as the transaction's conditional guard.
    pub previous_sink: Cursor,
    /// Exact source batch upper cursor whose effects are durably recorded.
    pub through: Cursor,
    /// Durable sink cursor after the transaction, possibly already ahead.
    pub sink_cursor: Cursor,
    /// Sink-persisted stable ID for the subsequent conditional source ack.
    pub ack_request_id: Vec<u8>,
    /// Effects and cursor are atomically recoverable under the epoch fence.
    pub durable: bool,
}

impl DurableDeliveryReceipt {
    /// Checks exact sink-transaction identity and finite metadata. Cursor
    /// ordering requires separate source-proof-bound comparisons in the event.
    ///
    /// # Errors
    /// Returns a typed binding, bounds, or identity error on contradiction.
    pub fn validate_against(
        &self,
        operation: Operation,
        registration: &RegisterReceipt,
        previous_sink: &Cursor,
        batch: &Batch,
        max_cursor_bytes: usize,
        limits: SubscriptionLimits,
    ) -> Result<(), SubscriptionError> {
        if !self.durable
            || self.operation != operation
            || self.key != registration.key
            || self.epoch != registration.epoch
            || self.previous_sink != *previous_sink
            || self.through != batch.coverage.through
        {
            return Err(SubscriptionError::Binding);
        }
        if self.ack_request_id.is_empty() || self.ack_request_id.len() > limits.max_request_bytes {
            return Err(SubscriptionError::Bounds);
        }
        self.sink_cursor
            .validate(&registration.key.scope, max_cursor_bytes)
            .map_err(SubscriptionError::Identity)
    }
}

/// Source-native conditional durable ack, issued only after sink durability.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitSubscriberAck {
    /// Exact stable subscriber and protected lineage.
    pub key: SubscriberKey,
    /// Source-registered epoch and strictly monotonic sink fence ordinal.
    pub epoch: SubscriptionEpoch,
    /// Old source-protected cursor; source must compare this atomically.
    pub previous: Cursor,
    /// New protected cursor proven durably applied in the sink.
    pub through: Cursor,
    /// Source proof under which this bounded batch was scanned.
    pub proof: ProofId,
    /// Sink-persisted stable idempotency ID; retries/readback reuse it.
    pub request_id: Vec<u8>,
}

/// Source-certified durable result for one exact ack request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriberAckReceipt {
    /// Exact conditional ack request durably accepted by the source.
    pub request: CommitSubscriberAck,
    /// Source confirms this ack and its retention boundary are durable.
    pub durable: bool,
}

impl SubscriberAckReceipt {
    /// Checks exact request binding and a durable source acceptance.
    ///
    /// # Errors
    /// Returns a binding error for an unrelated or non-durable response.
    pub fn validate_against(&self, request: &CommitSubscriberAck) -> Result<(), SubscriptionError> {
        if self.durable && self.request == *request {
            Ok(())
        } else {
            Err(SubscriptionError::Binding)
        }
    }
}
