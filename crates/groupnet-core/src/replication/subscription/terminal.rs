//! Source-durable terminal records for finite named-event retention.

use std::num::NonZeroU64;

use super::{SubscriberKey, SubscriptionEpoch, SubscriptionError, SubscriptionLimits};
use crate::replication::Cursor;

/// Why a source durably stopped protecting one subscriber's suffix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TerminalReason {
    /// The subscriber explicitly relinquished its retained suffix.
    Unsubscribed,
    /// The source's finite byte, event, age, or lag policy expired.
    Expired,
    /// The source could no longer prove a contiguous retained suffix.
    IrrecoverableGap,
}

/// Conditional source request that ends one exact protected lineage.
///
/// A source must atomically compare `epoch` and `acknowledged` with its durable
/// registration before persisting the tombstone or releasing retention. An
/// ambiguous result is resolved by `request_id` readback before any different
/// terminal request or reset is attempted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TerminalRequest {
    /// Exact native scope and stable subscriber name.
    pub key: SubscriberKey,
    /// Source-persisted fence epoch to terminate.
    pub epoch: SubscriptionEpoch,
    /// Source-protected durable ack observed before this terminal request.
    pub acknowledged: Cursor,
    /// Stable, bounded idempotency key retained for exact source readback.
    pub request_id: Vec<u8>,
    /// Reason for the durable terminal state.
    pub reason: TerminalReason,
}

impl TerminalRequest {
    /// Validates bounded scoped identity before a source operation is issued.
    ///
    /// # Errors
    /// Rejects invalid scope, history, epoch, cursor, or request ID.
    pub fn validate(
        &self,
        max_cursor_bytes: usize,
        limits: SubscriptionLimits,
    ) -> Result<(), SubscriptionError> {
        self.key
            .scope
            .validate(max_cursor_bytes)
            .map_err(SubscriptionError::Identity)?;
        if !limits.valid()
            || self.key.subscriber.name.is_empty()
            || self.key.subscriber.name.len() > limits.max_subscriber_bytes
            || self.epoch.native.is_empty()
            || self.epoch.native.len() > limits.max_epoch_bytes
            || self.request_id.is_empty()
            || self.request_id.len() > limits.max_request_bytes
        {
            return Err(SubscriptionError::Bounds);
        }
        if self.epoch.history != self.acknowledged.history {
            return Err(SubscriptionError::History);
        }
        self.acknowledged
            .validate(&self.key.scope, max_cursor_bytes)
            .map_err(SubscriptionError::Identity)
    }
}

/// Durable source tombstone proving retention may be released for one lineage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TerminalReceipt {
    /// Exact terminal request durably accepted by the source.
    pub request: TerminalRequest,
    /// Persistent ordinal of the terminated lineage. Reset must exceed it.
    pub tombstone_ordinal: NonZeroU64,
    /// Source has durably recorded the terminal state before compaction.
    pub durable: bool,
}

impl TerminalReceipt {
    /// Checks exact idempotency binding and durable terminal authority.
    ///
    /// # Errors
    /// Rejects a foreign, non-durable, or rollback tombstone.
    pub fn validate_against(&self, request: &TerminalRequest) -> Result<(), SubscriptionError> {
        if !self.durable || self.request != *request {
            return Err(SubscriptionError::Binding);
        }
        if self.tombstone_ordinal != request.epoch.ordinal {
            return Err(SubscriptionError::FenceLost);
        }
        Ok(())
    }

    /// Checks a reset's durable predecessor and strictly newer fence ordinal.
    ///
    /// # Errors
    /// Rejects key mismatches, non-durable tombstones, or ordinal reuse.
    pub fn validate_reset(
        &self,
        key: &SubscriberKey,
        next_ordinal: NonZeroU64,
    ) -> Result<(), SubscriptionError> {
        self.validate_against(&self.request)?;
        if !self.durable || self.request.key != *key {
            return Err(SubscriptionError::Binding);
        }
        if next_ordinal <= self.tombstone_ordinal {
            return Err(SubscriptionError::FenceLost);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::{TerminalReason, TerminalReceipt, TerminalRequest};
    use crate::replication::{
        Cursor, Scope, SourceHistory, Stream, SubscriberId, SubscriberKey, SubscriptionEpoch,
        SubscriptionError, SubscriptionLimits,
    };

    fn request() -> TerminalRequest {
        let scope = Scope {
            stream: Stream {
                group: "g".into(),
                topic: "events".into(),
                kind: "v1".into(),
            },
            partition: "p".into(),
        };
        let history = SourceHistory {
            source: "cas".into(),
            generation: 1,
        };
        TerminalRequest {
            key: SubscriberKey {
                scope: scope.clone(),
                subscriber: SubscriberId {
                    name: "billing".into(),
                },
            },
            epoch: SubscriptionEpoch {
                ordinal: NonZeroU64::new(7).expect("nonzero"),
                native: vec![7],
                history: history.clone(),
            },
            acknowledged: Cursor {
                scope,
                history,
                position: vec![2],
            },
            request_id: vec![9],
            reason: TerminalReason::Unsubscribed,
        }
    }

    #[test]
    fn terminal_tombstone_binds_exact_request_and_reset_requires_higher_ordinal() {
        let requested = request();
        requested
            .validate(256, SubscriptionLimits::default())
            .expect("bounded identity");
        let receipt = TerminalReceipt {
            request: requested.clone(),
            tombstone_ordinal: requested.epoch.ordinal,
            durable: true,
        };
        assert_eq!(receipt.validate_against(&requested), Ok(()));
        assert_eq!(
            receipt.validate_reset(&requested.key, requested.epoch.ordinal),
            Err(SubscriptionError::FenceLost)
        );
        receipt
            .validate_reset(&requested.key, NonZeroU64::new(8).expect("nonzero"))
            .expect("strictly newer source fence");
        let mut forged = receipt.clone();
        forged.tombstone_ordinal = NonZeroU64::new(6).expect("nonzero");
        assert_eq!(
            forged.validate_reset(&requested.key, NonZeroU64::new(8).expect("nonzero")),
            Err(SubscriptionError::FenceLost)
        );
        let mut foreign = requested.clone();
        foreign.request_id = vec![10];
        assert_eq!(
            receipt.validate_against(&foreign),
            Err(SubscriptionError::Binding)
        );
        let mut volatile = receipt;
        volatile.durable = false;
        assert_eq!(
            volatile.validate_against(&requested),
            Err(SubscriptionError::Binding)
        );
    }
}
