//! Bounded read-only inspection of a source's durable terminal ledger.

use std::time::Instant;

use groupnet_core::replication::{SubscriberKey, SubscriptionError, TerminalReceipt};

use super::{
    ApplicationAdapter, DurableEventSink, DurableSubscriptionSource, EventSubscriptions,
    FailureClass, SnapshotMode,
};
use crate::replication::SubscriptionSourceResult;

/// Failure of a read-only terminal-ledger inspection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TerminalInspectError {
    /// The requested key is outside the manager's bounded native group.
    InvalidKey,
    /// The original caller deadline elapsed during admission or source I/O.
    TimedOut,
    /// The source conclusively rejected this metadata query.
    Rejected(SubscriptionError),
    /// A source adapter failed without providing a durable terminal receipt.
    Source(FailureClass),
    /// The source returned a foreign, malformed, or non-durable tombstone.
    InvalidEvidence,
}

#[expect(
    private_bounds,
    reason = "the snapshot mode bridge is sealed; public constructors return only supported modes"
)]
impl<S, A, D, M> EventSubscriptions<S, A, D, M>
where
    S: DurableSubscriptionSource,
    A: ApplicationAdapter<S::Position, S::Batch>,
    D: DurableEventSink<S::Position, S::Batch>,
    M: SnapshotMode<S, A>,
{
    /// Reads a durable tombstone for a stable name under shared operation
    /// admission and one original deadline. This is metadata only: it neither
    /// changes a live session nor authorizes reset without source-conditional
    /// `ResetAt` registration.
    ///
    /// # Errors
    /// Returns invalid key, timeout, a typed source failure, or invalid proof.
    pub async fn read_current_terminal(
        &self,
        key: SubscriberKey,
        deadline: Instant,
    ) -> Result<Option<TerminalReceipt>, TerminalInspectError> {
        let max_cursor_bytes = self.replication.inner.limits.core.max_cursor_bytes;
        if key.scope.stream.group != self.replication.inner.group.id().as_str()
            || key.subscriber.name.is_empty()
            || key.subscriber.name.len() > self.limits.max_subscriber_bytes
            || key.scope.validate(max_cursor_bytes).is_err()
        {
            return Err(TerminalInspectError::InvalidKey);
        }
        if deadline <= Instant::now() {
            return Err(TerminalInspectError::TimedOut);
        }
        let result = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), async {
            let _admission = self
                .replication
                .inner
                .operations
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| TerminalInspectError::Source(FailureClass::Terminal))?;
            Ok(self
                .replication
                .inner
                .source
                .read_current_terminal(key.clone())
                .await)
        })
        .await
        .map_err(|_| TerminalInspectError::TimedOut)??;
        if Instant::now() >= deadline {
            return Err(TerminalInspectError::TimedOut);
        }
        let receipt = match result {
            SubscriptionSourceResult::Accepted(receipt) => receipt,
            SubscriptionSourceResult::Rejected(error) => {
                return Err(TerminalInspectError::Rejected(error));
            }
            SubscriptionSourceResult::Failed(error) => {
                return Err(TerminalInspectError::Source(error.class()));
            }
        };
        if let Some(receipt) = &receipt {
            receipt
                .request
                .validate(max_cursor_bytes, self.limits)
                .map_err(|_| TerminalInspectError::InvalidEvidence)?;
            receipt
                .validate_against(&receipt.request)
                .map_err(|_| TerminalInspectError::InvalidEvidence)?;
            if receipt.request.key != key {
                return Err(TerminalInspectError::InvalidEvidence);
            }
        }
        Ok(receipt)
    }
}
