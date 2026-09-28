//! Opt-in typed source and sink capabilities for durable named event delivery.

use std::error::Error;
use std::future::Future;

use groupnet_core::replication::{
    CommitSubscriberAck, FencedCheckpoint, RegisterReceipt, RegisterSubscriber, SourceProof,
    SourceSubscriberState, SubscriberAckReceipt, SubscriberKey, SubscriptionError,
};

use super::api::{AdapterFailure, ScanLimit, SourceAdapter, SourceBatch, TailLimit};
use super::fence::InstallPermit;

/// Source response distinguishes a conclusive no-write rejection from an
/// ambiguous transport/authority failure requiring exact request readback.
#[derive(Debug)]
pub enum SubscriptionSourceResult<T, E> {
    /// Source durably accepted the requested conditional operation.
    Accepted(T),
    /// Source conclusively rejected the request without applying it.
    Rejected(SubscriptionError),
    /// Source operation did not prove whether an external write happened.
    Failed(AdapterFailure<E>),
}

/// Native source with durable subscriber retention interlocked with history
/// retirement, registration, ack advancement, and explicit expiry.
///
/// The durable per-subscriber fence ordinal survives source-history replacement
/// and strictly increases on every incarnation/reset. A source that loses this
/// ordinal ledger must report authority loss. All responses below are locally
/// validated source evidence; peer feed bytes cannot become these receipts.
/// Native source futures must release bounded allocations when cancelled; any
/// detached work must keep its own admission until it actually completes.
pub trait DurableSubscriptionSource: SourceAdapter {
    /// Reads current durable ack and ordinal, or certified absence/expiry.
    fn read_current_subscriber(
        &self,
        key: SubscriberKey,
    ) -> impl Future<Output = SubscriptionSourceResult<Option<SourceSubscriberState>, Self::Error>> + Send;

    /// Atomically claims an absent name or compares both prior ordinal and
    /// source ack before replacement, protecting the exact requested cursor.
    fn register_subscriber(
        &self,
        request: RegisterSubscriber,
    ) -> impl Future<Output = SubscriptionSourceResult<RegisterReceipt, Self::Error>> + Send;

    /// Resolves an ambiguous registration by its exact stable request ID.
    fn read_subscriber_registration(
        &self,
        key: SubscriberKey,
        request_id: Vec<u8>,
    ) -> impl Future<Output = SubscriptionSourceResult<Option<RegisterReceipt>, Self::Error>> + Send;

    /// Probes a bounded committed cut and retained suffix under this exact
    /// registered epoch, independently of notifications or gossip.
    fn subscriber_tail(
        &self,
        registration: RegisterReceipt,
        from: Self::Position,
        limit: TailLimit,
    ) -> impl Future<Output = SubscriptionSourceResult<SourceProof, Self::Error>> + Send;

    /// Produces one contiguous protected whole-event batch. The returned
    /// native batch remains charged to Groupnet's existing global byte pool.
    fn scan_subscriber(
        &self,
        registration: RegisterReceipt,
        from: Self::Position,
        proof: SourceProof,
        limit: ScanLimit,
    ) -> impl Future<
        Output = SubscriptionSourceResult<SourceBatch<Self::Position, Self::Batch>, Self::Error>,
    > + Send;

    /// Atomically compare epoch and previous protected cursor, then durably
    /// advance the source ack without letting retention pass an unacked event.
    fn commit_subscriber_ack(
        &self,
        request: CommitSubscriberAck,
    ) -> impl Future<Output = SubscriptionSourceResult<SubscriberAckReceipt, Self::Error>> + Send;

    /// Reads back the exact stable ack request after an ambiguous outcome.
    fn read_subscriber_ack(
        &self,
        request: CommitSubscriberAck,
    ) -> impl Future<Output = SubscriptionSourceResult<Option<SubscriberAckReceipt>, Self::Error>> + Send;
}

/// Application transaction that fences an exact subscriber source epoch.
///
/// The sink must atomically reject a lower durable fence ordinal even if an
/// older bind began before a newer bind completed. `InstallPermit` guards
/// local publication; an external transaction must additionally condition
/// its own commit on the source epoch/ordinal and exact previous sink cursor.
/// Sink futures must not leave uncharged detached native work after timeout;
/// the sink's durable transaction remains externally fenced on takeover.
pub trait DurableEventSink<P, B>: Send + Sync + 'static {
    /// Sink-specific transaction failure.
    type Error: Error + Send + Sync + 'static;

    /// Binds a source-certified epoch and recovers the sink's exact durable
    /// cursor without rolling it backward. Same-epoch retry is idempotent;
    /// lower-ordinal delayed binds are rejected by the sink transaction.
    fn bind_subscriber_epoch(
        &self,
        registration: RegisterReceipt,
        permit: InstallPermit,
    ) -> impl Future<Output = Result<FencedCheckpoint, AdapterFailure<Self::Error>>> + Send;

    /// Atomically applies or verifies one whole native batch under the epoch
    /// and conditional previous sink cursor. On an ambiguous response, a
    /// retry must read back or idempotently prove the exact prior transaction;
    /// it cannot double-apply effects or silently advance beyond the batch.
    /// The stable source-ack request ID is persisted with the sink effects.
    fn apply_subscriber_batch(
        &self,
        registration: RegisterReceipt,
        from: P,
        through: P,
        previous_sink: P,
        native: B,
        permit: InstallPermit,
    ) -> impl Future<
        Output = Result<
            groupnet_core::replication::DurableDeliveryReceipt,
            AdapterFailure<Self::Error>,
        >,
    > + Send;
}
