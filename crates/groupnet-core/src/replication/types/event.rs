//! Inputs to the sans-IO replication session.

use super::super::{
    AckEvidence, AckWaitLimits, AckWaitRequest, BoundComparison, ChunkReceipt, Cursor,
    DurableDeliveryReceipt, FencedCheckpoint, HoldReceipt, RegisterReceipt, RegisterSubscriber,
    ResumeSubscriber, SnapshotOffer, SourceProof, SourceSubscriberState, SubscriberAckReceipt,
    SubscriberKey, SubscriptionError, SubscriptionLimits, TerminalReceipt,
};
use super::{ApplyReceipt, Batch, Operation, SnapshotCleanupDisposition, Time};

/// Input supplied by a driver after its source/application adapter acts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// Prompt a source-backed named subscription independently of feed hints.
    PollSubscriber,
    /// Source-certified committed cut and retained boundary for this subscription.
    SubscriberTail {
        /// Exact core-issued source-tail operation.
        op: Operation,
        /// Source proof over the registered native history.
        proof: SourceProof,
        /// Proof-bound comparisons for ack/retention/head.
        comparisons: Vec<BoundComparison>,
    },
    /// One bounded, contiguous source batch after the protected ack cursor.
    SubscriberScanned {
        /// Exact source scan operation.
        op: Operation,
        /// Trusted native batch metadata; worker retains typed records.
        batch: Box<Batch>,
    },
    /// Sink atomically recorded effects and cursor under its epoch fence.
    SubscriberApplied {
        /// Exact sink apply operation.
        op: Operation,
        /// Durable sink result with stable subsequent source-ack request ID.
        receipt: Box<DurableDeliveryReceipt>,
        /// Source-proof-bound result cursor is at least the applied batch.
        through_to_sink: Box<BoundComparison>,
        /// Source-proof-bound sink cursor does not move backward.
        previous_to_sink: Box<BoundComparison>,
    },
    /// Source durably accepted a conditional ack after sink effects.
    SubscriberAcked {
        /// Exact source ack operation.
        op: Operation,
        /// Durable source response bound to the original stable request.
        receipt: Box<SubscriberAckReceipt>,
    },
    /// Exact stable request readback after an ambiguous source ack.
    SubscriberAckRead {
        /// Exact source readback operation.
        op: Operation,
        /// Durable result, or certified absence of that request.
        receipt: Option<Box<SubscriberAckReceipt>>,
    },
    /// Begin explicit unsubscribe under one original total deadline.
    BeginSubscriberTerminal {
        /// Stable source idempotency ID, distinct from an operation token.
        request_id: Vec<u8>,
        /// Original logical deadline, never restarted on retry/readback.
        due: Time,
    },
    /// Source durably accepted the exact terminal request.
    SubscriberTerminalCommitted {
        /// Exact core-issued source terminal operation.
        op: Operation,
        /// Durable source tombstone.
        receipt: Box<TerminalReceipt>,
    },
    /// Exact source readback after an ambiguous terminal write.
    SubscriberTerminalRead {
        /// Exact core-issued terminal readback operation.
        op: Operation,
        /// Durable tombstone or certified absence of this request.
        receipt: Option<Box<TerminalReceipt>>,
    },
    /// Resume a stable subscriber from its source-certified durable ack.
    ResumeSubscription {
        /// Stable key, fresh incarnation, policy, and replacement request ID.
        request: Box<ResumeSubscriber>,
        /// Bounded identity and retention limits.
        limits: SubscriptionLimits,
    },
    /// Start source-only terminal reconciliation for a detached name.
    StartDetachedSubscriberTerminal {
        /// Exact stable native key whose source-current ack must be read.
        key: SubscriberKey,
        /// Stable idempotency ID for the conditional tombstone.
        request_id: Vec<u8>,
        /// Original total logical deadline, including current-state read.
        due: Time,
        /// Bounded name, epoch, proof, and request metadata.
        limits: SubscriptionLimits,
    },
    /// Source-certified current named registration and durable ack.
    CurrentSubscriberRead {
        /// Exact source-current read operation.
        op: Operation,
        /// Current active registration, or certified absence/expiry.
        state: Option<Box<SourceSubscriberState>>,
    },
    /// Source conclusively rejected a subscription operation without a write.
    SubscriberRejected {
        /// Exact in-flight registration or current-state read operation.
        op: Operation,
        /// Typed source rejection, distinct from an ambiguous failed response.
        error: SubscriptionError,
    },
    /// Begin a durable named `EventComplete` registration from an explicit cursor.
    StartSubscription {
        /// Exact stable request, source policy, and fresh subscriber incarnation.
        request: Box<RegisterSubscriber>,
        /// Bounded subscription identity and retention limits.
        limits: SubscriptionLimits,
    },
    /// Source atomically accepted the exact registration request.
    SubscriberRegistered {
        /// Exact register operation.
        op: Operation,
        /// Source-certified protected cursor and monotonic sink fence.
        receipt: Box<RegisterReceipt>,
    },
    /// Readback of an ambiguous registration by the same stable request ID.
    SubscriberRegistrationRead {
        /// Exact readback operation.
        op: Operation,
        /// Exact receipt if durably accepted, or certified absent result.
        receipt: Option<Box<RegisterReceipt>>,
    },
    /// Sink atomically installed the source epoch and durable application cursor.
    SinkEpochBound {
        /// Exact sink binding operation.
        op: Operation,
        /// Durable epoch/cursor receipt, possibly ahead of source ack.
        checkpoint: Box<FencedCheckpoint>,
        /// Proof-bound protected source ack <= recovered sink cursor.
        source_to_sink: Box<BoundComparison>,
        /// Proof-bound recovered sink cursor <= committed registration head.
        sink_to_head: Box<BoundComparison>,
    },
    /// Read or floor activity resets idle backoff and promptly rechecks stale proof.
    Activity,
    /// Begin one bounded named acknowledgement wait on a certified fixed roster.
    StartAckWait {
        /// Stable external request, exact target, and certified required set.
        request: Box<AckWaitRequest>,
        /// Explicit per-wait bounds; the source adapter certified the roster.
        limits: AckWaitLimits,
    },
    /// Source-confirmed evidence for one named subscriber.
    AckObserved {
        /// Exact wait and subscriber evidence.
        evidence: Box<AckEvidence>,
    },
    /// A bounded source check found no new matching evidence.
    AckChecked {
        /// Exact live wait operation.
        op: Operation,
    },
    /// The source can no longer certify the fixed roster or target history.
    AckAuthorityLost {
        /// Exact live wait operation.
        op: Operation,
    },
    /// Cancel the wait without rolling back any external source commit.
    CancelAckWait {
        /// Exact live wait operation.
        op: Operation,
    },
    /// Begin an opt-in state-sync snapshot with a deadline sampled now.
    StartSnapshot,
    /// Exact acquire response; a late response cannot renew an expired attempt.
    SnapshotHeld {
        /// Exact acquire operation.
        op: Operation,
        /// Request-bound finite hold certificate.
        receipt: HoldReceipt,
    },
    /// Consistent-cut image metadata from the held source.
    SnapshotOffered {
        /// Exact offer operation.
        op: Operation,
        /// Bounded consistent-cut image metadata.
        offer: Box<SnapshotOffer>,
    },
    /// Private stage opened with a reserved decoded-candidate budget.
    SnapshotOpened {
        /// Exact stage-open operation.
        op: Operation,
        /// Charged decoded/private candidate bytes.
        charged_bytes: u64,
    },
    /// One bounded image chunk was read into a worker-held payload.
    SnapshotRead {
        /// Exact chunk-read operation.
        op: Operation,
        /// Bounded worker-held chunk metadata.
        chunk: ChunkReceipt,
    },
    /// One exact image chunk was staged.
    SnapshotWritten {
        /// Exact stage-write operation.
        op: Operation,
        /// Index written in sequential order.
        index: u32,
        /// Exclusive encoded-image offset after this chunk.
        through: u64,
        /// Current decoded/private candidate byte charge.
        charged_bytes: u64,
    },
    /// Trusted application verified the image schema and digest.
    SnapshotVerified {
        /// Exact image-verification operation.
        op: Operation,
        /// Current decoded/private candidate byte charge.
        charged_bytes: u64,
    },
    /// Source barrier for bounded private replay.
    SnapshotBarrier {
        /// Exact barrier operation.
        op: Operation,
        /// Verified source-native replay barrier.
        proof: SourceProof,
        /// Exact comparisons of cut against head and retention boundary.
        comparisons: Vec<BoundComparison>,
    },
    /// One bounded source batch for the private stage.
    SnapshotScanned {
        /// Exact private scan operation.
        op: Operation,
        /// Contiguous bounded committed batch.
        batch: Box<Batch>,
    },
    /// Private application made a source batch visible within the stage.
    SnapshotApplied {
        /// Exact private apply operation.
        op: Operation,
        /// Exact source cursor visible within the private stage.
        through: Cursor,
        /// Current decoded/private candidate byte charge.
        charged_bytes: u64,
    },
    /// Private state was sealed at the exact replay barrier.
    SnapshotSealed {
        /// Exact private seal operation.
        op: Operation,
        /// Cursor sealed atomically with private state.
        through: Cursor,
        /// Worker-held candidate, keyed by the seal token.
        payload_id: u64,
        /// Current decoded/private candidate byte charge.
        charged_bytes: u64,
    },
    /// Exact durable snapshot candidate install completion.
    SnapshotInstalled {
        /// Exact guarded-install operation.
        op: Operation,
        /// Durable exact-cursor application receipt.
        receipt: ApplyReceipt,
    },
    /// Source delivery attached with no gap after the installed barrier.
    SnapshotAttached {
        /// Exact source attach operation.
        op: Operation,
        /// Source-proven attach barrier and retained suffix.
        proof: SourceProof,
        /// Exact comparisons of installed cursor against attach proof.
        comparisons: Vec<BoundComparison>,
    },
    /// Bounded best-effort cleanup completion; never grants authority.
    SnapshotCleaned {
        /// Exact best-effort cleanup operation.
        op: Operation,
    },
    /// Local resource disposal after cleanup failure or expiry.
    SnapshotDiscarded {
        /// Exact cleanup operation that requested local disposal.
        op: Operation,
        /// Disposition echoed from the exact disposal effect.
        disposition: SnapshotCleanupDisposition,
    },
    /// Begin source-backed bootstrap through the shared operation allocator.
    StartBootstrap,
    /// Private checkpoint load completion; both fields are absent together.
    CheckpointLoaded {
        /// Exact load operation.
        op: Operation,
        /// Validated native cursor of a recoverable atomic checkpoint.
        cursor: Option<Cursor>,
        /// Driver-held native candidate, keyed by the load token.
        payload_id: Option<u64>,
    },
    /// Guarded checkpoint install completion.
    CheckpointInstalled {
        /// Exact install operation, distinct from load.
        op: Operation,
        /// Durable application-visible cursor receipt.
        receipt: ApplyReceipt,
    },
    /// Restore an adapter-validated atomic state/cursor checkpoint.
    Resume {
        /// Adapter-validated durable state/cursor checkpoint.
        cursor: Cursor,
    },
    /// A best-effort feed hint; it does not provide source authority.
    Hint,
    /// Request at least this native floor, with comparison to an existing target.
    Demand {
        /// Requested source-native floor.
        cursor: Cursor,
        /// Bound comparison to an already demanded floor, if any.
        comparison: Option<BoundComparison>,
    },
    /// Advance virtual time and independently check the source tail when due.
    Tick(Time),
    /// Source tail and retained suffix proof, with comparisons against current
    /// materialized cursor, retention boundary, and demanded floor.
    Tail {
        /// Correlation of the requested tail check.
        op: Operation,
        /// Adapter-validated source statement.
        proof: SourceProof,
        /// Required exact-operand comparisons.
        comparisons: Vec<BoundComparison>,
    },
    /// Result of a requested bounded source scan.
    Scanned {
        /// Original scan operation.
        op: Operation,
        /// Bounded native record metadata.
        batch: Box<Batch>,
    },
    /// Application made the scanned batch visible.
    Applied {
        /// Apply operation.
        op: Operation,
        /// Application visibility and durability receipt.
        receipt: ApplyReceipt,
    },
    /// Application revoked stale serving; never advances materialized state.
    Invalidated {
        /// Revocation operation previously issued by the engine.
        op: Operation,
    },
    /// Authority policy changed, independently of source cursor progress.
    Authority(bool),
    /// An issued operation failed transiently.
    Failed {
        /// Timed-out or failed operation.
        op: Operation,
    },
    /// Cancel work and close the read gate, preserving materialized state.
    Cancel,
    /// Supersede the session after an incomparable source-history transition.
    Supersede,
}
