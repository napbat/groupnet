//! Ordered effects issued by the sans-IO replication session.

use super::super::{
    AckWaitOutcome, AckWaitRequest, ChunkReceipt, CommitSubscriberAck, Cursor, RegisterReceipt,
    RegisterSubscriber, RequiredSubscriber, Scope, SnapshotOffer, SubscriberKey, TerminalRequest,
};
use super::{Batch, Operation, SnapshotCleanupDisposition, Time};

/// Driver work requested by the sans-IO session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    /// Check source committed cut from a subscriber-protected cursor.
    CheckSubscriberTail {
        /// Exact core-issued source check operation.
        op: Operation,
        /// Registered subscriber key and source epoch.
        registration: Box<RegisterReceipt>,
        /// Current source-protected durable ack.
        from: Cursor,
    },
    /// Scan a bounded retained suffix for this exact subscriber.
    ScanSubscriber {
        /// Exact core-issued source scan operation.
        op: Operation,
        /// Registered subscriber lineage and protected cursor.
        registration: Box<RegisterReceipt>,
        /// Exclusive source-ack cursor.
        from: Cursor,
        /// Maximum committed records.
        max_events: usize,
        /// Maximum encoded bytes.
        max_bytes: usize,
    },
    /// Durably apply or idempotently verify a batch under the sink epoch.
    ApplySubscriberBatch {
        /// Exact core-issued sink operation.
        op: Operation,
        /// Registered source epoch and monotonic sink-fence ordinal.
        registration: Box<RegisterReceipt>,
        /// Source-proven contiguous native batch.
        batch: Box<Batch>,
        /// Recovered durable sink cursor; never roll this cursor back.
        previous_sink: Cursor,
    },
    /// Conditionally commit a protected source ack only after sink durability.
    CommitSubscriberAck {
        /// Exact core-issued source ack operation.
        op: Operation,
        /// Stable source request and expected prior protected cursor.
        request: Box<CommitSubscriberAck>,
    },
    /// Resolve an ambiguous source ack by its exact stable request ID.
    ReadSubscriberAck {
        /// Exact core-issued source readback operation.
        op: Operation,
        /// Stable original request; never switch to another epoch or cursor.
        request: Box<CommitSubscriberAck>,
    },
    /// Persist an exact terminal tombstone before releasing retention.
    CommitSubscriberTerminal {
        /// Exact core-issued source terminal operation.
        op: Operation,
        /// Stable conditional request bound to this lineage and ack.
        request: Box<TerminalRequest>,
    },
    /// Resolve an ambiguous terminal write by exact source request ID.
    ReadSubscriberTerminal {
        /// Exact core-issued terminal readback operation.
        op: Operation,
        /// Stable original request; never switch epoch or protected cursor.
        request: Box<TerminalRequest>,
    },
    /// Read current durable source ack before conditional replacement.
    ReadCurrentSubscriber {
        /// Core-issued source-current read operation.
        op: Operation,
        /// Exact existing named subscriber key.
        key: SubscriberKey,
    },
    /// Atomically protect an explicit native suffix before event delivery.
    RegisterSubscriber {
        /// Core-issued register operation.
        op: Operation,
        /// Stable source request and finite retention policy.
        request: Box<RegisterSubscriber>,
    },
    /// Resolve an ambiguous registration by its exact stable request ID.
    ReadSubscriberRegistration {
        /// Core-issued readback operation.
        op: Operation,
        /// Exact subscriber key.
        key: SubscriberKey,
        /// Stable original register request ID.
        request_id: Vec<u8>,
    },
    /// Durably fence the sink to the certified source epoch before delivery.
    BindSinkEpoch {
        /// Core-issued sink binding operation.
        op: Operation,
        /// Exact validated source registration receipt.
        registration: Box<RegisterReceipt>,
    },
    /// Event-driven source observation for one exact named wait; return
    /// `AckObserved` or a bounded empty `AckChecked` result.
    ObserveNamedAcks {
        /// Core-issued operation from the session allocator.
        op: Operation,
        /// Exact request and fixed certified roster.
        request: Box<AckWaitRequest>,
        /// Currently unmet members of that immutable certified roster.
        waiting: Vec<RequiredSubscriber>,
        /// Absolute deadline for this source observation, including capacity wait.
        due: Time,
    },
    /// Terminal wait outcome, independent of externally committed write status.
    AckWaitFinished {
        /// Core-issued wait operation.
        op: Operation,
        /// Exact fixed-set result or degradation.
        outcome: AckWaitOutcome,
    },
    /// Acquire a finite source hold before choosing a snapshot cut.
    AcquireSnapshotHold {
        /// Core-issued acquire operation.
        op: Operation,
        /// Source scope to hold before choosing a cut.
        scope: Scope,
        /// Total recovery deadline measured before the acquire request.
        total_due: Time,
    },
    /// Request a consistent source cut and bounded image offer.
    OfferSnapshot {
        /// Core-issued offer operation.
        op: Operation,
        /// Exact source scope under the held history.
        scope: Scope,
    },
    /// Begin a private stage with explicit decoded-candidate reservation.
    OpenSnapshotStage {
        /// Core-issued stage-open operation.
        op: Operation,
        /// Validated bounded image metadata.
        offer: Box<SnapshotOffer>,
        /// Decoded/private candidate byte budget to reserve.
        max_candidate_bytes: u64,
    },
    /// Read one bounded sequential image chunk.
    ReadSnapshotChunk {
        /// Core-issued chunk-read operation.
        op: Operation,
        /// Zero-based sequential chunk index.
        index: u32,
        /// Encoded image offset.
        offset: u64,
        /// Maximum encoded bytes to return.
        max_bytes: usize,
    },
    /// Transfer one worker-held chunk to the private stage.
    WriteSnapshotChunk {
        /// Core-issued stage-write operation.
        op: Operation,
        /// Worker-held exact chunk to stage.
        chunk: ChunkReceipt,
    },
    /// Verify complete image integrity in the private stage.
    VerifySnapshotImage {
        /// Core-issued verification operation.
        op: Operation,
        /// Source-verified expected complete-image digest.
        digest: Vec<u8>,
    },
    /// Check a source barrier under the same finite hold.
    SnapshotReplayBarrier {
        /// Core-issued barrier operation.
        op: Operation,
        /// Consistent-cut cursor to retain and replay after.
        from: Cursor,
    },
    /// Scan a bounded committed suffix for private replay.
    SnapshotScan {
        /// Core-issued private scan operation.
        op: Operation,
        /// Exclusive native cursor.
        from: Cursor,
        /// Committed-event budget.
        max_events: usize,
        /// Encoded batch byte budget.
        max_bytes: usize,
    },
    /// Apply a bounded source batch to private state.
    SnapshotApply {
        /// Core-issued private apply operation.
        op: Operation,
        /// Source-proven batch to apply to private state.
        batch: Box<Batch>,
    },
    /// Seal a private state/cursor pair at the exact replay barrier.
    SealSnapshotStage {
        /// Core-issued private seal operation.
        op: Operation,
        /// Exact replay barrier reached within the stage.
        through: Cursor,
    },
    /// Install a sealed private candidate with a guarded exact-cursor permit.
    InstallSnapshot {
        /// Core-issued guarded-install operation.
        op: Operation,
        /// Exact sealed candidate cursor.
        cursor: Cursor,
        /// Worker-held candidate identifier.
        payload_id: u64,
    },
    /// Attach ongoing source delivery after the installed barrier.
    AttachSnapshot {
        /// Core-issued source attach operation.
        op: Operation,
        /// Installed replay barrier to continue after.
        after: Cursor,
    },
    /// Best-effort release of hold/attachment and private-stage resources.
    CleanupSnapshot {
        /// Core-issued best-effort cleanup operation.
        op: Operation,
        /// Original acquire operation tagging the worker's resource set.
        attempt: Operation,
        /// Whether the live continuation attachment survives cleanup.
        disposition: SnapshotCleanupDisposition,
        /// Absolute logical-time cleanup deadline; execution cannot reset it.
        due: Time,
    },
    /// Synchronously drop only the resources of this exact attempt after
    /// cleanup expiry or failure, then report `SnapshotDiscarded`.
    DiscardSnapshotResources {
        /// Cleanup operation; old source replies remain fenced.
        op: Operation,
        /// Original acquire operation tagging the worker's resource set.
        attempt: Operation,
        /// Whether to preserve the live continuation attachment.
        disposition: SnapshotCleanupDisposition,
    },
    /// Load and validate a private atomic state/cursor checkpoint.
    LoadCheckpoint {
        /// Core-issued load operation.
        op: Operation,
        /// Source scope.
        scope: Scope,
    },
    /// Install a private checkpoint under this distinct guarded operation.
    InstallCheckpoint {
        /// Core-issued install operation.
        op: Operation,
        /// Expected exact checkpoint cursor.
        cursor: Cursor,
        /// Driver-held native candidate identifier.
        payload_id: u64,
    },
    /// Read the source independently of gossip.
    CheckTail {
        /// Tail operation.
        op: Operation,
        /// Source scope.
        scope: Scope,
        /// Current materialized cursor, when a checkpoint exists.
        from: Option<Cursor>,
    },
    /// Request a bounded continuous suffix after `from`.
    Scan {
        /// Scan operation.
        op: Operation,
        /// Exclusive cursor.
        from: Cursor,
        /// Event budget.
        max_events: usize,
        /// Byte budget.
        max_bytes: usize,
    },
    /// Apply these opaque records in order; report visibility separately.
    Apply {
        /// Apply operation.
        op: Operation,
        /// Driver-retained native batch metadata.
        batch: Box<Batch>,
    },
    /// Revoke local serving before retry, gap recovery, or cancellation.
    RevokeServing {
        /// Revocation operation to acknowledge with `Invalidated`.
        op: Operation,
    },
    /// Emergency local gate revocation after operation-token exhaustion. The
    /// terminal session cannot accept an acknowledgement or resume work.
    RevokeServingUnconfirmed,
    /// Schedule a logical-time wakeup.
    ArmTimer(Time),
    /// State-sync retention gap; a later snapshot tier must recover it.
    NeedsSnapshot,
    /// Event-complete subscription cannot skip the missing interval.
    IrrecoverableGap,
}
