//! Replay-only, source-backed replica session decisions.

use crate::Time;

use super::{
    AckEvidence, AckWaitError, AckWaitLimits, AckWaitOutcome, AckWaitRequest, BoundComparison,
    ChunkReceipt, Coverage, Cursor, HoldReceipt, IdentityError, RequiredSubscriber, Scope,
    SnapshotConfig, SnapshotOffer, SourceProof,
};

/// Replay contract selected for this session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// Missing history requires a covering snapshot in a later tier.
    StateSync,
    /// Missing any event is terminal; a snapshot cannot replace it.
    EventComplete,
}

/// Limits enforced before effects and before accepting adapter responses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    /// Optional finite state-sync snapshot recovery; absent for replay-only sessions.
    pub snapshot: Option<SnapshotConfig>,
    /// Maximum encoded native cursor or proof byte length.
    pub max_cursor_bytes: usize,
    /// Maximum records in one replay batch.
    pub max_batch_events: usize,
    /// Maximum encoded bytes in one replay batch.
    pub max_batch_bytes: usize,
    /// Independent source-tail check cadence in logical milliseconds.
    pub tail_check_ms: u64,
    /// Delay before retrying a transient failure.
    pub retry_ms: u64,
    /// Maximum transient failures before stopping automatic work.
    pub max_retries: u32,
    /// Maximum wait for one issued source or application operation.
    pub attempt_timeout_ms: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            snapshot: None,
            max_cursor_bytes: 4096,
            max_batch_events: 4096,
            max_batch_bytes: 8 * 1024 * 1024,
            tail_check_ms: 5000,
            retry_ms: 1000,
            max_retries: 3,
            attempt_timeout_ms: 30_000,
        }
    }
}

/// Invalid session configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigError {
    /// A budget or cadence is zero.
    Zero,
    /// Scope identity is empty or exceeds the configured encoding bound.
    Identity(IdentityError),
}

impl Config {
    /// Validates that all budgets and cadences make progress.
    ///
    /// # Errors
    /// Returns [`ConfigError::Zero`] when any required bound is zero.
    pub fn validate(self) -> Result<Self, ConfigError> {
        if self.max_cursor_bytes == 0
            || self.max_batch_events == 0
            || self.max_batch_bytes == 0
            || self.tail_check_ms == 0
            || self.retry_ms == 0
            || self.max_retries == 0
            || self.attempt_timeout_ms == 0
        {
            return Err(ConfigError::Zero);
        }
        if self.snapshot.is_some_and(|snapshot| !snapshot.valid()) {
            return Err(ConfigError::Zero);
        }
        Ok(self)
    }
}

/// Caller-supplied session incarnation, generation, and operation token.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Operation {
    /// New nonzero session incarnation on every engine reconstruction/restart.
    pub session: u64,
    /// Recovery generation.
    pub generation: u64,
    /// Monotonic operation token; never reused in this engine.
    pub token: u64,
}

/// Which worker-owned resources survive snapshot cleanup.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotCleanupDisposition {
    /// Recovery completed: retain the live source attachment only.
    Completed,
    /// Recovery aborted: drop the attachment and all transient resources.
    Aborted,
}

/// Current session stage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    /// No validated checkpoint or source barrier.
    Unready,
    /// Loading a private atomic state/cursor checkpoint.
    LoadingCheckpoint,
    /// Installing the loaded checkpoint through a guarded operation.
    InstallingCheckpoint,
    /// Waiting for a bounded source retention hold.
    SnapshotHolding,
    /// Waiting for a consistent-cut offer under the hold.
    SnapshotOffering,
    /// Creating a private bounded application stage.
    SnapshotOpening,
    /// Reading one sequential image chunk.
    SnapshotReading,
    /// Writing one image chunk to the private stage.
    SnapshotWriting,
    /// Verifying the complete private image.
    SnapshotVerifying,
    /// Checking a source replay barrier under the hold.
    SnapshotBarrier,
    /// Scanning a bounded committed suffix into private state.
    SnapshotScanning,
    /// Applying a bounded suffix batch to private state.
    SnapshotApplying,
    /// Sealing the private stage at the replay barrier.
    SnapshotSealing,
    /// Installing the sealed durable state/cursor pair.
    SnapshotInstalling,
    /// Attaching ongoing source delivery before admission.
    SnapshotAttaching,
    /// Waiting for an authoritative source tail.
    CheckingTail,
    /// Waiting for a retained source batch.
    Scanning,
    /// Waiting for the application to materialize a batch.
    Applying,
    /// Delayed retry after a transient failure.
    RetryWait,
    /// Retention gap; later state-sync snapshot work is needed.
    NeedsSnapshot,
    /// Event-complete history was irrecoverably lost.
    IrrecoverableGap,
    /// Snapshot transfer or continuity was rejected; private resources must be cleaned.
    SnapshotAborted,
    /// Explicitly cancelled until a new validated resume.
    Cancelled,
    /// Automatic retry budget was exhausted.
    RetryExhausted,
    /// Source and application are caught up to the checked barrier.
    Ready,
}

/// Visible progress, with volatile and durable positions kept distinct.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct State {
    /// Current recovery generation.
    pub generation: u64,
    /// Current stage.
    pub stage: Stage,
    /// Last successfully materialized native position, possibly volatile.
    pub materialized: Option<Cursor>,
    /// Last durable state/cursor checkpoint accepted by the adapter.
    pub checkpoint: Option<Cursor>,
    /// Latest source head proven by a tail response.
    pub head: Option<Cursor>,
    /// Highest requested read floor within the current history.
    pub target: Option<Cursor>,
}

/// Why a local read cannot be served.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// No complete replay state is installed.
    Unready,
    /// The requested floor has not been materialized.
    Floor,
    /// The selected source/mode has not granted read authority.
    Authority,
    /// The source has not checked its tail recently enough.
    TailCheckDue,
    /// A retention gap requires recovery outside this replay slice.
    Gap,
}

/// Local read-gate result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadDecision {
    /// Local state is eligible through this native cursor.
    Serve(Cursor),
    /// Caller should wait for source catch-up or use an authoritative source.
    Refuse(Refusal),
}

/// Why an input was rejected without advancing progress.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reject {
    /// A named acknowledgement request or evidence failed exact validation.
    AckWait(AckWaitError),
    /// Cursor/proof identity or bounds are invalid.
    Identity(IdentityError),
    /// Response belongs to a stale generation or replaced operation.
    StaleOperation,
    /// A source comparison is missing or is bound to other operands/proof.
    Comparison,
    /// Source history changed without a continuity proof.
    History,
    /// Source response exceeds a configured budget.
    Backpressure,
    /// Response contradicts source head or expected contiguous position.
    Discontinuity,
    /// A required counter or logical deadline would overflow.
    Exhausted,
    /// This event is invalid for the current stage.
    Stage,
}

/// Bounded contiguous source batch; the driver retains native records by handle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Batch {
    /// Source coverage of the committed interval.
    pub coverage: Coverage,
    /// Driver-private handle to the native records, scoped to the scan operation.
    pub payload_id: u64,
    /// Number of committed records in this batch.
    pub events: usize,
    /// Total encoded byte budget charged to this batch.
    pub bytes: usize,
    /// Proof-bound strict progress from the prior cursor to the batch end.
    pub advance: BoundComparison,
    /// Comparison of batch end to the checked source head.
    pub end_to_head: BoundComparison,
}

/// Application completion for a batch it made query-visible.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApplyReceipt {
    /// Exact materialized upper cursor.
    pub through: Cursor,
    /// Whether the matching state/cursor pair is durably recoverable.
    pub durable: bool,
}

/// Input supplied by a driver after its source/application adapter acts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
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

/// Driver work requested by the sans-IO session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
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

/// Result of one deterministic engine transition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Step {
    /// Driver actions, in order.
    pub effects: Vec<Effect>,
    /// Rejection of the input, when any.
    pub rejection: Option<Reject>,
}

impl Step {
    pub(super) fn ok(effects: Vec<Effect>) -> Self {
        Self {
            effects,
            rejection: None,
        }
    }

    pub(super) fn reject(reason: Reject) -> Self {
        Self {
            effects: Vec::new(),
            rejection: Some(reason),
        }
    }
}
