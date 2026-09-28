//! Source-backed replica replay, snapshot, and idle-session decisions.

use crate::Time;

use super::{
    AckWaitError, BoundComparison, Coverage, Cursor, IdentityError, IdlePolicy, SnapshotConfig,
    SubscriptionError,
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
    /// Optional source-check backoff for inactive scopes; never extends proof freshness.
    pub idle: Option<IdlePolicy>,
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
            idle: None,
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
        if self
            .idle
            .is_some_and(|policy| !policy.valid(self.tail_check_ms))
        {
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
    /// Atomically registering a protected native suffix for a named subscriber.
    Registering,
    /// Reading the current durable source ack before conditional incarnation replacement.
    ReadingCurrentSubscriber,
    /// Resolving an ambiguous registration by its stable source request ID.
    ReadingRegistration,
    /// Installing the registered source epoch as a durable sink fence.
    BindingSink,
    /// Source retention and sink epoch are bound; event replay can begin.
    Protected,
    /// Checking a committed source cut from the protected subscriber ack.
    CheckingSubscriberTail,
    /// Scanning one bounded protected suffix batch.
    ScanningSubscriber,
    /// Applying one batch durably under the sink's epoch fence.
    ApplyingSubscriber,
    /// Conditionally advancing the source-protected durable ack.
    AckingSubscriber,
    /// Resolving an ambiguous source ack by its stable request ID.
    ReadingSubscriberAck,
    /// Conditionally persisting a source tombstone before retention release.
    TerminatingSubscriber,
    /// Resolving an ambiguous terminal write by exact stable request ID.
    ReadingSubscriberTerminal,
    /// Source durably ended this lineage; delivery cannot resume.
    TerminatedSubscriber,
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
    /// Named subscription registration or sink fence failed exact validation.
    Subscription(SubscriptionError),
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

mod effect;
mod event;
pub use effect::Effect;
pub use event::Event;

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
