//! Replay-only, source-backed replica session decisions.

use crate::Time;

use super::{BoundComparison, Coverage, Cursor, IdentityError, Scope, SourceProof};

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

/// Current session stage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    /// No validated checkpoint or source barrier.
    Unready,
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
