//! Typed source and application boundaries for replay sessions.

use std::error::Error;
use std::future::Future;

use groupnet_core::replication::{
    BoundComparison, Config as CoreConfig, Cursor, ProofId, Refusal, Scope, SourceProof,
};

use super::fence::{InstallPermit, RevocationPermit};

/// Bounded shell capacity in addition to the sans-IO core's batch limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// Core cursor, batch, retry, and tail cadence limits.
    pub core: CoreConfig,
    /// Maximum registered local scopes/tasks.
    pub max_scopes: usize,
    /// Maximum concurrent source/application operations across all scopes.
    pub max_parallel_ops: usize,
    /// Total reserved native-batch bytes across all scopes.
    pub max_inflight_bytes: usize,
    /// Maximum private recoverable checkpoint candidate per scope.
    pub max_checkpoint_bytes: usize,
    /// Global reserve for private checkpoint candidates awaiting install.
    pub max_checkpoint_inflight_bytes: usize,
    /// Bounded command queue per scope; floor requests return backpressure.
    pub queue_depth: usize,
}

/// Invalid shell capacity or incompatible replay byte reserve.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LimitsError;

impl Default for Limits {
    fn default() -> Self {
        Self {
            core: CoreConfig::default(),
            max_scopes: 1024,
            max_parallel_ops: 16,
            max_inflight_bytes: 32 * 1024 * 1024,
            max_checkpoint_bytes: 64 * 1024 * 1024,
            max_checkpoint_inflight_bytes: 256 * 1024 * 1024,
            queue_depth: 32,
        }
    }
}

impl Limits {
    /// Checks all nonzero limits and the global byte reserve.
    ///
    /// # Errors
    /// Returns [`LimitsError`] for a zero bound, a byte budget smaller than one batch,
    /// or a batch reserve that cannot fit a Tokio semaphore permit count.
    pub fn validate(self) -> Result<Self, LimitsError> {
        self.core.validate().map_err(|_| LimitsError)?;
        if self.max_scopes == 0
            || self.max_parallel_ops == 0
            || self.max_parallel_ops > tokio::sync::Semaphore::MAX_PERMITS
            || self.max_inflight_bytes < self.core.max_batch_bytes
            || self.max_inflight_bytes > tokio::sync::Semaphore::MAX_PERMITS
            || self.max_checkpoint_bytes == 0
            || u32::try_from(self.max_checkpoint_bytes).is_err()
            || self.max_checkpoint_inflight_bytes < self.max_checkpoint_bytes
            || self.max_checkpoint_inflight_bytes > tokio::sync::Semaphore::MAX_PERMITS
            || self.queue_depth == 0
            || self.queue_depth > tokio::sync::Semaphore::MAX_PERMITS
            || u32::try_from(self.core.max_batch_bytes).is_err()
        {
            return Err(LimitsError);
        }
        Ok(self)
    }
}

/// Classification of an adapter failure, preserved in session outcomes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailureClass {
    /// Retry within the configured budget.
    Retryable,
    /// Adapter cannot safely proceed with this session.
    Terminal,
    /// Source or mode authority was lost; local serving must stand down.
    AuthorityLost,
}

/// Adapter error with an explicit recovery classification.
#[derive(Debug)]
pub enum AdapterFailure<E> {
    /// A bounded retry may succeed.
    Retryable(E),
    /// The session must stop.
    Terminal(E),
    /// Authority has been lost and the read gate must close.
    AuthorityLost(E),
}

impl<E> AdapterFailure<E> {
    /// Failure class without exposing the source-specific error type.
    #[must_use]
    pub fn class(&self) -> FailureClass {
        match self {
            Self::Retryable(_) => FailureClass::Retryable,
            Self::Terminal(_) => FailureClass::Terminal,
            Self::AuthorityLost(_) => FailureClass::AuthorityLost,
        }
    }
}

/// Source probe bounds. A native CAS adapter may stage a private replay fork
/// while probing; it must stop at these limits instead of scanning to head.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TailLimit {
    /// Maximum source records inspected in one step.
    pub events: usize,
    /// Maximum source bytes inspected in one step.
    pub bytes: usize,
}

/// One bounded replay request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScanLimit {
    /// Maximum committed records returned.
    pub events: usize,
    /// Maximum encoded bytes returned.
    pub bytes: usize,
}

/// Maximum native recovery candidate allocation during checkpoint load.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckpointLimit {
    /// Maximum retained native-state bytes, including a private fork's heap.
    pub bytes: usize,
}

/// Source records remain native and private to the shell until guarded apply.
#[derive(Debug)]
pub struct SourceBatch<P, B> {
    /// Exclusive prior native position.
    pub from: P,
    /// Inclusive native position reached by the batch.
    pub through: P,
    /// Source proof that authenticated the retained interval.
    pub proof: ProofId,
    /// Locally verified opaque contiguity certificate.
    pub certificate: Vec<u8>,
    /// Number of committed records covered.
    pub events: usize,
    /// Bytes charged to the configured replay budget.
    pub bytes: usize,
    /// Application-native replay window, private fork, or record collection.
    pub native: B,
}

/// Bounded source replay result.
pub type SourceScanResult<P, B, E> = Result<SourceBatch<P, B>, AdapterFailure<E>>;

/// Validated recoverable state/cursor candidate, private until guarded install.
#[derive(Debug)]
pub struct Checkpoint<P, R> {
    /// Native position recoverable with the loaded state.
    pub position: P,
    /// Application-native state image, fork, or durable handle to install.
    pub native: R,
    /// Adapter-accounted retained native-state bytes, bounded by load limit.
    pub bytes: usize,
}

/// Private recoverable checkpoint load result.
pub type CheckpointLoadResult<P, R, E> = Result<Option<Checkpoint<P, R>>, AdapterFailure<E>>;

/// Application-visible progress reported through a guarded install permit.
#[derive(Clone, Debug)]
pub struct Materialized<P> {
    pub(crate) position: P,
    pub(crate) durable: bool,
}

/// Outcome of an explicit native-floor wait.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CatchUp<P> {
    /// State through this native position is eligible for local read policy.
    Ready(P),
    /// No retained suffix can cover the missing state in this replay slice.
    NeedsSnapshot,
    /// The requested position belongs to another scope or source history.
    InvalidFloor,
    /// Source state reached the floor, but the application still bars reads.
    ReadPolicyBlocked,
    /// Admission or command queue is at its configured bound.
    Backpressured,
    /// The wait deadline elapsed; an external source commit is not rolled back.
    TimedOut,
    /// The session was cancelled or its driver stopped.
    Cancelled,
    /// The source or mode lost read authority.
    AuthorityLost,
    /// A classified source/application failure ended the wait.
    Failed(FailureClass),
}

/// Immediate local read verdict; applications may route to a valid source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadVerdict<P> {
    /// The application may serve through this native position.
    Serve(P),
    /// Local read is barred; route or wait according to the caller's policy.
    Fallback(Refusal),
}

/// Trusted local adapter for an authoritative committed source.
///
/// Remote bytes must be authenticated and checked against the source's own
/// commit/retention rules before producing these proof objects. A feed head or
/// peer agreement is not a source proof. Native `Batch` can be a private CAS
/// fork/window and need not serialize application records. `tail` may stage at
/// most its byte limit while its future runs and must drop that staging before
/// returning; only `scan_after` may return retained native data, which stays
/// charged to the shell's global byte reserve until apply finishes.
pub trait SourceAdapter: Send + Sync + 'static {
    /// Source-native position, such as a shard LSN.
    type Position: Clone + Send + Sync + std::fmt::Debug + 'static;
    /// Application-native bounded replay unit.
    type Batch: Send + 'static;
    /// Source-specific error.
    type Error: Error + Send + Sync + 'static;

    /// Encodes a typed position with full scope and history identity.
    ///
    /// # Errors
    /// Returns an adapter failure when identity or encoding cannot be proved.
    fn cursor(
        &self,
        scope: &Scope,
        position: &Self::Position,
    ) -> Result<Cursor, AdapterFailure<Self::Error>>;

    /// Decodes an already validated source cursor to the native position.
    ///
    /// # Errors
    /// Returns an adapter failure for an invalid native encoding.
    fn position(&self, cursor: &Cursor) -> Result<Self::Position, AdapterFailure<Self::Error>>;

    /// Compares exact scoped operands against this source proof.
    ///
    /// # Errors
    /// Returns an adapter failure when source ordering cannot be checked.
    fn compare(
        &self,
        left: &Cursor,
        right: &Cursor,
        proof: &SourceProof,
    ) -> Result<BoundComparison, AdapterFailure<Self::Error>>;

    /// Checks the source independently of gossip, bounded by `limit`.
    fn tail(
        &self,
        scope: Scope,
        from: Option<Self::Position>,
        limit: TailLimit,
    ) -> impl Future<Output = Result<SourceProof, AdapterFailure<Self::Error>>> + Send;

    /// Produces the next retained contiguous committed prefix after `from`.
    fn scan_after(
        &self,
        scope: Scope,
        from: Self::Position,
        proof: SourceProof,
        limit: ScanLimit,
    ) -> impl Future<Output = SourceScanResult<Self::Position, Self::Batch, Self::Error>> + Send;
}

/// Application state boundary. Groupnet owns recovery scheduling; this adapter
/// owns query visibility, state transactions, and domain conflict rules.
pub trait ApplicationAdapter<P, B>: Send + Sync + 'static {
    /// Application-specific error.
    type Error: Error + Send + Sync + 'static;
    /// Private recoverable state image or handle.
    type Recovery: Send + 'static;

    /// Loads and validates a recoverable *atomic* state/cursor pair. This is
    /// private recovery: it must not publish serving state or mutate the live
    /// replica. The adapter must honor `limit` while allocating, including a
    /// private fork's heap and domain clone; the shell keeps a global reserve
    /// until guarded install finishes. A volatile apply receipt may never be
    /// returned after restart.
    fn load_checkpoint(
        &self,
        scope: Scope,
        limit: CheckpointLimit,
    ) -> impl Future<Output = CheckpointLoadResult<P, Self::Recovery, Self::Error>> + Send;

    /// Atomically installs a private recovered state/cursor candidate through
    /// the bootstrap operation fence. The shell resumes core progress only
    /// after a durable receipt for this exact checkpoint position.
    fn install_checkpoint(
        &self,
        scope: Scope,
        checkpoint: Checkpoint<P, Self::Recovery>,
        permit: InstallPermit,
    ) -> impl Future<Output = Result<Materialized<P>, AdapterFailure<Self::Error>>> + Send;

    /// Revokes local stale serving through the scoped capability. A detached
    /// revocation must condition its serve-gate mutation on this permit so a
    /// closed session cannot revoke its successor. This is separate from
    /// materialization.
    fn revoke_serving(
        &self,
        scope: Scope,
        permit: RevocationPermit,
    ) -> impl Future<Output = Result<(), AdapterFailure<Self::Error>>> + Send;

    /// Applies one native committed prefix. Final synchronous publication must
    /// use [`InstallPermit::commit_sync`]. An async durable transaction must
    /// condition its state/cursor commit on the same operation fence *inside*
    /// that transaction before calling `confirm_durable_transaction`.
    fn apply(
        &self,
        scope: Scope,
        from: P,
        through: P,
        native: B,
        permit: InstallPermit,
    ) -> impl Future<Output = Result<Materialized<P>, AdapterFailure<Self::Error>>> + Send;

    /// Final domain read check (for example, an unflushed shard floor or an
    /// incomplete LIST index). It is independent of Groupnet's materialized
    /// cursor and serve revocation. The adapter need not provide an affirm
    /// callback: after a fresh unchanged source proof, the shell can reopen
    /// its gate, while this predicate remains the application's final veto.
    /// This method does not schedule recovery.
    fn may_serve(&self, scope: &Scope, through: &P) -> bool;
}

#[cfg(test)]
mod tests {
    use super::Limits;

    #[test]
    fn semaphore_and_batch_limits_reject_invalid_reserves() {
        let mut limits = Limits::default();
        limits.max_inflight_bytes = limits.core.max_batch_bytes - 1;
        assert!(limits.validate().is_err());

        let limits = Limits {
            max_parallel_ops: tokio::sync::Semaphore::MAX_PERMITS + 1,
            ..Limits::default()
        };
        assert!(limits.validate().is_err());

        let limits = Limits {
            queue_depth: 0,
            ..Limits::default()
        };
        assert!(limits.validate().is_err());

        let limits = Limits {
            max_checkpoint_inflight_bytes: 1,
            ..Limits::default()
        };
        assert!(limits.validate().is_err());

        if let Ok(beyond_u32) = usize::try_from(u64::from(u32::MAX) + 1) {
            let limits = Limits {
                max_checkpoint_bytes: beyond_u32,
                max_checkpoint_inflight_bytes: beyond_u32,
                ..Limits::default()
            };
            assert!(limits.validate().is_err());
        }
    }
}
