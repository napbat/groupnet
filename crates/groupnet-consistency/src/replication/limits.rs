//! Validated shell capacity around the sans-IO replication core.

use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;

use groupnet_core::replication::{Config as CoreConfig, ConfigError as CoreConfigError};
use groupnet_transport::QueueCapacity;
use tokio::sync::Semaphore;

const DEFAULT_MAX_SCOPES: NonZeroUsize = NonZeroUsize::new(1024).unwrap();
const DEFAULT_MAX_PARALLEL_OPS: NonZeroUsize = NonZeroUsize::new(16).unwrap();
const DEFAULT_MAX_INFLIGHT_BYTES: NonZeroUsize = NonZeroUsize::new(32 * 1024 * 1024).unwrap();
const DEFAULT_MAX_CHECKPOINT_BYTES: NonZeroUsize = NonZeroUsize::new(64 * 1024 * 1024).unwrap();
const DEFAULT_MAX_CHECKPOINT_INFLIGHT_BYTES: NonZeroUsize =
    NonZeroUsize::new(256 * 1024 * 1024).unwrap();
const DEFAULT_QUEUE_DEPTH: QueueCapacity = QueueCapacity::of(32);

/// Bounded shell capacity in addition to the sans-IO core's batch limits.
///
/// Construct with struct-update syntax over [`Limits::default`];
/// [`Limits::validate`] reports the first violated cross-field rule, and
/// [`Replication::new`](super::Replication::new) runs it before starting work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// Core cursor, batch, retry, and tail cadence limits.
    pub core: CoreConfig,
    /// Maximum registered local scopes/tasks.
    pub max_scopes: NonZeroUsize,
    /// Maximum concurrent source/application operations across all scopes.
    pub max_parallel_ops: NonZeroUsize,
    /// Total reserved native-batch bytes across all scopes; at least one
    /// `core.max_batch_bytes` batch.
    pub max_inflight_bytes: NonZeroUsize,
    /// Maximum private recoverable checkpoint candidate per scope.
    pub max_checkpoint_bytes: NonZeroUsize,
    /// Global reserve for private checkpoint candidates awaiting install; at
    /// least one `max_checkpoint_bytes` candidate.
    pub max_checkpoint_inflight_bytes: NonZeroUsize,
    /// Bounded command queue per scope; floor requests return backpressure.
    pub queue_depth: QueueCapacity,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            core: CoreConfig::default(),
            max_scopes: DEFAULT_MAX_SCOPES,
            max_parallel_ops: DEFAULT_MAX_PARALLEL_OPS,
            max_inflight_bytes: DEFAULT_MAX_INFLIGHT_BYTES,
            max_checkpoint_bytes: DEFAULT_MAX_CHECKPOINT_BYTES,
            max_checkpoint_inflight_bytes: DEFAULT_MAX_CHECKPOINT_INFLIGHT_BYTES,
            queue_depth: DEFAULT_QUEUE_DEPTH,
        }
    }
}

/// The first [`Limits`] rule a configuration violates, naming its field(s).
///
/// Every reserve is a Tokio semaphore, so its permit count is capped at
/// [`Semaphore::MAX_PERMITS`]; every single-acquire charge is a `u32`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LimitsError {
    /// `core` failed the sans-IO core's own validation; carries the core's
    /// error unchanged.
    Core(CoreConfigError),
    /// `max_parallel_ops` exceeds the semaphore permit ceiling.
    ParallelOpsAbovePermits {
        /// Configured `max_parallel_ops`.
        max_parallel_ops: usize,
    },
    /// `max_inflight_bytes` cannot hold one `core.max_batch_bytes` batch.
    InflightBelowBatch {
        /// Configured `max_inflight_bytes`.
        max_inflight_bytes: usize,
        /// Configured `core.max_batch_bytes`.
        max_batch_bytes: usize,
    },
    /// `max_inflight_bytes` exceeds the semaphore permit ceiling.
    InflightAbovePermits {
        /// Configured `max_inflight_bytes`.
        max_inflight_bytes: usize,
    },
    /// `core.max_batch_bytes` cannot be charged as one `u32` permit count.
    BatchAboveU32 {
        /// Configured `core.max_batch_bytes`.
        max_batch_bytes: usize,
    },
    /// `max_checkpoint_bytes` cannot be charged as one `u32` permit count.
    CheckpointAboveU32 {
        /// Configured `max_checkpoint_bytes`.
        max_checkpoint_bytes: usize,
    },
    /// `max_checkpoint_inflight_bytes` cannot hold one `max_checkpoint_bytes`
    /// candidate.
    CheckpointInflightBelowCheckpoint {
        /// Configured `max_checkpoint_inflight_bytes`.
        max_checkpoint_inflight_bytes: usize,
        /// Configured `max_checkpoint_bytes`.
        max_checkpoint_bytes: usize,
    },
    /// `max_checkpoint_inflight_bytes` exceeds the semaphore permit ceiling.
    CheckpointInflightAbovePermits {
        /// Configured `max_checkpoint_inflight_bytes`.
        max_checkpoint_inflight_bytes: usize,
    },
    /// Snapshot recovery (`core.snapshot`) holds one operation for the
    /// snapshot stream while replay continues, so `max_parallel_ops` must be
    /// at least two.
    SnapshotParallelOpsBelowTwo {
        /// Configured `max_parallel_ops`.
        max_parallel_ops: usize,
    },
    /// `core.snapshot.max_chunk_bytes` cannot be charged as one `u32` permit
    /// count.
    SnapshotChunkAboveU32 {
        /// Configured `core.snapshot.max_chunk_bytes`.
        max_chunk_bytes: usize,
    },
    /// `max_inflight_bytes` cannot reserve one replay batch beside one
    /// snapshot chunk: it must be at least `core.max_batch_bytes +
    /// max(core.snapshot.max_chunk_bytes, core.max_batch_bytes)`.
    SnapshotInflightBelowReserve {
        /// Configured `max_inflight_bytes`.
        max_inflight_bytes: usize,
        /// Configured `core.max_batch_bytes`.
        max_batch_bytes: usize,
        /// Configured `core.snapshot.max_chunk_bytes`.
        max_chunk_bytes: usize,
    },
    /// `core.snapshot.max_candidate_bytes` exceeds `max_checkpoint_bytes`.
    SnapshotCandidateAboveCheckpoint {
        /// Configured `core.snapshot.max_candidate_bytes`.
        max_candidate_bytes: u64,
        /// Configured `max_checkpoint_bytes`.
        max_checkpoint_bytes: usize,
    },
    /// `max_checkpoint_inflight_bytes` cannot reserve one checkpoint beside
    /// one snapshot candidate: it must be at least `max_checkpoint_bytes +
    /// core.snapshot.max_candidate_bytes`.
    SnapshotCheckpointInflightBelowReserve {
        /// Configured `max_checkpoint_inflight_bytes`.
        max_checkpoint_inflight_bytes: usize,
        /// Configured `max_checkpoint_bytes`.
        max_checkpoint_bytes: usize,
        /// Configured `core.snapshot.max_candidate_bytes`.
        max_candidate_bytes: u64,
    },
}

impl fmt::Display for LimitsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let permits = Semaphore::MAX_PERMITS;
        match *self {
            Self::Core(_) => f.write_str("replication limits: invalid core config"),
            Self::ParallelOpsAbovePermits { max_parallel_ops } => write!(
                f,
                "replication limits: max_parallel_ops ({max_parallel_ops}) exceeds {permits}"
            ),
            Self::InflightBelowBatch {
                max_inflight_bytes,
                max_batch_bytes,
            } => write!(
                f,
                "replication limits: max_inflight_bytes ({max_inflight_bytes}) is below \
                 core.max_batch_bytes ({max_batch_bytes})"
            ),
            Self::InflightAbovePermits { max_inflight_bytes } => write!(
                f,
                "replication limits: max_inflight_bytes ({max_inflight_bytes}) exceeds {permits}"
            ),
            Self::BatchAboveU32 { max_batch_bytes } => write!(
                f,
                "replication limits: core.max_batch_bytes ({max_batch_bytes}) exceeds u32::MAX"
            ),
            Self::CheckpointAboveU32 {
                max_checkpoint_bytes,
            } => write!(
                f,
                "replication limits: max_checkpoint_bytes ({max_checkpoint_bytes}) exceeds \
                 u32::MAX"
            ),
            Self::CheckpointInflightBelowCheckpoint {
                max_checkpoint_inflight_bytes,
                max_checkpoint_bytes,
            } => write!(
                f,
                "replication limits: max_checkpoint_inflight_bytes \
                 ({max_checkpoint_inflight_bytes}) is below max_checkpoint_bytes \
                 ({max_checkpoint_bytes})"
            ),
            Self::CheckpointInflightAbovePermits {
                max_checkpoint_inflight_bytes,
            } => write!(
                f,
                "replication limits: max_checkpoint_inflight_bytes \
                 ({max_checkpoint_inflight_bytes}) exceeds {permits}"
            ),
            Self::SnapshotParallelOpsBelowTwo { max_parallel_ops } => write!(
                f,
                "replication limits: snapshot recovery needs max_parallel_ops >= 2, got \
                 {max_parallel_ops}"
            ),
            Self::SnapshotChunkAboveU32 { max_chunk_bytes } => write!(
                f,
                "replication limits: core.snapshot.max_chunk_bytes ({max_chunk_bytes}) exceeds \
                 u32::MAX"
            ),
            Self::SnapshotInflightBelowReserve {
                max_inflight_bytes,
                max_batch_bytes,
                max_chunk_bytes,
            } => write!(
                f,
                "replication limits: max_inflight_bytes ({max_inflight_bytes}) cannot reserve \
                 core.max_batch_bytes ({max_batch_bytes}) beside max(core.snapshot.\
                 max_chunk_bytes ({max_chunk_bytes}), core.max_batch_bytes)"
            ),
            Self::SnapshotCandidateAboveCheckpoint {
                max_candidate_bytes,
                max_checkpoint_bytes,
            } => write!(
                f,
                "replication limits: core.snapshot.max_candidate_bytes ({max_candidate_bytes}) \
                 exceeds max_checkpoint_bytes ({max_checkpoint_bytes})"
            ),
            Self::SnapshotCheckpointInflightBelowReserve {
                max_checkpoint_inflight_bytes,
                max_checkpoint_bytes,
                max_candidate_bytes,
            } => write!(
                f,
                "replication limits: max_checkpoint_inflight_bytes \
                 ({max_checkpoint_inflight_bytes}) cannot reserve max_checkpoint_bytes \
                 ({max_checkpoint_bytes}) beside core.snapshot.max_candidate_bytes \
                 ({max_candidate_bytes})"
            ),
        }
    }
}

impl Error for LimitsError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Core(error) => Some(error),
            _ => None,
        }
    }
}

impl Limits {
    /// Checks the core config, every semaphore reserve, and the snapshot
    /// reserves when `core.snapshot` is set. Nothing is clamped.
    ///
    /// # Errors
    /// Returns the first violated rule as a [`LimitsError`] variant naming
    /// the offending field(s) and values.
    pub fn validate(&self) -> Result<(), LimitsError> {
        self.core.validate().map_err(LimitsError::Core)?;
        let permits = Semaphore::MAX_PERMITS;
        let max_parallel_ops = self.max_parallel_ops.get();
        let max_inflight_bytes = self.max_inflight_bytes.get();
        let max_batch_bytes = self.core.max_batch_bytes.get();
        let max_checkpoint_bytes = self.max_checkpoint_bytes.get();
        let max_checkpoint_inflight_bytes = self.max_checkpoint_inflight_bytes.get();
        if max_parallel_ops > permits {
            return Err(LimitsError::ParallelOpsAbovePermits { max_parallel_ops });
        }
        if max_inflight_bytes < max_batch_bytes {
            return Err(LimitsError::InflightBelowBatch {
                max_inflight_bytes,
                max_batch_bytes,
            });
        }
        if max_inflight_bytes > permits {
            return Err(LimitsError::InflightAbovePermits { max_inflight_bytes });
        }
        if u32::try_from(max_batch_bytes).is_err() {
            return Err(LimitsError::BatchAboveU32 { max_batch_bytes });
        }
        if u32::try_from(max_checkpoint_bytes).is_err() {
            return Err(LimitsError::CheckpointAboveU32 {
                max_checkpoint_bytes,
            });
        }
        if max_checkpoint_inflight_bytes < max_checkpoint_bytes {
            return Err(LimitsError::CheckpointInflightBelowCheckpoint {
                max_checkpoint_inflight_bytes,
                max_checkpoint_bytes,
            });
        }
        if max_checkpoint_inflight_bytes > permits {
            return Err(LimitsError::CheckpointInflightAbovePermits {
                max_checkpoint_inflight_bytes,
            });
        }
        if let Some(snapshot) = self.core.snapshot {
            let max_chunk_bytes = snapshot.max_chunk_bytes.get();
            let max_candidate_bytes = snapshot.max_candidate_bytes.get();
            if max_parallel_ops < 2 {
                return Err(LimitsError::SnapshotParallelOpsBelowTwo { max_parallel_ops });
            }
            if u32::try_from(max_chunk_bytes).is_err() {
                return Err(LimitsError::SnapshotChunkAboveU32 { max_chunk_bytes });
            }
            if max_batch_bytes
                .checked_add(max_chunk_bytes.max(max_batch_bytes))
                .is_none_or(|reserve| reserve > max_inflight_bytes)
            {
                return Err(LimitsError::SnapshotInflightBelowReserve {
                    max_inflight_bytes,
                    max_batch_bytes,
                    max_chunk_bytes,
                });
            }
            if u64::try_from(max_checkpoint_bytes).is_ok_and(|bytes| max_candidate_bytes > bytes) {
                return Err(LimitsError::SnapshotCandidateAboveCheckpoint {
                    max_candidate_bytes,
                    max_checkpoint_bytes,
                });
            }
            if usize::try_from(max_candidate_bytes)
                .ok()
                .and_then(|candidate| max_checkpoint_bytes.checked_add(candidate))
                .is_none_or(|reserve| reserve > max_checkpoint_inflight_bytes)
            {
                return Err(LimitsError::SnapshotCheckpointInflightBelowReserve {
                    max_checkpoint_inflight_bytes,
                    max_checkpoint_bytes,
                    max_candidate_bytes,
                });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
