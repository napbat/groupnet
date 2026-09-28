//! Thin async shell for source-backed, replay-only state synchronization.
//!
//! A source adapter proves committed native positions and returns bounded
//! native batches. An application adapter owns its state format and guarded
//! installation. [`Replication`] owns scheduling, retries, catch-up, and the
//! local read gate. This slice does not provide snapshots or event-complete
//! durable subscriptions.

mod api;
mod fence;
mod shell;

pub use api::{
    AdapterFailure, ApplicationAdapter, CatchUp, Checkpoint, CheckpointLimit, CheckpointLoadResult,
    FailureClass, Limits, LimitsError, Materialized, ReadVerdict, ScanLimit, SourceAdapter,
    SourceBatch, SourceScanResult, TailLimit,
};
pub use fence::{InstallPermit, OperationFence, RevocationPermit};
pub use shell::{OpenError, Replication, SessionHandle, SessionStatus};
