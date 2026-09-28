//! Thin async shell for source-backed state synchronization.
//!
//! A source adapter proves committed native positions and returns bounded
//! native batches. An application adapter owns its state format and guarded
//! installation. [`Replication`] owns scheduling, retries, catch-up, and the
//! local read gate. Native snapshots and source-certified fixed named
//! acknowledgement waits are opt-in. Event-complete durable subscriptions
//! are not provided by this slice.

mod ack_api;
mod api;
mod fence;
mod shell;
mod snapshot_api;
mod snapshot_runtime;

pub use ack_api::{
    AckEvidenceSource, AckObservation, AckSourceFailure, AckSourceFuture, AckWaitStartError,
    NamedAckRequest, NamedAckResult,
};
pub use api::{
    AdapterFailure, ApplicationAdapter, CatchUp, Checkpoint, CheckpointLimit, CheckpointLoadResult,
    FailureClass, Limits, LimitsError, Materialized, ReadVerdict, ScanLimit, SourceAdapter,
    SourceBatch, SourceScanResult, TailLimit,
};
pub use fence::{InstallPermit, OperationFence, RevocationPermit};
pub use shell::{OpenError, Replication, SessionHandle, SessionStatus};
pub use snapshot_api::{
    NativeSnapshot, ReplayOnly, SnapshotApplicationAdapter, SnapshotAttachment, SnapshotHold,
    SnapshotImage, SnapshotSourceAdapter, SnapshotStage,
};
