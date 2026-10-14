//! Thin async shell for source-backed state synchronization.
//!
//! A source adapter proves committed native positions and returns bounded
//! native batches. An application adapter owns its state format and guarded
//! installation. [`Replication`] owns scheduling, retries, catch-up, and the
//! local read gate. Native snapshots, source-certified fixed named
//! acknowledgement waits, and durable named event subscriptions are opt-in.
//!
//! This module is the tier's complete surface: it re-exports every
//! `groupnet_core::replication` domain type that an adapter signature, limit,
//! status, or error names ([`Scope`], [`Cursor`], [`SourceProof`],
//! [`CoreConfig`], …), so an adapter needs only this module and `std`. The
//! core's sans-IO engine (`SessionEngine`, `Event`, `Effect`, `Step`) stays in
//! `groupnet_core::replication` for custom drivers.

mod ack_api;
mod api;
mod fence;
mod limits;
mod open_error;
mod shell;
mod snapshot_api;
mod snapshot_runtime;
mod subscription_api;

pub use ack_api::{
    AckEvidenceSource, AckObservation, AckSourceFailure, AckWaitLimitsError, AckWaitStartError,
    NamedAckRequest, NamedAckResult,
};
pub use api::{
    AdapterFailure, ApplicationAdapter, CatchUp, Checkpoint, CheckpointLimit, CheckpointLoadResult,
    FailureClass, Materialized, ReadVerdict, ScanLimit, SourceAdapter, SourceBatch,
    SourceScanResult, TailLimit,
};
pub use fence::{InstallPermit, OperationFence, RevocationPermit};
pub use groupnet_core::Time;
pub use groupnet_core::replication::{
    AckEvidence, AckKind, AckTarget, AckWaitError, AckWaitLimits, AckWaitOutcome, AckWaitRequest,
    BoundComparison, CertifiedRoster, CommitSubscriberAck, Comparison, Config as CoreConfig,
    ConfigError as CoreConfigError, Cursor, DurableDeliveryReceipt, FencedCheckpoint, HoldReceipt,
    IdentityError, IdlePolicy, Operation, ProofId, Refusal, RegisterReceipt, RegisterSubscriber,
    Reject, RequiredSubscriber, ResumeSubscriber, RetentionPolicy, Scope, SnapshotConfig,
    SnapshotOffer, SourceHistory, SourceProof, SourceSubscriberState, Stage, Stream,
    SubscriberAckReceipt, SubscriberId, SubscriberKey, SubscriptionEpoch, SubscriptionError,
    SubscriptionLimits, TerminalReason, TerminalReceipt, TerminalRequest,
};
pub use limits::{Limits, LimitsError};
pub use open_error::OpenError;
pub use shell::{
    DetachedUnsubscribeError, EventSubscriptions, NamedSubscriptionHandle, NamedSubscriptionStatus,
    Replication, SessionHandle, SessionStatus, SubscriptionStart, TerminalInspectError,
    UnsubscribeError,
};
pub use snapshot_api::{
    NativeSnapshot, ReplayOnly, SnapshotApplicationAdapter, SnapshotAttachment, SnapshotHold,
    SnapshotImage, SnapshotSourceAdapter, SnapshotStage,
};
pub use subscription_api::{DurableEventSink, DurableSubscriptionSource, SubscriptionSourceResult};
