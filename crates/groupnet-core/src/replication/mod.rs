//! Sans-IO decisions for source-backed, replay-only replica sessions.
//!
//! This first slice never interprets native positions or commits application
//! state. A source adapter supplies scoped, proof-bound comparisons and
//! contiguous batches; an application adapter reports completed materialization.
//! The driver executes effects and returns their generation and operation token.

mod ack_types;
pub mod admission;
mod identity;
mod idle;
mod proof;
mod session;
mod snapshot;
mod subscription;
mod types;

pub use ack_types::{
    AckEvidence, AckKind, AckTarget, AckWaitError, AckWaitLimits, AckWaitOutcome, AckWaitRequest,
    CertifiedRoster, RequiredSubscriber,
};
pub use identity::{Cursor, IdentityError, Scope, SourceHistory, Stream};
pub use idle::IdlePolicy;
pub use proof::{BoundComparison, Comparison, Coverage, ProofId, SourceProof};
pub use session::SessionEngine;
pub use snapshot::{ChunkReceipt, HoldReceipt, SnapshotConfig, SnapshotOffer};
pub use subscription::{
    CommitSubscriberAck, DurableDeliveryReceipt, FencedCheckpoint, RegisterReceipt,
    RegisterSubscriber, ResumeSubscriber, RetentionPolicy, SourceSubscriberState,
    SubscriberAckReceipt, SubscriberId, SubscriberKey, SubscriptionEpoch, SubscriptionError,
    SubscriptionLimits, TerminalReason, TerminalReceipt, TerminalRequest,
};
pub use types::{
    ApplyReceipt, Batch, Config, ConfigError, Effect, Event, Mode, Operation, ReadDecision,
    Refusal, Reject, SnapshotCleanupDisposition, Stage, State, Step,
};

#[cfg(test)]
mod bootstrap_tests;
#[cfg(test)]
mod snapshot_tests;
#[cfg(test)]
mod tests;
