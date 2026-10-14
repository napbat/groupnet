//! Thin runtime for volatile-feed recovery decisions.
//!
//! The sans-IO [`groupnet_core::volatile_recovery::RecoveryEngine`] owns the
//! transition policy. This shell owns bounded work scheduling and a
//! synchronously revoked local publication gate.
//! It never treats a gossip head as a durable source position.
//!
//! The core domain types this module's signatures use are re-exported here;
//! the engine's events, effects, and steps stay in `groupnet_core`.

pub mod bootstrap;
mod error;
mod shell;

pub use error::{AdapterError, AdapterErrorKind};
pub use shell::{
    PeerObservation, PublicationPermit, ReadyCapturePermit, RecoveryAdapter, RecoveryBuilder,
    RecoveryHandle, RecoveryOpenError, RecoveryStatus,
};

pub use groupnet_core::volatile_recovery::{
    Mark, Peer, RecoveryConfig, RecoveryError, RecoveryFallback, RecoveryMode, RecoveryOperation,
    RecoveryRearm, RecoveryStage, RecoveryState, Renewal,
};
