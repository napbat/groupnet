//! Thin runtime for volatile-feed recovery decisions.
//!
//! The sans-IO [`groupnet_core::volatile_recovery::RecoveryEngine`] owns the
//! transition policy. This shell owns bounded work scheduling and a
//! synchronously revoked local publication gate.
//! It never treats a gossip head as a durable source position.

pub mod bootstrap;
mod shell;

pub use shell::{
    AdapterError, BoxRecoveryFuture, PeerObservation, PublicationPermit, ReadyCapturePermit,
    RecoveryAdapter, RecoveryHandle, RecoveryOpenError, RecoveryStatus,
};

pub use groupnet_core::volatile_recovery::{
    Mark, Peer, RecoveryConfig, RecoveryError, RecoveryFallback, RecoveryMode, RecoveryOperation,
    RecoveryRearm, RecoveryStage, Renewal,
};
