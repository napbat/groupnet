//! Bounded, sans-IO recovery for a volatile coherence feed and origin-backed state.
//!
//! This module never manufactures a durable source cursor from gossip. Its
//! read permission is only one conjunct of the consumer's lease and index gates.
//!
//! [`RecoveryConfig`] carries its nonzero roster caps as `NonZero*` fields;
//! [`RecoveryConfig::validate`] and [`RecoveryRearm::validate`] check the
//! remaining millisecond and cross-field rules and return typed errors whose
//! variants name the field.

mod engine;
mod types;

pub use engine::RecoveryEngine;
pub use types::{
    Mark, Peer, RecoveryConfig, RecoveryConfigError, RecoveryEffect, RecoveryError, RecoveryEvent,
    RecoveryFallback, RecoveryMode, RecoveryOperation, RecoveryRearm, RecoveryRearmError,
    RecoveryStage, RecoveryState, RecoveryStep, Renewal,
};

#[cfg(test)]
mod peer_tests;
#[cfg(test)]
mod seal_tests;
#[cfg(test)]
mod tests;
