//! Bounded, sans-IO recovery for a volatile coherence feed and origin-backed state.
//!
//! This module never manufactures a durable source cursor from gossip. Its
//! read permission is only one conjunct of the consumer's lease and index gates.

mod engine;
mod types;

pub use engine::RecoveryEngine;
pub use types::{
    Mark, Peer, RecoveryConfig, RecoveryEffect, RecoveryError, RecoveryEvent, RecoveryFallback,
    RecoveryMode, RecoveryOperation, RecoveryRearm, RecoveryStage, RecoveryState, RecoveryStep,
};

#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_peer;
