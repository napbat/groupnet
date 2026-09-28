//! Provisional builder choice for volatile peer index bootstrap.
//!
//! Claims coordinate origin work but never authorize serving. Image capture,
//! transfer, replay, and the final read gate belong to later slices.

pub mod journal;

mod engine;
mod types;

pub use engine::ClaimEngine;
pub use types::{
    BootstrapClaim, BootstrapConfig, BootstrapEffect, BootstrapError, BootstrapEvent,
    BootstrapMember, BootstrapOperation, BootstrapScope, BootstrapStage, BootstrapStep,
    ClaimIdentity, ClaimPhase,
};

#[cfg(test)]
mod tests;
