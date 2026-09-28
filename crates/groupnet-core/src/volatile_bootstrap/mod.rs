//! Provisional builder choice for volatile peer index bootstrap.
//!
//! Claims coordinate origin work but never authorize serving. Image capture,
//! transfer and replay are bounded child decisions. The final read gate is
//! always supplied by the application recovery and lease authority.

pub mod journal;
pub mod transfer;

mod engine;
mod types;

pub use engine::ClaimEngine;
pub use types::{
    BootId, BootstrapClaim, BootstrapConfig, BootstrapEffect, BootstrapError, BootstrapEvent,
    BootstrapMember, BootstrapOperation, BootstrapScope, BootstrapStage, BootstrapStep,
    ClaimIdentity, ClaimPhase,
};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod tests_transfer;
