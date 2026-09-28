//! Sans-IO decisions for source-backed, replay-only replica sessions.
//!
//! This first slice never interprets native positions or commits application
//! state. A source adapter supplies scoped, proof-bound comparisons and
//! contiguous batches; an application adapter reports completed materialization.
//! The driver executes effects and returns their generation and operation token.

pub mod admission;
mod identity;
mod proof;
mod session;
mod types;

pub use identity::{Cursor, IdentityError, Scope, SourceHistory, Stream};
pub use proof::{BoundComparison, Comparison, Coverage, ProofId, SourceProof};
pub use session::SessionEngine;
pub use types::{
    ApplyReceipt, Batch, Config, ConfigError, Effect, Event, Mode, Operation, ReadDecision,
    Refusal, Reject, Stage, State, Step,
};

#[cfg(test)]
mod bootstrap_tests;
#[cfg(test)]
mod tests;
