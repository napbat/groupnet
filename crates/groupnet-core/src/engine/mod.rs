//! The sans-IO group engine: SWIM-style liveness, digest/delta anti-entropy,
//! per-node keyed state, metadata registers, coordinator selection, and the
//! opt-in Hosted-mode election — pure decisions in, [`Effect`]s out.

mod anti_entropy;
mod command;
mod effect;
mod election;
mod liveness;
mod merge;
mod state;
mod stats;

pub use command::Command;
pub use effect::Effect;
pub use election::{RecoveredGrant, Role};
pub use state::{GroupEngine, VersionedValue};
pub use stats::NetStats;
