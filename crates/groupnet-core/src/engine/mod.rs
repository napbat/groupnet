//! The sans-IO group engine: SWIM-style liveness, digest/delta anti-entropy,
//! per-node keyed state, metadata registers, and coordinator selection —
//! pure decisions in, [`Effect`]s out.

mod anti_entropy;
mod command;
mod effect;
mod liveness;
mod merge;
mod state;
mod stats;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests_liveness;
#[cfg(test)]
mod tests_state;

pub use command::Command;
pub use effect::Effect;
pub use state::{GroupEngine, VersionedValue};
pub use stats::NetStats;
