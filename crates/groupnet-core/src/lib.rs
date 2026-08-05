//! # groupnet-core
//!
//! The **sans-IO** heart of Groupnet: a deterministic coordination state
//! machine with no networking, no clock, no async, and no dependencies.
//!
//! The engine never performs I/O and never reads the clock. Instead it consumes
//! *events* and returns *effects*:
//!
//! ```text
//!   on_message(from, wire) ─┐
//!   on_tick(now)           ─┼──▶ GroupEngine ──▶ Vec<Effect>
//!   apply(command)         ─┘                     (Send / ArmTimer / ...)
//! ```
//!
//! Because the engine is pure, a driver can run it any way it likes:
//!
//! * [`groupnet-sim`] steps it single-threaded against a virtual clock for
//!   fully reproducible tests.
//! * [`groupnet-runtime`] runs one engine per group as an async actor across
//!   every core.
//!
//! Determinism here is *structural*, not conventional: there is no clock to
//! read and no socket to touch, so a driver cannot accidentally make it
//! non-deterministic.
//!
//! [`groupnet-sim`]: https://docs.rs/groupnet-sim
//! [`groupnet-runtime`]: https://docs.rs/groupnet-runtime

mod config;
mod engine;
mod id;
mod membership;
mod time;

pub mod placement;
pub mod wire;

pub use config::{Activation, Config, GroupMode, HostedConfig};
pub use engine::{Command, Effect, GroupEngine, NetStats, Role, VersionedValue};
pub use id::{GroupId, NodeId};
pub use membership::Status;
pub use time::Time;
