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

mod coord;
mod engine;
mod id;

pub mod wire;

pub use engine::{Command, Config, Effect, GroupEngine, VersionedValue};
pub use id::{GroupId, NodeId};

/// A logical, monotonic timestamp in milliseconds since a driver-chosen epoch.
///
/// The engine never *reads* time — a driver passes it in via
/// [`GroupEngine::on_tick`]. Using a plain integer (rather than
/// `std::time::Instant`) is what lets a simulator fabricate and replay time
/// deterministically.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct Time(pub u64);

impl Time {
    /// The zero instant.
    pub const ZERO: Time = Time(0);

    /// Returns `self + ms`, saturating at `u64::MAX`.
    #[must_use]
    pub fn saturating_add(self, ms: u64) -> Time {
        Time(self.0.saturating_add(ms))
    }
}
