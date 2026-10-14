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
//! # What to use
//!
//! groupnet-core is a public protocol SDK with two audiences. Most
//! applications only need the first.
//!
//! ## Application-facing vocabulary
//!
//! The types an application names when it runs groups through
//! [`groupnet-runtime`] (directly or via the `groupnet` facade):
//!
//! * **Identity:** [`NodeId`], [`GroupId`].
//! * **Configuration:** [`Config`] (with [`Config::validate`] and
//!   [`ConfigError`]), [`GroupMode`], [`HostedConfig`], [`Activation`],
//!   [`VoterRoster`].
//! * **Observed state:** [`Status`] (a member's liveness), [`Role`] (this
//!   node's part in a Hosted election), [`NetStats`], [`VersionedValue`].
//! * **Placement:** [`placement`], the deterministic rendezvous hashing that
//!   shard/replica ownership and the derived coordinator are computed from.
//!
//! ## Protocol-driver items
//!
//! The sans-IO machinery a *driver* needs: the runtime, [`groupnet-sim`], or
//! a custom driver. An application that only uses the runtime never names
//! these.
//!
//! * **The group engine:** [`GroupEngine`], the [`Command`]s it accepts, the
//!   [`Effect`]s it returns, [`Time`] (its logical clock), and
//!   [`RecoveredGrant`] (durable voter state handed back on restart).
//! * **The wire:** [`wire`], the frame codec that drivers move between engines.
//! * **External activation:** [`anchor`], the CAS-anchor records and decision
//!   rules for [`Activation::External`].
//! * **Replication engines:** [`replication`] (source-backed replica
//!   sessions and admission), [`volatile_recovery`] and
//!   [`volatile_bootstrap`]. Each pure engine is driven by the matching
//!   `groupnet-consistency` tier, or by a custom driver.
//!
//! [`groupnet-sim`]: https://docs.rs/groupnet-sim
//! [`groupnet-runtime`]: https://docs.rs/groupnet-runtime

mod config;
mod engine;
mod id;
mod membership;
mod time;

pub mod anchor;
pub mod placement;
pub mod replication;
pub mod volatile_bootstrap;
pub mod volatile_recovery;
pub mod wire;

pub use config::{Activation, Config, ConfigError, GroupMode, HostedConfig, VoterRoster};
pub use engine::{Command, Effect, GroupEngine, NetStats, RecoveredGrant, Role, VersionedValue};
pub use id::{GroupId, NodeId};
pub use membership::Status;
pub use time::Time;
