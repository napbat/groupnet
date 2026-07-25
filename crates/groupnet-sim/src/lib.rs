//! # groupnet-sim
//!
//! A **deterministic, single-threaded** driver for [`GroupEngine`]s. It owns a
//! virtual clock and an in-memory network with a configurable (but *fixed*,
//! never random) latency and drop schedule, then steps every engine in strict
//! time order.
//!
//! This is the same core that [`groupnet-runtime`] runs across threads in
//! production — here it runs in a plain event loop so an entire cluster's
//! behaviour is reproducible bit-for-bit. No async runtime, no real sockets, no
//! wall clock.
//!
//! ```
//! use groupnet_core::{Config, GroupEngine, GroupId, NodeId, Time};
//! use groupnet_sim::Simulation;
//!
//! let group = GroupId::new("shard-42");
//! let ids: Vec<NodeId> = ["a", "b", "c"].iter().map(|s| NodeId::new(*s)).collect();
//! let mut sim = Simulation::new(10); // 10ms link latency
//! for id in &ids {
//!     let seeds = ids.iter().filter(|x| *x != id).cloned();
//!     sim.add(GroupEngine::new(group.clone(), id.clone(), seeds, Config::default()));
//! }
//! sim.run_until(Time(5_000));
//! assert!(sim.all_agree_on_coordinator());
//! ```
//!
//! [`groupnet-runtime`]: https://docs.rs/groupnet-runtime
//! [`GroupEngine`]: groupnet_core::GroupEngine

mod rng;
mod simulation;

pub use rng::SplitMix64;
pub use simulation::Simulation;
