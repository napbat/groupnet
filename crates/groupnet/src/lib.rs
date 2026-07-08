//! # Groupnet
//!
//! A deterministic, leaderless coordination fabric for systems that partition
//! state into shard groups. This umbrella crate re-exports the layered pieces:
//!
//! | Layer | Crate | Role |
//! |-------|-------|------|
//! | core | [`groupnet_core`] | sans-IO state machine — pure, deterministic, dep-free |
//! | transport | [`groupnet_transport`] | the [`Transport`] trait you bind (TCP/UDP/IPC/shmem) |
//! | runtime | [`groupnet_runtime`] | async, group-per-task [`Node`]/[`Group`] driver *(feature `runtime`, default)* |
//! | sim | [`groupnet_sim`] | deterministic single-threaded [`Simulation`] *(feature `sim`)* |
//!
//! ```no_run
//! use groupnet::{Node, NodeId};
//! use groupnet::mem::Network;
//!
//! # async fn demo() {
//! let net = Network::new();
//! let node = Node::builder(NodeId::new("node-a"), net.endpoint(NodeId::new("node-a")))
//!     .seed(NodeId::new("node-b"))
//!     .spawn();
//!
//! let group = node.join_group("shard-42");
//! if group.is_coordinator() {
//!     group.sync(|ctx| ctx.update_metadata("routing", "v3"));
//! }
//! # }
//! ```
//!
//! For just the engine and trait (no async runtime), depend with
//! `default-features = false`.

// Core types and the wire protocol are always available.
pub use groupnet_core::{
    Command, Config, Effect, GroupEngine, GroupId, NodeId, Time, VersionedValue, wire,
};
pub use groupnet_transport::{Inbound, Transport};

/// Async runtime layer (`Node`, `Group`, and the in-memory transport).
#[cfg(feature = "runtime")]
pub use groupnet_runtime::{Group, Node, NodeBuilder, SyncCtx, mem};

/// Deterministic simulation driver.
#[cfg(feature = "sim")]
pub use groupnet_sim::Simulation;
