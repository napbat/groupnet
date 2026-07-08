//! # Groupnet
//!
//! A deterministic, leaderless coordination fabric for systems that partition
//! state into shard groups. This umbrella crate re-exports the layered pieces:
//!
//! | Layer | Crate | Role |
//! |-------|-------|------|
//! | core | [`groupnet_core`] | sans-IO state machine + [`placement`] (weighted HA-hash) — pure, deterministic, dep-free |
//! | control transport | [`groupnet_transport`] | the datagram [`Transport`] trait you bind |
//! | control bindings | `groupnet-transport-{mem,udp}` | re-exported as [`mem`] / [`udp`] |
//! | data plane | [`groupnet_transport`]`::bulk` | stream `BulkTransport` + framing — re-exported as [`bulk`] *(feature `bulk`)* |
//! | data binding | `groupnet-transport-tcp` | TCP stream binding — re-exported as [`tcp`] *(feature `tcp`)* |
//! | runtime | [`groupnet_runtime`] | async, group-per-task [`Node`]/[`Group`] driver + [`Routing`] *(feature `runtime`, default)* |
//! | sim | [`groupnet_sim`] | deterministic single-threaded [`Simulation`] *(feature `sim`)* |
//!
//! Two planes: the **control plane** (small best-effort datagrams — gossip,
//! membership, routing) and the opt-in **data plane** (reliable byte streams —
//! replication, bulk transfer). Both are transport-agnostic; bind whichever
//! crate you want or your own impls.
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

// Core types, the placement primitive, and the wire protocol are always
// available.
pub use groupnet_core::{
    Command, Config, Effect, GroupEngine, GroupId, NodeId, Status, Time, VersionedValue, placement,
    wire,
};
pub use groupnet_transport::{Inbound, Transport};

/// Async runtime layer: `Node`, `Group`, and the routing table.
#[cfg(feature = "runtime")]
pub use groupnet_runtime::{Group, Node, NodeBuilder, Routing, SyncCtx};

/// In-memory control-plane transport binding (`groupnet-transport-mem`).
#[cfg(feature = "mem")]
pub use groupnet_transport_mem as mem;

/// UDP control-plane transport binding (`groupnet-transport-udp`).
#[cfg(feature = "udp")]
pub use groupnet_transport_udp as udp;

/// Data-plane stream transport: `BulkTransport`, `DataStream`, `DataPlane`
/// (`groupnet-transport`'s `bulk` module).
#[cfg(feature = "bulk")]
pub use groupnet_transport::bulk;

/// TCP data-plane binding (`groupnet-transport-tcp`).
#[cfg(feature = "tcp")]
pub use groupnet_transport_tcp as tcp;

/// Deterministic simulation driver.
#[cfg(feature = "sim")]
pub use groupnet_sim::Simulation;
