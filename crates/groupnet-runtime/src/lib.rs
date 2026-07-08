//! # groupnet-runtime
//!
//! The ergonomic, async face of Groupnet. It runs **one [`GroupEngine`] per
//! group as an independent actor task**, so a node hosting many groups spreads
//! them across every core with no shared lock on the hot path — the classic
//! single-writer-per-shard model.
//!
//! You bind any [`Transport`] and get a [`Node`]; from it you [`join_group`] to
//! get a [`Group`] handle. This crate is **transport-agnostic** — the concrete
//! bindings live in their own crates (`groupnet-transport-mem`,
//! `groupnet-transport-udp`, …), or you implement the trait yourself:
//!
//! ```no_run
//! use groupnet_runtime::Node;
//! use groupnet_core::NodeId;
//! use groupnet_transport::Transport;
//!
//! # async fn demo<T: Transport>(transport: T) {
//! let node = Node::builder(NodeId::new("node-a"), transport)
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
//! The protocol logic lives entirely in the sans-IO [`groupnet-core`]; this
//! crate is just the glue that pumps events between the engine and the
//! transport. Swap the transport (or drive the same core with
//! [`groupnet-sim`]) without touching a line of coordination logic.
//!
//! [`GroupEngine`]: groupnet_core::GroupEngine
//! [`Transport`]: groupnet_transport::Transport
//! [`join_group`]: Node::join_group
//! [`groupnet-core`]: groupnet_core
//! [`groupnet-sim`]: https://docs.rs/groupnet-sim

mod driver;
mod group;
mod node;
mod routing;

pub use group::{Group, SyncCtx};
pub use groupnet_core::Status;
pub use node::{Node, NodeBuilder};
pub use routing::Routing;
