//! # groupnet-runtime
//!
//! The ergonomic, async face of Groupnet. It runs **one [`GroupEngine`] per
//! group as an independent actor task**, so a node hosting many groups spreads
//! them across every core with no shared lock on the hot path — the classic
//! single-writer-per-shard model.
//!
//! You bind a [`Transport`] and get a [`Node`]; from it you [`join_group`] to
//! get a [`Group`] handle:
//!
//! ```no_run
//! use groupnet_runtime::{Node, mem::Network};
//! use groupnet_core::NodeId;
//!
//! # async fn demo() {
//! let net = Network::new(); // any Transport impl works here
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

pub mod mem;

pub use group::{Group, SyncCtx};
pub use node::{Node, NodeBuilder};
