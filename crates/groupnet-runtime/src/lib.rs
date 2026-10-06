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

mod anchor;
mod capability;
mod driver;
mod group;
#[cfg(feature = "router")]
mod network;
mod node;
mod routing;
mod seeds;
mod store;

pub use anchor::{Anchor, AnchorCas, AnchorFuture, AnchorToken, AnchorWriteIf};
pub use driver::GroupEvent;
pub use group::{
    BoundedRosterError, CommandRejected, EntryBudget, EntryInspectionError, EntryInspectionLimits,
    EntryMutationError, EntryMutationLimits, EntryRevision, Group, InspectedEntries,
    InspectedEntry, InspectedPair, InspectedPairEntry, Leadership, SyncCtx,
};
pub use groupnet_core::{RecoveredGrant, Role, Status};
#[cfg(feature = "router")]
pub use network::NetworkBuilder;
pub use node::{GroupProfile, Node, NodeBuilder};
pub use routing::Routing;
#[cfg(feature = "dns")]
pub use seeds::SystemResolver;
pub use seeds::{
    DEFAULT_REFRESH_INTERVAL, DEFAULT_RETRY_INTERVAL, DEFAULT_STARTUP_ATTEMPTS, NamedSeeds,
    ResolveFuture, SeedEvent, SeedResolver,
};
pub use store::{FileGrantStore, GrantStore};
