//! # groupnet-runtime
//!
//! The ergonomic, async face of Groupnet. It runs **one [`GroupEngine`] per
//! group as an independent actor task**, so a node hosting many groups spreads
//! them across every core with no shared lock on the hot path — the classic
//! single-writer-per-shard model.
//!
//! You configure protocol links and start a managed [`Node`]; from it you
//! [`join_group`] to get a [`Group`] handle. This crate is **protocol-agnostic**:
//! the concrete link providers live in their own crates
//! (`groupnet-transport-mem`, `groupnet-transport-udp`, …).
//!
//! ```no_run
//! use groupnet_runtime::Node;
//! use groupnet_core::NodeId;
//! use groupnet_transport::link::LinkProvider;
//!
//! # async fn demo(link: impl LinkProvider) -> std::io::Result<()> {
//! let node = Node::builder(NodeId::new("node-a"))
//!     .link(link)
//!     .seed(NodeId::new("node-b"))
//!     .start().await?;
//!
//! let group = node.join_group("shard-42");
//! if group.is_coordinator() {
//!     group.sync(|ctx| ctx.update_metadata("routing", "v3"));
//! }
//! # Ok(())
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
pub use node::{GroupProfile, Node, NodeBuilder};
pub use routing::Routing;
#[cfg(feature = "dns")]
pub use seeds::SystemResolver;
pub use seeds::{
    DEFAULT_REFRESH_INTERVAL, DEFAULT_RETRY_INTERVAL, DEFAULT_STARTUP_ATTEMPTS, NamedSeeds,
    ResolveFuture, SeedEvent, SeedResolver,
};
pub use store::{FileGrantStore, GrantStore};
