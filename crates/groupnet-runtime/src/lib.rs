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
//! use groupnet_runtime::LinkProvider;
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
//! Application protocols bind through [`Node::endpoint`] and [`Node::peer`]
//! with a descriptor — [`Messages`], [`Ordered`] or [`Unordered`] — or a custom
//! [`PeerImplementation`]. Ordered streams need configured
//! [`tunnels`](NodeBuilder::tunnels):
//!
//! ```no_run
//! use groupnet_core::NodeId;
//! use groupnet_runtime::{Node, Ordered};
//!
//! # async fn demo(node: Node, peer: NodeId) -> std::io::Result<()> {
//! let ordered = node.endpoint(Ordered::new())?;
//! let outgoing = ordered.connect(&peer).await?;
//! let (origin, incoming) = ordered.accept().await?;
//! # let _ = (outgoing, origin, incoming);
//! # Ok(())
//! # }
//! ```
//!
//! Ordered streams are scoped by an application [`StreamProtocolId`]: each id
//! has its own accept queue on every node, named in the stream's authenticated
//! setup, so independent consumers run independent accept loops and never
//! receive each other's streams. [`Ordered::protocol`] selects an id, and a
//! protocol-scoped [`Endpoint<Ordered>`](Endpoint) is itself a
//! [`BulkTransport`](groupnet_transport::bulk::BulkTransport), so an RPC
//! server or any other bulk consumer runs over its own protocol:
//!
//! ```no_run
//! use groupnet_runtime::{Node, Ordered, StreamProtocolId};
//! use groupnet_transport::bulk::DataPlane;
//!
//! const RPC: StreamProtocolId = StreamProtocolId::new(1);
//! const BLOBS: StreamProtocolId = StreamProtocolId::new(2);
//!
//! # async fn demo(node: Node) -> std::io::Result<()> {
//! let rpc = DataPlane::new(node.endpoint(Ordered::protocol(RPC))?);
//! let blobs = DataPlane::new(node.endpoint(Ordered::protocol(BLOBS))?);
//! // Each accept loop sees only its own protocol's streams.
//! let ((_, rpc_stream), (_, blob_stream)) = tokio::try_join!(rpc.accept(), blobs.accept())?;
//! # let _ = (rpc_stream, blob_stream);
//! # Ok(())
//! # }
//! ```
//!
//! Coordination protocol logic lives entirely in the sans-IO [`groupnet-core`];
//! [`groupnet-messaging`] owns opaque application frames and their delivery
//! receipts. This crate drives both through the managed network. Swap the
//! transport (or drive the same core with [`groupnet-sim`]) without touching
//! coordination logic.
//!
//! The sibling-crate types this crate's signatures take or return are
//! re-exported: routing and tunnel types ([`Router`], [`RouterConfig`],
//! [`TunnelConfig`], [`TunnelTransport`], [`LinkProvider`]) and session types
//! ([`OrderedProtocol`], [`TunneledStream`], [`UnorderedProtocol`],
//! [`UnorderedSession`], …) at the root, and message types in [`messaging`].
//!
//! [`GroupEngine`]: groupnet_core::GroupEngine
//! [`join_group`]: Node::join_group
//! [`groupnet-core`]: groupnet_core
//! [`groupnet-messaging`]: groupnet_messaging
//! [`groupnet-sim`]: https://docs.rs/groupnet-sim
#![cfg_attr(docsrs, feature(doc_cfg))]

mod anchor;
mod capability;
mod clock;
mod driver;
mod group;
pub mod messaging;
mod network;
mod node;
mod routing;
mod seeds;
mod store;

pub use anchor::{Anchor, AnchorCas, AnchorFuture, AnchorToken, AnchorWriteIf};
pub use driver::GroupEvent;
pub use group::{
    BoundedRosterError, CommandRejected, EntryBudget, EntryInspectionError, EntryInspectionLimits,
    EntryInspectionLimitsError, EntryMutationError, EntryMutationLimits, EntryMutationLimitsError,
    EntryRemoval, EntryRevision, EntryWrite, Group, InspectedEntries, InspectedEntry,
    InspectedPair, InspectedPairEntry, Leadership, SyncCtx,
};
pub use groupnet_network::tunnel::{
    PeerIdentity, PeerIdentityError, TlsIdentity, TlsIdentityError, TunnelTransport, TunneledStream,
};
pub use groupnet_network::{LinkProvider, Router, RouterConfig, TunnelConfig, TunnelConfigError};
pub use groupnet_streams::{
    OrderedConfig, OrderedConfigError, OrderedProtocol, SessionProtocol, StreamProtocolId,
    UnorderedConfig, UnorderedConfigError, UnorderedDelivery, UnorderedOptions,
    UnorderedOptionsError, UnorderedProtocol, UnorderedSession,
};
pub use node::{
    Endpoint, GroupProfile, ManagedProtocol, Messages, Node, NodeBuilder, Ordered, Peer,
    PeerImplementation, ReceiveBinding, Unordered,
};
pub use routing::Routing;
#[cfg(feature = "dns")]
pub use seeds::SystemResolver;
pub use seeds::{
    DEFAULT_REFRESH_INTERVAL, DEFAULT_RETRY_INTERVAL, DEFAULT_STARTUP_ATTEMPTS, NamedSeeds,
    NamedSeedsError, ResolveFuture, SeedEvent, SeedResolver,
};
pub use store::{FileGrantStore, GrantStore, VoterStorage};
