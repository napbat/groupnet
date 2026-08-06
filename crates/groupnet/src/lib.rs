//! # Groupnet
//!
//! A deterministic, leaderless coordination fabric for systems that partition
//! state into shard groups. This umbrella crate re-exports the layered pieces as
//! one namespaced hierarchy — each module mirrors an underlying crate:
//!
//! | Module | Role |
//! |--------|------|
//! | [`core`] | sans-IO state machine, identity, weighted [`placement`](core::placement) (HA-hash), and the [`wire`](core::wire) protocol — pure, deterministic, dep-free |
//! | [`transport`] | the datagram [`Transport`](transport::Transport) trait, the data-plane [`bulk`](transport::bulk) streams, and the concrete bindings [`mem`](transport::mem) / [`udp`](transport::udp) / [`tcp`](transport::tcp) (persistent control-plane connections *and* bulk streams) |
//! | [`runtime`] | async, group-per-task [`Node`](runtime::Node) / [`Group`](runtime::Group) driver + [`Routing`](runtime::Routing) *(feature `runtime`, default)* |
//! | [`consistency`] | session-consistency layer: per-writer sequenced [`WriteFeed`](consistency::WriteFeed)s with loss detection, and [`Frontier`](consistency::Frontier) read-your-writes barriers *(feature `consistency`)* |
//! | [`sim`] | deterministic single-threaded [`Simulation`](sim::Simulation) *(feature `sim`)* |
//!
//! Two planes: the **control plane** (small best-effort datagrams — gossip,
//! membership, routing) and the opt-in **data plane** (reliable byte streams —
//! replication, bulk transfer). Both are transport-agnostic; use a bundled
//! binding or implement the trait yourself.
//!
//! ```no_run
//! use groupnet::core::NodeId;
//! use groupnet::runtime::Node;
//! use groupnet::transport::mem::Network;
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
//! Want fewer dependencies? Depend on the underlying crates directly
//! (`groupnet-core`, `groupnet-transport`, …) rather than this facade — the split
//! is what keeps the core dependency-free and the async runtime optional.

/// Sans-IO core: the deterministic [`GroupEngine`](core::GroupEngine), the
/// identity types, weighted [`placement`](core::placement), and the
/// [`wire`](core::wire) protocol. Pure, deterministic, dependency-free.
pub use groupnet_core as core;

/// Transport layer: the control-plane [`Transport`](transport::Transport) trait,
/// the opt-in data-plane [`bulk`](transport::bulk) streams, and the concrete
/// socket / in-memory bindings.
pub mod transport {
    pub use groupnet_transport::{Inbound, Transport};

    /// Data-plane stream transport: `BulkTransport`, `DataStream`, `DataPlane`
    /// *(feature `bulk`)*.
    #[cfg(feature = "bulk")]
    pub use groupnet_transport::bulk;

    /// In-memory control-plane binding, for tests and single-process clusters
    /// *(feature `mem`)*.
    #[cfg(feature = "mem")]
    pub use groupnet_transport_mem as mem;

    /// UDP control-plane binding *(feature `udp`)*.
    #[cfg(feature = "udp")]
    pub use groupnet_transport_udp as udp;

    /// TCP bindings: the persistent-connection control plane
    /// (`TcpMsgTransport`, feature `tcp-msg`) and the data-plane streams
    /// (`TcpBulkTransport`, feature `tcp`).
    #[cfg(any(feature = "tcp", feature = "tcp-msg"))]
    pub use groupnet_transport_tcp as tcp;
}

/// Async runtime: the group-per-task [`Node`](runtime::Node) /
/// [`Group`](runtime::Group) driver and the cluster [`Routing`](runtime::Routing)
/// table.
#[cfg(feature = "runtime")]
pub use groupnet_runtime as runtime;

/// Session-consistency layer over groups: per-writer sequenced write feeds
/// ([`WriteFeed`](consistency::WriteFeed) / [`PeerWrites`](consistency::PeerWrites),
/// loss detected as explicit [`Gap`](consistency::PeerWrite::Gap)s) and
/// applied-write frontiers for read-your-writes barriers
/// ([`Frontier`](consistency::Frontier)). Honest scope: per-writer order and
/// session guarantees — consensus and fencing are deliberately out.
///
/// Two stronger tiers sit on top, each priced separately and each off by
/// default: `consistency-acks` adds applied-watermark acknowledgement
/// (`AckLedger` / `applied_cluster_wide` — writers wait on every alive member),
/// and `consistency-leases` adds the coherence-lease tier above it
/// (`consistency::lease` — readers hold self-expiring leases to *serve*, so a
/// writer's wait ends at an acknowledgement **or** at a silent peer's lease
/// lapse). See the crate's own docs for the scaling envelope of each.
#[cfg(feature = "consistency")]
pub use groupnet_consistency as consistency;

/// Deterministic simulation driver ([`Simulation`](sim::Simulation)) and its
/// seedable PRNG.
#[cfg(feature = "sim")]
pub use groupnet_sim as sim;
