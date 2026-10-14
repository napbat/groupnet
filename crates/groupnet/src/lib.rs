//! # Groupnet
//!
//! A deterministic, leaderless coordination fabric for systems that partition
//! state into shard groups. This umbrella crate re-exports the layered crates as
//! one namespaced hierarchy: every module mirrors exactly one underlying crate,
//! and nothing is re-exported twice at the root.
//!
//! | Module | Crate | Role | Feature |
//! |--------|-------|------|---------|
//! | [`core`] | `groupnet-core` | sans-IO state machine, identity, weighted [`placement`](core::placement), and the [`wire`](core::wire) protocol — pure, deterministic, dependency-free | always |
//! | [`transport`] | `groupnet-transport` + bindings | the control-plane [`Transport`](transport::Transport) trait, shared [`Bytes`](transport::Bytes) and [`QueueCapacity`](transport::QueueCapacity), and the bindings `mem` / `tcp` / `udp` / `ipc` as submodules | always; bindings by name |
//! | `connectivity` | `groupnet-transport-punch` | native traversal and rendezvous servers under TCP/UDP links | `connectivity` |
//! | `network` | `groupnet-network` | protocol-independent routing and pinned end-to-end TLS tunnels | `runtime` |
//! | `messaging` | `groupnet-messaging` | opaque application frames, delivery receipts, and the messaging engine | `runtime` |
//! | `streams` | `groupnet-streams` | ordered streams and reliable/unreliable unordered sessions | `runtime` |
//! | `runtime` | `groupnet-runtime` | the managed `Node` / `Group` driver, typed `Endpoint` / `Peer` handles, and receive callbacks (`runtime::messaging`) | `runtime` |
//! | `rpc` | `groupnet-rpc` | request/response calls over any data plane | `rpc` |
//! | `consistency` | `groupnet-consistency` | write feeds, frontiers, floors, acknowledgements, leases, hosted writes, handoff, replication | `consistency`, `consistency-*` |
//! | `sim` | `groupnet-sim` | deterministic single-threaded engine harness | `sim` |
//!
//! Name each type through the module of the crate that owns it:
//! `groupnet::core::NodeId`, `groupnet::runtime::{Node, Messages, Ordered}`,
//! `groupnet::messaging::SendOptions`, `groupnet::network::RouterConfig`,
//! `groupnet::streams::UnorderedDelivery`, and `groupnet::transport::Bytes` for
//! payloads. (Lower-layer crates' signature types are also re-exported by the
//! crates that use them, for direct dependents of those crates.)
//!
//! # Features
//!
//! Features come in two kinds. **Bindings** choose transport crates: `mem`,
//! `tcp`, `udp`, `ipc`. **Capabilities** choose planes: `runtime` (managed
//! nodes with routing, messages, ordered/unordered sessions and link
//! registration; it always includes the stream and tunnel machinery), `bulk`
//! (data-plane streams) and `connectivity` (native traversal under TCP/UDP
//! links). Every selected binding carries every selected capability: `tcp` with
//! `bulk` provides `TcpBulkTransport`, `mem` with `bulk` provides `MemBulkNet`,
//! and any binding with `runtime` provides its managed link.
//!
//! | Feature | Adds | Implies |
//! |---------|------|---------|
//! | `runtime` *(default)* | `network`, `messaging`, `streams`, `runtime`; `transport::link` / `transport::admission` | `bulk` |
//! | `mem` *(default)* | `transport::mem` | — |
//! | `tcp` | `transport::tcp` (`TcpMsgTransport`; `TcpBulkTransport` with `bulk`; `TcpLink` with `runtime`) | — |
//! | `udp`, `ipc` | `transport::udp`, `transport::ipc` | — |
//! | `bulk` | `transport::bulk`, `transport::framing` | — |
//! | `connectivity` | `connectivity`; native modes of `TcpLink` / `UdpLink` | — |
//! | `dns` | `runtime::SystemResolver` | `runtime` |
//! | `rpc` | `rpc` | `bulk` |
//! | `consistency` | `consistency` (feeds, frontiers, floors) | `runtime` |
//! | `consistency-acks`, `-leases`, `-hosted`, `-handoff`, `-replication`, `-volatile-recovery`, `-volatile-bootstrap-bulk` | the matching `consistency` tier | `consistency`; `-leases` → `-acks`; `-handoff` → `-hosted` + `bulk`; `-volatile-bootstrap-bulk` → `-volatile-recovery` + `bulk` |
//! | `sim` | `sim` | — |
//!
//! Minimal builds keep one dependency on this facade:
//!
//! ```toml
//! # Sans-IO core, placement, and the transport traits only.
//! groupnet = { version = "0.1", default-features = false }
//! # Reliable TCP streams and RPC without a managed node.
//! groupnet = { version = "0.1", default-features = false, features = ["tcp", "rpc"] }
//! ```
//!
//! # Example
//!
//! ```no_run
//! # #[cfg(all(feature = "runtime", feature = "mem"))]
//! # mod example {
//! use groupnet::core::NodeId;
//! use groupnet::network::RouterConfig;
//! use groupnet::runtime::Node;
//! use groupnet::transport::mem::{MemLink, Network};
//!
//! # async fn demo() -> std::io::Result<()> {
//! let net = Network::new();
//! let id = NodeId::new("node-a");
//! let node = Node::builder(id.clone())
//!     .link(MemLink::new(net.endpoint(id), vec![NodeId::new("node-b")]))
//!     .routing(RouterConfig::default())
//!     .start().await?;
//!
//! let group = node.join_group("shard-42");
//! if group.is_coordinator() {
//!     group.sync(|ctx| ctx.update_metadata("routing", "v3"));
//! }
//! # Ok(())
//! # }
//! # }
//! ```
#![cfg_attr(docsrs, feature(doc_cfg))]

pub use groupnet_core as core;

/// The transport layer: the control-plane [`Transport`](transport::Transport)
/// trait, shared [`Bytes`](transport::Bytes) payloads and
/// [`QueueCapacity`](transport::QueueCapacity) bounds, the data-plane `bulk`
/// streams and `framing` (feature `bulk`), link registration and admission
/// (`link` / `admission`, feature `runtime` or any binding's managed link), and
/// the concrete bindings as submodules.
pub mod transport {
    pub use groupnet_transport::*;

    #[cfg(feature = "ipc")]
    pub use groupnet_transport_ipc as ipc;
    #[cfg(feature = "mem")]
    pub use groupnet_transport_mem as mem;
    #[cfg(feature = "tcp")]
    pub use groupnet_transport_tcp as tcp;
    #[cfg(feature = "udp")]
    pub use groupnet_transport_udp as udp;
}

#[cfg(feature = "connectivity")]
pub use groupnet_transport_punch as connectivity;

#[cfg(feature = "runtime")]
pub use groupnet_messaging as messaging;
#[cfg(feature = "runtime")]
pub use groupnet_network as network;
#[cfg(feature = "runtime")]
pub use groupnet_runtime as runtime;
#[cfg(feature = "runtime")]
pub use groupnet_streams as streams;

#[cfg(feature = "rpc")]
pub use groupnet_rpc as rpc;

#[cfg(feature = "consistency")]
pub use groupnet_consistency as consistency;

#[cfg(feature = "sim")]
pub use groupnet_sim as sim;

/// Every Rust snippet in the repository README, compiled as a doctest so the
/// primary onboarding path cannot drift from the API.
#[cfg(all(
    doctest,
    feature = "runtime",
    feature = "mem",
    feature = "tcp",
    feature = "udp",
    feature = "connectivity",
    feature = "rpc",
    feature = "consistency-acks",
))]
#[doc = include_str!("../../../README.md")]
pub struct ReadmeDoctests;
