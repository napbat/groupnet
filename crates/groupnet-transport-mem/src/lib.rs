//! # groupnet-transport-mem
//!
//! In-process bindings of Groupnet's transport traits over Tokio channels and
//! pipes — concrete implementations of transport-agnostic traits, with the
//! sockets left out.
//!
//! Useful for integration tests, examples, and single-process clusters: stand
//! up several nodes on one fabric and they talk as if over a real link. That
//! makes this a product surface, not scaffolding — the same code a consumer
//! runs against TCP runs here, unchanged.
//!
//! * **Control plane** (always available): [`Network`] hands out
//!   [`MemTransport`] endpoints implementing [`Transport`]. It honours the
//!   best-effort contract — sending to an unknown peer is a silent drop, never
//!   an error.
//! * **Data plane** (feature `bulk`): `MemBulkNet` hands out
//!   `MemBulkTransport` endpoints implementing `BulkTransport` — reliable,
//!   ordered byte streams over `tokio::io::duplex`. Being connection-oriented,
//!   it reports an unknown peer as an error rather than dropping.
//!
//! ```
//! use groupnet_transport_mem::Network;
//! use groupnet_core::NodeId;
//!
//! let net = Network::new();
//! let _a = net.endpoint(NodeId::new("node-a"));
//! let _b = net.endpoint(NodeId::new("node-b"));
//! ```
//!
//! [`Transport`]: groupnet_transport::Transport

#[cfg(feature = "bulk")]
pub mod bulk;
mod mem;

#[cfg(feature = "bulk")]
pub use bulk::{MemBulkNet, MemBulkTransport};
pub use mem::{Closed, MemTransport, Network};
