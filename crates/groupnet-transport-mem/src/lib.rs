//! # groupnet-transport-mem
//!
//! An in-process [`Transport`] over Tokio channels — a concrete binding of
//! Groupnet's transport-agnostic trait.
//!
//! Useful for integration tests, examples, and single-process clusters: stand up
//! several nodes on one [`Network`] and they gossip as if over a real link,
//! minus the sockets. It honours the best-effort contract (sending to an unknown
//! peer is a silent drop, never an error).
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

mod mem;

pub use mem::{Closed, MemTransport, Network};
