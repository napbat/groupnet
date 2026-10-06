//! Typed heterogeneous routing and end-to-end tunnels for Groupnet.
//!
//! [`NetworkConfig`] starts registered link providers behind one [`Router`].
//! The router has no protocol-specific dependencies: transport crates own binding
//! and lifecycle. Transit forwarding is enabled by default, including between
//! neighbors on the same adapter. [`RouterConfig::forwarding`] can disable transit for an
//! endpoint-only node. Bounded queues, expiry, and hop limits constrain faults.
//! Address hints update only explicitly admitted adjacent peers; they never
//! grant admission. Closed routers retain routing records, not endpoint resources.
//!
//! Raw routing messages trust the configured fabric and adjacent peers. The
//! [`tunnel`] layer separately authenticates pinned TLS identities and encrypts
//! reliable full-duplex application streams end-to-end across forwarding nodes.
//! A route or shared group name is not authorization to use an application resource.
//!
//! Routing and reliable streams live here. Link implementations live in their own
//! crates and register through `groupnet_transport::link`. The sans-IO coordination
//! core remains dependency-free.

mod config;
mod router;
mod wire;

/// Codec-backed inspection helpers for transport fault/count fixtures.
#[cfg(feature = "test-support")]
pub mod testing;

/// Reliable, pinned, mutually authenticated TLS streams over routed packets.
pub mod tunnel;

pub use config::{Network, NetworkConfig, TunnelConfig};
pub use router::{Route, Router, RouterConfig, TransportId};
