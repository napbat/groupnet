//! Typed heterogeneous routing and end-to-end tunnels for Groupnet.
//!
//! [`NetworkConfig`] starts any number of IPC, TCP, UDP, native hole-punching,
//! or custom adapters behind one [`Router`]. Transit forwarding is enabled by
//! default, including between neighbors on the same adapter; peers need not share
//! a physical transport. [`RouterConfig::forwarding`] can disable transit for an
//! endpoint-only node. Bounded queues, expiry, and hop limits constrain faults.
//!
//! Raw routing messages trust the configured fabric and adjacent peers. The
//! [`tunnel`] layer separately authenticates pinned TLS identities and encrypts
//! reliable full-duplex application streams end-to-end across forwarding nodes.
//! A route or shared group name is not authorization to use an application resource.
//!
//! Native networking lives here, not in an external P2P/QUIC stack. The only new
//! production dependencies beyond Groupnet's existing I/O stack supply audited
//! cryptography/TLS. The sans-IO core remains dependency-free.

mod config;
mod router;
mod wire;

/// Local-only Unix socket and Windows named-pipe adapters.
pub mod ipc;
/// Authenticated UDP discovery, native hole-punching, and self-hosted relay fallback.
pub mod punch;
/// Reliable, pinned, mutually authenticated TLS streams over routed packets.
pub mod tunnel;

pub use config::{Network, NetworkConfig, PeerEndpoint, TransportConfig, TunnelConfig};
pub use router::{LinkConfig, Route, Router, RouterConfig, TransportId};
