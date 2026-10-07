//! # groupnet-transport-tcp
//!
//! Groupnet's TCP bindings — one crate, both planes, each behind its own
//! default-on feature:
//!
//! * **`msg` — control plane.** `TcpMsgTransport` implements the best-effort,
//!   message-oriented [`Transport`] over a bounded pool of **persistent**
//!   connections: dialed lazily on first send, reused, closed when idle,
//!   oldest-evicted at the cap. The constant-connection alternative to the
//!   UDP binding for deployments that want it — see the [`msg`
//!   module](self::msg)-level docs for the exact pooling behaviour.
//! * **`bulk` — data plane.** `TcpBulkTransport` implements `BulkTransport`:
//!   one reliable, ordered byte stream per `connect`, for replication and
//!   bulk transfer. Both ends set `TCP_NODELAY`; inbound identity handshakes
//!   run concurrently under a deadline (`TcpBulkConfig`), so a stalled client
//!   never delays other accepts.
//! * **`link` — router registration** (opt-in): `TcpLink` admits peers through
//!   an explicit application policy (a configured-peer allowlist by default).
//!   Managed sessions are full-duplex, bounded, and retained while connected;
//!   only live admitted sessions become routing neighbors.
//! * **`connectivity` — native TCP paths** (opt-in): the existing message transport
//!   and `TcpLink` can own a `TcpConnection` from `groupnet-transport-punch`,
//!   with rendezvous admission, candidate traversal, and maintained relay fallback.
//!   Use `TcpMsgTransport::bind_connectivity` or `TcpLink::connectivity`; no separate
//!   transport or link type is registered. Native paths retain their protocol MTU
//!   and live admission generations across conversion into a managed link.
//!   `TcpMsgTransport::path_changes` observes direct/relay path changes.
//!
//! `TcpMsgTransport::closed` resolves, stickily, once an endpoint shuts down.
//!
//! The low-level message and bulk APIs retain trusted-topology identity
//! attribution. Managed message admission uses its own bounded wire exchange,
//! with no downgrade to that raw handshake. Open admission is unauthenticated.
//! Every handshake introduces a node id of 1 to
//! `groupnet_transport::MAX_NODE_ID_BYTES` bytes, the bound session admission
//! enforces.
//!
//! [`Transport`]: groupnet_transport::Transport

#[cfg(any(feature = "bulk", feature = "msg"))]
mod handshake;

#[cfg(feature = "bulk")]
mod bulk;
#[cfg(feature = "bulk")]
pub use bulk::{TcpBulkConfig, TcpBulkTransport};

#[cfg(feature = "msg")]
pub mod msg;
#[cfg(feature = "link")]
pub use msg::TcpAdmissionConfig;
#[cfg(feature = "connectivity")]
pub use msg::TcpPathChanges;
#[cfg(feature = "msg")]
pub use msg::{TcpMsgConfig, TcpMsgTransport};

#[cfg(feature = "link")]
mod link;
#[cfg(feature = "msg")]
mod tasks;
#[cfg(feature = "link")]
pub use link::TcpLink;
