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
//!   bulk transfer.
//!
//! Both planes attribute connections with the same one-line node-id
//! handshake, because a TCP source address (ephemeral port) cannot identify
//! a peer the way a bound UDP source address can.
//!
//! [`Transport`]: groupnet_transport::Transport

#[cfg(any(feature = "bulk", feature = "msg"))]
mod handshake;

#[cfg(feature = "bulk")]
mod bulk;
#[cfg(feature = "bulk")]
pub use bulk::TcpBulkTransport;

#[cfg(feature = "msg")]
pub mod msg;
#[cfg(feature = "msg")]
pub use msg::{TcpMsgConfig, TcpMsgTransport};
