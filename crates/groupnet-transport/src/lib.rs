//! # groupnet-transport
//!
//! Groupnet's transport traits. Two planes, two shapes:
//!
//! * **Control plane** — the [`Transport`] trait below: best-effort,
//!   message-oriented datagrams (gossip, membership, routing). Always available,
//!   with `bytes` as the shared ownership buffer dependency.
//! * **Data plane** — the [`bulk`] module (feature `bulk`): reliable, ordered
//!   byte *streams* for replication and bulk transfer. Opt-in, because it adds
//!   `futures-io` / `zerocopy` — neither of which the control plane needs.
//!
//! Shared by every binding: [`QueueCapacity`] (always available) types every
//! bounded queue capacity, [`validate_node_id`] enforces the routing-identity
//! bound, and the `framing` module (feature `framing`, implied by `bulk`) owns
//! the stream-frame ceiling, the typed length prefix and the partial-write-safe
//! vectored writer. [`Bytes`] is re-exported because [`Inbound`] and the link
//! APIs carry it, so callers need not match the `bytes` version themselves.
//!
//! Bindings for either live in their own `groupnet-transport-*` crates.
//!
//! ## Bindings at a glance
//!
//! | Crate | Endpoint | Construction | Default / custom config | Needs a Tokio runtime |
//! |---|---|---|---|---|
//! | `groupnet-transport-mem` | `Network` → `MemTransport`, `MemBulkNet` → `MemBulkTransport` | synchronous `new()` / `with_config(config)`, then `endpoint(id)` | `NetworkConfig`, `MemBulkConfig` | only to send and receive |
//! | `groupnet-transport-ipc` | `IpcTransport` | synchronous `bind(id, &address)` / `bind_with(id, &address, config)` | `IpcConfig` | yes, at bind (spawns its listener task) |
//! | `groupnet-transport-tcp` | `TcpMsgTransport`, `TcpBulkTransport` | `async` `bind(id, addr)` / `bind_with(id, addr, config)` | `TcpMsgConfig`, `TcpBulkConfig` | yes |
//! | `groupnet-transport-udp` | `UdpTransport` | `async` `bind(id, addr)` | none (one socket) | yes |
//!
//! Memory and IPC construction is deliberately synchronous: neither performs
//! name resolution or a network round trip, so there is nothing to await.
//! An in-memory fabric owns no socket at all, so it is a value built with
//! `new()`/`with_config(..)` rather than bound; IPC binds a local listener
//! immediately and only requires the current Tokio runtime to spawn its
//! worker. TCP and UDP resolve [`ToSocketAddrs`] targets, which is
//! asynchronous. Every custom configuration is validated by the constructor,
//! which returns the configuration's typed error (or an `io::Error` wrapping
//! it at socket boundaries) instead of correcting a value.
//!
//! [`ToSocketAddrs`]: https://docs.rs/tokio/latest/tokio/net/trait.ToSocketAddrs.html
//!
//! ## Contract
//!
//! Delivery is **best-effort**. Messages MAY be dropped, reordered, or
//! duplicated — the [`GroupEngine`] tolerates all three. Do **not** add your own
//! reliability or ordering layer: it's wasted work, and it would defeat the
//! whole point of being bindable to UDP or a shared-memory ring. `send`
//! returning `Ok` means "handed off", not "delivered".
//!
//! The engine speaks only in [`NodeId`]s. A `Transport` owns the mapping from
//! `NodeId` to a concrete address (socket, path, ring slot) and typically learns
//! new bindings from the source of inbound messages.
//!
//! ## Why `impl Future`, not `async fn`
//!
//! We use return-position `impl Future<..> + Send` rather than `async fn` in the
//! trait so the returned futures are guaranteed `Send` and usable from a
//! multi-threaded runtime — with **zero** dependency on `async-trait` or its
//! boxing.
//!
//! [`GroupEngine`]: groupnet_core::GroupEngine

pub use bytes::Bytes;

mod identity;
mod transport;

pub use identity::{MAX_NODE_ID_BYTES, NodeIdError, validate_node_id};
pub use transport::{Inbound, Transport};

pub mod capacity;

pub use capacity::{QueueCapacity, QueueCapacityError};

#[cfg(feature = "framing")]
pub mod framing;

/// Data-plane stream transport (feature `bulk`): `BulkTransport`, `DataStream`,
/// `DataPlane`.
#[cfg(feature = "bulk")]
pub mod bulk;

/// Protocol-neutral link registration, workers, and lifecycle (feature `link`).
#[cfg(feature = "link")]
pub mod link;

/// Application admission and live adjacent-peer sessions (feature `link`).
#[cfg(feature = "link")]
pub mod admission;

#[cfg(doc)]
use groupnet_core::NodeId;
