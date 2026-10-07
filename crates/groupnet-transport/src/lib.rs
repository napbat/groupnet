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
//! bounded queue capacity, and the `framing` module (feature `framing`, implied
//! by `bulk`) owns the stream-frame ceiling, the typed length prefix and the
//! partial-write-safe vectored writer.
//!
//! Bindings for either live in their own `groupnet-transport-*` crates.
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

mod transport;

pub use transport::{Inbound, Transport};

pub mod capacity;

pub use capacity::QueueCapacity;

#[cfg(feature = "framing")]
pub mod framing;

/// Largest routing identity ([`NodeId`]) any adapter admits or introduces,
/// in bytes. An empty identity is never valid either.
pub const MAX_NODE_ID_BYTES: usize = 255;

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
