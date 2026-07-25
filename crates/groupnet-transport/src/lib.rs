//! # groupnet-transport
//!
//! Groupnet's transport traits. Two planes, two shapes:
//!
//! * **Control plane** — the [`Transport`] trait below: best-effort,
//!   message-oriented datagrams (gossip, membership, routing). Always available,
//!   and this crate is **dependency-free** at the default feature set.
//! * **Data plane** — the [`bulk`] module (feature `bulk`): reliable, ordered
//!   byte *streams* for replication and bulk transfer. Opt-in, because it pulls
//!   `futures-io` / `bytes` / `zerocopy` — none of which the control plane needs.
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

/// Data-plane stream transport (feature `bulk`): `BulkTransport`, `DataStream`,
/// `DataPlane`.
#[cfg(feature = "bulk")]
pub mod bulk;

#[cfg(doc)]
use groupnet_core::NodeId;
