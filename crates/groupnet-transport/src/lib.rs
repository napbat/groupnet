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

use std::error::Error;
use std::future::Future;

use groupnet_core::NodeId;

/// Data-plane stream transport (feature `bulk`): `BulkTransport`, `DataStream`,
/// `DataPlane`.
#[cfg(feature = "bulk")]
pub mod bulk;

/// A message received from a peer.
#[derive(Clone, Debug)]
pub struct Inbound {
    /// The node that sent it (as the transport resolved the source).
    pub from: NodeId,
    /// The opaque frame; hand it to `GroupEngine::on_message`.
    pub msg: Vec<u8>,
}

/// A pluggable, best-effort, message-oriented transport.
///
/// Implement this for TCP, UDP, IPC, shared memory, or an in-process test
/// harness. See the crate docs for the delivery contract.
pub trait Transport: Send + Sync + 'static {
    /// Transport-specific error type surfaced by [`recv`](Self::recv). `send`
    /// failures are usually swallowed (best-effort), but `recv` returning `Err`
    /// signals the transport is shut down and the driver should stop.
    type Error: Error + Send + Sync + 'static;

    /// Fires a single datagram at `to`. `Ok` means handed off, not delivered.
    /// Unknown/unreachable peers should be treated as a drop (`Ok`), not an
    /// error, unless the transport itself has failed.
    fn send(&self, to: &NodeId, msg: &[u8])
    -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Awaits the next inbound datagram. Returning `Err` ends the receive loop.
    fn recv(&self) -> impl Future<Output = Result<Inbound, Self::Error>> + Send;
}
