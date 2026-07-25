//! The control-plane contract: best-effort datagrams, addressed by
//! [`NodeId`], with optional address learning.

use std::error::Error;
use std::future::Future;

use groupnet_core::NodeId;

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

    /// Teaches the transport that `node` claims to be reachable at `addr` —
    /// the exact string the peer advertised (the runtime feeds gossiped
    /// `advertise_addr` values through here automatically, so only seeds need
    /// out-of-band addressing).
    ///
    /// The default does nothing: bindings that resolve peers another way (an
    /// in-memory fabric, fixed infrastructure) may ignore advertisements.
    /// Address-book-backed bindings parse the string and register it,
    /// silently ignoring what they cannot parse — an advertisement is a hint,
    /// never an error.
    fn learn_peer(&self, node: &NodeId, addr: &str) {
        let _ = (node, addr);
    }
}
