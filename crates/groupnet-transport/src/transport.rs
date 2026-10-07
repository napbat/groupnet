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
    pub msg: bytes::Bytes,
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

    /// Sends only to the admission generation selected by the router.
    /// Static transports use the default, which drops generation-tagged sends.
    /// Dynamic adapters must override this, reject missing or stale generations,
    /// and retain the same generation through all pending physical writes rather
    /// than resolving the ID again.
    #[cfg(feature = "link")]
    fn send_admitted(
        &self,
        to: &NodeId,
        msg: &[u8],
        session: Option<crate::admission::SessionId>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async move {
            if session.is_some() {
                return Ok(());
            }
            self.send(to, msg).await
        }
    }

    /// Transfers an owned frame to the admission generation selected by the router.
    /// Adapters with queued writes override this to retain the allocation; the
    /// default borrows it for the duration of the existing admitted send.
    #[cfg(feature = "link")]
    fn send_owned_admitted(
        &self,
        to: &NodeId,
        msg: bytes::Bytes,
        session: Option<crate::admission::SessionId>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async move { self.send_admitted(to, &msg, session).await }
    }

    /// Awaits the next inbound datagram. Returning `Err` ends the receive loop.
    fn recv(&self) -> impl Future<Output = Result<Inbound, Self::Error>> + Send;

    /// Receives a frame with the admission generation captured by its producer.
    /// Static transports use the default untagged receive. Dynamic transports
    /// must override this and preserve the producing generation through queues.
    #[cfg(feature = "link")]
    fn recv_admitted(
        &self,
    ) -> impl Future<Output = Result<crate::link::AdmittedInbound, Self::Error>> + Send {
        async {
            self.recv()
                .await
                .map(|packet| crate::link::AdmittedInbound {
                    packet,
                    session: None,
                })
        }
    }

    /// Offers the registering network's synchronous inbound sink when a link
    /// worker starts. A transport whose reader tasks can call it may retain it
    /// and deliver admitted frames directly, saving the worker's receive hop;
    /// it must stop reading input while a delivery runs, so its own bounds
    /// still apply, and it must keep the producing generation on every frame.
    /// Frames still returned by [`recv_admitted`](Self::recv_admitted) keep
    /// flowing through the worker. The default ignores the sink.
    #[cfg(feature = "link")]
    fn attach_inbound(&self, sink: crate::link::InboundSink) {
        let _ = sink;
    }

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
