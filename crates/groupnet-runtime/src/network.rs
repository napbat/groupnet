//! Managed node network access and the ordered-session adapters for bulk consumers.

use std::io;

use groupnet_core::NodeId;
use groupnet_network::Router;
use groupnet_network::tunnel::{TunnelTransport, TunneledStream};
use groupnet_transport::bulk::BulkTransport;

use crate::{Endpoint, Node, Ordered};

impl Node {
    /// The node's router, including route inspection and typed adapter registration.
    /// A router clone alone does not retain the managed network's lifetime.
    #[must_use]
    pub fn router(&self) -> &Router {
        self.network.router()
    }

    /// Accesses tunnel peer admission and revocation. Open ordered streams with
    /// `node.endpoint(Ordered::new())` (see [`Ordered`]).
    ///
    /// # Errors
    /// Returns `Unsupported` for a node without configured TLS tunnels.
    pub fn tunnels(&self) -> io::Result<&TunnelTransport> {
        self.network.tunnels()
    }

    /// Closes connections across all node clones and drains owned network tasks.
    /// Group handles may remain readable, but cannot exchange further messages.
    /// Every group actor of the node stops, whatever `Group` handles remain,
    /// and this returns only after each has stopped: no engine of the node
    /// claims, grants or writes to its store after the close. A command on a
    /// remaining handle is refused as
    /// [`CommandRejected::GroupClosed`](crate::CommandRejected::GroupClosed).
    pub async fn close(&self) {
        self.shutdown_protocols();
        self.network.close().await;
        self.close_groups().await;
        self.close_messaging().await;
        self.close_protocols().await;
    }
}

/// Adapter for code generic over [`BulkTransport`] (a
/// [`DataPlane`](groupnet_transport::bulk::DataPlane), an RPC client or a
/// consistency transfer), not an application entry point: applications open
/// ordered sessions with `node.endpoint(Ordered::new())`.
///
/// Each call resolves the node's shared [`Ordered::new`] endpoint — the
/// default [`StreamProtocolId`](crate::StreamProtocolId) — so adapter streams
/// share its accept queue, session ownership and shutdown: a stream connected
/// here is accepted by `endpoint(Ordered::new())?.accept()` and the reverse.
/// A consumer that must not share that queue runs over a protocol-scoped
/// endpoint instead (see [`Endpoint<Ordered>`](Endpoint)). Fails with
/// `Unsupported` without configured tunnels and `NotConnected` after node
/// shutdown.
impl BulkTransport for Node {
    type Error = io::Error;
    type Stream = TunneledStream;

    async fn connect(&self, to: &NodeId) -> io::Result<Self::Stream> {
        self.endpoint(Ordered::new())?.connect(to).await
    }

    async fn accept(&self) -> io::Result<(NodeId, Self::Stream)> {
        self.endpoint(Ordered::new())?.accept().await
    }
}

/// A protocol-scoped ordered endpoint as a [`BulkTransport`], so a
/// [`DataPlane`](groupnet_transport::bulk::DataPlane), an RPC client or server,
/// or any other bulk consumer runs over its own
/// [`StreamProtocolId`](crate::StreamProtocolId) with an independent accept
/// loop beside every other consumer on the node.
///
/// Connects open streams of the endpoint's protocol; accepts take only
/// streams of that protocol, from its isolated accept queue.
///
/// ```no_run
/// use groupnet_runtime::{Node, Ordered, StreamProtocolId};
/// use groupnet_transport::bulk::DataPlane;
///
/// const BLOBS: StreamProtocolId = StreamProtocolId::new(2);
///
/// # async fn demo(node: Node) -> std::io::Result<()> {
/// let blobs = DataPlane::new(node.endpoint(Ordered::protocol(BLOBS))?);
/// let (origin, stream) = blobs.accept().await?;
/// # let _ = (origin, stream);
/// # Ok(())
/// # }
/// ```
impl BulkTransport for Endpoint<Ordered> {
    type Error = io::Error;
    type Stream = TunneledStream;

    async fn connect(&self, to: &NodeId) -> io::Result<Self::Stream> {
        Endpoint::connect(self, to).await
    }

    async fn accept(&self) -> io::Result<(NodeId, Self::Stream)> {
        Endpoint::accept(self).await
    }
}
