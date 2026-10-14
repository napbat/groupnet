//! Managed node network access and the ordered-session adapter for bulk consumers.

use std::io;

use groupnet_core::NodeId;
use groupnet_network::Router;
use groupnet_network::tunnel::{TunnelTransport, TunneledStream};
use groupnet_transport::bulk::BulkTransport;

use crate::{Node, Ordered};

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
    pub async fn close(&self) {
        self.shutdown_protocols();
        self.network.close().await;
        self.close_messaging().await;
        self.close_protocols().await;
    }
}

/// Adapter for code generic over [`BulkTransport`] (a
/// [`DataPlane`](groupnet_transport::bulk::DataPlane), an RPC client or a
/// consistency transfer), not an application entry point: applications open
/// ordered sessions with `node.endpoint(Ordered::new())`.
///
/// Each call resolves the node's shared [`Ordered`] endpoint, so adapter
/// streams share its accept queue, session ownership and shutdown: a stream
/// connected here is accepted by `endpoint(Ordered::new())?.accept()` and the
/// reverse. Fails with `Unsupported` without configured tunnels and
/// `NotConnected` after node shutdown.
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
