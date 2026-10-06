//! Managed node network access and bulk streams.

use std::io;

use groupnet_core::NodeId;
use groupnet_transport::bulk::BulkTransport;
use groupnet_transport_router::Router;
use groupnet_transport_router::tunnel::{TunnelTransport, TunneledStream};

use crate::Node;

impl Node {
    /// The node's router, including route inspection and typed adapter registration.
    /// A router clone alone does not retain the managed network's lifetime.
    #[must_use]
    pub fn router(&self) -> &Router {
        self.network.router()
    }

    /// Accesses tunnel peer admission and revocation; use [`BulkTransport`] to stream.
    ///
    /// # Errors
    /// Returns `Unsupported` for a node without configured TLS tunnels.
    pub fn tunnels(&self) -> io::Result<&TunnelTransport> {
        self.network.tunnels()
    }

    /// Closes connections across all node clones and drains owned network tasks.
    /// Group handles may remain readable, but cannot exchange further messages.
    pub async fn close(&self) {
        self.network.close().await;
    }
}

impl BulkTransport for Node {
    type Error = io::Error;
    type Stream = TunneledStream;

    async fn connect(&self, to: &NodeId) -> io::Result<Self::Stream> {
        self.tunnels()?.connect(to).await
    }

    async fn accept(&self) -> io::Result<(NodeId, Self::Stream)> {
        self.tunnels()?.accept().await
    }
}
