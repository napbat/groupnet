//! In-process link configuration and identity validation.

use std::io;

use groupnet_core::NodeId;
use groupnet_transport::link::{BoundLink, LinkConfig, LinkFuture, LinkProvider};

use crate::MemTransport;

/// An in-process link that consumes an existing fabric endpoint.
///
/// Available with the `link` feature. The endpoint identity must match the
/// binding node; dropping the transport releases its channel registration.
#[derive(Debug)]
pub struct MemLink {
    transport: MemTransport,
    peers: Vec<NodeId>,
    cost: u32,
}

impl MemLink {
    /// Declares the peers directly reachable through this fabric endpoint.
    #[must_use]
    pub const fn new(transport: MemTransport, peers: Vec<NodeId>) -> Self {
        Self {
            transport,
            peers,
            cost: 1,
        }
    }

    /// Sets the routing cost of this link (default: one).
    #[must_use]
    pub const fn with_cost(mut self, cost: u32) -> Self {
        self.cost = cost;
        self
    }
}

impl LinkProvider for MemLink {
    fn peers(&self) -> &[NodeId] {
        &self.peers
    }

    fn bind(self: Box<Self>, local: NodeId) -> LinkFuture<'static, io::Result<BoundLink>> {
        Box::pin(async move {
            let Self {
                transport,
                peers,
                cost,
            } = *self;
            if transport.local_id() != &local {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "memory endpoint identity does not match the binding node",
                ));
            }
            let mut config = LinkConfig::new(peers);
            config.cost = cost;
            Ok(BoundLink::new(transport, config))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Network;

    #[tokio::test]
    async fn provider_rejects_mismatched_endpoint_identity() {
        let net = Network::new();
        let link = MemLink::new(net.endpoint(NodeId::new("actual")), Vec::new());
        let error = Box::new(link)
            .bind(NodeId::new("different"))
            .await
            .expect_err("identity mismatch must fail");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }
}
