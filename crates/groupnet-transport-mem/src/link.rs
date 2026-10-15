//! In-process link configuration and identity validation.

use std::io;
use std::num::NonZeroU32;

use groupnet_core::NodeId;
use groupnet_transport::link::{BoundLink, LinkBindError, LinkConfig, LinkFuture, LinkProvider};

use crate::MemTransport;

/// An in-process link that consumes an existing fabric endpoint.
///
/// Available with the `link` feature. The endpoint identity must match the
/// binding node ([`LinkBindError::IdentityMismatch`] otherwise), and the
/// declared peers must pass [`LinkConfig::validate_for`]; both are checked
/// before the link is handed over, and either failure is `InvalidInput`
/// carrying the typed error. Dropping the transport releases its channel
/// registration.
#[derive(Debug)]
pub struct MemLink {
    transport: MemTransport,
    config: LinkConfig,
}

impl MemLink {
    /// Declares the peers directly reachable through this fabric endpoint.
    #[must_use]
    pub fn new(transport: MemTransport, peers: Vec<NodeId>) -> Self {
        Self {
            transport,
            config: LinkConfig::new(peers),
        }
    }

    /// Sets the routing cost of this link (default: one).
    #[must_use]
    pub const fn with_cost(mut self, cost: NonZeroU32) -> Self {
        self.config.cost = cost;
        self
    }
}

impl LinkProvider for MemLink {
    fn peers(&self) -> &[NodeId] {
        &self.config.peers
    }

    fn bind(self: Box<Self>, local: NodeId) -> LinkFuture<'static, io::Result<BoundLink>> {
        Box::pin(async move {
            let Self { transport, config } = *self;
            if transport.local_id() != &local {
                return Err(LinkBindError::IdentityMismatch {
                    configured: transport.local_id().clone(),
                    binding: local,
                }
                .into());
            }
            config.validate_for(&local)?;
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
        assert_eq!(
            error
                .get_ref()
                .and_then(|inner| inner.downcast_ref::<LinkBindError>()),
            Some(&LinkBindError::IdentityMismatch {
                configured: NodeId::new("actual"),
                binding: NodeId::new("different"),
            })
        );
    }

    #[tokio::test]
    async fn provider_validates_cost_and_peers_before_handing_over() {
        use groupnet_transport::link::LinkConfigError;

        let net = Network::new();
        let local = NodeId::new("local");
        for (peers, expected) in [
            (
                vec![NodeId::new("a"), local.clone()],
                LinkConfigError::LocalPeer { index: 1 },
            ),
            (
                vec![NodeId::new("a"), NodeId::new("a")],
                LinkConfigError::DuplicatePeer { index: 1 },
            ),
        ] {
            let link = MemLink::new(net.endpoint(local.clone()), peers);
            let error = Box::new(link)
                .bind(local.clone())
                .await
                .expect_err("invalid peers must fail");
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
            assert_eq!(
                error
                    .get_ref()
                    .and_then(|inner| inner.downcast_ref::<LinkConfigError>()),
                Some(&expected)
            );
        }
        let cost = NonZeroU32::new(7).unwrap();
        let bound = Box::new(MemLink::new(net.endpoint(local.clone()), Vec::new()).with_cost(cost))
            .bind(local)
            .await
            .unwrap();
        assert_eq!(bound.config.cost, cost);
    }
}
