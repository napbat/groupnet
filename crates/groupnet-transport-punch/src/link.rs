//! Router-independent native UDP preparation.

use std::io;
use std::sync::Arc;

use groupnet_core::NodeId;
use groupnet_transport::link::{BoundLink, LinkConfig, LinkFuture, LinkProvider};

use crate::{MAX_MESSAGE, PunchConfig, PunchTransport};

/// An authenticated UDP punching configuration bound to its declared identity.
///
/// The router's local identity must match [`PunchConfig::local`]. Registration
/// advertises the native datagram MTU and owns the endpoint's shutdown lifecycle.
#[derive(Debug)]
pub struct PunchLink {
    config: PunchConfig,
    cost: u32,
}

impl PunchLink {
    /// Prepares a direct-preferred or relay-only native UDP endpoint.
    #[must_use]
    pub const fn new(config: PunchConfig) -> Self {
        Self { config, cost: 1 }
    }

    /// Sets the routing cost advertised for this link (one by default).
    #[must_use]
    pub const fn with_cost(mut self, cost: u32) -> Self {
        self.cost = cost;
        self
    }
}

impl LinkProvider for PunchLink {
    fn peers(&self) -> &[NodeId] {
        &self.config.peers
    }

    fn bind(self: Box<Self>, local: NodeId) -> LinkFuture<'static, io::Result<BoundLink>> {
        Box::pin(async move {
            let Self { config, cost } = *self;
            if config.local != local {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "punch link identity does not match the local node",
                ));
            }
            let transport = PunchTransport::bind(config).await?;
            let peers = transport.known_peers();
            let lifecycle = Arc::new(transport.clone());
            Ok(BoundLink::new(
                transport,
                LinkConfig {
                    peers,
                    cost,
                    mtu: MAX_MESSAGE,
                },
            )
            .with_lifecycle(lifecycle))
        })
    }
}
