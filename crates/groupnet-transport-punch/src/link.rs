//! Router-independent native UDP preparation.

use std::io;
use std::sync::Arc;

use groupnet_core::NodeId;
use groupnet_transport::link::{BoundLink, LinkConfig, LinkFuture, LinkProvider};

use crate::{MAX_MESSAGE, PunchConfig, PunchTransport};

/// A keyed or explicitly open UDP configuration bound to its declared identity.
///
/// The router's local identity must match [`PunchConfig::local`]. Registration
/// advertises the native datagram MTU and owns the endpoint's shutdown lifecycle.
/// Router neighbors come only from live admitted discovery sessions, including
/// when configured identities seed a keyed endpoint's discovery queries.
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

impl PunchTransport {
    /// Transfers this endpoint into a managed link with its live session registry.
    /// Use this rather than constructing a static `BoundLink` around the endpoint.
    #[must_use]
    pub fn into_bound_link(self, cost: u32) -> BoundLink {
        let config = LinkConfig {
            peers: self.known_peers(),
            cost,
            mtu: MAX_MESSAGE,
        };
        let lifecycle = Arc::new(self.clone());
        let sessions = self.sessions();
        BoundLink::new(self, config)
            .with_lifecycle(lifecycle)
            .with_sessions(sessions)
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
            Ok(transport.into_bound_link(cost))
        })
    }
}
