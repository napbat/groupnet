//! Typed adapter configuration and a node-owned network lifecycle.

use std::collections::BTreeSet;
use std::io;
use std::sync::Arc;

use crate::tunnel::{PeerIdentity, TlsIdentity, TunnelTransport, TunneledStream};
use crate::{Router, RouterConfig};
use groupnet_core::NodeId;
use groupnet_transport::{bulk::BulkTransport, link::LinkProvider};

/// End-to-end encrypted streams with explicit, bidirectional peer admission.
/// Certificate pins authenticate peers and allow tunnels; they do not grant
/// application permissions or permission-group membership.
#[derive(Debug)]
pub struct TunnelConfig {
    identity: TlsIdentity,
    peers: Vec<PeerIdentity>,
}

impl TunnelConfig {
    /// Configures this node's TLS identity and admitted peers' certificate pins.
    #[must_use]
    pub fn new(identity: TlsIdentity, peers: impl IntoIterator<Item = PeerIdentity>) -> Self {
        Self {
            identity,
            peers: peers.into_iter().collect(),
        }
    }
}

/// A managed group network: intrinsic routing over a collection of link providers.
///
/// [`with_link`](Self::with_link) and [`with_links`](Self::with_links) register any
/// implementation through the same contract, in insertion order. Protocol crates
/// own their configuration and binding. Configure [`with_tunnels`](Self::with_tunnels)
/// for confidential streams; link admission never authorizes application resources.
#[derive(Debug, Default)]
pub struct NetworkConfig {
    router: RouterConfig,
    links: Vec<Box<dyn LinkProvider>>,
    tunnels: Option<TunnelConfig>,
}

impl NetworkConfig {
    /// Sets routing limits and forwarding policy; forwarding is enabled by default.
    #[must_use]
    pub fn with_router(mut self, config: RouterConfig) -> Self {
        self.router = config;
        self
    }

    /// Adds a configured link implementation, subject to routing capacity limits.
    #[must_use]
    pub fn with_link(mut self, provider: impl LinkProvider) -> Self {
        self.links.push(Box::new(provider));
        self
    }

    /// Adds heterogeneous link implementations in iteration order.
    #[must_use]
    pub fn with_links(
        mut self,
        providers: impl IntoIterator<Item = Box<dyn LinkProvider>>,
    ) -> Self {
        self.links.extend(providers);
        self
    }

    /// Enables pinned, mutually authenticated TLS streams over the routed fabric.
    #[must_use]
    pub fn with_tunnels(mut self, config: TunnelConfig) -> Self {
        self.tunnels = Some(config);
        self
    }

    /// The configured adjacent peers, deduplicated for initial membership seeds.
    #[must_use]
    pub fn peers(&self) -> Vec<NodeId> {
        let mut peers = BTreeSet::new();
        for link in &self.links {
            peers.extend(link.peers().iter().cloned());
        }
        peers.into_iter().collect()
    }

    /// Binds adapters in insertion order and starts routing, rolling back on failure.
    ///
    /// # Errors
    /// Propagates invalid configuration, bind/initialization errors, and TLS errors.
    /// # Panics
    /// Requires a Tokio runtime. Propagates an earlier poisoned adapter lock.
    pub async fn bind(self, local: NodeId) -> io::Result<Network> {
        let mut network = NetworkInner {
            router: Router::new(local.clone(), self.router)?,
            tunnels: None,
        };
        let started = async {
            for provider in self.links {
                let link = provider.bind(local.clone()).await?;
                network.router.add_link(link).await?;
            }
            network.tunnels = self
                .tunnels
                .map(|config| {
                    TunnelTransport::new(network.router.clone(), config.identity, config.peers)
                })
                .transpose()?;
            Ok::<_, io::Error>(())
        }
        .await;
        if let Err(error) = started {
            network.close().await;
            return Err(error);
        }
        Ok(Network {
            inner: Arc::new(network),
        })
    }
}

#[derive(Debug)]
struct NetworkInner {
    router: Router,
    tunnels: Option<TunnelTransport>,
}

impl NetworkInner {
    async fn close(&self) {
        if let Some(tunnels) = &self.tunnels {
            tunnels.close().await;
        }
        self.router.close().await;
    }
}

impl Drop for NetworkInner {
    fn drop(&mut self) {
        self.router.shutdown();
    }
}

/// Shared network lifetime, also usable directly as a secure [`BulkTransport`].
/// Dropping the final clone initiates shutdown; [`close`](Self::close) drains tasks.
/// Owned link workers drain each provider's lifecycle through the shared contract.
#[derive(Clone, Debug)]
pub struct Network {
    inner: Arc<NetworkInner>,
}

impl Network {
    /// The initialized router, also usable for registering additional typed adapters.
    #[must_use]
    pub fn router(&self) -> &Router {
        &self.inner.router
    }

    /// The encrypted stream endpoint, including peer admission and revocation.
    ///
    /// # Errors
    /// Returns `Unsupported` when secure tunnels were not configured.
    pub fn tunnels(&self) -> io::Result<&TunnelTransport> {
        self.inner.tunnels.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "secure tunnels are not configured",
            )
        })
    }

    /// Shuts down encrypted sessions, routing, and all owned connection tasks.
    /// Closing any clone closes the network for every clone.
    pub async fn close(&self) {
        self.inner.close().await;
    }
}

impl BulkTransport for Network {
    type Error = io::Error;
    type Stream = TunneledStream;

    async fn connect(&self, to: &NodeId) -> io::Result<Self::Stream> {
        self.tunnels()?.connect(to).await
    }

    async fn accept(&self) -> io::Result<(NodeId, Self::Stream)> {
        self.tunnels()?.accept().await
    }
}
