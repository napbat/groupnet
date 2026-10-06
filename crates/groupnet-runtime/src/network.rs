//! Node-owned typed multi-transport initialization (feature `router`).

use std::io;

use groupnet_core::NodeId;
use groupnet_transport::{bulk::BulkTransport, link::LinkProvider};
use groupnet_transport_router::tunnel::{TunnelTransport, TunneledStream};
use groupnet_transport_router::{NetworkConfig, Router, RouterConfig, TunnelConfig};

use crate::{Node, NodeBuilder};

impl Node<Router> {
    /// Builds a managed node with intrinsic routing over registered link providers.
    #[must_use]
    pub fn network_builder(id: NodeId) -> NetworkBuilder {
        NetworkBuilder {
            id,
            config: NetworkConfig::default(),
        }
    }

    /// Initializes adapters, routing, optional TLS tunnels, and membership.
    /// Adjacent peers become initial membership seeds. Every ordinary [`Node`]
    /// clone retains network ownership; dropping the last initiates shutdown.
    /// With tunnels configured, the node is also a secure [`BulkTransport`].
    ///
    /// # Errors
    /// Rejects invalid configuration, failed binds, or failed security setup.
    /// # Panics
    /// Requires a Tokio runtime; propagates an earlier poisoned adapter lock.
    pub async fn network(id: NodeId, config: NetworkConfig) -> io::Result<Self> {
        Self::network_with(id, config, |builder| builder).await
    }

    /// Initializes a network-backed node with custom membership builder settings.
    /// Reuses the complete typed [`NodeBuilder`] API instead of duplicating settings
    /// in the transport layer. Clones retain the same lifetime as [`Self::network`].
    ///
    /// # Errors
    /// Rejects invalid configuration, failed binds, or failed security setup.
    /// # Panics
    /// Requires a Tokio runtime; propagates poisoned locks or a panicking callback.
    pub async fn network_with(
        id: NodeId,
        config: NetworkConfig,
        configure: impl FnOnce(NodeBuilder<Router>) -> NodeBuilder<Router>,
    ) -> io::Result<Self> {
        let peers = config.peers();
        let network = config.bind(id.clone()).await?;
        let mut builder = Self::builder(id, network.router().clone());
        for peer in peers {
            builder = builder.seed(peer);
        }
        let mut node = configure(builder).spawn();
        node.network = Some(network);
        Ok(node)
    }

    /// The node's router, including route inspection and typed adapter registration.
    /// A router clone alone does not retain the managed network's lifetime.
    #[must_use]
    pub fn router(&self) -> &Router {
        self.transport()
    }

    /// Accesses tunnel peer admission and revocation; use [`BulkTransport`] to stream.
    ///
    /// # Errors
    /// Returns `Unsupported` for a node without configured, managed TLS tunnels.
    pub fn tunnels(&self) -> io::Result<&TunnelTransport> {
        self.network
            .as_ref()
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::Unsupported, "node has no managed network")
            })?
            .tunnels()
    }

    /// Closes connections across all node clones and drains owned network tasks.
    /// Group handles may remain readable, but cannot exchange further messages.
    /// A low-level node built from a bare router closes that router instead.
    pub async fn close(&self) {
        if let Some(network) = &self.network {
            network.close().await;
        } else {
            self.router().close().await;
        }
    }
}

impl BulkTransport for Node<Router> {
    type Error = io::Error;
    type Stream = TunneledStream;

    async fn connect(&self, to: &NodeId) -> io::Result<Self::Stream> {
        self.tunnels()?.connect(to).await
    }

    async fn accept(&self) -> io::Result<(NodeId, Self::Stream)> {
        self.tunnels()?.accept().await
    }
}

/// Configures link implementations beneath a node's routing and membership layer.
/// Link providers own their protocol settings; no protocol enumeration is required.
#[derive(Debug)]
pub struct NetworkBuilder {
    id: NodeId,
    config: NetworkConfig,
}

impl NetworkBuilder {
    /// Registers one configured protocol implementation.
    #[must_use]
    pub fn link(mut self, provider: impl LinkProvider) -> Self {
        self.config = self.config.with_link(provider);
        self
    }

    /// Registers a heterogeneous collection of configured protocol implementations.
    #[must_use]
    pub fn links(mut self, providers: impl IntoIterator<Item = Box<dyn LinkProvider>>) -> Self {
        self.config = self.config.with_links(providers);
        self
    }

    /// Sets routing limits and transit policy, independently of link protocols.
    #[must_use]
    pub fn routing(mut self, config: RouterConfig) -> Self {
        self.config = self.config.with_router(config);
        self
    }

    /// Enables pinned, end-to-end TLS streams over routed paths.
    #[must_use]
    pub fn tunnels(mut self, config: TunnelConfig) -> Self {
        self.config = self.config.with_tunnels(config);
        self
    }

    /// Binds all links and starts routing and membership.
    ///
    /// # Errors
    /// Propagates binding, registration, or security setup failures.
    /// # Panics
    /// Requires a Tokio runtime; propagates poisoned internal locks.
    pub async fn start(self) -> io::Result<Node<Router>> {
        Node::network(self.id, self.config).await
    }

    /// Starts with the existing membership builder's settings, without duplicating them.
    ///
    /// # Errors
    /// Propagates binding, registration, or security setup failures.
    /// # Panics
    /// Requires a Tokio runtime; propagates poisoned locks or a panicking callback.
    pub async fn start_with(
        self,
        configure: impl FnOnce(NodeBuilder<Router>) -> NodeBuilder<Router>,
    ) -> io::Result<Node<Router>> {
        Node::network_with(self.id, self.config, configure).await
    }
}
