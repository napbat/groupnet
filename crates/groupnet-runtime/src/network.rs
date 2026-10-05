//! Node-owned typed multi-transport initialization (feature `router`).

use std::io;
use std::ops::Deref;

use groupnet_core::NodeId;
use groupnet_transport_router::{Network, NetworkConfig, Router};

use crate::{Node, NodeBuilder};

/// A Groupnet node which owns its configured connections and routing layer.
///
/// The ordinary Node API is available through dereferencing. Keep this owner (or
/// a clone) alive while using the node; a separately cloned low-level `Node`
/// handle does not extend network ownership. Dropping the final owner initiates
/// connection shutdown; [`close`](Self::close) waits for network tasks to drain.
#[derive(Clone, Debug)]
pub struct NetworkNode {
    node: Node<Router>,
    connections: Network,
}

impl Node<Router> {
    /// Initializes configured adapters, routing, optional tunnels, and membership.
    /// Adjacent peers are automatically used as initial membership seeds.
    ///
    /// # Errors
    /// Rejects invalid configuration, failed binds, or failed security setup.
    /// # Panics
    /// Requires a Tokio runtime; propagates an earlier poisoned adapter lock.
    pub async fn network(id: NodeId, connections: NetworkConfig) -> io::Result<NetworkNode> {
        Self::network_with(id, connections, |builder| builder).await
    }

    /// Initializes a network-backed node with custom membership builder settings.
    /// This preserves the complete typed [`NodeBuilder`] API without duplicating its
    /// configuration fields in the transport layer.
    ///
    /// # Errors
    /// Rejects invalid configuration, failed binds, or failed security setup.
    /// # Panics
    /// Requires a Tokio runtime; propagates poisoned locks or a panicking callback.
    pub async fn network_with(
        id: NodeId,
        config: NetworkConfig,
        configure: impl FnOnce(NodeBuilder<Router>) -> NodeBuilder<Router>,
    ) -> io::Result<NetworkNode> {
        let peers = config.peers();
        let connections = config.bind(id.clone()).await?;
        let mut builder = Self::builder(id, connections.router().clone());
        for peer in peers {
            builder = builder.seed(peer);
        }
        let node = configure(builder).spawn();
        Ok(NetworkNode { node, connections })
    }
}

impl NetworkNode {
    /// The node-owned router and optional encrypted stream endpoint.
    #[must_use]
    pub fn connections(&self) -> &Network {
        &self.connections
    }

    /// Closes this node's connections across all owner clones and drains tasks.
    /// Group handles may remain readable, but cannot exchange further messages.
    pub async fn close(&self) {
        self.connections.close().await;
    }
}

impl Deref for NetworkNode {
    type Target = Node<Router>;
    fn deref(&self) -> &Self::Target {
        &self.node
    }
}
