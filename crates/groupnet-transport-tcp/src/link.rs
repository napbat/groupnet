//! TCP control-plane link configuration and binding.

use std::io;
use std::net::SocketAddr;

use groupnet_core::NodeId;
use groupnet_transport::link::{BoundLink, LinkConfig, LinkFuture, LinkProvider, PeerEndpoint};

use crate::TcpMsgTransport;

/// A TCP control-plane link with explicit directly reachable peers.
///
/// Available with the `link` feature, which also activates `msg`. Binding owns
/// the listener and all session tasks; link shutdown cancels and drains them.
#[derive(Debug)]
pub struct TcpLink {
    bind: SocketAddr,
    peers: Vec<NodeId>,
    addresses: Vec<SocketAddr>,
    cost: u32,
}

impl TcpLink {
    /// Configures the listener address and directly reachable peer endpoints.
    #[must_use]
    pub fn new(bind: SocketAddr, peers: Vec<PeerEndpoint<SocketAddr>>) -> Self {
        let (peers, addresses) = peers
            .into_iter()
            .map(|peer| (peer.node, peer.address))
            .unzip();
        Self {
            bind,
            peers,
            addresses,
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

impl LinkProvider for TcpLink {
    fn peers(&self) -> &[NodeId] {
        &self.peers
    }

    fn bind(self: Box<Self>, local: NodeId) -> LinkFuture<'static, io::Result<BoundLink>> {
        Box::pin(async move {
            let Self {
                bind,
                peers,
                addresses,
                cost,
            } = *self;
            let transport = TcpMsgTransport::bind(local, bind).await?;
            for (node, address) in peers.iter().zip(addresses) {
                transport.register_peer(node.clone(), address);
            }
            let lifecycle = transport.lifecycle();
            let mut config = LinkConfig::new(peers);
            config.cost = cost;
            Ok(BoundLink::new(transport, config).with_lifecycle(lifecycle))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn provider_drains_listener_on_close() {
        let reservation = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve port");
        let address = reservation.local_addr().expect("address");
        drop(reservation);
        let peer = NodeId::new("tcp-peer");
        let link = TcpLink::new(
            address,
            vec![PeerEndpoint::new(
                peer,
                "127.0.0.1:12345".parse().expect("peer address"),
            )],
        )
        .with_cost(7);
        let provider: Box<dyn LinkProvider> = Box::new(link);
        let bound = provider.bind(NodeId::new("tcp-local")).await.expect("bind");
        assert!(std::net::TcpListener::bind(address).is_err());
        tokio::time::timeout(std::time::Duration::from_secs(5), bound.driver.close())
            .await
            .expect("drain listener");
        let _replacement = std::net::TcpListener::bind(address).expect("listener released");
    }

    #[tokio::test]
    async fn provider_propagates_listener_bind_errors() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let link = TcpLink::new(listener.local_addr().expect("address"), Vec::new());
        assert!(Box::new(link).bind(NodeId::new("tcp-local")).await.is_err());
    }

    #[tokio::test]
    async fn dropping_unregistered_link_cancels_listener() {
        let reservation = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve port");
        let address = reservation.local_addr().expect("address");
        drop(reservation);
        let bound = Box::new(TcpLink::new(address, Vec::new()))
            .bind(NodeId::new("tcp-local"))
            .await
            .expect("bind");
        drop(bound);
        let _replacement = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                match tokio::net::TcpListener::bind(address).await {
                    Ok(listener) => break listener,
                    Err(error) if error.kind() == io::ErrorKind::AddrInUse => {
                        tokio::task::yield_now().await;
                    }
                    Err(error) => panic!("rebind failed: {error}"),
                }
            }
        })
        .await
        .expect("dropped provider released listener");
    }
}
