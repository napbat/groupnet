//! UDP link configuration and binding.

use std::io;
use std::net::SocketAddr;

use groupnet_core::NodeId;
use groupnet_transport::link::{BoundLink, LinkConfig, LinkFuture, LinkProvider, PeerEndpoint};

use crate::UdpTransport;

/// A UDP link with explicit directly reachable peer endpoints.
///
/// Available with the `link` feature. No independent I/O tasks are spawned:
/// the bound socket lives exactly as long as its transport workers.
#[derive(Debug)]
pub struct UdpLink {
    bind: SocketAddr,
    peers: Vec<NodeId>,
    addresses: Vec<SocketAddr>,
    cost: u32,
}

impl UdpLink {
    /// Configures the local socket address and directly reachable peers.
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

impl LinkProvider for UdpLink {
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
            let transport = UdpTransport::bind(local, bind).await?;
            for (node, address) in peers.iter().zip(addresses) {
                transport.register_peer(node.clone(), address);
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

    #[tokio::test]
    async fn provider_propagates_socket_bind_errors() {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("socket");
        let link = UdpLink::new(socket.local_addr().expect("address"), Vec::new());
        assert!(Box::new(link).bind(NodeId::new("udp-local")).await.is_err());
    }

    #[tokio::test]
    async fn bound_link_drop_releases_its_socket() {
        let reservation = std::net::UdpSocket::bind("127.0.0.1:0").expect("reserve port");
        let address = reservation.local_addr().expect("address");
        drop(reservation);
        let peer = NodeId::new("udp-peer");
        let link = UdpLink::new(
            address,
            vec![PeerEndpoint::new(
                peer,
                "127.0.0.1:12345".parse().expect("peer address"),
            )],
        )
        .with_cost(3);
        let provider: Box<dyn LinkProvider> = Box::new(link);
        let bound = provider.bind(NodeId::new("udp-local")).await.expect("bind");
        assert!(std::net::UdpSocket::bind(address).is_err());
        drop(bound);
        let _replacement = std::net::UdpSocket::bind(address).expect("socket released");
    }
}
