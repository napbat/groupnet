//! UDP link configuration and binding.

use std::io;
use std::net::SocketAddr;

use groupnet_core::NodeId;
use groupnet_transport::link::{BoundLink, LinkConfig, LinkFuture, LinkProvider, PeerEndpoint};

use crate::UdpTransport;

#[derive(Debug)]
#[cfg_attr(
    feature = "connectivity",
    expect(
        clippy::large_enum_variant,
        reason = "one-shot binding keeps native configuration inline without another allocation"
    )
)]
enum Binding {
    Direct {
        bind: SocketAddr,
        peers: Vec<NodeId>,
        addresses: Vec<SocketAddr>,
    },
    #[cfg(feature = "connectivity")]
    Connectivity(groupnet_transport_punch::PunchConfig),
}

/// A UDP link with explicit endpoints or optional native connectivity.
///
/// Available with the `link` feature. Direct mode owns only its bound socket;
/// connectivity mode also owns admission, candidate-check and relay tasks.
#[derive(Debug)]
pub struct UdpLink {
    binding: Binding,
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
            binding: Binding::Direct {
                bind,
                peers,
                addresses,
            },
            cost: 1,
        }
    }

    /// Configures session-fenced native connectivity for the declared local id.
    /// Binding rejects a mismatch with the managed node's identity.
    #[cfg(feature = "connectivity")]
    #[must_use]
    pub const fn connectivity(config: groupnet_transport_punch::PunchConfig) -> Self {
        Self {
            binding: Binding::Connectivity(config),
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
        match &self.binding {
            Binding::Direct { peers, .. } => peers,
            #[cfg(feature = "connectivity")]
            Binding::Connectivity(config) => &config.peers,
        }
    }

    fn bind(self: Box<Self>, local: NodeId) -> LinkFuture<'static, io::Result<BoundLink>> {
        Box::pin(async move {
            let Self { binding, cost } = *self;
            match binding {
                Binding::Direct {
                    bind,
                    peers,
                    addresses,
                } => {
                    let transport = UdpTransport::bind(local, bind).await?;
                    for (node, address) in peers.iter().zip(addresses) {
                        transport.register_peer(node.clone(), address);
                    }
                    let mut config = LinkConfig::new(peers);
                    config.cost = cost;
                    Ok(BoundLink::new(transport, config))
                }
                #[cfg(feature = "connectivity")]
                Binding::Connectivity(config) => {
                    if config.local != local {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "UDP connectivity link identity does not match the local node",
                        ));
                    }
                    let transport = UdpTransport::bind_connectivity(config).await?;
                    Ok(transport.into_bound_link(cost))
                }
            }
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

    #[cfg(feature = "connectivity")]
    #[tokio::test]
    async fn connected_provider_rejects_identity_before_binding() {
        use groupnet_transport_punch::{NetworkKey, PunchConfig};

        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("reserved socket");
        let address = socket.local_addr().expect("address");
        let mut config = PunchConfig::new(
            NodeId::new("configured-local"),
            address,
            NetworkKey::from_bytes([19; 32]),
            Vec::new(),
        );
        config.bind = address;
        let error = Box::new(UdpLink::connectivity(config))
            .bind(NodeId::new("different-local"))
            .await
            .expect_err("identity mismatch");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(std::net::UdpSocket::bind(address).is_err());
    }
}
