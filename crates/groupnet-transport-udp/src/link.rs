//! UDP link configuration and binding.

use std::io;
use std::net::SocketAddr;

use groupnet_core::NodeId;
use groupnet_transport::link::{BoundLink, LinkConfig, LinkFuture, LinkProvider, PeerEndpoint};

use crate::UdpTransport;

/// A UDP link, registered with a router through [`LinkProvider`].
///
/// Available with the `link` feature. The mode parameter keeps each mode's
/// settings apart, as `TcpLink` does:
///
/// * [`UdpLink::new`] builds a direct link (`UdpLink<UdpDirect>`, the
///   default) over one bound socket and explicit peer addresses;
/// * with `connectivity`, `UdpLink::connectivity` builds a native link
///   (`UdpLink<UdpConnectivity>`) whose admission, candidate checks and relay
///   fallback come from its `UdpPunchConfig`, and which also owns those tasks.
///
/// Both modes are this one link type; no separate link is registered. Binding
/// is the one validation point: it fails with `InvalidInput` (carrying the
/// native configuration's typed error) before opening a socket.
#[derive(Debug)]
pub struct UdpLink<M = UdpDirect> {
    mode: M,
    cost: u32,
}

/// The direct mode of a [`UdpLink`]: one socket and explicit peer addresses.
/// Built by [`UdpLink::new`].
#[derive(Debug)]
pub struct UdpDirect {
    bind: SocketAddr,
    peers: Vec<NodeId>,
    addresses: Vec<SocketAddr>,
}

/// The native mode of a [`UdpLink`] (feature `connectivity`): session-fenced
/// admission, candidate checks and relay fallback configured by one
/// `UdpPunchConfig`. Built by `UdpLink::connectivity`.
#[cfg(feature = "connectivity")]
#[derive(Debug)]
pub struct UdpConnectivity {
    config: crate::UdpPunchConfig,
}

impl<M> UdpLink<M> {
    /// Sets the routing cost of this link (default: one).
    #[must_use]
    pub fn with_cost(mut self, cost: u32) -> Self {
        self.cost = cost;
        self
    }
}

impl UdpLink<UdpDirect> {
    /// Configures the local socket address and directly reachable peers.
    #[must_use]
    pub fn new(bind: SocketAddr, peers: Vec<PeerEndpoint<SocketAddr>>) -> Self {
        let (peers, addresses) = peers
            .into_iter()
            .map(|peer| (peer.node, peer.address))
            .unzip();
        Self {
            mode: UdpDirect {
                bind,
                peers,
                addresses,
            },
            cost: 1,
        }
    }
}

#[cfg(feature = "connectivity")]
impl UdpLink<UdpConnectivity> {
    /// Configures session-fenced native connectivity for the declared local id.
    /// Binding rejects a mismatch with the managed node's identity.
    #[must_use]
    pub const fn connectivity(config: crate::UdpPunchConfig) -> Self {
        Self {
            mode: UdpConnectivity { config },
            cost: 1,
        }
    }
}

impl LinkProvider for UdpLink<UdpDirect> {
    fn peers(&self) -> &[NodeId] {
        &self.mode.peers
    }

    fn bind(self: Box<Self>, local: NodeId) -> LinkFuture<'static, io::Result<BoundLink>> {
        Box::pin(async move {
            let Self {
                mode:
                    UdpDirect {
                        bind,
                        peers,
                        addresses,
                    },
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

#[cfg(feature = "connectivity")]
impl LinkProvider for UdpLink<UdpConnectivity> {
    fn peers(&self) -> &[NodeId] {
        &self.mode.config.peers
    }

    fn bind(self: Box<Self>, local: NodeId) -> LinkFuture<'static, io::Result<BoundLink>> {
        Box::pin(async move {
            let Self {
                mode: UdpConnectivity { config },
                cost,
            } = *self;
            if config.local != local {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "UdpPunchConfig.local {} differs from the binding node {local}",
                        config.local
                    ),
                ));
            }
            let transport = UdpTransport::bind_connectivity(config).await?;
            Ok(transport.into_bound_link(cost))
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
        use crate::{NetworkKey, UdpPunchConfig};

        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("reserved socket");
        let address = socket.local_addr().expect("address");
        let mut config = UdpPunchConfig::new(
            NodeId::new("configured-local"),
            address,
            NetworkKey::from_bytes([19; 32]),
            Vec::new(),
        );
        config.bind = address;
        let error = Box::new(UdpLink::connectivity(config).with_cost(2))
            .bind(NodeId::new("different-local"))
            .await
            .expect_err("identity mismatch");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(
            error.to_string().contains("UdpPunchConfig.local"),
            "{error}"
        );
        assert!(std::net::UdpSocket::bind(address).is_err());
    }
}
