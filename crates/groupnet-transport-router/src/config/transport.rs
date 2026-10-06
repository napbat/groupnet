//! Adapter-specific preparation; allocation and concrete variants stay private.

use std::collections::BTreeSet;
use std::{fmt, io, net::SocketAddr};

use groupnet_core::NodeId;
use groupnet_transport::Transport;
use groupnet_transport_tcp::TcpMsgTransport;
use groupnet_transport_udp::UdpTransport;

use super::OwnedAdapter;
use crate::ipc::{IpcAddress, IpcTransport};
use crate::punch::{PunchConfig, PunchTransport};
use crate::{LinkConfig, Router, TransportId};

/// A logical peer paired with an address of the adapter's concrete type.
#[derive(Clone, Debug)]
pub struct PeerEndpoint<A> {
    /// Logical identity, independent of its physical address.
    pub node: NodeId,
    /// Adapter-specific typed address.
    pub address: A,
}

impl<A> PeerEndpoint<A> {
    /// Pairs a logical peer with its concrete transport address.
    #[must_use]
    pub fn new(node: NodeId, address: A) -> Self {
        Self { node, address }
    }
}

#[derive(Debug)]
struct Endpoints<A> {
    bind: A,
    peers: Vec<PeerEndpoint<A>>,
}

#[derive(Debug)]
enum Kind {
    Tcp(Endpoints<SocketAddr>),
    Udp(Endpoints<SocketAddr>),
    Ipc(Endpoints<IpcAddress>),
    Punch(Box<PunchConfig>),
    Custom(CustomTransport),
}

type Attach = Box<dyn FnOnce(&Router, LinkConfig) -> io::Result<TransportId> + Send>;

struct CustomTransport {
    peers: Vec<NodeId>,
    mtu: usize,
    attach: Attach,
}

impl fmt::Debug for CustomTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CustomTransport")
            .field("peers", &self.peers)
            .field("mtu", &self.mtu)
            .finish_non_exhaustive()
    }
}

/// One built-in or already-bound custom adapter, with a positive routing cost.
/// Built-ins default to cost 1. Addresses stay typed; implementation storage and
/// initialization closures do not become part of the public configuration API.
#[derive(Debug)]
pub struct TransportConfig {
    kind: Kind,
    cost: u32,
}

impl TransportConfig {
    /// Persistent TCP messages on a trusted private network.
    #[must_use]
    pub fn tcp(
        bind: SocketAddr,
        peers: impl IntoIterator<Item = PeerEndpoint<SocketAddr>>,
    ) -> Self {
        Self {
            kind: Kind::Tcp(Endpoints {
                bind,
                peers: peers.into_iter().collect(),
            }),
            cost: 1,
        }
    }

    /// Datagram messages on a trusted private network.
    #[must_use]
    pub fn udp(
        bind: SocketAddr,
        peers: impl IntoIterator<Item = PeerEndpoint<SocketAddr>>,
    ) -> Self {
        Self {
            kind: Kind::Udp(Endpoints {
                bind,
                peers: peers.into_iter().collect(),
            }),
            cost: 1,
        }
    }

    /// Local Unix sockets or Windows named pipes.
    #[must_use]
    pub fn ipc(
        bind: IpcAddress,
        peers: impl IntoIterator<Item = PeerEndpoint<IpcAddress>>,
    ) -> Self {
        Self {
            kind: Kind::Ipc(Endpoints {
                bind,
                peers: peers.into_iter().collect(),
            }),
            cost: 1,
        }
    }

    /// Authenticated UDP discovery, native hole-punching, and relay fallback.
    /// The configuration's local identity must match the network's node identity.
    #[must_use]
    pub fn punch(config: PunchConfig) -> Self {
        Self {
            kind: Kind::Punch(Box::new(config)),
            cost: 1,
        }
    }

    /// Transfers an already-bound adapter to the router using its typed link settings.
    /// Only initialization erases the attachment closure; packet futures stay static.
    /// Router workers release the adapter on shutdown. Any independent background
    /// tasks must follow the adapter's own drop contract; external clones remain owned
    /// by their callers.
    #[must_use]
    pub fn custom<T: Transport>(transport: T, config: LinkConfig) -> Self {
        Self {
            cost: config.cost,
            kind: Kind::Custom(CustomTransport {
                peers: config.peers,
                mtu: config.mtu,
                attach: Box::new(move |router, config| router.add_transport(transport, config)),
            }),
        }
    }

    /// Overrides the routing cost. A zero cost is rejected when the network binds.
    #[must_use]
    pub fn with_cost(mut self, cost: u32) -> Self {
        self.cost = cost;
        self
    }

    pub(super) fn extend_peers(&self, peers: &mut BTreeSet<NodeId>) {
        match &self.kind {
            Kind::Tcp(config) | Kind::Udp(config) => {
                peers.extend(config.peers.iter().map(|peer| peer.node.clone()));
            }
            Kind::Ipc(config) => {
                peers.extend(config.peers.iter().map(|peer| peer.node.clone()));
            }
            Kind::Punch(config) => peers.extend(config.peers.iter().cloned()),
            Kind::Custom(config) => peers.extend(config.peers.iter().cloned()),
        }
    }

    pub(super) async fn bind(
        self,
        local: &NodeId,
        router: &Router,
        owned: &mut Vec<OwnedAdapter>,
    ) -> io::Result<()> {
        let cost = self.cost;
        match self.kind {
            Kind::Tcp(config) => {
                let adapter = TcpMsgTransport::bind(local.clone(), config.bind).await?;
                for peer in &config.peers {
                    adapter.register_peer(peer.node.clone(), peer.address);
                }
                router.add_transport(adapter, link(config.peers, cost, crate::wire::MAX_FRAME))?;
            }
            Kind::Udp(config) => {
                let adapter = UdpTransport::bind(local.clone(), config.bind).await?;
                for peer in &config.peers {
                    adapter.register_peer(peer.node.clone(), peer.address);
                }
                router.add_transport(adapter, link(config.peers, cost, 1200))?;
            }
            Kind::Ipc(config) => {
                let adapter = IpcTransport::bind(local.clone(), &config.bind)?;
                owned.push(OwnedAdapter::Ipc(adapter.clone()));
                for peer in &config.peers {
                    adapter.register_peer(peer.node.clone(), peer.address.clone())?;
                }
                router.add_transport(adapter, link(config.peers, cost, crate::wire::MAX_FRAME))?;
            }
            Kind::Punch(config) => {
                if &config.local != local {
                    return Err(crate::wire::invalid(
                        "punch identity differs from node identity",
                    ));
                }
                let peers = config.peers.clone();
                let adapter = PunchTransport::bind(*config).await?;
                owned.push(OwnedAdapter::Punch(adapter.clone()));
                router.add_transport(
                    adapter,
                    LinkConfig {
                        peers,
                        cost,
                        mtu: crate::punch::MAX_MESSAGE,
                    },
                )?;
            }
            Kind::Custom(config) => {
                (config.attach)(
                    router,
                    LinkConfig {
                        peers: config.peers,
                        cost,
                        mtu: config.mtu,
                    },
                )?;
            }
        }
        Ok(())
    }
}

fn link<A>(peers: Vec<PeerEndpoint<A>>, cost: u32, mtu: usize) -> LinkConfig {
    LinkConfig {
        peers: peers.into_iter().map(|peer| peer.node).collect(),
        cost,
        mtu,
    }
}
