//! Typed adapter configuration and a node-owned network lifecycle.

use std::collections::BTreeSet;
use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use groupnet_core::NodeId;
use groupnet_transport::Transport;
use groupnet_transport_tcp::TcpMsgTransport;
use groupnet_transport_udp::UdpTransport;

use crate::ipc::{IpcAddress, IpcTransport};
use crate::punch::{PunchConfig, PunchTransport};
use crate::tunnel::{PeerIdentity, TlsIdentity, TunnelTransport};
use crate::{LinkConfig, Router, RouterConfig, TransportId};

/// A peer identifier paired with an address of the adapter's concrete type.
#[derive(Clone, Debug)]
pub struct PeerEndpoint<A> {
    /// Logical peer identity; independent of its physical address.
    pub node: NodeId,
    /// Adapter-specific typed address.
    pub address: A,
}

/// A concrete connection option started as part of network initialization.
/// Multiple options of the same or different variants may be configured.
#[derive(Debug)]
pub enum TransportOption {
    /// Persistent TCP messages on a trusted private network.
    Tcp {
        /// Local listening socket.
        bind: SocketAddr,
        /// Explicitly configured adjacent endpoints.
        peers: Vec<PeerEndpoint<SocketAddr>>,
        /// Positive route cost.
        cost: u32,
    },
    /// Datagram messages on a trusted private network.
    Udp {
        /// Local UDP socket.
        bind: SocketAddr,
        /// Explicitly configured adjacent endpoints.
        peers: Vec<PeerEndpoint<SocketAddr>>,
        /// Positive route cost.
        cost: u32,
    },
    /// Local Unix sockets or Windows named pipes.
    Ipc {
        /// Local IPC listener address.
        bind: IpcAddress,
        /// Adjacent local-process endpoints.
        peers: Vec<PeerEndpoint<IpcAddress>>,
        /// Positive route cost.
        cost: u32,
    },
    /// Authenticated UDP discovery, native hole-punching, and relay fallback.
    Punch {
        /// Typed rendezvous, admission, identity, and path settings.
        config: Box<PunchConfig>,
        /// Positive route cost.
        cost: u32,
    },
}

/// Optional end-to-end encrypted stream endpoint initialized with the network.
#[derive(Debug)]
pub struct TunnelConfig {
    /// This node's private-CA TLS identity.
    pub identity: TlsIdentity,
    /// Explicit certificate pins for allowed logical peers.
    pub peers: Vec<PeerIdentity>,
}

type Attach = Box<dyn FnOnce(&Router) -> io::Result<TransportId> + Send>;
struct PreparedTransport {
    peers: Vec<NodeId>,
    attach: Attach,
}

/// Fully typed connection configuration for a Groupnet node.
///
/// TCP/UDP options assume a trusted private network. For Internet links use
/// `Punch`; for confidential application bytes configure `tunnels`. A network
/// key authorizes the routing fabric, not access to an application resource.
#[derive(Default)]
pub struct NetworkConfig {
    /// Routing/forwarding policy and bounds.
    pub router: RouterConfig,
    /// Any number of configured connection options, subject to router limits.
    pub transports: Vec<TransportOption>,
    /// Optional secure reliable stream endpoint.
    pub tunnels: Option<TunnelConfig>,
    prepared: Vec<PreparedTransport>,
}

impl fmt::Debug for NetworkConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NetworkConfig")
            .field("router", &self.router)
            .field("transports", &self.transports)
            .field("tunnels", &self.tunnels)
            .field("custom_adapters", &self.prepared.len())
            .finish()
    }
}

impl NetworkConfig {
    /// Adds an already-bound, statically typed custom adapter to initialization.
    /// The closure is erased once here; packet processing never boxes futures.
    #[must_use]
    pub fn with_transport<T: Transport>(mut self, transport: T, config: LinkConfig) -> Self {
        self.prepared.push(PreparedTransport {
            peers: config.peers.clone(),
            attach: Box::new(move |router| router.add_transport(transport, config)),
        });
        self
    }

    /// The configured adjacent peers, deduplicated for initial membership seeds.
    #[must_use]
    pub fn peers(&self) -> Vec<NodeId> {
        let mut peers = BTreeSet::new();
        for option in &self.transports {
            match option {
                TransportOption::Tcp { peers: list, .. }
                | TransportOption::Udp { peers: list, .. } => {
                    peers.extend(list.iter().map(|peer| peer.node.clone()));
                }
                TransportOption::Ipc { peers: list, .. } => {
                    peers.extend(list.iter().map(|peer| peer.node.clone()));
                }
                TransportOption::Punch { config, .. } => {
                    peers.extend(config.peers.iter().cloned());
                }
            }
        }
        for prepared in &self.prepared {
            peers.extend(prepared.peers.iter().cloned());
        }
        peers.into_iter().collect()
    }

    /// Binds every adapter and starts routing, rolling back on any failure.
    ///
    /// # Errors
    /// Propagates invalid options, bind/initialization errors, and TLS errors.
    /// # Panics
    /// Requires a Tokio runtime. Propagates an earlier poisoned adapter lock.
    pub async fn bind(self, local: NodeId) -> io::Result<Network> {
        let router = Router::new(local.clone(), self.router)?;
        let mut owned = Vec::new();
        let started = async {
            for option in self.transports {
                if let Some(adapter) = bind_option(&router, &local, option).await? {
                    owned.push(adapter);
                }
            }
            for prepared in self.prepared {
                (prepared.attach)(&router)?;
            }
            let tunnels = self
                .tunnels
                .map(|config| TunnelTransport::new(router.clone(), config.identity, config.peers))
                .transpose()?;
            Ok::<_, io::Error>(tunnels)
        }
        .await;
        match started {
            Ok(tunnels) => Ok(Network {
                inner: Arc::new(NetworkInner {
                    router,
                    tunnels,
                    owned,
                }),
            }),
            Err(error) => {
                router.shutdown();
                for adapter in &owned {
                    adapter.close().await;
                }
                router.close().await;
                Err(error)
            }
        }
    }
}

async fn bind_option(
    router: &Router,
    local: &NodeId,
    option: TransportOption,
) -> io::Result<Option<OwnedAdapter>> {
    match option {
        TransportOption::Tcp { bind, peers, cost } => {
            let adapter = TcpMsgTransport::bind(local.clone(), bind).await?;
            for peer in &peers {
                adapter.register_peer(peer.node.clone(), peer.address);
            }
            router.add_transport(
                adapter,
                LinkConfig {
                    peers: peers.into_iter().map(|peer| peer.node).collect(),
                    cost,
                    mtu: 65_000,
                },
            )?;
        }
        TransportOption::Udp { bind, peers, cost } => {
            let adapter = UdpTransport::bind(local.clone(), bind).await?;
            for peer in &peers {
                adapter.register_peer(peer.node.clone(), peer.address);
            }
            router.add_transport(
                adapter,
                LinkConfig {
                    peers: peers.into_iter().map(|peer| peer.node).collect(),
                    cost,
                    mtu: 1200,
                },
            )?;
        }
        TransportOption::Ipc { bind, peers, cost } => {
            let adapter = IpcTransport::bind(local.clone(), &bind)?;
            for peer in &peers {
                adapter.register_peer(peer.node.clone(), peer.address.clone())?;
            }
            let result = router.add_transport(
                adapter.clone(),
                LinkConfig {
                    peers: peers.into_iter().map(|peer| peer.node).collect(),
                    cost,
                    mtu: 65_000,
                },
            );
            if let Err(error) = result {
                adapter.close().await;
                return Err(error);
            }
            return Ok(Some(OwnedAdapter::Ipc(adapter)));
        }
        TransportOption::Punch { config, cost } => {
            if &config.local != local {
                return Err(crate::wire::invalid(
                    "punch identity differs from node identity",
                ));
            }
            let peers = config.peers.clone();
            let adapter = PunchTransport::bind(*config).await?;
            let result = router.add_transport(
                adapter.clone(),
                LinkConfig {
                    peers,
                    cost,
                    mtu: crate::punch::MAX_MESSAGE,
                },
            );
            if let Err(error) = result {
                adapter.close().await;
                return Err(error);
            }
            return Ok(Some(OwnedAdapter::Punch(adapter)));
        }
    }
    Ok(None)
}

#[derive(Debug)]
enum OwnedAdapter {
    Ipc(IpcTransport),
    Punch(PunchTransport),
}

impl OwnedAdapter {
    async fn close(&self) {
        match self {
            Self::Ipc(adapter) => adapter.close().await,
            Self::Punch(adapter) => adapter.close().await,
        }
    }
}

#[derive(Debug)]
struct NetworkInner {
    router: Router,
    tunnels: Option<TunnelTransport>,
    owned: Vec<OwnedAdapter>,
}
impl Drop for NetworkInner {
    fn drop(&mut self) {
        self.router.shutdown();
    }
}

/// Owned network lifetime: dropping the final clone shuts down every adapter.
/// A node's initialization retains this owner alongside its membership runtime.
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
    /// The optional end-to-end stream transport configured during initialization.
    #[must_use]
    pub fn tunnels(&self) -> Option<&TunnelTransport> {
        self.inner.tunnels.as_ref()
    }
    /// Shuts down encrypted sessions, routing, and all owned connection tasks.
    pub async fn close(&self) {
        if let Some(tunnels) = &self.inner.tunnels {
            tunnels.close().await;
        }
        self.inner.router.shutdown();
        for adapter in &self.inner.owned {
            adapter.close().await;
        }
        self.inner.router.close().await;
    }
}
