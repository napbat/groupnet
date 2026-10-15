//! Tunnel credentials, the managed-network builder and its node-owned lifecycle.

use std::collections::BTreeSet;
use std::fmt;
use std::io;
use std::sync::Arc;

use crate::router::validate_local;
use crate::tunnel::{
    PeerIdentity, SEGMENT_HEADER, SegmentSize, TlsIdentity, TunnelLimits, TunnelLimitsError,
    TunnelTransport, TunneledStream,
};
use crate::wire::{self, PayloadKind};
use crate::{Router, RouterConfig};
use groupnet_core::NodeId;
use groupnet_transport::{bulk::BulkTransport, link::LinkProvider};

/// End-to-end encrypted streams with explicit, bidirectional peer admission.
/// Certificate pins authenticate peers and allow tunnels; they do not grant
/// application permissions or permission-group membership.
///
/// The TLS identity and initially admitted pins are mandatory inputs taken by
/// [`new`](Self::new); node-wide capacities and reliability policy are the
/// `Default`-able [`limits`](Self::limits):
/// `TunnelConfig { limits, ..TunnelConfig::new(identity, peers) }`.
#[derive(Debug)]
pub struct TunnelConfig {
    /// This node's certificate chain and private key.
    pub identity: TlsIdentity,
    /// Initially admitted peers' certificate pins; at most `limits.max_peers`,
    /// one per routing alias.
    pub peers: Vec<PeerIdentity>,
    /// Node-wide tunnel capacities and reliability policy.
    pub limits: TunnelLimits,
}

impl TunnelConfig {
    /// Configures this node's TLS identity and admitted peers' certificate
    /// pins, with default [`TunnelLimits`].
    #[must_use]
    pub fn new(identity: TlsIdentity, peers: impl IntoIterator<Item = PeerIdentity>) -> Self {
        Self {
            identity,
            peers: peers.into_iter().collect(),
            limits: TunnelLimits::default(),
        }
    }

    /// Validates the limits, the admitted-peer count and each pin's routing
    /// alias. Envelope fit against a router is checked where the router's
    /// policy is known: [`NetworkBuilder::bind`] and
    /// [`TunnelTransport::with_config`].
    ///
    /// # Errors
    /// Returns the first violated [`TunnelConfigError`] rule.
    pub fn validate(&self) -> Result<(), TunnelConfigError> {
        self.limits.validate().map_err(TunnelConfigError::Limits)?;
        if self.peers.len() > self.limits.max_peers.get() {
            return Err(TunnelConfigError::TooManyPeers);
        }
        let mut seen = BTreeSet::new();
        for peer in &self.peers {
            if !wire::id_valid(&peer.node) {
                return Err(TunnelConfigError::InvalidPeer {
                    node: peer.node.clone(),
                });
            }
            if !seen.insert(&peer.node) {
                return Err(TunnelConfigError::DuplicatePeer {
                    node: peer.node.clone(),
                });
            }
        }
        Ok(())
    }

    /// Checks every admitted peer against a router's envelope toward it.
    pub(crate) fn validate_envelopes(
        &self,
        local: &NodeId,
        max_frame: usize,
    ) -> Result<(), TunnelConfigError> {
        self.peers.iter().try_for_each(|peer| {
            Self::check_peer(local, max_frame, &peer.node, self.limits.payload)
        })
    }

    /// Rejects a peer the routing wire cannot address, or whose largest
    /// segment does not fit the envelope from `local`.
    pub(crate) fn check_peer(
        local: &NodeId,
        max_frame: usize,
        peer: &NodeId,
        payload: SegmentSize,
    ) -> Result<(), TunnelConfigError> {
        if !wire::id_valid(peer) {
            return Err(TunnelConfigError::InvalidPeer { node: peer.clone() });
        }
        if payload.get() + SEGMENT_HEADER
            > max_frame.saturating_sub(PayloadKind::Tunnel.header_len(local, peer))
        {
            return Err(TunnelConfigError::PayloadAboveRouterFrame { node: peer.clone() });
        }
        Ok(())
    }
}

/// A violated [`TunnelConfig`] rule, naming the offending field or peer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TunnelConfigError {
    /// `limits` violates a [`TunnelLimits`] rule.
    Limits(TunnelLimitsError),
    /// `peers` holds more pins than `limits.max_peers`.
    TooManyPeers,
    /// A pin's routing alias is empty or longer than 255 bytes.
    InvalidPeer {
        /// The offending alias.
        node: NodeId,
    },
    /// `peers` pins the same routing alias twice.
    DuplicatePeer {
        /// The repeated alias.
        node: NodeId,
    },
    /// A `limits.payload` segment toward this peer exceeds the router's
    /// `max_frame` envelope.
    PayloadAboveRouterFrame {
        /// The peer whose envelope is too small.
        node: NodeId,
    },
}

impl fmt::Display for TunnelConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Limits(_) => f.write_str("tunnel limits are invalid"),
            Self::TooManyPeers => f.write_str("tunnel peers exceed limits.max_peers"),
            Self::InvalidPeer { node } => {
                write!(f, "tunnel peer {node:?} is not a routable identity")
            }
            Self::DuplicatePeer { node } => write!(f, "tunnel peer {node:?} is pinned twice"),
            Self::PayloadAboveRouterFrame { node } => write!(
                f,
                "tunnel limits.payload toward {node:?} exceeds the router max_frame"
            ),
        }
    }
}

impl std::error::Error for TunnelConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Limits(error) => Some(error),
            _ => None,
        }
    }
}

impl From<TunnelConfigError> for io::Error {
    fn from(error: TunnelConfigError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}

/// Builds a managed group network: intrinsic routing over a collection of link
/// providers.
///
/// [`link`](Self::link) and [`links`](Self::links) register any implementation
/// through the same contract, in insertion order. Protocol crates own their
/// configuration and binding. Configure [`tunnels`](Self::tunnels) for
/// confidential streams; link admission never authorizes application resources.
/// [`bind`](Self::bind) validates the complete configuration, binds every
/// provider and starts routing.
#[derive(Debug, Default)]
pub struct NetworkBuilder {
    router: RouterConfig,
    links: Vec<Box<dyn LinkProvider>>,
    tunnels: Option<TunnelConfig>,
}

impl NetworkBuilder {
    /// Sets routing limits and forwarding policy; forwarding is enabled by default.
    #[must_use]
    pub fn routing(mut self, config: RouterConfig) -> Self {
        self.router = config;
        self
    }

    /// Adds a configured link implementation, subject to routing capacity limits.
    #[must_use]
    pub fn link(mut self, provider: impl LinkProvider) -> Self {
        self.links.push(Box::new(provider));
        self
    }

    /// Adds heterogeneous link implementations in iteration order.
    #[must_use]
    pub fn links(mut self, providers: impl IntoIterator<Item = Box<dyn LinkProvider>>) -> Self {
        self.links.extend(providers);
        self
    }

    /// Enables pinned, mutually authenticated TLS streams over the routed fabric.
    #[must_use]
    pub fn tunnels(mut self, config: TunnelConfig) -> Self {
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

    /// Validates the complete configuration (router policy, local identity
    /// and tunnel limits, admission and per-peer envelopes) once, then binds
    /// adapters in insertion order and starts routing, rolling back on failure.
    /// Nothing is constructed or bound when a policy is invalid.
    ///
    /// # Errors
    /// Returns `InvalidInput` wrapping a [`RouterConfigError`](crate::RouterConfigError),
    /// [`TunnelConfigError`] or the local identifier's
    /// [`NodeIdError`](crate::NodeIdError) before binding; propagates
    /// bind/initialization errors.
    /// # Panics
    /// Requires a Tokio runtime. Propagates an earlier poisoned adapter lock.
    pub async fn bind(self, local: NodeId) -> io::Result<Network> {
        self.router.validate()?;
        validate_local(&local)?;
        if let Some(tunnels) = &self.tunnels {
            tunnels.validate()?;
            tunnels.validate_envelopes(&local, self.router.max_frame.get())?;
        }
        let mut network = NetworkInner {
            router: Router::start(local.clone(), self.router)?,
            tunnels: None,
        };
        let started = async {
            for provider in self.links {
                let link = provider.bind(local.clone()).await?;
                network.router.add_link(link).await?;
            }
            network.tunnels = self
                .tunnels
                .map(|config| TunnelTransport::start(network.router.clone(), config))
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
/// Start one with [`Network::builder`]. Dropping the final clone initiates
/// shutdown; [`close`](Self::close) drains tasks.
/// Owned link workers drain each provider's lifecycle through the shared contract.
#[derive(Clone, Debug)]
pub struct Network {
    inner: Arc<NetworkInner>,
}

impl Network {
    /// Starts configuring a managed network; nothing binds until
    /// [`NetworkBuilder::bind`].
    #[must_use]
    pub fn builder() -> NetworkBuilder {
        NetworkBuilder::default()
    }

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
