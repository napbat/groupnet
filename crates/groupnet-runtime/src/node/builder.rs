use std::collections::HashMap;
use std::io;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use groupnet_core::{Config, GroupId, NodeId};
use groupnet_messaging::ValidMessagingConfig;
use groupnet_network::{LinkProvider, NetworkBuilder, RouterConfig, TunnelConfig};
use groupnet_streams::{OrderedConfig, UnorderedConfig, ValidOrderedConfig, ValidUnorderedConfig};

use super::{Inner, Node, Posture, ROUTING_GROUP, recv_loop, sync_peer_addrs};
use crate::clock::core_millis;
use crate::seeds::{NamedSeeds, resolve_named_seeds};

/// Configures a managed [`Node`]'s protocol links, group memberships and
/// node-wide protocol policies.
///
/// Link providers bind their endpoints at [`start`](Self::start); adjacent
/// admitted peers also become initial membership seeds. Node-wide messaging,
/// ordered-stream and unordered-session policies are validated once by `start`
/// and fixed for the node's life; per-operation delivery options stay on the protocol
/// descriptors.
pub struct NodeBuilder {
    id: NodeId,
    network: NetworkBuilder,
    seeds: Vec<NodeId>,
    config: Config,
    advertise_addr: Option<String>,
    named_seeds: Option<NamedSeeds>,
    messaging: crate::messaging::MessagingConfig,
    ordered: OrderedConfig,
    unordered: UnorderedConfig,
}

impl std::fmt::Debug for NodeBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeBuilder")
            .field("id", &self.id)
            .field("network", &self.network)
            .field("seeds", &self.seeds)
            .field("named_seeds", &self.named_seeds)
            .field("messaging", &self.messaging)
            .field("ordered", &self.ordered)
            .field("unordered", &self.unordered)
            .finish_non_exhaustive()
    }
}

impl NodeBuilder {
    pub(super) fn new(id: NodeId) -> Self {
        Self {
            id,
            network: NetworkBuilder::default(),
            seeds: Vec::new(),
            config: Config::default(),
            advertise_addr: None,
            named_seeds: None,
            messaging: crate::messaging::MessagingConfig::default(),
            ordered: OrderedConfig::default(),
            unordered: UnorderedConfig::default(),
        }
    }

    /// Registers one configured protocol implementation.
    #[must_use]
    pub fn link(mut self, provider: impl LinkProvider) -> Self {
        self.network = self.network.link(provider);
        self
    }

    /// Registers a heterogeneous collection of configured protocol implementations.
    #[must_use]
    pub fn links(mut self, providers: impl IntoIterator<Item = Box<dyn LinkProvider>>) -> Self {
        self.network = self.network.links(providers);
        self
    }

    /// Sets routing limits and transit policy, independently of link protocols.
    #[must_use]
    pub fn routing(mut self, config: RouterConfig) -> Self {
        self.network = self.network.routing(config);
        self
    }

    /// Enables pinned, end-to-end TLS streams over routed paths.
    #[must_use]
    pub fn tunnels(mut self, config: TunnelConfig) -> Self {
        self.network = self.network.tunnels(config);
        self
    }

    /// Configures shared message payloads, inboxes, retries and group fanout bounds.
    /// [`start`](Self::start) rejects an invalid configuration.
    #[must_use]
    pub fn messaging(mut self, config: crate::messaging::MessagingConfig) -> Self {
        self.messaging = config;
        self
    }

    /// Configures node-wide ordered-stream accept queues (one per
    /// [`StreamProtocolId`](crate::StreamProtocolId)) and the setup deadline for
    /// every [`Ordered`](crate::Ordered) endpoint and peer of this node.
    /// [`start`](Self::start) rejects an invalid configuration; the policy
    /// cannot change once the node runs.
    #[must_use]
    pub fn ordered(mut self, config: OrderedConfig) -> Self {
        self.ordered = config;
        self
    }

    /// Configures node-wide unordered-session admission (`allow_reliable` /
    /// `allow_unreliable`), capacities and timers for every
    /// [`Unordered`](crate::Unordered) endpoint and peer of this node.
    /// [`start`](Self::start) rejects an invalid configuration; the policy
    /// cannot change once the node runs. Configuring it does not open the
    /// protocol, which the first unordered binding initializes.
    #[must_use]
    pub fn unordered(mut self, config: UnorderedConfig) -> Self {
        self.unordered = config;
        self
    }

    /// Adds a seed peer to bootstrap gossip against.
    /// This does not grant link admission; links must explicitly admit peers.
    #[must_use]
    pub fn seed(mut self, id: NodeId) -> Self {
        self.seeds.push(id);
        self
    }

    /// Adds seeds addressed by `host:port` name, resolved and kept current by
    /// the node itself (see [`NamedSeeds`]): every seed joins the seed set
    /// now, its address reaches admitted links once it resolves, and it is
    /// re-resolved for the life of the node. A later call replaces an earlier one.
    /// This does not grant link admission. Stored as given;
    /// [`start`](Self::start) validates it ([`NamedSeeds::validate`]).
    #[must_use]
    pub fn named_seeds(mut self, seeds: NamedSeeds) -> Self {
        self.named_seeds = Some(seeds);
        self
    }

    /// Enables or disables eager delta push (default: enabled) — see
    /// [`groupnet_core::Config::eager_push`].
    #[must_use]
    pub fn eager_push(mut self, enabled: bool) -> Self {
        self.config.eager_push = enabled;
        self
    }

    /// Overrides the gossip round's cadence — see
    /// [`Config::gossip_interval_ms`]. Every round sends a digest/delta
    /// anti-entropy exchange to [`gossip_fanout`](Self::gossip_fanout) peers.
    /// Lower is faster to converge but chattier. The core counts whole
    /// milliseconds: a sub-millisecond remainder rounds up to the next one and
    /// an interval beyond `u64::MAX` milliseconds saturates. Stored as given;
    /// [`start`](Self::start) rejects zero.
    #[must_use]
    pub fn gossip_interval(mut self, interval: Duration) -> Self {
        self.config.gossip_interval_ms = core_millis(interval);
        self
    }

    /// Overrides how many peers each gossip round sends a digest to
    /// (default 2) — see [`Config::gossip_fanout`]. Targets rotate
    /// round-robin so every peer is covered over successive rounds.
    #[must_use]
    pub fn gossip_fanout(mut self, peers: NonZeroUsize) -> Self {
        self.config.gossip_fanout = peers;
        self
    }

    /// Overrides the soft per-frame byte cap for digests and deltas (default
    /// `60_000`) — see [`Config::max_delta_frame_bytes`]. Larger deltas are
    /// split across successive gossip rounds.
    #[must_use]
    pub fn max_delta_frame_bytes(mut self, bytes: NonZeroUsize) -> Self {
        self.config.max_delta_frame_bytes = bytes;
        self
    }

    /// Overrides how often a given peer receives a full digest instead of a
    /// per-peer delta digest (default 4; `1` makes every digest full). Delta
    /// digests keep the steady-state round proportional to recent churn
    /// instead of membership size — see [`Config::full_digest_every`].
    #[must_use]
    pub fn full_digest_every(mut self, n: NonZeroU64) -> Self {
        self.config.full_digest_every = n;
        self
    }

    /// Replaces the full protocol [`Config`] (gossip cadence and fanout,
    /// probe/suspect/dead timings, indirect probes, frame cap, digest
    /// cadence, node-wide mode). The narrow per-knob setters remain for the
    /// common cases. Stored as given; [`start`](Self::start) validates it.
    #[must_use]
    pub fn config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }

    /// Advertise a reachable address for this node, disseminated cluster-wide
    /// as the reserved `~addr` state entry on the routing group — so only
    /// seeds need out-of-band addressing and everyone else resolves peers
    /// from gossip ([`crate::Group::node_entry`] / [`Node::peer_addr`]).
    ///
    /// Received advertisements update only links that explicitly admit the
    /// advertised peer. Gossip never grants link admission.
    #[must_use]
    pub fn advertise_addr(mut self, addr: impl Into<String>) -> Self {
        self.advertise_addr = Some(addr.into());
        self
    }

    /// Validates node-wide policies, binds all links, then starts routing and
    /// group coordination.
    /// Every ordinary [`Node`] clone retains network ownership; dropping the
    /// last initiates shutdown. Name resolution runs in the background and
    /// never delays startup.
    ///
    /// # Errors
    /// Returns `InvalidInput` before binding anything when a policy fails
    /// validation, wrapping the typed error that names the field: the group
    /// [`ConfigError`](groupnet_core::ConfigError) from [`Config::validate`], a
    /// [`MessagingConfigError`](crate::messaging::MessagingConfigError), an
    /// [`OrderedConfigError`](crate::OrderedConfigError), an
    /// [`UnorderedConfigError`](crate::UnorderedConfigError), or a
    /// [`NamedSeedsError`](crate::NamedSeedsError) from
    /// [`NamedSeeds::validate`]. Propagates invalid routing configuration,
    /// binding, registration, or security setup failures.
    ///
    /// # Panics
    /// Requires a Tokio runtime; propagates poisoned internal locks.
    pub async fn start(self) -> io::Result<Node> {
        self.config.validate()?;
        let messaging = ValidMessagingConfig::try_from(self.messaging)?;
        let ordered = ValidOrderedConfig::try_from(self.ordered)?;
        let unordered = ValidUnorderedConfig::try_from(self.unordered)?;
        if let Some(named) = &self.named_seeds {
            named.validate()?;
        }
        let mut seeds = self.seeds;
        seeds.extend(self.network.peers());
        if let Some(named) = &self.named_seeds {
            seeds.extend(named.nodes().cloned());
        }
        let network = self.network.bind(self.id.clone()).await?;
        let messaging = crate::messaging::Hub::new(network.router(), messaging)?;
        let inner = Arc::new(Inner {
            id: self.id,
            transport: Arc::new(network.router().clone()),
            seeds,
            config: self.config,
            messaging,
            ordered_config: ordered,
            ordered: Mutex::new(None),
            unordered_config: unordered,
            unordered: Mutex::new(None),
            routes: Mutex::new(HashMap::new()),
            start: Instant::now(),
            routing: OnceLock::new(),
        });
        tokio::spawn(recv_loop(inner.clone()));
        super::messaging::start_dispatcher(&inner);
        if let Some(named) = self.named_seeds {
            let router = inner.transport.clone();
            let local = inner.id.clone();
            tokio::spawn(async move {
                tokio::select! {
                    biased;
                    () = router.cancelled() => {}
                    () = resolve_named_seeds(Arc::downgrade(&router), local, named) => {}
                }
            });
        }
        let node = Node { inner, network };
        // Join the reserved routing group before returning any public handle.
        // `spawn_group` pins it Eventual whatever this profile asks for.
        let routing_group = node.get_or_spawn(
            GroupId::new(ROUTING_GROUP),
            None,
            Posture::from_mode(node.inner.config.mode.clone()),
        );
        if let Some(addr) = self.advertise_addr {
            let _ = routing_group.set_entry("~addr", addr.into_bytes(), None);
        }
        tokio::spawn(sync_peer_addrs(
            node.inner.transport.clone(),
            node.inner.id.clone(),
            routing_group.entries_watch(),
        ));
        let _ = node.inner.routing.set(routing_group);
        tokio::spawn(super::discovery::discover_peers(node.inner.clone()));
        Ok(node)
    }
}
