use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use groupnet_core::{Config, GroupId, NodeId};
use groupnet_network::{LinkProvider, NetworkBuilder, RouterConfig, TunnelConfig};
use groupnet_streams::UnorderedConfig;

use super::{GroupProfile, Inner, Node, ROUTING_GROUP, recv_loop, sync_peer_addrs};
use crate::seeds::{NamedSeeds, resolve_named_seeds};

/// Configures a managed [`Node`]'s protocol links, group memberships and
/// node-wide protocol policies.
///
/// Link providers bind their endpoints at [`start`](Self::start); adjacent
/// admitted peers also become initial membership seeds. Node-wide messaging
/// and unordered-session policies are validated by `start` and fixed for the
/// node's life; per-operation delivery options stay on the protocol
/// descriptors.
pub struct NodeBuilder {
    id: NodeId,
    network: NetworkBuilder,
    seeds: Vec<NodeId>,
    config: Config,
    advertise_addr: Option<String>,
    named_seeds: Option<NamedSeeds>,
    messaging: crate::messaging::MessagingConfig,
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
    /// This does not grant link admission.
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

    /// Overrides the gossip interval (milliseconds). Lower is faster to
    /// converge but chattier. Since G3 the round runs digest/delta anti-entropy,
    /// so this also sets the anti-entropy cadence in step (override it separately
    /// afterwards with [`anti_entropy_interval_ms`](Self::anti_entropy_interval_ms)).
    #[must_use]
    pub fn gossip_interval_ms(mut self, ms: u64) -> Self {
        let ms = ms.max(1);
        self.config.gossip_interval_ms = ms;
        self.config.anti_entropy_interval_ms = ms;
        self
    }

    /// Overrides just the anti-entropy digest cadence (milliseconds), leaving the
    /// gossip interval as set. Call after [`gossip_interval_ms`](Self::gossip_interval_ms),
    /// which sets both.
    #[must_use]
    pub fn anti_entropy_interval_ms(mut self, ms: u64) -> Self {
        self.config.anti_entropy_interval_ms = ms.max(1);
        self
    }

    /// Overrides how many peers each anti-entropy round sends a digest to
    /// (default 2). Fanout rotates round-robin so every peer is covered over
    /// successive rounds.
    #[must_use]
    pub fn anti_entropy_fanout(mut self, peers: usize) -> Self {
        self.config.anti_entropy_fanout = peers.max(1);
        self
    }

    /// Overrides the soft per-frame byte cap for digests and deltas (default
    /// `60_000`). Larger deltas are split across successive anti-entropy rounds.
    #[must_use]
    pub fn max_delta_frame_bytes(mut self, bytes: usize) -> Self {
        self.config.max_delta_frame_bytes = bytes.max(1);
        self
    }

    /// Overrides how often a given peer receives a full digest instead of a
    /// per-peer delta digest (default 4; `1` makes every digest full). Delta
    /// digests keep the steady-state round proportional to recent churn
    /// instead of membership size — see [`Config::full_digest_every`].
    #[must_use]
    pub fn full_digest_every(mut self, n: u64) -> Self {
        self.config.full_digest_every = n.max(1);
        self
    }

    /// Replaces the full protocol [`Config`] (probe/suspect/dead timings,
    /// fanout, indirect probes, anti-entropy cadence/fanout/frame cap). The
    /// narrow per-knob setters remain for the common cases.
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
    /// Returns `InvalidInput` wrapping a
    /// [`MessagingConfigError`](crate::messaging::MessagingConfigError) or
    /// [`UnorderedConfigError`](crate::UnorderedConfigError) before binding
    /// anything; propagates invalid routing configuration, binding,
    /// registration, or security setup failures.
    /// # Panics
    /// Requires a Tokio runtime; propagates poisoned internal locks.
    pub async fn start(self) -> io::Result<Node> {
        self.messaging.validate()?;
        self.unordered.validate()?;
        let mut seeds = self.seeds;
        seeds.extend(self.network.peers());
        if let Some(named) = &self.named_seeds {
            seeds.extend(named.nodes().cloned());
        }
        let network = self.network.bind(self.id.clone()).await?;
        let messaging = crate::messaging::Hub::new(network.router(), self.messaging)?;
        let inner = Arc::new(Inner {
            id: self.id,
            transport: Arc::new(network.router().clone()),
            seeds,
            config: self.config,
            messaging,
            ordered: Mutex::new(None),
            unordered_config: self.unordered,
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
            GroupProfile::from_mode(node.inner.config.mode.clone()),
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
