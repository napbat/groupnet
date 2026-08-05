use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use groupnet_core::{Config, GroupEngine, GroupId, GroupMode, HostedConfig, NodeId};
use groupnet_transport::Transport;
use tokio::sync::{mpsc, watch};

use crate::driver::{
    EVENTS_CAPACITY, Event, GroupViews, INBOX_CAPACITY, NodeEntriesSnapshot, Publishers,
    group_task, statuses_snapshot,
};
use crate::group::{Group, Leadership};
use crate::routing::Routing;
use tokio::sync::broadcast;

/// The reserved group every node joins to disseminate the inter-group routing
/// table. Its metadata holds `owner:<resource>` and `coord:<group>` entries.
pub(crate) const ROUTING_GROUP: &str = "__groupnet_routing__";

/// The consistency posture one group is joined under — what
/// [`Node::join_group_with`] takes.
///
/// The posture is **per group, not per node**: a node freely mixes hosted
/// shard groups with eventual fabric groups, and opting one group in cannot
/// make another run an election. Everything else about the group (gossip
/// cadence, detector timings, fanout) still comes from the node's builder
/// config — a profile only decides the mode.
///
/// ```no_run
/// use groupnet_core::{Activation, HostedConfig};
/// use groupnet_runtime::GroupProfile;
///
/// # fn demo<T: groupnet_transport::Transport>(node: &groupnet_runtime::Node<T>) {
/// let shard = node.join_group_with(
///     "shard-7",
///     GroupProfile::hosted(HostedConfig {
///         activation: Activation::Settle {
///             claim_settle_ms: 600,
///         },
///         lease_ms: 2_000,
///     }),
/// );
/// # let _ = shard;
/// # }
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GroupProfile {
    mode: GroupMode,
}

impl GroupProfile {
    /// Metadata and membership only, converging eventually — no election, no
    /// host, and no election frames on the wire. What every group was before
    /// Hosted mode existed, and what [`Node::join_group`] uses unless the
    /// node's [`Config::mode`] says otherwise.
    #[must_use]
    pub const fn eventual() -> Self {
        Self {
            mode: GroupMode::Eventual,
        }
    }

    /// The group elects one epoch-fenced host per `config`, surfaced through
    /// [`Group::leadership`](crate::Group::leadership) and
    /// [`GroupEvent::LeadershipChanged`](crate::GroupEvent::LeadershipChanged).
    ///
    /// Size `config`'s durations against the node's detector timings — the
    /// sizing rules (and the fact that both must be far larger than the
    /// driver's tick period) are on [`HostedConfig`].
    #[must_use]
    pub const fn hosted(config: HostedConfig) -> Self {
        Self {
            mode: GroupMode::Hosted(config),
        }
    }

    /// The profile a bare [`Node::join_group`] joins under: whatever the
    /// node's own [`Config::mode`] says.
    const fn from_mode(mode: GroupMode) -> Self {
        Self { mode }
    }
}

struct Inner<T: Transport> {
    id: NodeId,
    transport: Arc<T>,
    seeds: Vec<NodeId>,
    config: Config,
    /// Joined groups (handle + inbox). Holding the `Group` makes `join_group`
    /// idempotent: a repeat join returns the existing handle instead of
    /// spawning a second, orphaned actor for the same group.
    routes: Mutex<HashMap<GroupId, Group>>,
    start: Instant,
    /// The routing system group, joined once at spawn.
    routing: OnceLock<Group>,
}

/// A running Groupnet node: owns a bound transport and hosts group
/// memberships. Cheap to clone (it's an `Arc` inside).
pub struct Node<T: Transport> {
    inner: Arc<Inner<T>>,
}

impl<T: Transport> Clone for Node<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T: Transport> std::fmt::Debug for Node<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("id", &self.inner.id)
            .finish_non_exhaustive()
    }
}

impl<T: Transport> Node<T> {
    /// Starts building a node with the given id and bound transport.
    #[must_use]
    pub fn builder(id: NodeId, transport: T) -> NodeBuilder<T> {
        NodeBuilder {
            id,
            transport,
            seeds: Vec::new(),
            config: Config::default(),
            advertise_addr: None,
        }
    }

    /// This node's id.
    #[must_use]
    pub fn id(&self) -> &NodeId {
        &self.inner.id
    }

    /// Joins `group`, spawning its actor task, and returns a handle.
    /// Idempotent: joining a group this node already participates in returns
    /// the existing handle.
    ///
    /// The group is joined under the node's own [`Config::mode`] — normally
    /// [`Eventual`](groupnet_core::GroupMode::Eventual). Opt a single group
    /// into Hosted mode with [`join_group_with`](Self::join_group_with).
    ///
    /// # Panics
    /// If the internal group table was poisoned by a panic in another thread.
    pub fn join_group(&self, group: impl Into<GroupId>) -> Group {
        self.join_group_with(group, GroupProfile::from_mode(self.inner.config.mode))
    }

    /// Joins `group` under an explicit [`GroupProfile`] — the per-group
    /// consistency posture — and returns a handle.
    ///
    /// # The first join wins, loudly
    ///
    /// This is idempotent in exactly the way [`join_group`](Self::join_group)
    /// is: a repeat join of a group this node already participates in returns
    /// the **existing handle**, spawning nothing. So on a repeat join
    /// `profile` is *ignored* — the profile of the **first** join governs for
    /// the life of the node's membership, whatever a later call passes. A
    /// group's mode is baked into its engine at spawn (it decides whether an
    /// election exists at all), and silently restarting a live actor to change
    /// it would drop in-flight state and reset an epoch that peers still fence
    /// against.
    ///
    /// So a caller that means to host a group must say so on the join that
    /// creates it — including the implicit ones: [`join_group`](Self::join_group)
    /// counts, and so does a `join_group` on any other code path in the same
    /// process holding the same [`Node`]. If two call sites disagree about a
    /// group's profile, whichever ran first is the one in effect; to leave the
    /// group and rejoin under a different profile, build a new node.
    ///
    /// "First" is well defined even when the two calls are concurrent: the
    /// get-or-spawn is atomic under the group table's lock, so of two threads
    /// joining the same group at the same instant exactly one spawns the actor
    /// and **both** are handed that one group.
    ///
    /// # Panics
    /// If the internal group table was poisoned by a panic in another thread.
    pub fn join_group_with(&self, group: impl Into<GroupId>, profile: GroupProfile) -> Group {
        // Real groups announce their coordinator into the routing group. Read
        // outside the lock: the routing group is joined once, during
        // `NodeBuilder::spawn`, before any `Node` handle exists to race with.
        let routing = self.inner.routing.get().map(Group::command_sender);
        self.get_or_spawn(group.into(), routing, profile)
    }

    /// Returns the handle for `group`, spawning its actor if this node has not
    /// joined it yet — the whole decision under **one** hold of the routes
    /// lock, and the only place the table is written.
    ///
    /// The single hold is the contract, not an optimisation. Releasing the lock
    /// between the lookup and the insert would make "the first join governs" a
    /// lie under concurrency: two threads joining the same group at once would
    /// each find it absent, each spawn an actor, and the *second* insert would
    /// win — leaving two engines running the same group id (under two different
    /// profiles, if the callers disagreed), one of them orphaned in the table
    /// but still ticking, gossiping, and — in a Hosted group — electing.
    /// [`spawn_group`](Self::spawn_group) has no `.await` and never touches
    /// this lock, so nothing can yield or re-enter while it is held.
    fn get_or_spawn(
        &self,
        group: GroupId,
        routing: Option<mpsc::Sender<Event>>,
        profile: GroupProfile,
    ) -> Group {
        let mut routes = self.inner.routes.lock().expect("routes mutex poisoned");
        if let Some(existing) = routes.get(&group) {
            return existing.clone();
        }
        let handle = self.spawn_group(group.clone(), routing, profile);
        routes.insert(group, handle.clone());
        handle
    }

    /// The address `node` advertised via
    /// [`advertise_addr`](NodeBuilder::advertise_addr), as gossip currently
    /// shows it (UTF-8; `None` if unknown or not advertised).
    #[must_use]
    pub fn peer_addr(&self, node: &NodeId) -> Option<String> {
        let group = self.inner.routing.get()?;
        let bytes = group.node_entry(node, "~addr")?;
        String::from_utf8(bytes).ok()
    }

    /// The inter-group routing table: look up which group owns a resource and
    /// which node coordinates it, from any node in the cluster.
    ///
    /// # Panics
    /// Never in practice: the reserved routing group is joined during
    /// [`NodeBuilder::spawn`], before any `Node` handle exists.
    #[must_use]
    pub fn routing(&self) -> Routing {
        let group = self
            .inner
            .routing
            .get()
            .expect("routing group is joined at spawn")
            .clone();
        Routing::new(group)
    }

    /// Spawns a group actor and returns its handle, without touching the routes
    /// table — [`get_or_spawn`](Self::get_or_spawn) owns that, and calls this
    /// with the lock held, which is why nothing here may await.
    ///
    /// `routing` is the routing group's command channel (so this group can
    /// publish its coordinator), or `None` for the routing group itself.
    /// `profile` decides this group's mode; every other tunable comes from the
    /// node's config.
    fn spawn_group(
        &self,
        group: GroupId,
        routing: Option<mpsc::Sender<Event>>,
        profile: GroupProfile,
    ) -> Group {
        // Tick often enough to service the tightest engine deadline (probe
        // timeouts are the shortest), so failure detection isn't lagged by a
        // coarse gossip-only cadence. Sampling at `TICKS_PER_DEADLINE`× the
        // tightest deadline bounds how late a deadline can fire to one tick; the
        // engine is idempotent under early/extra ticks, so oversampling is safe.
        const TICKS_PER_DEADLINE: u64 = 2;

        let (tx, rx) = mpsc::channel(INBOX_CAPACITY);

        // The group's *own* config: the node's, with only the mode replaced.
        // Every group has had its own `Arc<Config>` since M0, so a per-group
        // mode costs nothing new — and the routing group is force-pinned
        // Eventual here, the one place every join funnels through. It is
        // fabric plumbing carrying the cluster's routing table on every node;
        // electing a host for it would put an epoch-fenced authority in front
        // of the table every other group publishes into, for no gain. No
        // profile (and no node-wide `Config::mode`) can opt it in.
        let mut config = self.inner.config.clone();
        config.mode = if group.as_str() == ROUTING_GROUP {
            GroupMode::Eventual
        } else {
            profile.mode
        };

        let engine = GroupEngine::new(
            group.clone(),
            self.inner.id.clone(),
            self.inner.seeds.iter().cloned(),
            config.clone(),
        );

        // Seed the readable views from the engine's current truth. The engine
        // only emits change effects on an actual change, so a node that is (and
        // stays) its own coordinator would otherwise never publish an initial
        // value.
        let (coord_tx, coord_rx) = watch::channel(engine.coordinator().cloned());
        let (epoch, host) = engine.leadership();
        let (lead_tx, lead_rx) =
            watch::channel(Leadership::observed(epoch, host.cloned(), &self.inner.id));
        let (meta_tx, meta_rx) = watch::channel(Arc::new(BTreeMap::new()));
        let initial_members: Vec<NodeId> = engine.members().cloned().collect();
        let (members_tx, members_rx) = watch::channel(Arc::new(initial_members));
        let (statuses_tx, statuses_rx) = watch::channel(statuses_snapshot(&engine));
        let (entries_tx, entries_rx) = watch::channel(Arc::new(BTreeMap::new()));
        let (net_stats_tx, net_stats_rx) = watch::channel(groupnet_core::NetStats::default());
        let (events_tx, _) = broadcast::channel(EVENTS_CAPACITY);

        let tightest_deadline_ms = config
            .gossip_interval_ms
            .min(config.probe_interval_ms)
            .min(config.probe_timeout_ms);
        let tick_period = Duration::from_millis((tightest_deadline_ms / TICKS_PER_DEADLINE).max(1));
        tokio::spawn(group_task(
            engine,
            rx,
            self.inner.transport.clone(),
            Publishers {
                coordinator: coord_tx,
                leadership: lead_tx,
                metadata: meta_tx,
                members: members_tx,
                statuses: statuses_tx,
                entries: entries_tx,
                net_stats: net_stats_tx,
                events: events_tx.clone(),
            },
            routing,
            self.inner.start,
            tick_period,
        ));

        Group::new(
            group,
            self.inner.id.clone(),
            // The *effective* config this group is running — the node's, with
            // this group's mode — shared with every handle to it so a consumer
            // sizes its timing windows off what is actually running.
            Arc::new(config),
            self.inner.start,
            tx,
            GroupViews {
                coordinator: coord_rx,
                leadership: lead_rx,
                metadata: meta_rx,
                members: members_rx,
                statuses: statuses_rx,
                entries: entries_rx,
                net_stats: net_stats_rx,
                events: events_tx,
            },
        )
    }
}

/// Builder for a [`Node`].
pub struct NodeBuilder<T: Transport> {
    id: NodeId,
    transport: T,
    seeds: Vec<NodeId>,
    config: Config,
    advertise_addr: Option<String>,
}

impl<T: Transport> std::fmt::Debug for NodeBuilder<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeBuilder")
            .field("id", &self.id)
            .field("seeds", &self.seeds)
            .finish_non_exhaustive()
    }
}

impl<T: Transport> NodeBuilder<T> {
    /// Adds a seed peer to bootstrap gossip against.
    #[must_use]
    pub fn seed(mut self, id: NodeId) -> Self {
        self.seeds.push(id);
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
    /// from gossip ([`Group::node_entry`] / [`Node::peer_addr`]).
    ///
    /// Every node also feeds the advertisements it *receives* into its own
    /// transport ([`Transport::learn_peer`]) automatically, so address-book
    /// transports (UDP, persistent TCP) fill themselves from gossip.
    #[must_use]
    pub fn advertise_addr(mut self, addr: impl Into<String>) -> Self {
        self.advertise_addr = Some(addr.into());
        self
    }

    /// Spawns the node: starts the transport receive loop and returns a handle.
    /// Must be called from within a Tokio runtime.
    pub fn spawn(self) -> Node<T> {
        let inner = Arc::new(Inner {
            id: self.id,
            transport: Arc::new(self.transport),
            seeds: self.seeds,
            config: self.config,
            routes: Mutex::new(HashMap::new()),
            start: Instant::now(),
            routing: OnceLock::new(),
        });
        let advertise = self.advertise_addr;
        tokio::spawn(recv_loop(inner.clone()));
        let node = Node { inner };
        // Join the reserved routing group (no coordinator publisher of its
        // own). `spawn_group` pins it Eventual whatever this asks for.
        let routing_group = node.get_or_spawn(
            GroupId::new(ROUTING_GROUP),
            None,
            GroupProfile::from_mode(node.inner.config.mode),
        );
        if let Some(addr) = advertise {
            let _ = routing_group.set_entry("~addr", addr.into_bytes(), None);
        }
        // Feed gossiped `~addr` advertisements into the transport's address
        // book, so only seeds need out-of-band registration. Transports that
        // resolve peers another way ignore the calls (default `learn_peer`).
        tokio::spawn(sync_peer_addrs(
            node.inner.transport.clone(),
            node.inner.id.clone(),
            routing_group.entries_watch(),
        ));
        let _ = node.inner.routing.set(routing_group);
        node
    }
}

/// Keeps the transport's address book fed with gossiped `~addr`
/// advertisements via [`Transport::learn_peer`]. Each distinct advertised
/// value is taught once (including unparseable ones, so a bad value is never
/// re-taught every wakeup). Ends when the routing group's actor does.
async fn sync_peer_addrs<T: Transport>(
    transport: Arc<T>,
    local: NodeId,
    mut entries: watch::Receiver<NodeEntriesSnapshot>,
) {
    let mut taught: HashMap<NodeId, Vec<u8>> = HashMap::new();
    loop {
        let snapshot = entries.borrow_and_update().clone();
        for (node, kv) in snapshot.iter() {
            if *node == local {
                continue;
            }
            let Some(advertised) = kv.get("~addr") else {
                continue;
            };
            if taught.get(node).is_some_and(|seen| seen == advertised) {
                continue;
            }
            if let Ok(addr) = std::str::from_utf8(advertised) {
                transport.learn_peer(node, addr);
            }
            taught.insert(node.clone(), advertised.clone());
        }
        if entries.changed().await.is_err() {
            return;
        }
    }
}

/// The node's single receive loop: pulls inbound frames off the transport and
/// demuxes each to the right group actor by peeking its [`GroupId`].
async fn recv_loop<T: Transport>(inner: Arc<Inner<T>>) {
    // Loop until the transport reports it's shut down (`recv` returns `Err`).
    while let Ok(inbound) = inner.transport.recv().await {
        let Some(group) = groupnet_core::wire::peek_group(&inbound.msg) else {
            continue; // undecodable header — drop
        };
        let tx = inner
            .routes
            .lock()
            .expect("routes mutex poisoned")
            .get(&group)
            .map(Group::command_sender);
        if let Some(tx) = tx {
            // Bounded inbox: a full actor DROPS network events (gossip is
            // loss-tolerant and anti-entropy re-teaches anything missed) —
            // never unbounded memory under overload.
            let _ = tx.try_send(Event::Message {
                from: inbound.from,
                wire: inbound.msg,
            });
        }
    }
}
