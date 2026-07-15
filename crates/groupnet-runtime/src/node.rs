use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use groupnet_core::{Config, GroupEngine, GroupId, NodeId, Status};
use groupnet_transport::Transport;
use tokio::sync::{mpsc, watch};

use crate::driver::{EVENTS_CAPACITY, Event, GroupViews, INBOX_CAPACITY, Publishers, group_task};
use tokio::sync::broadcast;
use crate::group::Group;
use crate::routing::Routing;

/// The reserved group every node joins to disseminate the inter-group routing
/// table. Its metadata holds `owner:<resource>` and `coord:<group>` entries.
pub(crate) const ROUTING_GROUP: &str = "__groupnet_routing__";

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
    pub fn join_group(&self, group: impl Into<GroupId>) -> Group {
        let group = group.into();
        if let Some(existing) = self
            .inner
            .routes
            .lock()
            .expect("routes mutex poisoned")
            .get(&group)
        {
            return existing.clone();
        }
        // Real groups announce their coordinator into the routing group.
        let routing = self.inner.routing.get().map(|g| g.command_sender());
        self.spawn_group(group, routing)
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

    /// Spawns a group actor. `routing` is the routing group's command channel
    /// (so this group can publish its coordinator), or `None` for the routing
    /// group itself.
    fn spawn_group(&self, group: GroupId, routing: Option<mpsc::Sender<Event>>) -> Group {
        let (tx, rx) = mpsc::channel(INBOX_CAPACITY);

        let engine = GroupEngine::new(
            group.clone(),
            self.inner.id.clone(),
            self.inner.seeds.iter().cloned(),
            self.inner.config.clone(),
        );

        // Seed the readable views from the engine's current truth. The engine
        // only emits change effects on an actual change, so a node that is (and
        // stays) its own coordinator would otherwise never publish an initial
        // value.
        let (coord_tx, coord_rx) = watch::channel(engine.coordinator().cloned());
        let (meta_tx, meta_rx) = watch::channel(Arc::new(BTreeMap::new()));
        let initial_members: Vec<NodeId> = engine.members().cloned().collect();
        let (members_tx, members_rx) = watch::channel(Arc::new(initial_members));
        let initial_statuses: BTreeMap<NodeId, Status> = engine
            .member_statuses()
            .map(|(n, s)| (n.clone(), s))
            .collect();
        let (statuses_tx, statuses_rx) = watch::channel(Arc::new(initial_statuses));
        let (entries_tx, entries_rx) = watch::channel(Arc::new(BTreeMap::new()));
        let (events_tx, _) = broadcast::channel(EVENTS_CAPACITY);

        // Tick often enough to service the tightest engine deadline (probe
        // timeouts are the shortest), so failure detection isn't lagged by a
        // coarse gossip-only cadence. Sampling at `TICKS_PER_DEADLINE`× the
        // tightest deadline bounds how late a deadline can fire to one tick; the
        // engine is idempotent under early/extra ticks, so oversampling is safe.
        const TICKS_PER_DEADLINE: u64 = 2;
        let cfg = &self.inner.config;
        let tightest_deadline_ms = cfg
            .gossip_interval_ms
            .min(cfg.probe_interval_ms)
            .min(cfg.probe_timeout_ms);
        let tick_period = Duration::from_millis((tightest_deadline_ms / TICKS_PER_DEADLINE).max(1));
        tokio::spawn(group_task(
            engine,
            rx,
            self.inner.transport.clone(),
            Publishers {
                coordinator: coord_tx,
                metadata: meta_tx,
                members: members_tx,
                statuses: statuses_tx,
                entries: entries_tx,
                events: events_tx.clone(),
            },
            routing,
            self.inner.start,
            tick_period,
        ));

        let handle = Group::new(
            group.clone(),
            self.inner.id.clone(),
            tx,
            GroupViews {
                coordinator: coord_rx,
                metadata: meta_rx,
                members: members_rx,
                statuses: statuses_rx,
                entries: entries_rx,
                events: events_tx,
            },
        );
        self.inner
            .routes
            .lock()
            .expect("routes mutex poisoned")
            .insert(group, handle.clone());
        handle
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

    /// Overrides the gossip interval (milliseconds). Lower is faster to
    /// converge but chattier.
    #[must_use]
    pub fn gossip_interval_ms(mut self, ms: u64) -> Self {
        self.config.gossip_interval_ms = ms.max(1);
        self
    }

    /// Replaces the full protocol [`Config`] (probe/suspect/dead timings,
    /// fanout, indirect probes). The narrow per-knob setters remain for the
    /// common cases.
    #[must_use]
    pub fn config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }

    /// Advertise a reachable address for this node, disseminated cluster-wide
    /// as the reserved `~addr` state entry on the routing group — so only
    /// seeds need out-of-band addressing and everyone else resolves peers
    /// from gossip ([`Group::node_entry`] / [`Node::peer_addr`]).
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
        // Join the reserved routing group (no coordinator publisher of its own).
        let routing_group = node.spawn_group(GroupId::new(ROUTING_GROUP), None);
        if let Some(addr) = advertise {
            let _ = routing_group.set_entry("~addr", addr.into_bytes(), None);
        }
        let _ = node.inner.routing.set(routing_group);
        node
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
