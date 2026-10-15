use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use groupnet_core::{Config, GroupEngine, GroupId, GroupMode, NodeId};
use groupnet_network::{Network, Router};
use groupnet_transport::Transport;
use tokio::sync::{mpsc, watch};

use crate::anchor::{AnchorTask, anchor_task};
use crate::driver::{
    EVENTS_CAPACITY, Event, GroupTask, GroupViews, INBOX_CAPACITY, NodeEntriesSnapshot, Publishers,
    group_task, statuses_snapshot,
};
use crate::group::{Group, Leadership};
use crate::routing::Routing;
use crate::store::{GrantStore, VoterStorage};
use tokio::sync::broadcast;

mod builder;
mod discovery;
mod messaging;
mod profile;
mod protocols;
pub use builder::NodeBuilder;
pub use profile::GroupProfile;
use profile::Posture;
pub use protocols::{
    Endpoint, ManagedProtocol, Messages, Ordered, Peer, PeerImplementation, ReceiveBinding,
    Unordered,
};

/// The reserved group every node joins to disseminate the inter-group routing
/// table. Its metadata holds `owner:<resource>` and `coord:<group>` entries.
pub(crate) const ROUTING_GROUP: &str = "__groupnet_routing__";

struct Inner {
    id: NodeId,
    transport: Arc<Router>,
    seeds: Vec<NodeId>,
    config: Config,
    messaging: crate::messaging::Hub,
    /// Node-wide ordered-stream policy, validated at start and fixed for the node's life.
    ordered_config: groupnet_streams::ValidOrderedConfig,
    ordered: Mutex<Option<groupnet_streams::OrderedProtocol>>,
    /// Node-wide unordered policy, validated at start and fixed for the node's life.
    unordered_config: groupnet_streams::ValidUnorderedConfig,
    unordered: Mutex<Option<groupnet_streams::UnorderedProtocol>>,
    /// Joined groups (handle + inbox). Holding the `Group` makes `join_group`
    /// idempotent: a repeat join returns the existing handle instead of
    /// spawning a second, orphaned actor for the same group.
    routes: Mutex<HashMap<GroupId, Group>>,
    start: Instant,
    /// The routing system group, joined once at spawn.
    routing: OnceLock<Group>,
}

/// A running Groupnet node: owns a managed network and hosts group
/// memberships. Cheap to clone; every clone retains the network's lifetime.
pub struct Node {
    inner: Arc<Inner>,
    // Kept on public handles, not Inner: the receive task retains Inner while
    // awaiting packets and must not keep its own network alive indefinitely.
    pub(super) network: Network,
}

impl Clone for Node {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            network: self.network.clone(),
        }
    }
}

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("id", &self.inner.id)
            .finish_non_exhaustive()
    }
}

impl Node {
    /// Starts building a managed node with the given logical id.
    #[must_use]
    pub fn builder(id: NodeId) -> NodeBuilder {
        NodeBuilder::new(id)
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
        let routing = self.inner.routing.get().map(Group::command_sender);
        self.get_or_spawn(
            group.into(),
            routing,
            Posture::from_mode(self.inner.config.mode.clone()),
        )
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
        // `NodeBuilder::start`, before any `Node` handle exists to race with.
        let routing = self.inner.routing.get().map(Group::command_sender);
        self.get_or_spawn(group.into(), routing, profile.into())
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
        posture: Posture,
    ) -> Group {
        let mut routes = self.inner.routes.lock().expect("routes mutex poisoned");
        if let Some(existing) = routes.get(&group) {
            return existing.clone();
        }
        let handle = self.spawn_group(group.clone(), routing, posture);
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
    /// [`NodeBuilder::start`], before any `Node` handle exists.
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

    /// Builds a group's engine from its effective `config`.
    ///
    /// A profile carrying voter storage builds a recovering engine and restores
    /// it before it can start, so the ledger is a floor from the first tick
    /// rather than something applied after the boot blackout has already been
    /// armed. Outside `Activation::Quorum` the restore is a no-op — a group
    /// with no voter ledger has nothing to restore.
    fn build_engine(
        &self,
        group: &GroupId,
        config: &Config,
        storage: Option<VoterStorage>,
    ) -> (GroupEngine, Option<Arc<dyn GrantStore>>) {
        // `Config::validate` ignores `mode`, the only thing a group overrides,
        // so the node config `NodeBuilder::start` validated stays valid here.
        const VALIDATED: &str = "NodeBuilder::start validated the node config, and \
                                 Config::validate does not inspect the per-group mode";
        let (group, local) = (group.clone(), self.inner.id.clone());
        let seeds = self.inner.seeds.iter().cloned();
        match storage {
            Some(storage) => (
                GroupEngine::recovering(group, local, seeds, config.clone())
                    .expect(VALIDATED)
                    .restore(storage.recovered),
                Some(storage.store),
            ),
            None => (
                GroupEngine::new(group, local, seeds, config.clone()).expect(VALIDATED),
                None,
            ),
        }
    }

    /// Spawns a group actor and returns its handle, without touching the routes
    /// table — [`get_or_spawn`](Self::get_or_spawn) owns that, and calls this
    /// with the lock held, which is why nothing here may await.
    ///
    /// `routing` is the routing group's command channel (so this group can
    /// publish its coordinator), or `None` for the routing group itself.
    /// `posture` decides this group's mode, **its voter storage and its
    /// anchor**; every other tunable comes from the node's config.
    ///
    /// An `External` group carrying an anchor gets a **second** task beside the
    /// group actor — see [`anchor_task`] — because an anchor round is a network
    /// round trip to a store, and the actor pumping gossip must never be behind
    /// one.
    ///
    /// The voter ledger is *recovered*, never *read*, here: the caller loaded
    /// it before joining (see [`GroupProfile::Quorum`]), so this stays a pure,
    /// non-awaiting function that the routes lock may safely be held across.
    fn spawn_group(
        &self,
        group: GroupId,
        routing: Option<mpsc::Sender<Event>>,
        posture: Posture,
    ) -> Group {
        let (tx, rx) = mpsc::channel(INBOX_CAPACITY);

        // The group's *own* config: the node's, with only the mode replaced.
        // Every group has had its own `Arc<Config>` since M0, so a per-group
        // mode costs nothing new — and the routing group is force-pinned
        // Eventual here, the one place every join funnels through. It is
        // fabric plumbing carrying the cluster's routing table on every node;
        // electing a host for it would put an epoch-fenced authority in front
        // of the table every other group publishes into, for no gain. No
        // profile (and no node-wide `Config::mode`) can opt it in.
        // Pinning it also drops whatever voter storage or anchor the profile
        // carried: an Eventual group has neither a ledger nor an anchor.
        let posture = if group.as_str() == ROUTING_GROUP {
            Posture::from_mode(GroupMode::Eventual)
        } else {
            posture
        };
        let mut config = self.inner.config.clone();
        config.mode = posture.mode;

        let reachable = self.inner.transport.reachable().borrow().clone();
        let (mut engine, store) = self.build_engine(&group, &config, posture.storage);
        // Discovery contacts receive bounded periodic digest attempts but are
        // not members until an actual participant exchanges group protocol.
        engine.apply(groupnet_core::Command::SetBootstrapContacts(
            reachable.as_ref().clone(),
        ));

        // Seed the readable views from the engine's current truth. The engine
        // only emits change effects on an actual change, so a node that is (and
        // stays) its own coordinator would otherwise never publish an initial
        // value.
        let (coord_tx, coord_rx) = watch::channel(engine.coordinator().cloned());
        let (epoch, host) = engine.leadership();
        let (lead_tx, lead_rx) =
            watch::channel(Leadership::observed(epoch, host.cloned(), &self.inner.id));
        let (meta_tx, meta_rx) = watch::channel(Arc::new(BTreeMap::new()));
        let initial_members: Arc<[NodeId]> = engine.members().cloned().collect();
        let (members_tx, members_rx) = watch::channel(initial_members);
        let (statuses_tx, statuses_rx) = watch::channel(statuses_snapshot(&engine));
        let (entries_tx, entries_rx) = watch::channel(Arc::new(BTreeMap::new()));
        let (net_stats_tx, net_stats_rx) = watch::channel(groupnet_core::NetStats::default());
        let (events_tx, _) = broadcast::channel(EVENTS_CAPACITY);

        // The External tier's driver half: one task per anchored group, owning
        // the register, this group's command channel and its leadership watch.
        //
        // The prompt channel's capacity of **one** is the debounce
        // `Effect::AnchorClaimDue` requires — the effect is a repeated level
        // signal, and a `try_send` into a full slot is how a round trip longer
        // than the anti-entropy interval is stopped from stacking claims. The
        // task holds a *weak* command sender, so it can never keep a dead
        // group's actor (and its gossip, and its anchor renewals) alive.
        let anchor_prompts = posture.anchor.map(|(anchor, lease_ms, steal_margin_ms)| {
            let (prompt_tx, prompt_rx) = mpsc::channel(1);
            tokio::spawn(anchor_task(AnchorTask {
                anchor,
                local: self.inner.id.clone(),
                commands: tx.downgrade(),
                prompts: prompt_rx,
                leadership: lead_rx.clone(),
                lease_ms,
                steal_margin_ms,
                start: self.inner.start,
            }));
            prompt_tx
        });

        let tick_period = group_tick_period(&config);
        tokio::spawn(group_task(GroupTask {
            engine,
            inbox: rx,
            transport: self.inner.transport.clone(),
            publishers: Publishers {
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
            store,
            anchor_prompts,
            start: self.inner.start,
            tick_period,
        }));

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
            self.inner.messaging.group(),
        )
    }
}

fn group_tick_period(config: &groupnet_core::Config) -> Duration {
    // Service the tightest engine deadline at twice its cadence. The engine is
    // idempotent under early ticks, so oversampling bounds failure-detector lag.
    const TICKS_PER_DEADLINE: u64 = 2;
    let tightest_deadline_ms = config
        .gossip_interval_ms
        .min(config.probe_interval_ms)
        .min(config.probe_timeout_ms);
    Duration::from_millis((tightest_deadline_ms / TICKS_PER_DEADLINE).max(1))
}

/// Keeps the transport's address book fed with gossiped `~addr`
/// advertisements via [`Transport::learn_peer`]. Each distinct advertised
/// value is taught once (including unparseable ones, so a bad value is never
/// re-taught every wakeup). Ends when the router is cancelled or the routing
/// group's actor does.
async fn sync_peer_addrs(
    transport: Arc<Router>,
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
        tokio::select! {
            biased;
            () = transport.cancelled() => return,
            changed = entries.changed() => {
                if changed.is_err() {
                    return;
                }
            }
        }
    }
}

/// The node's single receive loop: pulls inbound frames off the transport and
/// demuxes each to the right group actor by peeking its [`GroupId`].
async fn recv_loop(inner: Arc<Inner>) {
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
                wire: inbound.msg.into(),
            });
        }
    }
}
