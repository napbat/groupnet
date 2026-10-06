use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use groupnet_core::{
    Activation, Config, GroupEngine, GroupId, GroupMode, HostedConfig, NodeId, RecoveredGrant,
};
use groupnet_network::{Network, Router};
use groupnet_transport::Transport;
use tokio::sync::{mpsc, watch};

use crate::anchor::{Anchor, AnchorTask, anchor_task};
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
mod protocols;
pub use builder::NodeBuilder;
pub use protocols::{Endpoint, Messages, Ordered, Peer, PeerImplementation, Unordered};

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
/// # fn demo(node: &groupnet_runtime::Node) {
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
///
/// A `Quorum` group additionally carries the **voter storage** the driver
/// honours [`Effect::PersistGrant`](groupnet_core::Effect::PersistGrant)
/// through — see [`with_voter_storage`](Self::with_voter_storage) — and an
/// `External` group the **anchor** it allocates epochs at, see
/// [`with_anchor`](Self::with_anchor). Neither is part of a profile's identity
/// (two profiles differing only in which file their store writes to, or which
/// bucket their anchor lives in, describe the same group), which is why this
/// type carries no `PartialEq`.
#[derive(Clone)]
pub struct GroupProfile {
    mode: GroupMode,
    /// Voter durability, `None` for the blackout posture.
    storage: Option<VoterStorage>,
    /// The external CAS register, `None` for the never-claims posture.
    anchor: Option<Arc<dyn Anchor>>,
}

impl std::fmt::Debug for GroupProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Neither the store nor the anchor carries a `Debug` bound —
        // deliberately, so an implementation is free to hold a client handle
        // and nothing else.
        f.debug_struct("GroupProfile")
            .field("mode", &self.mode)
            .field("storage", &self.storage)
            .field("anchor", &self.anchor.is_some())
            .finish_non_exhaustive()
    }
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
            storage: None,
            anchor: None,
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
            storage: None,
            anchor: None,
        }
    }

    /// Gives this group's **voter ledger** durability: what storage says was
    /// granted before this process started, and where to write what it grants
    /// next.
    ///
    /// Only a [`Quorum`](groupnet_core::Activation::Quorum) group has a ledger,
    /// and only a node named in its roster ever writes to one. Setting this on
    /// any other profile is legal and inert — the store is simply never called,
    /// and `recovered` is ignored rather than half-applied (which is what makes
    /// it safe to configure uniformly across a fleet whose groups differ).
    ///
    /// # What it buys, and what it does not
    ///
    /// Without it, a Quorum voter falls back on the engine's **boot blackout**:
    /// it refuses every new claimant for one `lease_ms` after start, on the
    /// argument that any grant it made before the crash has expired by then.
    /// That is a timing rule standing in for a durability one. With it, the
    /// granted pair is a floor that survives the restart outright — this voter
    /// can never grant that epoch to a second claimant however long it was
    /// down — and the claimant named in it may be re-granted **immediately**,
    /// so a restarted voter stops starving the sitting host for a lease.
    ///
    /// What it does not buy is a shorter blackout for anyone *else*: recovery
    /// restores the pair, never the instant it was granted at, so a recovered
    /// voter still applies the boot-anchored window to every new claimant. See
    /// [`RecoveredGrant`] for that argument in full, and in particular for why
    /// [`RecoveredGrant::none`] is the one statement that lifts the blackout —
    /// and may only be made by a driver that really did persist every grant.
    ///
    /// # Load recovery *before* joining
    ///
    /// `recovered` is a value, not a callback, because the join path is
    /// synchronous and spawns the group actor under the node's group-table
    /// lock: it can await nothing and must touch no disk. Read the store, then
    /// join.
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use groupnet_core::{Activation, HostedConfig, RecoveredGrant, VoterRoster};
    /// # use groupnet_runtime::{GrantStore, GroupProfile};
    /// # fn demo(
    /// #     node: &groupnet_runtime::Node,
    /// #     store: Arc<dyn GrantStore>,
    /// #     recovered: RecoveredGrant,
    /// #     voters: VoterRoster,
    /// # ) {
    /// // `recovered` was read off `store` before this call — no I/O happens
    /// // inside the join.
    /// let shard = node.join_group_with(
    ///     "shard-7",
    ///     GroupProfile::hosted(HostedConfig {
    ///         activation: Activation::Quorum { voters },
    ///         lease_ms: 2_000,
    ///     })
    ///     .with_voter_storage(recovered, store),
    /// );
    /// # let _ = shard;
    /// # }
    /// ```
    #[must_use]
    pub fn with_voter_storage(
        mut self,
        recovered: RecoveredGrant,
        store: Arc<dyn GrantStore>,
    ) -> Self {
        self.storage = Some(VoterStorage { recovered, store });
        self
    }

    /// Gives this group the **external CAS anchor** its epochs are allocated
    /// at: the linearizable register a claimant must win a conditional write on
    /// to become host.
    ///
    /// Only an [`External`](groupnet_core::Activation::External) group has an
    /// anchor. Setting this on any other profile is legal and inert — the
    /// anchor is simply never called, not even once — which is what makes it
    /// safe to configure **uniformly across a fleet** whose groups differ, in
    /// exactly the way [`with_voter_storage`](Self::with_voter_storage) is.
    /// (The reserved routing group is pinned `Eventual`, so an anchor set on it
    /// is inert for the same reason.)
    ///
    /// # No anchor, no host
    ///
    /// An `External` group configured **without** one is a supported,
    /// deliberately fail-safe posture, not a misconfiguration the runtime
    /// rescues: the engine's [`AnchorClaimDue`] prompts are dropped, this node
    /// never claims, and the group stays at `(0, None)` unless it *adopts* a
    /// pair some other node's driver won and beaconed. So a read-only member of
    /// an `External` group — one that must never host, but must follow whoever
    /// does — is configured by leaving the anchor off, and it costs nothing on
    /// the wire.
    ///
    /// # One anchor, one object, one group
    ///
    /// A process hosting several `External` groups gives each its own [`Anchor`]
    /// over its own object: the trait names no group, because the profile a
    /// group is joined under is what pairs them. Two groups sharing one object
    /// would share one epoch sequence and fight, and no rule in the tier can
    /// detect it.
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use groupnet_core::{Activation, HostedConfig};
    /// # use groupnet_runtime::{Anchor, GroupProfile};
    /// # fn demo(
    /// #     node: &groupnet_runtime::Node,
    /// #     anchor: Arc<dyn Anchor>,
    /// # ) {
    /// let shard = node.join_group_with(
    ///     "shard-7",
    ///     GroupProfile::hosted(HostedConfig {
    ///         activation: Activation::External {
    ///             steal_margin_ms: 500,
    ///         },
    ///         lease_ms: 2_000,
    ///     })
    ///     .with_anchor(anchor),
    /// );
    /// # let _ = shard;
    /// # }
    /// ```
    ///
    /// [`AnchorClaimDue`]: groupnet_core::Effect::AnchorClaimDue
    #[must_use]
    pub fn with_anchor(mut self, anchor: Arc<dyn Anchor>) -> Self {
        self.anchor = Some(anchor);
        self
    }

    /// The profile a bare [`Node::join_group`] joins under: whatever the
    /// node's own [`Config::mode`] says.
    const fn from_mode(mode: GroupMode) -> Self {
        Self {
            mode,
            storage: None,
            anchor: None,
        }
    }
}

/// The anchor wiring a group actually runs, or `None` for every group that
/// claims nothing.
///
/// Both halves must line up: an anchor configured on a group whose activation
/// is not `External` is inert (the uniform-fleet posture
/// [`GroupProfile::with_anchor`] documents), and an `External` group with no
/// anchor never claims (the fail-safe posture). `config` is the group's
/// **effective** config, so the routing group — force-pinned `Eventual` —
/// lands here as `None` whatever profile it was joined under.
fn external_anchor(
    config: &Config,
    anchor: Option<Arc<dyn Anchor>>,
) -> Option<(Arc<dyn Anchor>, u64, u64)> {
    let anchor = anchor?;
    let GroupMode::Hosted(hosted) = &config.mode else {
        return None;
    };
    let Activation::External { steal_margin_ms } = &hosted.activation else {
        return None;
    };
    Some((anchor, hosted.lease_ms, *steal_margin_ms))
}

struct Inner {
    id: NodeId,
    transport: Arc<Router>,
    seeds: Vec<NodeId>,
    config: Config,
    messaging: crate::messaging::Hub,
    ordered: Mutex<Option<groupnet_streams::OrderedProtocol>>,
    unordered: Mutex<
        Option<(
            groupnet_streams::UnorderedConfig,
            Option<groupnet_streams::UnorderedProtocol>,
        )>,
    >,
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
        self.join_group_with(
            group,
            GroupProfile::from_mode(self.inner.config.mode.clone()),
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

    /// Spawns a group actor and returns its handle, without touching the routes
    /// table — [`get_or_spawn`](Self::get_or_spawn) owns that, and calls this
    /// with the lock held, which is why nothing here may await.
    ///
    /// `routing` is the routing group's command channel (so this group can
    /// publish its coordinator), or `None` for the routing group itself.
    /// `profile` decides this group's mode, **its voter storage and its
    /// anchor**; every other tunable comes from the node's config.
    ///
    /// An `External` group carrying an anchor gets a **second** task beside the
    /// group actor — see [`anchor_task`] — because an anchor round is a network
    /// round trip to a store, and the actor pumping gossip must never be behind
    /// one.
    ///
    /// The voter ledger is *recovered*, never *read*, here: the caller loaded
    /// it before joining (see
    /// [`GroupProfile::with_voter_storage`]), so this stays a pure,
    /// non-awaiting function that the routes lock may safely be held across.
    fn spawn_group(
        &self,
        group: GroupId,
        routing: Option<mpsc::Sender<Event>>,
        profile: GroupProfile,
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
        let mut config = self.inner.config.clone();
        config.mode = if group.as_str() == ROUTING_GROUP {
            GroupMode::Eventual
        } else {
            profile.mode
        };

        // A profile carrying voter storage builds the engine through
        // `with_recovered`, so the ledger is a floor from the first tick rather
        // than something applied after the boot blackout has already been
        // armed. Outside `Activation::Quorum` the two constructors are the same
        // engine — a group with no voter ledger has nothing to restore.
        let reachable = self.inner.transport.reachable().borrow().clone();
        let (mut engine, store) = match profile.storage {
            Some(storage) => (
                GroupEngine::with_recovered(
                    group.clone(),
                    self.inner.id.clone(),
                    self.inner.seeds.iter().cloned(),
                    config.clone(),
                    storage.recovered,
                ),
                Some(storage.store),
            ),
            None => (
                GroupEngine::new(
                    group.clone(),
                    self.inner.id.clone(),
                    self.inner.seeds.iter().cloned(),
                    config.clone(),
                ),
                None,
            ),
        };
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
        let initial_members: Vec<NodeId> = engine.members().cloned().collect();
        let (members_tx, members_rx) = watch::channel(Arc::new(initial_members));
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
        let anchor_prompts =
            external_anchor(&config, profile.anchor).map(|(anchor, lease_ms, steal_margin_ms)| {
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
                wire: inbound.msg,
            });
        }
    }
}
