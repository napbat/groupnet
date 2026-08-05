//! Async multi-node fixtures: bring up real [`Node`]s over the in-memory
//! transport, and poll a cluster to a converged state without hand-rolling a
//! sleep loop per test.
//!
//! Gated behind the `cluster` feature, so the default testkit stays dep-free
//! and `groupnet-core`'s test graph never sees Tokio. Everything here must run
//! inside a Tokio runtime (i.e. under `#[tokio::test]`).
//!
//! The polls are *bounded and iteration-counted*, never fixed sleeps: a fast
//! machine finishes in one interval, a loaded one still gets its full budget
//! of attempts before the test fails.

use std::fmt;
use std::time::Duration;

use groupnet_core::{GroupId, NodeId};
use groupnet_runtime::{Group, GroupProfile, Node};
use groupnet_transport_mem::{MemTransport, Network};

/// [`POLL_INTERVAL`] in milliseconds — the single literal both the interval and
/// the default budget are built from, so neither can drift from the other.
const POLL_INTERVAL_MS: u64 = 20;

/// How long a bounded poll sleeps between two checks of its condition.
pub const POLL_INTERVAL: Duration = Duration::from_millis(POLL_INTERVAL_MS);

/// How many [`POLL_INTERVAL`] sleeps a default [`eventually`] budget buys.
pub const DEFAULT_POLLS: u64 = 250;

/// The bound [`eventually`] polls for: [`DEFAULT_POLLS`] × [`POLL_INTERVAL`].
pub const DEFAULT_TIMEOUT: Duration = Duration::from_millis(DEFAULT_POLLS * POLL_INTERVAL_MS);

/// Polls `cond` every [`POLL_INTERVAL`] until it holds, for up to
/// [`DEFAULT_TIMEOUT`]. Panics naming `what` if it never does.
///
/// Distributed state settles asynchronously, so tests wait for a *predicate*
/// rather than sleeping a guessed amount: correct machines return on the first
/// poll, and a genuine regression still fails loudly instead of flaking.
///
/// Use [`eventually_within`] where a site needs a longer budget than the
/// default.
pub async fn eventually(what: &str, cond: impl FnMut() -> bool) {
    eventually_within(what, DEFAULT_TIMEOUT, cond).await;
}

/// [`eventually`] with an explicit budget: polls every [`POLL_INTERVAL`] for
/// `timeout`'s worth of attempts (at least one), then panics naming `what`.
///
/// The budget is spent as a *count of polls*, so a slow machine cannot shorten
/// the number of chances the condition gets.
///
/// # Panics
/// If `cond` has not held once by the time the budget is spent — that is the
/// failure signal the harness exists to produce.
pub async fn eventually_within(what: &str, timeout: Duration, mut cond: impl FnMut() -> bool) {
    let polls = (timeout.as_millis() / POLL_INTERVAL.as_millis()).max(1);
    for _ in 0..polls {
        if cond() {
            return;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    panic!("timed out after {polls} polls ({timeout:?}) waiting for {what}");
}

/// Waits until every group in `groups` sees the whole cluster — i.e. each
/// group's member count equals `groups.len()`. Panics on timeout.
pub async fn converged(groups: &[&Group]) {
    converged_within(groups, DEFAULT_TIMEOUT).await;
}

/// [`converged`] with an explicit budget, for sites that want a convergence
/// regression reported faster than [`DEFAULT_TIMEOUT`].
pub async fn converged_within(groups: &[&Group], timeout: Duration) {
    let size = groups.len();
    eventually_within("membership convergence", timeout, || {
        groups.iter().all(|g| g.members().len() == size)
    })
    .await;
}

/// Per-node options shared by [`MemCluster`] and [`spawn_mem_node`]: the group
/// to join plus the [`Node`] builder knobs the cluster tests actually set.
/// Unset knobs keep the runtime's defaults.
#[derive(Debug, Clone)]
pub struct NodeOpts {
    group: GroupId,
    gossip_interval_ms: Option<u64>,
    anti_entropy_interval_ms: Option<u64>,
    advertise_addr: Option<String>,
    group_profile: Option<GroupProfile>,
}

impl NodeOpts {
    /// Options that join `group` at the runtime's default timings.
    #[must_use]
    pub fn new(group: impl Into<GroupId>) -> Self {
        Self {
            group: group.into(),
            gossip_interval_ms: None,
            anti_entropy_interval_ms: None,
            advertise_addr: None,
            group_profile: None,
        }
    }

    /// Overrides the gossip interval — see
    /// [`NodeBuilder::gossip_interval_ms`](groupnet_runtime::NodeBuilder::gossip_interval_ms).
    #[must_use]
    pub fn gossip_interval_ms(mut self, ms: u64) -> Self {
        self.gossip_interval_ms = Some(ms);
        self
    }

    /// Overrides just the anti-entropy cadence — see
    /// [`NodeBuilder::anti_entropy_interval_ms`](groupnet_runtime::NodeBuilder::anti_entropy_interval_ms).
    /// Applied after the gossip interval (which sets both), whatever order the
    /// two are set here.
    #[must_use]
    pub fn anti_entropy_interval_ms(mut self, ms: u64) -> Self {
        self.anti_entropy_interval_ms = Some(ms);
        self
    }

    /// Advertises a reachable address for the node — see
    /// [`NodeBuilder::advertise_addr`](groupnet_runtime::NodeBuilder::advertise_addr).
    #[must_use]
    pub fn advertise_addr(mut self, addr: impl Into<String>) -> Self {
        self.advertise_addr = Some(addr.into());
        self
    }

    /// Joins the group under an explicit consistency posture — the fixture
    /// then uses [`Node::join_group_with`](groupnet_runtime::Node::join_group_with)
    /// instead of a bare `join_group`.
    ///
    /// Unset (the default) is a plain `join_group`, i.e. the node config's own
    /// mode, i.e. `Eventual` — so every existing fixture keeps joining exactly
    /// the group it always did. Set it to
    /// [`GroupProfile::hosted`](groupnet_runtime::GroupProfile::hosted) for a
    /// cluster that must elect a host.
    #[must_use]
    pub fn group_profile(mut self, profile: GroupProfile) -> Self {
        self.group_profile = Some(profile);
        self
    }
}

/// Spawns one node on `net` under `id`, seeded with `seeds`, and joins the
/// group named by `opts`. Returns the node's id, the node (drop it and the
/// node dies), and its group handle.
///
/// The standalone form: for clusters, prefer [`MemCluster`]. This one exists
/// for tests that re-spawn a node under an id that already lived on the same
/// [`Network`] — the writer-restart case.
#[must_use]
pub fn spawn_mem_node(
    net: &Network,
    id: &str,
    seeds: &[&str],
    opts: &NodeOpts,
) -> (NodeId, Node<MemTransport>, Group) {
    let me = NodeId::new(id);
    let seeds = seeds.iter().map(|s| NodeId::new(*s)).collect();
    let (node, group) = spawn_one(net, me.clone(), seeds, opts);
    (me, node, group)
}

/// Spawns the node and joins its group in one motion — the shape
/// [`spawn_mem_node`] needs for a node added to (or restarted on) a cluster
/// that is already running.
fn spawn_one(
    net: &Network,
    id: NodeId,
    seeds: Vec<NodeId>,
    opts: &NodeOpts,
) -> (Node<MemTransport>, Group) {
    let node = build_node(net, id, seeds, opts);
    let group = join(&node, opts);
    (node, group)
}

/// The one place a fixture joins its group, so the profile-carrying and the
/// plain path can never drift: an unset [`NodeOpts::group_profile`] is a bare
/// `join_group` (the node config's own mode), exactly as before.
fn join(node: &Node<MemTransport>, opts: &NodeOpts) -> Group {
    match opts.group_profile {
        Some(profile) => node.join_group_with(opts.group.clone(), profile),
        None => node.join_group(opts.group.clone()),
    }
}

/// The one place a `Node` is actually built, so every fixture applies the same
/// knobs in the same order (`anti_entropy_interval_ms` must land *after*
/// `gossip_interval_ms`, which sets both cadences).
fn build_node(
    net: &Network,
    id: NodeId,
    seeds: Vec<NodeId>,
    opts: &NodeOpts,
) -> Node<MemTransport> {
    let mut builder = Node::builder(id.clone(), net.endpoint(id));
    for seed in seeds {
        builder = builder.seed(seed);
    }
    if let Some(ms) = opts.gossip_interval_ms {
        builder = builder.gossip_interval_ms(ms);
    }
    if let Some(ms) = opts.anti_entropy_interval_ms {
        builder = builder.anti_entropy_interval_ms(ms);
    }
    if let Some(addr) = &opts.advertise_addr {
        builder = builder.advertise_addr(addr.clone());
    }
    builder.spawn()
}

/// A running all-to-all cluster on one in-memory [`Network`]: every node is
/// seeded with every other, and all of them have joined the same group.
///
/// Holding the struct keeps the nodes alive; dropping it tears the cluster
/// down. The fields are public because tests reach past the handles routinely
/// (a second `join_group` on `nodes[0]`, `ids[1]` for an assertion, and so on),
/// and `ids`/`nodes`/`groups` are index-aligned.
#[derive(Debug)]
pub struct MemCluster {
    /// The fabric every node's endpoint is registered on.
    pub net: Network,
    /// Node ids, in the order they were named to the builder.
    pub ids: Vec<NodeId>,
    /// The running nodes, index-aligned with [`ids`](Self::ids).
    pub nodes: Vec<Node<MemTransport>>,
    /// Each node's handle to the joined group, index-aligned with
    /// [`ids`](Self::ids).
    pub groups: Vec<Group>,
}

impl MemCluster {
    /// Starts building a cluster of nodes named `ids`.
    #[must_use]
    pub fn builder(ids: &[&str]) -> MemClusterBuilder {
        MemClusterBuilder {
            ids: ids.iter().map(|s| NodeId::new(*s)).collect(),
            opts: NodeOpts::new(MemClusterBuilder::DEFAULT_GROUP),
            advertise: None,
        }
    }
}

/// Derives a node's advertised address from its id; `None` advertises nothing.
type AdvertiseFn = Box<dyn Fn(&NodeId) -> Option<String>>;

/// Builder for a [`MemCluster`] — see [`MemCluster::builder`].
pub struct MemClusterBuilder {
    ids: Vec<NodeId>,
    opts: NodeOpts,
    advertise: Option<AdvertiseFn>,
}

impl fmt::Debug for MemClusterBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemClusterBuilder")
            .field("ids", &self.ids)
            .field("opts", &self.opts)
            .finish_non_exhaustive()
    }
}

impl MemClusterBuilder {
    /// The group joined when [`group`](Self::group) is not called.
    pub const DEFAULT_GROUP: &'static str = "g";

    /// The group every node joins (default [`DEFAULT_GROUP`](Self::DEFAULT_GROUP)).
    #[must_use]
    pub fn group(mut self, group: impl Into<GroupId>) -> Self {
        self.opts.group = group.into();
        self
    }

    /// Overrides every node's gossip interval — see
    /// [`NodeOpts::gossip_interval_ms`].
    #[must_use]
    pub fn gossip_interval_ms(mut self, ms: u64) -> Self {
        self.opts = self.opts.gossip_interval_ms(ms);
        self
    }

    /// Overrides every node's anti-entropy cadence — see
    /// [`NodeOpts::anti_entropy_interval_ms`].
    #[must_use]
    pub fn anti_entropy_interval_ms(mut self, ms: u64) -> Self {
        self.opts = self.opts.anti_entropy_interval_ms(ms);
        self
    }

    /// Joins every node's group under an explicit consistency posture — see
    /// [`NodeOpts::group_profile`]. Unset, the cluster joins a plain
    /// (`Eventual`) group.
    #[must_use]
    pub fn group_profile(mut self, profile: GroupProfile) -> Self {
        self.opts = self.opts.group_profile(profile);
        self
    }

    /// Advertises a per-node address, derived from the node's id. Returning
    /// `None` leaves that node advertising nothing — see
    /// [`NodeOpts::advertise_addr`].
    #[must_use]
    pub fn advertise_addr(mut self, addr: impl Fn(&NodeId) -> Option<String> + 'static) -> Self {
        self.advertise = Some(Box::new(addr));
        self
    }

    /// Brings the cluster up: a fresh [`Network`], one node per id seeded with
    /// every other, each joined to the configured group.
    ///
    /// Must be called from within a Tokio runtime.
    #[must_use]
    pub fn spawn(self) -> MemCluster {
        let net = Network::new();
        // Every node exists before any joins its group — the bring-up order
        // of the tests this harness consolidated, so no engine starts
        // gossiping toward a peer that has not bound its endpoint yet.
        let mut nodes = Vec::with_capacity(self.ids.len());
        for id in &self.ids {
            let seeds = self.ids.iter().filter(|o| *o != id).cloned().collect();
            let mut opts = self.opts.clone();
            opts.advertise_addr = self.advertise.as_ref().and_then(|f| f(id));
            nodes.push(build_node(&net, id.clone(), seeds, &opts));
        }
        let groups = nodes.iter().map(|node| join(node, &self.opts)).collect();
        MemCluster {
            net,
            ids: self.ids,
            nodes,
            groups,
        }
    }
}
