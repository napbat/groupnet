//! Integration test: **External activation over the async runtime** — M5's
//! runtime face, where an epoch is closed by a conditional write to a store
//! outside the cluster and by nothing in the fabric at all.
//!
//! The anchor's *decision rules* are pure functions proved in `groupnet-core`,
//! and the `X`-rows are proved against the engine there. What only this layer
//! can prove is the half that lives in the driver: that a real [`Anchor`] is
//! loaded and conditionally written on the engine's prompt, that what comes
//! back is fed in as [`AnchorActivated`] / [`AnchorObserved`], and that the
//! record in the store and the leadership every node reads stay one story:
//!
//! * three nodes elect through the anchor — the top-ranked one wins the
//!   conditional write, every observer agrees, the edge reaches the event
//!   stream, and the record holds exactly that `(epoch, host)`;
//! * killing the host leaves a record nobody renews, and a survivor **steals**
//!   it once it is `TTL + steal_margin` stale, at a strictly higher epoch;
//! * the inert postures: an anchor on an `Eventual` group is never called even
//!   once, and an `External` group with no anchor sits at `(0, None)` quietly;
//! * [`Group::leave`] **releases** the record — stamped already-expired — so a
//!   successor claims well inside the TTL it would otherwise have waited out.
//!
//! What the driver does when the **store itself** is broken — unreachable for
//! everybody, cut for the incumbent alone, applying writes it reports `Unknown`
//! for, or refusing writes while its reads keep working — is
//! `external_faults.rs`, which carries its own copy of this fixture and its own
//! fault knobs. The split is by schedule family, the same way the DST suites
//! split.
//!
//! All waiting is a bounded poll on a predicate (`eventually_within`), never a
//! bare sleep.
//!
//! [`AnchorActivated`]: groupnet_core::Command::AnchorActivated
//! [`AnchorObserved`]: groupnet_core::Command::AnchorObserved

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use groupnet_core::anchor::AnchorRecord;
use groupnet_core::{Activation, HostedConfig, NodeId, placement};
use groupnet_runtime::{
    Anchor, AnchorCas, AnchorFuture, AnchorToken, AnchorWriteIf, Group, GroupEvent, GroupProfile,
    Leadership, Node, Role,
};
use groupnet_testkit::cluster::{
    MemCluster, NodeOpts, converged_within, eventually_within, spawn_mem_node,
};
use groupnet_transport_mem::{MemTransport, Network};
use tokio::sync::broadcast::error::RecvError;

/// The poll budget for every assertion here. Deliberately looser than the
/// harness default: no `External` group may claim before its boot guard (one
/// `lease_ms`), and every succession additionally waits out a whole record TTL
/// plus the steal margin. A genuine regression still reports in seconds.
const SETTLE: Duration = Duration::from_secs(12);

/// A brisk gossip cadence, so anchor rounds and renewals happen in wall-clock
/// milliseconds. It also sets the driver's tick period (half the tightest
/// deadline, i.e. ~7ms here), which [`LEASE_MS`] is two orders of magnitude
/// above — the sizing rule [`HostedConfig`] states.
const GOSSIP_MS: u64 = 15;

/// The record's TTL **and** the engine lease — one knob by design, because the
/// two describe the same authority seen from the two clocks. It is this tier's
/// boot guard too, so nothing is elected before it has passed once.
const LEASE_MS: u64 = 600;

/// How far past a record's expiry a claimant must wait before it may steal.
const STEAL_MARGIN_MS: u64 = 200;

/// The `External` profile every fixture here runs, at `lease_ms`.
fn external_profile(lease_ms: u64) -> GroupProfile {
    GroupProfile::hosted(HostedConfig {
        activation: Activation::External {
            steal_margin_ms: STEAL_MARGIN_MS,
        },
        lease_ms,
    })
}

/// Rendezvous ranking of `ids` for `group`, best first — the order row X1's
/// claim guard reads, so `ranked(..)[0]` is the only node that ever prompts
/// while all of them are alive.
fn ranked(group: &str, ids: &[&str]) -> Vec<NodeId> {
    let members: Vec<(NodeId, u32)> = ids.iter().map(|id| (NodeId::new(*id), 1)).collect();
    placement::owners(group, &members, ids.len())
}

// ---------------------------------------------------------------------------
// The fixture: one shared CAS object, one crippleable handle per node.
// ---------------------------------------------------------------------------

/// The single object the whole fleet contends for — a linearizable CAS register
/// behind a mutex, which is exactly what one S3 key is.
///
/// The version marker is a **counter**, not a hash of the record: an etag is
/// therefore unique per *write*, even for two writes storing identical bytes. A
/// driver that compared record contents where it should compare tokens would
/// pass against hashes and fail here, which is the point of the choice.
#[derive(Debug, Default)]
struct AnchorObject {
    state: Mutex<ObjectState>,
}

/// [`AnchorObject`]'s contents: the record and the version it stands at.
#[derive(Debug, Default)]
struct ObjectState {
    /// The record and its version, or `None` for an object that does not exist.
    held: Option<(AnchorRecord, u64)>,
    /// The next version to hand out; monotone for the object's whole life.
    next_version: u64,
}

impl AnchorObject {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// A `GET`: the record and the version a conditional write must carry.
    fn load(&self) -> Option<(AnchorRecord, AnchorToken)> {
        let state = self.state.lock().expect("anchor object mutex poisoned");
        state
            .held
            .as_ref()
            .map(|(record, version)| (record.clone(), AnchorToken::new(version.to_string())))
    }

    /// The conditional write: `Absent` succeeds only on an object that does not
    /// exist, `Matches` only on the exact version it names.
    fn store(&self, pre: &AnchorWriteIf, record: AnchorRecord) -> AnchorCas {
        let mut state = self.state.lock().expect("anchor object mutex poisoned");
        let allowed = match (pre, &state.held) {
            (AnchorWriteIf::Absent, None) => true,
            (AnchorWriteIf::Matches(token), Some((_, version))) => {
                token.as_str() == version.to_string()
            }
            (AnchorWriteIf::Absent, Some(_)) | (AnchorWriteIf::Matches(_), None) => false,
        };
        if !allowed {
            return AnchorCas::Mismatch;
        }
        let version = state.next_version;
        state.next_version += 1;
        state.held = Some((record, version));
        AnchorCas::Stored(AnchorToken::new(version.to_string()))
    }

    /// What the object says right now, for a test's assertions.
    fn record(&self) -> Option<AnchorRecord> {
        self.load().map(|(record, _)| record)
    }
}

/// One call a node's driver made against its anchor.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Call {
    /// A read.
    Load,
    /// A conditional write, with the precondition and the record it carried.
    Store(AnchorWriteIf, AnchorRecord),
}

/// One node's access to the shared [`AnchorObject`].
///
/// Every call is logged whether or not it is allowed through, so a fixture can
/// prove the driver *asked* — the shape `quorum.rs`'s `RecordingStore` uses,
/// for the same reason. The knobs stay wired up here and are simply never
/// turned on: every schedule that cripples a store lives in
/// `external_faults.rs`, which carries the same fixture and the mutators for
/// them.
#[derive(Debug)]
struct FakeAnchor {
    object: Arc<AnchorObject>,
    calls: Mutex<Vec<Call>>,
    /// Every `load` fails.
    fail_loads: AtomicBool,
    /// Every `store` fails **without touching the object**.
    fail_stores: AtomicBool,
    /// Every `store` **applies** and then reports [`AnchorCas::Unknown`] — the
    /// timed-out `PUT` that actually landed, and the one outcome no driver can
    /// resolve without a read-back.
    unknown_but_applied: AtomicBool,
}

impl FakeAnchor {
    /// A healthy handle onto `object`.
    fn healthy(object: &Arc<AnchorObject>) -> Arc<Self> {
        Arc::new(Self {
            object: object.clone(),
            calls: Mutex::new(Vec::new()),
            fail_loads: AtomicBool::new(false),
            fail_stores: AtomicBool::new(false),
            unknown_but_applied: AtomicBool::new(false),
        })
    }

    /// Every call this node's driver made, in order.
    fn calls(&self) -> Vec<Call> {
        self.calls.lock().expect("call log mutex poisoned").clone()
    }

    /// Every record this node *tried* to write, accepted or not.
    fn written(&self) -> Vec<AnchorRecord> {
        self.calls()
            .into_iter()
            .filter_map(|call| match call {
                Call::Store(_, record) => Some(record),
                Call::Load => None,
            })
            .collect()
    }

    fn log(&self, call: Call) {
        self.calls
            .lock()
            .expect("call log mutex poisoned")
            .push(call);
    }

    /// This handle as the trait object a profile is configured with.
    fn as_anchor(self: &Arc<Self>) -> Arc<dyn Anchor> {
        self.clone()
    }
}

impl Anchor for FakeAnchor {
    fn load(&self) -> AnchorFuture<'_, std::io::Result<Option<(AnchorRecord, AnchorToken)>>> {
        Box::pin(async move {
            self.log(Call::Load);
            if self.fail_loads.load(Ordering::SeqCst) {
                return Err(std::io::Error::other("the fixture's anchor is unreachable"));
            }
            Ok(self.object.load())
        })
    }

    fn store(
        &self,
        pre: AnchorWriteIf,
        record: AnchorRecord,
    ) -> AnchorFuture<'_, std::io::Result<AnchorCas>> {
        Box::pin(async move {
            self.log(Call::Store(pre.clone(), record.clone()));
            if self.fail_stores.load(Ordering::SeqCst) {
                return Err(std::io::Error::other("the fixture's anchor is unreachable"));
            }
            let outcome = self.object.store(&pre, record);
            if self.unknown_but_applied.load(Ordering::SeqCst) {
                // Applied — and the caller is told nothing at all.
                return Ok(AnchorCas::Unknown);
            }
            Ok(outcome)
        })
    }
}

// ---------------------------------------------------------------------------
// Cluster helpers.
// ---------------------------------------------------------------------------

/// One running cluster: ids, nodes and group handles, index-aligned, plus the
/// object every node's anchor handle points at.
struct Fleet {
    ids: Vec<NodeId>,
    nodes: Vec<Node<MemTransport>>,
    groups: Vec<Group>,
}

impl Fleet {
    /// The index of `id`, which every caller here knows is present.
    fn index_of(&self, id: &NodeId) -> usize {
        self.ids
            .iter()
            .position(|other| other == id)
            .expect("the node is one of ours")
    }

    /// Faithful process death, exactly as `leadership.rs` performs it: dropping
    /// the handles is not enough on its own (the node's receive loop owns an
    /// `Arc` of the same inner state), so the endpoint is re-registered, which
    /// closes the old inbox, ends that loop and tears the actors down — the
    /// anchor task included, which is why the record then goes unrenewed
    /// instead of being kept alive by a zombie.
    fn kill(&mut self, net: &Network, id: &NodeId) {
        let index = self.index_of(id);
        drop(self.groups.remove(index));
        drop(self.nodes.remove(index));
        self.ids.remove(index);
        let _evicted = net.endpoint(id.clone());
    }

    fn refs(&self) -> Vec<&Group> {
        self.groups.iter().collect()
    }
}

/// Brings `ids` up as an all-to-all cluster on `net`, each node joining `group`
/// under `profile` **carrying its own anchor handle** — the profile must be
/// applied on the join that creates the group, since a later
/// `join_group_with` is handed the existing handle and ignores what it asks
/// for.
fn spawn_fleet(
    net: &Network,
    group: &str,
    ids: &[&str],
    anchors: &[Arc<FakeAnchor>],
    profile: impl Fn(&Arc<FakeAnchor>) -> GroupProfile,
) -> Fleet {
    let mut fleet = Fleet {
        ids: Vec::new(),
        nodes: Vec::new(),
        groups: Vec::new(),
    };
    for (id, anchor) in ids.iter().zip(anchors) {
        let seeds: Vec<&str> = ids.iter().copied().filter(|other| other != id).collect();
        let opts = NodeOpts::new(group)
            .gossip_interval_ms(GOSSIP_MS)
            .group_profile(profile(anchor));
        let (node_id, node, joined) = spawn_mem_node(net, id, &seeds, &opts);
        fleet.ids.push(node_id);
        fleet.nodes.push(node);
        fleet.groups.push(joined);
    }
    fleet
}

/// The leadership the whole cluster agrees on, or `None` while it is still
/// settling: the same `(epoch, host)` everywhere, exactly one [`Role::Host`],
/// and every node's derived coordinator equal to that host — asserted as one
/// indivisible predicate so a poll can never catch half of it.
fn agreed(groups: &[&Group]) -> Option<Leadership> {
    let first = groups.first()?.leadership();
    first.host.as_ref()?;
    let all: Vec<Leadership> = groups.iter().map(|g| g.leadership()).collect();
    if all
        .iter()
        .any(|l| l.epoch != first.epoch || l.host != first.host)
    {
        return None;
    }
    if all.iter().filter(|l| l.role == Role::Host).count() != 1 {
        return None;
    }
    if groups.iter().any(|g| g.coordinator() != first.host) {
        return None;
    }
    Some(first)
}

/// Every [`GroupEvent::LeadershipChanged`] one group handle has published, in
/// order. A background drain is the shape a real consumer uses, and the only
/// way to observe an edge already superseded by the time the test looks.
type LeadershipLog = Arc<Mutex<Vec<(u64, Option<NodeId>)>>>;

fn watch_leadership(group: &Group) -> LeadershipLog {
    let log: LeadershipLog = Arc::new(Mutex::new(Vec::new()));
    let sink = log.clone();
    let mut events = group.events();
    tokio::spawn(async move {
        loop {
            match events.recv().await {
                Ok(GroupEvent::LeadershipChanged { epoch, host }) => sink
                    .lock()
                    .expect("event log mutex poisoned")
                    .push((epoch, host)),
                Ok(_) | Err(RecvError::Lagged(_)) => {}
                Err(RecvError::Closed) => return,
            }
        }
    });
    log
}

fn logged(log: &LeadershipLog) -> Vec<(u64, Option<NodeId>)> {
    log.lock().expect("event log mutex poisoned").clone()
}

/// Polls until the cluster agrees on a host, and returns the agreed pair.
async fn elected(fleet: &Fleet, what: &str) -> Leadership {
    let refs = fleet.refs();
    converged_within(&refs, SETTLE).await;
    eventually_within(what, SETTLE, || agreed(&refs).is_some()).await;
    agreed(&refs).expect("agreed on the poll just above")
}

// ---------------------------------------------------------------------------
// 1. The happy path.
// ---------------------------------------------------------------------------

/// Three `External` nodes elect through the anchor: the top-ranked one wins the
/// conditional write, every observer converges on that `(epoch, host)`, the
/// edge reaches every event stream, and the object itself holds exactly the
/// pair the cluster believes.
#[tokio::test]
async fn three_external_nodes_elect_through_the_anchor() {
    const GROUP: &str = "external-elect";
    const IDS: [&str; 3] = ["xe-a", "xe-b", "xe-c"];

    let object = AnchorObject::new();
    let anchors: Vec<Arc<FakeAnchor>> = IDS.iter().map(|_| FakeAnchor::healthy(&object)).collect();
    let net = Network::new();
    let fleet = spawn_fleet(&net, GROUP, &IDS, &anchors, |anchor| {
        external_profile(LEASE_MS).with_anchor(anchor.as_anchor())
    });
    let logs: Vec<LeadershipLog> = fleet.groups.iter().map(watch_leadership).collect();

    let lead = elected(&fleet, "the cluster to win an epoch at the anchor").await;
    let host = lead.host.clone().expect("agreement requires a named host");
    assert_eq!(
        host,
        ranked(GROUP, &IDS)[0],
        "row X1's guard is row 1's guard: only the rendezvous top-ranked live \
         member ever prompts"
    );
    assert!(lead.epoch >= 1, "epoch 0 never names a host: {lead:?}");

    // The store is the authority, so it has to say the same thing the cluster
    // believes — that is the whole tier in one assertion.
    let record = object
        .record()
        .expect("a host activated, so a record exists");
    assert_eq!(record.epoch, lead.epoch, "the record's epoch is the fence");
    assert_eq!(
        record.host, host,
        "the record names the host everyone reads"
    );

    // Only the winner ever wrote: the others are not candidates, so their
    // drivers are never prompted and never touch the object.
    for (id, anchor) in IDS.iter().zip(&anchors) {
        let wrote = anchor.written();
        if *id == host.as_str() {
            assert!(!wrote.is_empty(), "the host wrote nothing to the anchor");
            assert!(
                wrote
                    .iter()
                    .all(|r| r.host == host && r.epoch == lead.epoch),
                "the host wrote a record it does not hold: {wrote:?}"
            );
        } else {
            assert!(
                wrote.is_empty(),
                "{id} is not top-ranked and must never have written: {wrote:?}"
            );
        }
    }

    // And the same edge reached the event stream, on every node.
    eventually_within("every node to see the leadership edge", SETTLE, || {
        logs.iter()
            .all(|log| logged(log).contains(&(lead.epoch, Some(host.clone()))))
    })
    .await;
}

// ---------------------------------------------------------------------------
// 2. Succession by steal.
// ---------------------------------------------------------------------------

/// Kill the host and its record stops being renewed. A survivor steals it once
/// it is `TTL + steal_margin` stale, at a **strictly higher** epoch — the
/// anchor allocates, so a successor can never reuse the dead host's fence.
#[tokio::test]
async fn killing_the_host_lets_a_survivor_steal_the_record() {
    const GROUP: &str = "external-steal";
    const IDS: [&str; 3] = ["xs-a", "xs-b", "xs-c"];

    let object = AnchorObject::new();
    let anchors: Vec<Arc<FakeAnchor>> = IDS.iter().map(|_| FakeAnchor::healthy(&object)).collect();
    let net = Network::new();
    let mut fleet = spawn_fleet(&net, GROUP, &IDS, &anchors, |anchor| {
        external_profile(LEASE_MS).with_anchor(anchor.as_anchor())
    });

    let first = elected(&fleet, "the cluster to elect").await;
    let dead_id = first.host.clone().expect("agreement requires a named host");
    fleet.kill(&net, &dead_id);

    let survivors = fleet.refs();
    eventually_within("a survivor to steal the stale record", SETTLE, || {
        agreed(&survivors).is_some_and(|l| l.epoch > first.epoch)
    })
    .await;

    let after = agreed(&survivors).expect("agreed on the poll just above");
    let successor = after.host.clone().expect("agreement requires a named host");
    assert!(
        after.epoch > first.epoch,
        "a steal must allocate above the record it took: {} is not above {}",
        after.epoch,
        first.epoch
    );
    assert_ne!(successor, dead_id, "the killed node must not host again");
    assert!(
        fleet.ids.contains(&successor),
        "the successor must be a survivor, not {successor}"
    );

    // The succession is visible in the object, not only in the beliefs.
    let record = object.record().expect("the successor wrote one");
    assert_eq!(record.epoch, after.epoch);
    assert_eq!(record.host, successor);

    // And it was a *supersede* against the dead host's version, never a create
    // — the object was never empty for the successor to claim from.
    let stole = anchors.iter().flat_map(|a| a.calls()).any(|call| {
        matches!(call, Call::Store(AnchorWriteIf::Matches(_), record)
            if record.host == successor && record.epoch == after.epoch)
    });
    assert!(stole, "the successor never superseded the record it stole");
}

// ---------------------------------------------------------------------------
// 3. The inert postures.
// ---------------------------------------------------------------------------

/// Several gossip rounds' worth of digests — a run long enough that a Hosted
/// group would have elected (and renewed) several times over.
const A_REAL_RUN: u64 = 8;

/// An anchor configured on an `Eventual` group is **never called** — not once,
/// on any node, for the group's whole life. That is what makes it safe to hand
/// every node in a fleet the same profile builder and let each group's own
/// activation decide whether it means anything.
#[tokio::test]
async fn an_anchor_on_an_eventual_group_is_never_called() {
    const GROUP: &str = "external-inert-eventual";
    const IDS: [&str; 3] = ["xi-a", "xi-b", "xi-c"];

    let object = AnchorObject::new();
    let anchors: Vec<Arc<FakeAnchor>> = IDS.iter().map(|_| FakeAnchor::healthy(&object)).collect();
    let net = Network::new();
    // Eventual — the default posture — carrying an anchor anyway.
    let fleet = spawn_fleet(&net, GROUP, &IDS, &anchors, |anchor| {
        GroupProfile::eventual().with_anchor(anchor.as_anchor())
    });

    let refs = fleet.refs();
    converged_within(&refs, SETTLE).await;
    eventually_within("the cluster to run for a while", SETTLE, || {
        fleet
            .groups
            .iter()
            .all(|g| g.net_stats().digests_built >= A_REAL_RUN)
    })
    .await;

    for (id, anchor) in IDS.iter().zip(&anchors) {
        assert!(
            anchor.calls().is_empty(),
            "{id}'s Eventual group touched its anchor: {:?}",
            anchor.calls()
        );
    }
    assert!(
        object.record().is_none(),
        "an Eventual group wrote a record"
    );
    for (id, group) in fleet.ids.iter().zip(&fleet.groups) {
        assert_eq!(
            group.leadership(),
            Leadership {
                epoch: 0,
                host: None,
                role: Role::Follower,
            },
            "{id} elected something in a group with no election"
        );
    }
}

/// An `External` group with **no anchor** is the fail-safe posture: the
/// engine's prompts are dropped, nobody claims, and every node sits at
/// `(0, None)` quietly — no host, no edge, no error.
#[tokio::test]
async fn an_external_group_without_an_anchor_never_hosts() {
    const GROUP: &str = "external-anchorless";

    let cluster = MemCluster::builder(&["xn-a", "xn-b", "xn-c"])
        .group(GROUP)
        .gossip_interval_ms(GOSSIP_MS)
        .group_profile(external_profile(LEASE_MS))
        .spawn();
    let logs: Vec<LeadershipLog> = cluster.groups.iter().map(watch_leadership).collect();

    let refs: Vec<&Group> = cluster.groups.iter().collect();
    converged_within(&refs, SETTLE).await;
    eventually_within("the cluster to run past several leases", SETTLE, || {
        cluster
            .groups
            .iter()
            .all(|g| g.net_stats().digests_built >= A_REAL_RUN)
    })
    .await;

    for (id, group) in cluster.ids.iter().zip(&cluster.groups) {
        assert_eq!(
            group.leadership(),
            Leadership {
                epoch: 0,
                host: None,
                role: Role::Follower,
            },
            "{id} hosted a group with nothing to allocate its epoch"
        );
    }
    for (id, log) in cluster.ids.iter().zip(&logs) {
        assert!(
            logged(log).is_empty(),
            "{id} published a leadership edge: {:?}",
            logged(log)
        );
    }
}

// ---------------------------------------------------------------------------
// 4. Release on leave.
// ---------------------------------------------------------------------------

/// The lease for the release test, deliberately far above [`LEASE_MS`]: the
/// point of a release is that a successor does **not** wait out the TTL, and
/// that is only a meaningful claim when the TTL is long enough to tell the two
/// paths apart.
const RELEASE_LEASE_MS: u64 = 3_000;

/// The budget the successor must claim inside. Without the release it could not
/// possibly: a host renews at `RELEASE_LEASE_MS / 2`, so an unreleased record
/// stays live until at least `leave + 1_500 + STEAL_MARGIN_MS` — 1.7s, and up
/// to 3.2s. The budget *is* the assertion.
const RELEASE_DEADLINE: Duration = Duration::from_millis(1_200);

/// Leaving the group **stamps the record expired** instead of abandoning it, so
/// a successor claims well inside the TTL it would otherwise have waited out.
/// Courteous and best-effort: the epoch is untouched (a release decides
/// nothing, so it allocates nothing) and the write is still conditional on the
/// leaver's own version, so a successor that got there first could not have
/// been clobbered.
#[tokio::test]
async fn leaving_releases_the_record_so_a_successor_claims_early() {
    const GROUP: &str = "external-release";
    const IDS: [&str; 3] = ["xr-a", "xr-b", "xr-c"];

    let object = AnchorObject::new();
    let anchors: Vec<Arc<FakeAnchor>> = IDS.iter().map(|_| FakeAnchor::healthy(&object)).collect();
    let net = Network::new();
    let fleet = spawn_fleet(&net, GROUP, &IDS, &anchors, |anchor| {
        external_profile(RELEASE_LEASE_MS).with_anchor(anchor.as_anchor())
    });

    let first = elected(&fleet, "the cluster to elect").await;
    let leaver = first.host.clone().expect("agreement requires a named host");
    let index = fleet.index_of(&leaver);
    let live_expiry = object.record().expect("elected").expires_at_wall_ms;

    // --- Leave. Row 15's demotion is what the anchor task's leadership watch
    // sees, and what makes it stamp the record. ---
    fleet.groups[index].leave();

    eventually_within("the leaver to release its record", SETTLE, || {
        object
            .record()
            .is_some_and(|r| r.host == leaver && r.expires_at_wall_ms < live_expiry)
    })
    .await;
    let released = object.record().expect("still there, just expired");
    assert_eq!(
        released.epoch, first.epoch,
        "a release decides nothing, so it must allocate nothing"
    );
    assert_eq!(
        released.host, leaver,
        "a release does not change the holder"
    );
    // The write that did it was conditional on the leaver's own version.
    assert!(
        anchors[index].calls().iter().any(|call| {
            matches!(call, Call::Store(AnchorWriteIf::Matches(_), r)
                if r.expires_at_wall_ms < live_expiry)
        }),
        "the release was not an if-match write: {:?}",
        anchors[index].calls()
    );

    let survivors: Vec<&Group> = fleet
        .groups
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != index)
        .map(|(_, group)| group)
        .collect();
    eventually_within(
        "a successor to claim well inside the TTL the release skipped",
        RELEASE_DEADLINE,
        || agreed(&survivors).is_some_and(|l| l.epoch > first.epoch),
    )
    .await;

    let after = agreed(&survivors).expect("agreed on the poll just above");
    let successor = after.host.expect("agreement requires a named host");
    assert_ne!(successor, leaver, "the leaver must not have re-taken it");
    assert!(
        after.epoch > first.epoch,
        "a successor's epoch is strictly higher by construction"
    );
    assert_eq!(object.record().expect("claimed").host, successor);
}
