//! Integration test: **External activation when the store misbehaves** — the
//! fault half of M5's runtime face, split out of `external.rs` the way a DST
//! harness splits by schedule family, each file carrying its own copy of the
//! fixture and asserting the floors its own schedules earn.
//!
//! `external.rs` proves the tier works: elect, steal, the inert postures, and
//! release-on-leave. This file proves what the driver does when the anchor is
//! the thing that is broken — which is where every claim about the tier being
//! *fail-closed* is either earned or false:
//!
//! * an anchor that errors for everybody leaves the group **hostless while it
//!   is observed**, and healing it elects immediately — availability, never
//!   safety;
//! * cutting **only the incumbent's** anchor access lapses its engine lease and
//!   demotes it although nothing about the fabric changed (row 5's renew-on-rank
//!   is gated to `Settle`, so rank cannot save it) — and the group then stays
//!   hostless right past the instant a steal becomes entitled, because
//!   candidacy is rank-gated and the stranded node still ranks top;
//! * a store that **applies writes and then reports `Unknown`** is settled by
//!   read-back: exactly one host, at the epoch it wrote, with exactly one
//!   leadership edge — no double activation and no epoch burnt per round;
//! * a store whose **writes fail while its reads succeed** — the write throttle,
//!   the read-only window, the expired write credential — reports `Unknown` for
//!   every renewal and applies none of them. The read-back must call each one
//!   *lost*: the incumbent's lease **lapses** instead of being extended for ever
//!   off a record nobody is refreshing, and a successor then steals normally.
//!
//! All waiting is a bounded poll on a predicate (`eventually_within`), never a
//! bare sleep.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use groupnet_core::anchor::AnchorRecord;
use groupnet_core::{Activation, HostedConfig, NodeId, placement};
use groupnet_runtime::{
    Anchor, AnchorCas, AnchorFuture, AnchorToken, AnchorWriteIf, Group, GroupEvent, GroupProfile,
    Leadership, Node, Role,
};
use groupnet_testkit::cluster::{NodeOpts, converged_within, eventually_within, spawn_mem_node};
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

/// One node's access to the shared [`AnchorObject`], plus the knobs a test
/// cripples it by.
///
/// Every call is logged whether or not it is allowed through, so a fixture can
/// prove the driver *asked* — the shape `quorum.rs`'s `RecordingStore` uses,
/// for the same reason.
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

    /// A handle whose every write lands and then answers `Unknown`.
    fn ambiguous(object: &Arc<AnchorObject>) -> Arc<Self> {
        let anchor = Self::healthy(object);
        anchor.unknown_but_applied.store(true, Ordering::SeqCst);
        anchor
    }

    /// Cut (or restore) this node's access to the anchor, in both directions.
    fn set_reachable(&self, reachable: bool) {
        self.fail_loads.store(!reachable, Ordering::SeqCst);
        self.fail_stores.store(!reachable, Ordering::SeqCst);
    }

    /// Cut (or restore) this node's **writes only**, leaving reads working: the
    /// write throttle, the read-only window, the expired write credential.
    ///
    /// The driver reads a failed `store` as [`AnchorCas::Unknown`] by contract,
    /// so this is the half of an ambiguous write that **did not apply** — and,
    /// applied to a renewal, the one a pair-only read-back verdict would
    /// mistake for a win.
    fn set_writable(&self, writable: bool) {
        self.fail_stores.store(!writable, Ordering::SeqCst);
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
// 1. An anchor nobody can reach.
// ---------------------------------------------------------------------------

/// An anchor erroring for everybody costs **availability, never safety**: the
/// group stays hostless for as long as it is observed, with the top-ranked
/// node's driver attempting rounds the whole time — and the moment the anchor
/// heals, it elects.
#[tokio::test]
async fn an_unreachable_anchor_leaves_the_group_hostless_until_it_heals() {
    const GROUP: &str = "external-unreachable";
    const IDS: [&str; 3] = ["xu-a", "xu-b", "xu-c"];
    /// Rounds the candidate must burn before the stall is called — enough that
    /// a driver which gave up after the first would be caught.
    const STALLED_ROUNDS: usize = 3;

    let object = AnchorObject::new();
    let anchors: Vec<Arc<FakeAnchor>> = IDS.iter().map(|_| FakeAnchor::healthy(&object)).collect();
    for anchor in &anchors {
        anchor.set_reachable(false);
    }

    let net = Network::new();
    let fleet = spawn_fleet(&net, GROUP, &IDS, &anchors, |anchor| {
        external_profile(LEASE_MS).with_anchor(anchor.as_anchor())
    });
    let refs = fleet.refs();
    converged_within(&refs, SETTLE).await;

    let hostless = |at: &str| {
        for (id, group) in fleet.ids.iter().zip(&fleet.groups) {
            assert_eq!(
                group.leadership(),
                Leadership {
                    epoch: 0,
                    host: None,
                    role: Role::Follower,
                },
                "{id} left the initial belief {at}: nobody won an epoch, so \
                 nothing may have moved"
            );
        }
    };
    let attempts = || anchors.iter().map(|a| a.calls().len()).sum::<usize>();

    hostless("before the first prompt");
    eventually_within(
        "the candidate to burn several anchor rounds",
        SETTLE,
        || {
            hostless("mid-stall");
            attempts() >= STALLED_ROUNDS
        },
    )
    .await;
    hostless("after the stall");
    assert!(
        object.record().is_none(),
        "something was written to an anchor nobody could reach"
    );

    // --- Heal it. ---
    for anchor in &anchors {
        anchor.set_reachable(true);
    }
    eventually_within("the healed anchor to close an epoch", SETTLE, || {
        agreed(&refs).is_some()
    })
    .await;

    let lead = agreed(&refs).expect("agreed on the poll just above");
    let host = lead.host.clone().expect("agreement requires a named host");
    assert_eq!(host, ranked(GROUP, &IDS)[0]);
    assert_eq!(object.record().expect("elected").host, host);
}

// ---------------------------------------------------------------------------
// 2. Anchor connectivity is the availability axis.
// ---------------------------------------------------------------------------

/// Cut **only the incumbent's** anchor access and it demotes itself although
/// nothing about the fabric changed: row 5's renew-on-rank is gated to
/// `Settle`, so an `External` host that cannot reach the anchor lapses however
/// top-ranked it still looks to itself.
///
/// The group then stays **hostless** while that node is still the top-ranked
/// live member — right past the instant its abandoned record becomes stealable.
/// Candidacy is rank-gated, and a single node's anchor connectivity pinning the
/// group is the design's stated rank-pinned shape rather than a bug. Only once
/// the stranded node leaves the ranking does a rival step up and take it.
#[tokio::test]
async fn cutting_only_the_incumbents_anchor_lapses_its_lease_and_pins_the_group() {
    const GROUP: &str = "external-cut-incumbent";
    const IDS: [&str; 3] = ["xc-a", "xc-b", "xc-c"];
    /// Failed rounds the stranded node must burn while the group is pinned, on
    /// top of the elapsed-time bound below.
    const STRANDED_ROUNDS: usize = 3;

    let object = AnchorObject::new();
    let anchors: Vec<Arc<FakeAnchor>> = IDS.iter().map(|_| FakeAnchor::healthy(&object)).collect();
    let net = Network::new();
    let mut fleet = spawn_fleet(&net, GROUP, &IDS, &anchors, |anchor| {
        external_profile(LEASE_MS).with_anchor(anchor.as_anchor())
    });

    let first = elected(&fleet, "the cluster to elect").await;
    let incumbent = first.host.clone().expect("agreement requires a named host");
    let index = fleet.index_of(&incumbent);

    // --- Cut its anchor, and nothing else. It stays alive, reachable by every
    // peer, and top-ranked. ---
    let cut_at = Instant::now();
    let cut_calls = anchors[index].calls().len();
    anchors[index].set_reachable(false);

    eventually_within(
        "the incumbent to demote itself on lease lapse",
        SETTLE,
        || fleet.groups[index].leadership().role != Role::Host,
    )
    .await;
    let demoted = fleet.groups[index].leadership();
    assert_eq!(
        demoted.host, None,
        "a lapse leaves this node hostless at the epoch it kept: {demoted:?}"
    );
    assert_eq!(
        demoted.epoch, first.epoch,
        "a demotion keeps the epoch, so a later pair still fences against it"
    );

    // The rank-pinned shape, held past the instant a steal becomes entitled:
    // nobody else may bid while the stranded node is still top-ranked, so the
    // fence does not move however stale the record gets.
    let stealable_at = Duration::from_millis(LEASE_MS + STEAL_MARGIN_MS + 200);
    eventually_within(
        "the stranded incumbent to keep retrying past the steal boundary",
        SETTLE,
        || {
            for (id, group) in fleet.ids.iter().zip(&fleet.groups) {
                assert!(
                    group.leadership().epoch <= first.epoch,
                    "{id} moved the fence while the group was rank-pinned"
                );
            }
            cut_at.elapsed() >= stealable_at
                && anchors[index].calls().len() >= cut_calls + STRANDED_ROUNDS
        },
    )
    .await;
    assert_eq!(
        object
            .record()
            .expect("the incumbent's record is still there")
            .host,
        incumbent,
        "a record nobody was entitled to take must still name its holder"
    );

    // --- Now take it out of the ranking. ---
    fleet.kill(&net, &incumbent);
    let survivors = fleet.refs();
    eventually_within("a rival to steal the abandoned record", SETTLE, || {
        agreed(&survivors).is_some_and(|l| l.epoch > first.epoch)
    })
    .await;

    let after = agreed(&survivors).expect("agreed on the poll just above");
    let successor = after.host.expect("agreement requires a named host");
    assert_ne!(successor, incumbent);
    assert_eq!(object.record().expect("stolen").host, successor);
}

// ---------------------------------------------------------------------------
// 3. The ambiguous write that applied.
// ---------------------------------------------------------------------------

/// A store whose every write **applies and then reports `Unknown`** — the
/// timed-out `PUT` that actually landed. The driver settles each one by reading
/// the record back, so the node hosts the epoch it really wrote: exactly one
/// host, at that epoch, and **exactly one** leadership edge across the whole
/// run. A driver that re-claimed instead of reading back would climb an epoch
/// per round and leave a trail of edges behind it.
#[tokio::test]
async fn unknown_writes_that_applied_are_resolved_by_read_back() {
    const GROUP: &str = "external-ambiguous";
    const IDS: [&str; 3] = ["xa-a", "xa-b", "xa-c"];
    /// Renewal rounds to observe before the no-double-activation claim is made.
    /// Each one is another ambiguous write that had to be read back.
    const RENEWALS: usize = 3;

    let object = AnchorObject::new();
    let anchors: Vec<Arc<FakeAnchor>> =
        IDS.iter().map(|_| FakeAnchor::ambiguous(&object)).collect();
    let net = Network::new();
    let fleet = spawn_fleet(&net, GROUP, &IDS, &anchors, |anchor| {
        external_profile(LEASE_MS).with_anchor(anchor.as_anchor())
    });
    let logs: Vec<LeadershipLog> = fleet.groups.iter().map(watch_leadership).collect();

    let lead = elected(&fleet, "the cluster to resolve an ambiguous win").await;
    let host = lead.host.clone().expect("agreement requires a named host");
    let host_index = fleet.index_of(&host);
    let record = object
        .record()
        .expect("the write applied, whatever it reported");
    assert_eq!(
        (record.epoch, &record.host),
        (lead.epoch, &host),
        "the engine must believe exactly what the read-back found"
    );

    // Let it renew a few times — every renewal is another Unknown to settle.
    eventually_within(
        "the host to renew through several ambiguous rounds",
        SETTLE,
        || anchors[host_index].written().len() > RENEWALS,
    )
    .await;

    // Every write the host made was at the *same* epoch: a renewal decides
    // nothing so it allocates nothing, and a read-back that says "applied" must
    // not be re-litigated as a fresh claim.
    let epochs: Vec<u64> = anchors[host_index]
        .written()
        .iter()
        .map(|r| r.epoch)
        .collect();
    assert!(
        epochs.iter().all(|e| *e == lead.epoch),
        "the host burnt epochs re-claiming an ambiguous write it had won: {epochs:?}"
    );
    assert_eq!(
        agreed(&fleet.refs()).map(|l| l.epoch),
        Some(lead.epoch),
        "the fence moved during a run in which nothing was contested"
    );

    // ...and exactly one edge, everywhere. Re-reporting an epoch already held
    // is row X3, which announces nothing.
    for (id, log) in IDS.iter().zip(&logs) {
        assert_eq!(
            logged(log),
            vec![(lead.epoch, Some(host.clone()))],
            "{id} saw more than the one activation this run contains"
        );
    }
}

// ---------------------------------------------------------------------------
// 4. The ambiguous write that did *not* apply.
// ---------------------------------------------------------------------------

/// A store whose **writes fail while its reads keep working** — an S3 write
/// throttle, a read-only maintenance window, an expired write credential. The
/// driver reads a failed `store` as [`AnchorCas::Unknown`] by contract, so
/// every renewal ends in a read-back, and this is the case where the read-back
/// has almost nothing to go on: a renewal keeps the epoch and the host and only
/// moves the expiry, so what it *attempted* and what is *standing* are the same
/// `(epoch, host)` pair.
///
/// Judged on the pair alone, every one of those failed renewals resolves as
/// **won** — and then this node's engine lease is extended, indefinitely, by
/// writes that never happened, while the record it is supposedly renewing ages
/// out underneath it and a rival becomes entitled to steal at
/// `expires + steal_margin`. Two hosts, from two perfect clocks.
///
/// So the assertions here are the two halves of the fix:
///
/// * **The lease lapses.** The incumbent demotes, exactly as it does when the
///   anchor is unreachable outright — a store that cannot be written to cannot
///   extend an authority, whatever it says while failing.
/// * **The record never moved.** Byte-identical to what stood before the cut:
///   its expiry is the discriminator the read-back used, and it proves the
///   premise (nothing applied) rather than assuming it.
/// * **And a successor steals normally** once the stranded node leaves the
///   ranking — the ordinary succession, at a strictly higher epoch, against a
///   record that really was left to age out.
#[tokio::test]
async fn writes_that_fail_while_reads_work_lapse_the_lease_instead_of_extending_it() {
    const GROUP: &str = "external-write-throttled";
    const IDS: [&str; 3] = ["xw-a", "xw-b", "xw-c"];
    /// Failed write rounds the incumbent must burn before the claim is made, so
    /// "the lease lapsed" cannot be a driver that simply stopped trying.
    const THROTTLED_ROUNDS: usize = 3;

    let object = AnchorObject::new();
    let anchors: Vec<Arc<FakeAnchor>> = IDS.iter().map(|_| FakeAnchor::healthy(&object)).collect();
    let net = Network::new();
    let mut fleet = spawn_fleet(&net, GROUP, &IDS, &anchors, |anchor| {
        external_profile(LEASE_MS).with_anchor(anchor.as_anchor())
    });

    let first = elected(&fleet, "the cluster to elect").await;
    let incumbent = first.host.clone().expect("agreement requires a named host");
    let index = fleet.index_of(&incumbent);
    let standing = object.record().expect("elected, so a record exists");
    assert_eq!(standing.host, incumbent);

    // --- Throttle its writes. Reads keep working, which is the whole point:
    // the driver can still see the record, it just cannot move it. ---
    let cut_writes = anchors[index].written().len();
    anchors[index].set_writable(false);

    eventually_within(
        "the incumbent to burn several failed renewals",
        SETTLE,
        || anchors[index].written().len() >= cut_writes + THROTTLED_ROUNDS,
    )
    .await;
    // Its reads worked throughout — every failed write was followed by the
    // read-back that had to judge it.
    assert!(
        anchors[index]
            .calls()
            .iter()
            .filter(|call| **call == Call::Load)
            .count()
            > 0,
        "the driver never read the record back, so nothing was ever judged"
    );

    eventually_within(
        "the incumbent's lease to lapse rather than be extended by a write that never landed",
        SETTLE,
        || fleet.groups[index].leadership().role != Role::Host,
    )
    .await;
    let demoted = fleet.groups[index].leadership();
    assert_eq!(
        demoted.host, None,
        "a lapse leaves this node hostless at the epoch it kept: {demoted:?}"
    );
    assert_eq!(
        demoted.epoch, first.epoch,
        "a demotion keeps the epoch, so a later pair still fences against it"
    );
    assert_eq!(
        object.record().expect("still there"),
        standing,
        "a write the store refused still reached the object — the premise of this \
         fixture is that none of them did"
    );
    // Every write it attempted was the renewal of an epoch it held, at the pair
    // that was already standing: exactly the shape a pair-only read-back cannot
    // tell from a win.
    let attempted = anchors[index].written();
    assert!(
        attempted[cut_writes..]
            .iter()
            .any(|r| r.epoch == first.epoch && r.host == incumbent),
        "the throttled rounds were not renewals of the held epoch: {attempted:?}"
    );

    // --- Take it out of the ranking, and the succession is the ordinary one.
    fleet.kill(&net, &incumbent);
    let survivors = fleet.refs();
    eventually_within(
        "a successor to steal the record left to age out",
        SETTLE,
        || agreed(&survivors).is_some_and(|l| l.epoch > first.epoch),
    )
    .await;

    let after = agreed(&survivors).expect("agreed on the poll just above");
    let successor = after.host.expect("agreement requires a named host");
    assert_ne!(
        successor, incumbent,
        "the throttled node must not host again"
    );
    assert!(
        after.epoch > first.epoch,
        "a steal allocates above the record it took: {} is not above {}",
        after.epoch,
        first.epoch
    );
    let stolen = object.record().expect("the successor wrote one");
    assert_eq!(stolen.host, successor);
    assert_eq!(stolen.epoch, after.epoch);
}
