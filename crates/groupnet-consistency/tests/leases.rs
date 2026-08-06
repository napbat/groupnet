//! The coherence-lease tier (feature `leases`), over real nodes on the
//! in-memory transport.
//!
//! The deterministic properties live in the simulator; what these prove is the
//! part a simulator cannot: that the tokio shell — three tasks, two gossiped
//! entries and a TTL armed by somebody else's engine — actually delivers the
//! bargain. A healthy group serves under leases and invalidates at ack speed; a
//! silent one still resolves, at the lapse, on the stale node's own clock.

#![cfg(feature = "leases")]

use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

use groupnet_consistency::lease::{grant_entry_key, renewal_entry_key};
use groupnet_consistency::{
    AckLedger, CAP_ACKS, CAP_LEASE, CoherenceOutcome, LeaseConfig, LeaseState, LeaseView, Leases,
    PeerWrite, PeerWrites, WriteFeed, WriteToken, applied_by,
};
use groupnet_core::NodeId;
use groupnet_runtime::Group;
use groupnet_testkit::cluster::{
    MemCluster, NodeOpts, converged_within, eventually_within, spawn_mem_node,
};
use groupnet_transport_mem::Network;
use tokio::task::JoinHandle;

/// The convergence budget these tests carry: a genuine regression reports in
/// 5 s rather than eating the harness default on every assertion.
const SETTLE: Duration = Duration::from_secs(5);

/// The lease duration every test here runs at, and the unit every timing
/// assertion is stated in. Short enough that a lapse test finishes in about a
/// second; long enough that a healthy round trip over this transport (single
/// milliseconds) is unambiguously *well inside* it rather than merely under it.
const LEASE: Duration = Duration::from_millis(900);

/// `LEASE`'s derived tuning: renewed every 300 ms, 9 ms of rate margin.
fn lease_cfg() -> LeaseConfig {
    LeaseConfig::for_duration(LEASE)
}

/// The timings every node in these tests runs at: fast gossip, brisk
/// anti-entropy, one shared group.
fn opts() -> NodeOpts {
    NodeOpts::new("stores")
        .gossip_interval_ms(10)
        .anti_entropy_interval_ms(25)
}

const fn cap(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).expect("nonzero")
}

fn decode(bytes: &[u8]) -> Option<String> {
    String::from_utf8(bytes.to_vec()).ok()
}

/// The apply loop both tiers are built on: peer writes applied, then
/// acknowledged. Returned as a handle so a test can kill exactly this half of a
/// node's participation without touching the node itself.
fn apply_loop(group: &Group, me: &NodeId) -> JoinHandle<()> {
    let mut peers = PeerWrites::new(group.clone(), me.clone(), decode);
    let ledger = AckLedger::new(group.clone());
    tokio::spawn(async move {
        while let Some(event) = peers.next().await {
            match event {
                PeerWrite::Wrote { peer, token, .. } => ledger.record(&peer, token).await,
                PeerWrite::Gap {
                    peer,
                    missed_through,
                } => ledger.record(&peer, missed_through).await,
            }
        }
    })
}

/// One node's full participation in the tier, exactly as the advertisement
/// promises: a lease set (so it renews and grants) and an apply loop feeding an
/// [`AckLedger`] (so a writer's fast path can resolve on an acknowledgement).
fn participate(group: &Group, me: &NodeId) -> (Leases, JoinHandle<()>) {
    group
        .advertise_capabilities([CAP_ACKS, CAP_LEASE])
        .expect("the advertisement is enqueued");
    let leases = Leases::new(group.clone(), me.clone(), lease_cfg());
    (leases, apply_loop(group, me))
}

/// The ack half alone: applying and acknowledging, advertised as `acks` and
/// nothing more. A node in this shape holds no lease at all.
fn participate_with_acks_only(group: &Group, me: &NodeId) -> JoinHandle<()> {
    group
        .advertise_capabilities([CAP_ACKS])
        .expect("the advertisement is enqueued");
    apply_loop(group, me)
}

/// Waits until `view`'s lease is confirmed *and* the consumer's catch-up
/// affirmation takes — the two steps a booting reader owes before it may serve.
async fn serving(what: &str, view: &LeaseView) {
    eventually_within(what, SETTLE, || view.mark_caught_up()).await;
    assert!(view.valid(), "{what}: affirmed, so serving");
}

/// The healthy path, end to end: three nodes all renewing, all granting, all
/// acknowledging. Every reader reaches [`LeaseState::Serving`], and a coherent
/// write costs an ack round rather than a lease remainder.
#[tokio::test]
async fn a_healthy_group_serves_under_leases_and_invalidates_at_ack_speed() {
    let cluster = MemCluster::builder(&["fast-a", "fast-b", "fast-c"])
        .group("stores")
        .gossip_interval_ms(10)
        .anti_entropy_interval_ms(25)
        .spawn();
    let groups: Vec<&Group> = cluster.groups.iter().collect();
    converged_within(&groups, SETTLE).await;

    let mut sets = Vec::new();
    let mut applies = Vec::new();
    for (id, group) in cluster.ids.iter().zip(&cluster.groups) {
        let (leases, apply) = participate(group, id);
        sets.push(leases);
        applies.push(apply);
    }
    // Readers wait for a confirmation from every member advertising the
    // capability, so the advertisements must land before a lease can converge —
    // the rolling-upgrade order `CAP_LEASE` documents.
    eventually_within(
        "every node sees all three lease participants",
        SETTLE,
        || {
            cluster
                .groups
                .iter()
                .all(|group| group.members_with_capability(CAP_LEASE).len() == 3)
        },
    )
    .await;

    for (index, leases) in sets.iter().enumerate() {
        let view = leases.view();
        assert!(
            !view.valid(),
            "node {index} boots into NeedsResync: it slept through invalidations"
        );
        serving(&format!("node {index}"), &view).await;
        assert_eq!(view.state(), LeaseState::Serving);
        assert_eq!(view.lapses(), 0, "a healthy group lapses nobody");
    }

    let writer = &cluster.ids[0];
    let feed = WriteFeed::new(cluster.groups[0].clone(), cap(8), |key: &String| {
        key.clone().into_bytes()
    });
    let token = feed.publish(&"k1".to_owned()).await;

    let started = Instant::now();
    let outcome = sets[0]
        .invalidated_coherently(writer, token, LEASE * 3)
        .await;
    let elapsed = started.elapsed();

    assert_eq!(
        outcome,
        CoherenceOutcome::AllApplied,
        "every lease-holder applied it"
    );
    assert!(
        elapsed < LEASE / 2,
        "the healthy path costs an ack round, not a lease remainder (took {elapsed:?})"
    );
    for member in &cluster.ids[1..] {
        assert_eq!(
            applied_by(&cluster.groups[0], member, writer),
            Some(token),
            "…and it resolved because {member} really applied it"
        );
    }
    drop(applies);
}

/// The lapse path, and the resync that must follow it.
///
/// One reader stops participating — its apply loop and its lease set both die —
/// while its **node** stays alive, so membership reaps nothing and the writer's
/// only way out is the `~lease` entry expiring on the writer's own clock. That
/// is the whole bargain: a bound ended by the stale node's clock instead of by
/// anyone's patience.
#[tokio::test]
async fn a_silent_readers_lease_lapses_and_it_must_resync_before_serving_again() {
    let cluster = MemCluster::builder(&["lapse-a", "lapse-b", "lapse-c"])
        .group("stores")
        .gossip_interval_ms(10)
        .anti_entropy_interval_ms(25)
        .spawn();
    let groups: Vec<&Group> = cluster.groups.iter().collect();
    converged_within(&groups, SETTLE).await;

    let (a_id, b_id) = (cluster.ids[0].clone(), cluster.ids[1].clone());
    let (a_group, b_group) = (cluster.groups[0].clone(), cluster.groups[1].clone());
    let (a_leases, _a_apply) = participate(&a_group, &a_id);
    let (b_leases, b_apply) = participate(&b_group, &b_id);
    let (_c_leases, _c_apply) = participate(&cluster.groups[2], &cluster.ids[2]);

    eventually_within(
        "every node sees all three lease participants",
        SETTLE,
        || {
            cluster
                .groups
                .iter()
                .all(|group| group.members_with_capability(CAP_LEASE).len() == 3)
        },
    )
    .await;
    let b_view = b_leases.view();
    serving("the reader about to fall silent", &b_view).await;
    eventually_within("the writer sees both readers' leases", SETTLE, || {
        a_leases.holders().len() == 2
    })
    .await;

    // Kill both halves of B's participation — and neither its node nor its
    // membership. The `~lease` entry it last published stays on the wire and
    // must expire by TTL.
    b_apply.abort();
    drop(b_leases);

    let feed = WriteFeed::new(a_group.clone(), cap(8), |key: &String| {
        key.clone().into_bytes()
    });
    let token = feed.publish(&"k1".to_owned()).await;

    let started = Instant::now();
    let outcome = a_leases
        .invalidated_coherently(&a_id, token, LEASE * 4)
        .await;
    let elapsed = started.elapsed();

    assert_eq!(
        outcome,
        CoherenceOutcome::LeaseLapsed {
            stragglers: vec![b_id.clone()],
        },
        "the silent reader is excused by lapse, and named"
    );
    assert!(
        outcome.is_coherent(),
        "a lapse is a guarantee, not a timeout"
    );
    // The lower bound is the point. The writer's copy of B's `~lease` expires
    // one duration after the writer *adopted* B's last renewal, so the wait
    // cannot end before `D − (that renewal's age when B fell silent)`. Two
    // terms eat into it and neither is worth pinning: the age itself is up to
    // one renewal interval (D/3), and the publish, the abort and the timer's own
    // start all land after it. D/3 is what survives that slack — and it is still
    // the assertion that matters, because anything faster means the entry
    // expired early, which is a lease nobody granted.
    assert!(
        elapsed >= LEASE / 3,
        "the wait ended before the lease could have expired (took {elapsed:?})"
    );
    assert!(
        elapsed <= LEASE * 2,
        "the wait outlived the lease it was bounded by (took {elapsed:?})"
    );
    // The integration echo of the DST's disjointness property: by the time the
    // writer is excused, the reader it was excused from is already out of
    // service. Nothing that node holds may be served.
    assert!(
        !b_view.valid(),
        "the lapsed reader must not still be serving"
    );
    assert_ne!(b_view.state(), LeaseState::Serving);

    // Re-acquisition. A fresh lease life, a fresh apply loop — and a confirmed
    // lease is deliberately *not* enough on its own: this node missed exactly
    // the invalidations whose writers proceeded because it had lapsed.
    let (b_leases, _b_apply) = participate(&b_group, &b_id);
    let b_view = b_leases.view();
    assert!(!b_view.valid(), "a fresh lease life starts out of service");
    eventually_within("the restarted reader is confirmed again", SETTLE, || {
        b_leases.confirmed().is_some()
    })
    .await;
    assert!(
        !b_view.valid(),
        "a confirmed lease alone does not put a lapsed reader back into service"
    );
    serving("the resynced reader", &b_view).await;
}

/// A mixed deployment: two nodes run the whole tier, one runs the ack tier
/// alone. The ack-only node is a first-class member — alive, applying,
/// acknowledging — and it is *not* in any coherent write's wait set, because it
/// holds no lease and so has nothing cached it could serve stale.
#[tokio::test]
async fn a_node_without_the_lease_tier_neither_blocks_writes_nor_serves() {
    let cluster = MemCluster::builder(&["mix-a", "mix-b", "mix-c"])
        .group("stores")
        .gossip_interval_ms(10)
        .anti_entropy_interval_ms(25)
        .spawn();
    let groups: Vec<&Group> = cluster.groups.iter().collect();
    converged_within(&groups, SETTLE).await;

    let (a_id, b_id, c_id) = (
        cluster.ids[0].clone(),
        cluster.ids[1].clone(),
        cluster.ids[2].clone(),
    );
    let a_group = cluster.groups[0].clone();
    let (a_leases, _a_apply) = participate(&a_group, &a_id);
    let (b_leases, _b_apply) = participate(&cluster.groups[1], &b_id);
    // C: the ack tier and nothing else. No `Leases`, so no `LeaseView` exists
    // for it to serve under — that is a property of the type, not of a runtime
    // check, and the visible consequence is the absence below.
    let _c_apply = participate_with_acks_only(&cluster.groups[2], &c_id);

    eventually_within("a sees exactly the two lease participants", SETTLE, || {
        a_group.members_with_capability(CAP_LEASE) == vec![a_id.clone(), b_id.clone()]
    })
    .await;
    eventually_within(
        "…and exactly one of them holds a lease here",
        SETTLE,
        || a_leases.holders() == vec![b_id.clone()],
    )
    .await;
    assert_eq!(
        a_group.node_entry(&c_id, &renewal_entry_key("")),
        None,
        "a node with no lease set publishes no renewal, so nobody ever waits on it"
    );
    serving("the lease-tier reader", &b_leases.view()).await;

    let feed = WriteFeed::new(a_group.clone(), cap(8), |key: &String| {
        key.clone().into_bytes()
    });
    let token = feed.publish(&"k1".to_owned()).await;

    let started = Instant::now();
    let outcome = a_leases
        .invalidated_coherently(&a_id, token, LEASE * 3)
        .await;
    let elapsed = started.elapsed();

    assert_eq!(outcome, CoherenceOutcome::AllApplied);
    assert!(
        elapsed < LEASE / 2,
        "a non-participant must not cost the write a lease remainder (took {elapsed:?})"
    );
    assert_eq!(
        applied_by(&a_group, &c_id, &a_id),
        Some(token),
        "and C is a real participant in the tier below — just not in this wait set"
    );
}

/// The warm-up guard. A booting node is a **writer** before it is a converged
/// observer: for its first moments "nobody here holds a lease" is
/// indistinguishable from "I have not looked long enough", and resolving on it
/// would complete a coherent write while a reader this node has never heard of
/// serves the state that write invalidated.
///
/// The isolated case is a node with no peers at all, where the wait set is
/// empty and will stay empty: without the guard it resolves instantly, with it
/// the fast path is refused until the window closes.
#[tokio::test]
async fn a_fresh_writer_refuses_the_no_holders_fast_path_until_it_has_warmed_up() {
    let net = Network::new();
    let born = Instant::now();
    let (w_id, _w_node, w_group) = spawn_mem_node(&net, "warm-w", &[], &opts());
    let w_leases = Leases::new(w_group.clone(), w_id.clone(), lease_cfg());
    w_group
        .advertise_capabilities([CAP_ACKS, CAP_LEASE])
        .expect("the advertisement is enqueued");

    let token = WriteToken { epoch: 1, seq: 1 };
    let outcome = w_leases
        .invalidated_coherently(&w_id, token, Duration::from_millis(120))
        .await;
    assert!(
        matches!(outcome, CoherenceOutcome::TimedOut { .. }),
        "an empty wait set is not a fact yet, so the write must not resolve on it: {outcome:?}"
    );
    assert!(!outcome.is_coherent(), "and it says so");

    // …and once the window closes, an empty wait set is a real (if weak)
    // answer, exactly as it is for `applied_by_selected`.
    let deadline = Instant::now() + SETTLE;
    loop {
        let outcome = w_leases
            .invalidated_coherently(&w_id, token, Duration::from_millis(60))
            .await;
        if outcome == CoherenceOutcome::AllApplied {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the warm-up guard never released: {outcome:?}"
        );
    }
    // The guard is sized off the group's *effective* config, and its smallest
    // term is the refutation window — so it cannot have released before then.
    let floor = Duration::from_millis(w_group.config().suspect_timeout_ms);
    assert!(
        born.elapsed() >= floor,
        "the guard released after {:?}, inside the {floor:?} floor its own config sets",
        born.elapsed()
    );

    // A peer joins and takes a lease: the wait set fills, the guard has nothing
    // left to hold, and a real write resolves on the peer's acknowledgement.
    let (p_id, _p_node, p_group) = spawn_mem_node(&net, "warm-p", &["warm-w"], &opts());
    converged_within(&[&w_group, &p_group], SETTLE).await;
    let (_p_leases, _p_apply) = participate(&p_group, &p_id);
    eventually_within("the writer sees the peer's lease", SETTLE, || {
        w_leases.holders() == vec![p_id.clone()]
    })
    .await;

    let feed = WriteFeed::new(w_group.clone(), cap(8), |key: &String| {
        key.clone().into_bytes()
    });
    let real = feed.publish(&"k1".to_owned()).await;
    let started = Instant::now();
    let outcome = w_leases
        .invalidated_coherently(&w_id, real, LEASE * 3)
        .await;
    assert_eq!(outcome, CoherenceOutcome::AllApplied);
    assert!(
        started.elapsed() < LEASE / 2,
        "a warmed-up writer with a visible lease landscape is back on the fast path"
    );
}

/// The **reader's** boot guard: the read-side twin of the warm-up guard above,
/// and the one that has to hold in the shell rather than in the core.
///
/// `LeaseCore` confirms an empty roster vacuously. That is the right rule for a
/// node that knows the group and nobody in it leases, and a hole for one that is
/// still *learning* the group: a booting reader's own first renewal is
/// "confirmed by everybody" for exactly as long as it knows nobody, so a
/// consumer affirming catch-up would put it into service under a window no
/// granter ever gave — a stale fill under a lease that does not exist. The shell
/// fails that closed until the node has observed for a whole warm-up window: the
/// affirmation declines and no serve deadline is published.
#[tokio::test]
async fn a_booting_reader_cannot_serve_before_it_has_observed_the_group() {
    let net = Network::new();
    let (a_id, _a_node, a_group) = spawn_mem_node(&net, "boot-a", &["boot-b"], &opts());
    let (b_id, _b_node, b_group) = spawn_mem_node(&net, "boot-b", &["boot-a"], &opts());
    converged_within(&[&a_group, &b_group], SETTLE).await;
    let (_a_leases, _a_apply) = participate(&a_group, &a_id);
    let (b_leases, _b_apply) = participate(&b_group, &b_id);
    serving("the established reader", &b_leases.view()).await;

    // A third node boots into that established, leased cluster.
    let born = Instant::now();
    let (c_id, _c_node, c_group) = spawn_mem_node(&net, "boot-c", &["boot-a"], &opts());
    let (c_leases, _c_apply) = participate(&c_group, &c_id);
    let c_view = c_leases.view();

    // Hammer the affirmation from the very first instant — the eager consumer
    // the guard exists for. The window is sized off the group's *effective*
    // config and its smallest term is the refutation window, so the guard cannot
    // have released before then however few members this node has learned.
    let floor = Duration::from_millis(c_group.config().suspect_timeout_ms);
    while born.elapsed() < floor {
        assert!(
            !c_view.mark_caught_up(),
            "the affirmation took at {:?}, inside the {floor:?} floor the guard's own \
             config sets",
            born.elapsed()
        );
        assert!(!c_view.valid(), "…so no window opened either");
        assert_ne!(c_view.state(), LeaseState::Serving);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        c_view.lapses(),
        0,
        "a guarded reader has nothing to lapse out of: it never served"
    );

    // Past the window, with the roster learned and the grants converged, it is
    // an ordinary reader.
    serving("the booted reader", &c_view).await;
    assert!(c_leases.confirmed().is_some(), "and on a real confirmation");
    assert_eq!(
        c_leases.holders(),
        vec![a_id.clone(), b_id.clone()],
        "…having learned the very landscape the guard was waiting for"
    );
}

/// A departing reader retracts its renewal rather than making the group wait
/// out a lapse for it — and stops serving itself in the same motion.
#[tokio::test]
async fn leaving_retracts_the_renewal_and_closes_the_departing_readers_window() {
    let net = Network::new();
    let (a_id, _a_node, a_group) = spawn_mem_node(&net, "bye-a", &["bye-b"], &opts());
    let (b_id, _b_node, b_group) = spawn_mem_node(&net, "bye-b", &["bye-a"], &opts());
    converged_within(&[&a_group, &b_group], SETTLE).await;

    let (a_leases, _a_apply) = participate(&a_group, &a_id);
    let (b_leases, _b_apply) = participate(&b_group, &b_id);
    let b_view = b_leases.view();
    eventually_within("the writer sees the reader's lease", SETTLE, || {
        a_leases.holders() == vec![b_id.clone()]
    })
    .await;
    serving("the reader about to leave", &b_view).await;

    b_leases.leave().expect("the retraction is enqueued");
    assert!(
        !b_view.valid(),
        "a departing reader stops serving in the same motion, not one request later"
    );
    eventually_within("the retraction converges to the writer", SETTLE, || {
        a_leases.holders().is_empty()
    })
    .await;
    // The grant entry is deliberately left behind. It carries no TTL and is
    // harmless: it can only ever confirm a renewal the departing reader really
    // published, and a reader's confirmation is capped at what it published, so
    // a retired grant map cannot extend anyone's window.
    assert!(
        a_group.node_entry(&b_id, &grant_entry_key("")).is_some(),
        "the grant map survives a departure, and costs nothing when it does"
    );
}
