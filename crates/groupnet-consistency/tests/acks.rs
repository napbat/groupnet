//! The strong-coherence tier (feature `acks`), over real nodes on the
//! in-memory transport.

#![cfg(feature = "acks")]

use std::num::NonZeroUsize;
use std::time::Duration;

use groupnet_consistency::{
    AckBasis, AckLedger, CAP_ACKS, Group, PeerWrite, PeerWrites, PublishError, Recorded, WriteFeed,
    WriteToken, applied_by, applied_by_roster, applied_by_selected, applied_cluster_wide,
};
use groupnet_core::NodeId;
use groupnet_testkit::cluster::{NodeOpts, converged_within, eventually, spawn_mem_node};
use groupnet_transport_mem::Network;

/// The convergence budget this test carried before the shared harness: a
/// genuine regression reports in 3 s, not the harness default.
const SETTLE: Duration = Duration::from_secs(3);

const fn cap(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).expect("nonzero")
}

/// The timings every node in these tests runs at: a brisk gossip round, one
/// shared group.
fn opts() -> NodeOpts {
    NodeOpts::new("stores").gossip_interval_ms(25)
}

fn decode(bytes: &[u8]) -> Option<String> {
    String::from_utf8(bytes.to_vec()).ok()
}

fn feed(group: &Group, epoch: u64) -> WriteFeed<String> {
    WriteFeed::new(group.clone(), cap(8), |key: &String| {
        key.clone().into_bytes()
    })
    .with_epoch(epoch)
}

/// The apply loop every participant runs: apply, then record into a ledger.
fn apply_loop(group: &Group, me: &NodeId) {
    let mut peers = PeerWrites::new(group.clone(), me.clone(), decode);
    let ledger = AckLedger::new(group.clone());
    tokio::spawn(async move {
        while let Some(event) = peers.next().await {
            let (peer, token) = match event {
                PeerWrite::Wrote { peer, token, .. } | PeerWrite::Sealed { peer, token } => {
                    (peer, token)
                }
                PeerWrite::Gap {
                    peer,
                    missed_through,
                } => (peer, missed_through),
                PeerWrite::Renewed { peer, epoch, .. } => (peer, WriteToken { epoch, seq: 0 }),
            };
            ledger
                .record(&peer, token)
                .await
                .expect("the ledger is enqueued");
        }
    });
}

/// The write-side coherence half: a subscriber's ledger advertises what it
/// applied, the writer's cluster-wide wait resolves only then — and it
/// resolves within the timeout while the apply loop runs.
#[tokio::test]
async fn applied_acknowledgements_round_trip() {
    let net = Network::new();
    let (a_id, _a_node, a_group) = spawn_mem_node(&net, "ack-a", &["ack-b"], &opts()).await;
    let (b_id, _b_node, b_group) = spawn_mem_node(&net, "ack-b", &["ack-a"], &opts()).await;
    converged_within(&[&a_group, &b_group], SETTLE).await;
    apply_loop(&b_group, &b_id);

    let feed = feed(&a_group, 4);
    let token = feed.publish(&"w1".to_owned()).await.expect("advertised");

    let outcome = applied_cluster_wide(&a_group, &a_id, token, Duration::from_secs(5)).await;
    assert!(outcome.is_complete(), "{outcome:?}");
    assert!(!outcome.is_vacuous(), "B was waited on: {outcome:?}");
    assert_eq!(outcome.basis, AckBasis::AliveMembers);
    assert_eq!(outcome.selected, vec![b_id.clone()]);
    assert_eq!(outcome.acknowledged, vec![b_id.clone()]);
    assert!(outcome.missing.is_empty() && outcome.skipped.is_empty());
    assert_eq!(
        applied_by(&a_group, &b_id, &a_id),
        Some(token),
        "B's ledger advertises exactly the applied token"
    );
    // A token nobody has applied yet must time out, not lie — and name who.
    let future = WriteToken { epoch: 4, seq: 99 };
    let outcome = applied_cluster_wide(&a_group, &a_id, future, Duration::from_millis(200)).await;
    assert!(
        !outcome.is_complete(),
        "an unapplied token must not be acknowledged"
    );
    assert_eq!(outcome.missing, vec![b_id]);
    assert_eq!(outcome.acknowledged, Vec::<NodeId>::new());
}

/// A writer stopping on purpose waits for its peers to observe its seal
/// before it exits. The seal is acknowledged like a write: the wait for the
/// seal's token resolves only once the subscriber delivered the seal itself,
/// not merely the last write before it.
#[tokio::test]
async fn a_seal_is_acknowledged_like_a_write() {
    let net = Network::new();
    let (a_id, _a_node, a_group) =
        spawn_mem_node(&net, "seal-ack-a", &["seal-ack-b"], &opts()).await;
    let (b_id, _b_node, b_group) =
        spawn_mem_node(&net, "seal-ack-b", &["seal-ack-a"], &opts()).await;
    converged_within(&[&a_group, &b_group], SETTLE).await;
    apply_loop(&b_group, &b_id);

    let feed = feed(&a_group, 4);
    let last = feed.publish(&"w1".to_owned()).await.expect("advertised");
    assert!(
        applied_cluster_wide(&a_group, &a_id, last, Duration::from_secs(5))
            .await
            .is_complete()
    );
    let seal = WriteToken { epoch: 4, seq: 2 };
    assert!(
        !applied_cluster_wide(&a_group, &a_id, seal, Duration::from_millis(200))
            .await
            .is_complete(),
        "the last write's acknowledgement is not the seal's"
    );
    assert_eq!(feed.seal().await, Ok(seal));
    assert!(
        applied_cluster_wide(&a_group, &a_id, seal, Duration::from_secs(5))
            .await
            .is_complete(),
        "the subscriber acknowledged the seal"
    );
    assert_eq!(applied_by(&a_group, &b_id, &a_id), Some(seal));
}

/// The mixed deployment the capability selector exists for: three nodes, one
/// of which runs no ledger at all. The cluster-wide wait eats its whole
/// timeout on that member (pinned behavior); a wait scoped to the peers
/// advertising [`CAP_ACKS`] resolves — and selection is not a way to wish a
/// silent peer away, so selecting it still times out.
#[tokio::test]
async fn selected_waits_skip_members_that_do_not_participate() {
    let net = Network::new();
    let (a_id, _a_node, a_group) =
        spawn_mem_node(&net, "sel-a", &["sel-b", "sel-c"], &opts()).await;
    let (b_id, _b_node, b_group) =
        spawn_mem_node(&net, "sel-b", &["sel-a", "sel-c"], &opts()).await;
    let (c_id, _c_node, c_group) =
        spawn_mem_node(&net, "sel-c", &["sel-a", "sel-b"], &opts()).await;
    converged_within(&[&a_group, &b_group, &c_group], SETTLE).await;

    // B participates: an apply loop feeding a ledger — and it says so.
    apply_loop(&b_group, &b_id);
    b_group.advertise_capabilities([CAP_ACKS]).unwrap();
    // C does not: no ledger. It is a member in every other respect — alive
    // and gossiping — and advertises an honest empty set.
    c_group.advertise_capabilities(Vec::<&str>::new()).unwrap();

    // The advertisement must land BEFORE the writer narrows onto it: a
    // selector cannot see a peer that has not advertised yet, so scoping too
    // early would skip B as well and resolve vacuously. This is exactly the
    // advertisement-lag footgun `applied_by_selected` documents.
    eventually("a sees b, and only b, advertising acks", || {
        a_group.members_with_capability(CAP_ACKS) == vec![b_id.clone()]
    })
    .await;

    let feed = feed(&a_group, 7);
    let token = feed.publish(&"w1".to_owned()).await.expect("advertised");

    let outcome = applied_by_selected(
        &a_group,
        &a_id,
        token,
        |peer| a_group.node_has_capability(peer, CAP_ACKS),
        Duration::from_secs(5),
    )
    .await;
    assert!(outcome.is_complete(), "{outcome:?}");
    assert_eq!(outcome.basis, AckBasis::AliveSelected);
    assert_eq!(outcome.acknowledged, vec![b_id.clone()]);
    assert_eq!(
        outcome.skipped,
        vec![c_id.clone()],
        "the non-participant is reported as skipped, not silently dropped"
    );

    // Pinned: the unscoped wait still hangs on C, which never acknowledges.
    let outcome = applied_cluster_wide(&a_group, &a_id, token, Duration::from_millis(300)).await;
    assert!(
        !outcome.is_complete(),
        "cluster-wide still waits on the non-participating member"
    );
    assert_eq!(outcome.missing, vec![c_id.clone()]);
    // Selecting a peer that never acknowledges times out exactly as it should.
    let outcome = applied_by_selected(
        &a_group,
        &a_id,
        token,
        |peer| *peer == c_id,
        Duration::from_millis(300),
    )
    .await;
    assert!(
        !outcome.is_complete(),
        "a selected peer that never acknowledges must still time out"
    );
    assert_eq!(outcome.skipped, vec![b_id]);
}

/// The two weak resolutions an `is_complete` alone cannot tell from a real
/// acknowledgement: a selector that admits nobody, and a participant whose
/// capability advertisement has not converged. Both resolve at once, and
/// both are visible in the outcome — and the fixed-roster wait, whose
/// denominator cannot lag, refuses to resolve without the participant.
#[tokio::test]
async fn empty_and_lagged_selections_are_visible_and_a_roster_does_not_lag() {
    let net = Network::new();
    let (a_id, _a_node, a_group) = spawn_mem_node(&net, "lag-a", &["lag-b"], &opts()).await;
    let (b_id, _b_node, b_group) = spawn_mem_node(&net, "lag-b", &["lag-a"], &opts()).await;
    converged_within(&[&a_group, &b_group], SETTLE).await;

    // Nobody advertises CAP_ACKS and B runs no ledger yet.
    let feed = feed(&a_group, 9);
    let token = feed.publish(&"w1".to_owned()).await.expect("advertised");

    let lagged = applied_by_selected(
        &a_group,
        &a_id,
        token,
        |peer| a_group.node_has_capability(peer, CAP_ACKS),
        Duration::from_secs(5),
    )
    .await;
    assert!(lagged.is_complete(), "nobody selected, nobody missing");
    assert!(lagged.is_vacuous(), "{lagged:?}");
    assert_eq!(lagged.acknowledged, Vec::<NodeId>::new());
    assert_eq!(
        lagged.skipped,
        vec![b_id.clone()],
        "the lagging peer is named"
    );

    // A fixed roster has a known denominator: B is waited on whatever its
    // advertisement says, and is missing until it really applies.
    let roster = [a_id.clone(), b_id.clone(), b_id.clone()];
    let pending = applied_by_roster(
        &a_group,
        &a_id,
        token,
        roster.clone(),
        Duration::from_millis(200),
    )
    .await;
    assert_eq!(pending.basis, AckBasis::FixedRoster);
    assert_eq!(
        pending.selected,
        vec![b_id.clone()],
        "writer and duplicates drop out"
    );
    assert_eq!(pending.missing, vec![b_id.clone()]);
    assert!(!pending.is_complete() && !pending.is_vacuous());

    // B starts applying; a subscriber starts at each feed's current end, so
    // the roster wait is for the next write.
    apply_loop(&b_group, &b_id);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let second = feed.publish(&"w2".to_owned()).await.expect("advertised");
    let done = applied_by_roster(&a_group, &a_id, second, roster, Duration::from_secs(5)).await;
    assert!(done.is_complete(), "{done:?}");
    assert_eq!(done.acknowledged, vec![b_id]);
    assert_eq!(done.skipped, Vec::<NodeId>::new());
}

/// `record` reports what it did locally — an advance that was enqueued, or a
/// token the ledger already covers — and `republish` re-enqueues without
/// recording anything.
#[tokio::test]
async fn record_reports_the_local_advance_and_republish_re_enqueues() {
    let net = Network::new();
    let (a_id, _a_node, a_group) = spawn_mem_node(&net, "rec-a", &["rec-b"], &opts()).await;
    let (b_id, _b_node, b_group) = spawn_mem_node(&net, "rec-b", &["rec-a"], &opts()).await;
    converged_within(&[&a_group, &b_group], SETTLE).await;

    let ledger = AckLedger::new(b_group.clone());
    let writer = a_id.clone();
    let token = WriteToken { epoch: 3, seq: 5 };
    assert_eq!(ledger.record(&writer, token).await, Ok(Recorded::Advanced));
    assert_eq!(ledger.record(&writer, token).await, Ok(Recorded::Covered));
    assert_eq!(
        ledger
            .record(&writer, WriteToken { epoch: 3, seq: 4 })
            .await,
        Ok(Recorded::Covered),
        "monotone: a lower token changes nothing"
    );
    assert_eq!(ledger.republish().await, Ok(()));
    eventually("a sees b's ledger", || {
        applied_by(&a_group, &b_id, &writer) == Some(token)
    })
    .await;
}

/// The retry loop's reporting of a sustained refusal is unit-tested in the
/// crate (`publish::tests`): a live group actor's inbox cannot be held full
/// from here deterministically. What this pins is the integration half: once
/// `record` has advanced, the same token is `Covered` locally whatever the
/// enqueue did, so a caller's retry is `republish`, never a re-record — and
/// a sealed feed refuses a write instead of panicking.
#[tokio::test]
async fn a_kept_advance_is_retried_by_republish_not_by_re_recording() {
    let net = Network::new();
    let (_a_id, _a_node, a_group) = spawn_mem_node(&net, "keep-a", &[], &opts()).await;
    let ledger = AckLedger::new(a_group.clone());
    let writer = NodeId::new("writer");
    let token = WriteToken { epoch: 1, seq: 1 };
    assert!(ledger.record(&writer, token).await.is_ok());
    assert_eq!(ledger.record(&writer, token).await, Ok(Recorded::Covered));
    assert_eq!(ledger.republish().await, Ok(()));

    let feed = feed(&a_group, 5);
    let first = feed.publish(&"w1".to_owned()).await.expect("advertised");
    assert_eq!(feed.last_token(), Some(first));
    assert_eq!(feed.republish().await, Ok(()));
    assert_eq!(feed.seal().await, Ok(WriteToken { epoch: 5, seq: 2 }));
    assert_eq!(
        feed.publish(&"late".to_owned()).await,
        Err(PublishError::Sealed),
        "a sealed life refuses further writes instead of panicking"
    );
    assert_eq!(feed.last_token(), Some(first), "and records nothing");
}
