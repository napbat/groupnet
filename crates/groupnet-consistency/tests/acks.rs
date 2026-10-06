//! The strong-coherence tier (feature `acks`), over real nodes on the
//! in-memory transport.

#![cfg(feature = "acks")]

use std::num::NonZeroUsize;
use std::time::Duration;

use groupnet_consistency::{
    AckLedger, CAP_ACKS, PeerWrite, PeerWrites, WriteFeed, WriteToken, applied_by,
    applied_by_selected, applied_cluster_wide,
};
use groupnet_testkit::cluster::{NodeOpts, converged_within, eventually, spawn_mem_node};
use groupnet_transport_mem::Network;

/// The convergence budget this test carried before the shared harness: a
/// genuine regression reports in 3 s, not the harness default.
const SETTLE: Duration = Duration::from_secs(3);

const fn cap(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).expect("nonzero")
}

/// The timings every node in these tests runs at: fast gossip, brisk
/// anti-entropy, one shared group.
fn opts() -> NodeOpts {
    NodeOpts::new("stores")
        .gossip_interval_ms(10)
        .anti_entropy_interval_ms(25)
}

fn decode(bytes: &[u8]) -> Option<String> {
    String::from_utf8(bytes.to_vec()).ok()
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

    // B: apply loop that records into a ledger after each application.
    let mut peers = PeerWrites::new(b_group.clone(), b_id.clone(), decode);
    let ledger = AckLedger::new(b_group);
    tokio::spawn(async move {
        while let Some(event) = peers.next().await {
            match event {
                PeerWrite::Wrote { peer, token, .. } | PeerWrite::Sealed { peer, token } => {
                    ledger.record(&peer, token).await;
                }
                PeerWrite::Gap {
                    peer,
                    missed_through,
                } => ledger.record(&peer, missed_through).await,
                PeerWrite::Renewed { peer, epoch, .. } => {
                    ledger.record(&peer, WriteToken { epoch, seq: 0 }).await;
                }
            }
        }
    });

    let feed = WriteFeed::new(a_group.clone(), cap(8), |key: &String| {
        key.clone().into_bytes()
    })
    .with_epoch(4);
    let token = feed.publish(&"w1".to_owned()).await;

    assert!(
        applied_cluster_wide(&a_group, &a_id, token, Duration::from_secs(5)).await,
        "the wait resolves once every alive member acknowledged"
    );
    assert_eq!(
        applied_by(&a_group, &b_id, &a_id),
        Some(token),
        "B's ledger advertises exactly the applied token"
    );
    // A token nobody has applied yet must time out, not lie.
    let future = WriteToken { epoch: 4, seq: 99 };
    assert!(
        !applied_cluster_wide(&a_group, &a_id, future, Duration::from_millis(200)).await,
        "an unapplied token must not be acknowledged"
    );
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
    let mut peers = PeerWrites::new(b_group.clone(), b_id.clone(), decode);
    let ledger = AckLedger::new(b_group);
    tokio::spawn(async move {
        while let Some(event) = peers.next().await {
            match event {
                PeerWrite::Wrote { peer, token, .. } | PeerWrite::Sealed { peer, token } => {
                    ledger.record(&peer, token).await;
                }
                other => panic!("no gap or renewal in one life: {other:?}"),
            }
        }
    });

    let feed = WriteFeed::new(a_group.clone(), cap(8), |key: &String| {
        key.clone().into_bytes()
    })
    .with_epoch(4);
    let last = feed.publish(&"w1".to_owned()).await;
    assert!(applied_cluster_wide(&a_group, &a_id, last, Duration::from_secs(5)).await);
    let seal = WriteToken { epoch: 4, seq: 2 };
    assert!(
        !applied_cluster_wide(&a_group, &a_id, seal, Duration::from_millis(200)).await,
        "the last write's acknowledgement is not the seal's"
    );
    assert_eq!(feed.seal().await, seal);
    assert!(
        applied_cluster_wide(&a_group, &a_id, seal, Duration::from_secs(5)).await,
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
    let mut peers = PeerWrites::new(b_group.clone(), b_id.clone(), decode);
    let ledger = AckLedger::new(b_group.clone());
    tokio::spawn(async move {
        while let Some(event) = peers.next().await {
            match event {
                PeerWrite::Wrote { peer, token, .. } | PeerWrite::Sealed { peer, token } => {
                    ledger.record(&peer, token).await;
                }
                PeerWrite::Gap {
                    peer,
                    missed_through,
                } => ledger.record(&peer, missed_through).await,
                PeerWrite::Renewed { peer, epoch, .. } => {
                    ledger.record(&peer, WriteToken { epoch, seq: 0 }).await;
                }
            }
        }
    });
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

    let feed = WriteFeed::new(a_group.clone(), cap(8), |key: &String| {
        key.clone().into_bytes()
    })
    .with_epoch(7);
    let token = feed.publish(&"w1".to_owned()).await;

    assert!(
        applied_by_selected(
            &a_group,
            &a_id,
            token,
            |peer| a_group.node_has_capability(peer, CAP_ACKS),
            Duration::from_secs(5),
        )
        .await,
        "the wait resolves on the participating peers alone"
    );
    assert_eq!(
        applied_by(&a_group, &b_id, &a_id),
        Some(token),
        "and it resolved because B really acknowledged, not because it was skipped"
    );

    // Pinned: the unscoped wait still hangs on C, which never acknowledges.
    assert!(
        !applied_cluster_wide(&a_group, &a_id, token, Duration::from_millis(300)).await,
        "cluster-wide still waits on the non-participating member"
    );
    // Selecting a peer that never acknowledges times out exactly as it should.
    assert!(
        !applied_by_selected(
            &a_group,
            &a_id,
            token,
            |peer| *peer == c_id,
            Duration::from_millis(300)
        )
        .await,
        "a selected peer that never acknowledges must still time out"
    );
}
