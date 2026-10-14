//! A writer that seals its feed before it stops ends its life at a known
//! position: a subscriber that delivered that life through the seal crosses
//! into the restarted writer's next epoch without a gap. Every restart the
//! subscriber cannot prove complete — no seal, or a seal it never saw — still
//! surfaces as the epoch-change gap, and a restarted writer's announcement
//! makes it surface at once rather than at its first write.

use std::num::NonZeroUsize;
use std::time::Duration;

use groupnet_consistency::{Frontier, PeerWrite, PeerWrites, WriteFeed, WriteToken};
use groupnet_core::NodeId;
use groupnet_runtime::Group;
use groupnet_testkit::cluster::{NodeOpts, converged_within, eventually_within, spawn_mem_node};
use groupnet_transport_mem::Network;

const GROUP: &str = "stores";

/// A genuine regression reports in 3 s, not the harness default.
const SETTLE: Duration = Duration::from_secs(3);

fn opts() -> NodeOpts {
    NodeOpts::new(GROUP)
        .gossip_interval_ms(10)
        .anti_entropy_interval_ms(25)
}

fn cap() -> NonZeroUsize {
    NonZeroUsize::new(8).expect("nonzero")
}

fn feed(group: Group, epoch: u64) -> WriteFeed<String> {
    WriteFeed::new(group, cap(), |key: &String| key.clone().into_bytes()).with_epoch(epoch)
}

fn decode(bytes: &[u8]) -> Option<String> {
    String::from_utf8(bytes.to_vec()).ok()
}

async fn next_event(peers: &mut PeerWrites<String>) -> PeerWrite<String> {
    tokio::time::timeout(Duration::from_secs(5), peers.next())
        .await
        .expect("timed out waiting for a peer write")
        .expect("event stream ended")
}

/// The epoch of the feed frame `group` currently holds for `peer`.
fn advertised_epoch(group: &Group, peer: &NodeId) -> Option<u64> {
    let frame = group.node_entry(peer, "~writes")?;
    Some(u64::from_le_bytes(frame.get(0..8)?.try_into().ok()?))
}

/// A planned stop: the old life seals after its last write, the subscriber
/// delivers the seal, and the restarted writer's announcement crosses into
/// the new life with no gap. Old-life barriers stay satisfied, and new-life
/// writes arrive and barrier as usual.
#[tokio::test]
async fn a_sealed_restart_renews_without_a_gap() {
    let net = Network::new();
    let (a_id, a_node, a_group) = spawn_mem_node(&net, "seal-a", &["seal-b"], &opts()).await;
    let (_b_id, _b_node, b_group) = spawn_mem_node(&net, "seal-b", &["seal-a"], &opts()).await;
    converged_within(&[&a_group, &b_group], SETTLE).await;
    let mut peers = PeerWrites::new(b_group, NodeId::new("seal-b"), decode);
    let (frontier, view) = Frontier::new();

    let old = feed(a_group.clone(), 7);
    let mut last = WriteToken { epoch: 0, seq: 0 };
    for key in ["w1", "w2"] {
        last = old.publish(&key.to_owned()).await;
    }
    let seal = old.seal().await;
    assert_eq!(seal, WriteToken { epoch: 7, seq: 3 });
    assert_eq!(old.seal().await, seal, "sealing again is the same seal");
    for seq in 1..=2 {
        match next_event(&mut peers).await {
            PeerWrite::Wrote { peer, token, .. } => {
                assert_eq!(token, WriteToken { epoch: 7, seq });
                frontier.advance(&peer, token);
            }
            other => panic!("the old life's writes first: {other:?}"),
        }
    }
    assert_eq!(
        next_event(&mut peers).await,
        PeerWrite::Sealed {
            peer: a_id.clone(),
            token: seal,
        }
    );
    frontier.advance(&a_id, seal);

    drop(old);
    drop(a_group);
    a_node.close().await;
    let (_reborn, _reborn_node, reborn_group) =
        spawn_mem_node(&net, "seal-a", &["seal-b"], &opts()).await;
    let new = feed(reborn_group, 9);
    new.republish().await;
    assert_eq!(
        next_event(&mut peers).await,
        PeerWrite::Renewed {
            peer: a_id.clone(),
            sealed: seal,
            epoch: 9,
        },
        "the announced new life crosses from the delivered seal"
    );
    frontier.advance(&a_id, WriteToken { epoch: 9, seq: 0 });
    assert!(
        view.reached(&a_id, last).await,
        "the old life stays covered"
    );

    let first = new.publish(&"n1".to_owned()).await;
    match next_event(&mut peers).await {
        PeerWrite::Wrote { peer, token, key } => {
            assert_eq!((token, key.as_str()), (first, "n1"));
            frontier.advance(&peer, token);
        }
        other => panic!("the new life's first write: {other:?}"),
    }
    assert!(view.reached(&a_id, first).await);
    assert_eq!(peers.gaps_seen(), 0);
}

/// A crash leaves the old life's end unknown. The restarted writer's
/// announcement alone — before it writes anything — surfaces the gap over
/// the whole previous life.
#[tokio::test]
async fn an_unsealed_restart_gaps_as_soon_as_the_new_life_is_announced() {
    let net = Network::new();
    let (a_id, a_node, a_group) = spawn_mem_node(&net, "crash-a", &["crash-b"], &opts()).await;
    let (_b_id, _b_node, b_group) = spawn_mem_node(&net, "crash-b", &["crash-a"], &opts()).await;
    converged_within(&[&a_group, &b_group], SETTLE).await;
    let mut peers = PeerWrites::new(b_group, NodeId::new("crash-b"), decode);

    let old = feed(a_group.clone(), 7);
    old.publish(&"w1".to_owned()).await;
    assert!(matches!(
        next_event(&mut peers).await,
        PeerWrite::Wrote { .. }
    ));

    drop(old);
    drop(a_group);
    a_node.close().await;
    let (_reborn, _reborn_node, reborn_group) =
        spawn_mem_node(&net, "crash-a", &["crash-b"], &opts()).await;
    feed(reborn_group, 9).republish().await;
    assert_eq!(
        next_event(&mut peers).await,
        PeerWrite::Gap {
            peer: a_id,
            missed_through: WriteToken { epoch: 9, seq: 0 },
        }
    );
    assert_eq!(peers.gaps_seen(), 1);
}

/// The old life sealed, but the subscriber never delivered that frame: by
/// the time it looks, the new life's frame has replaced it, as when the seal
/// was lost or propagated too late. Nothing proves the old life complete, so
/// the crossing is the gap.
#[tokio::test]
async fn a_seal_the_subscriber_never_saw_still_gaps() {
    let net = Network::new();
    let (a_id, a_node, a_group) = spawn_mem_node(&net, "lost-a", &["lost-b"], &opts()).await;
    let (_b_id, _b_node, b_group) = spawn_mem_node(&net, "lost-b", &["lost-a"], &opts()).await;
    converged_within(&[&a_group, &b_group], SETTLE).await;
    let mut peers = PeerWrites::new(b_group.clone(), NodeId::new("lost-b"), decode);

    let old = feed(a_group.clone(), 7);
    old.publish(&"w1".to_owned()).await;
    assert!(matches!(
        next_event(&mut peers).await,
        PeerWrite::Wrote { .. }
    ));
    // Sealed and replaced before the subscriber looks again.
    old.seal().await;
    drop(old);
    drop(a_group);
    a_node.close().await;
    let (_reborn, _reborn_node, reborn_group) =
        spawn_mem_node(&net, "lost-a", &["lost-b"], &opts()).await;
    let new = feed(reborn_group, 9);
    new.republish().await;
    eventually_within(
        "the new life's frame replaces the sealed one",
        SETTLE,
        || advertised_epoch(&b_group, &a_id) == Some(9),
    )
    .await;
    assert_eq!(
        next_event(&mut peers).await,
        PeerWrite::Gap {
            peer: a_id,
            missed_through: WriteToken { epoch: 9, seq: 0 },
        }
    );
}

/// Publishing after the seal would break the promise subscribers crossed
/// on, so the feed refuses it loudly.
#[tokio::test]
#[should_panic(expected = "WriteFeed::publish after the feed was sealed")]
async fn publishing_after_the_seal_panics() {
    let net = Network::new();
    let (_a_id, _a_node, a_group) = spawn_mem_node(&net, "late-a", &[], &opts()).await;
    let old = feed(a_group, 7);
    old.seal().await;
    drop(old.publish(&"late".to_owned()));
}
