//! Multi-node tests over the in-memory transport: writes published on one
//! node arrive in order on another, ring overflow degrades to an explicit
//! gap, a node never reacts to its own writes, the frontier gives a true
//! read-your-writes barrier, a writer restart surfaces as an epoch-change
//! gap with honest barriers on both sides of it, and named feeds stay
//! isolated.

use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use groupnet_consistency::{
    Frontier, PeerWrite, PeerWrites, WriteFeed, WriteToken, advertised_head,
};
use groupnet_testkit::cluster::{NodeOpts, converged_within, spawn_mem_node};
use groupnet_transport_mem::Network;

const GROUP: &str = "stores";

/// The convergence budget these tests carried before the shared harness: a
/// genuine regression reports in 3 s, not the harness default.
const SETTLE: Duration = Duration::from_secs(3);

const fn cap(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).expect("nonzero")
}

/// The timings every node in these tests runs at: fast gossip, brisk
/// anti-entropy, one shared group.
fn opts() -> NodeOpts {
    NodeOpts::new(GROUP)
        .gossip_interval_ms(10)
        .anti_entropy_interval_ms(25)
}

async fn next_event(peers: &mut PeerWrites<String>) -> PeerWrite<String> {
    tokio::time::timeout(Duration::from_secs(5), peers.next())
        .await
        .expect("timed out waiting for a peer write")
        .expect("event stream ended")
}

fn decode(bytes: &[u8]) -> Option<String> {
    String::from_utf8(bytes.to_vec()).ok()
}

#[tokio::test]
async fn peer_writes_arrive_in_order_and_apply_locally() {
    let net = Network::new();
    let (a_id, _a_node, a_group) = spawn_mem_node(&net, "node-a", &["node-b"], &opts());
    let (b_id, _b_node, b_group) = spawn_mem_node(&net, "node-b", &["node-a"], &opts());
    converged_within(&[&a_group, &b_group], SETTLE).await;

    // Node B: local state holding a soon-stale copy, and a subscription.
    let fresh: Arc<Mutex<HashSet<String>>> = Arc::default();
    fresh.lock().expect("lock").insert("user:1".to_owned());
    let mut peers = PeerWrites::new(b_group, b_id, decode);

    // Node A publishes two writes; the tokens are the RYW session tokens.
    let feed = WriteFeed::new(a_group, cap(128), |key: &String| key.clone().into_bytes());
    let epoch = feed.epoch();
    assert_eq!(
        feed.publish(&"user:1".to_owned()).await,
        WriteToken { epoch, seq: 1 }
    );
    assert_eq!(
        feed.publish(&"user:2".to_owned()).await,
        WriteToken { epoch, seq: 2 }
    );
    assert_eq!(feed.last_token(), Some(WriteToken { epoch, seq: 2 }));

    // B observes them in publication order and applies each.
    for (expected_seq, expected) in [(1, "user:1"), (2, "user:2")] {
        match next_event(&mut peers).await {
            PeerWrite::Wrote { peer, token, key } => {
                assert_eq!(peer, a_id);
                assert_eq!(
                    token,
                    WriteToken {
                        epoch,
                        seq: expected_seq
                    }
                );
                assert_eq!(key, expected);
                fresh.lock().expect("lock").remove(&key);
            }
            PeerWrite::Gap { .. } => panic!("no gap expected"),
        }
    }
    assert!(
        !fresh.lock().expect("lock").contains("user:1"),
        "the peer's write must drop the stale local copy"
    );
    assert_eq!(peers.gaps_seen(), 0);
    assert_eq!(peers.lag(&a_id), Some(0), "fully caught up");
}

#[tokio::test]
async fn ring_overflow_degrades_to_an_explicit_gap() {
    let net = Network::new();
    let (a_id, _a_node, a_group) = spawn_mem_node(&net, "ov-a", &["ov-b"], &opts());
    let (b_id, _b_node, b_group) = spawn_mem_node(&net, "ov-b", &["ov-a"], &opts());
    converged_within(&[&a_group, &b_group], SETTLE).await;

    let mut peers = PeerWrites::new(b_group, b_id, decode);
    // A tiny ring: two slots.
    let feed = WriteFeed::new(a_group, cap(2), |key: &String| key.clone().into_bytes());
    let epoch = feed.epoch();

    // B tracks the feed normally first (cursor lands at w1's end)…
    feed.publish(&"w1".to_owned()).await;
    match next_event(&mut peers).await {
        PeerWrite::Wrote { key, .. } => assert_eq!(key, "w1"),
        PeerWrite::Gap { .. } => panic!("no gap yet"),
    }

    // …then A writes three more without B draining: w2 falls off the ring.
    for key in ["w2", "w3", "w4"] {
        feed.publish(&key.to_owned()).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await; // let gossip settle

    // B must learn it missed something — loudly — then catch the survivors.
    assert_eq!(
        next_event(&mut peers).await,
        PeerWrite::Gap {
            peer: a_id.clone(),
            missed_through: WriteToken { epoch, seq: 2 }
        },
        "an overflowed ring must surface as a gap, never a silent skip"
    );
    assert_eq!(
        next_event(&mut peers).await,
        PeerWrite::Wrote {
            peer: a_id.clone(),
            token: WriteToken { epoch, seq: 3 },
            key: "w3".to_owned()
        }
    );
    assert_eq!(
        next_event(&mut peers).await,
        PeerWrite::Wrote {
            peer: a_id,
            token: WriteToken { epoch, seq: 4 },
            key: "w4".to_owned()
        }
    );
    assert_eq!(peers.gaps_seen(), 1);
}

/// The advertised head tracks the writer's latest publish (as gossip shows
/// it), and is absent before any publish or for unknown peers.
#[tokio::test]
async fn advertised_head_tracks_the_feed() {
    let net = Network::new();
    let (a_id, _a_node, a_group) = spawn_mem_node(&net, "hd-a", &["hd-b"], &opts());
    let (b_id, _b_node, b_group) = spawn_mem_node(&net, "hd-b", &["hd-a"], &opts());
    converged_within(&[&a_group, &b_group], SETTLE).await;

    assert_eq!(advertised_head(&b_group, &a_id), None, "no feed yet");
    assert_eq!(
        advertised_head(&b_group, &b_id),
        None,
        "unknown/no self feed"
    );

    let feed =
        WriteFeed::new(a_group, cap(8), |key: &String| key.clone().into_bytes()).with_epoch(9);
    feed.publish(&"w1".to_owned()).await;
    let last = feed.publish(&"w2".to_owned()).await;

    for _ in 0..300 {
        if advertised_head(&b_group, &a_id) == Some(last) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!(
        "head never reached B's view: {:?}",
        advertised_head(&b_group, &a_id)
    );
}

#[tokio::test]
async fn own_writes_are_ignored() {
    let net = Network::new();
    let (a_id, _a_node, a_group) = spawn_mem_node(&net, "self-a", &["self-b"], &opts());
    let (_b_id, _b_node, _b_group) = spawn_mem_node(&net, "self-b", &["self-a"], &opts());

    // Feed and subscription on the SAME node.
    let feed = WriteFeed::new(a_group.clone(), cap(8), |key: &String| {
        key.clone().into_bytes()
    });
    let mut own = PeerWrites::new(a_group, a_id, decode);
    feed.publish(&"local".to_owned()).await;

    // Nothing may arrive: a node does not notify itself.
    let quiet = tokio::time::timeout(Duration::from_millis(300), own.next()).await;
    assert!(quiet.is_err(), "own writes must not produce events");
}

#[tokio::test]
async fn read_your_writes_barrier_waits_for_the_applied_frontier() {
    let net = Network::new();
    let (a_id, _a_node, a_group) = spawn_mem_node(&net, "ryw-a", &["ryw-b"], &opts());
    let (b_id, _b_node, b_group) = spawn_mem_node(&net, "ryw-b", &["ryw-a"], &opts());
    converged_within(&[&a_group, &b_group], SETTLE).await;

    // Node B: stale local state, an apply loop, and a frontier.
    let fresh: Arc<Mutex<HashSet<String>>> = Arc::default();
    fresh.lock().expect("lock").insert("user:1".to_owned());

    let mut peers = PeerWrites::new(b_group, b_id, decode);
    let (frontier, view) = Frontier::new();
    let applied = Arc::clone(&fresh);
    tokio::spawn(async move {
        while let Some(event) = peers.next().await {
            match event {
                PeerWrite::Wrote { peer, token, key } => {
                    applied.lock().expect("lock").remove(&key);
                    frontier.advance(&peer, token);
                }
                PeerWrite::Gap {
                    peer,
                    missed_through,
                } => frontier.advance(&peer, missed_through),
            }
        }
    });

    // Node A writes; the returned token is the client's session token.
    let feed = WriteFeed::new(a_group, cap(64), |key: &String| key.clone().into_bytes());
    let token = feed.publish(&"user:1".to_owned()).await;

    // A client carrying (a, token) reads on B: the barrier resolves only
    // after the apply loop has actually applied — never a stale read.
    let reached = tokio::time::timeout(Duration::from_secs(5), view.reached(&a_id, token))
        .await
        .expect("barrier timed out");
    assert!(
        reached,
        "frontier must be reachable while the apply loop runs"
    );
    assert!(
        !fresh.lock().expect("lock").contains("user:1"),
        "after the barrier, the stale copy is provably gone"
    );
}

/// The restart story end to end: a writer dies and comes back under the
/// same id with a fresh ring. The subscriber gets an epoch-change gap that
/// covers the whole previous life, new-life barriers stay unsatisfied until
/// the gap is actually remediated, and old-life tokens remain satisfied
/// afterwards (the remediation covered them).
#[tokio::test]
async fn writer_restart_surfaces_as_a_gap_and_barriers_stay_honest() {
    let net = Network::new();
    let (a_id, a_node, a_group) = spawn_mem_node(&net, "rs-a", &["rs-b"], &opts());
    let (b_id, _b_node, b_group) = spawn_mem_node(&net, "rs-b", &["rs-a"], &opts());
    converged_within(&[&a_group, &b_group], SETTLE).await;

    let mut peers = PeerWrites::new(b_group, b_id, decode);
    let (frontier, view) = Frontier::new();

    // First life: explicit epoch 1, three writes, all applied on B.
    let feed = WriteFeed::new(a_group.clone(), cap(8), |key: &String| {
        key.clone().into_bytes()
    })
    .with_epoch(1);
    let mut old_life_last = WriteToken { epoch: 0, seq: 0 };
    for key in ["w1", "w2", "w3"] {
        old_life_last = feed.publish(&key.to_owned()).await;
    }
    for _ in 0..3 {
        match next_event(&mut peers).await {
            PeerWrite::Wrote { peer, token, .. } => frontier.advance(&peer, token),
            PeerWrite::Gap { .. } => panic!("no gap in the first life"),
        }
    }
    assert!(view.reached(&a_id, old_life_last).await);

    // Restart: tear the writer down completely, then boot a fresh node
    // under the same id (fresh engine, fresh ring — the amnesia case).
    drop(feed);
    drop(a_group);
    drop(a_node);
    let (_a2_id, _a2_node, a2_group) = spawn_mem_node(&net, "rs-a", &["rs-b"], &opts());
    let feed2 =
        WriteFeed::new(a2_group, cap(8), |key: &String| key.clone().into_bytes()).with_epoch(2);
    let new_token = feed2.publish(&"n1".to_owned()).await;
    assert_eq!(new_token, WriteToken { epoch: 2, seq: 1 });

    // B first sees the epoch change as a gap covering the whole old life…
    match next_event(&mut peers).await {
        PeerWrite::Gap {
            peer,
            missed_through,
        } => {
            assert_eq!(peer, a_id);
            assert_eq!(missed_through, WriteToken { epoch: 2, seq: 0 });
            assert!(
                missed_through > old_life_last,
                "epoch-major ordering: the gap covers every old-life token"
            );
            // …and until the gap is remediated, the new-life barrier must
            // NOT pass (the old watermark may not satisfy a new epoch).
            let premature =
                tokio::time::timeout(Duration::from_millis(100), view.reached(&a_id, new_token))
                    .await;
            assert!(
                premature.is_err(),
                "a new-life token must not be satisfied by an old-life watermark"
            );
            frontier.advance(&peer, missed_through);
        }
        PeerWrite::Wrote { .. } => panic!("the epoch change must surface before new writes"),
    }
    // Old-life tokens stay satisfied: the remediation covered that life.
    assert!(view.reached(&a_id, old_life_last).await);

    // …then the new life's write arrives and barriers normally.
    match next_event(&mut peers).await {
        PeerWrite::Wrote { peer, token, key } => {
            assert_eq!(token, new_token);
            assert_eq!(key, "n1");
            frontier.advance(&peer, token);
        }
        PeerWrite::Gap { .. } => panic!("only one gap expected"),
    }
    assert!(view.reached(&a_id, new_token).await);
}

/// Two subsystems sharing one group keep their feeds apart by name — no
/// cross-talk in either direction.
#[tokio::test]
async fn named_feeds_do_not_cross_talk() {
    let net = Network::new();
    let (_a_id, _a_node, a_group) = spawn_mem_node(&net, "nm-a", &["nm-b"], &opts());
    let (b_id, _b_node, b_group) = spawn_mem_node(&net, "nm-b", &["nm-a"], &opts());
    converged_within(&[&a_group, &b_group], SETTLE).await;

    let docs_feed = WriteFeed::named("docs", a_group.clone(), cap(8), |key: &String| {
        key.clone().into_bytes()
    });
    let index_feed = WriteFeed::named("index", a_group, cap(8), |key: &String| {
        key.clone().into_bytes()
    });
    let mut docs = PeerWrites::named("docs", b_group.clone(), b_id.clone(), decode);
    let mut index = PeerWrites::named("index", b_group, b_id, decode);

    docs_feed.publish(&"d1".to_owned()).await;
    index_feed.publish(&"i1".to_owned()).await;

    match next_event(&mut docs).await {
        PeerWrite::Wrote { key, .. } => assert_eq!(key, "d1"),
        PeerWrite::Gap { .. } => panic!("no gap expected"),
    }
    match next_event(&mut index).await {
        PeerWrite::Wrote { key, .. } => assert_eq!(key, "i1"),
        PeerWrite::Gap { .. } => panic!("no gap expected"),
    }
    // And nothing further on either: one write each, no cross-talk.
    let quiet = tokio::time::timeout(Duration::from_millis(200), docs.next()).await;
    assert!(quiet.is_err(), "docs feed must not see index writes");
    let quiet = tokio::time::timeout(Duration::from_millis(200), index.next()).await;
    assert!(quiet.is_err(), "index feed must not see docs writes");
}
