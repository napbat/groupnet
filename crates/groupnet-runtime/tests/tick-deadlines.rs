//! Integration test: the group driver ticks each engine at the engine's own
//! next deadline ([`GroupEngine::next_deadline`]), not on a fixed interval.
//!
//! On a paused clock, an idle group's ticks over a window are bounded by the
//! probe and gossip rounds that fall in it, and a deadline a command or an
//! inbound frame brings forward is still met — no timer is lost because the
//! driver sleeps between deadlines.
//!
//! [`GroupEngine::next_deadline`]: groupnet_core::GroupEngine::next_deadline

use std::time::Duration;

use groupnet_core::{Config, NodeId};
use groupnet_runtime::{Group, Node};
use groupnet_testkit::cluster::{converged_within, eventually_within};
use groupnet_transport_mem::{MemLink, Network};

/// How long a paused-clock wait may take before a test reports a failure.
const SETTLE: Duration = Duration::from_secs(5);

/// Starts two nodes, `a` and `b`, that seed each other, and joins both to `g`.
async fn pair(config: &Config) -> (Vec<Node>, Vec<Group>) {
    let net = Network::new();
    let ids = [NodeId::new("a"), NodeId::new("b")];
    let mut nodes = Vec::new();
    for id in &ids {
        let peers: Vec<NodeId> = ids.iter().filter(|other| *other != id).cloned().collect();
        let mut builder = Node::builder(id.clone())
            .link(MemLink::new(net.endpoint(id.clone()), peers.clone()))
            .config(config.clone());
        for peer in peers {
            builder = builder.seed(peer);
        }
        nodes.push(builder.start().await.expect("memory link binds"));
    }
    let groups = nodes.iter().map(|node| node.join_group("g")).collect();
    (nodes, groups)
}

/// An idle two-node group at the default timings (probe 100 ms, gossip
/// 200 ms) ticks at most once per probe and once per gossip round: at most
/// 150 ticks in 10 s, where a fixed 25 ms driver tick spent 400. Measured:
/// 100 (every gossip round lands on a probe round). Before (d88ce7c, the
/// fixed `min(gossip, probe, probe_timeout) / 2` interval): 400.
#[tokio::test(start_paused = true)]
async fn an_idle_group_ticks_only_at_its_own_deadlines() {
    const WINDOW: Duration = Duration::from_secs(10);
    let config = Config::default();
    let (_nodes, groups) = pair(&config).await;
    let refs: Vec<&Group> = groups.iter().collect();
    converged_within(&refs, SETTLE).await;

    let before = groups[0].net_stats().ticks;
    tokio::time::sleep(WINDOW).await;
    let ticks = groups[0].net_stats().ticks - before;

    let window_ms = u64::try_from(WINDOW.as_millis()).expect("short window");
    let rounds = window_ms / config.probe_interval_ms + window_ms / config.gossip_interval_ms;
    assert!(
        ticks <= rounds,
        "{ticks} ticks in {WINDOW:?}, above the {rounds} probe and gossip rounds in it"
    );
    assert!(
        ticks >= window_ms / config.probe_interval_ms,
        "{ticks} ticks in {WINDOW:?}: probe rounds were skipped"
    );
}

/// With probe and gossip rounds an hour apart, a 1 s TTL write still expires
/// on time on both nodes: the command brings the writer's next deadline
/// forward, and the eager-push frame brings the receiver's forward, and the
/// driver re-arms on both instead of sleeping until the next round.
#[tokio::test(start_paused = true)]
async fn a_deadline_brought_forward_by_a_command_or_a_frame_is_met() {
    const HOUR_MS: u64 = 3_600_000;
    let config = Config {
        probe_interval_ms: HOUR_MS,
        gossip_interval_ms: HOUR_MS,
        ..Config::default()
    };
    let (nodes, groups) = pair(&config).await;
    let (a, b) = (NodeId::new("a"), NodeId::new("b"));
    // Routes first: the write's eager push is the only frame that will carry
    // it to `b` within the hour.
    eventually_within("routes between a and b", SETTLE, || {
        nodes[0].router().route_to(&b).is_some() && nodes[1].router().route_to(&a).is_some()
    })
    .await;
    groups[0]
        .set_entry("k", b"v".to_vec(), Some(Duration::from_secs(1)))
        .expect("inbox has room");
    eventually_within("b to adopt a's entry", SETTLE, || {
        groups[1].node_entry(&a, "k").is_some()
    })
    .await;

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    for (name, group) in ["a", "b"].iter().zip(&groups) {
        assert_eq!(
            group.node_entry(&a, "k"),
            None,
            "{name} still holds the entry past its TTL"
        );
    }
    let ticks = groups[0].net_stats().ticks;
    assert!(ticks <= 10, "{ticks} ticks with nothing due but one expiry");
}
