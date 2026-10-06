//! Named seeds over *real* UDP: a node whose seed's name points at a dead
//! address — the rolling-restart wedge, where a peer's pod IP changed under a
//! stale resolution — heals once the name re-resolves, with no help from the
//! peer, which seeds nobody.

use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use groupnet_core::NodeId;
use groupnet_runtime::{NamedSeeds, Node, ResolveFuture, SeedEvent, SeedResolver};
use groupnet_testkit::cluster::eventually_within;
use groupnet_transport::link::{BoundLink, LinkConfig};
use groupnet_transport_mem::{MemLink, Network};
use groupnet_transport_router::RouterConfig;
use groupnet_transport_udp::UdpTransport;

/// The re-resolution cadence under test: short, so healing is quick.
const REFRESH: Duration = Duration::from_millis(50);

/// Convergence budget once the name points at the live peer.
const SETTLE: Duration = Duration::from_secs(5);

/// The gossip interval both nodes run.
const GOSSIP_MS: u64 = 30;

const SEED_NAME: &str = "node-b.peers.test:7000";

/// A one-name resolver whose answer the test moves.
#[derive(Clone)]
struct Moving(Arc<Mutex<SocketAddr>>);

impl SeedResolver for Moving {
    fn resolve<'a>(&'a self, name: &'a str) -> ResolveFuture<'a> {
        let answer = (name == SEED_NAME)
            .then(|| *self.0.lock().expect("answer lock"))
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound));
        Box::pin(async move { answer })
    }
}

/// A loopback address nothing listens on.
fn dead_addr() -> SocketAddr {
    let reservation = std::net::UdpSocket::bind("127.0.0.1:0").expect("reserve port");
    reservation.local_addr().expect("reserved address")
}

#[tokio::test]
async fn a_seed_whose_name_moves_is_rejoined_without_its_help() {
    let a_id = NodeId::new("node-a");
    let b_id = NodeId::new("node-b");
    let a_udp = UdpTransport::bind(a_id.clone(), "127.0.0.1:0")
        .await
        .expect("bind a");
    let b_udp = UdpTransport::bind(b_id.clone(), "127.0.0.1:0")
        .await
        .expect("bind b");
    let b_addr = b_udp.local_addr().expect("b addr");

    let answer = Arc::new(Mutex::new(dead_addr()));
    let moves = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&moves);
    let routing = RouterConfig {
        announce_interval: Duration::from_millis(GOSSIP_MS),
        ..RouterConfig::default()
    };
    let a = Node::builder(a_id.clone())
        .link(BoundLink::new(a_udp, LinkConfig::new(vec![b_id.clone()])))
        .routing(routing.clone())
        .gossip_interval_ms(GOSSIP_MS)
        .named_seeds(
            NamedSeeds::new(Moving(Arc::clone(&answer)))
                .seed(b_id.clone(), SEED_NAME)
                .refresh_interval(REFRESH)
                .on_event(move |event| {
                    if let SeedEvent::Resolved { addr, previous, .. } = event {
                        sink.lock().expect("moves lock").push((*addr, *previous));
                    }
                }),
        )
        .start()
        .await
        .expect("a UDP link binds");
    let b = Node::builder(b_id)
        .link(BoundLink::new(b_udp, LinkConfig::new(vec![a_id])))
        .routing(routing)
        .gossip_interval_ms(GOSSIP_MS)
        .start()
        .await
        .expect("b UDP link binds");
    let groups = [a.join_group("shard-1"), b.join_group("shard-1")];

    // Wedged: A gossips at a dead address and B knows nobody.
    tokio::time::sleep(REFRESH * 4).await;
    assert!(groups.iter().all(|g| g.members().len() == 1));

    let stale = std::mem::replace(&mut *answer.lock().expect("answer lock"), b_addr);
    eventually_within("the moved seed rejoins", SETTLE, || {
        groups.iter().all(|g| g.members().len() == 2)
    })
    .await;
    assert_eq!(
        *moves.lock().expect("moves lock"),
        vec![(stale, None), (b_addr, Some(stale))],
        "first resolution, then exactly one move"
    );
}

/// Resolves forever, recording both first poll and cancellation of the future.
#[derive(Clone)]
struct PendingResolver {
    started: Arc<AtomicUsize>,
    cancelled: Arc<AtomicUsize>,
}

struct PendingResolution(Arc<AtomicUsize>);

impl Drop for PendingResolution {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl SeedResolver for PendingResolver {
    fn resolve<'a>(&'a self, _name: &'a str) -> ResolveFuture<'a> {
        Box::pin(async move {
            let _resolution = PendingResolution(Arc::clone(&self.cancelled));
            self.started.fetch_add(1, Ordering::SeqCst);
            std::future::pending().await
        })
    }
}

#[tokio::test]
async fn closing_a_managed_node_cancels_a_pending_seed_lookup() {
    pending_lookup_stops(true).await;
}

#[tokio::test]
async fn dropping_the_final_node_owner_cancels_a_pending_seed_lookup() {
    pending_lookup_stops(false).await;
}

async fn pending_lookup_stops(explicit_close: bool) {
    let started = Arc::new(AtomicUsize::new(0));
    let cancelled = Arc::new(AtomicUsize::new(0));
    let resolver = PendingResolver {
        started: Arc::clone(&started),
        cancelled: Arc::clone(&cancelled),
    };
    let net = Network::new();
    let local = NodeId::new("pending-local");
    let peer = NodeId::new("pending-peer");
    let node = Node::builder(local.clone())
        .link(MemLink::new(net.endpoint(local), vec![peer.clone()]))
        .named_seeds(NamedSeeds::new(resolver).seed(peer, SEED_NAME))
        .start()
        .await
        .expect("memory link binds");
    // A borrowed router handle must not retain network ownership.
    let router = node.router().clone();
    let remaining = node.clone();
    drop(node);
    eventually_within("the seed lookup to begin", SETTLE, || {
        started.load(Ordering::SeqCst) == 1
    })
    .await;
    assert_eq!(
        cancelled.load(Ordering::SeqCst),
        0,
        "a node clone owns the lookup"
    );
    if explicit_close {
        remaining.close().await;
    }
    drop(remaining);
    eventually_within("the pending seed lookup to be cancelled", SETTLE, || {
        cancelled.load(Ordering::SeqCst) == 1
    })
    .await;
    tokio::time::timeout(SETTLE, router.cancelled())
        .await
        .expect("router cancellation accompanies resolver shutdown");
}

/// The operating system resolver resolves a literal `host:port` and a
/// `localhost` name alike.
#[cfg(feature = "dns")]
#[tokio::test]
async fn the_system_resolver_resolves_localhost() {
    use groupnet_runtime::SystemResolver;

    let addr = SystemResolver
        .resolve("127.0.0.1:7000")
        .await
        .expect("literal address");
    assert_eq!(addr, "127.0.0.1:7000".parse::<SocketAddr>().expect("addr"));
    let named = SystemResolver
        .resolve("localhost:7000")
        .await
        .expect("localhost");
    assert!(named.ip().is_loopback());
    assert_eq!(named.port(), 7000);
    assert!(
        SystemResolver.resolve("no-port").await.is_err(),
        "a name without a port is an error"
    );
}
