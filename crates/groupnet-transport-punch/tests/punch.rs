//! Loopback-only native discovery, direct/relay delivery and lifecycle scenarios.

use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use groupnet_core::NodeId;
use groupnet_testkit::cluster::eventually_within;
use groupnet_transport::Transport;
use groupnet_transport::link::{LinkLifecycle, LinkProvider};
use groupnet_transport_punch::{
    MAX_MESSAGE, NetworkKey, PathPolicy, PeerPath, PunchConfig, PunchLink, PunchTransport,
    Rendezvous,
};
use groupnet_transport_router::{Router, RouterConfig};
use tokio::net::UdpSocket;
use tokio::time::timeout;

const SETTLE: Duration = Duration::from_secs(8);

fn loopback() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 0))
}

fn key() -> NetworkKey {
    NetworkKey::from_bytes([37; 32])
}

async fn endpoint(
    local: &str,
    peers: &[&str],
    rendezvous: SocketAddr,
    policy: PathPolicy,
) -> PunchTransport {
    let mut config = PunchConfig::new(
        NodeId::from(local),
        rendezvous,
        key(),
        peers.iter().map(|name| NodeId::from(*name)).collect(),
    );
    config.bind = loopback();
    config.policy = policy;
    PunchTransport::bind(config).await.unwrap()
}

async fn pair(policy: PathPolicy) -> (Rendezvous, PunchTransport, PunchTransport) {
    let relay = Rendezvous::bind(loopback(), key(), vec!["a".into(), "b".into()])
        .await
        .unwrap();
    let address = relay.local_addr().unwrap();
    let a = endpoint("a", &["b"], address, policy).await;
    let b = endpoint("b", &["a"], address, policy).await;
    (relay, a, b)
}

#[tokio::test]
async fn simultaneous_punching_establishes_direct_paths_and_preserves_boundaries() {
    let (relay, a, b) = pair(PathPolicy::DirectPreferred).await;
    eventually_within("bidirectional direct UDP", SETTLE, || {
        a.path_to(&"b".into()) == Some(PeerPath::Direct)
            && b.path_to(&"a".into()) == Some(PeerPath::Direct)
    })
    .await;
    for message in [
        Vec::new(),
        vec![3; MAX_MESSAGE],
        b"separate message".to_vec(),
    ] {
        a.send(&"b".into(), &message).await.unwrap();
        let received = timeout(SETTLE, b.recv()).await.unwrap().unwrap();
        assert_eq!(received.from, NodeId::from("a"));
        assert_eq!(received.msg, message);
    }
    b.send(&"a".into(), b"reverse").await.unwrap();
    assert_eq!(
        timeout(SETTLE, a.recv()).await.unwrap().unwrap().msg,
        b"reverse"
    );
    let oversized = vec![0; MAX_MESSAGE + 1];
    assert_eq!(
        a.send(&"b".into(), &oversized).await.unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    relay.close().await;
    // Direct keepalives, rather than relay availability, maintain established paths.
    for _ in 0..7 {
        a.send(&"b".into(), b"without relay").await.unwrap();
        assert_eq!(
            timeout(SETTLE, b.recv()).await.unwrap().unwrap().msg,
            b"without relay"
        );
        let started = tokio::time::Instant::now();
        eventually_within("direct keepalive interval", SETTLE, || {
            started.elapsed() >= Duration::from_secs(1)
        })
        .await;
    }
    assert_eq!(a.path_to(&"b".into()), Some(PeerPath::Direct));
    a.close().await;
    b.close().await;
}

#[tokio::test]
async fn relay_only_paths_deliver_both_directions_and_keep_idle_registrations_live() {
    let (relay, a, b) = pair(PathPolicy::RelayOnly).await;
    eventually_within("bidirectional relay UDP", SETTLE, || {
        a.path_to(&"b".into()) == Some(PeerPath::Relay)
            && b.path_to(&"a".into()) == Some(PeerPath::Relay)
    })
    .await;
    // Wait through the full registration lifetime, with no application traffic.
    let started = tokio::time::Instant::now();
    eventually_within("idle registration keepalives", SETTLE, || {
        started.elapsed() >= Duration::from_secs(7)
    })
    .await;
    assert_eq!(a.path_to(&"b".into()), Some(PeerPath::Relay));
    for message in [Vec::new(), vec![7; MAX_MESSAGE], b"relay boundary".to_vec()] {
        a.send(&"b".into(), &message).await.unwrap();
        assert_eq!(
            timeout(SETTLE, b.recv()).await.unwrap().unwrap().msg,
            message
        );
    }
    b.send(&"a".into(), b"back").await.unwrap();
    assert_eq!(
        timeout(SETTLE, a.recv()).await.unwrap().unwrap().msg,
        b"back"
    );
    b.close().await;
    eventually_within(
        "dead relay registration expires",
        Duration::from_secs(14),
        || a.path_to(&"b".into()).is_none(),
    )
    .await;
    a.close().await;
    relay.close().await;
}

#[tokio::test]
async fn one_relay_only_peer_forces_relay_fallback() {
    let relay = Rendezvous::bind(loopback(), key(), vec!["a".into(), "b".into()])
        .await
        .unwrap();
    let address = relay.local_addr().unwrap();
    let a = endpoint("a", &["b"], address, PathPolicy::DirectPreferred).await;
    let b = endpoint("b", &["a"], address, PathPolicy::RelayOnly).await;
    eventually_within("policy-driven relay fallback", SETTLE, || {
        a.path_to(&"b".into()) == Some(PeerPath::Relay)
            && b.path_to(&"a".into()) == Some(PeerPath::Relay)
    })
    .await;
    a.send(&"b".into(), b"fallback").await.unwrap();
    assert_eq!(
        timeout(SETTLE, b.recv()).await.unwrap().unwrap().msg,
        b"fallback"
    );
    a.close().await;
    b.close().await;
    relay.close().await;
}

#[tokio::test]
async fn wrong_keys_unknown_identities_and_advertisements_cannot_join() {
    let relay = Rendezvous::bind(loopback(), key(), vec!["a".into(), "b".into()])
        .await
        .unwrap();
    let address = relay.local_addr().unwrap();
    let a = endpoint("a", &["b"], address, PathPolicy::RelayOnly).await;
    let mut config = PunchConfig::new(
        "b".into(),
        address,
        NetworkKey::from_bytes([9; 32]),
        vec!["a".into()],
    );
    config.bind = loopback();
    let wrong = PunchTransport::bind(config).await.unwrap();
    let unknown = endpoint("unknown", &["a"], address, PathPolicy::RelayOnly).await;
    a.learn_peer(
        &"unknown".into(),
        &unknown.local_addr().unwrap().to_string(),
    );
    assert_eq!(a.known_peers(), vec![NodeId::from("b")]);
    a.send(&"unknown".into(), b"no admission").await.unwrap();
    assert!(timeout(Duration::from_secs(2), a.recv()).await.is_err());
    assert!(a.path_to(&"b".into()).is_none());
    assert!(wrong.path_to(&"a".into()).is_none());
    assert!(unknown.path_to(&"a".into()).is_none());
    wrong.close().await;
    unknown.close().await;
    a.close().await;
    relay.close().await;
}

#[tokio::test]
async fn clones_shutdown_receivers_and_last_drop_releases_sockets() {
    let (relay, a, b) = pair(PathPolicy::RelayOnly).await;
    let address = a.local_addr().unwrap();
    let clone = a.clone();
    let receiver = tokio::spawn(async move { clone.recv().await });
    a.close().await;
    assert_eq!(
        timeout(SETTLE, receiver)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotConnected
    );
    assert_eq!(
        a.send(&"b".into(), b"closed").await.unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    assert!(a.local_addr().is_err());
    let rebound = UdpSocket::bind(address).await.unwrap();
    drop(rebound);
    let b_address = b.local_addr().unwrap();
    drop(b);
    let socket = timeout(SETTLE, async {
        loop {
            if let Ok(socket) = UdpSocket::bind(b_address).await {
                break socket;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(socket);
    let relay_address = relay.local_addr().unwrap();
    relay.close().await;
    let rebound = UdpSocket::bind(relay_address).await.unwrap();
    drop(rebound);
}

#[tokio::test]
async fn configuration_limits_and_secret_debug_are_explicit() {
    let secret = NetworkKey::generate().unwrap();
    let imported = NetworkKey::from_bytes(secret.to_bytes());
    assert_eq!(secret.to_bytes(), imported.to_bytes());
    assert_eq!(format!("{secret:?}"), "NetworkKey([REDACTED])");
    let address = SocketAddr::from(([127, 0, 0, 1], 12345));
    for peers in [
        vec!["b".into(), "b".into()],
        vec!["a".into()],
        vec![NodeId::new("x".repeat(65))],
        (0..129)
            .map(|index| NodeId::new(index.to_string()))
            .collect(),
    ] {
        let mut config = PunchConfig::new("a".into(), address, key(), peers);
        config.bind = loopback();
        assert_eq!(
            PunchTransport::bind(config).await.unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }
    assert!(
        Rendezvous::bind(loopback(), key(), vec![NodeId::from("")])
            .await
            .is_err()
    );
}

async fn unused_address() -> SocketAddr {
    let socket = UdpSocket::bind(loopback()).await.unwrap();
    socket.local_addr().unwrap()
}

#[tokio::test]
async fn provider_binds_identity_cost_and_native_mtu_and_router_closes_socket() {
    let relay = Rendezvous::bind(loopback(), key(), vec!["a".into(), "b".into()])
        .await
        .unwrap();
    let a = Router::new("a".into(), RouterConfig::default()).unwrap();
    let b = Router::new("b".into(), RouterConfig::default()).unwrap();
    let a_address = unused_address().await;
    let b_address = unused_address().await;
    for (router, peer, bind, cost) in [
        (&a, b.local_id(), a_address, 7),
        (&b, a.local_id(), b_address, 1),
    ] {
        let mut config = PunchConfig::new(
            router.local_id().clone(),
            relay.local_addr().unwrap(),
            key(),
            vec![peer.clone()],
        );
        config.bind = bind;
        config.policy = PathPolicy::RelayOnly;
        let provider = PunchLink::new(config).with_cost(cost);
        router
            .add_link(
                Box::new(provider)
                    .bind(router.local_id().clone())
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
    }
    eventually_within("native provider relay routes", SETTLE, || {
        a.route_to(b.local_id()).is_some() && b.route_to(a.local_id()).is_some()
    })
    .await;
    assert_eq!(a.route_to(b.local_id()).unwrap().cost, 7);
    let payload = vec![0x6d; MAX_MESSAGE * 3 + 1];
    a.send(b.local_id(), &payload).await.unwrap();
    let received = timeout(SETTLE, b.recv()).await.unwrap().unwrap();
    assert_eq!(received.from, *a.local_id());
    assert_eq!(received.msg, payload);
    a.close().await;
    b.close().await;
    let rebound = UdpSocket::bind(a_address).await.unwrap();
    drop(rebound);
    let rebound = UdpSocket::bind(b_address).await.unwrap();
    drop(rebound);
    relay.close().await;
}

#[tokio::test]
async fn provider_rejects_a_different_local_identity_before_binding() {
    let bind = unused_address().await;
    let mut config = PunchConfig::new(
        "configured".into(),
        SocketAddr::from(([127, 0, 0, 1], 12345)),
        key(),
        vec!["peer".into()],
    );
    config.bind = bind;
    assert_eq!(
        Box::new(PunchLink::new(config))
            .bind("different".into())
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    let rebound = UdpSocket::bind(bind).await.unwrap();
    drop(rebound);
}

#[tokio::test]
async fn failed_router_registration_drains_provider_endpoint() {
    let bind = unused_address().await;
    let router = Router::new("local".into(), RouterConfig::default()).unwrap();
    let mut config = PunchConfig::new(
        router.local_id().clone(),
        SocketAddr::from(([127, 0, 0, 1], 12345)),
        key(),
        vec!["peer".into()],
    );
    config.bind = bind;
    let bound = Box::new(PunchLink::new(config).with_cost(0))
        .bind(router.local_id().clone())
        .await
        .unwrap();
    assert!(router.add_link(bound).await.is_err());
    let rebound = UdpSocket::bind(bind).await.unwrap();
    drop(rebound);
    router.close().await;
}

#[tokio::test]
async fn bound_provider_drop_cancels_socket_owner() {
    let bind = unused_address().await;
    let mut config = PunchConfig::new(
        "local".into(),
        SocketAddr::from(([127, 0, 0, 1], 12345)),
        key(),
        Vec::new(),
    );
    config.bind = bind;
    let bound = Box::new(PunchLink::new(config))
        .bind("local".into())
        .await
        .unwrap();
    drop(bound);
    timeout(SETTLE, async {
        loop {
            if let Ok(socket) = UdpSocket::bind(bind).await {
                drop(socket);
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn lifecycle_shutdown_is_synchronous_and_close_drains_owned_socket() {
    let (relay, a, b) = pair(PathPolicy::RelayOnly).await;
    let bind = a.local_addr().unwrap();
    let clone = a.clone();
    LinkLifecycle::shutdown(&a);
    assert_eq!(
        clone.recv().await.unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    timeout(SETTLE, LinkLifecycle::close(&a)).await.unwrap();
    let rebound = UdpSocket::bind(bind).await.unwrap();
    drop(rebound);
    b.close().await;
    relay.close().await;
}

#[tokio::test]
async fn cancelled_close_preserves_the_task_for_a_later_drain() {
    let mut config = PunchConfig::new(
        "local".into(),
        SocketAddr::from(([127, 0, 0, 1], 12345)),
        key(),
        Vec::new(),
    );
    config.bind = loopback();
    let transport = PunchTransport::bind(config).await.unwrap();
    let bind = transport.local_addr().unwrap();
    // On this current-thread runtime the newly spawned endpoint has not been
    // polled yet. Poll close once to initiate cancellation without yielding to it.
    let mut closing = Box::pin(transport.close());
    let first = std::future::poll_fn(|cx| std::task::Poll::Ready(closing.as_mut().poll(cx))).await;
    assert!(first.is_pending());
    drop(closing);
    timeout(SETTLE, transport.close()).await.unwrap();
    let rebound = UdpSocket::bind(bind).await.unwrap();
    drop(rebound);
}
