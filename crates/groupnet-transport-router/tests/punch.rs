//! Loopback-only native discovery, direct/relay delivery and lifecycle scenarios.

use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use groupnet_core::NodeId;
use groupnet_testkit::cluster::eventually_within;
use groupnet_transport::Transport;
use groupnet_transport_router::punch::{
    MAX_MESSAGE, NetworkKey, PathPolicy, PeerPath, PunchConfig, PunchTransport, Rendezvous,
};
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
