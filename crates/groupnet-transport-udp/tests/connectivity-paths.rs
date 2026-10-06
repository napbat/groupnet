#![cfg(feature = "connectivity")]
//! Explicit keyless path policy, real UDP delivery, and fresh-session reconnects.

use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use groupnet_core::NodeId;
use groupnet_testkit::cluster::eventually_within;
use groupnet_transport::Transport;
use groupnet_transport_punch::{MAX_MESSAGE, PathPolicy, PeerPath, PunchConfig, Rendezvous};
use groupnet_transport_udp::UdpTransport;
use tokio::time::timeout;

const SETTLE: Duration = Duration::from_secs(15);

fn loopback() -> SocketAddr {
    ([127, 0, 0, 1], 0).into()
}

async fn endpoint(local: &str, address: SocketAddr, policy: PathPolicy) -> UdpTransport {
    let mut config = PunchConfig::open(local.into(), address);
    assert_eq!(config.policy, PathPolicy::RelayOnly);
    config.bind = loopback();
    config.policy = policy;
    UdpTransport::bind_connectivity(config).await.unwrap()
}

async fn paths(a: &UdpTransport, b: &UdpTransport, expected: PeerPath) {
    eventually_within("keyless bidirectional path convergence", SETTLE, || {
        a.path_to(&"b".into()) == Some(expected) && b.path_to(&"a".into()) == Some(expected)
    })
    .await;
}

async fn deliver(from: &UdpTransport, to: &UdpTransport, sender: &str, receiver: &str, msg: &[u8]) {
    from.send(&receiver.into(), msg).await.unwrap();
    let packet = timeout(SETTLE, to.recv()).await.unwrap().unwrap();
    assert_eq!(packet.from, NodeId::from(sender));
    assert_eq!(packet.msg, msg);
}

#[tokio::test]
async fn keyless_direct_preferred_discovers_bidirectional_paths_and_preserves_message_bounds() {
    let relay = Rendezvous::bind_open(loopback()).await.unwrap();
    let address = relay.local_addr().unwrap();
    let a = endpoint("a", address, PathPolicy::DirectPreferred).await;
    let b = endpoint("b", address, PathPolicy::DirectPreferred).await;
    paths(&a, &b, PeerPath::Direct).await;
    assert_eq!(a.known_peers(), vec![NodeId::from("b")]);
    assert_eq!(b.known_peers(), vec![NodeId::from("a")]);
    // With the rendezvous closed, successful delivery cannot be relay delivery.
    relay.close().await;
    for msg in [
        Vec::new(),
        vec![0x5a; MAX_MESSAGE],
        b"application payload".to_vec(),
    ] {
        deliver(&a, &b, "a", "b", &msg).await;
        deliver(&b, &a, "b", "a", &msg).await;
    }
    let oversized = vec![0; MAX_MESSAGE + 1];
    for (transport, peer) in [(&a, "b"), (&b, "a")] {
        assert_eq!(
            transport
                .send(&peer.into(), &oversized)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }
    assert_eq!(a.path_to(&"b".into()), Some(PeerPath::Direct));
    assert_eq!(b.path_to(&"a".into()), Some(PeerPath::Direct));
    a.connection().unwrap().close().await;
    b.connection().unwrap().close().await;
}

#[tokio::test]
async fn keyless_relay_only_and_mixed_policies_deliver_bidirectionally_without_direct_paths() {
    for (a_policy, b_policy) in [
        (PathPolicy::RelayOnly, PathPolicy::RelayOnly),
        (PathPolicy::DirectPreferred, PathPolicy::RelayOnly),
        (PathPolicy::RelayOnly, PathPolicy::DirectPreferred),
    ] {
        let relay = Rendezvous::bind_open(loopback()).await.unwrap();
        let address = relay.local_addr().unwrap();
        let a = endpoint("a", address, a_policy).await;
        let b = endpoint("b", address, b_policy).await;
        paths(&a, &b, PeerPath::Relay).await;
        for msg in [
            Vec::new(),
            vec![0xa5; MAX_MESSAGE],
            b"relayed application payload".to_vec(),
        ] {
            deliver(&a, &b, "a", "b", &msg).await;
            deliver(&b, &a, "b", "a", &msg).await;
            assert_eq!(a.path_to(&"b".into()), Some(PeerPath::Relay));
            assert_eq!(b.path_to(&"a".into()), Some(PeerPath::Relay));
        }
        a.connection().unwrap().close().await;
        b.connection().unwrap().close().await;
        relay.close().await;
    }
}

#[tokio::test]
async fn keyless_direct_reconnects_with_fresh_session_at_the_same_socket_address() {
    let relay = Rendezvous::bind_open(loopback()).await.unwrap();
    let address = relay.local_addr().unwrap();
    let a = endpoint("a", address, PathPolicy::DirectPreferred).await;
    let b = endpoint("b", address, PathPolicy::DirectPreferred).await;
    paths(&a, &b, PeerPath::Direct).await;
    let bind = b.local_addr().unwrap();
    for msg in [
        b"old session one".as_slice(),
        b"old session two",
        b"old session three",
    ] {
        deliver(&b, &a, "b", "a", msg).await;
    }
    b.connection().unwrap().close().await;
    eventually_within("old direct session and registration expire", SETTLE, || {
        a.path_to(&"b".into()).is_none() && a.known_peers().is_empty()
    })
    .await;
    let mut config = PunchConfig::open("b".into(), address);
    config.bind = bind;
    config.policy = PathPolicy::DirectPreferred;
    let replacement = UdpTransport::bind_connectivity(config).await.unwrap();
    assert_eq!(replacement.local_addr().unwrap(), bind);
    paths(&a, &replacement, PeerPath::Direct).await;
    // A fresh sender starts its sequence again; the incumbent must not retain
    // replay state or receive capabilities belonging to the previous session.
    relay.close().await;
    deliver(&replacement, &a, "b", "a", b"fresh session sequence").await;
    deliver(&a, &replacement, "a", "b", b"fresh session reverse").await;
    assert_eq!(a.path_to(&"b".into()), Some(PeerPath::Direct));
    assert_eq!(replacement.path_to(&"a".into()), Some(PeerPath::Direct));
    replacement.connection().unwrap().close().await;
    a.connection().unwrap().close().await;
}
