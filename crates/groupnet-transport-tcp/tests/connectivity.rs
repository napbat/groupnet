#![cfg(feature = "connectivity")]
//! Real TCP-only traversal, relay, admission, and generation regressions.
use groupnet_core::NodeId;
use groupnet_testkit::cluster::eventually_within;
use groupnet_transport::{
    Transport,
    admission::{AcceptedPeer, Admission, JoinRequest},
    link::{LinkFuture, LinkProvider},
};
use groupnet_transport_punch::{
    MAX_TCP_MESSAGE, NetworkKey, PathPolicy, PeerPath, TcpPunchConfig, TcpRendezvous,
};
use groupnet_transport_tcp::{TcpLink, TcpMsgTransport};
use std::{io, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{io::AsyncWriteExt, net::TcpStream, time::timeout};

const SETTLE: Duration = Duration::from_secs(10);

fn loopback() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

fn key() -> NetworkKey {
    NetworkKey::from_bytes([67; 32])
}

fn config(local: &str, rendezvous: SocketAddr, policy: PathPolicy) -> TcpPunchConfig {
    let mut config =
        TcpPunchConfig::dynamic(NodeId::from(local), rendezvous, Some(key()), Vec::new());
    config.bind = SocketAddr::new(rendezvous.ip(), 0);
    config.candidate_binds = vec![config.bind];
    config.policy = policy;
    config
}

async fn pair(policy: PathPolicy) -> (TcpRendezvous, TcpMsgTransport, TcpMsgTransport) {
    let relay = TcpRendezvous::bind_with_admission(
        loopback(),
        Some(key()),
        Arc::new(groupnet_transport::admission::OpenAdmission),
    )
    .await
    .unwrap();
    let address = relay.local_addr().unwrap();
    let a = TcpMsgTransport::bind_connectivity(config("a", address, policy))
        .await
        .unwrap();
    let b = TcpMsgTransport::bind_connectivity(config("b", address, policy))
        .await
        .unwrap();
    let expected = if policy == PathPolicy::RelayOnly {
        PeerPath::Relay
    } else {
        PeerPath::Direct
    };
    eventually_within("TCP pair paths", SETTLE, || {
        a.path_to(&"b".into()) == Some(expected) && b.path_to(&"a".into()) == Some(expected)
    })
    .await;
    (relay, a, b)
}

async fn exchange(a: &TcpMsgTransport, b: &TcpMsgTransport, data: &[u8]) {
    a.send(&"b".into(), data).await.unwrap();
    let packet = timeout(SETTLE, b.recv()).await.unwrap().unwrap();
    assert_eq!(packet.from, NodeId::from("a"));
    assert_eq!(packet.msg, data);
    b.send(&"a".into(), b"reverse").await.unwrap();
    assert_eq!(
        timeout(SETTLE, a.recv()).await.unwrap().unwrap().msg,
        b"reverse"
    );
}

#[tokio::test]
async fn tcp_only_relay_and_relay_only_candidate_privacy() {
    let (relay, a, b) = pair(PathPolicy::RelayOnly).await;
    assert_eq!(a.local_candidates().unwrap(), []);
    assert_eq!(b.local_candidates().unwrap(), []);
    assert_eq!(a.direct_addr_to(&"b".into()), None);
    exchange(&a, &b, b"TCP without UDP").await;
    a.close().await;
    b.close().await;
    relay.close().await;
}

#[tokio::test]
async fn direct_candidates_reuse_registration_port_and_preserve_message_boundaries() {
    let (relay, a, b) = pair(PathPolicy::DirectPreferred).await;
    assert_eq!(a.observed_addr().unwrap().port(), a.local_addr().port());
    assert!(a.local_addrs().unwrap().len() >= 2);
    assert!(a.local_candidates().unwrap().len() >= 2);
    let a_ports: Vec<_> = a
        .local_addrs()
        .unwrap()
        .into_iter()
        .map(|address| address.port())
        .collect();
    let b_ports: Vec<_> = b
        .local_addrs()
        .unwrap()
        .into_iter()
        .map(|address| address.port())
        .collect();
    assert!(a_ports.contains(&b.direct_addr_to(&"a".into()).unwrap().port()));
    assert!(b_ports.contains(&a.direct_addr_to(&"b".into()).unwrap().port()));
    for data in [
        Vec::new(),
        vec![3; MAX_TCP_MESSAGE],
        b"separate frame".to_vec(),
    ] {
        exchange(&a, &b, &data).await;
    }
    assert_eq!(
        a.send(&"b".into(), &vec![0; MAX_TCP_MESSAGE + 1])
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    relay.close().await;
    // Established authenticated direct sessions are independent of control loss.
    exchange(&a, &b, b"direct after rendezvous shutdown").await;
    a.close().await;
    b.close().await;
}

#[tokio::test]
async fn one_relay_only_peer_forces_relay_without_exposing_candidates() {
    let relay = TcpRendezvous::bind_open(loopback()).await.unwrap();
    let address = relay.local_addr().unwrap();
    let mut ac = TcpPunchConfig::open("a".into(), address);
    ac.bind = loopback();
    ac.policy = PathPolicy::DirectPreferred;
    let mut bc = TcpPunchConfig::open("b".into(), address);
    bc.bind = loopback();
    let a = TcpMsgTransport::bind_connectivity(ac).await.unwrap();
    let b = TcpMsgTransport::bind_connectivity(bc).await.unwrap();
    eventually_within("keyless TCP relay fallback", SETTLE, || {
        a.path_to(&"b".into()) == Some(PeerPath::Relay)
            && b.path_to(&"a".into()) == Some(PeerPath::Relay)
    })
    .await;
    exchange(&a, &b, b"keyless, session fenced").await;
    assert_eq!(b.local_candidates().unwrap(), []);
    a.close().await;
    b.close().await;
    relay.close().await;
}

#[tokio::test]
async fn duplicate_identity_cannot_evict_incumbent_and_shutdown_withdraws_sessions() {
    let (relay, a, b) = pair(PathPolicy::RelayOnly).await;
    let duplicate = TcpMsgTransport::bind_connectivity(config(
        "a",
        relay.local_addr().unwrap(),
        PathPolicy::RelayOnly,
    ))
    .await;
    assert_eq!(
        duplicate.unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
    exchange(&a, &b, b"incumbent retained").await;
    a.close().await;
    eventually_within("closed TCP session withdrawn", SETTLE, || {
        b.path_to(&"a".into()).is_none()
    })
    .await;
    assert_eq!(
        a.local_addrs().unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    assert_eq!(
        a.recv().await.unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    assert_eq!(
        a.send(&"b".into(), b"closed").await.unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    b.close().await;
    relay.close().await;
}

#[tokio::test]
async fn replacement_sessions_fence_queued_frames_and_generation_tagged_sends() {
    let (relay, a, b) = pair(PathPolicy::RelayOnly).await;
    let bound = b.clone().into_bound_link(1);
    let registry = bound.sessions.as_ref().unwrap();
    let old = registry
        .subscribe()
        .borrow()
        .iter()
        .find(|peer| peer.node == NodeId::from("a"))
        .unwrap()
        .id;
    a.send(&"b".into(), b"stale queued frame").await.unwrap();
    a.close().await;
    eventually_within("old TCP generation revoked", SETTLE, || {
        b.path_to(&"a".into()).is_none()
    })
    .await;
    let a = TcpMsgTransport::bind_connectivity(config(
        "a",
        relay.local_addr().unwrap(),
        PathPolicy::RelayOnly,
    ))
    .await
    .unwrap();
    eventually_within("replacement TCP session", SETTLE, || {
        b.path_to(&"a".into()) == Some(PeerPath::Relay)
    })
    .await;
    b.send_admitted(&"a".into(), b"stale generation send", Some(old))
        .await
        .unwrap();
    assert!(timeout(Duration::from_millis(100), a.recv()).await.is_err());
    a.send(&"b".into(), b"replacement frame").await.unwrap();
    assert_eq!(
        timeout(SETTLE, b.recv()).await.unwrap().unwrap().msg,
        b"replacement frame"
    );
    a.close().await;
    b.close().await;
    relay.close().await;
}

#[tokio::test]
async fn unauthenticated_direct_data_and_idle_accepts_do_not_authorize_messages() {
    let (relay, a, b) = pair(PathPolicy::DirectPreferred).await;
    let mut hostile = TcpStream::connect(b.local_addrs().unwrap()[1])
        .await
        .unwrap();
    // Current unkeyed wire header and valid Data body, but no direct handshake.
    let bytes = b"\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0b\x00\x07unknown";
    hostile
        .write_u32(u32::try_from(bytes.len()).unwrap())
        .await
        .unwrap();
    hostile.write_all(bytes).await.unwrap();
    assert!(timeout(Duration::from_millis(100), b.recv()).await.is_err());
    let mut idle = Vec::new();
    for _ in 0..20 {
        idle.push(
            TcpStream::connect(b.local_addrs().unwrap()[1])
                .await
                .unwrap(),
        );
    }
    exchange(&a, &b, b"independent authenticated stream").await;
    timeout(SETTLE, b.close()).await.unwrap();
    drop(idle);
    a.close().await;
    relay.close().await;
}

#[derive(Debug)]
struct CredentialAdmission;

impl Admission for CredentialAdmission {
    fn admit<'a>(&'a self, request: JoinRequest<'a>) -> LinkFuture<'a, io::Result<AcceptedPeer>> {
        Box::pin(async move {
            if request.credential != b"approved" {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "credential denied",
                ));
            }
            Ok(AcceptedPeer::new(request.claimed.clone()))
        })
    }
}

#[tokio::test]
async fn application_admission_and_keyed_mode_fail_closed() {
    let relay =
        TcpRendezvous::bind_with_admission(loopback(), Some(key()), Arc::new(CredentialAdmission))
            .await
            .unwrap();
    let address = relay.local_addr().unwrap();
    assert!(
        TcpMsgTransport::bind_connectivity(config("denied", address, PathPolicy::RelayOnly))
            .await
            .is_err()
    );
    assert!(
        TcpMsgTransport::bind_connectivity(TcpPunchConfig::open("keyless".into(), address))
            .await
            .is_err()
    );
    let mut ac = config("a", address, PathPolicy::RelayOnly);
    ac.credential = b"approved".to_vec();
    let a = TcpMsgTransport::bind_connectivity(ac).await.unwrap();
    assert_eq!(a.known_peers(), []);
    a.close().await;
    relay.close().await;
}

#[tokio::test]
async fn tcp_connectivity_link_rejects_identity_mismatch_before_binding() {
    let relay = TcpRendezvous::bind_open(loopback()).await.unwrap();
    let address = relay.local_addr().unwrap();
    assert!(
        Box::new(TcpLink::connectivity(TcpPunchConfig::open(
            "a".into(),
            address
        )))
        .bind("other".into())
        .await
        .is_err()
    );
    relay.close().await;
}

#[tokio::test]
async fn ipv6_tcp_candidates_and_relay_when_supported() {
    let loopback: SocketAddr = "[::1]:0".parse().unwrap();
    let Ok(relay) = TcpRendezvous::bind_with_admission(
        loopback,
        Some(key()),
        Arc::new(groupnet_transport::admission::OpenAdmission),
    )
    .await
    else {
        return;
    };
    let address = relay.local_addr().unwrap();
    let a = TcpMsgTransport::bind_connectivity(config("a", address, PathPolicy::DirectPreferred))
        .await
        .unwrap();
    let b = TcpMsgTransport::bind_connectivity(config("b", address, PathPolicy::DirectPreferred))
        .await
        .unwrap();
    eventually_within("IPv6 TCP direct candidates", SETTLE, || {
        a.path_to(&"b".into()) == Some(PeerPath::Direct)
            && b.path_to(&"a".into()) == Some(PeerPath::Direct)
    })
    .await;
    assert!(
        a.local_candidates()
            .unwrap()
            .iter()
            .all(SocketAddr::is_ipv6)
    );
    exchange(&a, &b, b"IPv6 TCP").await;
    a.close().await;
    b.close().await;
    relay.close().await;
}

#[tokio::test]
async fn invalid_tcp_candidates_fail_before_network_work() {
    for candidate in [
        "0.0.0.0:1234",
        "239.1.1.1:1234",
        "127.0.0.1:0",
        "[fe80::1]:1234",
    ] {
        let mut config = config(
            "a",
            "127.0.0.1:1".parse().unwrap(),
            PathPolicy::DirectPreferred,
        );
        config
            .advertised_candidates
            .push(candidate.parse().unwrap());
        assert_eq!(
            TcpMsgTransport::bind_connectivity(config)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }
}

#[tokio::test]
async fn control_loss_is_terminal_without_a_remaining_direct_session() {
    let (relay, a, b) = pair(PathPolicy::RelayOnly).await;
    relay.close().await;
    assert_eq!(
        timeout(SETTLE, a.recv()).await.unwrap().unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    assert_eq!(
        timeout(SETTLE, b.recv()).await.unwrap().unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    a.close().await;
    b.close().await;
    let (relay, a, b) = pair(PathPolicy::DirectPreferred).await;
    relay.close().await;
    exchange(&a, &b, b"direct while control is absent").await;
    a.close().await;
    assert_eq!(
        timeout(SETTLE, b.recv()).await.unwrap().unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    b.close().await;
}

#[tokio::test]
async fn candidate_scheduling_is_fair_across_more_than_four_peers() {
    let relay = TcpRendezvous::bind_with_admission(
        loopback(),
        Some(key()),
        Arc::new(groupnet_transport::admission::OpenAdmission),
    )
    .await
    .unwrap();
    let address = relay.local_addr().unwrap();
    let hub =
        TcpMsgTransport::bind_connectivity(config("hub", address, PathPolicy::DirectPreferred))
            .await
            .unwrap();
    let mut peers = Vec::new();
    for index in 0..7 {
        let node = NodeId::from(format!("peer-{index}"));
        let mut config =
            TcpPunchConfig::new(node.clone(), address, key(), vec![NodeId::from("hub")]);
        config.bind = loopback();
        config.candidate_binds = vec![loopback()];
        config.advertised_candidates = vec!["192.0.2.1:59999".parse().unwrap()];
        peers.push((
            node,
            TcpMsgTransport::bind_connectivity(config).await.unwrap(),
        ));
    }
    eventually_within(
        "fair TCP candidate checks for seven peers",
        Duration::from_secs(20),
        || {
            peers.iter().all(|(node, peer)| {
                hub.path_to(node) == Some(PeerPath::Direct)
                    && peer.path_to(&NodeId::from("hub")) == Some(PeerPath::Direct)
            })
        },
    )
    .await;
    for (node, peer) in &peers {
        hub.send(node, node.as_str().as_bytes()).await.unwrap();
        let received = timeout(SETTLE, peer.recv()).await.unwrap().unwrap();
        assert_eq!(received.from, NodeId::from("hub"));
        assert_eq!(received.msg, node.as_str().as_bytes());
        peer.send(&NodeId::from("hub"), b"fair reverse")
            .await
            .unwrap();
        let received = timeout(SETTLE, hub.recv()).await.unwrap().unwrap();
        assert_eq!(received.from, *node);
        assert_eq!(received.msg, b"fair reverse");
    }
    for (_, peer) in peers {
        peer.close().await;
    }
    hub.close().await;
    relay.close().await;
}

#[tokio::test]
async fn unauthenticated_accept_flood_does_not_starve_established_relay_or_heartbeats() {
    let (relay, a, b) = pair(PathPolicy::RelayOnly).await;
    let address = relay.local_addr().unwrap();
    let bound = a.clone().into_bound_link(1);
    let registry = bound.sessions.as_ref().unwrap();
    let generation = registry
        .subscribe()
        .borrow()
        .iter()
        .find(|peer| peer.node == NodeId::from("b"))
        .unwrap()
        .id;
    let mut idle = Vec::new();
    for _ in 0..64 {
        idle.push(TcpStream::connect(address).await.unwrap());
    }
    let cancel = tokio_util::sync::CancellationToken::new();
    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut flood = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let cancel = cancel.clone();
        let attempts = attempts.clone();
        flood.spawn(async move {
            loop {
                tokio::select! {
                    () = cancel.cancelled() => break,
                    result = TcpStream::connect(address) => {
                        if let Ok(stream) = result {
                            attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            drop(stream);
                        }
                    }
                }
                tokio::task::yield_now().await;
            }
        });
    }
    eventually_within("unauthenticated TCP flood is active", SETTLE, || {
        attempts.load(std::sync::atomic::Ordering::Relaxed) >= 128
    })
    .await;
    // Longer than the six-second control idle deadline: a starved heartbeat
    // handler would withdraw the live peers even if one early frame slipped by.
    for _ in 0..7 {
        exchange(&a, &b, b"relay survives accept pressure").await;
        let start = tokio::time::Instant::now();
        eventually_within("live heartbeat under accept pressure", SETTLE, || {
            start.elapsed() >= Duration::from_secs(1)
        })
        .await;
        assert_eq!(a.path_to(&NodeId::from("b")), Some(PeerPath::Relay));
        assert!(registry.is_active(&NodeId::from("b"), generation));
    }
    cancel.cancel();
    while flood.join_next().await.is_some() {}
    drop(idle);
    a.close().await;
    b.close().await;
    relay.close().await;
}
