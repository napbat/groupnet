#![cfg(feature = "connectivity")]
//! Native multi-socket candidates, privacy, address families, and bounded configuration.

use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use groupnet_testkit::cluster::eventually_within;
use groupnet_transport::Transport;
use groupnet_transport_punch::{PathPolicy, PeerPath, PunchConfig, Rendezvous};
use groupnet_transport_udp::UdpTransport;
use tokio::net::UdpSocket;
use tokio::time::timeout;

const SETTLE: Duration = Duration::from_secs(15);

fn loopback() -> SocketAddr {
    ([127, 0, 0, 1], 0).into()
}

async fn pair(
    address: SocketAddr,
    bind: SocketAddr,
    policy: PathPolicy,
) -> (UdpTransport, UdpTransport) {
    let unused = UdpSocket::bind(bind).await.unwrap();
    let unreachable = unused.local_addr().unwrap();
    let mut config = PunchConfig::open("a".into(), address);
    config.policy = policy;
    config.bind = bind;
    config.candidate_binds = vec![bind];
    config.advertised_candidates = vec![unreachable];
    let a = UdpTransport::bind_connectivity(config.clone())
        .await
        .unwrap();
    config.local = "b".into();
    let b = UdpTransport::bind_connectivity(config).await.unwrap();
    drop(unused);
    (a, b)
}

async fn paths(a: &UdpTransport, b: &UdpTransport, path: PeerPath) {
    eventually_within("independent UDP candidate paths converge", SETTLE, || {
        a.path_to(&"b".into()) == Some(path) && b.path_to(&"a".into()) == Some(path)
    })
    .await;
}

async fn exchange(a: &UdpTransport, b: &UdpTransport) {
    for (from, to, sender, receiver) in [(a, b, "a", "b"), (b, a, "b", "a")] {
        from.send(&receiver.into(), b"native candidates")
            .await
            .unwrap();
        let packet = timeout(SETTLE, to.recv()).await.unwrap().unwrap();
        assert_eq!(packet.from.as_str(), sender);
        assert_eq!(packet.msg.as_ref(), b"native candidates");
    }
}

#[tokio::test]
async fn multiple_leased_sockets_unreachable_hint_and_observed_path_deliver_bidirectionally() {
    let relay = Rendezvous::bind_open(loopback()).await.unwrap();
    let (a, b) = pair(
        relay.local_addr().unwrap(),
        loopback(),
        PathPolicy::DirectPreferred,
    )
    .await;
    paths(&a, &b, PeerPath::Direct).await;
    eventually_within("observed mappings available", SETTLE, || {
        a.observed_addr().is_some() && b.observed_addr().is_some()
    })
    .await;
    for endpoint in [&a, &b] {
        let binds = endpoint.local_addrs().unwrap();
        assert_eq!(binds.len(), 2);
        assert_ne!(binds[0], binds[1]);
        let candidates = endpoint.local_candidates().unwrap();
        assert!(binds.iter().all(|address| candidates.contains(address)));
        assert_eq!(candidates.len(), 3);
    }
    let selected = a.direct_addr_to(&"b".into()).unwrap();
    exchange(&a, &b).await;
    assert_eq!(a.direct_addr_to(&"b".into()), Some(selected));
    relay.close().await;
    exchange(&a, &b).await;
    a.connection().unwrap().close().await;
    b.connection().unwrap().close().await;
    assert_eq!(
        a.local_candidates().unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
}

#[tokio::test]
async fn relay_only_keeps_candidates_private_and_sends_no_candidate_checks() {
    let relay = Rendezvous::bind_open(loopback()).await.unwrap();
    let spy = UdpSocket::bind(loopback()).await.unwrap();
    let mut config = PunchConfig::open("a".into(), relay.local_addr().unwrap());
    config.bind = loopback();
    config.candidate_binds = vec![loopback()];
    config.advertised_candidates = vec![spy.local_addr().unwrap()];
    let a = UdpTransport::bind_connectivity(config.clone())
        .await
        .unwrap();
    config.local = "b".into();
    let b = UdpTransport::bind_connectivity(config).await.unwrap();
    paths(&a, &b, PeerPath::Relay).await;
    assert_eq!(a.local_candidates().unwrap(), []);
    assert_eq!(b.local_candidates().unwrap(), []);
    assert!(a.observed_addr().is_none());
    assert!(a.direct_addr_to(&"b".into()).is_none());
    exchange(&a, &b).await;
    let mut bytes = [0; 1201];
    assert!(
        timeout(Duration::from_millis(1100), spy.recv_from(&mut bytes))
            .await
            .is_err()
    );
    a.connection().unwrap().close().await;
    b.connection().unwrap().close().await;
    relay.close().await;
}

#[tokio::test]
async fn ipv6_candidates_and_multisocket_direct_delivery_where_available() {
    let bind: SocketAddr = "[::1]:0".parse().unwrap();
    let relay = match Rendezvous::bind_open(bind).await {
        Ok(relay) => relay,
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::AddrNotAvailable | io::ErrorKind::Unsupported
            ) =>
        {
            return;
        }
        Err(error) => panic!("IPv6 rendezvous bind failed: {error}"),
    };
    let (a, b) = pair(
        relay.local_addr().unwrap(),
        bind,
        PathPolicy::DirectPreferred,
    )
    .await;
    paths(&a, &b, PeerPath::Direct).await;
    assert!(a.direct_addr_to(&"b".into()).unwrap().is_ipv6());
    exchange(&a, &b).await;
    a.connection().unwrap().close().await;
    b.connection().unwrap().close().await;
    relay.close().await;
}

#[tokio::test]
async fn invalid_candidate_targets_and_socket_counts_fail_closed() {
    let relay = Rendezvous::bind_open(loopback()).await.unwrap();
    for address in [
        "0.0.0.0:9",
        "127.0.0.1:0",
        "224.0.0.1:9",
        "255.255.255.255:9",
        "169.254.1.1:9",
        "[ff02::1]:9",
        "[fe80::1%4]:9",
    ] {
        let mut config = PunchConfig::open("a".into(), relay.local_addr().unwrap());
        config.advertised_candidates = vec![address.parse().unwrap()];
        assert_eq!(
            UdpTransport::bind_connectivity(config)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }
    let mut config = PunchConfig::open("a".into(), relay.local_addr().unwrap());
    config.candidate_binds = vec![loopback(); 4];
    assert_eq!(
        UdpTransport::bind_connectivity(config)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    let mut config = PunchConfig::open("a".into(), relay.local_addr().unwrap());
    config.advertised_candidates = vec![([127, 0, 0, 1], 9).into(); 9];
    assert_eq!(
        UdpTransport::bind_connectivity(config)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    relay.close().await;
}

#[tokio::test]
async fn ipv4_relay_registration_owns_ipv6_candidate_sockets_and_gathers_wildcard_interfaces() {
    let ipv6: SocketAddr = "[::1]:0".parse().unwrap();
    let available = match UdpSocket::bind(ipv6).await {
        Ok(socket) => socket,
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::AddrNotAvailable | io::ErrorKind::Unsupported
            ) =>
        {
            return;
        }
        Err(error) => panic!("IPv6 candidate socket bind failed: {error}"),
    };
    drop(available);
    let relay = Rendezvous::bind_open(loopback()).await.unwrap();
    let mut config = PunchConfig::open("a".into(), relay.local_addr().unwrap());
    config.policy = PathPolicy::DirectPreferred;
    config.candidate_binds = vec![ipv6];
    let a = UdpTransport::bind_connectivity(config.clone())
        .await
        .unwrap();
    config.local = "b".into();
    let b = UdpTransport::bind_connectivity(config).await.unwrap();
    paths(&a, &b, PeerPath::Direct).await;
    for endpoint in [&a, &b] {
        let candidates = endpoint.local_candidates().unwrap();
        assert!(candidates.iter().any(SocketAddr::is_ipv4));
        assert!(candidates.iter().any(SocketAddr::is_ipv6));
        assert!(
            candidates
                .iter()
                .all(|address| !address.ip().is_unspecified())
        );
        assert!(candidates.len() <= 8);
        assert_eq!(endpoint.local_addrs().unwrap().len(), 2);
    }
    exchange(&a, &b).await;
    let binds = a.local_addrs().unwrap();
    a.connection().unwrap().close().await;
    // The additional candidate socket has no detached reader keeping it alive.
    let rebound = UdpSocket::bind(binds[1]).await.unwrap();
    drop(rebound);
    b.connection().unwrap().close().await;
    relay.close().await;
}
