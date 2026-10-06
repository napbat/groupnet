//! Composition of native path selection, router MTUs, and pinned TLS streams.

use futures_util::io::{AsyncReadExt, AsyncWriteExt};
use groupnet_core::NodeId;
use groupnet_network::tunnel::{PeerIdentity, TunnelTransport};
use groupnet_network::{Router, RouterConfig};
use groupnet_testkit::cluster::eventually_within;
use groupnet_transport::bulk::BulkTransport;
use groupnet_transport_punch::{
    NetworkKey, PathPolicy, PeerPath, PunchConfig, Rendezvous, TcpPunchConfig, TcpRendezvous,
};
use groupnet_transport_tcp::TcpMsgTransport;
use groupnet_transport_udp::UdpTransport;
use tokio::time::timeout;

use super::{DEADLINE, binary, fixtures::credentials};

#[tokio::test]
async fn native_direct_and_relay_paths_carry_pinned_tls_and_half_close() {
    for policy in [PathPolicy::DirectPreferred, PathPolicy::RelayOnly] {
        let left = NodeId::new("left");
        let right = NodeId::new("right");
        let key = NetworkKey::from_bytes([19; 32]);
        let relay = Rendezvous::bind(
            "127.0.0.1:0".parse().unwrap(),
            key.clone(),
            vec![left.clone(), right.clone()],
        )
        .await
        .unwrap();
        let configure = |local, peer| {
            let mut config =
                PunchConfig::new(local, relay.local_addr().unwrap(), key.clone(), vec![peer]);
            config.bind = "127.0.0.1:0".parse().unwrap();
            config.policy = policy;
            config
        };
        let left_udp = UdpTransport::bind_connectivity(configure(left.clone(), right.clone()))
            .await
            .unwrap();
        let right_udp = UdpTransport::bind_connectivity(configure(right.clone(), left.clone()))
            .await
            .unwrap();
        let path = match policy {
            PathPolicy::DirectPreferred => PeerPath::Direct,
            PathPolicy::RelayOnly => PeerPath::Relay,
        };
        eventually_within("native bidirectional path selected", DEADLINE, || {
            left_udp.path_to(&right) == Some(path) && right_udp.path_to(&left) == Some(path)
        })
        .await;
        let left_router = Router::new(left.clone(), RouterConfig::default()).unwrap();
        let right_router = Router::new(right.clone(), RouterConfig::default()).unwrap();
        left_router
            .add_link(left_udp.clone().into_bound_link(1))
            .await
            .unwrap();
        right_router
            .add_link(right_udp.clone().into_bound_link(1))
            .await
            .unwrap();
        eventually_within("native routes established", DEADLINE, || {
            left_router.route_to(&right).is_some() && right_router.route_to(&left).is_some()
        })
        .await;
        let (left_tls, right_tls, _) = credentials();
        let client_pin = PeerIdentity::new(left.clone(), &left_tls.leaf).unwrap();
        let server_pin = PeerIdentity::new(right.clone(), &right_tls.leaf).unwrap();
        let client_endpoint =
            TunnelTransport::new(left_router.clone(), left_tls.identity, vec![server_pin]).unwrap();
        let server_endpoint =
            TunnelTransport::new(right_router.clone(), right_tls.identity, vec![client_pin])
                .unwrap();
        exchange(&client_endpoint, &server_endpoint, &left, &right).await;
        client_endpoint.close().await;
        server_endpoint.close().await;
        left_router.close().await;
        right_router.close().await;
        relay.close().await;
    }
}

#[tokio::test]
async fn native_tcp_relay_large_echo_half_closes_without_revoking_admission() {
    let left = NodeId::new("tcp-left");
    let right = NodeId::new("tcp-right");
    let key = NetworkKey::from_bytes([29; 32]);
    let relay = TcpRendezvous::bind(
        "127.0.0.1:0".parse().unwrap(),
        key.clone(),
        vec![left.clone(), right.clone()],
    )
    .await
    .unwrap();
    let configure = |local, peer| {
        let mut config =
            TcpPunchConfig::new(local, relay.local_addr().unwrap(), key.clone(), vec![peer]);
        config.bind = "127.0.0.1:0".parse().unwrap();
        config.policy = PathPolicy::RelayOnly;
        config
    };
    let left_tcp = TcpMsgTransport::bind_connectivity(configure(left.clone(), right.clone()))
        .await
        .unwrap();
    let right_tcp = TcpMsgTransport::bind_connectivity(configure(right.clone(), left.clone()))
        .await
        .unwrap();
    eventually_within("native TCP relay paths selected", DEADLINE, || {
        left_tcp.path_to(&right) == Some(PeerPath::Relay)
            && right_tcp.path_to(&left) == Some(PeerPath::Relay)
    })
    .await;
    let left_router = Router::new(left.clone(), RouterConfig::default()).unwrap();
    let right_router = Router::new(right.clone(), RouterConfig::default()).unwrap();
    left_router
        .add_link(left_tcp.clone().into_bound_link(1))
        .await
        .unwrap();
    right_router
        .add_link(right_tcp.clone().into_bound_link(1))
        .await
        .unwrap();
    eventually_within("native TCP relay routes established", DEADLINE, || {
        left_router.route_to(&right).is_some() && right_router.route_to(&left).is_some()
    })
    .await;
    let (left_tls, right_tls, _) = credentials();
    let client_pin = PeerIdentity::new(left.clone(), &left_tls.leaf).unwrap();
    let server_pin = PeerIdentity::new(right.clone(), &right_tls.leaf).unwrap();
    let client_endpoint =
        TunnelTransport::new(left_router.clone(), left_tls.identity, vec![server_pin]).unwrap();
    let server_endpoint =
        TunnelTransport::new(right_router.clone(), right_tls.identity, vec![client_pin]).unwrap();

    // 128 KiB requires more than the relay's 256-frame burst once TLS framing
    // and ACKs are included. Exhausting that budget must not revoke admission.
    timeout(std::time::Duration::from_secs(20), async {
        let (client, server) =
            tokio::join!(client_endpoint.connect(&right), server_endpoint.accept());
        let mut client = client.unwrap();
        let (authenticated, mut server) = server.unwrap();
        assert_eq!(authenticated, left);
        let payload = binary(128 * 1024);
        let send = async {
            client.write_all(&payload).await.unwrap();
            client.close().await.unwrap();
            let mut echoed = Vec::new();
            client.read_to_end(&mut echoed).await.unwrap();
            assert_eq!(echoed, payload);
        };
        let receive = async {
            let mut uploaded = Vec::new();
            server.read_to_end(&mut uploaded).await.unwrap();
            assert_eq!(uploaded, payload);
            server.write_all(&uploaded).await.unwrap();
            server.close().await.unwrap();
        };
        tokio::join!(send, receive);
        assert_eq!(left_tcp.path_to(&right), Some(PeerPath::Relay));
        assert_eq!(right_tcp.path_to(&left), Some(PeerPath::Relay));
    })
    .await
    .unwrap();
    // A subsequent session proves the shared admitted TCP control connection
    // remains usable after both directions cross the frame-rate budget.
    exchange(&client_endpoint, &server_endpoint, &left, &right).await;
    client_endpoint.close().await;
    server_endpoint.close().await;
    left_router.close().await;
    right_router.close().await;
    relay.close().await;
}

async fn exchange(
    client_endpoint: &TunnelTransport,
    server_endpoint: &TunnelTransport,
    expected_peer: &NodeId,
    destination: &NodeId,
) {
    timeout(DEADLINE, async {
        let (client, server) = tokio::join!(
            client_endpoint.connect(destination),
            server_endpoint.accept()
        );
        let mut client = client.unwrap();
        let (authenticated, mut server) = server.unwrap();
        assert_eq!(&authenticated, expected_peer);
        let payload = binary(65_793);
        let send = async {
            client.write_all(&payload).await.unwrap();
            client.close().await.unwrap();
            let mut response = Vec::new();
            client.read_to_end(&mut response).await.unwrap();
            assert_eq!(response, b"native tunnel verified");
        };
        let receive = async {
            let mut uploaded = Vec::new();
            server.read_to_end(&mut uploaded).await.unwrap();
            assert_eq!(uploaded, payload);
            server.write_all(b"native tunnel verified").await.unwrap();
            server.close().await.unwrap();
        };
        tokio::join!(send, receive);
    })
    .await
    .unwrap();
}
