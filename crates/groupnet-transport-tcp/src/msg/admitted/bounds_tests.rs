//! Minimum-capacity convergence and bounded managed address retention.

use groupnet_transport::Transport;
use groupnet_transport::admission::OpenAdmission;
use tokio::io::AsyncReadExt;

use super::tests::{
    available, connect, endpoint, eventually, managed, no_connections, peers, receive,
};
use super::*;

async fn bounded_endpoint(
    id: &str,
    max_peers: usize,
    handshake_timeout: Duration,
) -> TcpMsgTransport {
    TcpMsgTransport::bind_admitted(
        NodeId::new(id),
        "127.0.0.1:0",
        TcpMsgConfig::default(),
        TcpAdmission {
            config: TcpAdmissionConfig {
                max_pending: QueueCapacity::MIN,
                max_peers: std::num::NonZeroUsize::new(max_peers).expect("nonzero capacity"),
                handshake_timeout,
            },
            ..TcpAdmission::new(Arc::new(OpenAdmission))
        },
    )
    .await
    .expect("bounded endpoint")
}

#[tokio::test]
async fn minimum_capacity_simultaneous_dials_keep_the_lexical_winner_and_exchange_payloads() {
    let a = bounded_endpoint("a", 2, Duration::from_secs(5)).await;
    let b = bounded_endpoint("b", 2, Duration::from_secs(5)).await;
    a.register_peer(b.local_id().clone(), b.local_addr());
    b.register_peer(a.local_id().clone(), a.local_addr());
    // No await between dials: both outbound permits are occupied before either
    // accept task runs, reproducing mutual rejection with one shared permit.
    a.connect_peer(b.local_id()).expect("a dial");
    b.connect_peer(a.local_id()).expect("b dial");
    assert_eq!(available(&a), (1, 0));
    assert_eq!(available(&b), (1, 0));
    eventually(|| {
        peers(&a) == 1 && peers(&b) == 1 && available(&a) == (1, 1) && available(&b) == (1, 1)
    })
    .await;
    assert_eq!(a.outbound_connections(), 1);
    assert_eq!(b.outbound_connections(), 0);
    let incumbent = b.sessions().expect("sessions").subscribe().borrow()[0].id;
    let duplicate = bounded_endpoint("a", 2, Duration::from_secs(5)).await;
    connect(&duplicate, &b);
    eventually(|| no_connections(&duplicate)).await;
    assert_eq!(peers(&duplicate), 0);
    assert!(
        b.sessions()
            .expect("sessions")
            .is_active(a.local_id(), incumbent)
    );
    a.send(b.local_id(), b"a to b").await.expect("send");
    b.send(a.local_id(), b"b to a").await.expect("send");
    let to_a = receive(&a).await;
    let to_b = receive(&b).await;
    assert_eq!(to_a.from, *b.local_id());
    assert_eq!(to_a.msg.as_ref(), b"b to a");
    assert_eq!(to_b.from, *a.local_id());
    assert_eq!(to_b.msg.as_ref(), b"a to b");
    duplicate.close().await;
    a.close().await;
    b.close().await;
}

async fn occupy_both_directions(
    endpoint: &TcpMsgTransport,
    listener: &tokio::net::TcpListener,
) -> (TcpStream, TcpStream) {
    endpoint.register_peer(
        NodeId::new("silent"),
        listener.local_addr().expect("address"),
    );
    endpoint.connect_peer(&NodeId::new("silent")).expect("dial");
    let (mut outbound, _) = timeout(Duration::from_secs(5), listener.accept())
        .await
        .expect("accept deadline")
        .expect("accept");
    read_hello(&mut outbound).await.expect("outbound hello");
    let inbound = TcpStream::connect(endpoint.local_addr())
        .await
        .expect("inbound");
    eventually(|| available(endpoint) == (0, 0)).await;
    (inbound, outbound)
}

async fn assert_socket_closed(mut socket: TcpStream) {
    let mut byte = [0];
    assert!(matches!(
        timeout(Duration::from_secs(5), socket.read(&mut byte))
            .await
            .expect("socket release deadline"),
        Ok(0) | Err(_)
    ));
}

#[tokio::test]
async fn both_handshake_directions_release_capacity_at_deadline_and_on_shutdown() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener");
    let expiring = bounded_endpoint("expiring", 2, Duration::from_secs(1)).await;
    let (inbound, outbound) = occupy_both_directions(&expiring, &listener).await;
    assert_socket_closed(inbound).await;
    assert_socket_closed(outbound).await;
    eventually(|| available(&expiring) == (1, 1) && no_connections(&expiring)).await;
    assert_eq!(peers(&expiring), 0);
    expiring.close().await;

    let stopping = bounded_endpoint("stopping", 2, Duration::from_secs(60)).await;
    let (inbound, outbound) = occupy_both_directions(&stopping, &listener).await;
    timeout(Duration::from_secs(5), stopping.close())
        .await
        .expect("shutdown does not await handshake deadline");
    assert_socket_closed(inbound).await;
    assert_socket_closed(outbound).await;
    assert!(managed(&stopping).pending_inbound.is_closed());
    assert!(managed(&stopping).pending_outbound.is_closed());
    assert_eq!(available(&stopping), (1, 1));
    assert!(no_connections(&stopping));
    assert_eq!(peers(&stopping), 0);
}

#[tokio::test]
async fn churn_bounds_handshake_and_hint_learning_but_preserves_configured_bootstrap_redial() {
    let server = bounded_endpoint("server", 2, Duration::from_secs(5)).await;
    let advertised = "127.0.0.1:1".parse().expect("advertised address");
    let seed = TcpMsgTransport::bind_admitted(
        NodeId::new("seed"),
        "127.0.0.1:0",
        TcpMsgConfig {
            advertise: Some(advertised),
            ..TcpMsgConfig::default()
        },
        TcpAdmission::new(Arc::new(OpenAdmission)),
    )
    .await
    .expect("seed");
    server.register_peer(seed.local_id().clone(), seed.local_addr());
    connect(&seed, &server);
    eventually(|| peers(&server) == 1 && peers(&seed) == 1).await;
    server.learn_peer(seed.local_id(), &advertised.to_string());
    assert_eq!(server.peer_addr(seed.local_id()), Some(seed.local_addr()));
    server.sessions().expect("sessions").revoke(seed.local_id());
    eventually(|| no_connections(&server) && no_connections(&seed)).await;

    let mut learned_ids = Vec::new();
    for index in 0..5 {
        let id = format!("churn-{index}");
        let client = endpoint(&id, Arc::new(OpenAdmission), &[]).await;
        connect(&client, &server);
        eventually(|| {
            peers(&server) == 1
                && peers(&client) == 1
                && server.peer_addr(client.local_id()) == Some(client.local_addr())
        })
        .await;
        learned_ids.push(client.local_id().clone());
        let retained: Vec<_> = learned_ids
            .iter()
            .filter(|node| server.peer_addr(node).is_some())
            .collect();
        assert_eq!(retained.len(), learned_ids.len().min(2));
        assert_eq!(
            retained,
            learned_ids[learned_ids.len().saturating_sub(2)..]
                .iter()
                .collect::<Vec<_>>()
        );
        assert_eq!(server.peer_addr(seed.local_id()), Some(seed.local_addr()));
        client.close().await;
        eventually(|| no_connections(&server)).await;
    }

    // Updates and gossip hints share the handshake-learned capacity and age.
    let recent = endpoint("churn-3", Arc::new(OpenAdmission), &[]).await;
    for _ in 0..4 {
        server.learn_peer(recent.local_id(), &recent.local_addr().to_string());
    }
    let hint = endpoint("hint", Arc::new(OpenAdmission), &[]).await;
    server.learn_peer(hint.local_id(), &hint.local_addr().to_string());
    assert_eq!(server.peer_addr(&NodeId::new("churn-4")), None);
    assert_eq!(
        server.peer_addr(recent.local_id()),
        Some(recent.local_addr())
    );
    assert_eq!(server.peer_addr(hint.local_id()), Some(hint.local_addr()));
    assert_eq!(server.peer_addr(seed.local_id()), Some(seed.local_addr()));

    // No registration at the sender: the recent updated address must redial.
    server
        .send(recent.local_id(), b"learned reconnect")
        .await
        .expect("send");
    assert_eq!(receive(&recent).await.msg.as_ref(), b"learned reconnect");
    recent
        .send(server.local_id(), b"learned reply")
        .await
        .expect("reply");
    assert_eq!(receive(&server).await.msg.as_ref(), b"learned reply");
    recent.close().await;
    eventually(|| no_connections(&server)).await;

    server.connect_peer(hint.local_id()).expect("hint redial");
    eventually(|| peers(&server) == 1 && peers(&hint) == 1).await;
    server
        .send(hint.local_id(), b"hint connected")
        .await
        .expect("send");
    assert_eq!(receive(&hint).await.msg.as_ref(), b"hint connected");
    hint.close().await;
    eventually(|| no_connections(&server)).await;

    server
        .connect_peer(seed.local_id())
        .expect("configured bootstrap redial");
    eventually(|| peers(&server) == 1 && peers(&seed) == 1).await;
    assert_eq!(server.peer_addr(seed.local_id()), Some(seed.local_addr()));
    server
        .send(seed.local_id(), b"configured seed")
        .await
        .expect("send");
    assert_eq!(receive(&seed).await.msg.as_ref(), b"configured seed");
    seed.close().await;
    server.close().await;
}
