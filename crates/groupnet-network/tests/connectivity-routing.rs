//! Connectivity feeds protocol links; routing preserves heterogeneous transit and failover.

use std::{net::SocketAddr, time::Duration};

use groupnet_core::NodeId;
use groupnet_network::{Router, RouterConfig};
use groupnet_testkit::cluster::eventually_within;
use groupnet_transport::Transport;
use groupnet_transport::link::LinkProvider;
use groupnet_transport_mem::Network;
use groupnet_transport_punch::{PathPolicy, PeerPath, TcpPunchConfig, TcpRendezvous};
use groupnet_transport_tcp::{TcpLink, TcpMsgTransport};

const WAIT: Duration = Duration::from_secs(15);

fn router(id: &str) -> Router {
    Router::new(
        NodeId::new(id),
        RouterConfig {
            announce_interval: Duration::from_millis(30),
            route_ttl: Duration::from_secs(1),
            ..RouterConfig::default()
        },
    )
    .unwrap()
}

fn config(id: &str, address: SocketAddr, policy: PathPolicy) -> TcpPunchConfig {
    let mut config = TcpPunchConfig::open(NodeId::new(id), address);
    config.bind = "127.0.0.1:0".parse().unwrap();
    config.policy = policy;
    config.candidate_binds.push(config.bind);
    config
}

async fn exchange(a: &Router, c: &Router, payload: &[u8]) {
    for (sender, receiver) in [(a, c), (c, a)] {
        sender.send(receiver.local_id(), payload).await.unwrap();
        let packet = tokio::time::timeout(WAIT, receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(packet.from, *sender.local_id());
        assert_eq!(packet.msg.as_ref(), payload);
    }
}

async fn topology(policy: PathPolicy) -> (TcpRendezvous, Router, Router, Router) {
    let relay = TcpRendezvous::bind_open("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let address = relay.local_addr().unwrap();
    let (a, b, c) = (router("a"), router("b"), router("c"));
    let memory = Network::new();
    a.add_transport(
        memory.endpoint(a.local_id().clone()),
        groupnet_transport::link::LinkConfig::new(vec![b.local_id().clone()]),
    )
    .unwrap();
    b.add_transport(
        memory.endpoint(b.local_id().clone()),
        groupnet_transport::link::LinkConfig::new(vec![a.local_id().clone()]),
    )
    .unwrap();
    for node in [&b, &c] {
        let link = Box::new(TcpLink::connectivity(config(
            node.local_id().as_str(),
            address,
            policy,
        )))
        .bind(node.local_id().clone())
        .await
        .unwrap();
        node.add_link(link).await.unwrap();
    }
    eventually_within(
        "memory-only A discovers remote C through TCP edge B",
        WAIT,
        || {
            a.route_to(c.local_id()).is_some_and(|route| {
                route.path
                    == [
                        a.local_id().clone(),
                        b.local_id().clone(),
                        c.local_id().clone(),
                    ]
            }) && c.route_to(a.local_id()).is_some()
        },
    )
    .await;
    (relay, a, b, c)
}

#[tokio::test]
async fn memory_only_peer_crosses_tcp_direct_and_relay_edges_and_withdraws_departure() {
    for policy in [PathPolicy::DirectPreferred, PathPolicy::RelayOnly] {
        let (relay, a, b, c) = topology(policy).await;
        let mut payload: Vec<u8> = (0..=255).cycle().take(64_975).collect();
        assert_eq!(
            a.send(c.local_id(), &payload).await.unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
        payload.pop();
        exchange(&a, &c, &payload).await;
        c.close().await;
        eventually_within(
            "departed TCP peer withdraws from memory-only route",
            WAIT,
            || a.route_to(c.local_id()).is_none() && b.route_to(c.local_id()).is_none(),
        )
        .await;
        a.send(b.local_id(), b"memory adjacency survives TCP loss")
            .await
            .unwrap();
        let packet = tokio::time::timeout(WAIT, b.recv()).await.unwrap().unwrap();
        assert_eq!(packet.from, *a.local_id());
        assert_eq!(packet.msg.as_ref(), b"memory adjacency survives TCP loss");
        a.close().await;
        b.close().await;
        relay.close().await;
    }
}

#[tokio::test]
async fn compatible_connectivity_promotes_direct_route_and_loss_restores_bridge() {
    let (relay, a, b, c) = topology(PathPolicy::DirectPreferred).await;
    exchange(&a, &c, b"initial transit").await;
    let a_tcp = TcpMsgTransport::bind_connectivity(config(
        "a",
        relay.local_addr().unwrap(),
        PathPolicy::DirectPreferred,
    ))
    .await
    .unwrap();
    eventually_within(
        "admitted A-C candidate becomes a direct TCP path",
        WAIT,
        || a_tcp.path_to(c.local_id()) == Some(PeerPath::Direct),
    )
    .await;
    a.add_link(a_tcp.clone().into_bound_link(1)).await.unwrap();
    eventually_within("router prefers compatible one-hop route", WAIT, || {
        a.route_to(c.local_id())
            .is_some_and(|route| route.path == [a.local_id().clone(), c.local_id().clone()])
            && c.route_to(a.local_id())
                .is_some_and(|route| route.path == [c.local_id().clone(), a.local_id().clone()])
    })
    .await;
    exchange(&a, &c, b"direct without changing destination identity").await;
    a_tcp.close().await;
    eventually_within(
        "connectivity loss restores heterogeneous route",
        WAIT,
        || {
            a.route_to(c.local_id()).is_some_and(|route| {
                route.path
                    == [
                        a.local_id().clone(),
                        b.local_id().clone(),
                        c.local_id().clone(),
                    ]
            }) && c.route_to(a.local_id()).is_some_and(|route| {
                route.path
                    == [
                        c.local_id().clone(),
                        b.local_id().clone(),
                        a.local_id().clone(),
                    ]
            })
        },
    )
    .await;
    exchange(&a, &c, b"transit restored without application forwarding").await;
    for node in [&c, &b, &a] {
        node.close().await;
    }
    relay.close().await;
}
