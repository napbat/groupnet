//! Groupnet initialization owns routing and heterogeneous connection lifetimes.
#![cfg(feature = "router")]

use groupnet_core::NodeId;
use groupnet_runtime::Node;
use groupnet_testkit::cluster::eventually_within;
use groupnet_transport::Transport;
use groupnet_transport_mem::{MemLink, Network};
use groupnet_transport_router::{NetworkConfig, RouterConfig};
use std::io;
use std::time::Duration;

#[tokio::test]
async fn initialized_nodes_route_membership_and_entries_across_a_bridge() -> io::Result<()> {
    let left = Network::new();
    let right = Network::new();
    let a = NodeId::new("a");
    let b = NodeId::new("bridge");
    let c = NodeId::new("c");
    let configure = || {
        NetworkConfig::default().with_router(RouterConfig {
            announce_interval: Duration::from_millis(30),
            route_ttl: Duration::from_millis(600),
            ..RouterConfig::default()
        })
    };
    let a_config = configure().with_link(MemLink::new(left.endpoint(a.clone()), vec![b.clone()]));
    let b_config = configure()
        .with_link(MemLink::new(left.endpoint(b.clone()), vec![a.clone()]))
        .with_link(MemLink::new(right.endpoint(b.clone()), vec![c.clone()]));
    let c_config = configure().with_link(MemLink::new(right.endpoint(c.clone()), vec![b.clone()]));
    let node_a = Node::network_with(a.clone(), a_config, |builder| {
        builder.gossip_interval_ms(30)
    })
    .await?;
    let node_b = Node::network_with(b.clone(), b_config, |builder| {
        builder.gossip_interval_ms(30)
    })
    .await?;
    let node_c = Node::network_with(c.clone(), c_config, |builder| {
        builder.gossip_interval_ms(30)
    })
    .await?;
    let groups = [
        node_a.join_group("devices"),
        node_b.join_group("devices"),
        node_c.join_group("devices"),
    ];
    eventually_within("routed membership", Duration::from_secs(10), || {
        groups.iter().all(|group| {
            group.members().contains(&a)
                && group.members().contains(&b)
                && group.members().contains(&c)
        })
    })
    .await;
    groups[0]
        .set_entry("device", b"test-security-key", None)
        .expect("entry accepted");
    eventually_within(
        "device discovery across the bridge",
        Duration::from_secs(10),
        || groups[2].node_entry(&a, "device").as_deref() == Some(b"test-security-key"),
    )
    .await;
    assert_eq!(
        node_a.router().route_to(&c).expect("route").path,
        vec![a, b, c]
    );
    node_a.close().await;
    node_b.close().await;
    node_c.close().await;
    Ok(())
}

#[tokio::test]
async fn final_network_owner_drop_closes_connections_even_with_a_borrowed_router_clone()
-> io::Result<()> {
    let node = Node::network(NodeId::new("owned"), NetworkConfig::default()).await?;
    let remaining = Node::clone(&node);
    let router = node.router().clone();
    drop(node);
    router.send(router.local_id(), b"still owned").await?;
    drop(remaining);
    assert_eq!(
        router
            .send(router.local_id(), b"closed")
            .await
            .expect_err("last owner closed network")
            .kind(),
        io::ErrorKind::NotConnected
    );
    router.close().await;
    Ok(())
}
