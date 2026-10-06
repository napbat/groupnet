//! Groupnet initialization owns routing and heterogeneous connection lifetimes.

use groupnet_core::NodeId;
use groupnet_network::RouterConfig;
use groupnet_runtime::Node;
use groupnet_testkit::cluster::eventually_within;
use groupnet_transport::Transport;
use groupnet_transport_mem::{MemLink, Network};
use std::io;
use std::time::Duration;

#[tokio::test]
async fn initialized_nodes_route_membership_and_entries_across_a_bridge() -> io::Result<()> {
    let left = Network::new();
    let right = Network::new();
    let a = NodeId::new("a");
    let b = NodeId::new("bridge");
    let c = NodeId::new("c");
    let configure = || RouterConfig {
        announce_interval: Duration::from_millis(30),
        route_ttl: Duration::from_millis(600),
        ..RouterConfig::default()
    };
    let node_a = Node::builder(a.clone())
        .routing(configure())
        .link(MemLink::new(left.endpoint(a.clone()), vec![b.clone()]))
        .gossip_interval_ms(30)
        .start()
        .await?;
    let node_b = Node::builder(b.clone())
        .routing(configure())
        .link(MemLink::new(left.endpoint(b.clone()), vec![a.clone()]))
        .link(MemLink::new(right.endpoint(b.clone()), vec![c.clone()]))
        .gossip_interval_ms(30)
        .start()
        .await?;
    let node_c = Node::builder(c.clone())
        .routing(configure())
        .link(MemLink::new(right.endpoint(c.clone()), vec![b.clone()]))
        .gossip_interval_ms(30)
        .start()
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
    let node = Node::builder(NodeId::new("owned")).start().await?;
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
