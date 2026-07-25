//! Two real nodes on loopback with the **persistent-TCP control plane**:
//! membership converges and an entry authored on one node arrives on the
//! other, with the connection pool staying bounded to the active fanout.
//!
//! This is the constant-connection deployment shape: the choice lives
//! entirely in the transport handed to `Node::builder` — the engine, the
//! protocol, and gossip-over-UDP deployments are untouched.

#![cfg(all(feature = "runtime", feature = "tcp-msg"))]

use std::time::Duration;

use groupnet::core::NodeId;
use groupnet::runtime::Node;
use groupnet::transport::tcp::TcpMsgTransport;

/// Polls until `cond` holds or a deadline passes.
async fn eventually(mut cond: impl FnMut() -> bool, what: &str) {
    for _ in 0..1000 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for: {what}");
}

#[tokio::test]
async fn nodes_gossip_and_sync_entries_over_persistent_tcp() {
    let a_id = NodeId::new("tcp-a");
    let b_id = NodeId::new("tcp-b");
    let ta = TcpMsgTransport::bind(a_id.clone(), "127.0.0.1:0")
        .await
        .expect("bind a");
    let tb = TcpMsgTransport::bind(b_id.clone(), "127.0.0.1:0")
        .await
        .expect("bind b");
    ta.register_peer(b_id.clone(), tb.local_addr());
    tb.register_peer(a_id.clone(), ta.local_addr());

    // Clones share the pool, so we can watch it from outside the nodes.
    let a_pool = ta.clone();
    let b_pool = tb.clone();

    let a = Node::builder(a_id.clone(), ta)
        .seed(b_id.clone())
        .gossip_interval_ms(20)
        .anti_entropy_interval_ms(50)
        .spawn();
    let b = Node::builder(b_id.clone(), tb)
        .seed(a_id.clone())
        .gossip_interval_ms(20)
        .anti_entropy_interval_ms(50)
        .spawn();

    let ga = a.join_group("shard");
    let gb = b.join_group("shard");
    eventually(
        || ga.members().len() == 2 && gb.members().len() == 2,
        "membership convergence over TCP",
    )
    .await;

    ga.set_entry("route", b"v3", None).expect("entry accepted");
    eventually(
        || gb.node_entry(&a_id, "route").as_deref() == Some(b"v3"),
        "entry replication over TCP",
    )
    .await;

    // One peer each: the persistent pool holds one warm socket, not a mesh.
    assert!(a_pool.outbound_connections() <= 1);
    assert!(b_pool.outbound_connections() <= 1);
}
