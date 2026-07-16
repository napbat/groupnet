//! Integration coverage for the keyed per-node state surface: cross-node
//! dissemination, TTL expiry, deletes, the events stream, idempotent joins,
//! and `~addr` dissemination.

use std::time::Duration;

use groupnet_core::NodeId;
use groupnet_runtime::{GroupEvent, Node};
use groupnet_transport_mem::Network;

async fn poll<F: Fn() -> bool>(what: &str, f: F) {
    for _ in 0..250 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test]
async fn keyed_entries_disseminate_expire_and_delete() {
    let net = Network::new();
    let ids: Vec<NodeId> = ["node-a", "node-b"]
        .iter()
        .map(|s| NodeId::new(*s))
        .collect();
    let mut nodes = Vec::new();
    for id in &ids {
        let mut b = Node::builder(id.clone(), net.endpoint(id.clone()))
            .gossip_interval_ms(20)
            .advertise_addr(format!(
                "10.0.0.{}",
                if id.as_str() == "node-a" { 1 } else { 2 }
            ));
        for other in &ids {
            if other != id {
                b = b.seed(other.clone());
            }
        }
        nodes.push(b.spawn());
    }
    let a = nodes[0].join_group("g");
    let b = nodes[1].join_group("g");

    // Idempotent join: a second join returns a handle to the SAME actor (a
    // write through one is visible through the other).
    let a2 = nodes[0].join_group("g");

    // Cross-node dissemination of independent keys, one with a TTL.
    a.set_entry("ready", b"1", None).unwrap();
    a.set_entry("hot/0", b"page-0", Some(400)).unwrap();
    poll("b sees a's entries", || {
        b.node_entry(&ids[0], "ready").is_some() && b.node_entry(&ids[0], "hot/0").is_some()
    })
    .await;
    assert_eq!(a2.node_entry(&ids[0], "ready").as_deref(), Some(&b"1"[..]));

    // Delete: tombstone disseminates, key drops everywhere.
    a.delete_entry("ready").unwrap();
    poll("delete reaches b", || {
        b.node_entry(&ids[0], "ready").is_none()
    })
    .await;
    assert!(
        b.node_entry(&ids[0], "hot/0").is_some(),
        "other keys untouched by the delete"
    );

    // TTL: once node-a stops refreshing hot/0, it expires on BOTH nodes
    // (locally too — the author's own copy ages out the same way).
    poll("ttl expiry on b", || {
        b.node_entry(&ids[0], "hot/0").is_none()
    })
    .await;
    poll("ttl expiry on a", || {
        a.node_entry(&ids[0], "hot/0").is_none()
    })
    .await;

    // ~addr dissemination: each node resolves the other from gossip.
    poll("addr resolution", || {
        nodes[0].peer_addr(&ids[1]).as_deref() == Some("10.0.0.2")
            && nodes[1].peer_addr(&ids[0]).as_deref() == Some("10.0.0.1")
    })
    .await;
}

#[tokio::test]
async fn events_stream_fires_on_entry_changes() {
    let net = Network::new();
    let ids: Vec<NodeId> = ["node-a", "node-b"]
        .iter()
        .map(|s| NodeId::new(*s))
        .collect();
    let mut nodes = Vec::new();
    for id in &ids {
        let mut b = Node::builder(id.clone(), net.endpoint(id.clone())).gossip_interval_ms(20);
        for other in &ids {
            if other != id {
                b = b.seed(other.clone());
            }
        }
        nodes.push(b.spawn());
    }
    let a = nodes[0].join_group("g");
    let b = nodes[1].join_group("g");

    let mut events = b.events();
    a.set_entry("progress", b"42", None).unwrap();

    // b's subscriber sees the entry change arrive via gossip (bounded wait).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let event = tokio::time::timeout_at(deadline, events.recv())
            .await
            .expect("timed out waiting for NodeStateChanged")
            .expect("stream open");
        if let GroupEvent::NodeStateChanged { node, key } = event
            && node == ids[0]
            && key == "progress"
        {
            break;
        }
    }
    assert_eq!(
        b.node_entry(&ids[0], "progress").as_deref(),
        Some(&b"42"[..])
    );
}
