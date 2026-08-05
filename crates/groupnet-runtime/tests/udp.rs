//! Integration test over *real* UDP sockets: three nodes on loopback must
//! converge on a coordinator through the network stack — exercising the
//! `groupnet-transport-udp` binding against the transport-agnostic runtime.

use groupnet_core::NodeId;
use groupnet_runtime::Node;
use groupnet_testkit::cluster::eventually;
use groupnet_transport_udp::UdpTransport;

#[tokio::test]
async fn three_nodes_converge_over_udp() {
    let ids: Vec<NodeId> = ["node-a", "node-b", "node-c"]
        .iter()
        .map(|s| NodeId::new(*s))
        .collect();

    // Bind ephemeral loopback ports, then teach every endpoint every peer's
    // address (static address book).
    let mut transports = Vec::new();
    for id in &ids {
        let t = UdpTransport::bind(id.clone(), "127.0.0.1:0")
            .await
            .expect("bind");
        transports.push(t);
    }
    let addrs: Vec<_> = transports
        .iter()
        .map(|t| t.local_addr().expect("addr"))
        .collect();
    for (i, t) in transports.iter().enumerate() {
        for (j, id) in ids.iter().enumerate() {
            if i != j {
                t.register_peer(id.clone(), addrs[j]);
            }
        }
    }

    // Bring up the nodes over their UDP transports.
    let mut nodes = Vec::new();
    for (i, t) in transports.into_iter().enumerate() {
        let mut builder = Node::builder(ids[i].clone(), t).gossip_interval_ms(30);
        for (j, id) in ids.iter().enumerate() {
            if i != j {
                builder = builder.seed(id.clone());
            }
        }
        nodes.push(builder.spawn());
    }

    let groups: Vec<_> = nodes.iter().map(|n| n.join_group("shard-42")).collect();

    eventually("nodes to converge over UDP", || {
        let c = groups[0].coordinator();
        c.is_some()
            && groups
                .iter()
                .all(|g| g.coordinator() == c && g.members().len() == 3)
    })
    .await;
    assert_eq!(groups.iter().filter(|g| g.is_coordinator()).count(), 1);

    drop(nodes);
}
