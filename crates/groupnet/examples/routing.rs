//! Inter-group routing: resolve a resource to the node that owns it, from any
//! node in the cluster, with no global consensus.
//!
//! Each group's coordinator is auto-published into a cluster-wide routing table
//! (itself just gossiped last-writer-wins metadata). Claim a resource for a
//! group, and any node can then route to that group's coordinator.
//!
//! ```text
//! cargo run --example routing
//! ```

use std::time::Duration;

use groupnet::core::{GroupId, NodeId};
use groupnet::runtime::Node;
use groupnet::transport::mem::{MemTransport, Network};

const NODE_IDS: [&str; 3] = ["node-a", "node-b", "node-c"];
const SHARD: &str = "shard-1";

#[tokio::main]
async fn main() {
    let net = Network::new();

    // Every node joins group "shard-1". Each group actor auto-publishes the
    // coordinator it observes into the routing table, so routing needs no
    // separate bookkeeping.
    let nodes: Vec<(NodeId, Node<MemTransport>)> = NODE_IDS
        .iter()
        .map(|id| {
            let me = NodeId::new(*id);
            let mut builder = Node::builder(me.clone(), net.endpoint(me.clone()));
            for peer in NODE_IDS.iter().filter(|p| *p != id) {
                builder = builder.seed(NodeId::new(*peer));
            }
            let node = builder.spawn();
            node.join_group(SHARD); // the group actor stays alive inside the node
            (me, node)
        })
        .collect();

    // Wait until every node's routing table knows shard-1's coordinator.
    let shard = GroupId::new(SHARD);
    wait_until(|| {
        nodes
            .iter()
            .all(|(_, n)| n.routing().coordinator_of(&shard).is_some())
    })
    .await;

    // Claim the "users" resource for shard-1 (in practice, its coordinator does
    // this). The claim gossips out as routing-table metadata.
    nodes[0].1.routing().claim("users", &shard);
    wait_until(|| {
        nodes
            .iter()
            .all(|(_, n)| n.routing().owner("users").is_some())
    })
    .await;

    // Now any node resolves "users" -> owning group -> that group's coordinator.
    println!("Resolving resource \"users\" from every node:");
    for (id, node) in &nodes {
        let routing = node.routing();
        let owner = routing.owner("users").map(|g| g.to_string());
        let route = routing.route("users").map(|n| n.to_string());
        println!("  {id}: owner={owner:?}  route={route:?}");
    }
}

/// Polls `cond` until it holds or a generous deadline elapses.
async fn wait_until(mut cond: impl FnMut() -> bool) -> bool {
    for _ in 0..200 {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    cond()
}
