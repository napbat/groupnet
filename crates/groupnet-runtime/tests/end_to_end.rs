//! Integration test: real async runtime, real Transport trait, group-per-task
//! actors — three nodes must converge on one coordinator over the in-memory
//! transport.

use std::time::Duration;

use groupnet_core::NodeId;
use groupnet_runtime::Node;
use groupnet_runtime::mem::Network;

#[tokio::test]
async fn three_nodes_converge_over_mem_transport() {
    let net = Network::new();
    let ids: Vec<NodeId> = ["node-a", "node-b", "node-c"]
        .iter()
        .map(|s| NodeId::new(*s))
        .collect();

    // Bring up three nodes, each seeded with the other two.
    let mut nodes = Vec::new();
    for id in &ids {
        let mut builder =
            Node::builder(id.clone(), net.endpoint(id.clone())).gossip_interval_ms(20);
        for other in &ids {
            if other != id {
                builder = builder.seed(other.clone());
            }
        }
        nodes.push(builder.spawn());
    }

    let groups: Vec<_> = nodes.iter().map(|n| n.join_group("shard-42")).collect();

    // Poll for convergence with a bounded timeout — no fixed-sleep race.
    let mut converged = false;
    for _ in 0..100 {
        let coords: Vec<_> = groups.iter().map(|g| g.coordinator()).collect();
        let all_some = coords.iter().all(Option::is_some);
        let all_equal = coords.windows(2).all(|w| w[0] == w[1]);
        if all_some && all_equal {
            converged = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(converged, "nodes did not converge on a coordinator");

    // Exactly one node considers itself the coordinator.
    let leaders = groups.iter().filter(|g| g.is_coordinator()).count();
    assert_eq!(leaders, 1, "expected exactly one coordinator");

    // Write metadata on one node; it must gossip to every node.
    groups[0].sync(|ctx| ctx.update_metadata("routing", "v3"));

    let mut propagated = false;
    for _ in 0..100 {
        if groups
            .iter()
            .all(|g| g.metadata("routing").as_deref() == Some("v3"))
        {
            propagated = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(propagated, "metadata did not propagate to all nodes");

    // Keep nodes alive until the end of the test.
    drop(nodes);
}
