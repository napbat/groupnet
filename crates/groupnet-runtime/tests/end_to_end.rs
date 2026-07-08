//! Integration test: real async runtime, real Transport trait, group-per-task
//! actors — three nodes must converge on one coordinator over the in-memory
//! transport.

use std::time::Duration;

use groupnet_core::NodeId;
use groupnet_runtime::Node;
use groupnet_transport_mem::Network;

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

    // Each node advertises app-defined per-node state; it must reach every node.
    for (i, g) in groups.iter().enumerate() {
        g.set_state(format!("weight={i}"));
    }
    let mut states_converged = false;
    for _ in 0..100 {
        if groups.iter().all(|g| {
            ids.iter().enumerate().all(|(i, id)| {
                g.node_state(id).as_deref() == Some(format!("weight={i}").as_bytes())
            })
        }) {
            states_converged = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(states_converged, "per-node state did not converge");

    // Membership converged to all three over the live read path.
    assert!(groups.iter().all(|g| g.members().len() == 3));

    // node-c leaves gracefully; the other two must drop it from their view.
    let leaver = ids[2].clone();
    groups[2].leave();
    let mut removed = false;
    for _ in 0..150 {
        if groups[..2].iter().all(|g| !g.members().contains(&leaver)) {
            removed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(removed, "graceful leave did not propagate");
    for g in &groups[..2] {
        assert_eq!(g.members().len(), 2);
    }

    // Keep nodes alive until the end of the test.
    drop(nodes);
}
