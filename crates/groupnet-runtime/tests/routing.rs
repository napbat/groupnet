//! Integration test for the inter-group routing table: from any node, resolve a
//! resource to the coordinator of the group that owns it.

use std::time::Duration;

use groupnet_core::{GroupId, NodeId};
use groupnet_runtime::Node;
use groupnet_transport_mem::Network;

async fn settle<F: Fn() -> bool>(cond: F) -> bool {
    for _ in 0..200 {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    false
}

#[tokio::test]
async fn resolves_resource_to_owning_groups_coordinator() {
    let net = Network::new();
    let ids: Vec<NodeId> = ["node-a", "node-b", "node-c"]
        .iter()
        .map(|s| NodeId::new(*s))
        .collect();

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

    // All three nodes host shard-1.
    let shard1 = GroupId::new("shard-1");
    let groups: Vec<_> = nodes.iter().map(|n| n.join_group(shard1.clone())).collect();

    // Wait for shard-1 to converge on a coordinator.
    assert!(
        settle(|| {
            let c = groups[0].coordinator();
            c.is_some() && groups.iter().all(|g| g.coordinator() == c)
        })
        .await,
        "shard-1 did not converge on a coordinator"
    );
    let shard1_coord = groups[0].coordinator().expect("coordinator");

    // node-a claims the "users" key-range for shard-1.
    nodes[0].routing().claim("users", &shard1);

    // From *every* node, routing must resolve "users" to shard-1's coordinator.
    let resolved = settle(|| {
        nodes.iter().all(|n| {
            let r = n.routing();
            r.owner("users") == Some(shard1.clone())
                && r.route("users") == Some(shard1_coord.clone())
        })
    })
    .await;
    assert!(resolved, "routing did not converge cluster-wide");

    // An unknown resource resolves to nothing.
    assert_eq!(nodes[1].routing().route("nonexistent"), None);

    drop(nodes);
}
