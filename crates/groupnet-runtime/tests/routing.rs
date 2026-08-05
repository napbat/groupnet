//! Integration test for the inter-group routing table: from any node, resolve a
//! resource to the coordinator of the group that owns it.

use groupnet_core::GroupId;
use groupnet_testkit::cluster::{MemCluster, eventually};

#[tokio::test]
async fn resolves_resource_to_owning_groups_coordinator() {
    // All three nodes host shard-1.
    let shard1 = GroupId::new("shard-1");
    let cluster = MemCluster::builder(&["node-a", "node-b", "node-c"])
        .group(shard1.clone())
        .gossip_interval_ms(20)
        .spawn();
    let groups = &cluster.groups;
    let nodes = &cluster.nodes;

    // Wait for shard-1 to converge on a coordinator.
    eventually("shard-1 to converge on a coordinator", || {
        let c = groups[0].coordinator();
        c.is_some() && groups.iter().all(|g| g.coordinator() == c)
    })
    .await;
    let shard1_coord = groups[0].coordinator().expect("coordinator");

    // node-a claims the "users" key-range for shard-1.
    nodes[0].routing().claim("users", &shard1);

    // From *every* node, routing must resolve "users" to shard-1's coordinator.
    eventually("routing to converge cluster-wide", || {
        nodes.iter().all(|n| {
            let r = n.routing();
            r.owner("users") == Some(shard1.clone())
                && r.route("users") == Some(shard1_coord.clone())
        })
    })
    .await;

    // An unknown resource resolves to nothing. (The cluster stays alive until
    // the end of the test — dropping it would tear the nodes down.)
    assert_eq!(nodes[1].routing().route("nonexistent"), None);
}
