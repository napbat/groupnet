//! Integration test: real async runtime, real Transport trait, group-per-task
//! actors — three nodes must converge on one coordinator over the in-memory
//! transport.

use groupnet_testkit::cluster::{MemCluster, eventually};

#[tokio::test]
async fn three_nodes_converge_over_mem_transport() {
    // Bring up three nodes, each seeded with the other two.
    let cluster = MemCluster::builder(&["node-a", "node-b", "node-c"])
        .group("shard-42")
        .gossip_interval_ms(20)
        .spawn();
    let ids = &cluster.ids;
    let groups = &cluster.groups;

    // Poll for convergence with a bounded timeout — no fixed-sleep race.
    eventually("nodes to converge on a coordinator", || {
        let coords: Vec<_> = groups.iter().map(|g| g.coordinator()).collect();
        coords.iter().all(Option::is_some) && coords.windows(2).all(|w| w[0] == w[1])
    })
    .await;

    // Exactly one node considers itself the coordinator.
    let leaders = groups.iter().filter(|g| g.is_coordinator()).count();
    assert_eq!(leaders, 1, "expected exactly one coordinator");

    // Write metadata on one node; it must gossip to every node.
    groups[0].sync(|ctx| ctx.update_metadata("routing", "v3"));

    eventually("metadata to propagate to all nodes", || {
        groups
            .iter()
            .all(|g| g.metadata("routing").as_deref() == Some("v3"))
    })
    .await;

    // Each node advertises app-defined per-node state; it must reach every node.
    for (i, g) in groups.iter().enumerate() {
        g.set_state(format!("weight={i}"));
    }
    eventually("per-node state to converge", || {
        groups.iter().all(|g| {
            ids.iter().enumerate().all(|(i, id)| {
                g.node_state(id).as_deref() == Some(format!("weight={i}").as_bytes())
            })
        })
    })
    .await;

    // Membership converged to all three over the live read path.
    assert!(groups.iter().all(|g| g.members().len() == 3));

    // node-c leaves gracefully; the other two must drop it from their view.
    let leaver = ids[2].clone();
    groups[2].leave();
    eventually("the graceful leave to propagate", || {
        groups[..2].iter().all(|g| !g.members().contains(&leaver))
    })
    .await;
    for g in &groups[..2] {
        assert_eq!(g.members().len(), 2);
    }

    // The cluster stays alive until the end of the test.
}
