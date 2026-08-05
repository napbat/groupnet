//! The **default-feature** smoke test for the umbrella crate: a three-node
//! cluster built only out of `groupnet::*` re-exports (`core`, `runtime`,
//! `transport::mem`), converging and replicating a metadata write.
//!
//! Its subject is the facade surface itself — the paths a new user copies out
//! of the crate docs must compile and run with nothing but `default`
//! features on. The underlying behaviour is covered in each layer's own tests.

#![cfg(all(feature = "runtime", feature = "mem"))]

use groupnet::core::NodeId;
use groupnet::runtime::{Group, Node};
use groupnet::transport::mem::{MemTransport, Network};
use groupnet::transport::{Inbound, Transport};
use groupnet_testkit::cluster::eventually;

/// Nodes named for the test, all-to-all seeded.
const IDS: [&str; 3] = ["facade-a", "facade-b", "facade-c"];

#[tokio::test]
async fn cluster_built_from_facade_paths_converges_and_replicates() {
    let ids: Vec<NodeId> = IDS.iter().map(|s| NodeId::new(*s)).collect();
    let net = Network::new();

    // Built by hand rather than via a fixture: the point is that these exact
    // facade paths resolve under default features.
    let mut nodes: Vec<Node<MemTransport>> = Vec::with_capacity(ids.len());
    let mut groups: Vec<Group> = Vec::with_capacity(ids.len());
    for id in &ids {
        let mut builder =
            Node::builder(id.clone(), net.endpoint(id.clone())).gossip_interval_ms(20);
        for seed in ids.iter().filter(|other| *other != id) {
            builder = builder.seed(seed.clone());
        }
        let node = builder.spawn();
        groups.push(node.join_group("shard-42"));
        nodes.push(node);
    }

    eventually("the facade-built cluster to see all three members", || {
        groups.iter().all(|g| g.members().len() == ids.len())
    })
    .await;
    eventually("the cluster to agree on one coordinator", || {
        let coords: Vec<_> = groups.iter().map(Group::coordinator).collect();
        coords.iter().all(Option::is_some) && coords.windows(2).all(|w| w[0] == w[1])
    })
    .await;

    groups[0].sync(|ctx| ctx.update_metadata("routing", "v3"));
    eventually("a metadata write to reach every node", || {
        groups
            .iter()
            .all(|g| g.metadata("routing").as_deref() == Some("v3"))
    })
    .await;

    // The nodes are still the ones we named — and still running, since a
    // dropped `Node` stops its tasks.
    assert!(nodes.iter().map(Node::id).eq(ids.iter()));
}

/// The transport re-exports stand on their own: the trait, its `Inbound`
/// type, and the in-memory binding are all reachable as `groupnet::transport::*`.
#[tokio::test]
async fn mem_transport_round_trips_through_the_facade_trait() {
    let net = Network::new();
    let a = net.endpoint(NodeId::new("facade-t-a"));
    let b = net.endpoint(NodeId::new("facade-t-b"));

    a.send(&NodeId::new("facade-t-b"), b"ping")
        .await
        .expect("send");

    let got: Inbound = b.recv().await.expect("recv");
    assert_eq!(got.from, NodeId::new("facade-t-a"));
    assert_eq!(got.msg, b"ping".to_vec());
}
