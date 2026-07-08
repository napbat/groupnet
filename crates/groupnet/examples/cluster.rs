//! A 3-node cluster over the in-memory transport: watch it converge on a derived
//! coordinator, then have that coordinator publish metadata the others read.
//!
//! The in-memory transport stands in for real sockets — swap it for
//! `groupnet::transport::udp` and nothing else changes.
//!
//! ```text
//! cargo run --example cluster
//! ```

use std::time::Duration;

use groupnet::core::NodeId;
use groupnet::runtime::{Group, Node};
use groupnet::transport::mem::{MemTransport, Network};

const GROUP: &str = "shard-42";
const NODE_IDS: [&str; 3] = ["node-a", "node-b", "node-c"];

#[tokio::main]
async fn main() {
    // One shared in-memory fabric; every endpoint created from it can reach the
    // others.
    let net = Network::new();

    // Bring up each node seeded with its peers, and join the shared group. The
    // `Node`s are kept alive for the run; all state lives in their actor tasks.
    let cluster: Vec<(NodeId, Node<MemTransport>, Group)> = NODE_IDS
        .iter()
        .map(|id| {
            let me = NodeId::new(*id);
            let mut builder = Node::builder(me.clone(), net.endpoint(me.clone()));
            for peer in NODE_IDS.iter().filter(|p| *p != id) {
                builder = builder.seed(NodeId::new(*peer));
            }
            let node = builder.spawn();
            let group = node.join_group(GROUP);
            (me, node, group)
        })
        .collect();

    // Gossip converges the membership and the derived coordinator. Every node
    // computes the same coordinator from the same live-member set.
    let converged = wait_until(|| {
        let coords: Vec<_> = cluster.iter().map(|(_, _, g)| g.coordinator()).collect();
        coords.iter().all(Option::is_some) && coords.windows(2).all(|w| w[0] == w[1])
    })
    .await;
    println!(
        "== membership {} ==",
        if converged {
            "converged"
        } else {
            "did not converge in time"
        }
    );
    for (id, _, group) in &cluster {
        let coord = group
            .coordinator()
            .map_or_else(|| "?".to_string(), |c| c.to_string());
        let members: Vec<String> = group.members().iter().map(NodeId::to_string).collect();
        println!("  {id}: coordinator={coord}  members={members:?}");
    }

    // Whichever node the cluster derived as coordinator writes shared metadata;
    // it disseminates by gossip and merges last-writer-wins everywhere.
    let coordinator = cluster.iter().find(|(_, _, g)| g.is_coordinator());
    if let Some((id, _, group)) = coordinator {
        println!("\n{id} is the coordinator — publishing metadata key \"leader\"...");
        group.sync(|ctx| ctx.update_metadata("leader", id.to_string()));
    }

    wait_until(|| {
        cluster
            .iter()
            .all(|(_, _, g)| g.metadata("leader").is_some())
    })
    .await;
    println!("\n== metadata converged ==");
    for (id, _, group) in &cluster {
        println!(
            "  {id}: metadata[\"leader\"] = {:?}",
            group.metadata("leader")
        );
    }
}

/// Polls `cond` until it holds or a generous deadline elapses. Gossip is
/// eventually consistent, so a demo waits rather than assumes instant delivery.
/// Returns whether the condition ultimately held.
async fn wait_until(mut cond: impl FnMut() -> bool) -> bool {
    for _ in 0..200 {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    cond()
}
