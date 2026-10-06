//! A 3-node cluster over the in-memory transport: watch it converge on a derived
//! coordinator, then have that coordinator publish metadata the others read.
//!
//! Every node uses the same builder. Replace `MemLink` with a socket link, or
//! register several links, without changing the node type or group operations.
//!
//! ```text
//! cargo run --example cluster
//! ```

use std::time::Duration;

use groupnet::core::NodeId;
use groupnet::runtime::{Group, Node};
use groupnet::transport::mem::{MemLink, Network};

const GROUP: &str = "shard-42";
const NODE_IDS: [&str; 3] = ["node-a", "node-b", "node-c"];

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // One shared in-memory fabric; every endpoint created from it can reach the
    // others.
    let net = Network::new();

    // Adjacent peers also seed membership. Keep each node alive for the run.
    let mut cluster: Vec<(NodeId, Node, Group)> = Vec::with_capacity(NODE_IDS.len());
    for id in NODE_IDS {
        let me = NodeId::new(id);
        let peers = NODE_IDS
            .iter()
            .filter(|peer| **peer != id)
            .map(|peer| NodeId::new(*peer))
            .collect();
        let node = Node::builder(me.clone())
            .link(MemLink::new(net.endpoint(me.clone()), peers))
            .start()
            .await?;
        let group = node.join_group(GROUP);
        cluster.push((me, node, group));
    }

    // Gossip converges the membership and the derived coordinator. Every node
    // computes the same coordinator from the same live-member set.
    if !wait_until(|| {
        let coordinator = cluster[0].2.coordinator();
        coordinator.is_some()
            && cluster.iter().all(|(_, _, group)| {
                group.members().len() == NODE_IDS.len() && group.coordinator() == coordinator
            })
    })
    .await
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "membership did not converge",
        ));
    }
    println!("== membership converged ==");
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

    if !wait_until(|| {
        cluster
            .iter()
            .all(|(_, _, g)| g.metadata("leader").is_some())
    })
    .await
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "metadata did not converge",
        ));
    }
    println!("\n== metadata converged ==");
    for (id, _, group) in &cluster {
        println!(
            "  {id}: metadata[\"leader\"] = {:?}",
            group.metadata("leader")
        );
    }
    Ok(())
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
