//! A 3-node cluster over the in-memory transport: watch it converge on a derived
//! coordinator, then have that coordinator publish metadata the others read.
//!
//! Every node uses the same builder. Replace `MemLink` with a socket link, or
//! register several links, without changing the node type or group operations.
//!
//! Observation is event-driven: subscribe to a group's events *before* the
//! first snapshot read, then re-read the snapshots (the shared `roster`,
//! `coordinator`, `metadata`) after every edge. The event stream is bounded;
//! a subscriber that falls behind sees `Lagged` and resyncs from the snapshots,
//! which are always current.
//!
//! ```text
//! cargo run --example cluster
//! ```

use std::io;
use std::time::Duration;

use groupnet::core::NodeId;
use groupnet::runtime::{Group, GroupEvent, Node};
use groupnet::transport::mem::{MemLink, Network};
use tokio::sync::broadcast::{self, error::RecvError};
use tokio::sync::mpsc;

const GROUP: &str = "shard-42";
const NODE_IDS: [&str; 3] = ["node-a", "node-b", "node-c"];
/// A generous bound for in-memory gossip to converge.
const DEADLINE: Duration = Duration::from_secs(10);

#[tokio::main]
async fn main() -> io::Result<()> {
    // One shared in-memory fabric; every endpoint created from it can reach the
    // others.
    let net = Network::new();
    // One wake-up channel fed by every group's event stream. Capacity one
    // coalesces bursts: a pending wake already means "re-read the snapshots".
    let (wake, mut wakes) = mpsc::channel(1);

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
        // Subscribe before any snapshot read, so no change can fall between them.
        forward(group.events(), wake.clone());
        cluster.push((me, node, group));
    }
    drop(wake);

    // Gossip converges the membership and the derived coordinator. Every node
    // computes the same coordinator from the same live-member set.
    settle(&mut wakes, "membership", || {
        let coordinator = cluster[0].2.coordinator();
        coordinator.is_some()
            && cluster.iter().all(|(_, _, group)| {
                group.roster().len() == NODE_IDS.len() && group.coordinator() == coordinator
            })
    })
    .await?;
    println!("== membership converged ==");
    for (id, _, group) in &cluster {
        let coord = group
            .coordinator()
            .map_or_else(|| "?".to_string(), |c| c.to_string());
        // `roster` shares the published snapshot instead of cloning it.
        let members: Vec<String> = group.roster().iter().map(NodeId::to_string).collect();
        println!("  {id}: coordinator={coord}  members={members:?}");
    }

    // Whichever node the cluster derived as coordinator writes shared metadata;
    // it disseminates by gossip and merges last-writer-wins everywhere.
    let coordinator = cluster.iter().find(|(_, _, g)| g.is_coordinator());
    if let Some((id, _, group)) = coordinator {
        println!("\n{id} is the coordinator — publishing metadata key \"leader\"...");
        group.sync(|ctx| ctx.update_metadata("leader", id.to_string()));
    }

    settle(&mut wakes, "metadata", || {
        cluster
            .iter()
            .all(|(_, _, g)| g.metadata("leader").is_some())
    })
    .await?;
    println!("\n== metadata converged ==");
    for (id, _, group) in &cluster {
        println!(
            "  {id}: metadata[\"leader\"] = {:?}",
            group.metadata("leader")
        );
    }
    Ok(())
}

/// Turns one group's bounded event stream into wake-ups for the snapshot reader.
fn forward(mut events: broadcast::Receiver<GroupEvent>, wake: mpsc::Sender<()>) {
    tokio::spawn(async move {
        loop {
            match events.recv().await {
                // `Lagged` means some edges were dropped. Snapshots are always
                // current, so one re-read resyncs exactly like a delivered edge.
                Ok(_) | Err(RecvError::Lagged(_)) => {
                    if wake.try_send(()).is_err() && wake.is_closed() {
                        return;
                    }
                }
                // The group actor is gone; there is nothing left to observe.
                Err(RecvError::Closed) => return,
            }
        }
    });
}

/// Re-reads the snapshots behind `converged` now and after every change, until
/// it holds or [`DEADLINE`] elapses.
async fn settle(
    wakes: &mut mpsc::Receiver<()>,
    what: &str,
    mut converged: impl FnMut() -> bool,
) -> io::Result<()> {
    let settled = tokio::time::timeout(DEADLINE, async {
        while !converged() {
            if wakes.recv().await.is_none() {
                return false;
            }
        }
        true
    })
    .await;
    if settled == Ok(true) {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!("{what} did not converge"),
        ))
    }
}
