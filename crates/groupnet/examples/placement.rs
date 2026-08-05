//! Weighted rendezvous ("HA-hash") placement — the deterministic primitive
//! Groupnet derives shard/replica ownership and coordinators from.
//!
//! Pure and synchronous: no runtime, no I/O, no clock. Every node computes the
//! same answer on every platform.
//!
//! ```text
//! cargo run --example placement
//! ```

use std::collections::BTreeSet;

use groupnet::core::NodeId;
use groupnet::core::placement::{owner, owners};

fn main() {
    // Members with capacity weights: node-c is twice as beefy, so it should win
    // roughly twice as many keys.
    let members: Vec<(NodeId, u32)> = [("node-a", 1), ("node-b", 1), ("node-c", 2)]
        .into_iter()
        .map(|(id, weight)| (NodeId::new(id), weight))
        .collect();
    let keys = ["users", "orders", "sessions", "invoices"];

    println!("Members (id, weight): [(node-a, 1), (node-b, 1), (node-c, 2)]\n");

    println!("Top-2 owners (primary + one replica) of each key:");
    for key in keys {
        let top = owners(key, &members, 2);
        println!("  {key:>9} -> {}", join(&top));
    }

    // Rendezvous hashing's defining property: removing a node moves only the keys
    // it owned — every other key stays put.
    println!("\nRemove node-c; only its keys move:");
    let reduced: Vec<(NodeId, u32)> = members
        .iter()
        .filter(|(n, _)| n.as_str() != "node-c")
        .cloned()
        .collect();
    for key in keys {
        let before = owners(key, &members, 1)[0].clone();
        let after = owners(key, &reduced, 1)[0].clone();
        let note = if before == after { "" } else { "  (moved)" };
        println!("  {key:>9}: {before} -> {after}{note}");
    }

    // The group coordinator is just the single owner of the group id among live
    // members — the same math, unweighted.
    let live: BTreeSet<NodeId> = members.iter().map(|(n, _)| n.clone()).collect();
    let coord = owner("shard-42", &live).expect("non-empty membership");
    println!("\nCoordinator of \"shard-42\" = {coord}");
}

/// Renders a node list as `[a, b]` for readable output.
fn join(nodes: &[NodeId]) -> String {
    let names: Vec<&str> = nodes.iter().map(NodeId::as_str).collect();
    format!("[{}]", names.join(", "))
}
