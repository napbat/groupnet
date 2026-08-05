//! Placement cost: how long it takes a node to work out who owns a key.
//!
//! Every node recomputes this independently — for the coordinator on each
//! membership change, and for a shard/replica lookup on the request path — so it
//! runs at 5, 50, and 500 members, the same axis as the wire benches.
//!
//! Rendezvous hashing is O(members) per lookup, and the weighted form is
//! O(Σ weights) because a node's score is the best of its virtual tokens. Both
//! shapes are measured, since the second is what a heterogeneous cluster pays.
//!
//! ```text
//! cargo bench -p groupnet-core --bench placement
//! ```

use std::collections::BTreeSet;

use divan::Bencher;

use groupnet_core::{NodeId, placement};

/// Cluster sizes — the axis the fabric scales along.
const SIZES: [usize; 3] = [5, 50, 500];

/// Replicas requested by the `owners` benches: a typical replica set.
const REPLICAS: usize = 3;

/// Virtual tokens per node in the weighted case — a node with four times a
/// baseline node's capacity.
const HEAVY_WEIGHT: u32 = 4;

fn main() {
    divan::main();
}

fn node(i: usize) -> NodeId {
    NodeId::new(format!("node-{i:03}"))
}

fn member_set(members: usize) -> BTreeSet<NodeId> {
    (0..members).map(node).collect()
}

fn weighted(members: usize, weight: u32) -> Vec<(NodeId, u32)> {
    (0..members).map(|i| (node(i), weight)).collect()
}

/// The coordinator path: one owner over an unweighted live set, no allocation.
#[divan::bench(args = SIZES)]
fn owner(bencher: Bencher, members: usize) {
    let set = member_set(members);
    bencher.bench(|| placement::owner(divan::black_box("shard-42"), divan::black_box(&set)));
}

/// The replica path: rank every member and take the top three.
#[divan::bench(args = SIZES)]
fn owners_unit_weight(bencher: Bencher, members: usize) {
    let set = weighted(members, 1);
    bencher.bench(|| {
        placement::owners(
            divan::black_box("users/42"),
            divan::black_box(&set),
            REPLICAS,
        )
    });
}

/// The same ranking on a heterogeneous cluster: each node's score is the best
/// of its `HEAVY_WEIGHT` virtual tokens, so the hashing work scales with the
/// weight total rather than the member count.
#[divan::bench(args = SIZES)]
fn owners_weighted(bencher: Bencher, members: usize) {
    let set = weighted(members, HEAVY_WEIGHT);
    bencher.bench(|| {
        placement::owners(
            divan::black_box("users/42"),
            divan::black_box(&set),
            REPLICAS,
        )
    });
}
