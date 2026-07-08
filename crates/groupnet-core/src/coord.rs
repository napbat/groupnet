//! Deterministic coordinator selection.
//!
//! Groupnet does not elect a leader — every node derives the same coordinator
//! from the same member set. We use **rendezvous (highest-random-weight)
//! hashing**: each candidate is scored by `hash(group ‖ node)` and the highest
//! score wins. Compared to "lowest node id" this spreads coordinator load
//! evenly across groups and stays stable under churn (adding/removing a node
//! only moves the coordinator if that node *was* or *becomes* the winner).
//!
//! The hash is a hand-rolled FNV-1a so the result is identical on every
//! platform and toolchain — a requirement, since all nodes must independently
//! agree. (`std`'s `DefaultHasher` is explicitly *not* stable across versions,
//! so it must never be used for cross-node agreement.)

use std::collections::BTreeSet;

use crate::{GroupId, NodeId};

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

fn fnv1a(parts: &[&[u8]]) -> u64 {
    let mut h = FNV_OFFSET;
    for part in parts {
        for &byte in *part {
            h ^= u64::from(byte);
            h = h.wrapping_mul(FNV_PRIME);
        }
        // separator so ("ab","c") and ("a","bc") hash differently
        h ^= 0xff;
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

/// Score of a node's claim to coordinate `group`. Higher wins.
fn score(group: &GroupId, node: &NodeId) -> u64 {
    fnv1a(&[group.as_str().as_bytes(), node.as_str().as_bytes()])
}

/// Deterministically selects the coordinator for `group` from `members`.
///
/// Returns `None` only when the member set is empty. Given the same inputs on
/// any machine, always returns the same node.
pub fn select(group: &GroupId, members: &BTreeSet<NodeId>) -> Option<NodeId> {
    members
        // Iterate in a defined order and break score ties by id, so the result
        // never depends on hash-map iteration order.
        .iter()
        .max_by(|a, b| score(group, a).cmp(&score(group, b)).then_with(|| a.cmp(b)))
        .cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn members(ids: &[&str]) -> BTreeSet<NodeId> {
        ids.iter().map(|s| NodeId::new(*s)).collect()
    }

    #[test]
    fn selection_is_order_independent() {
        let g = GroupId::new("shard-42");
        let a = select(&g, &members(&["node-a", "node-b", "node-c"]));
        let b = select(&g, &members(&["node-c", "node-a", "node-b"]));
        assert_eq!(a, b);
        assert!(a.is_some());
    }

    #[test]
    fn different_groups_can_pick_different_coordinators() {
        // Not guaranteed for every pair, but across enough groups the winner
        // must vary — otherwise the hash isn't spreading load at all.
        let set = members(&["node-a", "node-b", "node-c", "node-d"]);
        let winners: BTreeSet<_> = (0..32)
            .filter_map(|i| select(&GroupId::new(format!("shard-{i}")), &set))
            .collect();
        assert!(winners.len() > 1, "coordinator never varied across groups");
    }

    #[test]
    fn empty_set_has_no_coordinator() {
        assert_eq!(select(&GroupId::new("g"), &BTreeSet::new()), None);
    }
}
