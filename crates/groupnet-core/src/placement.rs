//! Deterministic placement: which node(s) own a key.
//!
//! This is the framework's **HA-hash** primitive — the thing a system built on
//! Groupnet uses to decide shard/replica ownership, and the same math the
//! [coordinator](crate::GroupEngine::coordinator) is derived from.
//!
//! It's **weighted rendezvous (highest-random-weight) hashing**: every node
//! independently computes the same ranking of nodes for a key, and the top-`r`
//! win. Compared to modulo/consistent hashing it gives *minimal disruption* —
//! adding or removing a node only moves the keys that node wins, nothing else.
//!
//! Weighting is done with **integer virtual tokens**: a node of weight `w` gets
//! `w` tokens, and its score for a key is the best of its tokens. Since each of
//! the `Σw` tokens is equally likely to be the global best, a node wins a key
//! with probability exactly `w / Σw` — load proportional to weight. Crucially
//! this uses only integer hashing (a fixed FNV-1a), so every node on every
//! platform computes byte-identical placement — no floating-point `ln`, no
//! cross-machine drift. (`std`'s `DefaultHasher` is deliberately not stable and
//! must never be used here.)

use std::collections::BTreeSet;

use crate::NodeId;

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

const GOLDEN: u64 = 0x9e37_79b9_7f4a_7c15;

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

/// splitmix64 finalizer — strong avalanche, so successive virtual tokens (which
/// differ only by an additive step) decorrelate fully. FNV-1a alone has weak
/// avalanche on trailing bytes, which would leave a node's tokens correlated and
/// bias the weighted distribution toward uniform.
fn mix64(z: u64) -> u64 {
    let z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    let z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// A node's base hash for `key`, before the per-token spread.
fn base_hash(key: &str, node: &NodeId) -> u64 {
    fnv1a(&[key.as_bytes(), node.as_str().as_bytes()])
}

/// Score of one of a node's virtual tokens for `key`.
fn token_score(key: &str, node: &NodeId, token: u32) -> u64 {
    mix64(base_hash(key, node).wrapping_add(u64::from(token).wrapping_mul(GOLDEN)))
}

/// A node's score for `key` — the best of its `weight` virtual tokens.
fn node_score(key: &str, node: &NodeId, weight: u32) -> u64 {
    let base = base_hash(key, node);
    (0..weight)
        .map(|token| mix64(base.wrapping_add(u64::from(token).wrapping_mul(GOLDEN))))
        .max()
        .unwrap_or(0)
}

/// Ranks `members` for `key` and returns the top `replicas` owners, best first.
///
/// Each member is `(node, weight)`; a weight of `0` makes a node ineligible.
/// Deterministic and order-independent: the same inputs always yield the same
/// ranking on any machine. The primary owner is `owners(..)[0]`.
#[must_use]
pub fn owners(key: &str, members: &[(NodeId, u32)], replicas: usize) -> Vec<NodeId> {
    let mut ranked: Vec<(u64, &NodeId)> = members
        .iter()
        .filter(|(_, weight)| *weight > 0)
        .map(|(node, weight)| (node_score(key, node, *weight), node))
        .collect();
    // Highest score first; ties broken by node id (ascending) for determinism.
    ranked.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(b.1)));
    ranked
        .into_iter()
        .take(replicas)
        .map(|(_, node)| node.clone())
        .collect()
}

/// The single owner of `key` among unweighted `members` — the primary /
/// coordinator. Equivalent to [`owners`] with every weight `1` and `replicas`
/// `1`, but without allocating.
#[must_use]
pub fn owner(key: &str, members: &BTreeSet<NodeId>) -> Option<NodeId> {
    members
        .iter()
        .max_by(|a, b| {
            token_score(key, a, 0)
                .cmp(&token_score(key, b, 0))
                // tie: smaller id wins (matches `owners`' ascending-id tiebreak)
                .then_with(|| b.cmp(a))
        })
        .cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `SplitMix64` — a tiny deterministic PRNG so these property tests explore
    /// thousands of configurations reproducibly (no external test deps, matching
    /// the house style). It's exactly the production [`mix64`] finalizer applied
    /// to a golden-ratio-strided counter, so the mixing constants live in one
    /// place.
    struct Rng(u64);
    impl Rng {
        fn new(seed: u64) -> Self {
            Self(seed)
        }
        fn next_u64(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(GOLDEN);
            mix64(self.0)
        }
        fn below(&mut self, n: u32) -> u32 {
            u32::try_from(self.next_u64() % u64::from(n)).expect("`% n` bounds the draw by n")
        }
    }

    fn node(i: u32) -> NodeId {
        NodeId::new(format!("node-{i}"))
    }

    fn key(i: u64) -> String {
        format!("key-{i}")
    }

    /// A random member set of `n` nodes, each weight in `1..=max_weight`.
    fn members(rng: &mut Rng, n: u32, max_weight: u32) -> Vec<(NodeId, u32)> {
        (0..n)
            .map(|i| (node(i), 1 + rng.below(max_weight)))
            .collect()
    }

    #[test]
    fn owner_matches_owners_with_unit_weights() {
        let mut rng = Rng::new(1);
        for _ in 0..200 {
            let n = 1 + rng.below(10);
            let set: BTreeSet<NodeId> = (0..n).map(node).collect();
            let unit: Vec<(NodeId, u32)> = set.iter().map(|id| (id.clone(), 1)).collect();
            for k in 0..50 {
                let key = key(rng.next_u64() ^ k);
                assert_eq!(owner(&key, &set), owners(&key, &unit, 1).into_iter().next());
            }
        }
    }

    #[test]
    fn deterministic_and_order_independent() {
        let mut rng = Rng::new(2);
        for _ in 0..300 {
            let n = 1 + rng.below(12);
            let m = members(&mut rng, n, 5);
            let mut shuffled = m.clone();
            // Fisher-Yates with the deterministic rng.
            for i in (1..shuffled.len()).rev() {
                let bound = u32::try_from(i).expect("member sets here are tiny") + 1;
                shuffled.swap(i, rng.below(bound) as usize);
            }
            let r = 1 + rng.below(4) as usize;
            let k = key(rng.next_u64());
            assert_eq!(owners(&k, &m, r), owners(&k, &shuffled, r));
        }
    }

    #[test]
    fn returns_distinct_owners_capped_at_membership() {
        let mut rng = Rng::new(3);
        for _ in 0..300 {
            let n = 1 + rng.below(8);
            let m = members(&mut rng, n, 4);
            let r = rng.below(12) as usize;
            let got = owners(&key(rng.next_u64()), &m, r);
            assert_eq!(got.len(), r.min(n as usize));
            let distinct: BTreeSet<_> = got.iter().cloned().collect();
            assert_eq!(distinct.len(), got.len(), "owners must be distinct");
        }
    }

    #[test]
    #[expect(
        clippy::cast_precision_loss,
        reason = "key counts here are tens of thousands — exact in an f64 — and the \
                  floats only express the tolerance band"
    )]
    fn unit_weights_spread_load_uniformly() {
        let n = 10u32;
        let m: Vec<(NodeId, u32)> = (0..n).map(|i| (node(i), 1)).collect();
        let keys = 50_000u64;
        let mut counts = vec![0u64; n as usize];
        for k in 0..keys {
            let o = owners(&key(k), &m, 1);
            let idx = o[0]
                .as_str()
                .strip_prefix("node-")
                .unwrap()
                .parse::<usize>()
                .unwrap();
            counts[idx] += 1;
        }
        let avg = keys / u64::from(n);
        for (i, &c) in counts.iter().enumerate() {
            assert!(
                c as f64 > avg as f64 * 0.85 && (c as f64) < avg as f64 * 1.15,
                "node {i} owns {c}, expected ~{avg} (uniform within 15%)"
            );
        }
    }

    #[test]
    #[expect(
        clippy::cast_precision_loss,
        reason = "key counts here are hundreds of thousands — exact in an f64 — and the \
                  floats only express the tolerance band"
    )]
    fn load_is_proportional_to_weight() {
        // Weights 1,1,2,4 → expected shares 1/8, 1/8, 2/8, 4/8.
        let weights = [1u32, 1, 2, 4];
        let total: u32 = weights.iter().sum();
        let m: Vec<(NodeId, u32)> = (0u32..).zip(weights).map(|(i, w)| (node(i), w)).collect();
        let keys = 120_000u64;
        let mut counts = vec![0u64; weights.len()];
        for k in 0..keys {
            let o = owners(&key(k), &m, 1);
            let idx = o[0]
                .as_str()
                .strip_prefix("node-")
                .unwrap()
                .parse::<usize>()
                .unwrap();
            counts[idx] += 1;
        }
        for (i, &w) in weights.iter().enumerate() {
            let expected = keys as f64 * f64::from(w) / f64::from(total);
            let got = counts[i] as f64;
            assert!(
                got > expected * 0.9 && got < expected * 1.1,
                "node {i} (weight {w}) owns {got}, expected ~{expected} (within 10%)"
            );
        }
    }

    #[test]
    fn removing_a_node_moves_only_its_own_keys() {
        // The defining property of rendezvous hashing vs modulo hashing.
        let mut rng = Rng::new(4);
        for _ in 0..100 {
            let n = 2 + rng.below(8);
            let m = members(&mut rng, n, 4);
            let victim = rng.below(n) as usize;
            let mut reduced = m.clone();
            let removed = reduced.remove(victim).0;

            for k in 0..2_000u64 {
                let key = key(rng.next_u64() ^ k);
                let before = owner_of(&key, &m);
                let after = owner_of(&key, &reduced);
                if before == removed {
                    assert_ne!(after, removed, "removed node must lose its keys");
                } else {
                    assert_eq!(
                        after, before,
                        "keys not owned by the removed node must not move"
                    );
                }
            }
        }
    }

    #[test]
    fn adding_a_node_only_pulls_keys_to_itself() {
        let mut rng = Rng::new(5);
        for _ in 0..100 {
            let n = 1 + rng.below(8);
            let before_members = members(&mut rng, n, 4);
            let newcomer = (node(1000), 1 + rng.below(4));
            let mut after_members = before_members.clone();
            after_members.push(newcomer.clone());

            for k in 0..2_000u64 {
                let key = key(rng.next_u64() ^ k);
                let before = owner_of(&key, &before_members);
                let after = owner_of(&key, &after_members);
                assert!(
                    after == before || after == newcomer.0,
                    "adding a node may only move keys TO it, never between existing nodes"
                );
            }
        }
    }

    #[test]
    fn replica_set_removal_is_minimal_and_order_preserving() {
        let mut rng = Rng::new(6);
        let r = 3usize;
        for _ in 0..100 {
            let n = 4 + rng.below(6);
            let m = members(&mut rng, n, 4);
            let victim = rng.below(n) as usize;
            let mut reduced = m.clone();
            let removed = reduced.remove(victim).0;

            for k in 0..1_500u64 {
                let key = key(rng.next_u64() ^ k);
                let before = owners(&key, &m, r);
                let after = owners(&key, &reduced, r);
                if before.contains(&removed) {
                    // Survivors keep their relative order; the freed slot is
                    // filled from the tail — nothing else reshuffles.
                    let survivors: Vec<NodeId> =
                        before.iter().filter(|o| **o != removed).cloned().collect();
                    assert!(
                        after.starts_with(&survivors),
                        "surviving replicas must stay, in order"
                    );
                    assert_eq!(after.len(), r.min((n - 1) as usize));
                } else {
                    assert_eq!(
                        after, before,
                        "replica sets not containing the victim are untouched"
                    );
                }
            }
        }
    }

    fn owner_of(key: &str, members: &[(NodeId, u32)]) -> NodeId {
        owners(key, members, 1).into_iter().next().unwrap()
    }
}
