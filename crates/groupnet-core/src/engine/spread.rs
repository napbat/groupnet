//! Deterministic load spreading: stable per-node and per-pair hashes and a
//! tiny splitmix64 stream, so nodes that start together pick different probe
//! targets, gossip targets, indirect probers and digest slices, while every
//! run of the same schedule stays bit-for-bit reproducible (no clock, no OS
//! randomness, no `std` hasher).

use crate::NodeId;
use crate::placement::{fnv1a, mix64};

/// splitmix64's stream increment (the golden-ratio constant).
const GOLDEN: u64 = 0x9e37_79b9_7f4a_7c15;

/// A stable, well-mixed hash of `node`'s id — the same on every platform and
/// every run.
pub(super) fn node_hash(node: &NodeId) -> u64 {
    mix64(fnv1a(&[node.as_str().as_bytes()]))
}

/// A stable, well-mixed hash of the ordered pair `(local, peer)`.
pub(super) fn pair_hash(local: &NodeId, peer: &NodeId) -> u64 {
    mix64(fnv1a(&[
        local.as_str().as_bytes(),
        peer.as_str().as_bytes(),
    ]))
}

/// `hash` reduced to an index below `len`, which must be nonzero.
pub(super) fn index(hash: u64, len: usize) -> usize {
    let len = u64::try_from(len).unwrap_or(u64::MAX);
    usize::try_from(hash % len).expect("an index below a usize length fits usize")
}

/// `hash` as a round-robin cursor's starting value: its top `usize::BITS`
/// bits, so the start is uniform modulo any candidate count.
pub(super) fn cursor(hash: u64) -> usize {
    usize::try_from(hash >> (u64::BITS - usize::BITS)).expect("usize::BITS bits fit usize")
}

/// A splitmix64 stream: one engine's deterministic source of random choices.
#[derive(Clone, Copy, Debug)]
pub(super) struct SplitMix(u64);

impl SplitMix {
    pub(super) const fn new(seed: u64) -> Self {
        Self(seed)
    }

    /// The next 64 pseudo-random bits.
    pub(super) fn draw(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(GOLDEN);
        mix64(self.0)
    }

    /// A pseudo-random index below `len`, which must be nonzero.
    pub(super) fn below(&mut self, len: usize) -> usize {
        index(self.draw(), len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashes_are_stable_and_distinguish_ids() {
        let (a, b) = (NodeId::new("a"), NodeId::new("b"));
        assert_eq!(node_hash(&a), node_hash(&NodeId::new("a")));
        assert_ne!(node_hash(&a), node_hash(&b));
        assert_ne!(pair_hash(&a, &b), pair_hash(&b, &a));
    }

    #[test]
    fn draws_stay_below_the_bound_and_cover_it() {
        let mut rng = SplitMix::new(node_hash(&NodeId::new("seed")));
        let mut seen = [false; 7];
        for _ in 0..1_000 {
            seen[rng.below(7)] = true;
        }
        assert!(seen.iter().all(|s| *s), "every index below 7 is drawn");
        assert_eq!(index(u64::MAX, 1), 0);
        assert!(cursor(u64::MAX) > 0);
    }
}
