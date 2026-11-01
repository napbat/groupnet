//! Advert freshness: a neighbor's route adverts are accepted only in the
//! order its router sent them.
//!
//! Every advert carries its sender's routed identity: the sender router's
//! random 8-byte boot nonce followed by a big-endian sequence drawn from the
//! counter its data packets use, so one boot's adverts strictly increase.
//! For each neighbor on each link the receiver keeps the newest identity it
//! accepted and accepts an advert only when it is newer: a higher sequence
//! under the same nonce, or a nonce it has not seen there (the neighbor
//! restarted). The last [`RETIRED`] nonces a neighbor left behind are
//! refused, so an advert captured before a restart does not return after
//! it. At most `capacity` neighbors are tracked; the least recently active
//! is forgotten first.

use std::collections::HashMap;

use groupnet_core::NodeId;

/// Earlier boot nonces of a neighbor that stay refused.
pub(super) const RETIRED: usize = 4;

#[derive(Debug)]
struct Neighbor {
    link: usize,
    nonce: [u8; 8],
    newest: u64,
    /// Nonces of the neighbor's earlier boots, oldest first.
    retired: Vec<[u8; 8]>,
    /// Activity stamp for least-recently-active eviction.
    touched: u64,
}

impl Neighbor {
    fn accept(&mut self, nonce: [u8; 8], sequence: u64) -> bool {
        if nonce == self.nonce {
            if sequence <= self.newest {
                return false;
            }
            self.newest = sequence;
            return true;
        }
        if self.retired.contains(&nonce) {
            return false;
        }
        if self.retired.len() == RETIRED {
            self.retired.remove(0);
        }
        self.retired.push(self.nonce);
        self.nonce = nonce;
        self.newest = sequence;
        true
    }
}

#[derive(Debug)]
pub(super) struct AdvertFreshness {
    neighbors: HashMap<NodeId, Vec<Neighbor>>,
    tracked: usize,
    capacity: usize,
    clock: u64,
}

impl AdvertFreshness {
    /// Tracks at most `capacity` (link, neighbor) pairs; `capacity` is nonzero.
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            neighbors: HashMap::new(),
            tracked: 0,
            capacity,
            clock: 0,
        }
    }

    /// Records an advert from `from` on `link` with routed identity `id`,
    /// returning `false` when it is not newer than the last one accepted.
    pub(super) fn accept(&mut self, link: usize, from: &NodeId, id: [u8; 16]) -> bool {
        let (nonce, sequence) = id.split_at(8);
        let nonce: [u8; 8] = nonce.try_into().expect("8-byte nonce");
        let sequence = u64::from_be_bytes(sequence.try_into().expect("8-byte sequence"));
        self.clock += 1;
        let touched = self.clock;
        if let Some(neighbor) = self
            .neighbors
            .get_mut(from)
            .and_then(|links| links.iter_mut().find(|neighbor| neighbor.link == link))
        {
            neighbor.touched = touched;
            return neighbor.accept(nonce, sequence);
        }
        if self.tracked >= self.capacity {
            self.evict();
        }
        self.neighbors
            .entry(from.clone())
            .or_default()
            .push(Neighbor {
                link,
                nonce,
                newest: sequence,
                retired: Vec::new(),
                touched,
            });
        self.tracked += 1;
        true
    }

    /// Forgets the least recently active pair; runs only when a new pair
    /// arrives at capacity.
    fn evict(&mut self) {
        let Some((node, link)) = self
            .neighbors
            .iter()
            .flat_map(|(node, links)| links.iter().map(move |neighbor| (node, neighbor)))
            .min_by_key(|(_, neighbor)| neighbor.touched)
            .map(|(node, neighbor)| (node.clone(), neighbor.link))
        else {
            return;
        };
        if let Some(links) = self.neighbors.get_mut(&node) {
            links.retain(|neighbor| neighbor.link != link);
            if links.is_empty() {
                self.neighbors.remove(&node);
            }
        }
        self.tracked -= 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(nonce: u8, sequence: u64) -> [u8; 16] {
        let mut id = [nonce; 16];
        id[8..].copy_from_slice(&sequence.to_be_bytes());
        id
    }

    #[test]
    fn only_newer_adverts_of_one_boot_are_accepted() {
        let peer = NodeId::new("peer");
        let mut adverts = AdvertFreshness::new(8);
        assert!(adverts.accept(0, &peer, id(1, 5)));
        assert!(!adverts.accept(0, &peer, id(1, 5)), "a replay");
        assert!(!adverts.accept(0, &peer, id(1, 4)), "an older advert");
        assert!(adverts.accept(0, &peer, id(1, 9)), "gaps are fine");
        // Each link keeps its own order: the same advert arrives on both.
        assert!(adverts.accept(1, &peer, id(1, 9)));
        assert!(adverts.accept(0, &NodeId::new("other"), id(1, 9)));
    }

    #[test]
    fn a_restart_starts_a_new_epoch_and_retires_the_old_one() {
        let peer = NodeId::new("peer");
        let mut adverts = AdvertFreshness::new(8);
        assert!(adverts.accept(0, &peer, id(1, 100)));
        assert!(adverts.accept(0, &peer, id(2, 0)), "a restarted neighbor");
        assert!(
            !adverts.accept(0, &peer, id(1, 101)),
            "the old boot stays retired"
        );
        for nonce in 3..3 + u8::try_from(RETIRED).expect("few retired boots") {
            assert!(adverts.accept(0, &peer, id(nonce, 0)));
        }
        // Only the last RETIRED boots are remembered.
        assert!(!adverts.accept(0, &peer, id(3, 1)));
        assert!(adverts.accept(0, &peer, id(1, 102)));
    }

    #[test]
    fn the_least_recently_active_pair_is_forgotten_at_capacity() {
        let (a, b, c) = (NodeId::new("a"), NodeId::new("b"), NodeId::new("c"));
        let mut adverts = AdvertFreshness::new(2);
        assert!(adverts.accept(0, &a, id(1, 1)));
        assert!(adverts.accept(0, &b, id(1, 1)));
        assert!(adverts.accept(0, &a, id(1, 2)));
        assert!(adverts.accept(0, &c, id(1, 1)), "evicts b");
        assert_eq!(adverts.tracked, 2);
        assert!(!adverts.accept(0, &a, id(1, 2)), "a is still tracked");
        assert!(adverts.accept(0, &b, id(1, 1)), "b was forgotten");
    }
}
