//! Advert freshness: a neighbor's route adverts are accepted only in the
//! order its router sent them.
//!
//! Every advert carries its sender's [`AdvertId`]: the sender router's boot
//! epoch (wall-clock nanoseconds when it started), its random boot nonce, and
//! a sequence from a counter only adverts use. Boots are ordered by epoch,
//! then nonce, so a restarted neighbor's adverts outrank everything its
//! earlier boots sent, and one boot's adverts strictly increase. For each
//! neighbor on each link the receiver keeps the newest identity it accepted
//! and accepts an advert only when it is newer. An advert from an earlier
//! boot is refused however old it is, so a replayed one can neither revive a
//! route nor displace the live boot. A refused advert leaves the pair's
//! activity untouched. At most `capacity` pairs are tracked; the least
//! recently accepted is forgotten first.

use std::collections::HashMap;

use groupnet_core::NodeId;

use crate::wire::AdvertId;

#[derive(Debug)]
struct Neighbor {
    link: usize,
    newest: AdvertId,
    /// Stamp of the last accepted advert, for least-recently-active eviction.
    touched: u64,
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

    /// Records an advert from `from` on `link` with identity `id`, returning
    /// `false` when it is not newer than the last one accepted there.
    pub(super) fn accept(&mut self, link: usize, from: &NodeId, id: AdvertId) -> bool {
        self.clock += 1;
        let touched = self.clock;
        if let Some(neighbor) = self
            .neighbors
            .get_mut(from)
            .and_then(|links| links.iter_mut().find(|neighbor| neighbor.link == link))
        {
            if id <= neighbor.newest {
                return false;
            }
            neighbor.newest = id;
            neighbor.touched = touched;
            return true;
        }
        if self.tracked >= self.capacity {
            self.evict();
        }
        self.neighbors
            .entry(from.clone())
            .or_default()
            .push(Neighbor {
                link,
                newest: id,
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

    fn id(epoch: u64, nonce: u8, sequence: u64) -> AdvertId {
        AdvertId {
            epoch,
            nonce: [nonce; 8],
            sequence,
        }
    }

    #[test]
    fn only_newer_adverts_of_one_boot_are_accepted() {
        let peer = NodeId::new("peer");
        let mut adverts = AdvertFreshness::new(8);
        assert!(adverts.accept(0, &peer, id(1, 1, 5)));
        assert!(!adverts.accept(0, &peer, id(1, 1, 5)), "a replay");
        assert!(!adverts.accept(0, &peer, id(1, 1, 4)), "an older advert");
        assert!(adverts.accept(0, &peer, id(1, 1, 9)), "gaps are fine");
        // Each link keeps its own order: the same advert arrives on both.
        assert!(adverts.accept(1, &peer, id(1, 1, 9)));
        assert!(adverts.accept(0, &NodeId::new("other"), id(1, 1, 9)));
    }

    /// The re-review's lockout: a replayed advert from an earlier boot the
    /// receiver never tracked must not displace the live boot.
    #[test]
    fn an_older_untracked_boot_never_displaces_the_live_one() {
        let peer = NodeId::new("peer");
        let mut adverts = AdvertFreshness::new(8);
        assert!(adverts.accept(0, &peer, id(20, 7, 1)));
        assert!(
            !adverts.accept(0, &peer, id(10, 3, 50)),
            "a replayed older boot"
        );
        assert!(
            adverts.accept(0, &peer, id(20, 7, 2)),
            "the live boot still counts"
        );
    }

    #[test]
    fn a_restart_outranks_every_earlier_boot() {
        let peer = NodeId::new("peer");
        let mut adverts = AdvertFreshness::new(8);
        assert!(adverts.accept(0, &peer, id(10, 1, 100)));
        assert!(
            adverts.accept(0, &peer, id(20, 2, 0)),
            "a restarted neighbor"
        );
        assert!(!adverts.accept(0, &peer, id(10, 1, 101)), "the old boot");
        assert!(!adverts.accept(0, &peer, id(5, 9, 0)), "any older boot");
        // Boots that share an epoch are ordered by nonce.
        assert!(adverts.accept(0, &peer, id(20, 3, 0)));
        assert!(!adverts.accept(0, &peer, id(20, 2, 1)));
    }

    #[test]
    fn refused_adverts_do_not_keep_a_pair_alive() {
        let (a, b, c) = (NodeId::new("a"), NodeId::new("b"), NodeId::new("c"));
        let mut adverts = AdvertFreshness::new(2);
        assert!(adverts.accept(0, &a, id(1, 1, 1)));
        assert!(adverts.accept(0, &b, id(1, 1, 1)));
        assert!(!adverts.accept(0, &a, id(1, 1, 1)), "a replay");
        assert!(
            adverts.accept(0, &c, id(1, 1, 1)),
            "evicts a, untouched by its replay"
        );
        assert!(adverts.accept(0, &a, id(1, 1, 1)), "a was forgotten");
    }

    #[test]
    fn the_least_recently_active_pair_is_forgotten_at_capacity() {
        let (a, b, c) = (NodeId::new("a"), NodeId::new("b"), NodeId::new("c"));
        let mut adverts = AdvertFreshness::new(2);
        assert!(adverts.accept(0, &a, id(1, 1, 1)));
        assert!(adverts.accept(0, &b, id(1, 1, 1)));
        assert!(adverts.accept(0, &a, id(1, 1, 2)));
        assert!(adverts.accept(0, &c, id(1, 1, 1)), "evicts b");
        assert_eq!(adverts.tracked, 2);
        assert!(!adverts.accept(0, &a, id(1, 1, 2)), "a is still tracked");
        assert!(adverts.accept(0, &b, id(1, 1, 1)), "b was forgotten");
    }
}
