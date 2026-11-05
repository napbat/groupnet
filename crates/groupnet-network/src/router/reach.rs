//! The reachable-destination index, kept in step with the route table one
//! destination at a time.
//!
//! A destination is reachable while it has a live candidate route: one
//! refreshed within `route_ttl` whose link still admits its next hop. An
//! advert changes one destination's candidates, so it re-examines that one
//! destination; lapsed routes leave through an expiry index ordered by the
//! instant a destination's last live candidate lapses. A change to the links
//! themselves (a link added or removed, its admitted neighbors changing) is
//! rare and rebuilds the index from the whole table. Between those, the index
//! equals what a full rebuild from the table would compute at the same
//! instant — except that a session admission lapsing inside a registry is
//! seen once its neighbor-change event arrives, which rebuilds.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use groupnet_core::NodeId;
use tokio::time::Instant;

#[cfg(test)]
use std::collections::HashMap;

use super::Table;

/// When a destination stops being reachable unless refreshed.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Lapse {
    /// At this instant.
    At(Instant),
    /// Never: the instant is past what the clock represents.
    Never,
}

/// Each reachable destination with the instant it stops being reachable.
#[derive(Default)]
pub(super) struct Reach {
    /// Reachable destinations, in id order, each with the instant its last
    /// live candidate lapses.
    until: BTreeMap<NodeId, Lapse>,
    /// The same lapse instants, earliest first.
    lapses: BTreeSet<(Instant, NodeId)>,
    /// Whether the set changed since it was last published.
    changed: bool,
}

impl Table {
    /// When `target`'s live routes lapse: the latest lapse among its
    /// candidates that are fresh at `now` and admitted by their link. `None`
    /// when it has none — exactly when [`Table::route`] finds none.
    fn live_until(&mut self, target: &NodeId, ttl: Duration, now: Instant) -> Option<Lapse> {
        #[cfg(test)]
        {
            self.reach_visits += 1;
            self.reach_candidates += self.candidates.get(target).map_or(0, HashMap::len) as u64;
        }
        let mut latest: Option<Lapse> = None;
        for candidate in self.candidates.get(target)?.values() {
            let lapse = candidate
                .updated
                .checked_add(ttl)
                .map_or(Lapse::Never, Lapse::At);
            let fresh = match lapse {
                Lapse::At(at) => now < at,
                Lapse::Never => true,
            };
            let admitted = self
                .links
                .get(&candidate.route.transport)
                .is_some_and(|link| link.admits(&candidate.route.next_hop, candidate.session));
            if fresh && admitted {
                latest = latest.max(Some(lapse));
            }
        }
        latest
    }

    /// Updates the index after an advert refreshed one of `target`'s
    /// candidates at `now`. That candidate is the freshest `target` has, so
    /// when its link `admitted` it, `target` is reachable until its lapse —
    /// no other candidate need be examined, and a destination that was
    /// already reachable only moves its lapse (`O(log N)`, no change to the
    /// set). Otherwise the destination is re-examined whole.
    pub(super) fn reach_advert(
        &mut self,
        target: &NodeId,
        admitted: bool,
        ttl: Duration,
        now: Instant,
    ) {
        if !admitted {
            self.reach_refresh(target, ttl, now);
            return;
        }
        let lapse = now.checked_add(ttl).map_or(Lapse::Never, Lapse::At);
        let old = self.reach.until.get(target).copied();
        let until = old.map_or(lapse, |old| old.max(lapse));
        if old == Some(until) {
            return;
        }
        if let Some(Lapse::At(at)) = old {
            self.reach.lapses.remove(&(at, target.clone()));
        }
        if let Lapse::At(at) = until {
            self.reach.lapses.insert((at, target.clone()));
        }
        if let Some(slot) = self.reach.until.get_mut(target) {
            *slot = until;
        } else {
            self.reach.until.insert(target.clone(), until);
            self.reach.changed = true;
        }
    }

    /// Re-examines one destination after its candidates changed.
    pub(super) fn reach_refresh(&mut self, target: &NodeId, ttl: Duration, now: Instant) {
        let live = self.live_until(target, ttl, now);
        let old = match live {
            Some(until) => self.reach.until.insert(target.clone(), until),
            None => self.reach.until.remove(target),
        };
        if let Some(Lapse::At(at)) = old {
            self.reach.lapses.remove(&(at, target.clone()));
        }
        if let Some(Lapse::At(at)) = live {
            self.reach.lapses.insert((at, target.clone()));
        }
        self.reach.changed |= old.is_some() != live.is_some();
    }

    /// Drops every destination whose last live route lapsed by `now`.
    pub(super) fn reach_expire(&mut self, now: Instant) {
        while let Some((at, _)) = self.reach.lapses.first() {
            if *at > now {
                break;
            }
            if let Some((_, node)) = self.reach.lapses.pop_first() {
                self.reach.until.remove(&node);
                self.reach.changed = true;
            }
        }
    }

    /// Rebuilds the index from the whole table, after a change to the links
    /// themselves or a prune of the table.
    pub(super) fn reach_rebuild(&mut self, ttl: Duration, now: Instant) {
        let was = std::mem::take(&mut self.reach);
        let targets: Vec<NodeId> = self.candidates.keys().cloned().collect();
        for target in targets {
            self.reach_refresh(&target, ttl, now);
        }
        self.reach.changed = was.changed || !self.reach.until.keys().eq(was.until.keys());
    }

    /// Whether the set changed since the last call — so a caller publishes
    /// it only then, whichever path changed it.
    pub(super) fn reach_take_changed(&mut self) -> bool {
        let changed = std::mem::take(&mut self.reach.changed);
        #[cfg(test)]
        {
            self.reach_publishes += u64::from(changed);
        }
        changed
    }

    /// The destinations the index would still hold once lapses up to `now`
    /// are applied, in id order — what the next advert publishes.
    #[cfg(test)]
    pub(super) fn reachable_at(&self, now: Instant) -> impl Iterator<Item = &NodeId> {
        self.reach
            .until
            .iter()
            .filter(move |(_, until)| match until {
                Lapse::At(at) => *at > now,
                Lapse::Never => true,
            })
            .map(|(node, _)| node)
    }

    /// The reachable destinations, in id order.
    pub(super) fn reachable(&self) -> impl Iterator<Item = &NodeId> {
        self.reach.until.keys()
    }
}
