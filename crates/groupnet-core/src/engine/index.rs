//! The engine's ordered index over its members: who is due when, who is
//! live by coordinator score, and who changed when. Every change to a
//! member's liveness or entries re-files it here in `O(log N)`, so a tick
//! reaps, and a round lists, only the members that are due or changed — never
//! a walk of every member and entry.
//!
//! The index is deterministic like the rest of the engine: ordered
//! structures only, keyed by logical time, score and node id.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};

use crate::membership::{Filed, Member, Status};
use crate::{NodeId, Time, placement};

use super::state::GroupEngine;

#[cfg(test)]
mod tests;

/// The engine's ordered views of its member map, each kept by
/// [`GroupEngine::reindex`] and [`GroupEngine::forget`].
#[derive(Debug, Default)]
pub(super) struct Index {
    /// Every member with a pending deadline, under its earliest (see
    /// [`Filed::due`]): what the next tick must look at, first first.
    pub(super) due: BTreeSet<(Time, NodeId)>,
    /// The live (not `Dead`) members by coordinator score; the coordinator is
    /// the last (highest score, smaller id on a tie).
    pub(super) live: BTreeSet<(u64, Reverse<NodeId>)>,
    /// Members by change stamp ([`Member::changed_at`], unique per member):
    /// a delta digest lists the range after its peer's cursor.
    pub(super) changed: BTreeMap<u64, NodeId>,
}

impl GroupEngine {
    /// Logical milliseconds after which a `Dead` tombstone or an entry
    /// tombstone is reaped: twice `dead_timeout`.
    pub(super) fn reap_after(&self) -> u64 {
        self.config.dead_timeout_ms.saturating_mul(2)
    }

    /// When `node`'s liveness next needs a tick: a suspect's window, a
    /// `Dead` tombstone's reap. Never for the local node, which is neither
    /// declared dead nor reaped by itself.
    pub(super) fn liveness_due(&self, node: &NodeId, m: &Member) -> Time {
        if *node == self.local {
            return Time::MAX;
        }
        match m.status {
            Status::Alive => Time::MAX,
            Status::Suspect => m
                .suspect_since
                .saturating_add(self.config.suspect_timeout_ms),
            Status::Dead => m.dead_since.saturating_add(self.reap_after()),
        }
    }

    /// Re-files `node` after its liveness or entries changed: under its
    /// earliest deadline, and under its coordinator score while it is live.
    /// A member entering or leaving the live set also drops the cached
    /// probe ring and dissemination targets. [`stamp`](Self::stamp) calls
    /// this, and so does every path that changes a member without stamping
    /// it (a reap).
    pub(super) fn reindex(&mut self, node: &NodeId) {
        let Some(m) = self.members.get(node) else {
            return;
        };
        self.touch(1);
        let due = self.liveness_due(node, m).min(m.next_entry_due());
        let live = m.status != Status::Dead;
        let was = m.filed;
        if due != was.due {
            if was.due != Time::MAX {
                self.index.due.remove(&(was.due, node.clone()));
            }
            if due != Time::MAX {
                self.index.due.insert((due, node.clone()));
            }
        }
        let score = match (was.score, live) {
            (Some(score), true) => Some(score),
            (None, true) => {
                let score = placement::owner_score(self.group.as_str(), node);
                self.index.live.insert((score, Reverse(node.clone())));
                self.live_set_changed();
                Some(score)
            }
            (Some(score), false) => {
                self.index.live.remove(&(score, Reverse(node.clone())));
                self.live_set_changed();
                None
            }
            (None, false) => None,
        };
        if let Some(m) = self.members.get_mut(node) {
            m.filed = Filed { due, score };
        }
    }

    /// Removes `node` from the member map and from every index.
    pub(super) fn forget(&mut self, node: &NodeId) -> Option<Member> {
        let m = self.members.remove(node)?;
        if m.filed.due != Time::MAX {
            self.index.due.remove(&(m.filed.due, node.clone()));
        }
        if let Some(score) = m.filed.score {
            self.index.live.remove(&(score, Reverse(node.clone())));
            self.live_set_changed();
        }
        if m.changed_at > 0 {
            self.index.changed.remove(&m.changed_at);
        }
        self.digest_peers.remove(node);
        Some(m)
    }

    /// The live set changed: the probe ring and the dissemination targets
    /// are rebuilt on next use.
    fn live_set_changed(&mut self) {
        self.ring = None;
        self.targets = None;
    }

    /// The members with a deadline at or before `now`, in id order.
    pub(super) fn due_members(&self, now: Time) -> Vec<NodeId> {
        let mut due: Vec<NodeId> = self
            .index
            .due
            .iter()
            .take_while(|(at, _)| *at <= now)
            .map(|(_, node)| node.clone())
            .collect();
        self.touch(due.len());
        due.sort_unstable();
        due
    }

    /// The earliest suspect window, `Dead` reap, entry TTL and entry
    /// tombstone reap of any member.
    pub(super) fn slow_deadline(&self) -> Time {
        self.index.due.first().map_or(Time::MAX, |(at, _)| *at)
    }

    /// The live members other than us, in id order — the probe ring, built
    /// on first use after the live set changed.
    pub(super) fn probe_ring(&mut self) -> &[NodeId] {
        if self.ring.is_none() {
            let ring: Vec<NodeId> = self.probe_candidates().cloned().collect();
            self.touch(ring.len());
            self.ring = Some(ring);
        }
        let ring = self.ring.as_deref().unwrap_or_default();
        debug_assert!(
            ring.iter().eq(self.probe_candidates()),
            "the cached probe ring went stale"
        );
        ring
    }

    /// Checks every index against a full walk of the members and their
    /// entries — what each index replaces — and the coordinator against the
    /// placement owner of the live set. Debug builds run it on every
    /// [`next_deadline`](Self::next_deadline).
    #[cfg(any(test, debug_assertions))]
    pub(super) fn assert_index(&self) {
        let reap_after = self.reap_after();
        let (mut due, mut live, mut stamped) = (0, 0, 0);
        for (node, m) in &self.members {
            let mut listed = 0;
            for (key, e) in &m.entries {
                let at = e.reap_due(reap_after);
                if at != Time::MAX {
                    listed += 1;
                    assert!(
                        m.entry_dues.get(&at).is_some_and(|keys| keys.contains(key)),
                        "{node}'s entry {key} is not listed under its deadline {at:?}"
                    );
                }
            }
            assert_eq!(
                m.entry_dues.values().map(BTreeSet::len).sum::<usize>(),
                listed,
                "{node} lists an entry deadline it no longer has"
            );
            let at = self.liveness_due(node, m).min(m.next_entry_due());
            assert_eq!(m.filed.due, at, "{node} is filed under a stale deadline");
            if at != Time::MAX {
                due += 1;
                assert!(
                    self.index.due.contains(&(at, node.clone())),
                    "{node} is missing from the deadline index"
                );
            }
            let score = (m.status != Status::Dead)
                .then(|| placement::owner_score(self.group.as_str(), node));
            assert_eq!(m.filed.score, score, "{node} has a stale live filing");
            if let Some(score) = score {
                live += 1;
                assert!(
                    self.index.live.contains(&(score, Reverse(node.clone()))),
                    "{node} is missing from the live index"
                );
            }
            if m.changed_at > 0 {
                stamped += 1;
                assert_eq!(
                    self.index.changed.get(&m.changed_at),
                    Some(node),
                    "{node} is missing from the change index"
                );
            }
        }
        assert_eq!(
            self.index.due.len(),
            due,
            "the deadline index holds a stale member"
        );
        assert_eq!(
            self.index.live.len(),
            live,
            "the live index holds a stale member"
        );
        assert_eq!(
            self.index.changed.len(),
            stamped,
            "the change index holds a stale member"
        );
        assert_eq!(
            self.compute_coordinator(),
            placement::owner_among(self.group.as_str(), self.members()),
            "the indexed coordinator is not the placement owner"
        );
    }
}
