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
use std::ops::Bound::{Excluded, Unbounded};

use crate::membership::{Member, Status};
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
    /// a delta digest lists the range after its peer's cursor, and a full
    /// digest leads with its top.
    pub(super) changed: BTreeMap<u64, NodeId>,
    /// Stamped members by `(liveness_since, changed_at)`: a full digest's
    /// liveness news is the top of it, newest first.
    pub(super) news: BTreeSet<(Time, u64, NodeId)>,
    /// Members forgotten, by the change stamp they were forgotten at — the
    /// latest [`FORGOTTEN_KEPT`] — for readers that mirror the roster.
    pub(super) forgotten: BTreeMap<u64, NodeId>,
    /// The stamp of the latest record dropped from `forgotten` (0 = none):
    /// a reader at or after it has seen every one it needs.
    pub(super) forgotten_floor: u64,
}

/// How many forgotten members [`Index::forgotten`] keeps for readers.
const FORGOTTEN_KEPT: usize = 4_096;

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
    /// A member entering or leaving the live set also patches the cached
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
                self.live_set_changed(node, true);
                Some(score)
            }
            (Some(score), false) => {
                self.index.live.remove(&(score, Reverse(node.clone())));
                self.live_set_changed(node, false);
                None
            }
            (None, false) => None,
        };
        if let Some(m) = self.members.get_mut(node) {
            m.filed.due = due;
            m.filed.score = score;
        }
    }

    /// Moves `node` to change stamp `stamp` in the change index and the
    /// liveness news, under its current `liveness_since`.
    pub(super) fn restamp(&mut self, node: &NodeId, stamp: u64) {
        let Some(m) = self.members.get_mut(node) else {
            return;
        };
        if m.changed_at > 0 {
            self.index.changed.remove(&m.changed_at);
            self.index
                .news
                .remove(&(m.filed.liveness, m.changed_at, node.clone()));
        }
        m.changed_at = stamp;
        m.filed.liveness = m.liveness_since;
        self.index.changed.insert(stamp, node.clone());
        self.index
            .news
            .insert((m.liveness_since, stamp, node.clone()));
    }

    /// Removes `node` from the member map and from every index, and records
    /// it as forgotten at a fresh change stamp.
    pub(super) fn forget(&mut self, node: &NodeId) -> Option<Member> {
        let m = self.members.remove(node)?;
        if m.filed.due != Time::MAX {
            self.index.due.remove(&(m.filed.due, node.clone()));
        }
        if let Some(score) = m.filed.score {
            self.index.live.remove(&(score, Reverse(node.clone())));
            self.live_set_changed(node, false);
        }
        if m.changed_at > 0 {
            self.index.changed.remove(&m.changed_at);
            self.index
                .news
                .remove(&(m.filed.liveness, m.changed_at, node.clone()));
        }
        self.digest_peers.remove(node);
        self.change_clock += 1;
        self.index.forgotten.insert(self.change_clock, node.clone());
        if self.index.forgotten.len() > FORGOTTEN_KEPT
            && let Some((stamp, _)) = self.index.forgotten.pop_first()
        {
            self.index.forgotten_floor = stamp;
        }
        Some(m)
    }

    /// The change clock: it advances whenever a member's record changes —
    /// liveness, incarnation, entries or version high-water — and whenever a
    /// member is forgotten. A reader that mirrors the roster keeps the value
    /// it last read and asks [`members_changed_since`](Self::members_changed_since)
    /// and [`members_forgotten_since`](Self::members_forgotten_since) for
    /// what moved after it.
    #[must_use]
    pub fn change_clock(&self) -> u64 {
        self.change_clock
    }

    /// The known members whose record changed after change-clock `since`,
    /// in the order they last changed: `O(log N)` plus one step per member.
    /// Each is listed once however often it changed.
    pub fn members_changed_since(&self, since: u64) -> impl Iterator<Item = &NodeId> {
        self.index
            .changed
            .range((Excluded(since), Unbounded))
            .map(|(_, node)| node)
    }

    /// The members forgotten (their `Dead` tombstone reaped) after
    /// change-clock `since` — a member learned again since then is listed
    /// too, and is known again. `None` when the record of forgotten members
    /// no longer reaches back to `since`: it keeps the latest
    /// 4,096, and a reader that far behind re-reads the whole roster.
    #[must_use]
    pub fn members_forgotten_since(&self, since: u64) -> Option<impl Iterator<Item = &NodeId>> {
        (since >= self.index.forgotten_floor).then(|| {
            self.index
                .forgotten
                .range((Excluded(since), Unbounded))
                .map(|(_, node)| node)
        })
    }

    /// `node` entered (`live`) or left the live set: patch it into or out
    /// of the cached probe ring and dissemination targets, which stay
    /// sorted, instead of rebuilding them.
    fn live_set_changed(&mut self, node: &NodeId, live: bool) {
        let candidate = live && *node != self.local;
        let target =
            candidate || self.seeds.contains(node) || self.bootstrap_contacts.contains(node);
        if let Some(ring) = &mut self.ring {
            patch_sorted(ring, node, candidate);
        }
        if let Some(targets) = &mut self.targets {
            patch_sorted(targets, node, target);
        }
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
    /// on first use and then patched as members enter and leave the live
    /// set.
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
                assert!(
                    m.filed.liveness == m.liveness_since
                        && self.index.news.contains(&(
                            m.liveness_since,
                            m.changed_at,
                            node.clone()
                        )),
                    "{node} is missing from the liveness news"
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
            self.index.news.len(),
            stamped,
            "the liveness news holds a stale member"
        );
        assert_eq!(
            self.compute_coordinator(),
            placement::owner_among(self.group.as_str(), self.members()),
            "the indexed coordinator is not the placement owner"
        );
    }
}

/// Inserts `node` into, or removes it from, the sorted `list` so that it is
/// there exactly when `present`.
fn patch_sorted(list: &mut Vec<NodeId>, node: &NodeId, present: bool) {
    match list.binary_search(node) {
        Ok(at) if !present => {
            list.remove(at);
        }
        Err(at) if present => list.insert(at, node.clone()),
        _ => {}
    }
}
