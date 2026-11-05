//! Failure detection (probes, suspicion) and reaping.

use std::collections::BTreeMap;

use crate::membership::Status;
use crate::{NodeId, Time, wire};

use super::effect::Effect;
use super::state::{GroupEngine, Pending, ProbePhase};

impl GroupEngine {
    pub(super) fn probe(&mut self, now: Time) -> Vec<Effect> {
        if self.pending.is_some() {
            return Vec::new(); // one probe outstanding at a time
        }
        let cursor = self.probe_cursor;
        let ring = self.probe_ring();
        if ring.is_empty() {
            return Vec::new();
        }
        let target = ring[cursor % ring.len()].clone();
        self.probe_cursor = self.probe_cursor.wrapping_add(1);
        self.pending = Some(Pending {
            target: target.clone(),
            deadline: now.saturating_add(self.config.probe_timeout_ms),
            phase: ProbePhase::Direct,
        });
        vec![self.send_probe(target, wire::Kind::Ping, None)]
    }

    /// A direct probe missed: enlist indirect probers instead of suspecting
    /// outright. This is what prevents a single dropped packet or one-way link
    /// from falsely killing a healthy node.
    ///
    /// The probers are drawn at random (from this node's deterministic
    /// stream) among the live members other than the target, so indirect
    /// probe load spreads over the whole group instead of landing on the
    /// lowest ids every node would otherwise pick.
    pub(super) fn escalate_indirect(&mut self, target: &NodeId, now: Time) -> Vec<Effect> {
        let indirect = self.config.indirect_probes.get();
        let mut rng = self.rng;
        let candidates = self.probe_ring();
        // The eligible members are the probe ring without the target. A partial
        // Fisher–Yates shuffle of them, recording only the slots it swapped,
        // so the first `k` draws are a uniform random `k`-subset at `O(k)`.
        let skip = candidates.binary_search(target).ok();
        let eligible = candidates.len() - usize::from(skip.is_some());
        let at = |slot: usize| match skip {
            Some(pos) if slot >= pos => &candidates[slot + 1],
            _ => &candidates[slot],
        };
        let k = indirect.min(eligible);
        let mut swapped: BTreeMap<usize, usize> = BTreeMap::new();
        let mut probers: Vec<NodeId> = Vec::with_capacity(k);
        for i in 0..k {
            let j = i + rng.below(eligible - i);
            let here = swapped.get(&i).copied().unwrap_or(i);
            let there = swapped.get(&j).copied().unwrap_or(j);
            swapped.insert(j, here);
            probers.push(at(there).clone());
        }
        self.rng = rng;
        if probers.is_empty() {
            // No one to ask (tiny cluster) — fall back to direct suspicion.
            self.pending = None;
            return self.suspect(target, now);
        }
        self.pending = Some(Pending {
            target: target.clone(),
            // Two round trips — to the probers, and theirs to the target —
            // so two probe timeouts.
            deadline: now.saturating_add(self.config.probe_timeout_ms.saturating_mul(2)),
            phase: ProbePhase::Indirect,
        });
        probers
            .into_iter()
            .map(|p| self.send_probe(p, wire::Kind::PingReq, Some(target.clone())))
            .collect()
    }

    pub(super) fn clear_pending_if(&mut self, target: &NodeId) {
        if self.pending.as_ref().is_some_and(|p| p.target == *target) {
            self.pending = None;
        }
    }

    /// How long a suspicion this node only heard through gossip stands
    /// before it turns into a death: the refutation window plus
    /// [`liveness_hot_ms`](Self::liveness_hot_ms), the time liveness news
    /// takes to cross the group. The suspecter's own window is the bare
    /// `suspect_timeout`; if the suspicion was real its `Dead` verdict reaches
    /// this node through the same gossip well before this runs out, and if
    /// it was false the refutation does — the refutation starts later than
    /// the suspicion and travels by other paths, so a node that heard the
    /// suspicion early must not time it out on the suspecter's clock. Once
    /// that clock has run out the node asks the member itself (see
    /// [`reap_suspects`](Self::reap_suspects)), so the doubt clears within a
    /// round trip of it rather than when gossip happens to bring the
    /// refutation.
    pub(super) fn heard_suspicion_ms(&self) -> u64 {
        self.config
            .suspect_timeout_ms
            .saturating_add(self.liveness_hot_ms())
    }

    /// Marks `target` `Suspect` from this node's own probe and tells it so
    /// directly (a one-line digest), repeating the accusation every
    /// `probe_timeout` until the target refutes or the window runs out. A
    /// live target answers with its current record, refuting first if it
    /// must, so this node stops doubting it within a round trip — and one
    /// lost frame only costs one more. A target this node already suspects
    /// from gossip gets this node's own, shorter window, and the
    /// accusations from now.
    pub(super) fn suspect(&mut self, target: &NodeId, now: Time) -> Vec<Effect> {
        let window = self.config.suspect_timeout_ms;
        let every = self.config.probe_timeout_ms;
        let became_suspect = match self.members.get_mut(target) {
            Some(m) if m.status == Status::Alive => {
                m.adopt_status(Status::Suspect, now);
                m.suspect_here(now, window, every);
                true
            }
            Some(m) if m.status == Status::Suspect && !m.suspected_here => {
                // Confirmed by our own probe: our window, and our accusation.
                let due = m.suspect_due.min(now.saturating_add(window));
                m.suspect_here(now, window, every);
                m.suspect_due = due;
                self.reindex(target);
                return self
                    .liveness_notice(target.clone(), target, now)
                    .into_iter()
                    .collect();
            }
            _ => false,
        };
        if !became_suspect {
            return Vec::new();
        }
        self.stamp(target);
        let mut effects = vec![Effect::MembershipChanged];
        effects.extend(self.recompute_coordinator());
        effects.extend(self.liveness_notice(target.clone(), target, now));
        self.nudge_anti_entropy();
        effects
    }

    /// Declares suspects past their window `Dead`, telling each so directly
    /// like [`suspect`](Self::suspect) does: a live one answers with its
    /// current record, including a suspect this node only heard of through
    /// gossip at an incarnation it has since refuted. Accuses every other
    /// suspect whose next accusation is due — one this node's own probe
    /// found silent, every `probe_timeout` from the first; one it heard of
    /// through gossip, from when its own window would have run out.
    /// Examines only the members whose deadline passed.
    pub(super) fn reap_suspects(&mut self, now: Time) -> Vec<Effect> {
        let due = self.due_members(now);
        let suspect = |node: &NodeId| {
            self.members
                .get(node)
                .filter(|m| *node != self.local && m.status == Status::Suspect)
        };
        let dead: Vec<NodeId> = due
            .iter()
            .filter(|node| suspect(node).is_some_and(|m| now >= m.suspect_due))
            .cloned()
            .collect();
        let accused: Vec<NodeId> = due
            .iter()
            .filter(|node| suspect(node).is_some_and(|m| now < m.suspect_due && now >= m.accuse_at))
            .cloned()
            .collect();
        let mut effects = Vec::new();
        let every = self.config.probe_timeout_ms;
        for node in accused {
            if let Some(m) = self.members.get_mut(&node) {
                m.accuse_at = now.saturating_add(every);
            }
            self.reindex(&node);
            effects.extend(self.liveness_notice(node.clone(), &node, now));
        }
        if dead.is_empty() {
            return effects;
        }
        for node in &dead {
            if let Some(m) = self.members.get_mut(node) {
                m.adopt_status(Status::Dead, now);
                m.dead_since = now;
            }
            self.stamp(node);
        }
        effects.push(Effect::MembershipChanged);
        effects.extend(self.recompute_coordinator());
        for node in dead {
            effects.extend(self.liveness_notice(node.clone(), &node, now));
        }
        self.nudge_anti_entropy();
        effects
    }

    /// Removes `Dead` tombstones that have aged past `2×dead_timeout`. By then
    /// they have stopped being gossiped (see `should_gossip`), so no peer
    /// re-teaches them and the removal converges.
    ///
    /// A reap is an observable membership change — the member stops being
    /// *known*, not merely stops being live — so it emits
    /// [`Effect::MembershipChanged`]. Without it a driver's published roster
    /// would keep listing a member the engine has forgotten, until some
    /// unrelated change happened to refresh it.
    pub(super) fn reap_dead(&mut self, now: Time) -> Vec<Effect> {
        let reap_after = self.reap_after();
        let stale: Vec<NodeId> = self
            .due_members(now)
            .into_iter()
            .filter(|node| {
                *node != self.local
                    && self.members.get(node).is_some_and(|m| {
                        m.status == Status::Dead && now >= m.dead_since.saturating_add(reap_after)
                    })
            })
            .collect();
        if stale.is_empty() {
            return Vec::new();
        }
        for node in stale {
            self.forget(&node);
        }
        // The live set (which never counted a Dead member) is unchanged, so
        // the coordinator cannot move — no recompute needed.
        vec![Effect::MembershipChanged]
    }

    /// Drop expired TTL entries (they converge to absent everywhere once the
    /// author stops refreshing — no tombstone needed) and reap entry tombstones
    /// past `2×dead_timeout` (no longer gossiped after 1×, so no peer re-teaches
    /// them). The member's high-water mark is left untouched by reaping, so a
    /// digest can never claim to be behind on — and so resurrect — a reaped
    /// version. A TTL expiry is an observable state change and emits
    /// [`Effect::NodeStateChanged`]; a tombstone reap is not (the key already
    /// read as absent). Examines only the members and entries that are due.
    pub(super) fn reap_entries(&mut self, now: Time) -> Vec<Effect> {
        let mut effects = Vec::new();
        for node in self.due_members(now) {
            let Some(member) = self.members.get_mut(&node) else {
                continue;
            };
            let due = member.take_due_entries(now);
            if due.is_empty() {
                continue;
            }
            self.touch(due.len());
            for (key, entry) in due {
                if !entry.tombstone {
                    effects.push(Effect::NodeStateChanged {
                        node: node.clone(),
                        key,
                    });
                }
            }
            self.reindex(&node);
        }
        effects
    }

    /// Live members (excluding self) we may probe or gossip to.
    pub(super) fn probe_candidates(&self) -> impl Iterator<Item = &NodeId> {
        self.members
            .iter()
            .filter(|(node, m)| **node != self.local && m.status != Status::Dead)
            .map(|(node, _)| node)
    }

    /// Builds a `Send` effect carrying a bare probe frame (no piggybacked view).
    pub(super) fn send_probe(
        &self,
        to: NodeId,
        kind: wire::Kind,
        target: Option<NodeId>,
    ) -> Effect {
        Effect::Send {
            to,
            wire: wire::encode(&wire::Frame {
                kind,
                group: self.group.clone(),
                target,
                digest: Vec::new(),
                wants: Vec::new(),
                members: Vec::new(),
                metadata: Vec::new(),
                lead: None,
            }),
        }
    }
}
