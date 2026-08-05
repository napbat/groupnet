//! Failure detection (probes, suspicion) and reaping.

use crate::membership::Status;
use crate::{NodeId, Time, wire};

use super::effect::Effect;
use super::state::{GroupEngine, Pending, ProbePhase};

impl GroupEngine {
    pub(super) fn probe(&mut self, now: Time) -> Vec<Effect> {
        if self.pending.is_some() {
            return Vec::new(); // one probe outstanding at a time
        }
        let candidates: Vec<NodeId> = self.probe_candidates().cloned().collect();
        if candidates.is_empty() {
            return Vec::new();
        }
        let target = candidates[self.probe_cursor % candidates.len()].clone();
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
    pub(super) fn escalate_indirect(&mut self, target: &NodeId, now: Time) -> Vec<Effect> {
        let probers: Vec<NodeId> = self
            .probe_candidates()
            .filter(|n| **n != *target)
            .take(self.config.indirect_probes.max(1))
            .cloned()
            .collect();
        if probers.is_empty() {
            // No one to ask (tiny cluster) — fall back to direct suspicion.
            self.pending = None;
            return self.suspect(target, now);
        }
        self.pending = Some(Pending {
            target: target.clone(),
            deadline: now.saturating_add(self.config.probe_timeout_ms),
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

    pub(super) fn suspect(&mut self, target: &NodeId, now: Time) -> Vec<Effect> {
        let became_suspect = match self.members.get_mut(target) {
            Some(m) if m.status == Status::Alive => {
                m.adopt_status(Status::Suspect, now);
                m.suspect_since = now;
                true
            }
            _ => false,
        };
        if !became_suspect {
            return Vec::new();
        }
        self.stamp(target);
        let mut effects = vec![Effect::MembershipChanged];
        effects.extend(self.recompute_coordinator());
        self.nudge_anti_entropy();
        effects
    }

    pub(super) fn reap_suspects(&mut self, now: Time) -> Vec<Effect> {
        let timeout = self.config.suspect_timeout_ms;
        let dead: Vec<NodeId> = self
            .members
            .iter()
            .filter(|(node, m)| {
                **node != self.local
                    && m.status == Status::Suspect
                    && now >= m.suspect_since.saturating_add(timeout)
            })
            .map(|(node, _)| node.clone())
            .collect();
        if dead.is_empty() {
            return Vec::new();
        }
        for node in &dead {
            if let Some(m) = self.members.get_mut(node) {
                m.adopt_status(Status::Dead, now);
                m.dead_since = now;
            }
            self.stamp(node);
        }
        let mut effects = vec![Effect::MembershipChanged];
        effects.extend(self.recompute_coordinator());
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
        let reap_after = self.config.dead_timeout_ms.saturating_mul(2);
        let stale: Vec<NodeId> = self
            .members
            .iter()
            .filter(|(node, m)| {
                **node != self.local
                    && m.status == Status::Dead
                    && now >= m.dead_since.saturating_add(reap_after)
            })
            .map(|(node, _)| node.clone())
            .collect();
        if stale.is_empty() {
            return Vec::new();
        }
        for node in stale {
            self.members.remove(&node);
            self.digest_cursors.remove(&node);
            self.digest_visits.remove(&node);
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
    /// read as absent).
    pub(super) fn reap_entries(&mut self, now: Time) -> Vec<Effect> {
        let reap_after = self.config.dead_timeout_ms.saturating_mul(2);
        let mut expired: Vec<(NodeId, String)> = Vec::new();
        for (node, member) in &mut self.members {
            member.entries.retain(|key, e| {
                if e.tombstone {
                    now < e.tombstone_since.saturating_add(reap_after)
                } else if e.expired(now) {
                    expired.push((node.clone(), key.clone()));
                    false
                } else {
                    true
                }
            });
        }
        expired
            .into_iter()
            .map(|(node, key)| Effect::NodeStateChanged { node, key })
            .collect()
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
