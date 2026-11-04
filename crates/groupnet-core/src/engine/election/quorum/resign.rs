//! Row Q16: a leaving host **resigns** its epoch, and each voter ends its
//! promise to it at once (napbat/groupnet#13).
//!
//! Without it, a host that leaves on purpose still holds the promise of each
//! voter that renewed it: row Q3 refuses every other claimant until that
//! promise ends, one `lease_ms` after the last renewal. The group has no host
//! for that time, although the old host already gave the group up.
//!
//! # The rows
//!
//! * **Q16a (send).** Row 15 demotes a leaving host (or abandons its claim)
//!   first. Then the node sends [`wire::LeadBody::Resign`] with the epoch it
//!   gave up to every claim target: every live peer and every voter.
//! * **Q16b (release).** A voter whose grant is exactly `(epoch, host)`, from
//!   `host` itself, ends its promise now and marks the pair resigned: row Q2
//!   refuses that pair from then on, and row Q3a does not re-send it to its
//!   claimant. A grant of any other pair is not changed: a late resign of an
//!   earlier epoch releases nothing.
//! * **Q16c (adopt).** A node that holds `(epoch, Some(host))` adopts
//!   `(epoch, None)`: it knows the group has no host at that epoch. A peer
//!   that missed the resign can teach it `(epoch, Some(host))` again, because
//!   `(epoch, None)` is the weakest pair at that epoch (row 12). That changes
//!   only what the node believes until a later epoch: the voter ledger keeps
//!   the resigned mark, and a claim depends on rank, not on the believed
//!   host.
//! * **Q16d (receipt).** A voter answers each resign with
//!   `LeadState { epoch, host: None }` to the resigning node. The resigning
//!   node keeps the voters that did not answer yet, and sends them the
//!   resign again on each anti-entropy round while it runs.
//!   [`GroupEngine::resign_pending`] counts them, so that a driver can wait
//!   until each voter has the resign before it stops the node: a resign
//!   still in a queue of the node when it stops never leaves. A state from a
//!   pending voter at a higher epoch, or at the resigned epoch with another
//!   host or none, also counts as the answer: that voter has moved on.
//!
//! # Why fencing stays safe
//!
//! The promise exists so that no other claimant can win while the host it
//! protects can still act on its lease. A resign comes only from the host
//! itself (Q16b checks the sender), and only after row 15 demoted it. A
//! leaving node never enters hostship again: row 1's guard refuses every claim
//! while it leaves, and a grant counts only toward a standing claim. A voter
//! never re-grants the resigned pair, so no renewal can revive it. So once a
//! voter has seen the resign, the old host writes nothing more at the old
//! epoch, and a successor elected at a higher epoch fences what is left of it
//! exactly as after a lapse.
//!
//! Nothing is persisted. A voter that restarts after a resign recovers the
//! pair and applies the boot blackout, which is the posture without a resign.
//!
//! A lost resign costs only time: the voter keeps its promise until it ends,
//! as before this row, unless a later copy of row Q16d reaches it.

use crate::{Effect, GroupEngine, NodeId, Time, wire};

use super::super::Role;

impl GroupEngine {
    /// Row Q16a's epoch: what a leaving node gives up under Quorum — its
    /// adopted epoch as host, or its standing claim's epoch as claimant.
    /// `None` under every other activation, and for a follower. Read before
    /// row 15 demotes, which forgets it.
    pub(in crate::engine::election) fn resign_epoch(&self) -> Option<u64> {
        if !self.is_quorum() {
            return None;
        }
        let el = self.election.as_ref()?;
        match el.role {
            Role::Host => Some(el.epoch),
            Role::Claimant => el.claim.map(|claim| claim.epoch),
            Role::Follower => None,
        }
    }

    /// Row Q16a's frames: the resign of `epoch` to every claim target. Every
    /// other voter of the roster is pending until it answers (row Q16d).
    pub(in crate::engine::election) fn broadcast_resign(&mut self, epoch: u64) -> Vec<Effect> {
        let local = self.local.clone();
        let voters = self
            .quorum_voters()
            .map(|voters| voters.iter().filter(|v| **v != local).cloned().collect())
            .unwrap_or_default();
        if let Some(q) = self.quorum_mut() {
            q.resigning = Some((epoch, voters));
        }
        let body = wire::LeadBody::Resign { epoch, host: local };
        self.claim_targets()
            .into_iter()
            .map(|to| self.send_lead(to, body.clone()))
            .collect()
    }

    /// Row Q16d's resend: on an anti-entropy round, the resign again to each
    /// voter that did not answer it yet.
    pub(in crate::engine::election) fn resend_resign(&self, anti_entropy_due: bool) -> Vec<Effect> {
        let Some((epoch, pending)) = self
            .election
            .as_ref()
            .and_then(|el| el.quorum.as_ref())
            .and_then(|q| q.resigning.as_ref())
        else {
            return Vec::new();
        };
        if !anti_entropy_due || pending.is_empty() {
            return Vec::new();
        }
        let body = wire::LeadBody::Resign {
            epoch: *epoch,
            host: self.local.clone(),
        };
        pending
            .iter()
            .map(|to| self.send_lead(to.clone(), body.clone()))
            .collect()
    }

    /// Row Q16d's receipt: a state from `from` at (`epoch`, `host`) answers
    /// the resign of this node when it shows that `from` no longer holds
    /// this node as host of the resigned epoch.
    pub(in crate::engine::election) fn note_resign_receipt(
        &mut self,
        from: &NodeId,
        epoch: u64,
        host: Option<&NodeId>,
    ) {
        let local = self.local.clone();
        let Some((resigned, pending)) = self.quorum_mut().and_then(|q| q.resigning.as_mut()) else {
            return;
        };
        if epoch > *resigned || (epoch == *resigned && host != Some(&local)) {
            pending.remove(from);
        }
    }

    /// The voters that did not yet answer the resign of this node (row
    /// Q16d): zero when it resigned nothing, and once each voter answered.
    #[must_use]
    pub fn resign_pending(&self) -> usize {
        self.election
            .as_ref()
            .and_then(|el| el.quorum.as_ref())
            .and_then(|q| q.resigning.as_ref())
            .map_or(0, |(_, pending)| pending.len())
    }

    /// Rows Q16b and Q16c: an inbound resign. Dropped unless it comes from the
    /// node it names, and outside Quorum.
    pub(in crate::engine::election) fn on_lead_resign(
        &mut self,
        from: &NodeId,
        epoch: u64,
        host: &NodeId,
        now: Time,
    ) -> Vec<Effect> {
        if from != host || !self.is_quorum() {
            return Vec::new();
        }
        if let Some(q) = self.quorum_mut()
            && q.granted
                .as_ref()
                .is_some_and(|(granted, claimant)| *granted == epoch && claimant == host)
        {
            q.resigned = true;
            q.grant_promise_until = q.grant_promise_until.min(now);
        }
        // Row Q16d: a voter answers every copy, so that the resigning node
        // can stop.
        let mut effects = Vec::new();
        if self.is_voter() {
            effects.push(self.send_lead(from.clone(), wire::LeadBody::State { epoch, host: None }));
        }
        let Some(el) = self.election.as_mut() else {
            return effects;
        };
        el.highest_seen = el.highest_seen.max(epoch);
        if el.epoch == epoch && el.host.as_ref() == Some(host) {
            el.host = None;
            effects.insert(0, Effect::LeadershipChanged { epoch, host: None });
        }
        effects
    }
}

#[cfg(test)]
mod tests {
    use super::super::{Verdict, grant_verdict};
    use crate::{NodeId, Time};

    /// Row Q16b in the verdict: a resigned pair is refused whenever it is
    /// asked for again, while a higher epoch of the same claimant is still a
    /// new grant.
    #[test]
    fn a_resigned_pair_is_never_granted_again() {
        let pair = (5, NodeId::new("b"));
        let verdict = |resigned: bool, epoch: u64, claimant: &str| {
            grant_verdict(
                false,
                Some(&pair),
                resigned,
                Time::ZERO,
                Time(10),
                epoch,
                &NodeId::new(claimant),
            )
        };
        assert_eq!(verdict(false, 5, "b"), Verdict::Repeat);
        assert_eq!(verdict(true, 5, "b"), Verdict::Refuse);
        assert_eq!(verdict(true, 6, "b"), Verdict::Fresh);
        assert_eq!(verdict(true, 6, "c"), Verdict::Fresh);
        assert_eq!(verdict(true, 5, "c"), Verdict::Refuse);
    }
}
