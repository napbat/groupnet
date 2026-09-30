//! Bounded candidate selection and progress-based donor-wait handling.

use super::{
    BTreeMap, BTreeSet, BootstrapClaim, BootstrapEffect, BootstrapError, BootstrapMember,
    BootstrapOperation, BootstrapStage, BootstrapStep, ClaimEngine, ClaimIdentity, ClaimPhase,
    Time,
};
use crate::placement;

impl ClaimEngine {
    pub(super) fn choose(
        &mut self,
        members: &[BootstrapMember],
        claims: Vec<BootstrapClaim>,
    ) -> BootstrapStep {
        if self.validate_roster(members, &claims).is_err() {
            return Self::reject(BootstrapError::InvalidObservation);
        }
        if self.follow_due.is_some_and(|due| self.now >= due)
            && let Some(selected) = self.selected.take()
        {
            self.excluded.insert(selected);
            self.follow_due = None;
        }
        // Keep the bounded renewal high-water marks until the episode ends.
        // Forgetting an expired mark would let a stale source record revive it.
        let mut candidates = BTreeMap::new();
        for claim in claims {
            // Exclusion is permanent for this finite episode. An old failed
            // attempt need not consume the bounded renewal map when its node
            // publishes a fresh attempt in a later complete source cut.
            if self.excluded.contains(&claim.identity) {
                continue;
            }
            match self.track_claim(&claim) {
                Ok(true) => {
                    candidates.insert(claim.identity.node.clone(), claim);
                }
                Ok(_) => {}
                Err(error) => return Self::reject(error),
            }
        }
        if candidates.len() > self.config.max_members || !candidates.contains_key(&self.me) {
            return Self::reject(BootstrapError::InvalidObservation);
        }
        let incumbents: BTreeSet<_> = candidates
            .iter()
            .filter(|(_, claim)| claim.phase != ClaimPhase::Willing)
            .map(|(node, _)| node.clone())
            .collect();
        let roster = if incumbents.is_empty() {
            candidates.keys().cloned().collect()
        } else {
            incumbents
        };
        let Some(winner) = placement::owner(&self.scope.placement_key(), &roster) else {
            return Self::reject(BootstrapError::InvalidObservation);
        };
        let selected = candidates.remove(&winner).expect("winner was in roster");
        let same_selection = self.selected.as_ref() == Some(&selected.identity);
        self.selected = Some(selected.identity.clone());
        self.operation = None;
        self.operation_due = None;
        if winner == self.me {
            self.choose_local(selected)
        } else {
            self.choose_remote(selected, same_selection)
        }
    }

    fn choose_local(&mut self, selected: BootstrapClaim) -> BootstrapStep {
        if selected.phase != ClaimPhase::Ready
            && self.ready_retry_due.is_some_and(|due| self.now < due)
        {
            // A selected Ready donor was temporarily unavailable. Sample
            // another complete cut before starting a duplicate origin scan.
            let due = self.ready_retry_due.expect("checked above");
            let Some(next) = self.now.0.checked_add(self.config.observe_ms).map(Time) else {
                return self.terminate();
            };
            self.selected = None;
            self.stage = BootstrapStage::Settling;
            self.settle_due = Some(next.min(due));
            return self.ok(Vec::new());
        }
        self.ready_retry_due = None;
        self.follow_due = None;
        if selected.phase == ClaimPhase::Ready {
            self.stage = BootstrapStage::DonorAvailable;
            self.total_due = None;
            return self.ok(Vec::new());
        }
        self.local_phase = ClaimPhase::Building;
        let Ok(claim) = self.publish_renewal() else {
            return self.terminate();
        };
        let Ok(op) = self.operation(self.config.donor_wait_ms) else {
            return self.terminate();
        };
        self.stage = BootstrapStage::Building;
        self.ok(vec![
            claim,
            BootstrapEffect::BuildOrigin {
                op,
                selected: selected.identity,
            },
        ])
    }

    /// A local build that keeps committing work is never failed by its stall
    /// bound: each advance restarts that bound and the episode budget, and a
    /// fresh renewal carries the new progress to followers.
    pub(super) fn build_progressed(
        &mut self,
        op: BootstrapOperation,
        selected: &ClaimIdentity,
    ) -> BootstrapStep {
        if self.stage != BootstrapStage::Building
            || self.operation != Some(op)
            || self.operation_due.is_none_or(|due| self.now >= due)
            || self.selected.as_ref() != Some(selected)
            || *selected != self.identity()
        {
            return Self::reject(BootstrapError::StaleOperation);
        }
        let (Some(progress), Some(total_due), Some(build_due)) = (
            self.local_progress.checked_add(1),
            self.now.0.checked_add(self.config.total_ms).map(Time),
            self.now.0.checked_add(self.config.donor_wait_ms).map(Time),
        ) else {
            return self.terminate();
        };
        self.local_progress = progress;
        self.total_due = Some(total_due);
        self.operation_due = Some(build_due.min(total_due));
        let Ok(claim) = self.publish_renewal() else {
            return self.terminate();
        };
        self.ok(vec![claim])
    }

    /// How long a follower keeps a selected build without seeing it advance:
    /// the builder's own stall bound, plus the claim TTL within which its last
    /// advance is visible if the claim is live at all, plus one observation.
    /// A builder that really stalls ends itself at its own bound and withdraws
    /// its claim, so this is only the backstop for an unwithdrawn claim.
    fn follow_due_from_now(&self) -> Option<Time> {
        self.config
            .donor_wait_ms
            .checked_add(self.config.claim_ttl_ms)
            .and_then(|wait| wait.checked_add(self.config.observe_ms))
            .and_then(|wait| self.now.0.checked_add(wait))
            .map(Time)
    }

    fn choose_remote(&mut self, selected: BootstrapClaim, same_selection: bool) -> BootstrapStep {
        let mut progressed = false;
        if selected.phase == ClaimPhase::Ready {
            if self.ready_retry_due.is_none() {
                let Some(due) = self.now.0.checked_add(self.config.donor_wait_ms).map(Time) else {
                    return self.terminate();
                };
                self.ready_retry_due = Some(due.min(self.total_due.unwrap_or(due)));
            }
            self.follow_due = None;
        } else if !same_selection || self.follow_due.is_none() {
            let Some(due) = self.follow_due_from_now() else {
                return self.terminate();
            };
            self.follow_due = Some(due.min(self.total_due.unwrap_or(due)));
            self.follow_progress = Some(selected.progress);
        } else if self
            .follow_progress
            .is_some_and(|seen| selected.progress > seen)
        {
            // The followed build advanced: a progressing builder keeps its
            // follower, and only a build seen stalled for longer than its own
            // stall bound is excluded in favor of this node's origin scan.
            let (Some(total_due), Some(follow_due)) = (
                self.now.0.checked_add(self.config.total_ms).map(Time),
                self.follow_due_from_now(),
            ) else {
                return self.terminate();
            };
            self.total_due = Some(total_due);
            self.follow_due = Some(follow_due.min(total_due));
            self.follow_progress = Some(selected.progress);
            progressed = true;
        }
        let Ok(op) = self.operation(if selected.phase == ClaimPhase::Ready {
            self.config.donor_wait_ms
        } else {
            self.config.observe_ms
        }) else {
            return self.terminate();
        };
        if selected.phase == ClaimPhase::Ready {
            self.operation_due = self.operation_due.map(|due| {
                due.min(
                    self.ready_retry_due
                        .expect("Ready selection set first donor deadline"),
                )
            });
        }
        self.stage = if selected.phase == ClaimPhase::Ready {
            BootstrapStage::DonorAvailable
        } else {
            BootstrapStage::Following
        };
        let effect = if selected.phase == ClaimPhase::Ready {
            BootstrapEffect::DonorAvailable {
                op,
                selected: selected.identity,
            }
        } else {
            BootstrapEffect::FollowBuilder {
                op,
                selected: selected.identity,
            }
        };
        let mut effects = Vec::with_capacity(2);
        if progressed {
            effects.push(BootstrapEffect::BuilderProgressed);
        }
        effects.push(effect);
        self.ok(effects)
    }
}
