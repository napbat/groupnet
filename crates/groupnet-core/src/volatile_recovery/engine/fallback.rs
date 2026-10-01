//! Every path by which an episode abandons its current plan. Each reports
//! `FellBack` with the stage it left and why, after the fallback's effects.

use super::super::types::{RecoveryEffect, RecoveryFallback, RecoveryStage, RecoveryStep};
use super::{Baseline, Plan, RecoveryEngine};
use crate::Time;

impl RecoveryEngine {
    fn fell_back(&self, reason: RecoveryFallback) -> RecoveryEffect {
        RecoveryEffect::FellBack {
            from: self.state.stage,
            reason,
        }
    }

    /// End the episode serving from origin only, or rearm a later one.
    pub(super) fn origin_only(&mut self, reason: RecoveryFallback) -> RecoveryStep {
        let report = self.fell_back(reason);
        let mut cancel = self.cancel_baseline();
        cancel.push(report);
        self.clear_work();
        self.state.stage = RecoveryStage::OriginOnly;
        self.state.recovered = false;
        let Some(policy) = self.rearm else {
            return Self::step_ok(cancel);
        };
        if self.rearm_exhausted || self.next_token == 0 || self.state.generation == u64::MAX {
            self.rearm_exhausted = true;
            return RecoveryStep {
                effects: cancel,
                rejection: Some(super::RecoveryError::Exhausted),
            };
        }
        let Some(due) = self.now.0.checked_add(self.next_rearm_ms).map(Time) else {
            self.rearm_exhausted = true;
            return RecoveryStep {
                effects: cancel,
                rejection: Some(super::RecoveryError::Exhausted),
            };
        };
        self.rearm_due = Some(due);
        self.next_rearm_ms = self.next_rearm_ms.saturating_mul(2).min(policy.max_ms);
        self.with_timer(cancel)
    }

    /// A lapse proof failed: restart as a full rebuild.
    pub(super) fn fallback(&mut self, reason: RecoveryFallback) -> RecoveryStep {
        let report = self.fell_back(reason);
        let lapses = self.state.covered_lapses;
        let mut step = self.begin(Plan::Full, lapses);
        step.effects.push(report);
        step
    }

    pub(super) fn fallback_or_origin(&mut self, reason: RecoveryFallback) -> RecoveryStep {
        if self.plan == Plan::Lapse {
            self.fallback(reason)
        } else if self.baseline == Baseline::Peer {
            self.recover_origin(reason)
        } else {
            self.origin_only(reason)
        }
    }

    /// Abandon a peer baseline, or its acquisition, for this episode's own
    /// origin rebuild.
    pub(super) fn recover_origin(&mut self, reason: RecoveryFallback) -> RecoveryStep {
        let report = self.fell_back(reason);
        let mut step = self.rebuild_origin();
        step.effects.push(report);
        step
    }

    /// Rebuild from origin: the plan of an episode without a peer child, and
    /// the fallback of one whose peer path failed.
    pub(super) fn rebuild_origin(&mut self) -> RecoveryStep {
        let mut effects = self.cancel_baseline();
        self.baseline = Baseline::Origin;
        self.seen.clear();
        self.exempt.clear();
        self.known_heads.clear();
        self.seals.clear();
        self.heads.clear();
        self.barrier_rounds = 0;
        self.peer_members.clear();
        let Ok(op) = self.issue(RecoveryStage::Rebuilding) else {
            let mut terminal = self.origin_only(RecoveryFallback::Exhausted);
            effects.append(&mut terminal.effects);
            terminal.effects = effects;
            return terminal;
        };
        effects.push(RecoveryEffect::RebuildOrigin { op });
        self.with_timer(effects)
    }

    /// A full-plan operation expired or failed: retry its stage after one
    /// poll interval, or end in origin-only service from any other stage.
    pub(super) fn retry_full(&mut self, reason: RecoveryFallback) -> RecoveryStep {
        match self.state.stage {
            RecoveryStage::Invalidating | RecoveryStage::Rebuilding | RecoveryStage::Affirming => {
                self.wait_for(self.state.stage, self.config.poll_ms)
            }
            _ => self.origin_only(reason),
        }
    }

    pub(super) fn retry_invalidation(&mut self) -> RecoveryStep {
        let Ok(op) = self.issue(RecoveryStage::Invalidating) else {
            return self.origin_only(RecoveryFallback::Exhausted);
        };
        self.with_timer(vec![RecoveryEffect::Invalidate {
            op,
            distrust_bodies: true,
        }])
    }

    pub(super) fn retry_rebuild(&mut self) -> RecoveryStep {
        let Ok(op) = self.issue(RecoveryStage::Rebuilding) else {
            return self.origin_only(RecoveryFallback::Exhausted);
        };
        self.with_timer(vec![RecoveryEffect::RebuildOrigin { op }])
    }
}
