//! Idle source-poll decisions and independent proof-freshness bounds.

use super::SessionEngine;
use crate::Time;
use crate::replication::{Effect, Reject, Stage, Step};

pub(super) use crate::replication::idle::IdleState;

impl SessionEngine {
    pub(super) fn record_tail_schedule(&mut self, unchanged: bool) -> Result<(), Reject> {
        let fresh = self
            .now
            .0
            .checked_add(self.config.tail_check_ms)
            .ok_or(Reject::Exhausted)?;
        let interval = self
            .idle_state
            .checked_interval(
                unchanged,
                self.config.idle,
                self.config.tail_check_ms,
                &self.scope,
                self.session_id,
            )
            .ok_or(Reject::Exhausted)?;
        let poll = self.now.0.checked_add(interval).ok_or(Reject::Exhausted)?;
        self.freshness_due = Time(fresh);
        self.tail_due = Time(poll);
        Ok(())
    }

    pub(super) fn idle_exhausted(&mut self) -> Step {
        let closed = self.close_gate();
        self.state.stage = Stage::RetryExhausted;
        self.outstanding = None;
        self.operation_due = None;
        self.pending_tail = false;
        self.retry_due = None;
        Step {
            effects: closed.effects,
            rejection: Some(Reject::Exhausted),
        }
    }

    pub(super) fn activity(&mut self) -> Step {
        if matches!(
            self.state.stage,
            Stage::Cancelled
                | Stage::RetryWait
                | Stage::RetryExhausted
                | Stage::NeedsSnapshot
                | Stage::SnapshotAborted
                | Stage::IrrecoverableGap
        ) {
            return Step::ok(Vec::new());
        }
        self.idle_state.force_hot_next();
        if self.state.stage == Stage::CheckingTail && self.outstanding.is_some() {
            return Step::ok(Vec::new());
        }
        if self.snapshot.is_some() || self.snapshot_cleanup.is_some() {
            return Step::ok(Vec::new());
        }
        if self.proof.is_none() || !self.ready || self.now >= self.freshness_due {
            return self.request_tail();
        }
        let Some(normal) = self.now.0.checked_add(self.config.tail_check_ms) else {
            return self.idle_exhausted();
        };
        let earlier = Time(normal).min(self.freshness_due);
        if earlier >= self.tail_due {
            return Step::ok(Vec::new());
        }
        self.tail_due = earlier;
        Step::ok(
            self.next_deadline()
                .map_or_else(Vec::new, |due| vec![Effect::ArmTimer(due)]),
        )
    }

    pub(super) fn expire_freshness(&mut self) -> Step {
        if !self.ready || self.now < self.freshness_due {
            return Step::ok(Vec::new());
        }
        self.close_gate()
    }
}
