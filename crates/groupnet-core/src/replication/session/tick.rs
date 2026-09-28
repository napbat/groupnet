//! Independent replay and recovery deadlines for one logical clock sample.

use super::{RetryTarget, SessionEngine};
use crate::Time;
use crate::replication::{Event, Stage, Step};

impl SessionEngine {
    pub(super) fn tick_replay(&mut self, now: Time) -> Step {
        let freshness = self.expire_freshness();
        let mut ordinary = self.tick_replay_progress(now);
        if !freshness.effects.is_empty() {
            let mut effects = freshness.effects;
            effects.extend(ordinary.effects);
            if let Some(due) = self.next_deadline() {
                effects.push(crate::replication::Effect::ArmTimer(due));
            }
            ordinary.effects = effects;
        }
        ordinary.rejection = freshness.rejection.or(ordinary.rejection);
        ordinary
    }

    fn tick_replay_progress(&mut self, now: Time) -> Step {
        if self
            .snapshot_cleanup
            .as_ref()
            .is_some_and(|cleanup| !cleanup.discarding && now >= cleanup.due)
        {
            return self.expire_snapshot_cleanup();
        }
        if self
            .snapshot
            .as_ref()
            .is_some_and(|snapshot| now >= snapshot.total_due)
        {
            return self.abort_snapshot();
        }
        if matches!(
            self.state.stage,
            Stage::Cancelled
                | Stage::RetryExhausted
                | Stage::NeedsSnapshot
                | Stage::SnapshotAborted
                | Stage::IrrecoverableGap
        ) {
            return Step::ok(Vec::new());
        }
        if self
            .outstanding
            .is_some_and(|_| self.operation_due.is_some_and(|due| now >= due))
        {
            if let Some((op, _)) = self.outstanding {
                return self.step(Event::Failed { op });
            }
        }
        if let Some(due) = self.retry_due {
            if now >= due {
                self.retry_due = None;
                return if self.retry_target == RetryTarget::Bootstrap {
                    self.load_checkpoint()
                } else {
                    self.check_tail()
                };
            }
            return Step::ok(Vec::new());
        }
        if now >= self.tail_due && self.state.stage != Stage::CheckingTail {
            self.request_tail()
        } else {
            Step::ok(Vec::new())
        }
    }
}
