//! Private checkpoint bootstrap transitions sharing the session allocator.

use super::{RetryTarget, SessionEngine};
use crate::replication::{ApplyReceipt, Cursor, Effect, Operation, Reject, Stage, Step};

impl SessionEngine {
    pub(super) fn start_bootstrap(&mut self) -> Step {
        if self.state.stage != Stage::Unready
            || self.outstanding.is_some()
            || self.snapshot_cleanup.is_some()
        {
            return Step::reject(Reject::Stage);
        }
        self.retry_target = RetryTarget::Bootstrap;
        self.load_checkpoint()
    }

    pub(super) fn load_checkpoint(&mut self) -> Step {
        self.bootstrap_candidate = None;
        let Ok(op) = self.issue(Stage::LoadingCheckpoint) else {
            self.state.stage = Stage::Unready;
            return Step::reject(Reject::Exhausted);
        };
        Step::ok(vec![
            Effect::LoadCheckpoint {
                op,
                scope: self.scope.clone(),
            },
            Effect::ArmTimer(self.operation_due.expect("issued deadline")),
        ])
    }

    pub(super) fn checkpoint_loaded(
        &mut self,
        op: Operation,
        cursor: Option<Cursor>,
        payload_id: Option<u64>,
    ) -> Step {
        if !self.matches(op, Stage::LoadingCheckpoint) {
            return Step::reject(Reject::StaleOperation);
        }
        if cursor.is_none() != payload_id.is_none() {
            return Step::reject(Reject::Discontinuity);
        }
        let Some(cursor) = cursor else {
            self.outstanding = None;
            self.operation_due = None;
            self.retry_target = RetryTarget::Tail;
            return self.check_tail();
        };
        if let Err(error) = cursor.validate(&self.scope, self.config.max_cursor_bytes) {
            return Step::reject(Reject::Identity(error));
        }
        if payload_id != Some(op.token) {
            return Step::reject(Reject::Discontinuity);
        }
        let Some(payload_id) = payload_id else {
            return Step::reject(Reject::Discontinuity);
        };
        // A floor queued before a checkpoint was known cannot define this
        // source's history. Drop an incompatible demand without discarding a
        // valid loaded state/cursor pair.
        if self
            .state
            .target
            .as_ref()
            .is_some_and(|target| target.history != cursor.history)
        {
            self.state.target = None;
        }
        let Ok(install_op) = self.issue(Stage::InstallingCheckpoint) else {
            self.state.stage = Stage::Unready;
            return Step::reject(Reject::Exhausted);
        };
        self.bootstrap_candidate = Some((cursor.clone(), payload_id));
        Step::ok(vec![
            Effect::InstallCheckpoint {
                op: install_op,
                cursor,
                payload_id,
            },
            Effect::ArmTimer(self.operation_due.expect("issued deadline")),
        ])
    }

    pub(super) fn checkpoint_installed(&mut self, op: Operation, receipt: ApplyReceipt) -> Step {
        if !self.matches(op, Stage::InstallingCheckpoint) {
            return Step::reject(Reject::StaleOperation);
        }
        let Some((expected, _)) = self.bootstrap_candidate.as_ref() else {
            return Step::reject(Reject::Stage);
        };
        if !receipt.durable || receipt.through != *expected {
            return Step::reject(Reject::Discontinuity);
        }
        self.resume_state(receipt.through, false)
    }

    pub(super) fn resume_state(&mut self, cursor: Cursor, next_generation: bool) -> Step {
        if let Err(error) = cursor.validate(&self.scope, self.config.max_cursor_bytes) {
            return Step::reject(Reject::Identity(error));
        }
        let closed = self.close_gate();
        if closed.rejection.is_some() {
            return closed;
        }
        let mut effects = closed.effects;
        if next_generation {
            let Some(next) = self.state.generation.checked_add(1) else {
                self.state.stage = Stage::Unready;
                return Step {
                    effects,
                    rejection: Some(Reject::Exhausted),
                };
            };
            self.state.generation = next;
        }
        let drop_target = next_generation
            || self
                .state
                .target
                .as_ref()
                .is_some_and(|target| target.history != cursor.history);
        self.state.materialized = Some(cursor.clone());
        self.state.checkpoint = Some(cursor);
        self.state.head = None;
        if drop_target {
            self.state.target = None;
        }
        self.outstanding = None;
        self.operation_due = None;
        self.pending_batch = None;
        self.bootstrap_candidate = None;
        self.retry_target = RetryTarget::Tail;
        self.proof = None;
        self.retries = 0;
        self.retry_due = None;
        self.revoke_op = None;
        self.pending_tail = false;
        let check = self.check_tail();
        effects.extend(check.effects);
        Step {
            effects,
            rejection: check.rejection,
        }
    }
}
