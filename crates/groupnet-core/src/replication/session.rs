//! Source-backed replica replay and snapshot session decisions.

mod bootstrap;
#[cfg(test)]
mod exhaustion_tests;
mod snapshot;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RetryTarget {
    Tail,
    Bootstrap,
}

use super::types::{
    Batch, Config, ConfigError, Effect, Event, Mode, Operation, ReadDecision, Refusal, Reject,
    Stage, State, Step,
};
use super::{BoundComparison, Comparison, Cursor, Scope, SourceProof};
use crate::Time;

/// One replay session for a scoped replica, driven only by explicit events.
#[derive(Clone, Debug)]
pub struct SessionEngine {
    session_id: u64,
    scope: Scope,
    mode: Mode,
    config: Config,
    state: State,
    next_token: u64,
    now: Time,
    tail_due: Time,
    retry_due: Option<Time>,
    outstanding: Option<(Operation, Stage)>,
    operation_due: Option<Time>,
    revoke_op: Option<Operation>,
    pending_tail: bool,
    retries: u32,
    pending_batch: Option<Batch>,
    bootstrap_candidate: Option<(Cursor, u64)>,
    retry_target: RetryTarget,
    proof: Option<SourceProof>,
    snapshot: Option<snapshot::Progress>,
    snapshot_cleanup: Option<snapshot::CleanupState>,
    live_attachment_attempt: Option<Operation>,
    mode_authority: bool,
    ready: bool,
}

impl SessionEngine {
    /// Creates one unready replay session. The caller must validate a checkpoint
    /// through its application adapter before sending [`Event::Resume`].
    /// `session_id` must be nonzero and unique across engine recreations while
    /// responses from an earlier incarnation can still arrive.
    ///
    /// # Errors
    /// Returns [`ConfigError::Zero`] for zero budgets/session incarnation or
    /// [`ConfigError::Identity`] for an invalid scope.
    pub fn new(
        scope: Scope,
        mode: Mode,
        config: Config,
        session_id: u64,
    ) -> Result<Self, ConfigError> {
        let config = config.validate()?;
        if session_id == 0 {
            return Err(ConfigError::Zero);
        }
        scope
            .validate(config.max_cursor_bytes)
            .map_err(ConfigError::Identity)?;
        Ok(Self {
            session_id,
            scope,
            mode,
            config,
            state: State {
                generation: 1,
                stage: Stage::Unready,
                materialized: None,
                checkpoint: None,
                head: None,
                target: None,
            },
            next_token: 1,
            now: Time::ZERO,
            tail_due: Time::ZERO,
            retry_due: None,
            outstanding: None,
            operation_due: None,
            revoke_op: None,
            pending_tail: false,
            retries: 0,
            pending_batch: None,
            bootstrap_candidate: None,
            retry_target: RetryTarget::Tail,
            proof: None,
            snapshot: None,
            snapshot_cleanup: None,
            live_attachment_attempt: None,
            mode_authority: false,
            ready: false,
        })
    }

    /// Current progress snapshot.
    #[must_use]
    pub fn state(&self) -> &State {
        &self.state
    }

    /// Current source/application operation, excluding a separate revocation.
    #[must_use]
    pub fn current_operation(&self) -> Option<Operation> {
        self.outstanding.map(|(op, _)| op)
    }

    /// Whether a queued effect or response still names the live operation.
    /// Revocation has independent correlation and remains valid alongside a
    /// newer source operation until cancelled or acknowledged.
    #[must_use]
    pub fn accepts_operation(&self, op: Operation) -> bool {
        self.revoke_op == Some(op)
            || self.snapshot_cleanup.as_ref().is_some_and(|cleanup| {
                cleanup.op == op && !cleanup.discarding && self.now < cleanup.due
            })
            || (self.outstanding.is_some_and(|(current, _)| current == op)
                && self.operation_due.is_some_and(|due| self.now < due))
    }

    /// Next live logical deadline, without retaining historical timer effects.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Time> {
        let ordinary = if self.outstanding.is_some() {
            self.operation_due
        } else if self.state.stage == Stage::RetryWait {
            self.retry_due
        } else if self.proof.is_some()
            && !matches!(
                self.state.stage,
                Stage::Cancelled
                    | Stage::RetryExhausted
                    | Stage::NeedsSnapshot
                    | Stage::SnapshotAborted
                    | Stage::IrrecoverableGap
            )
        {
            Some(self.tail_due)
        } else {
            None
        };
        let cleanup_due = self
            .snapshot_cleanup
            .as_ref()
            .and_then(|cleanup| (!cleanup.discarding).then_some(cleanup.due));
        let ordinary = match (ordinary, cleanup_due) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, None) | (None, a) => a,
        };
        match (
            ordinary,
            self.snapshot.as_ref().map(|snapshot| snapshot.total_due),
        ) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, None) | (None, a) => a,
        }
    }

    /// Current read verdict; the caller still supplies domain-specific checks.
    #[must_use]
    pub fn read_decision(&self) -> ReadDecision {
        if matches!(
            self.state.stage,
            Stage::NeedsSnapshot | Stage::SnapshotAborted | Stage::IrrecoverableGap
        ) {
            return ReadDecision::Refuse(Refusal::Gap);
        }
        if !self.ready || self.state.stage != Stage::Ready {
            return ReadDecision::Refuse(Refusal::Unready);
        }
        if self.now >= self.tail_due {
            return ReadDecision::Refuse(Refusal::TailCheckDue);
        }
        if !self.mode_authority || !self.proof.as_ref().is_some_and(|p| p.read_authority) {
            return ReadDecision::Refuse(Refusal::Authority);
        }
        self.state
            .materialized
            .clone()
            .map_or(ReadDecision::Refuse(Refusal::Unready), ReadDecision::Serve)
    }

    fn fresh_operation(&mut self) -> Result<Operation, Reject> {
        let token = self.next_token;
        if token == 0 {
            return Err(Reject::Exhausted);
        }
        self.next_token = token.checked_add(1).unwrap_or(0);
        Ok(Operation {
            session: self.session_id,
            generation: self.state.generation,
            token,
        })
    }

    fn issue(&mut self, stage: Stage) -> Result<Operation, Reject> {
        let due = self
            .now
            .0
            .checked_add(self.config.attempt_timeout_ms)
            .ok_or(Reject::Exhausted)?;
        let op = self.fresh_operation()?;
        self.state.stage = stage;
        self.outstanding = Some((op, stage));
        self.operation_due = Some(Time(due));
        Ok(op)
    }

    fn matches(&self, op: Operation, stage: Stage) -> bool {
        self.outstanding == Some((op, stage))
            && op.session == self.session_id
            && op.generation == self.state.generation
            && self.operation_due.is_some_and(|due| self.now < due)
    }

    fn close_gate(&mut self) -> Step {
        if self.ready {
            self.ready = false;
            let Ok(op) = self.fresh_operation() else {
                self.state.stage = Stage::RetryExhausted;
                self.revoke_op = None;
                return Step {
                    effects: vec![Effect::RevokeServingUnconfirmed],
                    rejection: Some(Reject::Exhausted),
                };
            };
            self.revoke_op = Some(op);
            Step::ok(vec![Effect::RevokeServing { op }])
        } else {
            Step::ok(Vec::new())
        }
    }

    fn request_tail(&mut self) -> Step {
        if self.outstanding.is_some() {
            self.pending_tail = true;
            return Step::ok(Vec::new());
        }
        self.check_tail()
    }

    fn check_tail(&mut self) -> Step {
        let closed = self.close_gate();
        if closed.rejection.is_some() {
            return closed;
        }
        let mut effects = closed.effects;
        self.pending_tail = false;
        let Ok(op) = self.issue(Stage::CheckingTail) else {
            self.state.stage = Stage::Unready;
            return Step {
                effects,
                rejection: Some(Reject::Exhausted),
            };
        };
        effects.push(Effect::CheckTail {
            op,
            scope: self.scope.clone(),
            from: self.state.materialized.clone(),
        });
        effects.push(Effect::ArmTimer(
            self.next_deadline().expect("issued deadline"),
        ));
        Step::ok(effects)
    }

    fn relation(
        comparisons: &[BoundComparison],
        left: &Cursor,
        right: &Cursor,
        proof: &SourceProof,
    ) -> Result<Comparison, Reject> {
        if left.history != proof.head.history || right.history != proof.head.history {
            return Err(Reject::History);
        }
        comparisons
            .iter()
            .find_map(|c| c.for_operands(left, right, &proof.id))
            .filter(|c| *c != Comparison::Incomparable)
            .ok_or(Reject::Comparison)
    }

    fn start_scan(&mut self) -> Step {
        let Some(from) = self.state.materialized.clone() else {
            self.state.stage = Stage::NeedsSnapshot;
            return Step::ok(vec![Effect::NeedsSnapshot]);
        };
        let Ok(op) = self.issue(Stage::Scanning) else {
            self.state.stage = Stage::Unready;
            return Step::reject(Reject::Exhausted);
        };
        Step::ok(vec![
            Effect::Scan {
                op,
                from,
                max_events: self.config.max_batch_events,
                max_bytes: self.config.max_batch_bytes,
            },
            Effect::ArmTimer(self.next_deadline().expect("issued deadline")),
        ])
    }

    fn set_ready_or_wait(&mut self, comparisons: &[BoundComparison]) -> Step {
        let Some(proof) = self.proof.as_ref() else {
            return Step::reject(Reject::Stage);
        };
        let Some(materialized) = self.state.materialized.as_ref() else {
            return Step::reject(Reject::Stage);
        };
        let head_order = match Self::relation(comparisons, materialized, &proof.head, proof) {
            Ok(order) => order,
            Err(error) => return Step::reject(error),
        };
        match head_order {
            Comparison::Before => return self.start_scan(),
            Comparison::After => return Step::reject(Reject::Discontinuity),
            Comparison::Equal => {}
            Comparison::Incomparable => return Step::reject(Reject::Comparison),
        }
        if let Some(target) = self.state.target.as_ref() {
            let reached = match Self::relation(comparisons, materialized, target, proof) {
                Ok(order) => order,
                Err(error) => return Step::reject(error),
            };
            if reached == Comparison::Before {
                self.state.stage = Stage::Unready;
                return Step::ok(Vec::new());
            }
        }
        if self
            .snapshot
            .as_ref()
            .is_some_and(|snapshot| !snapshot.attached)
        {
            return Step::reject(Reject::Stage);
        }
        if let Some(target) = self
            .snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.attach_head.as_ref())
        {
            let reached = match Self::relation(comparisons, materialized, target, proof) {
                Ok(order) => order,
                Err(error) => return Step::reject(error),
            };
            if reached == Comparison::Before {
                return self.start_scan();
            }
        }
        self.state.stage = Stage::Ready;
        self.ready = true;
        self.retries = 0;
        self.finish_snapshot()
    }

    /// Consumes one explicit input and returns the next bounded driver effects.
    #[expect(
        clippy::too_many_lines,
        reason = "the event match is one auditable sans-IO transition table; transition helpers are already split out"
    )]
    pub fn step(&mut self, event: Event) -> Step {
        match event {
            Event::StartSnapshot => self.start_snapshot(),
            Event::SnapshotHeld { op, receipt } => self.snapshot_held(op, receipt),
            Event::SnapshotOffered { op, offer } => self.snapshot_offered(op, *offer),
            Event::SnapshotOpened { op, charged_bytes } => self.snapshot_opened(op, charged_bytes),
            Event::SnapshotRead { op, chunk } => self.snapshot_read(op, chunk),
            Event::SnapshotWritten {
                op,
                index,
                through,
                charged_bytes,
            } => self.snapshot_written(op, index, through, charged_bytes),
            Event::SnapshotVerified { op, charged_bytes } => {
                self.snapshot_verified(op, charged_bytes)
            }
            Event::SnapshotBarrier {
                op,
                proof,
                comparisons,
            } => self.snapshot_barrier(op, proof, &comparisons),
            Event::SnapshotScanned { op, batch } => self.snapshot_scanned(op, *batch),
            Event::SnapshotApplied {
                op,
                through,
                charged_bytes,
            } => self.snapshot_applied(op, through, charged_bytes),
            Event::SnapshotSealed {
                op,
                through,
                payload_id,
                charged_bytes,
            } => self.snapshot_sealed(op, through, payload_id, charged_bytes),
            Event::SnapshotInstalled { op, receipt } => self.snapshot_installed(op, receipt),
            Event::SnapshotAttached {
                op,
                proof,
                comparisons,
            } => self.snapshot_attached(op, proof, &comparisons),
            Event::SnapshotCleaned { op } => self.snapshot_cleaned(op),
            Event::SnapshotDiscarded { op, disposition } => {
                self.snapshot_discarded(op, disposition)
            }
            Event::StartBootstrap => self.start_bootstrap(),
            Event::CheckpointLoaded {
                op,
                cursor,
                payload_id,
            } => self.checkpoint_loaded(op, cursor, payload_id),
            Event::CheckpointInstalled { op, receipt } => self.checkpoint_installed(op, receipt),
            Event::Resume { cursor } => {
                if self.snapshot_cleanup.is_some() {
                    return Step::reject(Reject::Stage);
                }
                if !matches!(
                    self.state.stage,
                    Stage::Unready
                        | Stage::Cancelled
                        | Stage::RetryExhausted
                        | Stage::NeedsSnapshot
                        | Stage::SnapshotAborted
                ) {
                    return Step::reject(Reject::Stage);
                }
                self.resume_state(cursor, true)
            }
            Event::Hint => {
                if matches!(
                    self.state.stage,
                    Stage::Cancelled
                        | Stage::RetryWait
                        | Stage::RetryExhausted
                        | Stage::NeedsSnapshot
                        | Stage::SnapshotAborted
                        | Stage::IrrecoverableGap
                ) {
                    Step::reject(Reject::Stage)
                } else {
                    self.request_tail()
                }
            }
            Event::Demand { cursor, comparison } => {
                if matches!(
                    self.state.stage,
                    Stage::Cancelled
                        | Stage::RetryWait
                        | Stage::RetryExhausted
                        | Stage::NeedsSnapshot
                        | Stage::SnapshotAborted
                        | Stage::IrrecoverableGap
                ) {
                    return Step::reject(Reject::Stage);
                }
                if let Err(e) = cursor.validate(&self.scope, self.config.max_cursor_bytes) {
                    return Step::reject(Reject::Identity(e));
                }
                if self
                    .state
                    .materialized
                    .as_ref()
                    .is_some_and(|current| current.history != cursor.history)
                {
                    return Step::reject(Reject::History);
                }
                if let Some(existing) = &self.state.target {
                    let Some(proof) = self.proof.as_ref() else {
                        return Step::reject(Reject::Comparison);
                    };
                    let Some(order) = comparison
                        .as_ref()
                        .and_then(|c| c.for_operands(existing, &cursor, &proof.id))
                    else {
                        return Step::reject(Reject::Comparison);
                    };
                    if existing.history != cursor.history || order == Comparison::Incomparable {
                        return Step::reject(Reject::History);
                    }
                    if order == Comparison::Before {
                        self.state.target = Some(cursor);
                    }
                } else {
                    self.state.target = Some(cursor);
                }
                self.request_tail()
            }
            Event::Tick(now) => {
                if now < self.now {
                    return Step::reject(Reject::Discontinuity);
                }
                self.now = now;
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
            Event::Tail {
                op,
                proof,
                comparisons,
            } => {
                if !self.matches(op, Stage::CheckingTail) {
                    return Step::reject(Reject::StaleOperation);
                }
                if let Err(e) = proof.validate(&self.scope, self.config.max_cursor_bytes) {
                    return Step::reject(Reject::Identity(e));
                }
                let Some(due) = self.now.0.checked_add(self.config.tail_check_ms) else {
                    self.state.stage = Stage::Unready;
                    return Step::reject(Reject::Exhausted);
                };
                let Some(materialized) = self.state.materialized.as_ref() else {
                    self.state.stage = match self.mode {
                        Mode::StateSync => Stage::NeedsSnapshot,
                        Mode::EventComplete => Stage::IrrecoverableGap,
                    };
                    self.outstanding = None;
                    self.operation_due = None;
                    if self.mode == Mode::StateSync && self.config.snapshot.is_some() {
                        return self.start_snapshot();
                    }
                    return Step::ok(vec![match self.mode {
                        Mode::StateSync => Effect::NeedsSnapshot,
                        Mode::EventComplete => Effect::IrrecoverableGap,
                    }]);
                };
                if self.snapshot.is_some() && materialized.history != proof.head.history {
                    return self.abort_snapshot();
                }
                let orders = (
                    Self::relation(&comparisons, materialized, &proof.head, &proof),
                    Self::relation(&comparisons, materialized, &proof.retained_from, &proof),
                    Self::relation(&comparisons, &proof.retained_from, &proof.head, &proof),
                );
                let (Ok(to_head), Ok(to_low), Ok(low_to_head)) = orders else {
                    return Step::reject(
                        orders
                            .0
                            .err()
                            .or(orders.1.err())
                            .or(orders.2.err())
                            .unwrap_or(Reject::Comparison),
                    );
                };
                if to_head == Comparison::After || low_to_head == Comparison::After {
                    return Step::reject(Reject::Discontinuity);
                }
                self.tail_due = Time(due);
                self.outstanding = None;
                self.operation_due = None;
                self.state.head = Some(proof.head.clone());
                self.proof = Some(proof);
                let mut effects = vec![Effect::ArmTimer(
                    self.next_deadline().unwrap_or(self.tail_due),
                )];
                let hinted_during_check = self.pending_tail;
                self.pending_tail = false;
                if to_low == Comparison::Before {
                    if self.snapshot.is_some() {
                        return self.abort_snapshot();
                    }
                    if self.live_attachment_attempt.is_some() {
                        self.state.stage = Stage::NeedsSnapshot;
                        effects.extend(self.cancel_snapshot_resources());
                        return Step::ok(effects);
                    }
                    self.state.stage = match self.mode {
                        Mode::StateSync => Stage::NeedsSnapshot,
                        Mode::EventComplete => Stage::IrrecoverableGap,
                    };
                    effects.push(match self.mode {
                        Mode::StateSync => Effect::NeedsSnapshot,
                        Mode::EventComplete => Effect::IrrecoverableGap,
                    });
                    if self.mode == Mode::StateSync && self.config.snapshot.is_some() {
                        effects.pop();
                        let start = self.start_snapshot();
                        effects.extend(start.effects);
                        return Step {
                            effects,
                            rejection: start.rejection,
                        };
                    }
                    return Step::ok(effects);
                }
                let next = if to_head == Comparison::Before {
                    self.start_scan()
                } else {
                    self.set_ready_or_wait(&comparisons)
                };
                effects.extend(next.effects);
                if hinted_during_check
                    && self.outstanding.is_none()
                    && self.state.stage == Stage::Ready
                {
                    self.pending_tail = false;
                    let followup = self.check_tail();
                    effects.extend(followup.effects);
                    return Step {
                        effects,
                        rejection: followup.rejection.or(next.rejection),
                    };
                }
                Step {
                    effects,
                    rejection: next.rejection,
                }
            }
            Event::Scanned { op, batch } => {
                if !self.matches(op, Stage::Scanning) {
                    return Step::reject(Reject::StaleOperation);
                }
                let Some(proof) = self.proof.as_ref() else {
                    return Step::reject(Reject::Stage);
                };
                let Some(from) = self.state.materialized.as_ref() else {
                    return Step::reject(Reject::Stage);
                };
                if batch.coverage.from != *from
                    || batch.coverage.proof != proof.id
                    || batch.coverage.through.history != proof.head.history
                    || batch.coverage.certificate.is_empty()
                    || batch.coverage.certificate.len() > self.config.max_cursor_bytes
                {
                    return Step::reject(Reject::Discontinuity);
                }
                if let Err(e) = batch
                    .coverage
                    .through
                    .validate(&self.scope, self.config.max_cursor_bytes)
                {
                    return Step::reject(Reject::Identity(e));
                }
                if batch.events == 0
                    || batch.events > self.config.max_batch_events
                    || batch.bytes == 0
                    || batch.bytes > self.config.max_batch_bytes
                {
                    return Step::reject(Reject::Backpressure);
                }
                if batch
                    .advance
                    .for_operands(from, &batch.coverage.through, &proof.id)
                    != Some(Comparison::Before)
                {
                    return Step::reject(Reject::Discontinuity);
                }
                let Some(order) =
                    batch
                        .end_to_head
                        .for_operands(&batch.coverage.through, &proof.head, &proof.id)
                else {
                    return Step::reject(Reject::Comparison);
                };
                if !matches!(order, Comparison::Before | Comparison::Equal) {
                    return Step::reject(Reject::Discontinuity);
                }
                let Ok(apply_op) = self.issue(Stage::Applying) else {
                    self.state.stage = Stage::Unready;
                    return Step::reject(Reject::Exhausted);
                };
                self.pending_batch = Some(*batch.clone());
                Step::ok(vec![
                    Effect::Apply {
                        op: apply_op,
                        batch,
                    },
                    Effect::ArmTimer(self.next_deadline().unwrap_or(self.tail_due)),
                ])
            }
            Event::Applied { op, receipt } => {
                if !self.matches(op, Stage::Applying) {
                    return Step::reject(Reject::StaleOperation);
                }
                let Some(batch) = self.pending_batch.take() else {
                    return Step::reject(Reject::Stage);
                };
                if receipt.through != batch.coverage.through {
                    self.pending_batch = Some(batch);
                    return Step::reject(Reject::Discontinuity);
                }
                self.state.materialized = Some(receipt.through.clone());
                if receipt.durable {
                    self.state.checkpoint = Some(receipt.through);
                }
                self.outstanding = None;
                self.operation_due = None;
                self.retries = 0;
                self.request_tail()
            }
            Event::Invalidated { op } => {
                if self.revoke_op != Some(op)
                    || op.session != self.session_id
                    || op.generation != self.state.generation
                {
                    return Step::reject(Reject::StaleOperation);
                }
                self.revoke_op = None;
                Step::ok(Vec::new())
            }
            Event::Authority(allowed) => {
                let changed = self.mode_authority != allowed;
                self.mode_authority = allowed;
                if allowed
                    && changed
                    && self.state.materialized.is_some()
                    && !matches!(
                        self.state.stage,
                        Stage::Cancelled
                            | Stage::RetryWait
                            | Stage::RetryExhausted
                            | Stage::NeedsSnapshot
                            | Stage::SnapshotAborted
                            | Stage::IrrecoverableGap
                    )
                {
                    self.request_tail()
                } else if allowed {
                    Step::ok(Vec::new())
                } else {
                    self.close_gate()
                }
            }
            Event::Failed { op } => {
                if self
                    .snapshot_cleanup
                    .as_ref()
                    .is_some_and(|cleanup| cleanup.op == op)
                {
                    return self.expire_snapshot_cleanup();
                }
                if self.outstanding.is_none_or(|(issued, _)| issued != op) {
                    return Step::reject(Reject::StaleOperation);
                }
                if self.snapshot.is_some() {
                    return self.abort_snapshot();
                }
                let closed = self.close_gate();
                if closed.rejection.is_some() {
                    return closed;
                }
                let mut effects = closed.effects;
                self.outstanding = None;
                self.operation_due = None;
                self.pending_batch = None;
                self.bootstrap_candidate = None;
                self.retry_target = if matches!(
                    self.state.stage,
                    Stage::LoadingCheckpoint | Stage::InstallingCheckpoint
                ) {
                    RetryTarget::Bootstrap
                } else {
                    RetryTarget::Tail
                };
                self.retries = self.retries.saturating_add(1);
                if self.retries >= self.config.max_retries {
                    self.state.stage = Stage::RetryExhausted;
                    self.retry_due = None;
                    return Step::ok(effects);
                }
                let Some(due) = self.now.0.checked_add(self.config.retry_ms) else {
                    self.state.stage = Stage::Unready;
                    return Step {
                        effects,
                        rejection: Some(Reject::Exhausted),
                    };
                };
                self.retry_due = Some(Time(due));
                self.state.stage = Stage::RetryWait;
                effects.push(Effect::ArmTimer(Time(due)));
                Step::ok(effects)
            }
            Event::Cancel | Event::Supersede => {
                let cleanup = self.cancel_snapshot_resources();
                let cancelled = matches!(event, Event::Cancel);
                let was_ready = self.ready;
                let Some(next) = self.state.generation.checked_add(1) else {
                    self.ready = false;
                    self.state.stage = Stage::RetryExhausted;
                    self.state.head = None;
                    self.state.target = None;
                    self.outstanding = None;
                    self.operation_due = None;
                    self.revoke_op = None;
                    self.pending_tail = false;
                    self.pending_batch = None;
                    self.bootstrap_candidate = None;
                    self.retry_target = RetryTarget::Tail;
                    self.proof = None;
                    self.retry_due = None;
                    let mut effects = if was_ready {
                        vec![Effect::RevokeServingUnconfirmed]
                    } else {
                        Vec::new()
                    };
                    effects.extend(cleanup);
                    return Step {
                        effects,
                        rejection: Some(Reject::Exhausted),
                    };
                };
                self.state.generation = next;
                self.state.stage = if cancelled {
                    Stage::Cancelled
                } else {
                    Stage::Unready
                };
                self.state.head = None;
                self.state.target = None;
                self.outstanding = None;
                self.operation_due = None;
                self.revoke_op = None;
                self.pending_tail = false;
                self.pending_batch = None;
                self.bootstrap_candidate = None;
                self.retry_target = RetryTarget::Tail;
                self.proof = None;
                self.retry_due = None;
                let mut closed = self.close_gate();
                closed.effects.extend(cleanup);
                closed
            }
        }
    }
}
