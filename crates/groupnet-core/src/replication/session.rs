//! Source-backed replica replay and snapshot session decisions.

mod ack_wait;
mod bootstrap;
#[cfg(test)]
mod exhaustion_tests;
mod idle;
#[cfg(test)]
mod idle_tests;
mod snapshot;
mod step;
mod subscription;
mod tick;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RetryTarget {
    Tail,
    Bootstrap,
    SubscriptionCurrentRead,
    SubscriptionReadback,
    SubscriptionBind,
    SubscriptionTail,
    SubscriptionAckRead,
    SubscriptionTerminalRead,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SessionProtocol {
    Replay,
    NamedSubscription,
}

use super::types::{
    Batch, Config, ConfigError, Effect, Mode, Operation, ReadDecision, Refusal, Reject, Stage,
    State, Step,
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
    freshness_due: Time,
    tail_due: Time,
    idle_state: idle::IdleState,
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
    ack_wait: Option<ack_wait::AckWait>,
    subscription: Option<subscription::Progress>,
    protocol: SessionProtocol,
    mode_authority: bool,
    ready: bool,
}

impl SessionEngine {
    /// Creates one unready replay session. The caller must validate a checkpoint
    /// through its application adapter before sending [`crate::replication::Event::Resume`].
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
            freshness_due: Time::ZERO,
            tail_due: Time::ZERO,
            idle_state: idle::IdleState::default(),
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
            ack_wait: None,
            subscription: None,
            protocol: SessionProtocol::Replay,
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

    /// Exact deadline of the current replay or snapshot operation. This is
    /// separate from the earliest timer across independent facets.
    #[must_use]
    pub fn operation_deadline(&self, op: Operation) -> Option<Time> {
        (self.outstanding.is_some_and(|(current, _)| current == op))
            .then_some(self.operation_due?)
            .map(|due| {
                self.snapshot
                    .as_ref()
                    .map_or(due, |snapshot| due.min(snapshot.total_due))
            })
    }

    /// Stable operation of the active named wait, distinct from each poll.
    #[must_use]
    pub fn ack_wait_operation(&self) -> Option<Operation> {
        self.ack_wait.as_ref().map(ack_wait::AckWait::operation)
    }

    /// Current exact fixed-set acknowledgement progress, when a wait is live.
    #[must_use]
    pub fn ack_wait_status(&self) -> Option<super::AckWaitOutcome> {
        self.ack_wait.as_ref().map(ack_wait::AckWait::status)
    }

    /// Whether a queued effect or response still names the live operation.
    /// Revocation has independent correlation and remains valid alongside a
    /// newer source operation until cancelled or acknowledged.
    #[must_use]
    pub fn accepts_operation(&self, op: Operation) -> bool {
        self.revoke_op == Some(op)
            || self.ack_wait.as_ref().is_some_and(|wait| {
                self.now < wait.deadline()
                    && (wait.operation() == op || wait.current_poll() == Some(op))
            })
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
        } else if (self.proof.is_some() || self.state.stage == Stage::Protected)
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
        let ordinary = match (
            ordinary,
            self.ack_wait.as_ref().map(ack_wait::AckWait::next_deadline),
        ) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, None) | (None, a) => a,
        };
        let ordinary = match (
            ordinary,
            self.snapshot.as_ref().map(|snapshot| snapshot.total_due),
        ) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, None) | (None, a) => a,
        };
        let ordinary = match (ordinary, self.subscriber_terminal_due()) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, None) | (None, a) => a,
        };
        match (ordinary, self.ready.then_some(self.freshness_due)) {
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
        if self.now >= self.freshness_due {
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
        if self.now >= self.freshness_due {
            return self.check_tail();
        }
        self.state.stage = Stage::Ready;
        self.ready = true;
        self.retries = 0;
        let mut ready = self.finish_snapshot();
        if self.config.idle.is_some() && self.freshness_due < self.tail_due {
            if let Some(due) = self.next_deadline() {
                ready.effects.push(Effect::ArmTimer(due));
            }
        }
        ready
    }
}
