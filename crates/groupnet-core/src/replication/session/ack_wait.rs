//! Bounded named acknowledgement sidecar for a replay session.
//!
//! The owning engine supplies its ordinary operation token and clock sample.
//! This facet never interprets native cursor ordering, commits source events,
//! or changes the replay/read-authority state.

use crate::Time;

use super::super::ack_types::{
    AckEvidence, AckKind, AckTarget, AckWaitError, AckWaitLimits, AckWaitOutcome, AckWaitRequest,
    RequiredSubscriber,
};
use super::super::{Effect, Operation, Reject, Scope, Step};
use super::SessionEngine;

/// One bounded fixed-roster wait. The owning session permits at most one live
/// instance per scope and allocates `op` from its existing operation counter.
#[derive(Clone, Debug)]
pub(super) struct AckWait {
    op: Operation,
    request: AckWaitRequest,
    observed: Vec<bool>,
    terminal: Option<AckWaitOutcome>,
    poll_ms: u64,
    next_poll_due: Time,
    poll_op: Option<Operation>,
    poll_due: Option<Time>,
}

impl AckWait {
    pub(super) fn new(
        scope: &Scope,
        op: Operation,
        request: AckWaitRequest,
        limits: AckWaitLimits,
        now: Time,
    ) -> Result<Self, AckWaitError> {
        if op.session == 0 || op.generation == 0 || op.token == 0 {
            return Err(AckWaitError::Identity);
        }
        if limits.max_required == 0
            || limits.max_identity_bytes == 0
            || limits.max_certificate_bytes == 0
            || limits.max_metadata_bytes == 0
            || limits.max_wait_ms == 0
            || limits.poll_ms == 0
        {
            return Err(AckWaitError::Backpressure);
        }
        if request.due <= now || request.due.0 - now.0 > limits.max_wait_ms {
            return Err(AckWaitError::Deadline);
        }
        validate_target(&request.target, scope, limits.max_identity_bytes)?;
        if request.roster.target != request.target
            || request.roster.kind != request.kind
            || request.roster.policy_version == 0
        {
            return Err(AckWaitError::Roster);
        }
        if !matches!(
            (&request.kind, &request.target),
            (AckKind::Invalidated, AckTarget::Intent { .. })
                | (AckKind::Materialized, AckTarget::Cursor(_))
        ) {
            return Err(AckWaitError::Roster);
        }
        if request.roster.required.len() > limits.max_required {
            return Err(AckWaitError::Backpressure);
        }
        if request.request_id.is_empty()
            || request.request_id.len() > limits.max_identity_bytes
            || request.roster.certificate.is_empty()
            || request.roster.certificate.len() > limits.max_certificate_bytes
        {
            return Err(AckWaitError::Identity);
        }
        let mut bytes = request
            .request_id
            .len()
            .checked_add(request.roster.certificate.len())
            .and_then(|n| n.checked_add(target_bytes(&request.target)?))
            .and_then(|n| n.checked_add(target_bytes(&request.roster.target)?))
            .ok_or(AckWaitError::Backpressure)?;
        for (index, member) in request.roster.required.iter().enumerate() {
            if member.name.is_empty() || member.epoch.is_empty() || member.incarnation == 0 {
                return Err(AckWaitError::Identity);
            }
            if member.name.len() > limits.max_identity_bytes
                || member.epoch.len() > limits.max_identity_bytes
            {
                return Err(AckWaitError::Backpressure);
            }
            if request.roster.required[..index]
                .iter()
                .any(|prior| prior.name == member.name)
            {
                return Err(AckWaitError::Roster);
            }
            bytes = bytes
                .checked_add(member.name.len())
                .and_then(|n| n.checked_add(member.epoch.len()))
                .ok_or(AckWaitError::Backpressure)?;
        }
        if bytes > limits.max_metadata_bytes {
            return Err(AckWaitError::Backpressure);
        }
        let observed = vec![false; request.roster.required.len()];
        let terminal = observed.is_empty().then_some(AckWaitOutcome::Satisfied);
        Ok(Self {
            op,
            request,
            observed,
            terminal,
            poll_ms: limits.poll_ms,
            next_poll_due: now,
            poll_op: None,
            poll_due: None,
        })
    }

    pub(super) fn operation(&self) -> Operation {
        self.op
    }

    pub(super) fn deadline(&self) -> Time {
        self.request.due
    }

    pub(super) fn request(&self) -> &AckWaitRequest {
        &self.request
    }

    pub(super) fn is_terminal(&self) -> bool {
        self.terminal.is_some()
    }

    pub(super) fn next_deadline(&self) -> Time {
        self.poll_due
            .unwrap_or(self.next_poll_due)
            .min(self.request.due)
    }

    pub(super) fn poll(&mut self, op: Operation, now: Time, attempt_due: Time) -> bool {
        if self.terminal.is_some() || self.poll_op.is_some() || now < self.next_poll_due {
            return false;
        }
        self.poll_op = Some(op);
        self.poll_due = Some(attempt_due.min(self.request.due));
        true
    }

    pub(super) fn current_poll(&self) -> Option<Operation> {
        self.poll_op
    }

    pub(super) fn expire_poll(&mut self, now: Time) {
        if self.poll_due.is_some_and(|due| now >= due) {
            self.poll_op = None;
            self.poll_due = None;
            self.next_poll_due = now;
        }
    }

    pub(super) fn checked(&mut self, op: Operation, now: Time) -> Result<Time, AckWaitError> {
        self.tick(now);
        if self.terminal.is_some() {
            return Err(AckWaitError::Closed);
        }
        if self.poll_op != Some(op) || self.poll_due.is_some_and(|due| now >= due) {
            return Err(AckWaitError::Evidence);
        }
        self.poll_op = None;
        self.poll_due = None;
        self.next_poll_due = Time(now.0.saturating_add(self.poll_ms).min(self.request.due.0));
        Ok(self.next_poll_due)
    }

    pub(super) fn status(&self) -> AckWaitOutcome {
        self.terminal
            .clone()
            .unwrap_or_else(|| AckWaitOutcome::Pending(self.waiting()))
    }

    pub(super) fn observe(
        &mut self,
        evidence: &AckEvidence,
        now: Time,
    ) -> Result<(), AckWaitError> {
        self.tick(now);
        if self.terminal.is_some() {
            return Err(AckWaitError::Closed);
        }
        if self.poll_op != Some(evidence.op)
            || self.poll_due.is_some_and(|due| now >= due)
            || evidence.request_id != self.request.request_id
            || evidence.target != self.request.target
            || evidence.kind != self.request.kind
            || evidence.roster_certificate != self.request.roster.certificate
        {
            return Err(AckWaitError::Evidence);
        }
        let Some(index) = self
            .request
            .roster
            .required
            .iter()
            .position(|member| member == &evidence.subscriber)
        else {
            return Err(AckWaitError::Evidence);
        };
        if self.observed[index] {
            return Err(AckWaitError::Duplicate);
        }
        self.observed[index] = true;
        self.poll_op = None;
        self.poll_due = None;
        self.next_poll_due = now;
        if self.observed.iter().all(|accepted| *accepted) {
            self.terminal = Some(AckWaitOutcome::Satisfied);
        }
        Ok(())
    }

    pub(super) fn tick(&mut self, now: Time) {
        if self.terminal.is_none() && now >= self.request.due {
            self.terminal = Some(AckWaitOutcome::TimedOut(self.waiting()));
        }
    }

    pub(super) fn cancel(&mut self) {
        if self.terminal.is_none() {
            self.terminal = Some(AckWaitOutcome::Cancelled);
        }
    }

    pub(super) fn authority_lost(&mut self) {
        if self.terminal.is_none() {
            self.terminal = Some(AckWaitOutcome::AuthorityLost);
        }
    }

    fn waiting(&self) -> Vec<RequiredSubscriber> {
        self.request
            .roster
            .required
            .iter()
            .zip(&self.observed)
            .filter_map(|(member, accepted)| (!accepted).then_some(member.clone()))
            .collect()
    }
}

fn validate_target(target: &AckTarget, scope: &Scope, limit: usize) -> Result<(), AckWaitError> {
    scope.validate(limit).map_err(|_| AckWaitError::Identity)?;
    match target {
        AckTarget::Intent {
            scope: target_scope,
            history,
            id,
        } => {
            if target_scope != scope
                || history.source.is_empty()
                || history.source.len() > limit
                || id.is_empty()
                || id.len() > limit
            {
                return Err(AckWaitError::Identity);
            }
        }
        AckTarget::Cursor(cursor) => {
            cursor
                .validate(scope, limit)
                .map_err(|_| AckWaitError::Identity)?;
        }
    }
    Ok(())
}

fn target_bytes(target: &AckTarget) -> Option<usize> {
    let scope = target.scope();
    [
        scope.stream.group.len(),
        scope.stream.topic.len(),
        scope.stream.kind.len(),
        scope.partition.len(),
        target.history().source.len(),
        match target {
            AckTarget::Intent { id, .. } => id.len(),
            AckTarget::Cursor(cursor) => cursor.position.len(),
        },
    ]
    .into_iter()
    .try_fold(0usize, usize::checked_add)
}

impl SessionEngine {
    pub(super) fn start_ack_wait(
        &mut self,
        request: AckWaitRequest,
        limits: AckWaitLimits,
    ) -> Step {
        if self.ack_wait.is_some() {
            return Step::reject(Reject::Backpressure);
        }
        if self.state.stage == super::super::Stage::Cancelled {
            return Step::reject(Reject::Stage);
        }
        let provisional = Operation {
            session: self.session_id,
            generation: self.state.generation,
            token: self.next_token,
        };
        if self.next_token == 0 {
            return Step::reject(Reject::Exhausted);
        }
        let wait = match AckWait::new(&self.scope, provisional, request, limits, self.now) {
            Ok(wait) => wait,
            Err(error) => return Step::reject(Reject::AckWait(error)),
        };
        let Ok(op) = self.fresh_operation() else {
            return Step::reject(Reject::Exhausted);
        };
        debug_assert_eq!(op, wait.operation());
        self.ack_wait = Some(wait);
        if self.ack_wait.as_ref().is_some_and(AckWait::is_terminal) {
            return self.finish_ack_wait();
        }
        self.poll_ack_wait()
    }

    pub(super) fn observe_ack(&mut self, evidence: &AckEvidence) -> Step {
        let Some(wait) = self.ack_wait.as_mut() else {
            return Step::reject(Reject::StaleOperation);
        };
        if wait.current_poll() != Some(evidence.op) {
            return Step::reject(Reject::StaleOperation);
        }
        if let Err(error) = wait.observe(evidence, self.now) {
            return if wait.is_terminal() {
                self.finish_ack_wait()
            } else {
                Step::reject(Reject::AckWait(error))
            };
        }
        if wait.is_terminal() {
            self.finish_ack_wait()
        } else {
            self.poll_ack_wait()
        }
    }

    pub(super) fn ack_checked(&mut self, op: Operation) -> Step {
        let Some(wait) = self.ack_wait.as_mut() else {
            return Step::reject(Reject::StaleOperation);
        };
        if wait.current_poll() != Some(op) {
            return Step::reject(Reject::StaleOperation);
        }
        match wait.checked(op, self.now) {
            Ok(_) => Step::ok(
                self.next_deadline()
                    .map_or_else(Vec::new, |due| vec![Effect::ArmTimer(due)]),
            ),
            Err(error) => {
                if wait.is_terminal() {
                    self.finish_ack_wait()
                } else {
                    Step::reject(Reject::AckWait(error))
                }
            }
        }
    }

    pub(super) fn ack_authority_lost(&mut self, op: Operation) -> Step {
        let Some(wait) = self.ack_wait.as_mut() else {
            return Step::reject(Reject::StaleOperation);
        };
        if op != wait.operation() {
            return Step::reject(Reject::StaleOperation);
        }
        wait.authority_lost();
        self.finish_ack_wait()
    }

    pub(super) fn cancel_ack_wait(&mut self, op: Operation) -> Step {
        let Some(wait) = self.ack_wait.as_mut() else {
            return Step::reject(Reject::StaleOperation);
        };
        if op != wait.operation() {
            return Step::reject(Reject::StaleOperation);
        }
        wait.cancel();
        self.finish_ack_wait()
    }

    pub(super) fn tick_ack_wait(&mut self) -> Vec<Effect> {
        let Some(wait) = self.ack_wait.as_mut() else {
            return Vec::new();
        };
        wait.tick(self.now);
        if wait.is_terminal() {
            return self.finish_ack_wait().effects;
        }
        wait.expire_poll(self.now);
        if self.now >= wait.next_deadline() {
            return self.poll_ack_wait().effects;
        }
        Vec::new()
    }

    pub(super) fn cancel_ack_wait_for_generation(&mut self) -> Vec<Effect> {
        if let Some(wait) = self.ack_wait.as_mut() {
            wait.cancel();
            self.finish_ack_wait().effects
        } else {
            Vec::new()
        }
    }

    fn poll_ack_wait(&mut self) -> Step {
        let Some(wait) = self.ack_wait.as_ref() else {
            return Step::reject(Reject::Stage);
        };
        if wait.is_terminal() || wait.current_poll().is_some() || self.now < wait.next_deadline() {
            return Step::ok(Vec::new());
        }
        let Some(attempt_due) = self.now.0.checked_add(self.config.attempt_timeout_ms) else {
            self.ack_wait.as_mut().expect("active").authority_lost();
            return self.finish_ack_wait();
        };
        let Ok(poll_op) = self.fresh_operation() else {
            self.ack_wait.as_mut().expect("active").authority_lost();
            return self.finish_ack_wait();
        };
        let wait = self.ack_wait.as_mut().expect("active");
        let started = wait.poll(poll_op, self.now, Time(attempt_due));
        debug_assert!(started);
        let observe = Effect::ObserveNamedAcks {
            op: poll_op,
            request: Box::new(wait.request().clone()),
            waiting: wait.waiting(),
            due: wait.next_deadline(),
        };
        let mut effects = vec![observe];
        if let Some(due) = self.next_deadline() {
            effects.push(Effect::ArmTimer(due));
        }
        Step::ok(effects)
    }

    fn finish_ack_wait(&mut self) -> Step {
        let Some(wait) = self.ack_wait.take() else {
            return Step::reject(Reject::Stage);
        };
        let mut effects = vec![Effect::AckWaitFinished {
            op: wait.operation(),
            outcome: wait.status(),
        }];
        if let Some(due) = self.next_deadline() {
            effects.push(Effect::ArmTimer(due));
        }
        Step::ok(effects)
    }
}

#[cfg(test)]
#[path = "ack_wait_tests.rs"]
mod tests;
