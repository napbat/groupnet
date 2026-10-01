//! Source-certified named wait admission and finite evidence polls.

use std::sync::Arc;
use std::time::{Duration, Instant};

use groupnet_core::Time;
use groupnet_core::replication::{
    AckWaitError, AckWaitOutcome, AckWaitRequest, Event, Operation, Reject, RequiredSubscriber,
    Step,
};
use tokio::sync::oneshot;

use super::super::{Command, lock};
use super::{ActiveAck, Driver};
use crate::replication::ack_api::{
    AckObservation, AckSourceFailure, AckWaitStartError, NamedAckRequest,
};
use crate::replication::api::{ApplicationAdapter, SourceAdapter};
use crate::replication::snapshot_runtime::SnapshotMode;

impl<S, A, M> Driver<S, A, M>
where
    S: SourceAdapter,
    A: ApplicationAdapter<S::Position, S::Batch>,
    M: SnapshotMode<S, A>,
{
    pub(super) async fn dispatch_command(&mut self, command: Command) {
        match command {
            Command::StartAck {
                generation,
                request,
                deadline,
                reply,
            } => self.start_ack(generation, *request, deadline, reply).await,
            other => self.command(other),
        }
    }

    fn ack_slot_matches(&self, generation: u64, request_id: &[u8]) -> bool {
        let slot = lock(&self.shared.ack_slot);
        slot.matches(generation, request_id) && !slot.cancel_requested
    }

    fn finish_prestart(
        &self,
        generation: u64,
        request_id: &[u8],
        reply: oneshot::Sender<Result<AckWaitOutcome, AckWaitStartError>>,
        result: Result<AckWaitOutcome, AckWaitStartError>,
    ) {
        let mut slot = lock(&self.shared.ack_slot);
        if slot.matches(generation, request_id) {
            if let Ok(outcome) = &result
                && let Some(active) = slot.active.as_ref()
            {
                lock(&active.progress).terminal = Some(outcome.clone());
            }
            slot.active = None;
            slot.cancel_requested = false;
        }
        drop(slot);
        let _ = reply.send(result);
    }

    fn source_failure(error: AckSourceFailure) -> Result<AckWaitOutcome, AckWaitStartError> {
        match error {
            AckSourceFailure::Unsupported => Err(AckWaitStartError::Unsupported),
            AckSourceFailure::AuthorityLost => Ok(AckWaitOutcome::AuthorityLost),
            AckSourceFailure::Retryable => Err(AckWaitStartError::Backpressured),
            AckSourceFailure::Terminal => Err(AckWaitStartError::Invalid(AckWaitError::Roster)),
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "certification admission, deadline binding, and core start share one exact request lifecycle"
    )]
    async fn start_ack(
        &mut self,
        generation: u64,
        request: NamedAckRequest,
        deadline: Instant,
        reply: oneshot::Sender<Result<AckWaitOutcome, AckWaitStartError>>,
    ) {
        let id = request.request_id.clone();
        let waiting = request.roster.required.clone();
        if !self.ack_slot_matches(generation, &id) {
            self.finish_prestart(generation, &id, reply, Ok(AckWaitOutcome::Cancelled));
            return;
        }
        let Some(capability) = self.manager.ack.as_ref() else {
            self.finish_prestart(generation, &id, reply, Err(AckWaitStartError::Unsupported));
            return;
        };
        let limits = capability.limits;
        let source = Arc::clone(&capability.source);
        let Some(elapsed) = deadline.checked_duration_since(self.started) else {
            self.finish_prestart(
                generation,
                &id,
                reply,
                Ok(AckWaitOutcome::TimedOut(waiting)),
            );
            return;
        };
        let Ok(due_ms) = u64::try_from(elapsed.as_millis()) else {
            self.finish_prestart(
                generation,
                &id,
                reply,
                Err(AckWaitStartError::Invalid(AckWaitError::Deadline)),
            );
            return;
        };
        let Some(attempt) = Instant::now().checked_add(self.timeout()) else {
            self.finish_prestart(
                generation,
                &id,
                reply,
                Err(AckWaitStartError::Invalid(AckWaitError::Deadline)),
            );
            return;
        };
        let attempt_deadline = deadline.min(attempt);
        let Some(_permit) = Self::operation_permit(&self.manager, attempt_deadline).await else {
            self.finish_prestart(
                generation,
                &id,
                reply,
                Err(AckWaitStartError::Backpressured),
            );
            return;
        };
        let certified = tokio::time::timeout_at(
            tokio::time::Instant::from_std(attempt_deadline),
            source.certify(&request, limits),
        )
        .await;
        if !self.ack_slot_matches(generation, &id) {
            self.finish_prestart(generation, &id, reply, Ok(AckWaitOutcome::Cancelled));
            return;
        }
        match certified {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                self.finish_prestart(generation, &id, reply, Self::source_failure(error));
                return;
            }
            Err(_) => {
                let result = if Instant::now() >= deadline {
                    Ok(AckWaitOutcome::TimedOut(waiting))
                } else {
                    Err(AckWaitStartError::Backpressured)
                };
                self.finish_prestart(generation, &id, reply, result);
                return;
            }
        }
        self.tick();
        if Instant::now() >= deadline || self.logical_now() >= Time(due_ms) {
            self.finish_prestart(
                generation,
                &id,
                reply,
                Ok(AckWaitOutcome::TimedOut(waiting)),
            );
            return;
        }
        let core_request = AckWaitRequest {
            request_id: request.request_id,
            target: request.target,
            kind: request.kind,
            roster: request.roster,
            due: Time(due_ms),
        };
        let Step { effects, rejection } = self.engine.step(Event::StartAckWait {
            request: Box::new(core_request),
            limits,
        });
        if let Some(reason) = rejection {
            self.enqueue(effects);
            let error = match reason {
                Reject::Backpressure | Reject::Stage => AckWaitStartError::Backpressured,
                Reject::AckWait(error) => AckWaitStartError::Invalid(error),
                Reject::Exhausted => AckWaitStartError::Invalid(AckWaitError::Deadline),
                _ => AckWaitStartError::Invalid(AckWaitError::Roster),
            };
            self.finish_prestart(generation, &id, reply, Err(error));
            return;
        }
        let wait_op = self.engine.ack_wait_operation().or_else(|| {
            effects.iter().find_map(|effect| match effect {
                groupnet_core::replication::Effect::AckWaitFinished { op, .. } => Some(*op),
                _ => None,
            })
        });
        let Some(wait_op) = wait_op else {
            self.finish_prestart(generation, &id, reply, Err(AckWaitStartError::Closed));
            return;
        };
        self.ack = Some(ActiveAck {
            generation,
            request_id: id,
            wait_op,
            reply,
        });
        self.enqueue(effects);
        self.publish();
    }

    pub(super) fn cancel_ack(&mut self, generation: u64, request_id: &[u8]) {
        let mut slot = lock(&self.shared.ack_slot);
        if slot.matches(generation, request_id) {
            slot.cancel_requested = true;
        }
        drop(slot);
        self.cancel_ack_if_requested();
    }

    pub(super) fn cancel_ack_if_requested(&mut self) {
        let Some(active) = self.ack.as_ref() else {
            return;
        };
        let slot = lock(&self.shared.ack_slot);
        let cancel = slot.cancel_requested && slot.matches(active.generation, &active.request_id);
        let op = active.wait_op;
        drop(slot);
        if cancel && self.engine.ack_wait_operation() == Some(op) {
            self.step(Event::CancelAckWait { op });
        }
    }

    pub(super) fn finish_ack(&mut self, op: Operation, outcome: AckWaitOutcome) {
        let Some(active) = self.ack.take() else {
            return;
        };
        if active.wait_op != op {
            self.ack = Some(active);
            return;
        }
        let mut slot = lock(&self.shared.ack_slot);
        if slot.matches(active.generation, &active.request_id) {
            if let Some(current) = slot.active.as_ref() {
                lock(&current.progress).terminal = Some(outcome.clone());
            }
            slot.active = None;
            slot.cancel_requested = false;
        }
        drop(slot);
        let _ = active.reply.send(Ok(outcome));
    }

    pub(super) fn cancel_ack_local(&mut self) {
        if let Some(op) = self.ack.as_ref().map(|active| active.wait_op) {
            self.finish_ack(op, AckWaitOutcome::Cancelled);
        }
    }

    pub(super) fn record_ack_terminal(&self, op: Operation, outcome: AckWaitOutcome) {
        let Some(active) = self.ack.as_ref().filter(|active| active.wait_op == op) else {
            return;
        };
        let slot = lock(&self.shared.ack_slot);
        if slot.matches(active.generation, &active.request_id)
            && let Some(current) = slot.active.as_ref()
        {
            lock(&current.progress).terminal = Some(outcome);
        }
    }

    fn record_ack_progress(&self) {
        let Some(active) = self.ack.as_ref() else {
            return;
        };
        let Some(AckWaitOutcome::Pending(waiting)) = self.engine.ack_wait_status() else {
            return;
        };
        let slot = lock(&self.shared.ack_slot);
        if slot.matches(active.generation, &active.request_id)
            && let Some(current) = slot.active.as_ref()
        {
            lock(&current.progress).waiting = waiting;
        }
    }

    pub(super) async fn observe_ack(
        &mut self,
        op: Operation,
        request: Box<AckWaitRequest>,
        waiting: Vec<RequiredSubscriber>,
        due: Time,
    ) {
        if !self.engine.accepts_operation(op) {
            self.deadlines.remove(&op);
            return;
        }
        let Some(capability) = self.manager.ack.as_ref() else {
            self.ack_authority_lost();
            return;
        };
        let source = Arc::clone(&capability.source);
        let limits = capability.limits;
        let Some(deadline) = self.started.checked_add(Duration::from_millis(due.0)) else {
            self.ack_authority_lost();
            return;
        };
        let Some(_permit) = Self::operation_permit(&self.manager, deadline).await else {
            self.tick();
            return;
        };
        if !self.engine.accepts_operation(op) {
            self.deadlines.remove(&op);
            return;
        }
        let observed = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            source.observe(&request, op, &waiting, limits),
        )
        .await;
        self.tick();
        if !self.engine.accepts_operation(op) {
            self.deadlines.remove(&op);
            return;
        }
        match observed {
            Ok(Ok(AckObservation::Evidence(evidence))) => {
                if evidence.op != op {
                    self.ack_authority_lost();
                    return;
                }
                let Step { effects, rejection } = self.engine.step(Event::AckObserved { evidence });
                self.deadlines.remove(&op);
                if rejection.is_none() {
                    self.record_ack_progress();
                }
                self.enqueue(effects);
                if rejection.is_some_and(|reason| reason != Reject::StaleOperation) {
                    self.ack_authority_lost();
                }
                self.publish();
            }
            Ok(Ok(AckObservation::Pending) | Err(AckSourceFailure::Retryable)) => {
                self.step(Event::AckChecked { op });
            }
            Ok(
                Ok(AckObservation::AuthorityLost)
                | Err(
                    AckSourceFailure::AuthorityLost
                    | AckSourceFailure::Terminal
                    | AckSourceFailure::Unsupported,
                ),
            ) => self.ack_authority_lost(),
            Err(_) => self.step(Event::AckChecked { op }),
        }
    }

    fn ack_authority_lost(&mut self) {
        if let Some(op) = self.engine.ack_wait_operation() {
            self.step(Event::AckAuthorityLost { op });
        }
    }
}
