//! Top-level replication event transition table.

use super::{RetryTarget, SessionEngine, SessionProtocol};
use crate::Time;
use crate::replication::{Comparison, Effect, Event, Mode, Reject, Stage, Step};

impl SessionEngine {
    /// Consumes one explicit input and returns the next bounded driver effects.
    #[expect(
        clippy::too_many_lines,
        reason = "the event match is one auditable sans-IO transition table; transition helpers are already split out"
    )]
    pub fn step(&mut self, event: Event) -> Step {
        match event {
            Event::BeginSubscriberTerminal { request_id, due } => {
                self.begin_subscriber_terminal(request_id, due)
            }
            Event::SubscriberTerminalCommitted { op, receipt } => {
                self.subscriber_terminal_committed(op, *receipt)
            }
            Event::SubscriberTerminalRead { op, receipt } => {
                self.subscriber_terminal_read(op, receipt.map(|r| *r))
            }
            Event::PollSubscriber => self.poll_subscriber(),
            Event::SubscriberTail {
                op,
                proof,
                comparisons,
            } => self.subscriber_tail(op, proof, &comparisons),
            Event::SubscriberScanned { op, batch } => self.subscriber_scanned(op, batch),
            Event::SubscriberApplied {
                op,
                receipt,
                through_to_sink,
                previous_to_sink,
            } => self.subscriber_applied(op, *receipt, &through_to_sink, &previous_to_sink),
            Event::SubscriberAcked { op, receipt } => {
                self.subscriber_acked(op, &receipt, Stage::AckingSubscriber)
            }
            Event::SubscriberAckRead { op, receipt } => {
                self.subscriber_ack_read(op, receipt.map(|r| *r))
            }
            Event::ResumeSubscription { request, limits } => {
                self.resume_subscription(*request, limits)
            }
            Event::StartDetachedSubscriberTerminal {
                key,
                request_id,
                due,
                limits,
            } => self.start_detached_subscriber_terminal(key, request_id, due, limits),
            Event::CurrentSubscriberRead { op, state } => {
                self.current_subscriber_read(op, state.map(|s| *s))
            }
            Event::SubscriberRejected { op, error } => self.subscriber_rejected(op, error),
            Event::StartSubscription { request, limits } => {
                self.start_subscription(*request, limits)
            }
            Event::SubscriberRegistered { op, receipt } => self.subscriber_registered(op, *receipt),
            Event::SubscriberRegistrationRead { op, receipt } => {
                self.subscriber_registration_read(op, receipt.map(|r| *r))
            }
            Event::SinkEpochBound {
                op,
                checkpoint,
                source_to_sink,
                sink_to_head,
            } => self.sink_epoch_bound(op, *checkpoint, &source_to_sink, &sink_to_head),
            Event::Activity => {
                if self.protocol == SessionProtocol::NamedSubscription {
                    Step::ok(Vec::new())
                } else {
                    self.activity()
                }
            }
            Event::StartAckWait { request, limits } => self.start_ack_wait(*request, limits),
            Event::AckObserved { evidence } => self.observe_ack(&evidence),
            Event::AckChecked { op } => self.ack_checked(op),
            Event::AckAuthorityLost { op } => self.ack_authority_lost(op),
            Event::CancelAckWait { op } => self.cancel_ack_wait(op),
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
            Event::StartBootstrap => {
                if self.protocol == SessionProtocol::NamedSubscription {
                    Step::reject(Reject::Stage)
                } else {
                    self.start_bootstrap()
                }
            }
            Event::CheckpointLoaded {
                op,
                cursor,
                payload_id,
            } => self.checkpoint_loaded(op, cursor, payload_id),
            Event::CheckpointInstalled { op, receipt } => self.checkpoint_installed(op, receipt),
            Event::Resume { cursor } => {
                if self.protocol == SessionProtocol::NamedSubscription
                    || self.snapshot_cleanup.is_some()
                {
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
                if self.protocol == SessionProtocol::NamedSubscription {
                    return if self.state.stage == Stage::Protected {
                        self.poll_subscriber()
                    } else {
                        Step::ok(Vec::new())
                    };
                }
                if matches!(
                    self.state.stage,
                    Stage::Cancelled
                        | Stage::RetryWait
                        | Stage::RetryExhausted
                        | Stage::Protected
                        | Stage::NeedsSnapshot
                        | Stage::SnapshotAborted
                        | Stage::IrrecoverableGap
                ) {
                    Step::reject(Reject::Stage)
                } else {
                    self.idle_state.force_hot_next();
                    self.request_tail()
                }
            }
            Event::Demand { cursor, comparison } => {
                if self.protocol == SessionProtocol::NamedSubscription {
                    return Step::reject(Reject::Stage);
                }
                if matches!(
                    self.state.stage,
                    Stage::Cancelled
                        | Stage::RetryWait
                        | Stage::RetryExhausted
                        | Stage::Protected
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
                self.idle_state.force_hot_next();
                self.request_tail()
            }
            Event::Tick(now) => {
                if now < self.now {
                    return Step::reject(Reject::Discontinuity);
                }
                self.now = now;
                let had_ack = self.ack_wait.is_some();
                let ack_effects = self.tick_ack_wait();
                let mut ordinary = self.tick_replay(now);
                let should_rearm =
                    had_ack && (!ack_effects.is_empty() || !ordinary.effects.is_empty());
                ordinary.effects.extend(ack_effects);
                if should_rearm && let Some(due) = self.next_deadline() {
                    ordinary.effects.push(Effect::ArmTimer(due));
                }
                ordinary
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
                let unchanged = to_head == Comparison::Equal
                    && proof.read_authority
                    && to_low != Comparison::Before
                    && !self.pending_tail
                    && self.snapshot.is_none()
                    && self.state.head.as_ref() == Some(&proof.head)
                    && self
                        .proof
                        .as_ref()
                        .is_some_and(|prior| prior.read_authority == proof.read_authority);
                if self.record_tail_schedule(unchanged).is_err() {
                    return self.idle_exhausted();
                }
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
                self.idle_state.force_hot_next();
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
                if self.protocol == SessionProtocol::NamedSubscription {
                    self.mode_authority = allowed;
                    return Step::ok(Vec::new());
                }
                let changed = self.mode_authority != allowed;
                self.mode_authority = allowed;
                if changed {
                    self.idle_state.force_hot_next();
                }
                if allowed
                    && changed
                    && self.state.materialized.is_some()
                    && !matches!(
                        self.state.stage,
                        Stage::Cancelled
                            | Stage::RetryWait
                            | Stage::RetryExhausted
                            | Stage::Protected
                            | Stage::NeedsSnapshot
                            | Stage::SnapshotAborted
                            | Stage::IrrecoverableGap
                    )
                {
                    self.request_tail()
                } else if allowed {
                    Step::ok(Vec::new())
                } else {
                    let mut closed = self.close_gate();
                    if self.config.idle.is_some()
                        && !closed.effects.is_empty()
                        && let Some(due) = self.next_deadline()
                    {
                        closed.effects.push(Effect::ArmTimer(due));
                    }
                    closed
                }
            }
            Event::Failed { op } => {
                if let Some(result) = self.fail_subscriber_terminal(op) {
                    return result;
                }
                if let Some(result) = self.fail_subscription_delivery(op) {
                    return result;
                }
                if let Some(result) = self.fail_subscription(op) {
                    return result;
                }
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
                self.idle_state.force_hot_next();
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
                let ack = self.cancel_ack_wait_for_generation();
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
                    self.subscription = None;
                    self.retry_target = RetryTarget::Tail;
                    self.proof = None;
                    self.idle_state.reset();
                    self.retry_due = None;
                    let mut effects = if was_ready {
                        vec![Effect::RevokeServingUnconfirmed]
                    } else {
                        Vec::new()
                    };
                    effects.extend(cleanup);
                    effects.extend(ack);
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
                self.subscription = None;
                self.retry_target = RetryTarget::Tail;
                self.proof = None;
                self.idle_state.reset();
                self.retry_due = None;
                let mut closed = self.close_gate();
                closed.effects.extend(cleanup);
                closed.effects.extend(ack);
                closed
            }
        }
    }
}
