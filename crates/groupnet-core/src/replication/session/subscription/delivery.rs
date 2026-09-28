//! Source-protected delivery: durable sink effects precede source ack.

use super::SessionEngine;
use crate::Time;
use crate::replication::{
    Batch, BoundComparison, CommitSubscriberAck, Comparison, DurableDeliveryReceipt, Effect, Mode,
    Operation, Reject, SourceProof, Stage, Step, SubscriberAckReceipt,
};

impl SessionEngine {
    pub(in crate::replication::session) fn poll_subscriber(&mut self) -> Step {
        if self.mode != Mode::EventComplete || self.state.stage != Stage::Protected {
            return Step::reject(Reject::Stage);
        }
        let Some(progress) = self.subscription.as_ref() else {
            return Step::reject(Reject::Stage);
        };
        let (Some(registration), Some(from), Some(_sink)) = (
            progress.registration.clone(),
            progress.source_ack.clone(),
            progress.sink.as_ref(),
        ) else {
            return Step::reject(Reject::Stage);
        };
        let Ok(op) = self.issue(Stage::CheckingSubscriberTail) else {
            self.state.stage = Stage::RetryExhausted;
            return Step::reject(Reject::Exhausted);
        };
        Step::ok(vec![
            Effect::CheckSubscriberTail {
                op,
                registration: Box::new(registration),
                from,
            },
            Effect::ArmTimer(self.operation_due.expect("issued deadline")),
        ])
    }

    pub(in crate::replication::session) fn subscriber_tail(
        &mut self,
        op: Operation,
        proof: SourceProof,
        comparisons: &[BoundComparison],
    ) -> Step {
        if !self.matches(op, Stage::CheckingSubscriberTail) {
            return Step::reject(Reject::StaleOperation);
        }
        if let Err(error) = proof.validate(&self.scope, self.config.max_cursor_bytes) {
            return Step::reject(Reject::Identity(error));
        }
        let Some(progress) = self.subscription.as_ref() else {
            return Step::reject(Reject::Stage);
        };
        let Some(from) = progress.source_ack.as_ref() else {
            return Step::reject(Reject::Stage);
        };
        let orders = (
            Self::relation(comparisons, &proof.retained_from, from, &proof),
            Self::relation(comparisons, from, &proof.head, &proof),
            Self::relation(comparisons, &proof.retained_from, &proof.head, &proof),
        );
        let (Ok(retained_to_ack), Ok(ack_to_head), Ok(retained_to_head)) = orders else {
            return Step::reject(
                orders
                    .0
                    .err()
                    .or(orders.1.err())
                    .or(orders.2.err())
                    .unwrap_or(Reject::Comparison),
            );
        };
        if retained_to_ack == Comparison::After {
            self.outstanding = None;
            self.operation_due = None;
            self.state.stage = Stage::IrrecoverableGap;
            return Step::ok(vec![Effect::IrrecoverableGap]);
        }
        if ack_to_head == Comparison::After || retained_to_head == Comparison::After {
            return Step::reject(Reject::Discontinuity);
        }
        self.state.head = Some(proof.head.clone());
        self.proof = Some(proof);
        self.outstanding = None;
        self.operation_due = None;
        if let Some(terminal) = self.maybe_commit_subscriber_terminal() {
            return terminal;
        }
        if ack_to_head == Comparison::Before {
            self.scan_subscriber()
        } else {
            self.retries = 0;
            let Some(due) = self.now.0.checked_add(self.config.tail_check_ms) else {
                self.state.stage = Stage::RetryExhausted;
                return Step::reject(Reject::Exhausted);
            };
            self.tail_due = Time(due);
            self.state.stage = Stage::Protected;
            Step::ok(vec![Effect::ArmTimer(self.tail_due)])
        }
    }

    fn scan_subscriber(&mut self) -> Step {
        let Some(progress) = self.subscription.as_ref() else {
            return Step::reject(Reject::Stage);
        };
        let (Some(registration), Some(from)) =
            (progress.registration.clone(), progress.source_ack.clone())
        else {
            return Step::reject(Reject::Stage);
        };
        let Ok(op) = self.issue(Stage::ScanningSubscriber) else {
            self.state.stage = Stage::RetryExhausted;
            return Step::reject(Reject::Exhausted);
        };
        Step::ok(vec![
            Effect::ScanSubscriber {
                op,
                registration: Box::new(registration),
                from,
                max_events: self.config.max_batch_events,
                max_bytes: self.config.max_batch_bytes,
            },
            Effect::ArmTimer(self.operation_due.expect("issued deadline")),
        ])
    }

    pub(in crate::replication::session) fn subscriber_scanned(
        &mut self,
        op: Operation,
        batch: Box<Batch>,
    ) -> Step {
        if !self.matches(op, Stage::ScanningSubscriber) {
            return Step::reject(Reject::StaleOperation);
        }
        let Some(progress) = self.subscription.as_ref() else {
            return Step::reject(Reject::Stage);
        };
        let (Some(from), Some(registration), Some(sink), Some(proof)) = (
            progress.source_ack.as_ref(),
            progress.registration.as_ref(),
            progress.sink.as_ref(),
            self.proof.as_ref(),
        ) else {
            return Step::reject(Reject::Stage);
        };
        if batch.coverage.from != *from
            || batch.coverage.proof != proof.id
            || batch.coverage.certificate.is_empty()
            || batch.coverage.certificate.len() > self.config.max_cursor_bytes
            || batch.coverage.through.history != registration.epoch.history
        {
            return Step::reject(Reject::Discontinuity);
        }
        if let Err(error) = batch
            .coverage
            .through
            .validate(&self.scope, self.config.max_cursor_bytes)
        {
            return Step::reject(Reject::Identity(error));
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
            || !matches!(
                batch
                    .end_to_head
                    .for_operands(&batch.coverage.through, &proof.head, &proof.id),
                Some(Comparison::Before | Comparison::Equal)
            )
        {
            return Step::reject(Reject::Discontinuity);
        }
        let registration = registration.clone();
        let previous_sink = sink.cursor.clone();
        let Ok(next) = self.issue(Stage::ApplyingSubscriber) else {
            self.state.stage = Stage::RetryExhausted;
            return Step::reject(Reject::Exhausted);
        };
        self.pending_batch = Some(*batch.clone());
        Step::ok(vec![
            Effect::ApplySubscriberBatch {
                op: next,
                registration: Box::new(registration),
                batch,
                previous_sink,
            },
            Effect::ArmTimer(self.operation_due.expect("issued deadline")),
        ])
    }

    pub(in crate::replication::session) fn subscriber_applied(
        &mut self,
        op: Operation,
        receipt: DurableDeliveryReceipt,
        through_to_sink: &BoundComparison,
        previous_to_sink: &BoundComparison,
    ) -> Step {
        if !self.matches(op, Stage::ApplyingSubscriber) {
            return Step::reject(Reject::StaleOperation);
        }
        let Some(batch) = self.pending_batch.as_ref() else {
            return Step::reject(Reject::Stage);
        };
        let Some(progress) = self.subscription.as_ref() else {
            return Step::reject(Reject::Stage);
        };
        let (Some(registration), Some(sink), Some(from), Some(proof)) = (
            progress.registration.as_ref(),
            progress.sink.as_ref(),
            progress.source_ack.as_ref(),
            self.proof.as_ref(),
        ) else {
            return Step::reject(Reject::Stage);
        };
        if let Err(error) = receipt.validate_against(
            op,
            registration,
            &sink.cursor,
            batch,
            self.config.max_cursor_bytes,
            progress.limits,
        ) {
            return Step::reject(Reject::Subscription(error));
        }
        let delivered_order =
            through_to_sink.for_operands(&batch.coverage.through, &receipt.sink_cursor, &proof.id);
        let previous_order =
            previous_to_sink.for_operands(&sink.cursor, &receipt.sink_cursor, &proof.id);
        if !matches!(
            delivered_order,
            Some(Comparison::Before | Comparison::Equal)
        ) || !matches!(previous_order, Some(Comparison::Before | Comparison::Equal))
            || !matches!(
                (delivered_order, previous_order),
                (Some(Comparison::Equal), _) | (_, Some(Comparison::Equal))
            )
        {
            return Step::reject(Reject::Comparison);
        }
        let request = CommitSubscriberAck {
            key: registration.key.clone(),
            epoch: registration.epoch.clone(),
            previous: from.clone(),
            through: batch.coverage.through.clone(),
            proof: proof.id.clone(),
            request_id: receipt.ack_request_id,
        };
        if let Some(progress) = self.subscription.as_mut() {
            if let Some(sink) = progress.sink.as_mut() {
                sink.cursor = receipt.sink_cursor.clone();
            }
            progress.pending_ack = Some(request.clone());
        }
        self.state.materialized = Some(receipt.sink_cursor.clone());
        self.state.checkpoint = Some(receipt.sink_cursor);
        self.pending_batch = None;
        let Ok(next) = self.issue(Stage::AckingSubscriber) else {
            self.state.stage = Stage::RetryExhausted;
            return Step::reject(Reject::Exhausted);
        };
        Step::ok(vec![
            Effect::CommitSubscriberAck {
                op: next,
                request: Box::new(request),
            },
            Effect::ArmTimer(self.operation_due.expect("issued deadline")),
        ])
    }

    pub(in crate::replication::session) fn subscriber_acked(
        &mut self,
        op: Operation,
        receipt: &SubscriberAckReceipt,
        stage: Stage,
    ) -> Step {
        if !self.matches(op, stage) {
            return Step::reject(Reject::StaleOperation);
        }
        let Some(progress) = self.subscription.as_ref() else {
            return Step::reject(Reject::Stage);
        };
        let Some(request) = progress.pending_ack.clone() else {
            return Step::reject(Reject::Stage);
        };
        if let Err(error) = receipt.validate_against(&request) {
            return Step::reject(Reject::Subscription(error));
        }
        if let Some(progress) = self.subscription.as_mut() {
            progress.source_ack = Some(request.through.clone());
            progress.pending_ack = None;
        }
        self.outstanding = None;
        self.operation_due = None;
        self.retries = 0;
        self.state.stage = Stage::Protected;
        if let Some(terminal) = self.maybe_commit_subscriber_terminal() {
            return terminal;
        }
        self.poll_subscriber()
    }

    pub(in crate::replication::session) fn subscriber_ack_read(
        &mut self,
        op: Operation,
        receipt: Option<SubscriberAckReceipt>,
    ) -> Step {
        if !self.matches(op, Stage::ReadingSubscriberAck) {
            return Step::reject(Reject::StaleOperation);
        }
        if let Some(receipt) = receipt {
            return self.subscriber_acked(op, &receipt, Stage::ReadingSubscriberAck);
        }
        let Some(request) = self
            .subscription
            .as_ref()
            .and_then(|progress| progress.pending_ack.clone())
        else {
            return Step::reject(Reject::Stage);
        };
        let Ok(next) = self.issue(Stage::AckingSubscriber) else {
            self.state.stage = Stage::RetryExhausted;
            return Step::reject(Reject::Exhausted);
        };
        Step::ok(vec![
            Effect::CommitSubscriberAck {
                op: next,
                request: Box::new(request),
            },
            Effect::ArmTimer(self.operation_due.expect("issued deadline")),
        ])
    }

    pub(in crate::replication::session) fn fail_subscription_delivery(
        &mut self,
        op: Operation,
    ) -> Option<Step> {
        let stage = self
            .outstanding
            .and_then(|(current, stage)| (current == op).then_some(stage))?;
        let target = match stage {
            Stage::CheckingSubscriberTail
            | Stage::ScanningSubscriber
            | Stage::ApplyingSubscriber => super::super::RetryTarget::SubscriptionTail,
            Stage::AckingSubscriber | Stage::ReadingSubscriberAck => {
                super::super::RetryTarget::SubscriptionAckRead
            }
            _ => return None,
        };
        self.pending_batch = None;
        let Some(retries) = self.retries.checked_add(1) else {
            self.state.stage = Stage::RetryExhausted;
            self.outstanding = None;
            self.operation_due = None;
            return Some(Step::reject(Reject::Exhausted));
        };
        if retries > self.config.max_retries {
            self.state.stage = Stage::RetryExhausted;
            self.outstanding = None;
            self.operation_due = None;
            return Some(Step::ok(Vec::new()));
        }
        let Some(due) = self.now.0.checked_add(self.config.retry_ms) else {
            self.state.stage = Stage::RetryExhausted;
            self.outstanding = None;
            self.operation_due = None;
            return Some(Step::reject(Reject::Exhausted));
        };
        self.retries = retries;
        self.retry_due = Some(Time(due));
        self.retry_target = target;
        self.state.stage = Stage::RetryWait;
        self.outstanding = None;
        self.operation_due = None;
        Some(Step::ok(vec![Effect::ArmTimer(Time(due))]))
    }

    pub(in crate::replication::session) fn retry_subscription_delivery(&mut self) -> Step {
        match self.retry_target {
            super::super::RetryTarget::SubscriptionTail => {
                self.state.stage = Stage::Protected;
                if let Some(terminal) = self.maybe_commit_subscriber_terminal() {
                    return terminal;
                }
                self.poll_subscriber()
            }
            super::super::RetryTarget::SubscriptionAckRead => {
                let Some(request) = self
                    .subscription
                    .as_ref()
                    .and_then(|progress| progress.pending_ack.clone())
                else {
                    return Step::reject(Reject::Stage);
                };
                let Ok(op) = self.issue(Stage::ReadingSubscriberAck) else {
                    self.state.stage = Stage::RetryExhausted;
                    return Step::reject(Reject::Exhausted);
                };
                Step::ok(vec![
                    Effect::ReadSubscriberAck {
                        op,
                        request: Box::new(request),
                    },
                    Effect::ArmTimer(self.operation_due.expect("issued deadline")),
                ])
            }
            _ => Step::reject(Reject::Stage),
        }
    }
}
