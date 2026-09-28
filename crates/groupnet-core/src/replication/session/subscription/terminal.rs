//! Conditional source tombstones and exact readback for named subscriptions.

use super::super::{RetryTarget, SessionEngine, SessionProtocol};
use super::Progress;
use crate::Time;
use crate::replication::{
    Effect, IdentityError, Mode, Operation, Reject, SourceSubscriberState, Stage, Step,
    SubscriberKey, SubscriptionError, SubscriptionLimits, TerminalReason, TerminalReceipt,
    TerminalRequest,
};

#[derive(Clone, Debug)]
pub(super) struct TerminalProgress {
    pub(super) request_id: Vec<u8>,
    pub(super) due: Time,
    pub(super) request: Option<TerminalRequest>,
    pub(super) receipt: Option<TerminalReceipt>,
}

impl SessionEngine {
    pub(in crate::replication::session) fn start_detached_subscriber_terminal(
        &mut self,
        key: SubscriberKey,
        request_id: Vec<u8>,
        due: Time,
        limits: SubscriptionLimits,
    ) -> Step {
        if self.mode != Mode::EventComplete
            || self.subscription.is_some()
            || self.state.stage != Stage::Unready
            || self.config.snapshot.is_some()
            || due <= self.now
        {
            return Step::reject(Reject::Stage);
        }
        if key.scope != self.scope {
            return Step::reject(Reject::Subscription(SubscriptionError::Identity(
                IdentityError::WrongScope,
            )));
        }
        if !limits.valid()
            || key.subscriber.name.is_empty()
            || key.subscriber.name.len() > limits.max_subscriber_bytes
            || request_id.is_empty()
            || request_id.len() > limits.max_request_bytes
        {
            return Step::reject(Reject::Subscription(SubscriptionError::Bounds));
        }
        let Ok(op) = self.issue(Stage::ReadingCurrentSubscriber) else {
            return Step::reject(Reject::Exhausted);
        };
        self.operation_due = self.operation_due.map(|operation| operation.min(due));
        self.subscription = Some(Progress {
            request: None,
            resume: None,
            detached_key: Some(key.clone()),
            limits,
            registration: None,
            sink: None,
            source_ack: None,
            pending_ack: None,
            terminal: Some(TerminalProgress {
                request_id,
                due,
                request: None,
                receipt: None,
            }),
        });
        self.protocol = SessionProtocol::NamedSubscription;
        Step::ok(vec![
            Effect::ReadCurrentSubscriber { op, key },
            Effect::ArmTimer(self.operation_due.expect("issued deadline")),
        ])
    }

    pub(in crate::replication::session) fn detached_subscriber_current_read(
        &mut self,
        op: Operation,
        state: Option<SourceSubscriberState>,
    ) -> Step {
        if !self.matches(op, Stage::ReadingCurrentSubscriber) {
            return Step::reject(Reject::StaleOperation);
        }
        let Some(progress) = self.subscription.as_ref() else {
            return Step::reject(Reject::Stage);
        };
        let (Some(key), Some(terminal)) =
            (progress.detached_key.as_ref(), progress.terminal.as_ref())
        else {
            return Step::reject(Reject::Stage);
        };
        let Some(state) = state else {
            self.state.stage = Stage::IrrecoverableGap;
            self.outstanding = None;
            self.operation_due = None;
            return Step::reject(Reject::Subscription(SubscriptionError::HistoryUnavailable));
        };
        if let Err(error) =
            state.validate_terminal_target(key, self.config.max_cursor_bytes, progress.limits)
        {
            if error == SubscriptionError::HistoryUnavailable {
                self.state.stage = Stage::IrrecoverableGap;
                self.outstanding = None;
                self.operation_due = None;
            }
            return Step::reject(Reject::Subscription(error));
        }
        let request = TerminalRequest {
            key: key.clone(),
            epoch: state.epoch,
            acknowledged: state.acknowledged.clone(),
            request_id: terminal.request_id.clone(),
            reason: TerminalReason::Unsubscribed,
        };
        if let Err(error) = request.validate(self.config.max_cursor_bytes, progress.limits) {
            return Step::reject(Reject::Subscription(error));
        }
        if let Some(progress) = self.subscription.as_mut() {
            progress.source_ack = Some(state.acknowledged);
            if let Some(terminal) = progress.terminal.as_mut() {
                terminal.request = Some(request);
            }
        }
        self.outstanding = None;
        self.operation_due = None;
        self.retry_due = None;
        self.retries = 0;
        self.commit_subscriber_terminal()
    }

    /// Exact durable source tombstone, only after the source confirmed it.
    #[must_use]
    pub fn subscription_terminal(&self) -> Option<&TerminalReceipt> {
        self.subscription
            .as_ref()?
            .terminal
            .as_ref()?
            .receipt
            .as_ref()
    }

    pub(in crate::replication::session) fn begin_subscriber_terminal(
        &mut self,
        request_id: Vec<u8>,
        due: Time,
    ) -> Step {
        if self.mode != Mode::EventComplete
            || self.protocol != SessionProtocol::NamedSubscription
            || due <= self.now
            || !matches!(
                self.state.stage,
                Stage::BindingSink
                    | Stage::Protected
                    | Stage::CheckingSubscriberTail
                    | Stage::ScanningSubscriber
                    | Stage::ApplyingSubscriber
                    | Stage::AckingSubscriber
                    | Stage::ReadingSubscriberAck
                    | Stage::RetryWait
            )
        {
            return Step::reject(Reject::Stage);
        }
        let Some(progress) = self.subscription.as_ref() else {
            return Step::reject(Reject::Stage);
        };
        if progress.registration.is_none() || progress.source_ack.is_none() {
            return Step::reject(Reject::Stage);
        }
        if progress.terminal.is_some()
            || request_id.is_empty()
            || request_id.len() > progress.limits.max_request_bytes
        {
            return Step::reject(Reject::Subscription(SubscriptionError::Bounds));
        }
        if let Some(progress) = self.subscription.as_mut() {
            progress.terminal = Some(TerminalProgress {
                request_id,
                due,
                request: None,
                receipt: None,
            });
        }
        self.retries = 0;
        if self.state.stage == Stage::Protected
            || (self.state.stage == Stage::RetryWait
                && self
                    .subscription
                    .as_ref()
                    .is_some_and(|p| p.pending_ack.is_none()))
        {
            self.commit_subscriber_terminal()
        } else {
            Step::ok(vec![Effect::ArmTimer(due)])
        }
    }

    pub(super) fn maybe_commit_subscriber_terminal(&mut self) -> Option<Step> {
        self.subscription
            .as_ref()
            .and_then(|p| p.terminal.as_ref())
            .filter(|terminal| terminal.receipt.is_none())
            .and_then(|_| {
                self.subscription
                    .as_ref()
                    .filter(|progress| progress.pending_ack.is_none())
            })?;
        Some(self.commit_subscriber_terminal())
    }

    fn terminal_due(&self) -> Option<Time> {
        self.subscription.as_ref()?.terminal.as_ref().map(|t| t.due)
    }

    fn clamp_terminal_operation(&mut self, due: Time) {
        self.operation_due = self.operation_due.map(|operation| operation.min(due));
    }

    fn commit_subscriber_terminal(&mut self) -> Step {
        let Some(progress) = self.subscription.as_ref() else {
            return Step::reject(Reject::Stage);
        };
        let Some(terminal) = progress.terminal.as_ref() else {
            return Step::reject(Reject::Stage);
        };
        if terminal.due <= self.now {
            return self.expire_subscriber_terminal();
        }
        let request = if let Some(request) = &terminal.request {
            request.clone()
        } else {
            let (Some(registration), Some(acknowledged)) =
                (progress.registration.as_ref(), progress.source_ack.as_ref())
            else {
                return Step::reject(Reject::Stage);
            };
            TerminalRequest {
                key: registration.key.clone(),
                epoch: registration.epoch.clone(),
                acknowledged: acknowledged.clone(),
                request_id: terminal.request_id.clone(),
                reason: TerminalReason::Unsubscribed,
            }
        };
        if let Err(error) = request.validate(self.config.max_cursor_bytes, progress.limits) {
            return Step::reject(Reject::Subscription(error));
        }
        let total_due = terminal.due;
        if let Some(terminal) = self.subscription.as_mut().and_then(|p| p.terminal.as_mut()) {
            terminal.request = Some(request.clone());
        }
        self.retry_due = None;
        self.pending_batch = None;
        let Ok(op) = self.issue(Stage::TerminatingSubscriber) else {
            return self.expire_subscriber_terminal();
        };
        self.clamp_terminal_operation(total_due);
        Step::ok(vec![
            Effect::CommitSubscriberTerminal {
                op,
                request: Box::new(request),
            },
            Effect::ArmTimer(self.operation_due.expect("issued deadline")),
        ])
    }

    fn read_subscriber_terminal(&mut self) -> Step {
        let Some(terminal) = self.subscription.as_ref().and_then(|p| p.terminal.as_ref()) else {
            return Step::reject(Reject::Stage);
        };
        let Some(request) = terminal.request.clone() else {
            return Step::reject(Reject::Stage);
        };
        if terminal.due <= self.now {
            return self.expire_subscriber_terminal();
        }
        let total_due = terminal.due;
        let Ok(op) = self.issue(Stage::ReadingSubscriberTerminal) else {
            return self.expire_subscriber_terminal();
        };
        self.clamp_terminal_operation(total_due);
        Step::ok(vec![
            Effect::ReadSubscriberTerminal {
                op,
                request: Box::new(request),
            },
            Effect::ArmTimer(self.operation_due.expect("issued deadline")),
        ])
    }

    pub(in crate::replication::session) fn subscriber_terminal_committed(
        &mut self,
        op: Operation,
        receipt: TerminalReceipt,
    ) -> Step {
        if !self.matches(op, Stage::TerminatingSubscriber)
            && !self.matches(op, Stage::ReadingSubscriberTerminal)
        {
            return Step::reject(Reject::StaleOperation);
        }
        let Some(request) = self
            .subscription
            .as_ref()
            .and_then(|p| p.terminal.as_ref())
            .and_then(|t| t.request.as_ref())
        else {
            return Step::reject(Reject::Stage);
        };
        if let Err(error) = receipt.validate_against(request) {
            return Step::reject(Reject::Subscription(error));
        }
        if let Some(terminal) = self.subscription.as_mut().and_then(|p| p.terminal.as_mut()) {
            terminal.receipt = Some(receipt);
        }
        self.outstanding = None;
        self.operation_due = None;
        self.retry_due = None;
        self.state.stage = Stage::TerminatedSubscriber;
        Step::ok(Vec::new())
    }

    pub(in crate::replication::session) fn subscriber_terminal_read(
        &mut self,
        op: Operation,
        receipt: Option<TerminalReceipt>,
    ) -> Step {
        if !self.matches(op, Stage::ReadingSubscriberTerminal) {
            return Step::reject(Reject::StaleOperation);
        }
        if let Some(receipt) = receipt {
            return self.subscriber_terminal_committed(op, receipt);
        }
        self.commit_subscriber_terminal()
    }

    pub(in crate::replication::session) fn fail_subscriber_terminal(
        &mut self,
        op: Operation,
    ) -> Option<Step> {
        let stage = self
            .outstanding
            .and_then(|(current, stage)| (current == op).then_some(stage))?;
        if !matches!(
            stage,
            Stage::TerminatingSubscriber | Stage::ReadingSubscriberTerminal
        ) {
            return None;
        }
        let Some(retries) = self.retries.checked_add(1) else {
            return Some(self.expire_subscriber_terminal());
        };
        if retries > self.config.max_retries
            || self.terminal_due().is_some_and(|due| due <= self.now)
        {
            return Some(self.expire_subscriber_terminal());
        }
        self.retries = retries;
        self.outstanding = None;
        self.operation_due = None;
        if stage == Stage::TerminatingSubscriber {
            return Some(self.read_subscriber_terminal());
        }
        let retry_at = self
            .now
            .0
            .checked_add(self.config.retry_ms)
            .map(Time)
            .and_then(|retry| self.terminal_due().map(|due| retry.min(due)));
        let Some(retry_at) = retry_at else {
            return Some(self.expire_subscriber_terminal());
        };
        self.retry_due = Some(retry_at);
        self.retry_target = RetryTarget::SubscriptionTerminalRead;
        self.state.stage = Stage::RetryWait;
        Some(Step::ok(vec![Effect::ArmTimer(retry_at)]))
    }

    pub(in crate::replication::session) fn retry_subscriber_terminal(&mut self) -> Step {
        self.read_subscriber_terminal()
    }

    pub(in crate::replication::session) fn expire_subscriber_terminal(&mut self) -> Step {
        self.state.stage = Stage::RetryExhausted;
        self.outstanding = None;
        self.operation_due = None;
        self.retry_due = None;
        Step::ok(Vec::new())
    }

    pub(in crate::replication::session) fn subscriber_terminal_due(&self) -> Option<Time> {
        if matches!(
            self.state.stage,
            Stage::TerminatedSubscriber
                | Stage::RetryExhausted
                | Stage::Cancelled
                | Stage::IrrecoverableGap
        ) {
            None
        } else {
            self.terminal_due()
        }
    }
}
