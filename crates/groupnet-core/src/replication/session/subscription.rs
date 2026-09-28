//! Durable registration and sink-fence transitions before event delivery.

use super::{RetryTarget, SessionEngine, SessionProtocol};
use crate::Time;
use crate::replication::{
    BoundComparison, CommitSubscriberAck, Comparison, Cursor, Effect, FencedCheckpoint,
    IdentityError, Mode, Operation, RegisterReceipt, RegisterSubscriber, Reject, ResumeSubscriber,
    SourceSubscriberState, Stage, Step, SubscriptionError, SubscriptionLimits,
};

mod delivery;
#[cfg(test)]
mod delivery_tests;
mod terminal;
#[cfg(test)]
mod terminal_tests;

/// Bounded metadata for one registered `EventComplete` source/sink lineage.
#[derive(Clone, Debug)]
pub(super) struct Progress {
    request: Option<RegisterSubscriber>,
    resume: Option<ResumeSubscriber>,
    detached_key: Option<crate::replication::SubscriberKey>,
    limits: SubscriptionLimits,
    registration: Option<RegisterReceipt>,
    sink: Option<FencedCheckpoint>,
    source_ack: Option<Cursor>,
    pending_ack: Option<CommitSubscriberAck>,
    terminal: Option<terminal::TerminalProgress>,
}

impl SessionEngine {
    /// Exact durable source registration and sink fence, once both are bound.
    #[must_use]
    pub fn subscription_registration(&self) -> Option<(&RegisterReceipt, &FencedCheckpoint)> {
        let progress = self.subscription.as_ref()?;
        Some((progress.registration.as_ref()?, progress.sink.as_ref()?))
    }

    /// Exact source-protected durable ack, independent of the sink cursor.
    #[must_use]
    pub fn subscription_acknowledged(&self) -> Option<&Cursor> {
        self.subscription.as_ref()?.source_ack.as_ref()
    }

    pub(super) fn start_subscription(
        &mut self,
        request: RegisterSubscriber,
        limits: SubscriptionLimits,
    ) -> Step {
        if self.mode != Mode::EventComplete
            || self.subscription.is_some()
            || self.state.stage != Stage::Unready
            || self.config.snapshot.is_some()
        {
            return Step::reject(Reject::Stage);
        }
        if request.key.scope != self.scope {
            return Step::reject(Reject::Subscription(SubscriptionError::Identity(
                IdentityError::WrongScope,
            )));
        }
        if let Err(error) = request.validate(self.config.max_cursor_bytes, limits) {
            return Step::reject(Reject::Subscription(error));
        }
        let Ok(op) = self.issue(Stage::Registering) else {
            return Step::reject(Reject::Exhausted);
        };
        self.subscription = Some(Progress {
            request: Some(request.clone()),
            resume: None,
            detached_key: None,
            limits,
            registration: None,
            sink: None,
            source_ack: None,
            pending_ack: None,
            terminal: None,
        });
        self.protocol = SessionProtocol::NamedSubscription;
        Step::ok(vec![
            Effect::RegisterSubscriber {
                op,
                request: Box::new(request),
            },
            Effect::ArmTimer(self.operation_due.expect("issued deadline")),
        ])
    }

    pub(super) fn resume_subscription(
        &mut self,
        request: ResumeSubscriber,
        limits: SubscriptionLimits,
    ) -> Step {
        if self.mode != Mode::EventComplete
            || self.subscription.is_some()
            || self.state.stage != Stage::Unready
            || self.config.snapshot.is_some()
        {
            return Step::reject(Reject::Stage);
        }
        if request.key.scope != self.scope {
            return Step::reject(Reject::Subscription(SubscriptionError::Identity(
                IdentityError::WrongScope,
            )));
        }
        if let Err(error) = request.validate(self.config.max_cursor_bytes, limits) {
            return Step::reject(Reject::Subscription(error));
        }
        let Ok(op) = self.issue(Stage::ReadingCurrentSubscriber) else {
            return Step::reject(Reject::Exhausted);
        };
        let key = request.key.clone();
        self.subscription = Some(Progress {
            request: None,
            resume: Some(request),
            detached_key: None,
            limits,
            registration: None,
            sink: None,
            source_ack: None,
            pending_ack: None,
            terminal: None,
        });
        self.protocol = SessionProtocol::NamedSubscription;
        Step::ok(vec![
            Effect::ReadCurrentSubscriber { op, key },
            Effect::ArmTimer(self.operation_due.expect("issued deadline")),
        ])
    }

    pub(super) fn current_subscriber_read(
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
        if progress.detached_key.is_some() {
            return self.detached_subscriber_current_read(op, state);
        }
        let Some(resume) = progress.resume.as_ref() else {
            return Step::reject(Reject::Stage);
        };
        let Some(state) = state else {
            self.state.stage = Stage::IrrecoverableGap;
            self.outstanding = None;
            self.operation_due = None;
            return Step::reject(Reject::Subscription(SubscriptionError::HistoryUnavailable));
        };
        if let Err(error) =
            state.validate_against(resume, self.config.max_cursor_bytes, progress.limits)
        {
            if error == SubscriptionError::HistoryUnavailable {
                self.state.stage = Stage::IrrecoverableGap;
                self.outstanding = None;
                self.operation_due = None;
            }
            return Step::reject(Reject::Subscription(error));
        }
        let request = RegisterSubscriber {
            key: resume.key.clone(),
            incarnation: resume.incarnation,
            start: state.acknowledged,
            policy: resume.policy.clone(),
            request_id: resume.request_id.clone(),
            expected_prior_ordinal: Some(state.epoch.ordinal),
            reset_from: None,
        };
        if let Some(progress) = self.subscription.as_mut() {
            progress.request = Some(request.clone());
        }
        self.outstanding = None;
        self.operation_due = None;
        self.retries = 0;
        let Ok(next) = self.issue(Stage::Registering) else {
            self.state.stage = Stage::RetryExhausted;
            return Step::reject(Reject::Exhausted);
        };
        Step::ok(vec![
            Effect::RegisterSubscriber {
                op: next,
                request: Box::new(request),
            },
            Effect::ArmTimer(self.operation_due.expect("issued deadline")),
        ])
    }

    pub(super) fn subscriber_rejected(&mut self, op: Operation, error: SubscriptionError) -> Step {
        let Some((current, stage)) = self.outstanding else {
            return Step::reject(Reject::StaleOperation);
        };
        if current != op
            || !self.matches(op, stage)
            || !matches!(
                stage,
                Stage::ReadingCurrentSubscriber
                    | Stage::Registering
                    | Stage::ReadingRegistration
                    | Stage::BindingSink
                    | Stage::CheckingSubscriberTail
                    | Stage::ScanningSubscriber
                    | Stage::ApplyingSubscriber
                    | Stage::AckingSubscriber
                    | Stage::ReadingSubscriberAck
                    | Stage::TerminatingSubscriber
                    | Stage::ReadingSubscriberTerminal
            )
            || self.subscription.is_none()
        {
            return Step::reject(Reject::StaleOperation);
        }
        self.outstanding = None;
        self.operation_due = None;
        self.retry_due = None;
        self.pending_batch = None;
        if let Some(progress) = self.subscription.as_mut() {
            progress.pending_ack = None;
        }
        self.state.stage = if matches!(
            error,
            SubscriptionError::Expired
                | SubscriptionError::HistoryUnavailable
                | SubscriptionError::FenceLost
        ) {
            Stage::IrrecoverableGap
        } else {
            Stage::RetryExhausted
        };
        Step::reject(Reject::Subscription(error))
    }

    fn accept_registration(
        &mut self,
        op: Operation,
        stage: Stage,
        receipt: RegisterReceipt,
    ) -> Step {
        if !self.matches(op, stage) {
            return Step::reject(Reject::StaleOperation);
        }
        let Some(progress) = self.subscription.as_ref() else {
            return Step::reject(Reject::Stage);
        };
        let Some(request) = progress.request.as_ref() else {
            return Step::reject(Reject::Stage);
        };
        if let Err(error) =
            receipt.validate_against(request, self.config.max_cursor_bytes, progress.limits)
        {
            return Step::reject(Reject::Subscription(error));
        }
        self.outstanding = None;
        self.operation_due = None;
        self.retries = 0;
        if let Some(progress) = self.subscription.as_mut() {
            progress.source_ack = Some(receipt.protected.clone());
            progress.registration = Some(receipt.clone());
        }
        let Ok(next) = self.issue(Stage::BindingSink) else {
            self.state.stage = Stage::RetryExhausted;
            return Step::reject(Reject::Exhausted);
        };
        Step::ok(vec![
            Effect::BindSinkEpoch {
                op: next,
                registration: Box::new(receipt),
            },
            Effect::ArmTimer(self.operation_due.expect("issued deadline")),
        ])
    }

    pub(super) fn subscriber_registered(
        &mut self,
        op: Operation,
        receipt: RegisterReceipt,
    ) -> Step {
        self.accept_registration(op, Stage::Registering, receipt)
    }

    pub(super) fn subscriber_registration_read(
        &mut self,
        op: Operation,
        receipt: Option<RegisterReceipt>,
    ) -> Step {
        if !self.matches(op, Stage::ReadingRegistration) {
            return Step::reject(Reject::StaleOperation);
        }
        if let Some(receipt) = receipt {
            return self.accept_registration(op, Stage::ReadingRegistration, receipt);
        }
        let Some(request) = self.subscription.as_ref().and_then(|p| p.request.clone()) else {
            return Step::reject(Reject::Stage);
        };
        self.outstanding = None;
        self.operation_due = None;
        let Ok(next) = self.issue(Stage::Registering) else {
            self.state.stage = Stage::RetryExhausted;
            return Step::reject(Reject::Exhausted);
        };
        Step::ok(vec![
            Effect::RegisterSubscriber {
                op: next,
                request: Box::new(request),
            },
            Effect::ArmTimer(self.operation_due.expect("issued deadline")),
        ])
    }

    pub(super) fn sink_epoch_bound(
        &mut self,
        op: Operation,
        checkpoint: FencedCheckpoint,
        source_to_sink: &BoundComparison,
        sink_to_head: &BoundComparison,
    ) -> Step {
        if !self.matches(op, Stage::BindingSink) {
            return Step::reject(Reject::StaleOperation);
        }
        let Some(registration) = self
            .subscription
            .as_ref()
            .and_then(|p| p.registration.as_ref())
        else {
            return Step::reject(Reject::Stage);
        };
        if let Err(error) = checkpoint.validate_against(registration, self.config.max_cursor_bytes)
        {
            return Step::reject(Reject::Subscription(error));
        }
        if !matches!(
            source_to_sink.for_operands(
                &registration.protected,
                &checkpoint.cursor,
                &registration.proof.id,
            ),
            Some(Comparison::Before | Comparison::Equal)
        ) || !matches!(
            sink_to_head.for_operands(
                &checkpoint.cursor,
                &registration.proof.head,
                &registration.proof.id,
            ),
            Some(Comparison::Before | Comparison::Equal)
        ) {
            return Step::reject(Reject::Comparison);
        }
        if let Some(progress) = self.subscription.as_mut() {
            progress.sink = Some(checkpoint.clone());
        }
        self.state.checkpoint = Some(checkpoint.cursor.clone());
        self.state.materialized = Some(checkpoint.cursor);
        self.state.stage = Stage::Protected;
        self.tail_due = self.now;
        self.outstanding = None;
        self.operation_due = None;
        self.retry_due = None;
        self.retries = 0;
        if let Some(terminal) = self.maybe_commit_subscriber_terminal() {
            return terminal;
        }
        Step::ok(vec![Effect::ArmTimer(self.now)])
    }

    pub(super) fn fail_subscription(&mut self, op: Operation) -> Option<Step> {
        let stage = self
            .outstanding
            .and_then(|(current, stage)| (current == op).then_some(stage))?;
        let target = match stage {
            Stage::ReadingCurrentSubscriber => RetryTarget::SubscriptionCurrentRead,
            Stage::Registering | Stage::ReadingRegistration => RetryTarget::SubscriptionReadback,
            Stage::BindingSink => RetryTarget::SubscriptionBind,
            _ => return None,
        };
        if self.subscription.is_none()
            || op.session != self.session_id
            || op.generation != self.state.generation
        {
            return Some(Step::reject(Reject::StaleOperation));
        }
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
        self.state.stage = Stage::RetryWait;
        self.outstanding = None;
        self.operation_due = None;
        self.retry_target = target;
        self.retry_due = Some(Time(due));
        Some(Step::ok(vec![Effect::ArmTimer(Time(due))]))
    }

    pub(super) fn retry_subscription(&mut self) -> Step {
        match self.retry_target {
            RetryTarget::SubscriptionCurrentRead => {
                let Some(key) = self.subscription.as_ref().and_then(|p| {
                    p.detached_key
                        .clone()
                        .or_else(|| p.resume.as_ref().map(|resume| resume.key.clone()))
                }) else {
                    return Step::reject(Reject::Stage);
                };
                let Ok(op) = self.issue(Stage::ReadingCurrentSubscriber) else {
                    self.state.stage = Stage::RetryExhausted;
                    return Step::reject(Reject::Exhausted);
                };
                if let Some(due) = self.subscriber_terminal_due() {
                    self.operation_due = self.operation_due.map(|operation| operation.min(due));
                }
                Step::ok(vec![
                    Effect::ReadCurrentSubscriber { op, key },
                    Effect::ArmTimer(self.operation_due.expect("issued deadline")),
                ])
            }
            RetryTarget::SubscriptionReadback => {
                let Some(request) = self.subscription.as_ref().and_then(|p| p.request.as_ref())
                else {
                    return Step::reject(Reject::Stage);
                };
                let key = request.key.clone();
                let request_id = request.request_id.clone();
                let Ok(op) = self.issue(Stage::ReadingRegistration) else {
                    self.state.stage = Stage::RetryExhausted;
                    return Step::reject(Reject::Exhausted);
                };
                Step::ok(vec![
                    Effect::ReadSubscriberRegistration {
                        op,
                        key,
                        request_id,
                    },
                    Effect::ArmTimer(self.operation_due.expect("issued deadline")),
                ])
            }
            RetryTarget::SubscriptionBind => {
                let Some(receipt) = self
                    .subscription
                    .as_ref()
                    .and_then(|p| p.registration.clone())
                else {
                    return Step::reject(Reject::Stage);
                };
                let Ok(op) = self.issue(Stage::BindingSink) else {
                    self.state.stage = Stage::RetryExhausted;
                    return Step::reject(Reject::Exhausted);
                };
                Step::ok(vec![
                    Effect::BindSinkEpoch {
                        op,
                        registration: Box::new(receipt),
                    },
                    Effect::ArmTimer(self.operation_due.expect("issued deadline")),
                ])
            }
            RetryTarget::Tail
            | RetryTarget::Bootstrap
            | RetryTarget::SubscriptionTail
            | RetryTarget::SubscriptionAckRead
            | RetryTarget::SubscriptionTerminalRead => Step::reject(Reject::Stage),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;
    use crate::replication::{
        BoundComparison, Comparison, Config, Cursor, Event, ProofId, RetentionPolicy, Scope,
        SourceHistory, SourceProof, Stream, SubscriberId, SubscriberKey, SubscriptionEpoch,
    };

    pub(super) fn scope() -> Scope {
        Scope {
            stream: Stream {
                group: "g".into(),
                topic: "events".into(),
                kind: "v1".into(),
            },
            partition: "p".into(),
        }
    }

    pub(super) fn cursor(at: u8) -> Cursor {
        Cursor {
            scope: scope(),
            history: SourceHistory {
                source: "native".into(),
                generation: 1,
            },
            position: vec![at],
        }
    }

    pub(super) fn comparison(left: Cursor, right: Cursor, order: Comparison) -> BoundComparison {
        BoundComparison {
            left,
            right,
            proof: ProofId(vec![9]),
            order,
        }
    }

    pub(super) fn request() -> RegisterSubscriber {
        RegisterSubscriber {
            key: SubscriberKey {
                scope: scope(),
                subscriber: SubscriberId {
                    name: "sink".into(),
                },
            },
            incarnation: NonZeroU64::new(3).expect("nonzero"),
            start: cursor(1),
            policy: RetentionPolicy {
                fingerprint: vec![7],
                max_bytes: 1024,
                max_events: 100,
                max_age_ms: 1000,
                max_lag_events: 100,
            },
            request_id: vec![4],
            expected_prior_ordinal: None,
            reset_from: None,
        }
    }

    pub(super) fn receipt(request: &RegisterSubscriber) -> RegisterReceipt {
        RegisterReceipt {
            key: request.key.clone(),
            incarnation: request.incarnation,
            request_id: request.request_id.clone(),
            epoch: SubscriptionEpoch {
                ordinal: NonZeroU64::new(6).expect("nonzero"),
                native: vec![8],
                history: request.start.history.clone(),
            },
            protected: request.start.clone(),
            proof: SourceProof {
                id: ProofId(vec![9]),
                head: cursor(3),
                retained_from: cursor(0),
                read_authority: false,
            },
            retained_to_protected: comparison(cursor(0), cursor(1), Comparison::Before),
            protected_to_head: comparison(cursor(1), cursor(3), Comparison::Before),
            policy_fingerprint: request.policy.fingerprint.clone(),
        }
    }

    pub(super) fn source_state(request: &RegisterSubscriber) -> SourceSubscriberState {
        SourceSubscriberState {
            key: request.key.clone(),
            epoch: SubscriptionEpoch {
                ordinal: NonZeroU64::new(6).expect("nonzero"),
                native: vec![8],
                history: request.start.history.clone(),
            },
            acknowledged: request.start.clone(),
            policy_fingerprint: request.policy.fingerprint.clone(),
            proof: SourceProof {
                id: ProofId(vec![9]),
                head: cursor(3),
                retained_from: cursor(0),
                read_authority: false,
            },
            retained_to_ack: comparison(cursor(0), cursor(1), Comparison::Before),
            ack_to_head: comparison(cursor(1), cursor(3), Comparison::Before),
        }
    }

    pub(super) fn engine() -> SessionEngine {
        SessionEngine::new(scope(), Mode::EventComplete, Config::default(), 41)
            .expect("valid engine")
    }

    pub(super) fn registration_op(step: &Step) -> Operation {
        step.effects
            .iter()
            .find_map(|effect| match effect {
                Effect::RegisterSubscriber { op, .. } => Some(*op),
                _ => None,
            })
            .expect("register effect")
    }

    #[test]
    fn exact_source_registration_and_sink_fence_are_required_before_protected() {
        let mut engine = engine();
        let request = request();
        let first = engine.step(Event::StartSubscription {
            request: Box::new(request.clone()),
            limits: SubscriptionLimits::default(),
        });
        let op = registration_op(&first);
        assert_eq!(engine.state.stage, Stage::Registering);
        let mut wrong = receipt(&request);
        wrong.protected.position = vec![2];
        assert_eq!(
            engine
                .step(Event::SubscriberRegistered {
                    op,
                    receipt: Box::new(wrong),
                })
                .rejection,
            Some(Reject::Subscription(
                crate::replication::SubscriptionError::Binding
            ))
        );
        assert_eq!(engine.state.stage, Stage::Registering);
        let valid = receipt(&request);
        let bind = engine.step(Event::SubscriberRegistered {
            op,
            receipt: Box::new(valid.clone()),
        });
        let bind_op = bind
            .effects
            .iter()
            .find_map(|effect| match effect {
                Effect::BindSinkEpoch { op, .. } => Some(*op),
                _ => None,
            })
            .expect("bind effect");
        assert_ne!(op, bind_op);
        let checkpoint = FencedCheckpoint {
            key: request.key,
            request_id: request.request_id,
            epoch: valid.epoch,
            cursor: cursor(2),
            durable: true,
        };
        let behind = engine.step(Event::SinkEpochBound {
            op: bind_op,
            checkpoint: Box::new(checkpoint.clone()),
            source_to_sink: Box::new(comparison(cursor(1), cursor(2), Comparison::After)),
            sink_to_head: Box::new(comparison(cursor(2), cursor(3), Comparison::Before)),
        });
        assert_eq!(behind.rejection, Some(Reject::Comparison));
        assert_eq!(engine.state.stage, Stage::BindingSink);
        let protected = engine.step(Event::SinkEpochBound {
            op: bind_op,
            checkpoint: Box::new(checkpoint),
            source_to_sink: Box::new(comparison(cursor(1), cursor(2), Comparison::Before)),
            sink_to_head: Box::new(comparison(cursor(2), cursor(3), Comparison::Before)),
        });
        assert!(protected.rejection.is_none());
        assert_eq!(engine.state.stage, Stage::Protected);
        assert_eq!(engine.state.materialized, Some(cursor(2)));
        assert_eq!(engine.state.checkpoint, Some(cursor(2)));
        assert!(engine.subscription_registration().is_some());
        assert!(matches!(
            engine.read_decision(),
            crate::replication::ReadDecision::Refuse(_)
        ));
    }

    #[test]
    fn ambiguous_registration_uses_same_request_readback_before_sink_binding() {
        let mut engine = engine();
        let request = request();
        let first = engine.step(Event::StartSubscription {
            request: Box::new(request.clone()),
            limits: SubscriptionLimits::default(),
        });
        let register = registration_op(&first);
        let retry = engine.step(Event::Failed { op: register });
        assert_eq!(engine.state.stage, Stage::RetryWait);
        assert!(
            retry
                .effects
                .iter()
                .any(|effect| matches!(effect, Effect::ArmTimer(_)))
        );
        let early = engine.step(Event::Tick(Time(999)));
        assert!(early.effects.is_empty());
        let read = engine.step(Event::Tick(Time(1000)));
        let (read_op, key, request_id) = read
            .effects
            .iter()
            .find_map(|effect| match effect {
                Effect::ReadSubscriberRegistration {
                    op,
                    key,
                    request_id,
                } => Some((*op, key.clone(), request_id.clone())),
                _ => None,
            })
            .expect("read back ambiguous request");
        assert_eq!(key, request.key);
        assert_eq!(request_id, request.request_id);
        assert_ne!(register, read_op);
        let stale = engine.step(Event::SubscriberRegistered {
            op: register,
            receipt: Box::new(receipt(&request)),
        });
        assert_eq!(stale.rejection, Some(Reject::StaleOperation));
        let found = engine.step(Event::SubscriberRegistrationRead {
            op: read_op,
            receipt: Some(Box::new(receipt(&request))),
        });
        assert!(
            found
                .effects
                .iter()
                .any(|effect| matches!(effect, Effect::BindSinkEpoch { .. }))
        );
    }

    #[test]
    fn resume_reads_exact_source_ack_then_conditionally_replaces_old_ordinal() {
        let mut engine = engine();
        let original = request();
        let resume = ResumeSubscriber {
            key: original.key.clone(),
            incarnation: NonZeroU64::new(4).expect("nonzero"),
            policy: original.policy.clone(),
            request_id: vec![5],
        };
        let begin = engine.step(Event::ResumeSubscription {
            request: Box::new(resume.clone()),
            limits: SubscriptionLimits::default(),
        });
        let read = begin
            .effects
            .iter()
            .find_map(|effect| match effect {
                Effect::ReadCurrentSubscriber { op, key } if *key == original.key => Some(*op),
                _ => None,
            })
            .expect("source current read");
        let replace = engine.step(Event::CurrentSubscriberRead {
            op: read,
            state: Some(Box::new(source_state(&original))),
        });
        let (register, replacement) = replace
            .effects
            .iter()
            .find_map(|effect| match effect {
                Effect::RegisterSubscriber { op, request } => Some((*op, request.as_ref())),
                _ => None,
            })
            .expect("conditional replacement");
        assert_eq!(replacement.start, original.start);
        assert_eq!(replacement.expected_prior_ordinal, NonZeroU64::new(6));
        assert_eq!(replacement.request_id, resume.request_id);
        let mut stale_fence = receipt(replacement);
        assert_eq!(
            engine
                .step(Event::SubscriberRegistered {
                    op: register,
                    receipt: Box::new(stale_fence.clone()),
                })
                .rejection,
            Some(Reject::Subscription(SubscriptionError::FenceLost))
        );
        stale_fence.epoch.ordinal = NonZeroU64::new(7).expect("nonzero");
        let accepted = engine.step(Event::SubscriberRegistered {
            op: register,
            receipt: Box::new(stale_fence),
        });
        assert!(
            accepted
                .effects
                .iter()
                .any(|effect| matches!(effect, Effect::BindSinkEpoch { .. }))
        );
        assert_ne!(engine.state.stage, Stage::Protected);
    }

    #[test]
    fn conclusive_conditional_mismatch_stops_without_unbounded_takeover_loop() {
        let mut engine = engine();
        let original = request();
        let begin = engine.step(Event::ResumeSubscription {
            request: Box::new(ResumeSubscriber {
                key: original.key.clone(),
                incarnation: NonZeroU64::new(4).expect("nonzero"),
                policy: original.policy.clone(),
                request_id: vec![5],
            }),
            limits: SubscriptionLimits::default(),
        });
        let read = begin
            .effects
            .iter()
            .find_map(|effect| match effect {
                Effect::ReadCurrentSubscriber { op, .. } => Some(*op),
                _ => None,
            })
            .expect("source current read");
        let replace = engine.step(Event::CurrentSubscriberRead {
            op: read,
            state: Some(Box::new(source_state(&original))),
        });
        let register = registration_op(&replace);
        let rejected = engine.step(Event::SubscriberRejected {
            op: register,
            error: SubscriptionError::ConditionalMismatch,
        });
        assert_eq!(
            rejected.rejection,
            Some(Reject::Subscription(SubscriptionError::ConditionalMismatch))
        );
        assert_eq!(engine.state.stage, Stage::RetryExhausted);
        assert!(engine.next_deadline().is_none());
    }

    #[test]
    fn resume_with_certified_retention_gap_stops_without_registering_or_reset_authority() {
        let mut engine = engine();
        let original = request();
        let begin = engine.step(Event::ResumeSubscription {
            request: Box::new(ResumeSubscriber {
                key: original.key.clone(),
                incarnation: NonZeroU64::new(4).expect("nonzero"),
                policy: original.policy.clone(),
                request_id: vec![5],
            }),
            limits: SubscriptionLimits::default(),
        });
        let read = begin
            .effects
            .iter()
            .find_map(|effect| match effect {
                Effect::ReadCurrentSubscriber { op, .. } => Some(*op),
                _ => None,
            })
            .expect("source current read");
        let mut current = source_state(&original);
        current.proof.retained_from = cursor(2);
        current.retained_to_ack = comparison(cursor(2), cursor(1), Comparison::After);
        let stopped = engine.step(Event::CurrentSubscriberRead {
            op: read,
            state: Some(Box::new(current)),
        });
        assert_eq!(
            stopped.rejection,
            Some(Reject::Subscription(SubscriptionError::HistoryUnavailable))
        );
        assert_eq!(engine.state.stage, Stage::IrrecoverableGap);
        assert!(stopped.effects.is_empty());
        assert!(engine.subscription_terminal().is_none());
    }
}
