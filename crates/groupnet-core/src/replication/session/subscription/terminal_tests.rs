//! Explicit unsubscribe only advances after a durable exact tombstone.

use super::tests::{comparison, cursor, engine, receipt, registration_op, request, source_state};
use crate::Time;
use crate::replication::{
    Comparison, Effect, Event, FencedCheckpoint, Operation, Stage, Step, TerminalReason,
    TerminalReceipt,
};

fn op(step: &Step, kind: &str) -> Operation {
    step.effects
        .iter()
        .find_map(|effect| match (kind, effect) {
            ("bind", Effect::BindSinkEpoch { op, .. })
            | ("terminal", Effect::CommitSubscriberTerminal { op, .. })
            | ("read", Effect::ReadSubscriberTerminal { op, .. }) => Some(*op),
            _ => None,
        })
        .expect("expected operation")
}

fn protected() -> super::SessionEngine {
    let mut engine = engine();
    let requested = request();
    let start = engine.step(Event::StartSubscription {
        request: Box::new(requested.clone()),
        limits: crate::replication::SubscriptionLimits::default(),
    });
    let registered = receipt(&requested);
    let bind = engine.step(Event::SubscriberRegistered {
        op: registration_op(&start),
        receipt: Box::new(registered.clone()),
    });
    let ready = engine.step(Event::SinkEpochBound {
        op: op(&bind, "bind"),
        checkpoint: Box::new(FencedCheckpoint {
            key: registered.key.clone(),
            request_id: registered.request_id.clone(),
            epoch: registered.epoch,
            cursor: cursor(1),
            durable: true,
        }),
        source_to_sink: Box::new(comparison(cursor(1), cursor(1), Comparison::Equal)),
        sink_to_head: Box::new(comparison(cursor(1), cursor(3), Comparison::Before)),
    });
    assert!(ready.rejection.is_none());
    assert_eq!(engine.state().stage, Stage::Protected);
    engine
}

#[test]
fn unknown_terminal_must_be_read_back_by_exact_stable_request() {
    let mut engine = protected();
    let begin = engine.step(Event::BeginSubscriberTerminal {
        request_id: vec![40],
        due: Time(1000),
    });
    let terminal_op = op(&begin, "terminal");
    let request = begin
        .effects
        .iter()
        .find_map(|effect| match effect {
            Effect::CommitSubscriberTerminal { request, .. } => Some(*request.clone()),
            _ => None,
        })
        .expect("terminal request");
    assert_eq!(request.reason, TerminalReason::Unsubscribed);
    assert_eq!(request.acknowledged, cursor(1));
    assert!(engine.subscription_terminal().is_none());
    let failed = engine.step(Event::Failed { op: terminal_op });
    let read_op = op(&failed, "read");
    let durable = TerminalReceipt {
        tombstone_ordinal: request.epoch.ordinal,
        request,
        durable: true,
    };
    assert!(
        engine
            .step(Event::SubscriberTerminalCommitted {
                op: terminal_op,
                receipt: Box::new(durable.clone()),
            })
            .rejection
            .is_some()
    );
    let mut unrelated = durable.clone();
    unrelated.request.request_id = vec![41];
    assert!(
        engine
            .step(Event::SubscriberTerminalRead {
                op: read_op,
                receipt: Some(Box::new(unrelated)),
            })
            .rejection
            .is_some()
    );
    let completed = engine.step(Event::SubscriberTerminalRead {
        op: read_op,
        receipt: Some(Box::new(durable.clone())),
    });
    assert!(completed.rejection.is_none());
    assert_eq!(engine.state().stage, Stage::TerminatedSubscriber);
    assert_eq!(engine.subscription_terminal(), Some(&durable));
    assert_eq!(engine.subscription_acknowledged(), Some(&cursor(1)));
}

#[test]
fn terminal_total_deadline_never_restarts_after_ambiguous_write() {
    let mut engine = protected();
    let begin = engine.step(Event::BeginSubscriberTerminal {
        request_id: vec![42],
        due: Time(50),
    });
    let terminal_op = op(&begin, "terminal");
    assert_eq!(engine.operation_deadline(terminal_op), Some(Time(50)));
    let failed = engine.step(Event::Failed { op: terminal_op });
    let read_op = op(&failed, "read");
    assert_eq!(engine.operation_deadline(read_op), Some(Time(50)));
    engine.step(Event::Tick(Time(50)));
    assert_eq!(engine.state().stage, Stage::RetryExhausted);
    assert!(engine.subscription_terminal().is_none());
    assert!(
        engine
            .step(Event::SubscriberTerminalRead {
                op: read_op,
                receipt: None,
            })
            .rejection
            .is_some()
    );
}

#[test]
fn unsubscribe_during_unconfirmed_registration_does_not_poison_later_delivery() {
    let mut engine = engine();
    let requested = request();
    let start = engine.step(Event::StartSubscription {
        request: Box::new(requested.clone()),
        limits: crate::replication::SubscriptionLimits::default(),
    });
    assert_eq!(engine.state().stage, Stage::Registering);
    let refused = engine.step(Event::BeginSubscriberTerminal {
        request_id: vec![43],
        due: Time(1000),
    });
    assert!(refused.rejection.is_some());
    assert_eq!(engine.state().stage, Stage::Registering);
    let valid = engine.step(Event::SubscriberRegistered {
        op: registration_op(&start),
        receipt: Box::new(receipt(&requested)),
    });
    assert!(valid.rejection.is_none());
    assert_eq!(engine.state().stage, Stage::BindingSink);
}

#[test]
fn unsubscribe_during_registration_retry_wait_preserves_exact_readback() {
    let mut engine = engine();
    let requested = request();
    let start = engine.step(Event::StartSubscription {
        request: Box::new(requested.clone()),
        limits: crate::replication::SubscriptionLimits::default(),
    });
    let pending = registration_op(&start);
    let failed = engine.step(Event::Failed { op: pending });
    assert_eq!(engine.state().stage, Stage::RetryWait);
    let refused = engine.step(Event::BeginSubscriberTerminal {
        request_id: vec![44],
        due: Time(1_000),
    });
    assert!(refused.rejection.is_some());
    assert_eq!(engine.state().stage, Stage::RetryWait);
    let retry_due = failed
        .effects
        .iter()
        .find_map(|effect| match effect {
            Effect::ArmTimer(due) => Some(*due),
            _ => None,
        })
        .expect("retry timer");
    let read = engine.step(Event::Tick(retry_due));
    let read_op = read
        .effects
        .iter()
        .find_map(|effect| match effect {
            Effect::ReadSubscriberRegistration { op, .. } => Some(*op),
            _ => None,
        })
        .expect("same registration readback");
    let bound = engine.step(Event::SubscriberRegistrationRead {
        op: read_op,
        receipt: Some(Box::new(receipt(&requested))),
    });
    assert!(bound.rejection.is_none());
    assert_eq!(engine.state().stage, Stage::BindingSink);
}

#[test]
fn queued_unsubscribe_keeps_original_deadline_during_source_tail() {
    let mut engine = protected();
    let pending = engine.step(Event::PollSubscriber);
    assert!(
        pending
            .effects
            .iter()
            .any(|effect| matches!(effect, Effect::CheckSubscriberTail { .. }))
    );
    let queued = engine.step(Event::BeginSubscriberTerminal {
        request_id: vec![45],
        due: Time(20),
    });
    assert!(queued.rejection.is_none());
    assert_eq!(engine.state().stage, Stage::CheckingSubscriberTail);
    engine.step(Event::Tick(Time(20)));
    assert_eq!(engine.state().stage, Stage::RetryExhausted);
    assert!(engine.subscription_terminal().is_none());
}

#[test]
fn detached_terminal_reads_source_ack_and_never_requires_sink_binding() {
    let mut engine = engine();
    let original = request();
    let start = engine.step(Event::StartDetachedSubscriberTerminal {
        key: original.key.clone(),
        request_id: vec![46],
        due: Time(1_000),
        limits: crate::replication::SubscriptionLimits::default(),
    });
    let read = start
        .effects
        .iter()
        .find_map(|effect| match effect {
            Effect::ReadCurrentSubscriber { op, .. } => Some(*op),
            _ => None,
        })
        .expect("source-current read");
    let commit = engine.step(Event::CurrentSubscriberRead {
        op: read,
        state: Some(Box::new(source_state(&original))),
    });
    let terminal = commit.effects.iter().find_map(|effect| match effect {
        Effect::CommitSubscriberTerminal { request, .. } => Some(request.as_ref()),
        _ => None,
    });
    let request = terminal.expect("conditional source tombstone");
    assert_eq!(request.acknowledged, cursor(1));
    assert_eq!(request.request_id, vec![46]);
    assert!(!commit.effects.iter().any(|effect| matches!(
        effect,
        Effect::BindSinkEpoch { .. } | Effect::ApplySubscriberBatch { .. }
    )));
    assert_eq!(engine.state().stage, Stage::TerminatingSubscriber);
}

#[test]
fn detached_terminal_can_relinquish_an_already_gapped_lineage() {
    let mut engine = engine();
    let original = request();
    let begin = engine.step(Event::StartDetachedSubscriberTerminal {
        key: original.key.clone(),
        request_id: vec![47],
        due: Time(1_000),
        limits: crate::replication::SubscriptionLimits::default(),
    });
    let read = begin
        .effects
        .iter()
        .find_map(|effect| match effect {
            Effect::ReadCurrentSubscriber { op, .. } => Some(*op),
            _ => None,
        })
        .expect("source-current read");
    let mut current = source_state(&original);
    current.proof.retained_from = cursor(2);
    current.retained_to_ack = comparison(cursor(2), cursor(1), Comparison::After);
    let terminal = engine.step(Event::CurrentSubscriberRead {
        op: read,
        state: Some(Box::new(current)),
    });
    assert!(terminal.rejection.is_none());
    assert!(
        terminal
            .effects
            .iter()
            .any(|effect| matches!(effect, Effect::CommitSubscriberTerminal { .. }))
    );
    assert_eq!(engine.state().stage, Stage::TerminatingSubscriber);
}
