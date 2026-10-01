use super::tests::{comparison, cursor, engine, receipt, registration_op, request};
use crate::Time;
use crate::replication::{
    Batch, CommitSubscriberAck, Comparison, Coverage, DurableDeliveryReceipt, Effect, Event,
    FencedCheckpoint, Operation, ProofId, Reject, SourceProof, Stage, Step, SubscriberAckReceipt,
    SubscriptionLimits,
};

fn effect_op(step: &Step, kind: &str) -> Operation {
    step.effects
        .iter()
        .find_map(|effect| match (kind, effect) {
            ("bind", Effect::BindSinkEpoch { op, .. })
            | ("tail", Effect::CheckSubscriberTail { op, .. })
            | ("scan", Effect::ScanSubscriber { op, .. })
            | ("apply", Effect::ApplySubscriberBatch { op, .. })
            | ("ack", Effect::CommitSubscriberAck { op, .. })
            | ("read", Effect::ReadSubscriberAck { op, .. }) => Some(*op),
            _ => None,
        })
        .expect("expected operation")
}

fn bound_engine(sink_at: u8) -> super::SessionEngine {
    let mut engine = engine();
    let requested = request();
    let start = engine.step(Event::StartSubscription {
        request: Box::new(requested.clone()),
        limits: SubscriptionLimits::default(),
    });
    let registered = receipt(&requested);
    let bind = engine.step(Event::SubscriberRegistered {
        op: registration_op(&start),
        receipt: Box::new(registered.clone()),
    });
    let bound = engine.step(Event::SinkEpochBound {
        op: effect_op(&bind, "bind"),
        checkpoint: Box::new(FencedCheckpoint {
            key: requested.key,
            request_id: requested.request_id,
            epoch: registered.epoch,
            cursor: cursor(sink_at),
            durable: true,
        }),
        source_to_sink: Box::new(comparison(
            cursor(1),
            cursor(sink_at),
            if sink_at == 1 {
                Comparison::Equal
            } else {
                Comparison::Before
            },
        )),
        sink_to_head: Box::new(comparison(cursor(sink_at), cursor(3), Comparison::Before)),
    });
    assert!(bound.rejection.is_none());
    engine
}

fn proof(retained: u8) -> SourceProof {
    SourceProof {
        id: ProofId(vec![9]),
        head: cursor(3),
        retained_from: cursor(retained),
        read_authority: false,
    }
}

fn tail(engine: &mut super::SessionEngine, retained: u8) -> Step {
    let start = engine.step(Event::PollSubscriber);
    let op = effect_op(&start, "tail");
    engine.step(Event::SubscriberTail {
        op,
        proof: proof(retained),
        comparisons: vec![
            comparison(
                cursor(retained),
                cursor(1),
                if retained > 1 {
                    Comparison::After
                } else {
                    Comparison::Before
                },
            ),
            comparison(cursor(1), cursor(3), Comparison::Before),
            comparison(cursor(retained), cursor(3), Comparison::Before),
        ],
    })
}

fn batch() -> Batch {
    Batch {
        coverage: Coverage {
            from: cursor(1),
            through: cursor(2),
            proof: ProofId(vec![9]),
            certificate: vec![4],
        },
        payload_id: 1,
        events: 1,
        bytes: 8,
        advance: comparison(cursor(1), cursor(2), Comparison::Before),
        end_to_head: comparison(cursor(2), cursor(3), Comparison::Before),
    }
}

fn apply_receipt(
    engine: &super::SessionEngine,
    op: Operation,
    previous_sink: u8,
    sink: u8,
) -> DurableDeliveryReceipt {
    let registration = engine.subscription_registration().expect("bound").0;
    DurableDeliveryReceipt {
        operation: op,
        key: registration.key.clone(),
        epoch: registration.epoch.clone(),
        previous_sink: cursor(previous_sink),
        through: cursor(2),
        sink_cursor: cursor(sink),
        ack_request_id: vec![44],
        durable: true,
    }
}

fn applied(engine: &mut super::SessionEngine, op: Operation, previous_sink: u8, sink: u8) -> Step {
    let receipt = apply_receipt(engine, op, previous_sink, sink);
    engine.step(Event::SubscriberApplied {
        op,
        receipt: Box::new(receipt),
        through_to_sink: Box::new(comparison(
            cursor(2),
            cursor(sink),
            if sink == 2 {
                Comparison::Equal
            } else {
                Comparison::Before
            },
        )),
        previous_to_sink: Box::new(comparison(
            cursor(previous_sink),
            cursor(sink),
            if previous_sink == sink {
                Comparison::Equal
            } else {
                Comparison::Before
            },
        )),
    })
}

fn ack_request(step: &Step) -> CommitSubscriberAck {
    step.effects
        .iter()
        .find_map(|effect| match effect {
            Effect::CommitSubscriberAck { request, .. } => Some(*request.clone()),
            _ => None,
        })
        .expect("ack request")
}

#[test]
fn durable_sink_receipt_precedes_conditional_source_ack_and_unknown_is_read_back() {
    let mut engine = bound_engine(1);
    let scan = tail(&mut engine, 0);
    let apply = engine.step(Event::SubscriberScanned {
        op: effect_op(&scan, "scan"),
        batch: Box::new(batch()),
    });
    assert!(
        !apply
            .effects
            .iter()
            .any(|effect| matches!(effect, Effect::CommitSubscriberAck { .. }))
    );
    let apply_op = effect_op(&apply, "apply");
    let mut volatile = apply_receipt(&engine, apply_op, 1, 2);
    volatile.durable = false;
    let rejected = engine.step(Event::SubscriberApplied {
        op: apply_op,
        receipt: Box::new(volatile),
        through_to_sink: Box::new(comparison(cursor(2), cursor(2), Comparison::Equal)),
        previous_to_sink: Box::new(comparison(cursor(1), cursor(2), Comparison::Before)),
    });
    assert!(rejected.rejection.is_some());
    assert_eq!(engine.state().checkpoint, Some(cursor(1)));
    let forged_jump = applied(&mut engine, apply_op, 1, 3);
    assert_eq!(forged_jump.rejection, Some(Reject::Comparison));
    assert_eq!(engine.state().checkpoint, Some(cursor(1)));
    let ack = applied(&mut engine, apply_op, 1, 2);
    let request = ack_request(&ack);
    assert_eq!(request.previous, cursor(1));
    assert_eq!(request.through, cursor(2));
    assert_eq!(engine.state().checkpoint, Some(cursor(2)));
    let ack_op = effect_op(&ack, "ack");
    let retry = engine.step(Event::Failed { op: ack_op });
    assert_eq!(engine.state().stage, Stage::RetryWait);
    assert!(
        retry
            .effects
            .iter()
            .any(|e| matches!(e, Effect::ArmTimer(_)))
    );
    let read = engine.step(Event::Tick(Time(1000)));
    let read_op = effect_op(&read, "read");
    assert_eq!(
        engine
            .step(Event::SubscriberAcked {
                op: ack_op,
                receipt: Box::new(SubscriberAckReceipt {
                    request: request.clone(),
                    durable: true,
                }),
            })
            .rejection,
        Some(Reject::StaleOperation)
    );
    let completed = engine.step(Event::SubscriberAckRead {
        op: read_op,
        receipt: Some(Box::new(SubscriberAckReceipt {
            request,
            durable: true,
        })),
    });
    assert!(completed.effects.iter().any(
        |effect| matches!(effect, Effect::CheckSubscriberTail { from, .. } if *from == cursor(2))
    ));
}

#[test]
fn sink_ahead_replays_without_rollback_and_retention_gap_is_terminal() {
    let mut engine = bound_engine(2);
    let scan = tail(&mut engine, 0);
    let apply = engine.step(Event::SubscriberScanned {
        op: effect_op(&scan, "scan"),
        batch: Box::new(batch()),
    });
    let ack = applied(&mut engine, effect_op(&apply, "apply"), 2, 2);
    assert!(ack.rejection.is_none());
    assert_eq!(engine.state().checkpoint, Some(cursor(2)));
    let request = ack_request(&ack);
    let next = engine.step(Event::SubscriberAcked {
        op: effect_op(&ack, "ack"),
        receipt: Box::new(SubscriberAckReceipt {
            request,
            durable: true,
        }),
    });
    assert!(next.effects.iter().any(
        |effect| matches!(effect, Effect::CheckSubscriberTail { from, .. } if *from == cursor(2))
    ));

    let mut gap = bound_engine(1);
    let result = tail(&mut gap, 2);
    assert_eq!(gap.state().stage, Stage::IrrecoverableGap);
    assert!(gap.subscription_terminal().is_none());
    assert!(
        result
            .effects
            .iter()
            .any(|effect| matches!(effect, Effect::IrrecoverableGap))
    );
    assert!(!result.effects.iter().any(|effect| matches!(
        effect,
        Effect::CommitSubscriberTerminal { .. } | Effect::ReadSubscriberTerminal { .. }
    )));
    assert!(
        gap.step(Event::BeginSubscriberTerminal {
            request_id: vec![44],
            due: Time(1_000),
        })
        .rejection
        .is_some()
    );
    assert!(gap.subscription_terminal().is_none());
}

#[test]
fn ordinary_replay_inputs_cannot_bypass_registration_or_sink_fence() {
    let mut engine = engine();
    let requested = request();
    let start = engine.step(Event::StartSubscription {
        request: Box::new(requested.clone()),
        limits: SubscriptionLimits::default(),
    });
    for event in [
        Event::Activity,
        Event::Hint,
        Event::Authority(true),
        Event::Demand {
            cursor: cursor(3),
            comparison: None,
        },
        Event::StartBootstrap,
        Event::Resume { cursor: cursor(2) },
    ] {
        let result = engine.step(event);
        assert!(!result.effects.iter().any(|effect| matches!(
            effect,
            Effect::LoadCheckpoint { .. } | Effect::CheckTail { .. } | Effect::Apply { .. }
        )));
        assert_eq!(engine.state().stage, Stage::Registering);
    }
    assert!(matches!(
        engine.read_decision(),
        crate::replication::ReadDecision::Refuse(_)
    ));
    let valid = receipt(&requested);
    let bind = engine.step(Event::SubscriberRegistered {
        op: registration_op(&start),
        receipt: Box::new(valid),
    });
    assert_eq!(engine.state().stage, Stage::BindingSink);
    let old_bind = effect_op(&bind, "bind");
    let bypass = engine.step(Event::Resume { cursor: cursor(3) });
    assert_eq!(bypass.rejection, Some(Reject::Stage));
    assert_eq!(engine.state().stage, Stage::BindingSink);
    engine.step(Event::Cancel);
    assert_eq!(engine.state().stage, Stage::Cancelled);
    assert_eq!(
        engine.step(Event::Resume { cursor: cursor(3) }).rejection,
        Some(Reject::Stage)
    );
    assert_eq!(
        engine
            .step(Event::SinkEpochBound {
                op: old_bind,
                checkpoint: Box::new(FencedCheckpoint {
                    key: requested.key,
                    request_id: requested.request_id,
                    epoch: receipt(&request()).epoch,
                    cursor: cursor(1),
                    durable: true,
                }),
                source_to_sink: Box::new(comparison(cursor(1), cursor(1), Comparison::Equal)),
                sink_to_head: Box::new(comparison(cursor(1), cursor(3), Comparison::Before)),
            })
            .rejection,
        Some(Reject::StaleOperation)
    );
}

#[test]
fn registration_and_resume_reject_another_native_scope_before_source_effect() {
    let mut requested = request();
    requested.key.scope.partition = "elsewhere".into();
    requested.start.scope = requested.key.scope.clone();
    let mut engine = engine();
    let start = engine.step(Event::StartSubscription {
        request: Box::new(requested.clone()),
        limits: SubscriptionLimits::default(),
    });
    assert_eq!(
        start.rejection,
        Some(Reject::Subscription(
            crate::replication::SubscriptionError::Identity(
                crate::replication::IdentityError::WrongScope
            )
        ))
    );
    assert_eq!(start.effects, [] as [crate::replication::Effect; 0]);
    let resume = engine.step(Event::ResumeSubscription {
        request: Box::new(crate::replication::ResumeSubscriber {
            key: requested.key,
            incarnation: requested.incarnation,
            policy: requested.policy,
            request_id: requested.request_id,
        }),
        limits: SubscriptionLimits::default(),
    });
    assert_eq!(resume.rejection, start.rejection);
    assert_eq!(resume.effects, [] as [crate::replication::Effect; 0]);
    assert_eq!(engine.state().stage, Stage::Unready);
}
