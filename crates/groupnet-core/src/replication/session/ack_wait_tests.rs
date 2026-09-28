use super::*;
use crate::replication::{SourceHistory, Stream};

fn scope() -> Scope {
    Scope {
        stream: Stream {
            group: "group".into(),
            topic: "writes".into(),
            kind: "v1".into(),
        },
        partition: "shard-1".into(),
    }
}

fn op() -> Operation {
    Operation {
        session: 4,
        generation: 2,
        token: 7,
    }
}

fn poll_op(token: u64) -> Operation {
    Operation { token, ..op() }
}

fn target() -> AckTarget {
    AckTarget::Intent {
        scope: scope(),
        history: SourceHistory {
            source: "cas".into(),
            generation: 3,
        },
        id: b"intent-9".to_vec(),
    }
}

fn member(name: &str, incarnation: u64) -> RequiredSubscriber {
    RequiredSubscriber {
        name: name.into(),
        incarnation,
        epoch: format!("epoch-{incarnation}").into_bytes(),
    }
}

fn make_request() -> AckWaitRequest {
    let target = target();
    AckWaitRequest {
        request_id: b"write-9".to_vec(),
        target: target.clone(),
        kind: AckKind::Invalidated,
        roster: super::super::super::ack_types::CertifiedRoster {
            target,
            kind: AckKind::Invalidated,
            policy_version: 1,
            certificate: b"cert-9".to_vec(),
            required: vec![member("alpha", 2), member("beta", 5)],
        },
        due: Time(20),
    }
}

fn limits() -> AckWaitLimits {
    AckWaitLimits {
        max_required: 2,
        max_identity_bytes: 64,
        max_certificate_bytes: 64,
        max_metadata_bytes: 1024,
        max_wait_ms: 20,
        poll_ms: 3,
    }
}

fn evidence(request: &AckWaitRequest, subscriber: RequiredSubscriber) -> AckEvidence {
    AckEvidence {
        op: poll_op(8),
        request_id: request.request_id.clone(),
        target: request.target.clone(),
        kind: request.kind,
        roster_certificate: request.roster.certificate.clone(),
        subscriber,
    }
}

#[test]
fn exact_fixed_roster_counts_each_registration_once() {
    let request = make_request();
    let mut wait = AckWait::new(&scope(), op(), request.clone(), limits(), Time(0)).unwrap();
    assert!(wait.poll(poll_op(8), Time(0), Time(10)));
    assert_eq!(wait.operation(), op());
    assert_eq!(wait.deadline(), Time(20));
    assert_eq!(
        wait.status(),
        AckWaitOutcome::Pending(request.roster.required.clone())
    );
    let first = evidence(&request, member("alpha", 2));
    wait.observe(&first, Time(1)).unwrap();
    assert_eq!(
        wait.status(),
        AckWaitOutcome::Pending(vec![member("beta", 5)])
    );
    assert_eq!(wait.observe(&first, Time(2)), Err(AckWaitError::Evidence));
    assert!(wait.poll(poll_op(9), Time(2), Time(10)));
    let mut duplicate = first.clone();
    duplicate.op = poll_op(9);
    assert_eq!(
        wait.observe(&duplicate, Time(2)),
        Err(AckWaitError::Duplicate)
    );
    let mut second = evidence(&request, member("beta", 5));
    second.op = poll_op(9);
    wait.observe(&second, Time(3)).unwrap();
    assert_eq!(wait.status(), AckWaitOutcome::Satisfied);
    wait.tick(Time(30));
    assert_eq!(wait.status(), AckWaitOutcome::Satisfied);
}

#[test]
fn other_target_kind_epoch_and_operation_never_satisfy() {
    let request = make_request();
    let mut wait = AckWait::new(&scope(), op(), request.clone(), limits(), Time(0)).unwrap();
    assert!(wait.poll(poll_op(8), Time(0), Time(10)));
    let baseline = evidence(&request, member("alpha", 2));
    let mut cases = Vec::new();
    let mut wrong = baseline.clone();
    wrong.op.token += 1;
    cases.push(wrong);
    let mut wrong = baseline.clone();
    wrong.request_id = b"another-write".to_vec();
    cases.push(wrong);
    let mut wrong = baseline.clone();
    wrong.kind = AckKind::Materialized;
    cases.push(wrong);
    let mut wrong = baseline.clone();
    wrong.roster_certificate = b"another-roster".to_vec();
    cases.push(wrong);
    let mut wrong = baseline.clone();
    wrong.subscriber = member("alpha", 3);
    cases.push(wrong);
    let mut wrong = baseline.clone();
    if let AckTarget::Intent { ref mut id, .. } = wrong.target {
        *id = b"newer-intent".to_vec();
    }
    cases.push(wrong);
    let mut wrong = baseline.clone();
    if let AckTarget::Intent {
        ref mut history, ..
    } = wrong.target
    {
        history.generation += 1;
    }
    cases.push(wrong);
    for case in cases {
        assert_eq!(wait.observe(&case, Time(1)), Err(AckWaitError::Evidence));
    }
    assert_eq!(
        wait.status(),
        AckWaitOutcome::Pending(request.roster.required)
    );
}

#[test]
fn deadline_and_cancellation_report_unmet_without_rewriting_commit() {
    let request = make_request();
    let mut wait = AckWait::new(&scope(), op(), request.clone(), limits(), Time(0)).unwrap();
    assert!(wait.poll(poll_op(8), Time(0), Time(20)));
    wait.observe(&evidence(&request, member("alpha", 2)), Time(19))
        .unwrap();
    wait.tick(Time(20));
    assert_eq!(
        wait.status(),
        AckWaitOutcome::TimedOut(vec![member("beta", 5)])
    );
    assert_eq!(
        wait.observe(&evidence(&request, member("beta", 5)), Time(20)),
        Err(AckWaitError::Closed)
    );
    wait.cancel();
    assert_eq!(
        wait.status(),
        AckWaitOutcome::TimedOut(vec![member("beta", 5)])
    );
    let mut cancelled = AckWait::new(&scope(), op(), request.clone(), limits(), Time(0)).unwrap();
    cancelled.cancel();
    assert_eq!(cancelled.status(), AckWaitOutcome::Cancelled);
    cancelled.authority_lost();
    assert_eq!(cancelled.status(), AckWaitOutcome::Cancelled);
    let mut lost = AckWait::new(&scope(), op(), request, limits(), Time(0)).unwrap();
    lost.authority_lost();
    assert_eq!(lost.status(), AckWaitOutcome::AuthorityLost);
}

#[test]
fn roster_is_bounded_and_cannot_change_target_or_reuse_name() {
    let mut request = make_request();
    request.roster.required.push(member("gamma", 1));
    assert_eq!(
        AckWait::new(&scope(), op(), request, limits(), Time(0)).unwrap_err(),
        AckWaitError::Backpressure
    );
    let mut request = make_request();
    request.roster.required[1] = member("alpha", 3);
    assert_eq!(
        AckWait::new(&scope(), op(), request, limits(), Time(0)).unwrap_err(),
        AckWaitError::Roster
    );
    let mut request = make_request();
    request.roster.target = AckTarget::Intent {
        scope: scope(),
        history: SourceHistory {
            source: "cas".into(),
            generation: 4,
        },
        id: b"intent-9".to_vec(),
    };
    assert_eq!(
        AckWait::new(&scope(), op(), request, limits(), Time(0)).unwrap_err(),
        AckWaitError::Roster
    );
    let mut request = make_request();
    request.roster.certificate = vec![1; 65];
    assert_eq!(
        AckWait::new(&scope(), op(), request, limits(), Time(0)).unwrap_err(),
        AckWaitError::Identity
    );
    let mut request = make_request();
    request.due = Time(21);
    assert_eq!(
        AckWait::new(&scope(), op(), request, limits(), Time(0)).unwrap_err(),
        AckWaitError::Deadline
    );
}

#[test]
fn empty_roster_satisfies_only_after_exact_certificate_validation() {
    let mut request = make_request();
    request.roster.required.clear();
    let wait = AckWait::new(&scope(), op(), request.clone(), limits(), Time(0)).unwrap();
    assert_eq!(wait.status(), AckWaitOutcome::Satisfied);
    request.roster.kind = AckKind::Materialized;
    assert_eq!(
        AckWait::new(&scope(), op(), request, limits(), Time(0)).unwrap_err(),
        AckWaitError::Roster
    );
}

fn make_engine() -> SessionEngine {
    SessionEngine::new(
        scope(),
        super::super::super::Mode::StateSync,
        super::super::super::Config::default(),
        4,
    )
    .unwrap()
}

fn observe_effect(step: &Step) -> Operation {
    step.effects
        .iter()
        .find_map(|effect| match effect {
            Effect::ObserveNamedAcks { op, .. } => Some(*op),
            _ => None,
        })
        .expect("source observation effect")
}

fn finished(step: &Step) -> Option<(Operation, AckWaitOutcome)> {
    step.effects.iter().find_map(|effect| match effect {
        Effect::AckWaitFinished { op, outcome } => Some((*op, outcome.clone())),
        _ => None,
    })
}

#[test]
fn delayed_empty_reply_cannot_clear_new_poll_and_emitted_timers_drive_timeout() {
    let mut engine = make_engine();
    let request = make_request();
    let started = engine.step(super::super::super::Event::StartAckWait {
        request: Box::new(request.clone()),
        limits: limits(),
    });
    let wait_op = engine.ack_wait_operation().unwrap();
    let first = observe_effect(&started);
    assert_ne!(first, wait_op);
    assert!(
        started
            .effects
            .iter()
            .any(|effect| matches!(effect, Effect::ArmTimer(Time(20))))
    );
    let timed_poll = engine.step(super::super::super::Event::Tick(Time(20)));
    assert_eq!(
        finished(&timed_poll),
        Some((wait_op, AckWaitOutcome::TimedOut(request.roster.required)))
    );
    assert_eq!(engine.ack_wait_operation(), None);
    assert_eq!(
        engine
            .step(super::super::super::Event::AckChecked { op: first })
            .rejection,
        Some(Reject::StaleOperation)
    );

    let mut engine = make_engine();
    let request = make_request();
    let first = observe_effect(&engine.step(super::super::super::Event::StartAckWait {
        request: Box::new(request.clone()),
        limits: limits(),
    }));
    let checked = engine.step(super::super::super::Event::AckChecked { op: first });
    assert!(
        checked
            .effects
            .iter()
            .any(|effect| matches!(effect, Effect::ArmTimer(Time(3))))
    );
    let second = observe_effect(&engine.step(super::super::super::Event::Tick(Time(3))));
    assert_ne!(first, second);
    assert_eq!(
        engine
            .step(super::super::super::Event::AckChecked { op: first })
            .rejection,
        Some(Reject::StaleOperation)
    );
    assert!(engine.accepts_operation(second));
    let partial = engine.step(super::super::super::Event::AckObserved {
        evidence: Box::new(AckEvidence {
            op: second,
            ..evidence(&request, member("alpha", 2))
        }),
    });
    let third = observe_effect(&partial);
    assert_ne!(second, third);
    assert_eq!(
        engine
            .step(super::super::super::Event::AckChecked { op: second })
            .rejection,
        Some(Reject::StaleOperation)
    );
    assert!(engine.accepts_operation(third));
}

#[test]
fn local_serve_authority_loss_does_not_cancel_source_certified_wait() {
    let mut engine = make_engine();
    engine.state.stage = super::super::super::Stage::RetryExhausted;
    let request = make_request();
    let start = engine.step(super::super::super::Event::StartAckWait {
        request: Box::new(request),
        limits: limits(),
    });
    let poll = observe_effect(&start);
    let wait_op = engine.ack_wait_operation().unwrap();
    let local = engine.step(super::super::super::Event::Authority(false));
    assert!(finished(&local).is_none());
    assert!(engine.accepts_operation(poll));
    assert_eq!(
        finished(&engine.step(super::super::super::Event::AckAuthorityLost { op: wait_op })),
        Some((wait_op, AckWaitOutcome::AuthorityLost))
    );
}

#[test]
fn supersede_cancels_wait_and_old_poll_cannot_complete_new_generation() {
    let mut engine = make_engine();
    let request = make_request();
    let first = observe_effect(&engine.step(super::super::super::Event::StartAckWait {
        request: Box::new(request.clone()),
        limits: limits(),
    }));
    let wait_op = engine.ack_wait_operation().unwrap();
    assert_eq!(
        finished(&engine.step(super::super::super::Event::Supersede)),
        Some((wait_op, AckWaitOutcome::Cancelled))
    );
    assert!(!engine.accepts_operation(first));
    assert_eq!(
        engine
            .step(super::super::super::Event::AckObserved {
                evidence: Box::new(AckEvidence {
                    op: first,
                    ..evidence(&request, member("alpha", 2))
                })
            })
            .rejection,
        Some(Reject::StaleOperation)
    );
    let next = engine.step(super::super::super::Event::StartAckWait {
        request: Box::new(request),
        limits: limits(),
    });
    let second = observe_effect(&next);
    assert_ne!(first, second);
    assert_ne!(wait_op, engine.ack_wait_operation().unwrap());
}

#[test]
fn finishing_wait_rearms_independent_replay_deadline() {
    let mut engine = make_engine();
    let start = engine.step(super::super::super::Event::StartAckWait {
        request: Box::new(make_request()),
        limits: limits(),
    });
    assert!(start.rejection.is_none());
    let wait_op = engine.ack_wait_operation().unwrap();
    engine.state.stage = super::super::super::Stage::RetryWait;
    engine.retry_due = Some(Time(30));
    let stopped = engine.step(super::super::super::Event::CancelAckWait { op: wait_op });
    assert_eq!(
        finished(&stopped),
        Some((wait_op, AckWaitOutcome::Cancelled))
    );
    assert!(
        stopped
            .effects
            .iter()
            .any(|effect| matches!(effect, Effect::ArmTimer(Time(30))))
    );
}

#[test]
fn empty_ack_poll_arms_earlier_replay_operation_deadline() {
    let mut engine = make_engine();
    let started = engine.step(super::super::super::Event::StartAckWait {
        request: Box::new(make_request()),
        limits: limits(),
    });
    let poll = observe_effect(&started);
    let ordinary = engine.fresh_operation().unwrap();
    engine.outstanding = Some((ordinary, super::super::super::Stage::CheckingTail));
    engine.operation_due = Some(Time(2));
    engine.state.stage = super::super::super::Stage::CheckingTail;
    assert_eq!(engine.operation_deadline(ordinary), Some(Time(2)));
    assert_eq!(engine.operation_deadline(poll), None);
    let checked = engine.step(super::super::super::Event::AckChecked { op: poll });
    assert!(
        checked
            .effects
            .iter()
            .any(|effect| matches!(effect, Effect::ArmTimer(Time(2))))
    );
}

#[test]
fn simultaneous_ack_poll_and_snapshot_deadlines_process_both() {
    let mut config = super::super::super::Config {
        attempt_timeout_ms: 5,
        snapshot: Some(super::super::super::SnapshotConfig {
            max_metadata_bytes: 512,
            max_snapshot_bytes: 16,
            max_chunks: 4,
            max_chunk_bytes: 4,
            max_candidate_bytes: 32,
            max_total_ms: 5,
        }),
        ..super::super::super::Config::default()
    };
    let mut engine =
        SessionEngine::new(scope(), super::super::super::Mode::StateSync, config, 4).unwrap();
    assert!(
        engine
            .step(super::super::super::Event::StartSnapshot)
            .rejection
            .is_none()
    );
    let started = engine.step(super::super::super::Event::StartAckWait {
        request: Box::new(make_request()),
        limits: limits(),
    });
    let first = observe_effect(&started);
    assert!(
        started
            .effects
            .iter()
            .any(|effect| matches!(effect, Effect::ArmTimer(Time(5))))
    );
    let expired = engine.step(super::super::super::Event::Tick(Time(5)));
    assert_eq!(
        engine.state.stage,
        super::super::super::Stage::SnapshotAborted
    );
    assert!(
        expired
            .effects
            .iter()
            .any(|effect| matches!(effect, Effect::CleanupSnapshot { .. }))
    );
    let second = observe_effect(&expired);
    assert_ne!(first, second);
    assert!(engine.accepts_operation(second));
    let cleanup_expired = engine.step(super::super::super::Event::Tick(Time(10)));
    assert!(
        cleanup_expired
            .effects
            .iter()
            .any(|effect| matches!(effect, Effect::DiscardSnapshotResources { .. }))
    );
    assert!(
        cleanup_expired
            .effects
            .iter()
            .any(|effect| matches!(effect, Effect::ObserveNamedAcks { .. }))
    );

    config.snapshot = None;
    let mut serving =
        SessionEngine::new(scope(), super::super::super::Mode::StateSync, config, 5).unwrap();
    serving.ready = true;
    serving.state.stage = super::super::super::Stage::Ready;
    serving.state.materialized = Some(super::super::super::Cursor {
        scope: scope(),
        history: SourceHistory {
            source: "cas".into(),
            generation: 3,
        },
        position: vec![1],
    });
    let cursor = serving.state.materialized.clone().unwrap();
    serving.proof = Some(super::super::super::SourceProof {
        id: super::super::super::ProofId(vec![1]),
        head: cursor.clone(),
        retained_from: cursor,
        read_authority: true,
    });
    serving.mode_authority = true;
    serving.tail_due = Time(5);
    serving.step(super::super::super::Event::StartAckWait {
        request: Box::new(make_request()),
        limits: limits(),
    });
    let simultaneous = serving.step(super::super::super::Event::Tick(Time(5)));
    assert!(!serving.ready);
    let revoke = simultaneous
        .effects
        .iter()
        .position(|effect| matches!(effect, Effect::RevokeServing { .. }))
        .unwrap();
    let poll = simultaneous
        .effects
        .iter()
        .position(|effect| matches!(effect, Effect::ObserveNamedAcks { .. }))
        .unwrap();
    assert!(
        revoke < poll,
        "serving revocation precedes independent ack observation"
    );
}
