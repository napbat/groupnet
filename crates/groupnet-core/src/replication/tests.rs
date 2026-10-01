use super::*;
use crate::Time;

fn scope() -> Scope {
    Scope {
        stream: Stream {
            group: "g".into(),
            topic: "t".into(),
            kind: "v1".into(),
        },
        partition: "p".into(),
    }
}

fn cursor(n: u8) -> Cursor {
    Cursor {
        scope: scope(),
        history: SourceHistory {
            source: "cas".into(),
            generation: 7,
        },
        position: vec![n],
    }
}

fn proof(head: u8, low: u8) -> SourceProof {
    SourceProof {
        id: ProofId(vec![head, low]),
        head: cursor(head),
        retained_from: cursor(low),
        read_authority: true,
    }
}

fn relation(p: &SourceProof, a: u8, b: u8) -> BoundComparison {
    BoundComparison {
        left: cursor(a),
        right: cursor(b),
        proof: p.id.clone(),
        order: match a.cmp(&b) {
            std::cmp::Ordering::Less => Comparison::Before,
            std::cmp::Ordering::Equal => Comparison::Equal,
            std::cmp::Ordering::Greater => Comparison::After,
        },
    }
}

fn tail(p: SourceProof, from: u8, op: Operation) -> Event {
    Event::Tail {
        op,
        comparisons: vec![
            relation(&p, from, p.head.position[0]),
            relation(&p, from, p.retained_from.position[0]),
            relation(&p, p.retained_from.position[0], p.head.position[0]),
        ],
        proof: p,
    }
}

fn tail_op(step: &Step) -> Operation {
    step.effects
        .iter()
        .find_map(|e| match e {
            Effect::CheckTail { op, .. } => Some(*op),
            _ => None,
        })
        .expect("tail effect")
}

fn scan_op(step: &Step) -> Operation {
    step.effects
        .iter()
        .find_map(|e| match e {
            Effect::Scan { op, .. } => Some(*op),
            _ => None,
        })
        .expect("scan effect")
}

fn apply_op(step: &Step) -> Operation {
    step.effects
        .iter()
        .find_map(|e| match e {
            Effect::Apply { op, .. } => Some(*op),
            _ => None,
        })
        .expect("apply effect")
}

fn batch(p: &SourceProof, from: u8, through: u8) -> Batch {
    Batch {
        coverage: Coverage {
            from: cursor(from),
            through: cursor(through),
            proof: p.id.clone(),
            certificate: vec![1],
        },
        payload_id: 9,
        events: usize::from(through - from),
        bytes: 32,
        advance: relation(p, from, through),
        end_to_head: relation(p, through, p.head.position[0]),
    }
}

#[test]
fn configuration_and_identity_are_bounded() {
    assert_eq!(
        Config {
            max_cursor_bytes: 0,
            ..Config::default()
        }
        .validate(),
        Err(ConfigError::Zero)
    );
    let mut bad = cursor(1);
    bad.scope.partition = "elsewhere".into();
    assert_eq!(bad.validate(&scope(), 16), Err(IdentityError::WrongScope));
    let mut huge = cursor(1);
    huge.position = vec![1; 17];
    assert_eq!(huge.validate(&scope(), 16), Err(IdentityError::TooLarge));
    let mut long_name = cursor(1);
    long_name.history.source = "x".repeat(17);
    assert_eq!(
        long_name.validate(&scope(), 16),
        Err(IdentityError::TooLarge)
    );
}

#[test]
fn source_replay_needs_application_completion_and_durable_cursor_is_separate() {
    let mut e = SessionEngine::new(scope(), Mode::StateSync, Config::default(), 1).unwrap();
    let first = e.step(Event::Resume { cursor: cursor(1) });
    e.step(Event::Authority(true));
    let p = proof(3, 1);
    let scanned = e.step(tail(p.clone(), 1, tail_op(&first)));
    assert_eq!(e.state().materialized, Some(cursor(1)));
    assert_eq!(e.read_decision(), ReadDecision::Refuse(Refusal::Unready));
    let apply = e.step(Event::Scanned {
        op: scan_op(&scanned),
        batch: Box::new(batch(&p, 1, 3)),
    });
    assert_eq!(e.state().materialized, Some(cursor(1)));
    let done = e.step(Event::Applied {
        op: apply_op(&apply),
        receipt: ApplyReceipt {
            through: cursor(3),
            durable: false,
        },
    });
    assert_eq!(e.state().materialized, Some(cursor(3)));
    assert_eq!(e.state().checkpoint, Some(cursor(1)));
    e.step(tail(proof(3, 1), 3, tail_op(&done)));
    assert_eq!(e.read_decision(), ReadDecision::Serve(cursor(3)));
}

#[test]
fn gap_policy_and_bad_comparisons_fail_closed() {
    for (mode, expected) in [
        (Mode::StateSync, Stage::NeedsSnapshot),
        (Mode::EventComplete, Stage::IrrecoverableGap),
    ] {
        let mut e = SessionEngine::new(scope(), mode, Config::default(), 1).unwrap();
        let first = e.step(Event::Resume { cursor: cursor(1) });
        e.step(tail(proof(5, 3), 1, tail_op(&first)));
        assert_eq!(e.state().stage, expected);
        assert!(!matches!(e.read_decision(), ReadDecision::Serve(_)));
    }
    let mut e = SessionEngine::new(scope(), Mode::StateSync, Config::default(), 1).unwrap();
    let first = e.step(Event::Resume { cursor: cursor(1) });
    let mut bad = tail(proof(2, 1), 1, tail_op(&first));
    if let Event::Tail { comparisons, .. } = &mut bad {
        for comparison in comparisons {
            comparison.proof = ProofId(vec![99]);
        }
    }
    assert_eq!(e.step(bad).rejection, Some(Reject::Comparison));
    assert_ne!(e.state().stage, Stage::Ready);
}

#[test]
fn pending_work_coalesces_and_late_operations_are_rejected() {
    let mut e = SessionEngine::new(scope(), Mode::StateSync, Config::default(), 1).unwrap();
    let first = e.step(Event::Resume { cursor: cursor(1) });
    let p = proof(3, 1);
    let scan = e.step(tail(p.clone(), 1, tail_op(&first)));
    assert_eq!(
        e.step(Event::Hint).effects,
        [] as [crate::replication::Effect; 0]
    );
    let apply = e.step(Event::Scanned {
        op: scan_op(&scan),
        batch: Box::new(batch(&p, 1, 3)),
    });
    assert_eq!(
        e.step(Event::Tick(Time(5000))).effects,
        [] as [crate::replication::Effect; 0]
    );
    assert_eq!(
        e.step(Event::Applied {
            op: apply_op(&apply),
            receipt: ApplyReceipt {
                through: cursor(3),
                durable: true
            }
        })
        .rejection,
        None
    );
    assert_eq!(e.state().checkpoint, Some(cursor(3)));
    assert_eq!(
        e.step(Event::Applied {
            op: apply_op(&apply),
            receipt: ApplyReceipt {
                through: cursor(3),
                durable: true
            }
        })
        .rejection,
        Some(Reject::StaleOperation)
    );
    e.step(Event::Cancel);
    assert_eq!(e.state().stage, Stage::Cancelled);
    assert_eq!(
        e.step(Event::Tick(Time(100_000))).effects,
        [] as [crate::replication::Effect; 0]
    );
    assert_ne!(e.state().stage, Stage::Ready);
}

#[test]
fn same_or_backward_batch_cannot_advance_and_retry_is_bounded() {
    let cfg = Config {
        max_retries: 2,
        ..Config::default()
    };
    let mut e = SessionEngine::new(scope(), Mode::StateSync, cfg, 1).unwrap();
    let first = e.step(Event::Resume { cursor: cursor(1) });
    let p = proof(3, 1);
    let scan = e.step(tail(p.clone(), 1, tail_op(&first)));
    let mut same = batch(&p, 1, 2);
    same.advance.order = Comparison::Equal;
    assert_eq!(
        e.step(Event::Scanned {
            op: scan_op(&scan),
            batch: Box::new(same)
        })
        .rejection,
        Some(Reject::Discontinuity)
    );
    e.step(Event::Failed { op: scan_op(&scan) });
    let retry = e.step(Event::Tick(Time(1000)));
    assert!(
        retry
            .effects
            .iter()
            .any(|x| matches!(x, Effect::CheckTail { .. }))
    );
    e.step(Event::Failed {
        op: tail_op(&retry),
    });
    assert_eq!(e.state().stage, Stage::RetryExhausted);
    assert_eq!(
        e.step(Event::Tick(Time(2000))).effects,
        [] as [crate::replication::Effect; 0]
    );
}

#[test]
fn restart_incarnation_rejects_old_reply_even_when_generation_and_token_repeat() {
    let mut before = SessionEngine::new(scope(), Mode::StateSync, Config::default(), 41).unwrap();
    let old = tail_op(&before.step(Event::Resume { cursor: cursor(1) }));
    let mut after = SessionEngine::new(scope(), Mode::StateSync, Config::default(), 42).unwrap();
    let current = tail_op(&after.step(Event::Resume { cursor: cursor(1) }));
    assert_eq!(
        (old.generation, old.token),
        (current.generation, current.token)
    );
    assert_ne!(old.session, current.session);
    assert_eq!(
        after.step(tail(proof(1, 1), 1, old)).rejection,
        Some(Reject::StaleOperation)
    );
    assert_eq!(after.step(tail(proof(1, 1), 1, current)).rejection, None);
}

#[test]
fn retry_wait_holds_back_hints_until_due_and_revocation_needs_its_token() {
    let mut e = SessionEngine::new(scope(), Mode::StateSync, Config::default(), 1).unwrap();
    let first = e.step(Event::Resume { cursor: cursor(1) });
    e.step(tail(proof(1, 1), 1, tail_op(&first)));
    let refresh = e.step(Event::Hint);
    let revoked = refresh
        .effects
        .iter()
        .find_map(|effect| match effect {
            Effect::RevokeServing { op } => Some(*op),
            _ => None,
        })
        .expect("ready serving is revoked");
    let mut wrong = revoked;
    wrong.token += 1;
    assert_eq!(
        e.step(Event::Invalidated { op: wrong }).rejection,
        Some(Reject::StaleOperation)
    );
    assert_eq!(e.step(Event::Invalidated { op: revoked }).rejection, None);
    e.step(Event::Failed {
        op: tail_op(&refresh),
    });
    assert_eq!(e.state().stage, Stage::RetryWait);
    assert_eq!(
        e.step(Event::Tick(Time(999))).effects,
        [] as [crate::replication::Effect; 0]
    );
    assert_eq!(e.step(Event::Hint).rejection, Some(Reject::Stage));
    assert!(
        e.step(Event::Tick(Time(1000)))
            .effects
            .iter()
            .any(|effect| matches!(effect, Effect::CheckTail { .. }))
    );
}

#[test]
fn native_floor_coalesces_without_skipping_an_unseen_source_commit() {
    let mut e = SessionEngine::new(scope(), Mode::StateSync, Config::default(), 1).unwrap();
    e.step(Event::Authority(true));
    let first = e.step(Event::Resume { cursor: cursor(1) });
    e.step(tail(proof(1, 1), 1, tail_op(&first)));
    assert_eq!(e.read_decision(), ReadDecision::Serve(cursor(1)));

    let demand = e.step(Event::Demand {
        cursor: cursor(3),
        comparison: None,
    });
    assert_eq!(e.state().target, Some(cursor(3)));
    let p1 = proof(1, 1);
    assert_eq!(
        e.step(Event::Demand {
            cursor: cursor(2),
            comparison: Some(relation(&p1, 3, 2)),
        })
        .rejection,
        None
    );
    assert_eq!(e.state().target, Some(cursor(3)));
    let mut wrong_scope = cursor(4);
    wrong_scope.scope.partition = "other".into();
    assert_eq!(
        e.step(Event::Demand {
            cursor: wrong_scope,
            comparison: None
        })
        .rejection,
        Some(Reject::Identity(IdentityError::WrongScope))
    );
    let mut wrong_history = cursor(4);
    wrong_history.history.generation += 1;
    assert_eq!(
        e.step(Event::Demand {
            cursor: wrong_history,
            comparison: None
        })
        .rejection,
        Some(Reject::History)
    );
    let mut unbound = relation(&p1, 3, 4);
    unbound.proof = ProofId(vec![99]);
    assert_eq!(
        e.step(Event::Demand {
            cursor: cursor(4),
            comparison: Some(unbound)
        })
        .rejection,
        Some(Reject::Comparison)
    );
    assert_eq!(e.state().target, Some(cursor(3)));

    let mut held = tail(p1.clone(), 1, tail_op(&demand));
    if let Event::Tail { comparisons, .. } = &mut held {
        comparisons.push(relation(&p1, 1, 3));
    }
    e.step(held);
    assert_eq!(e.state().stage, Stage::Unready);
    assert!(!matches!(e.read_decision(), ReadDecision::Serve(_)));
    let check = e.step(Event::Tick(Time(5000)));
    let p3 = proof(3, 1);
    let scan = e.step(tail(p3.clone(), 1, tail_op(&check)));
    let apply = e.step(Event::Scanned {
        op: scan_op(&scan),
        batch: Box::new(batch(&p3, 1, 3)),
    });
    assert_eq!(e.state().materialized, Some(cursor(1)));
    let checked = e.step(Event::Applied {
        op: apply_op(&apply),
        receipt: ApplyReceipt {
            through: cursor(3),
            durable: true,
        },
    });
    let mut settled = tail(p3.clone(), 3, tail_op(&checked));
    if let Event::Tail { comparisons, .. } = &mut settled {
        comparisons.push(relation(&p3, 3, 3));
    }
    e.step(settled);
    assert_eq!(e.state().target, Some(cursor(3)));
    assert_eq!(e.read_decision(), ReadDecision::Serve(cursor(3)));
}

#[test]
fn terminal_states_and_active_tail_check_ignore_unrelated_wakeups() {
    let mut e = SessionEngine::new(scope(), Mode::EventComplete, Config::default(), 1).unwrap();
    let first = e.step(Event::Resume { cursor: cursor(1) });
    assert_eq!(
        e.step(Event::Tick(Time(5))).effects,
        [] as [crate::replication::Effect; 0]
    );
    e.step(tail(proof(3, 2), 1, tail_op(&first)));
    assert_eq!(e.state().stage, Stage::IrrecoverableGap);
    assert_eq!(
        e.step(Event::Tick(Time(5000))).effects,
        [] as [crate::replication::Effect; 0]
    );
    assert_eq!(e.step(Event::Hint).rejection, Some(Reject::Stage));
    assert_eq!(
        e.step(Event::Demand {
            cursor: cursor(3),
            comparison: None
        })
        .rejection,
        Some(Reject::Stage)
    );
    assert_eq!(
        e.step(Event::Authority(true)).effects,
        [] as [crate::replication::Effect; 0]
    );
    assert_eq!(e.state().stage, Stage::IrrecoverableGap);
}

fn ready_session(session_id: u64) -> SessionEngine {
    let mut engine =
        SessionEngine::new(scope(), Mode::StateSync, Config::default(), session_id).unwrap();
    engine.step(Event::Authority(true));
    let first = engine.step(Event::Resume { cursor: cursor(1) });
    engine.step(tail(proof(1, 1), 1, tail_op(&first)));
    assert_eq!(engine.read_decision(), ReadDecision::Serve(cursor(1)));
    engine
}

fn revocation(step: &Step) -> Operation {
    step.effects
        .iter()
        .find_map(|effect| match effect {
            Effect::RevokeServing { op } => Some(*op),
            _ => None,
        })
        .expect("revocation effect")
}

#[test]
fn cancel_fences_old_work_and_accepts_only_new_generation_revocation_once() {
    let mut engine = ready_session(51);
    let old_generation = engine.state().generation;
    let cancelled = engine.step(Event::Cancel);
    let revoke = revocation(&cancelled);
    assert_eq!(revoke.generation, old_generation + 1);
    assert_eq!(engine.state().stage, Stage::Cancelled);
    assert_eq!(
        engine.read_decision(),
        ReadDecision::Refuse(Refusal::Unready)
    );
    let stale = Operation {
        generation: old_generation,
        ..revoke
    };
    assert_eq!(
        engine.step(Event::Invalidated { op: stale }).rejection,
        Some(Reject::StaleOperation)
    );
    assert_eq!(
        engine.step(Event::Invalidated { op: revoke }).rejection,
        None
    );
    assert_eq!(
        engine.step(Event::Invalidated { op: revoke }).rejection,
        Some(Reject::StaleOperation)
    );
    assert_eq!(engine.state().stage, Stage::Cancelled);
    assert_eq!(
        engine.step(Event::Tick(Time(50_000))).effects,
        [] as [crate::replication::Effect; 0]
    );
}

#[test]
fn supersede_fences_old_revocation_and_new_one_is_acknowledgeable_while_unready() {
    let mut engine = ready_session(52);
    let superseded = engine.step(Event::Supersede);
    let current = revocation(&superseded);
    assert_eq!(engine.state().stage, Stage::Unready);
    assert_eq!(
        engine.step(Event::Invalidated { op: current }).rejection,
        None
    );
    assert_eq!(
        engine.step(Event::Invalidated { op: current }).rejection,
        Some(Reject::StaleOperation)
    );

    let mut with_prior = ready_session(53);
    let prior = revocation(&with_prior.step(Event::Hint));
    let cancelled = with_prior.step(Event::Cancel);
    assert_eq!(with_prior.state().stage, Stage::Cancelled);
    assert!(
        cancelled
            .effects
            .iter()
            .all(|effect| !matches!(effect, Effect::RevokeServing { .. }))
    );
    assert_eq!(
        with_prior.step(Event::Invalidated { op: prior }).rejection,
        Some(Reject::StaleOperation)
    );
}
