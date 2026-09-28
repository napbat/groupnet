use super::*;
use crate::Time;
use crate::replication::{Event, IdlePolicy, ProofId, SourceHistory, Stream};

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
            generation: 1,
        },
        position: vec![n],
    }
}

fn proof(head: u8) -> SourceProof {
    SourceProof {
        id: ProofId(vec![head]),
        head: cursor(head),
        retained_from: cursor(1),
        read_authority: true,
    }
}

fn comparison(proof: &SourceProof, left: u8, right: u8) -> BoundComparison {
    BoundComparison {
        left: cursor(left),
        right: cursor(right),
        proof: proof.id.clone(),
        order: match left.cmp(&right) {
            std::cmp::Ordering::Less => Comparison::Before,
            std::cmp::Ordering::Equal => Comparison::Equal,
            std::cmp::Ordering::Greater => Comparison::After,
        },
    }
}

fn tail_op(step: &Step) -> Operation {
    step.effects
        .iter()
        .find_map(|effect| match effect {
            Effect::CheckTail { op, .. } => Some(*op),
            _ => None,
        })
        .expect("source tail effect")
}

fn respond_tail(engine: &mut SessionEngine, op: Operation, head: u8) -> Step {
    let source = proof(head);
    engine.step(Event::Tail {
        op,
        comparisons: vec![
            comparison(&source, 1, head),
            comparison(&source, 1, 1),
            comparison(&source, 1, head),
        ],
        proof: source,
    })
}

fn make_engine() -> SessionEngine {
    let config = Config {
        idle: Some(IdlePolicy {
            unchanged_checks: 1,
            max_interval_ms: 80,
            jitter_ms: 0,
        }),
        tail_check_ms: 10,
        ..Config::default()
    };
    let mut engine = SessionEngine::new(scope(), Mode::StateSync, config, 1).unwrap();
    assert!(engine.step(Event::Authority(true)).rejection.is_none());
    let first = engine.step(Event::Resume { cursor: cursor(1) });
    let ready = respond_tail(&mut engine, tail_op(&first), 1);
    assert!(ready.rejection.is_none(), "{ready:?}");
    assert!(matches!(engine.read_decision(), ReadDecision::Serve(_)));
    engine
}

#[test]
fn policy_bounds_are_explicit() {
    assert_eq!(
        Config {
            idle: Some(IdlePolicy {
                unchanged_checks: 0,
                max_interval_ms: 20,
                jitter_ms: 0,
            }),
            tail_check_ms: 10,
            ..Config::default()
        }
        .validate(),
        Err(ConfigError::Zero)
    );
    assert_eq!(
        Config {
            idle: Some(IdlePolicy {
                unchanged_checks: 1,
                max_interval_ms: 9,
                jitter_ms: 0,
            }),
            tail_check_ms: 10,
            ..Config::default()
        }
        .validate(),
        Err(ConfigError::Zero)
    );
}

#[test]
fn unchanged_idle_backoff_does_not_extend_freshness_or_serve_permission() {
    let mut engine = make_engine();
    let at_ten = engine.step(Event::Tick(Time(10)));
    let p = respond_tail(&mut engine, tail_op(&at_ten), 1);
    assert!(p.rejection.is_none());
    assert_eq!(engine.next_deadline(), Some(Time(20)));
    assert!(
        p.effects
            .iter()
            .any(|effect| matches!(effect, Effect::ArmTimer(Time(20))))
    );
    assert!(matches!(engine.read_decision(), ReadDecision::Serve(_)));
    let expiry = engine.step(Event::Tick(Time(20)));
    assert!(
        expiry
            .effects
            .iter()
            .any(|effect| matches!(effect, Effect::RevokeServing { .. }))
    );
    assert!(
        !expiry
            .effects
            .iter()
            .any(|effect| matches!(effect, Effect::CheckTail { .. }))
    );
    assert!(
        expiry
            .effects
            .iter()
            .any(|effect| matches!(effect, Effect::ArmTimer(Time(30))))
    );
    assert!(!matches!(engine.read_decision(), ReadDecision::Serve(_)));
    let later = engine.step(Event::Tick(Time(30)));
    assert!(
        later
            .effects
            .iter()
            .any(|effect| matches!(effect, Effect::CheckTail { .. }))
    );
}

#[test]
fn first_activity_restores_hot_cadence_without_per_read_checks() {
    let mut engine = make_engine();
    let at_ten = engine.step(Event::Tick(Time(10)));
    respond_tail(&mut engine, tail_op(&at_ten), 1);
    engine.step(Event::Tick(Time(15)));
    let first = engine.step(Event::Activity);
    assert!(
        !first
            .effects
            .iter()
            .any(|effect| matches!(effect, Effect::CheckTail { .. }))
    );
    assert_eq!(engine.next_deadline(), Some(Time(20)));
    for _ in 0..20 {
        assert!(engine.step(Event::Activity).effects.is_empty());
    }
    let due = engine.step(Event::Tick(Time(20)));
    assert_eq!(
        due.effects
            .iter()
            .filter(|effect| matches!(effect, Effect::CheckTail { .. }))
            .count(),
        1
    );
}

#[test]
fn freshness_revoke_survives_blocked_operation_and_mode_loss_rearms_poll() {
    let mut engine = make_engine();
    let at_ten = engine.step(Event::Tick(Time(10)));
    respond_tail(&mut engine, tail_op(&at_ten), 1);
    let unrelated = Operation {
        session: 1,
        generation: engine.state().generation,
        token: 1000,
    };
    engine.outstanding = Some((unrelated, Stage::Applying));
    engine.operation_due = Some(Time(40));
    engine.state.stage = Stage::Applying;
    let expired = engine.step(Event::Tick(Time(20)));
    assert!(
        expired
            .effects
            .iter()
            .any(|effect| matches!(effect, Effect::RevokeServing { .. }))
    );
    assert!(engine.outstanding.is_some());
    assert!(!matches!(engine.read_decision(), ReadDecision::Serve(_)));

    let mut mode = make_engine();
    let at_ten = mode.step(Event::Tick(Time(10)));
    respond_tail(&mut mode, tail_op(&at_ten), 1);
    mode.step(Event::Tick(Time(15)));
    let lost = mode.step(Event::Authority(false));
    assert!(
        lost.effects
            .iter()
            .any(|effect| matches!(effect, Effect::ArmTimer(Time(30))))
    );
    assert!(!matches!(mode.read_decision(), ReadDecision::Serve(_)));
}

#[test]
fn activity_cannot_bypass_retry_or_terminal_recovery_stage() {
    let mut engine = make_engine();
    engine.state.stage = Stage::RetryWait;
    engine.retry_due = Some(Time(50));
    let retry = engine.step(Event::Activity);
    assert!(retry.rejection.is_none());
    assert!(retry.effects.is_empty());
    assert_eq!(engine.state.stage, Stage::RetryWait);
    assert_eq!(engine.retry_due, Some(Time(50)));

    engine.state.stage = Stage::NeedsSnapshot;
    engine.retry_due = None;
    let gap = engine.step(Event::Activity);
    assert!(gap.rejection.is_none());
    assert!(gap.effects.is_empty());
    assert_eq!(engine.state.stage, Stage::NeedsSnapshot);
}

#[test]
fn activity_during_due_tail_does_not_queue_duplicate_source_check() {
    let mut engine = make_engine();
    let due = engine.step(Event::Tick(Time(10)));
    let op = tail_op(&due);
    let activity = engine.step(Event::Activity);
    assert!(activity.rejection.is_none());
    assert!(activity.effects.is_empty());
    assert!(!engine.pending_tail);
    let completed = respond_tail(&mut engine, op, 1);
    assert!(
        !completed
            .effects
            .iter()
            .any(|effect| matches!(effect, Effect::CheckTail { .. }))
    );
    assert_eq!(engine.next_deadline(), Some(Time(20)));
    assert_eq!(engine.tail_due, Time(20));
}

#[test]
fn authority_denied_source_checks_stay_at_hot_cadence() {
    let mut engine = make_engine();
    for time in [10, 20] {
        let due = engine.step(Event::Tick(Time(time)));
        let op = tail_op(&due);
        let mut denied = proof(1);
        denied.read_authority = false;
        let checked = engine.step(Event::Tail {
            op,
            comparisons: vec![
                comparison(&denied, 1, 1),
                comparison(&denied, 1, 1),
                comparison(&denied, 1, 1),
            ],
            proof: denied,
        });
        assert!(checked.rejection.is_none());
        assert_eq!(engine.tail_due, Time(time + 10));
        assert!(!matches!(engine.read_decision(), ReadDecision::Serve(_)));
    }
}
