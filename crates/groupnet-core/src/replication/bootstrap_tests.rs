//! Deterministic private checkpoint bootstrap transition tests.

use super::{
    ApplyReceipt, BoundComparison, Comparison, Config, ConfigError, Cursor, Effect, Event,
    IdentityError, Mode, Operation, ProofId, ReadDecision, Refusal, Reject, Scope, SessionEngine,
    SourceHistory, SourceProof, Stage, Step, Stream,
};
use crate::Time;

fn scope() -> Scope {
    Scope {
        stream: Stream {
            group: "g".into(),
            topic: "state".into(),
            kind: "v1".into(),
        },
        partition: "shard".into(),
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

fn engine() -> SessionEngine {
    SessionEngine::new(
        scope(),
        Mode::StateSync,
        Config {
            retry_ms: 5,
            attempt_timeout_ms: 10,
            max_retries: 3,
            ..Config::default()
        },
        72,
    )
    .unwrap()
}

#[test]
fn constructor_rejects_empty_and_oversized_scope_before_emitting_work() {
    let mut empty = scope();
    empty.stream.topic.clear();
    assert!(matches!(
        SessionEngine::new(empty, Mode::StateSync, Config::default(), 1),
        Err(ConfigError::Identity(IdentityError::Empty))
    ));
    let mut oversized = scope();
    oversized.partition = "x".repeat(17);
    assert!(matches!(
        SessionEngine::new(
            oversized,
            Mode::StateSync,
            Config {
                max_cursor_bytes: 16,
                ..Config::default()
            },
            1
        ),
        Err(ConfigError::Identity(IdentityError::TooLarge))
    ));
}

fn load_op(step: &Step) -> Operation {
    step.effects
        .iter()
        .find_map(|effect| match effect {
            Effect::LoadCheckpoint { op, .. } => Some(*op),
            _ => None,
        })
        .expect("load effect")
}

fn install(step: &Step) -> (Operation, u64) {
    step.effects
        .iter()
        .find_map(|effect| match effect {
            Effect::InstallCheckpoint { op, payload_id, .. } => Some((*op, *payload_id)),
            _ => None,
        })
        .expect("install effect")
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

#[test]
fn bootstrap_load_and_install_use_distinct_shared_tokens_before_tail() {
    let mut engine = engine();
    let first = engine.step(Event::StartBootstrap);
    let load = load_op(&first);
    assert_eq!(engine.current_operation(), Some(load));
    assert_eq!(engine.next_deadline(), Some(Time(10)));
    assert!(engine.accepts_operation(load));
    assert_eq!(
        engine.read_decision(),
        ReadDecision::Refuse(Refusal::Unready)
    );
    let loaded = engine.step(Event::CheckpointLoaded {
        op: load,
        cursor: Some(cursor(2)),
        payload_id: Some(load.token),
    });
    let (install_op, payload) = install(&loaded);
    assert_eq!(payload, load.token);
    assert_ne!(install_op, load);
    assert!(!engine.accepts_operation(load));
    assert!(engine.accepts_operation(install_op));
    assert_eq!(engine.state().stage, Stage::InstallingCheckpoint);
    let resumed = engine.step(Event::CheckpointInstalled {
        op: install_op,
        receipt: ApplyReceipt {
            through: cursor(2),
            durable: true,
        },
    });
    let tail = resumed
        .effects
        .iter()
        .find_map(|effect| match effect {
            Effect::CheckTail { op, .. } => Some(*op),
            _ => None,
        })
        .unwrap();
    assert_ne!(tail, install_op);
    assert_eq!(tail.generation, load.generation);
    assert_eq!(engine.state().checkpoint, Some(cursor(2)));
    assert_eq!(engine.state().materialized, Some(cursor(2)));
    assert_eq!(engine.state().stage, Stage::CheckingTail);
    assert_eq!(
        engine.read_decision(),
        ReadDecision::Refuse(Refusal::Unready)
    );
}

#[test]
fn absent_checkpoint_checks_source_then_requires_snapshot() {
    let mut engine = engine();
    let load = load_op(&engine.step(Event::StartBootstrap));
    let next = engine.step(Event::CheckpointLoaded {
        op: load,
        cursor: None,
        payload_id: None,
    });
    let tail = next
        .effects
        .iter()
        .find_map(|effect| match effect {
            Effect::CheckTail { op, .. } => Some(*op),
            _ => None,
        })
        .unwrap();
    assert!(tail.token > load.token);
    let proof = super::SourceProof {
        id: super::ProofId(vec![1]),
        head: cursor(1),
        retained_from: cursor(1),
        read_authority: true,
    };
    let needs = engine.step(Event::Tail {
        op: tail,
        proof,
        comparisons: vec![],
    });
    assert!(needs.effects.contains(&Effect::NeedsSnapshot));
    assert_eq!(engine.state().stage, Stage::NeedsSnapshot);
    assert!(engine.state().checkpoint.is_none());
}

#[test]
fn timeout_retries_private_load_and_cannot_accept_old_reply() {
    let mut engine = engine();
    let first = load_op(&engine.step(Event::StartBootstrap));
    let timeout = engine.step(Event::Tick(Time(10)));
    assert_eq!(engine.state().stage, Stage::RetryWait);
    assert_eq!(engine.next_deadline(), Some(Time(15)));
    assert!(!engine.accepts_operation(first));
    assert!(timeout.effects.contains(&Effect::ArmTimer(Time(15))));
    let retry = load_op(&engine.step(Event::Tick(Time(15))));
    assert_ne!(retry, first);
    assert_eq!(engine.current_operation(), Some(retry));
    assert_eq!(
        engine
            .step(Event::CheckpointLoaded {
                op: first,
                cursor: Some(cursor(1)),
                payload_id: Some(first.token),
            })
            .rejection,
        Some(Reject::StaleOperation)
    );
    assert!(engine.accepts_operation(retry));
}

#[test]
fn invalid_install_receipt_and_cancel_never_publish_checkpoint() {
    let mut engine = engine();
    let load = load_op(&engine.step(Event::StartBootstrap));
    let (install_op, _) = install(&engine.step(Event::CheckpointLoaded {
        op: load,
        cursor: Some(cursor(3)),
        payload_id: Some(load.token),
    }));
    for bad in [
        ApplyReceipt {
            through: cursor(3),
            durable: false,
        },
        ApplyReceipt {
            through: cursor(2),
            durable: true,
        },
    ] {
        assert_eq!(
            engine
                .step(Event::CheckpointInstalled {
                    op: install_op,
                    receipt: bad,
                })
                .rejection,
            Some(Reject::Discontinuity)
        );
        assert!(engine.state().checkpoint.is_none());
    }
    engine.step(Event::Cancel);
    assert_eq!(engine.state().stage, Stage::Cancelled);
    assert!(!engine.accepts_operation(install_op));
    assert_eq!(
        engine
            .step(Event::CheckpointInstalled {
                op: install_op,
                receipt: ApplyReceipt {
                    through: cursor(3),
                    durable: true,
                },
            })
            .rejection,
        Some(Reject::StaleOperation)
    );
    assert!(engine.state().checkpoint.is_none());
}

#[test]
fn install_timeout_reloads_from_source_not_old_payload() {
    let mut engine = engine();
    let load = load_op(&engine.step(Event::StartBootstrap));
    let (install_op, _) = install(&engine.step(Event::CheckpointLoaded {
        op: load,
        cursor: Some(cursor(4)),
        payload_id: Some(load.token),
    }));
    engine.step(Event::Tick(Time(10)));
    assert_eq!(engine.state().stage, Stage::RetryWait);
    assert!(!engine.accepts_operation(install_op));
    let next = load_op(&engine.step(Event::Tick(Time(15))));
    assert_ne!(next, load);
    assert_ne!(next, install_op);
}

#[test]
fn incompatible_floor_queued_during_bootstrap_does_not_poison_valid_checkpoint() {
    let mut engine = engine();
    engine.step(Event::Authority(true));
    let load = load_op(&engine.step(Event::StartBootstrap));
    let mut wrong = cursor(9);
    wrong.history.generation = 99;
    assert_eq!(
        engine
            .step(Event::Demand {
                cursor: wrong,
                comparison: None,
            })
            .rejection,
        None
    );
    let (install_op, _) = install(&engine.step(Event::CheckpointLoaded {
        op: load,
        cursor: Some(cursor(2)),
        payload_id: Some(load.token),
    }));
    assert!(engine.state().target.is_none());
    let mut wrong_during_install = cursor(8);
    wrong_during_install.history.generation = 98;
    assert_eq!(
        engine
            .step(Event::Demand {
                cursor: wrong_during_install,
                comparison: None,
            })
            .rejection,
        None
    );
    let next = engine.step(Event::CheckpointInstalled {
        op: install_op,
        receipt: ApplyReceipt {
            through: cursor(2),
            durable: true,
        },
    });
    assert!(engine.state().target.is_none());
    let tail_op = next
        .effects
        .iter()
        .find_map(|effect| match effect {
            Effect::CheckTail { op, .. } => Some(*op),
            _ => None,
        })
        .unwrap();
    let p2 = proof(2);
    engine.step(Event::Tail {
        op: tail_op,
        proof: p2.clone(),
        comparisons: vec![
            comparison(&p2, 2, 2),
            comparison(&p2, 2, 1),
            comparison(&p2, 1, 2),
        ],
    });
    assert_eq!(engine.state().stage, Stage::Ready);
    assert_eq!(engine.read_decision(), ReadDecision::Serve(cursor(2)));
    assert_eq!(
        engine
            .step(Event::Demand {
                cursor: cursor(3),
                comparison: None,
            })
            .rejection,
        None
    );
    assert_eq!(engine.state().target, Some(cursor(3)));
}
