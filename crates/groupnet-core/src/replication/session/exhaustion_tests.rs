//! Terminal generation exhaustion still disposes snapshot resources.

use super::SessionEngine;
use crate::replication::{
    Config, Effect, Event, Mode, Operation, Reject, Scope, SnapshotCleanupDisposition,
    SnapshotConfig, Stage, Stream,
};

fn engine() -> SessionEngine {
    SessionEngine::new(
        Scope {
            stream: Stream {
                group: "g".into(),
                topic: "t".into(),
                kind: "v1".into(),
            },
            partition: "p".into(),
        },
        Mode::StateSync,
        Config {
            snapshot: Some(SnapshotConfig {
                max_metadata_bytes: 128,
                max_snapshot_bytes: 16,
                max_chunks: 2,
                max_chunk_bytes: 8,
                max_candidate_bytes: 32,
                max_total_ms: 100,
            }),
            ..Config::default()
        },
        1,
    )
    .unwrap()
}

#[test]
fn cancel_at_generation_limit_keeps_active_hold_cleanup_and_clears_work() {
    let mut session = engine();
    session.state.generation = u64::MAX;
    let start = session.step(Event::StartSnapshot);
    let acquire = start
        .effects
        .iter()
        .find_map(|effect| match effect {
            Effect::AcquireSnapshotHold { op, .. } => Some(*op),
            _ => None,
        })
        .unwrap();
    let terminal = session.step(Event::Cancel);
    assert_eq!(terminal.rejection, Some(Reject::Exhausted));
    assert!(terminal.effects.iter().any(|effect| matches!(effect,
        Effect::CleanupSnapshot { attempt, disposition: SnapshotCleanupDisposition::Aborted, .. }
        if *attempt == acquire)));
    assert_eq!(session.state.stage, Stage::RetryExhausted);
    assert!(session.outstanding.is_none());
    assert!(session.operation_due.is_none());
    assert!(session.proof.is_none());
    assert!(!session.ready);
    assert!(!session.accepts_operation(acquire));
}

#[test]
fn supersede_at_generation_limit_discards_live_attachment_and_revokes_serve() {
    let mut session = engine();
    session.state.generation = u64::MAX;
    session.state.stage = Stage::Ready;
    session.ready = true;
    let attempt = Operation {
        session: 1,
        generation: u64::MAX,
        token: 7,
    };
    session.live_attachment_attempt = Some(attempt);
    session.next_token = 8;
    let terminal = session.step(Event::Supersede);
    assert_eq!(terminal.rejection, Some(Reject::Exhausted));
    assert!(
        terminal
            .effects
            .iter()
            .any(|effect| matches!(effect, Effect::RevokeServingUnconfirmed))
    );
    let discard = terminal
        .effects
        .iter()
        .find_map(|effect| match effect {
            Effect::DiscardSnapshotResources {
                op,
                attempt: owned,
                disposition: SnapshotCleanupDisposition::Aborted,
            } if *owned == attempt => Some(*op),
            _ => None,
        })
        .expect("tagged attachment disposal");
    assert_eq!(session.state.stage, Stage::RetryExhausted);
    assert!(!session.ready);
    assert!(session.live_attachment_attempt.is_none());
    assert!(
        session
            .step(Event::SnapshotDiscarded {
                op: discard,
                disposition: SnapshotCleanupDisposition::Aborted
            })
            .rejection
            .is_none()
    );
}
