//! Poll timeout, partition, and session-restart schedules for named waits.

use std::collections::VecDeque;

use groupnet_core::Time;
use groupnet_core::replication::{
    AckEvidence, AckKind, AckTarget, AckWaitLimits, AckWaitOutcome, AckWaitRequest,
    CertifiedRoster, Config, Cursor, Effect, Event, Mode, ProofId, RequiredSubscriber, Scope,
    SessionEngine, SourceHistory, SourceProof, Step, Stream,
};
use groupnet_sim::SplitMix64;

fn scope() -> Scope {
    Scope {
        stream: Stream {
            group: "g".into(),
            topic: "writes".into(),
            kind: "v1".into(),
        },
        partition: "p".into(),
    }
}

fn cursor() -> Cursor {
    Cursor {
        scope: scope(),
        history: SourceHistory {
            source: "cas".into(),
            generation: 1,
        },
        position: vec![9],
    }
}

fn request(kind: AckKind) -> AckWaitRequest {
    let target = match kind {
        AckKind::Invalidated => AckTarget::Intent {
            scope: scope(),
            history: cursor().history,
            id: vec![9],
        },
        AckKind::Materialized => AckTarget::Cursor(cursor()),
    };
    AckWaitRequest {
        request_id: vec![7],
        target: target.clone(),
        kind,
        roster: CertifiedRoster {
            target,
            kind,
            policy_version: 1,
            certificate: vec![4],
            required: vec![
                RequiredSubscriber {
                    name: "a".into(),
                    incarnation: 1,
                    epoch: vec![1],
                },
                RequiredSubscriber {
                    name: "b".into(),
                    incarnation: 2,
                    epoch: vec![2],
                },
            ],
        },
        due: Time(50),
    }
}

fn limits() -> AckWaitLimits {
    AckWaitLimits {
        max_required: 2,
        max_identity_bytes: 32,
        max_certificate_bytes: 32,
        max_metadata_bytes: 256,
        max_wait_ms: 50,
        poll_ms: 2,
    }
}

fn make_engine(session: u64, now: u32) -> SessionEngine {
    let mut engine = SessionEngine::new(
        scope(),
        Mode::StateSync,
        Config {
            attempt_timeout_ms: 4,
            ..Config::default()
        },
        session,
    )
    .unwrap();
    let start = engine.step(Event::Tick(Time(u64::from(now))));
    let op = start
        .effects
        .iter()
        .find_map(|effect| match effect {
            Effect::CheckTail { op, .. } => Some(*op),
            _ => None,
        })
        .unwrap();
    let source = cursor();
    let gap = engine.step(Event::Tail {
        op,
        proof: SourceProof {
            id: ProofId(vec![1]),
            head: source.clone(),
            retained_from: source,
            read_authority: false,
        },
        comparisons: Vec::new(),
    });
    assert!(gap.rejection.is_none());
    engine
}

fn enqueue(queue: &mut VecDeque<Effect>, step: Step, seed: u64) {
    assert!(step.rejection.is_none(), "seed {seed}: {step:?}");
    queue.extend(step.effects);
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one seeded partition/restart schedule asserts timeout, stale-response, and post-heal liveness floors"
)]
fn expired_polls_and_old_sessions_cannot_steal_healed_acknowledgements() {
    let mut saw_timeout = false;
    let mut saw_drop = false;
    let mut saw_restart = false;
    let mut saw_stale = false;
    let mut completed = [false; 2];
    for seed in 0..48 {
        let mut rng = SplitMix64::new(seed);
        let kind = if seed % 2 == 0 {
            AckKind::Invalidated
        } else {
            AckKind::Materialized
        };
        let req = request(kind);
        let restart = seed % 3 == 0;
        let mut engine = make_engine(seed + 1, 0);
        let mut queue = VecDeque::new();
        enqueue(
            &mut queue,
            engine.step(Event::StartAckWait {
                request: Box::new(req.clone()),
                limits: limits(),
            }),
            seed,
        );
        let mut wait_op = engine.ack_wait_operation().unwrap();
        let mut timers = Vec::new();
        let mut delayed = Vec::<(u32, Event)>::new();
        let mut last_poll = None;
        let mut old_cancelled = false;
        let mut terminal = None;
        for time in 0..=50 {
            if restart && time == 8 {
                let old_poll = last_poll.expect("pre-restart source poll");
                let old = engine.step(Event::Supersede);
                old_cancelled = old.effects.iter().any(|effect| matches!(effect,
                    Effect::AckWaitFinished { op, outcome: AckWaitOutcome::Cancelled } if *op == wait_op));
                assert!(old_cancelled, "seed {seed}");
                engine = make_engine(seed + 100, time);
                timers.clear();
                delayed.push((time + 1, Event::AckChecked { op: old_poll }));
                enqueue(
                    &mut queue,
                    engine.step(Event::StartAckWait {
                        request: Box::new(req.clone()),
                        limits: limits(),
                    }),
                    seed,
                );
                wait_op = engine.ack_wait_operation().unwrap();
                saw_restart = true;
            }
            let mut index = 0;
            while index < delayed.len() {
                if delayed[index].0 <= time {
                    let (_, event) = delayed.swap_remove(index);
                    enqueue(
                        &mut queue,
                        engine.step(Event::Tick(Time(u64::from(time)))),
                        seed,
                    );
                    let result = engine.step(event);
                    if result.rejection.is_some() {
                        saw_stale = true;
                    } else {
                        queue.extend(result.effects);
                    }
                } else {
                    index += 1;
                }
            }
            if timers.contains(&Time(u64::from(time))) {
                timers.retain(|due| *due != Time(u64::from(time)));
                let prior = last_poll;
                enqueue(
                    &mut queue,
                    engine.step(Event::Tick(Time(u64::from(time)))),
                    seed,
                );
                if prior.is_some_and(|op| !engine.accepts_operation(op)) && time < 50 {
                    saw_timeout = true;
                }
            }
            let mut operations = 0;
            while let Some(effect) = queue.pop_front() {
                operations += 1;
                assert!(operations < 20, "seed {seed}: unbounded source work");
                match effect {
                    Effect::ObserveNamedAcks {
                        op,
                        request,
                        waiting,
                        due,
                    } => {
                        assert_ne!(op, wait_op);
                        assert!(due <= request.due);
                        assert_ne!(
                            waiting,
                            [] as [groupnet_core::replication::RequiredSubscriber; 0]
                        );
                        last_poll = Some(op);
                        if time < 15 {
                            if time == 0 || rng.below(3) == 0 {
                                saw_drop = true;
                            } else {
                                delayed.push((time + 6, Event::AckChecked { op }));
                            }
                        } else {
                            let subscriber = waiting[0].clone();
                            delayed.push((
                                time + 1,
                                Event::AckObserved {
                                    evidence: Box::new(AckEvidence {
                                        op,
                                        request_id: request.request_id.clone(),
                                        target: request.target.clone(),
                                        kind: request.kind,
                                        roster_certificate: request.roster.certificate.clone(),
                                        subscriber,
                                    }),
                                },
                            ));
                        }
                    }
                    Effect::ArmTimer(due) => {
                        assert!(due >= Time(u64::from(time)));
                        timers.push(due);
                    }
                    Effect::AckWaitFinished { op, outcome } => {
                        assert_eq!(op, wait_op, "seed {seed}");
                        terminal = Some(outcome);
                    }
                    other => panic!("seed {seed}: unrelated effect {other:?}"),
                }
            }
            if terminal.is_some() {
                break;
            }
        }
        assert_eq!(terminal, Some(AckWaitOutcome::Satisfied), "seed {seed}");
        assert_eq!(engine.ack_wait_operation(), None);
        if restart {
            assert!(old_cancelled);
        }
        completed[usize::from(seed % 2 != 0)] = true;
    }
    assert!(saw_timeout && saw_drop && saw_restart && saw_stale);
    assert_eq!(completed, [true, true]);
}
