//! Seeded missed-hint idle schedules driven only by core-emitted timers.

use std::collections::VecDeque;

use groupnet_core::Time;
use groupnet_core::replication::{
    ApplyReceipt, Batch, BoundComparison, Comparison, Config, Coverage, Cursor, Effect, Event,
    IdlePolicy, Mode, Operation, ProofId, ReadDecision, Scope, SessionEngine, SourceHistory,
    SourceProof, Step, Stream,
};
use groupnet_sim::SplitMix64;

fn scope() -> Scope {
    Scope {
        stream: Stream {
            group: "g".into(),
            topic: "state".into(),
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

fn compare(proof: &SourceProof, left: u8, right: u8) -> BoundComparison {
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

fn tail(op: Operation, from: u8, head: u8) -> Event {
    let source = proof(head);
    Event::Tail {
        op,
        comparisons: vec![
            compare(&source, from, head),
            compare(&source, from, 1),
            compare(&source, 1, head),
        ],
        proof: source,
    }
}

fn batch(op: Operation) -> Batch {
    let source = proof(2);
    Batch {
        coverage: Coverage {
            from: cursor(1),
            through: cursor(2),
            proof: source.id.clone(),
            certificate: vec![1],
        },
        payload_id: op.token,
        events: 1,
        bytes: 1,
        advance: compare(&source, 1, 2),
        end_to_head: compare(&source, 2, 2),
    }
}

fn enqueue(queue: &mut VecDeque<Effect>, step: Step, seed: u64) {
    assert!(step.rejection.is_none(), "seed {seed}: {step:?}");
    queue.extend(step.effects);
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one seeded source/replica schedule proves bounded stale serving and missed-hint discovery"
)]
fn idle_backoff_revokes_stale_serving_and_discovers_silent_commit() {
    let mut partitioned_seeds = 0;
    let mut saw_backoff = false;
    for seed in 0..64 {
        let mut rng = SplitMix64::new(seed);
        let committed_value = u8::try_from(10 + rng.below(100)).unwrap();
        let partitioned = seed % 5 == 0;
        let config = Config {
            idle: Some(IdlePolicy {
                unchanged_checks: 1,
                max_interval_ms: 40,
                jitter_ms: 5,
            }),
            tail_check_ms: 10,
            retry_ms: 2,
            max_retries: 8,
            ..Config::default()
        };
        let mut engine = SessionEngine::new(scope(), Mode::StateSync, config, seed + 1).unwrap();
        let mut queue = VecDeque::new();
        let mut timers = Vec::new();
        let mut source_head = 1u8;
        let mut app_cursor = 1u8;
        let mut app_value = 1u8;
        let mut source_calls = 0;
        let mut first_idle_poll = None;
        let mut stale_refused = false;
        let mut recovered = false;
        enqueue(&mut queue, engine.step(Event::Authority(true)), seed);
        enqueue(
            &mut queue,
            engine.step(Event::Resume { cursor: cursor(1) }),
            seed,
        );
        for time in 0..=80 {
            if time == 11 {
                source_head = 2; // Committed without a hint or any later write.
            }
            if timers.contains(&Time(time)) {
                timers.retain(|due| *due != Time(time));
                enqueue(&mut queue, engine.step(Event::Tick(Time(time))), seed);
            }
            let mut effects = 0;
            while let Some(effect) = queue.pop_front() {
                effects += 1;
                assert!(effects < 25, "seed {seed}: unbounded immediate work");
                match effect {
                    Effect::CheckTail { op, from, .. } => {
                        source_calls += 1;
                        if time > 10 && first_idle_poll.is_none() {
                            first_idle_poll = Some(time);
                        }
                        if partitioned && (30..36).contains(&time) {
                            enqueue(&mut queue, engine.step(Event::Failed { op }), seed);
                            partitioned_seeds += 1;
                        } else {
                            enqueue(
                                &mut queue,
                                engine.step(tail(op, from.unwrap().position[0], source_head)),
                                seed,
                            );
                        }
                    }
                    Effect::Scan { op, from, .. } => {
                        assert_eq!(from, cursor(1), "seed {seed}");
                        enqueue(
                            &mut queue,
                            engine.step(Event::Scanned {
                                op,
                                batch: Box::new(batch(op)),
                            }),
                            seed,
                        );
                    }
                    Effect::Apply { op, batch } => {
                        assert_eq!(batch.coverage.through, cursor(2));
                        app_cursor = 2;
                        app_value = committed_value;
                        enqueue(
                            &mut queue,
                            engine.step(Event::Applied {
                                op,
                                receipt: ApplyReceipt {
                                    through: cursor(2),
                                    durable: true,
                                },
                            }),
                            seed,
                        );
                    }
                    Effect::RevokeServing { op } => {
                        enqueue(&mut queue, engine.step(Event::Invalidated { op }), seed);
                    }
                    Effect::ArmTimer(due) => {
                        assert!(due >= Time(time), "seed {seed}");
                        timers.push(due);
                    }
                    other => panic!("seed {seed}: unexpected effect {other:?}"),
                }
            }
            if time >= 20 && app_cursor == 1 {
                stale_refused |= !matches!(engine.read_decision(), ReadDecision::Serve(_));
                assert!(
                    !matches!(engine.read_decision(), ReadDecision::Serve(_)),
                    "seed {seed}"
                );
            }
            if app_cursor == source_head
                && app_value == committed_value
                && matches!(engine.read_decision(), ReadDecision::Serve(_))
            {
                recovered = true;
                break;
            }
        }
        assert!(
            stale_refused && recovered,
            "seed {seed}: failed safety/liveness"
        );
        assert!(source_calls >= 3, "seed {seed}");
        assert!(first_idle_poll.is_some_and(|time| time > 20), "seed {seed}");
        saw_backoff = true;
    }
    assert!(saw_backoff && partitioned_seeds > 0);
}
