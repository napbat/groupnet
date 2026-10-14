//! Effects-driven source-call counts as registered and active scope counts grow.

use std::collections::VecDeque;

use groupnet_core::Time;
use groupnet_core::replication::{
    BoundComparison, Comparison, Config, Cursor, Effect, Event, IdlePolicy, Mode, ProofId, Scope,
    SessionEngine, SourceHistory, SourceProof, Step, Stream,
};

fn scope(index: usize) -> Scope {
    Scope {
        stream: Stream {
            group: "g".into(),
            topic: "state".into(),
            kind: "v1".into(),
        },
        partition: format!("p{index}"),
    }
}

fn cursor(scope: &Scope) -> Cursor {
    Cursor {
        scope: scope.clone(),
        history: SourceHistory {
            source: "cas".into(),
            generation: 1,
        },
        position: vec![1],
    }
}

fn unchanged_tail(op: groupnet_core::replication::Operation, from: Cursor) -> Event {
    let proof = SourceProof {
        id: ProofId(vec![1]),
        head: from.clone(),
        retained_from: from.clone(),
        read_authority: true,
    };
    let relation = BoundComparison {
        left: from.clone(),
        right: from,
        proof: proof.id.clone(),
        order: Comparison::Equal,
    };
    Event::Tail {
        op,
        proof,
        comparisons: vec![relation.clone(), relation.clone(), relation],
    }
}

fn enqueue(queue: &mut VecDeque<Effect>, step: Step) {
    assert!(step.rejection.is_none(), "{step:?}");
    queue.extend(step.effects);
}

fn source_calls(scopes: usize, active: usize, idle: bool) -> usize {
    let config = Config {
        idle: idle.then_some(IdlePolicy {
            unchanged_checks: 1,
            max_interval_ms: 80,
            jitter_ms: 0,
        }),
        tail_check_ms: 10,
        ..Config::default()
    };
    let mut engines = Vec::new();
    let mut queues = Vec::new();
    let mut timers: Vec<Vec<Time>> = Vec::new();
    for index in 0..scopes {
        let session = u64::try_from(index).expect("small scope count") + 1;
        let mut engine = SessionEngine::new(scope(index), Mode::StateSync, config, session)
            .expect("valid scope and policy");
        let mut queue = VecDeque::new();
        enqueue(&mut queue, engine.step(Event::Authority(true)));
        enqueue(
            &mut queue,
            engine.step(Event::Resume {
                cursor: cursor(&scope(index)),
            }),
        );
        engines.push(engine);
        queues.push(queue);
        timers.push(Vec::new());
    }

    let mut calls = 0;
    for time in 0..=100 {
        for index in 0..scopes {
            let engine = &mut engines[index];
            let queue = &mut queues[index];
            let mut sampled = false;
            if timers[index].contains(&Time(time)) {
                timers[index].retain(|due| *due != Time(time));
                enqueue(queue, engine.step(Event::Tick(Time(time))));
                sampled = true;
            }
            if index < active && time > 0 && time % 5 == 0 {
                if !sampled {
                    enqueue(queue, engine.step(Event::Tick(Time(time))));
                }
                enqueue(queue, engine.step(Event::Activity));
            }
            let mut immediate = 0;
            while let Some(effect) = queue.pop_front() {
                immediate += 1;
                assert!(immediate < 16, "unbounded immediate effects");
                match effect {
                    Effect::CheckTail { op, from, .. } => {
                        calls += 1;
                        enqueue(
                            queue,
                            engine.step(unchanged_tail(op, from.expect("resumed"))),
                        );
                    }
                    Effect::RevokeServing { op } => {
                        enqueue(queue, engine.step(Event::Invalidated { op }));
                    }
                    Effect::ArmTimer(due) => {
                        assert!(due >= Time(time));
                        timers[index].push(due);
                    }
                    other => panic!("unexpected effect {other:?}"),
                }
            }
        }
    }
    calls
}

#[test]
fn registered_scope_and_active_fraction_source_call_budget() {
    for scopes in [1, 4, 16] {
        let baseline = source_calls(scopes, 0, false);
        let idle = source_calls(scopes, 0, true);
        let mixed = source_calls(scopes, scopes / 2, true);
        let active = source_calls(scopes, scopes, true);
        println!(
            "scopes={scopes} baseline={baseline} idle={idle} half_active={mixed} active={active}"
        );
        assert!(idle < baseline, "scopes {scopes}: {idle} >= {baseline}");
        assert!(idle <= mixed && mixed <= active);
        assert!(active <= baseline + scopes);
        assert!(baseline <= scopes * 12);
    }
}
