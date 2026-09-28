//! Seeded standalone schedules for the source-backed replay engine.
//!
//! The scheduler supplies virtual time, bounded source responses, and delayed
//! application receipts. It deliberately does not use sockets or the existing
//! membership simulation: these are replay-session protocol properties.

use groupnet_core::Time;
use groupnet_core::replication::{
    ApplyReceipt, Batch, BoundComparison, Comparison, Config, Coverage, Cursor, Effect, Event,
    Mode, Operation, ProofId, ReadDecision, Scope, SessionEngine, SourceHistory, SourceProof,
    Stage, Stream,
};
use groupnet_sim::SplitMix64;

fn scope(id: usize) -> Scope {
    Scope {
        stream: Stream {
            group: "cell".into(),
            topic: "replica".into(),
            kind: "v1".into(),
        },
        partition: format!("shard-{id}"),
    }
}

fn cursor(id: usize, position: u8) -> Cursor {
    Cursor {
        scope: scope(id),
        history: SourceHistory {
            source: "cas".into(),
            generation: 1,
        },
        position: vec![position],
    }
}

fn proof(id: usize, head: u8) -> SourceProof {
    SourceProof {
        id: ProofId(vec![head, 1]),
        head: cursor(id, head),
        retained_from: cursor(id, 1),
        read_authority: true,
    }
}

fn cmp(p: &SourceProof, left: Cursor, right: Cursor) -> BoundComparison {
    let order = match left.position[0].cmp(&right.position[0]) {
        std::cmp::Ordering::Less => Comparison::Before,
        std::cmp::Ordering::Equal => Comparison::Equal,
        std::cmp::Ordering::Greater => Comparison::After,
    };
    BoundComparison {
        left,
        right,
        proof: p.id.clone(),
        order,
    }
}

fn tail(id: usize, from: u8, head: u8, op: Operation) -> Event {
    let p = proof(id, head);
    Event::Tail {
        op,
        comparisons: vec![
            cmp(&p, cursor(id, from), p.head.clone()),
            cmp(&p, cursor(id, from), p.retained_from.clone()),
            cmp(&p, p.retained_from.clone(), p.head.clone()),
        ],
        proof: p,
    }
}

fn batch(id: usize, from: u8, through: u8, head: u8) -> Batch {
    let p = proof(id, head);
    Batch {
        coverage: Coverage {
            from: cursor(id, from),
            through: cursor(id, through),
            proof: p.id.clone(),
            certificate: vec![1],
        },
        payload_id: u64::from(through),
        events: usize::from(through - from),
        bytes: 16 * usize::from(through - from),
        advance: cmp(&p, cursor(id, from), cursor(id, through)),
        end_to_head: cmp(&p, cursor(id, through), p.head.clone()),
    }
}

#[derive(Clone)]
struct Queued {
    at: u64,
    node: usize,
    event: Event,
}

fn enqueue(rng: &mut SplitMix64, queue: &mut Vec<Queued>, now: u64, node: usize, event: Event) {
    queue.push(Queued {
        at: now + u64::from(rng.below(3)) * 10,
        node,
        event: event.clone(),
    });
    if rng.below(5) == 0 {
        queue.push(Queued {
            at: now + u64::from(rng.below(4)) * 10,
            node,
            event,
        });
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "standalone DST harness passes its explicit virtual schedule and model state"
)]
fn drive_effects(
    rng: &mut SplitMix64,
    queue: &mut Vec<Queued>,
    now: u64,
    node: usize,
    head: u8,
    partitioned: bool,
    applied: &mut [u8; 3],
    effects: Vec<Effect>,
) {
    for effect in effects {
        match effect {
            Effect::CheckTail { op, from, .. } if !partitioned => {
                let from = from.as_ref().map_or(1, |c| c.position[0]);
                enqueue(rng, queue, now, node, tail(node, from, head, op));
            }
            Effect::Scan { op, from, .. } if !partitioned => {
                let start = from.position[0];
                let through = head.min(start.saturating_add(2));
                if through > start {
                    enqueue(
                        rng,
                        queue,
                        now,
                        node,
                        Event::Scanned {
                            op,
                            batch: Box::new(batch(node, start, through, head)),
                        },
                    );
                }
            }
            Effect::Apply { op, batch } => {
                let through = batch.coverage.through.position[0];
                applied[node] = applied[node].max(through);
                enqueue(
                    rng,
                    queue,
                    now,
                    node,
                    Event::Applied {
                        op,
                        receipt: ApplyReceipt {
                            through: batch.coverage.through,
                            durable: true,
                        },
                    },
                );
            }
            Effect::RevokeServing { op } => {
                enqueue(rng, queue, now, node, Event::Invalidated { op });
            }
            _ => {}
        }
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one seeded schedule owns its replay model and safety/liveness assertions"
)]
fn seeded_loss_reorder_partition_and_restart_preserve_replay_safety() {
    for seed in 0..24 {
        let mut rng = SplitMix64::new(seed);
        let cfg = Config {
            tail_check_ms: 10,
            retry_ms: 10,
            attempt_timeout_ms: 200,
            max_retries: 8,
            ..Config::default()
        };
        let mut nodes: Vec<_> = (0..3)
            .map(|id| SessionEngine::new(scope(id), Mode::StateSync, cfg, id as u64 + 1).unwrap())
            .collect();
        let mut heads = [1_u8; 3];
        let mut app_applied = [1_u8; 3];
        let mut prior_materialized = [1_u8; 3];
        let mut queue = Vec::<Queued>::new();
        for (id, node) in nodes.iter_mut().enumerate() {
            let result = node.step(Event::Resume {
                cursor: cursor(id, 1),
            });
            node.step(Event::Authority(true));
            drive_effects(
                &mut rng,
                &mut queue,
                0,
                id,
                1,
                false,
                &mut app_applied,
                result.effects,
            );
        }
        for tick in 0..180_u64 {
            let now = tick * 10;
            let partitioned = (15..35).contains(&tick);
            if tick < 65 && rng.below(4) == 0 {
                let id = rng.below(3) as usize;
                heads[id] = heads[id].saturating_add(1);
                if rng.below(2) == 0 {
                    let step = nodes[id].step(Event::Hint);
                    drive_effects(
                        &mut rng,
                        &mut queue,
                        now,
                        id,
                        heads[id],
                        partitioned && id == 1,
                        &mut app_applied,
                        step.effects,
                    );
                }
            }
            if tick == 55 {
                let checkpoint = nodes[2].state().checkpoint.clone().unwrap();
                prior_materialized[2] = checkpoint.position[0];
                nodes[2] = SessionEngine::new(scope(2), Mode::StateSync, cfg, 99).unwrap();
                let step = nodes[2].step(Event::Resume { cursor: checkpoint });
                nodes[2].step(Event::Authority(true));
                drive_effects(
                    &mut rng,
                    &mut queue,
                    now,
                    2,
                    heads[2],
                    false,
                    &mut app_applied,
                    step.effects,
                );
            }
            for id in 0..3 {
                let result = nodes[id].step(Event::Tick(Time(now)));
                drive_effects(
                    &mut rng,
                    &mut queue,
                    now,
                    id,
                    heads[id],
                    partitioned && id == 1,
                    &mut app_applied,
                    result.effects,
                );
            }
            let mut index = 0;
            while index < queue.len() {
                if queue[index].at > now {
                    index += 1;
                    continue;
                }
                let item = queue.swap_remove(index);
                let id = item.node;
                let result = nodes[id].step(item.event);
                drive_effects(
                    &mut rng,
                    &mut queue,
                    now,
                    id,
                    heads[id],
                    partitioned && id == 1,
                    &mut app_applied,
                    result.effects,
                );
            }
            for (id, node) in nodes.iter().enumerate() {
                if let Some(materialized) = &node.state().materialized {
                    let position = materialized.position[0];
                    assert!(
                        position >= prior_materialized[id],
                        "seed {seed} node {id}: regressed within a session"
                    );
                    assert!(
                        position <= app_applied[id],
                        "seed {seed} node {id}: cursor outran application"
                    );
                    prior_materialized[id] = position;
                }
                if let Some(checkpoint) = &node.state().checkpoint {
                    assert!(
                        checkpoint.position[0] <= app_applied[id],
                        "seed {seed} node {id}: durable cursor outran application"
                    );
                }
                if let ReadDecision::Serve(through) = node.read_decision() {
                    assert_eq!(node.state().stage, Stage::Ready, "seed {seed} node {id}");
                    assert_eq!(
                        node.state().materialized,
                        Some(through.clone()),
                        "seed {seed} node {id}"
                    );
                    assert_eq!(node.state().head, Some(through), "seed {seed} node {id}");
                }
            }
        }
        for (id, node) in nodes.iter().enumerate() {
            assert_eq!(
                node.state().materialized,
                Some(cursor(id, heads[id])),
                "seed {seed} node {id}"
            );
            assert_eq!(
                node.state().checkpoint,
                Some(cursor(id, heads[id])),
                "seed {seed} node {id}"
            );
        }
    }
}
