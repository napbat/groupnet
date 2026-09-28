//! Seeded source-ack ambiguity, crash, duplicate, and retention-gap schedules.

use std::num::NonZeroU64;

use groupnet_core::Time;
use groupnet_core::replication::{
    Batch, BoundComparison, CommitSubscriberAck, Comparison, Config, Coverage,
    DurableDeliveryReceipt, Effect, Event, FencedCheckpoint, Mode, Operation, ProofId,
    RegisterReceipt, RegisterSubscriber, RetentionPolicy, Scope, SessionEngine, SourceHistory,
    SourceProof, Stage, Step, Stream, SubscriberAckReceipt, SubscriberId, SubscriberKey,
    SubscriptionEpoch, SubscriptionLimits,
};
use groupnet_sim::SplitMix64;

fn scope() -> Scope {
    Scope {
        stream: Stream {
            group: "g".into(),
            topic: "events".into(),
            kind: "v1".into(),
        },
        partition: "p".into(),
    }
}

fn cursor(at: u8) -> groupnet_core::replication::Cursor {
    groupnet_core::replication::Cursor {
        scope: scope(),
        history: SourceHistory {
            source: "cas".into(),
            generation: 1,
        },
        position: vec![at],
    }
}

fn comparison(a: u8, b: u8) -> BoundComparison {
    BoundComparison {
        left: cursor(a),
        right: cursor(b),
        proof: ProofId(vec![9]),
        order: match a.cmp(&b) {
            std::cmp::Ordering::Less => Comparison::Before,
            std::cmp::Ordering::Equal => Comparison::Equal,
            std::cmp::Ordering::Greater => Comparison::After,
        },
    }
}

fn proof(retained: u8) -> SourceProof {
    SourceProof {
        id: ProofId(vec![9]),
        head: cursor(2),
        retained_from: cursor(retained),
        read_authority: false,
    }
}

fn op(step: &Step, kind: &str) -> Operation {
    step.effects
        .iter()
        .find_map(|effect| match (kind, effect) {
            ("register", Effect::RegisterSubscriber { op, .. })
            | ("bind", Effect::BindSinkEpoch { op, .. })
            | ("tail", Effect::CheckSubscriberTail { op, .. })
            | ("scan", Effect::ScanSubscriber { op, .. })
            | ("apply", Effect::ApplySubscriberBatch { op, .. })
            | ("ack", Effect::CommitSubscriberAck { op, .. })
            | ("read", Effect::ReadSubscriberAck { op, .. }) => Some(*op),
            _ => None,
        })
        .expect("expected source or sink operation")
}

fn registered(session: u64, ordinal: u64, sink_at: u8) -> (SessionEngine, RegisterReceipt) {
    let mut engine = SessionEngine::new(scope(), Mode::EventComplete, Config::default(), session)
        .expect("valid engine");
    let request = RegisterSubscriber {
        key: SubscriberKey {
            scope: scope(),
            subscriber: SubscriberId {
                name: "billing".into(),
            },
        },
        incarnation: NonZeroU64::new(session).expect("nonzero"),
        start: cursor(1),
        policy: RetentionPolicy {
            fingerprint: vec![7],
            max_bytes: 1024,
            max_events: 100,
            max_age_ms: 1000,
            max_lag_events: 100,
        },
        request_id: session.to_le_bytes().to_vec(),
        expected_prior_ordinal: None,
        reset_from: None,
    };
    let start = engine.step(Event::StartSubscription {
        request: Box::new(request.clone()),
        limits: SubscriptionLimits::default(),
    });
    let receipt = RegisterReceipt {
        key: request.key.clone(),
        incarnation: request.incarnation,
        request_id: request.request_id,
        epoch: SubscriptionEpoch {
            ordinal: NonZeroU64::new(ordinal).expect("nonzero"),
            native: ordinal.to_le_bytes().to_vec(),
            history: cursor(1).history,
        },
        protected: cursor(1),
        proof: proof(0),
        retained_to_protected: comparison(0, 1),
        protected_to_head: comparison(1, 2),
        policy_fingerprint: vec![7],
    };
    let bind = engine.step(Event::SubscriberRegistered {
        op: op(&start, "register"),
        receipt: Box::new(receipt.clone()),
    });
    assert!(bind.rejection.is_none());
    let bound = engine.step(Event::SinkEpochBound {
        op: op(&bind, "bind"),
        checkpoint: Box::new(FencedCheckpoint {
            key: receipt.key.clone(),
            request_id: receipt.request_id.clone(),
            epoch: receipt.epoch.clone(),
            cursor: cursor(sink_at),
            durable: true,
        }),
        source_to_sink: Box::new(comparison(1, sink_at)),
        sink_to_head: Box::new(comparison(sink_at, 2)),
    });
    assert!(bound.rejection.is_none());
    (engine, receipt)
}

fn tail(engine: &mut SessionEngine, retained: u8) -> Step {
    let poll = engine.step(Event::PollSubscriber);
    let tail_op = op(&poll, "tail");
    engine.step(Event::SubscriberTail {
        op: tail_op,
        proof: proof(retained),
        comparisons: vec![
            comparison(retained, 1),
            comparison(1, 2),
            comparison(retained, 2),
        ],
    })
}

fn deliver(engine: &mut SessionEngine, receipt: &RegisterReceipt, sink_before: u8) -> Step {
    let scan = tail(engine, 0);
    let scanned = engine.step(Event::SubscriberScanned {
        op: op(&scan, "scan"),
        batch: Box::new(Batch {
            coverage: Coverage {
                from: cursor(1),
                through: cursor(2),
                proof: ProofId(vec![9]),
                certificate: vec![4],
            },
            payload_id: 1,
            events: 1,
            bytes: 8,
            advance: comparison(1, 2),
            end_to_head: comparison(2, 2),
        }),
    });
    assert!(scanned.rejection.is_none());
    assert!(
        !scanned
            .effects
            .iter()
            .any(|e| matches!(e, Effect::CommitSubscriberAck { .. }))
    );
    let apply_op = op(&scanned, "apply");
    let applied = engine.step(Event::SubscriberApplied {
        op: apply_op,
        receipt: Box::new(DurableDeliveryReceipt {
            operation: apply_op,
            key: receipt.key.clone(),
            epoch: receipt.epoch.clone(),
            previous_sink: cursor(sink_before),
            through: cursor(2),
            sink_cursor: cursor(2),
            ack_request_id: vec![44],
            durable: true,
        }),
        through_to_sink: Box::new(comparison(2, 2)),
        previous_to_sink: Box::new(comparison(sink_before, 2)),
    });
    assert!(applied.rejection.is_none());
    assert_eq!(engine.state().checkpoint, Some(cursor(2)));
    applied
}

fn ack_request(step: &Step) -> CommitSubscriberAck {
    step.effects
        .iter()
        .find_map(|effect| match effect {
            Effect::CommitSubscriberAck { request, .. } => Some(*request.clone()),
            _ => None,
        })
        .expect("sink durability must precede source ack")
}

#[test]
fn seeded_crash_ambiguity_and_retention_schedules_preserve_protected_ack() {
    let mut saw_gap = false;
    let mut saw_restart = false;
    let mut saw_unknown = false;
    for seed in 0..48 {
        let mut rng = SplitMix64::new(seed);
        let (mut engine, registration) = registered(seed + 1, seed + 10, 1);
        if rng.next_u64() % 5 == 0 {
            let result = tail(&mut engine, 2);
            assert_eq!(engine.state().stage, Stage::IrrecoverableGap);
            assert!(
                result
                    .effects
                    .iter()
                    .any(|effect| matches!(effect, Effect::IrrecoverableGap))
            );
            assert_eq!(engine.subscription_acknowledged(), Some(&cursor(1)));
            saw_gap = true;
            continue;
        }
        let applied = deliver(&mut engine, &registration, 1);
        let request = ack_request(&applied);
        let old_ack_op = op(&applied, "ack");
        assert_eq!(engine.subscription_acknowledged(), Some(&cursor(1)));
        if rng.next_u64() % 2 == 0 {
            // Process dies after sink commit and before source ack. A new
            // source epoch replays from the old protected ack without sink
            // rollback, then an old operation reply is rejected.
            let (mut recovered, fresh_registration) = registered(seed + 101, seed + 110, 2);
            let stale = recovered.step(Event::SubscriberAcked {
                op: old_ack_op,
                receipt: Box::new(SubscriberAckReceipt {
                    request: request.clone(),
                    durable: true,
                }),
            });
            assert!(stale.rejection.is_some());
            assert_eq!(recovered.subscription_acknowledged(), Some(&cursor(1)));
            let replayed = deliver(&mut recovered, &fresh_registration, 2);
            let ack = recovered.step(Event::SubscriberAcked {
                op: op(&replayed, "ack"),
                receipt: Box::new(SubscriberAckReceipt {
                    request: ack_request(&replayed),
                    durable: true,
                }),
            });
            assert!(ack.rejection.is_none());
            assert_eq!(recovered.subscription_acknowledged(), Some(&cursor(2)));
            saw_restart = true;
        } else {
            let retry = engine.step(Event::Failed { op: old_ack_op });
            assert_eq!(engine.state().stage, Stage::RetryWait);
            assert!(retry.rejection.is_none());
            let read = engine.step(Event::Tick(Time(1_000)));
            let read_op = op(&read, "read");
            let stale = engine.step(Event::SubscriberAcked {
                op: old_ack_op,
                receipt: Box::new(SubscriberAckReceipt {
                    request: request.clone(),
                    durable: true,
                }),
            });
            assert!(stale.rejection.is_some());
            let finished = engine.step(Event::SubscriberAckRead {
                op: read_op,
                receipt: Some(Box::new(SubscriberAckReceipt {
                    request,
                    durable: true,
                })),
            });
            assert!(finished.rejection.is_none());
            assert_eq!(engine.subscription_acknowledged(), Some(&cursor(2)));
            saw_unknown = true;
        }
    }
    assert!(saw_gap && saw_restart && saw_unknown);
}
