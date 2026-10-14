//! Seeded fixed-roster acknowledgement schedules with missed hints and stale polls.

use std::collections::VecDeque;

use groupnet_core::Time;
use groupnet_core::replication::{
    AckEvidence, AckKind, AckTarget, AckWaitLimits, AckWaitOutcome, AckWaitRequest,
    CertifiedRoster, Config, Cursor, Effect, Event, Mode, Operation, ProofId, RequiredSubscriber,
    Scope, SessionEngine, SourceHistory, SourceProof, Stream,
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

fn member(name: &str, incarnation: u64) -> RequiredSubscriber {
    RequiredSubscriber {
        name: name.into(),
        incarnation,
        epoch: vec![u8::try_from(incarnation).unwrap()],
    }
}

fn request() -> AckWaitRequest {
    let target = AckTarget::Intent {
        scope: scope(),
        history: SourceHistory {
            source: "cas".into(),
            generation: 1,
        },
        id: vec![9],
    };
    AckWaitRequest {
        request_id: vec![7],
        target: target.clone(),
        kind: AckKind::Invalidated,
        roster: CertifiedRoster {
            target,
            kind: AckKind::Invalidated,
            policy_version: 1,
            certificate: vec![4],
            required: vec![member("a", 1), member("b", 2)],
        },
        due: Time(40),
    }
}

fn limits() -> AckWaitLimits {
    AckWaitLimits {
        max_required: 2,
        max_identity_bytes: 64,
        max_certificate_bytes: 64,
        max_metadata_bytes: 512,
        max_wait_ms: 40,
        poll_ms: 3,
    }
}

fn enqueue(queue: &mut VecDeque<Effect>, step: groupnet_core::replication::Step, seed: u64) {
    assert!(step.rejection.is_none(), "seed {seed}: {step:?}");
    queue.extend(step.effects);
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one seeded source/effect schedule checks fixed-roster safety and finite missed-hint liveness together"
)]
fn named_waits_keep_fixed_epochs_and_find_acks_without_hints() {
    for seed in 0..80 {
        let mut rng = SplitMix64::new(seed);
        let mut engine =
            SessionEngine::new(scope(), Mode::StateSync, Config::default(), seed + 1).unwrap();
        // Put the unrelated replica replay side into NeedsSnapshot. The named
        // source-certified wait must progress even with no local read state.
        let ordinary = engine.step(Event::Tick(Time(0)));
        let tail = ordinary
            .effects
            .iter()
            .find_map(|effect| match effect {
                Effect::CheckTail { op, .. } => Some(*op),
                _ => None,
            })
            .expect("initial tail check");
        let cursor = Cursor {
            scope: scope(),
            history: SourceHistory {
                source: "cas".into(),
                generation: 1,
            },
            position: vec![1],
        };
        let ignored = engine.step(Event::Tail {
            op: tail,
            proof: SourceProof {
                id: ProofId(vec![1]),
                head: cursor.clone(),
                retained_from: cursor,
                read_authority: false,
            },
            comparisons: Vec::new(),
        });
        assert!(ignored.rejection.is_none(), "seed {seed}");
        let req = request();
        let activation = if seed % 17 == 0 {
            [0, 0]
        } else {
            [
                2 + rng.below(18),
                if seed % 4 == 0 { 50 } else { 3 + rng.below(18) },
            ]
        };
        let cancel_at = (seed % 11 == 0).then_some(5 + rng.below(13));
        let lost_at = (seed % 13 == 0).then_some(6 + rng.below(12));
        let mut queue = VecDeque::new();
        enqueue(
            &mut queue,
            engine.step(Event::StartAckWait {
                request: Box::new(req.clone()),
                limits: limits(),
            }),
            seed,
        );
        let wait_op = engine.ack_wait_operation().unwrap();
        let mut seen = [false; 2];
        let mut timers = Vec::new();
        let mut last_poll: Option<Operation> = None;
        let mut terminal = None;
        for time in 0..=40 {
            if terminal.is_none() {
                if cancel_at == Some(time) {
                    enqueue(
                        &mut queue,
                        engine.step(Event::CancelAckWait { op: wait_op }),
                        seed,
                    );
                } else if lost_at == Some(time) {
                    enqueue(
                        &mut queue,
                        engine.step(Event::AckAuthorityLost { op: wait_op }),
                        seed,
                    );
                } else if timers.contains(&Time(u64::from(time))) {
                    timers.retain(|due| *due != Time(u64::from(time)));
                    enqueue(
                        &mut queue,
                        engine.step(Event::Tick(Time(u64::from(time)))),
                        seed,
                    );
                }
            }
            let mut operations = 0;
            while let Some(effect) = queue.pop_front() {
                operations += 1;
                assert!(operations < 20, "seed {seed}: unbounded poll loop");
                match effect {
                    Effect::ObserveNamedAcks {
                        op,
                        request,
                        waiting,
                        due,
                    } => {
                        assert_ne!(op, wait_op);
                        assert!(due >= Time(u64::from(time)));
                        let expected_waiting: Vec<_> = request
                            .roster
                            .required
                            .iter()
                            .enumerate()
                            .filter_map(|(index, member)| (!seen[index]).then_some(member.clone()))
                            .collect();
                        assert_eq!(waiting, expected_waiting, "seed {seed}");
                        if let Some(stale) = last_poll {
                            assert_ne!(stale, op);
                            assert!(
                                engine
                                    .step(Event::AckChecked { op: stale })
                                    .rejection
                                    .is_some()
                            );
                        }
                        last_poll = Some(op);
                        let ready = waiting.iter().find_map(|member| {
                            let index = request
                                .roster
                                .required
                                .iter()
                                .position(|required| required == member)?;
                            (time >= activation[index]).then_some(index)
                        });
                        if let Some(index) = ready {
                            let subscriber = request.roster.required[index].clone();
                            let wrong = AckEvidence {
                                op,
                                request_id: request.request_id.clone(),
                                target: request.target.clone(),
                                kind: AckKind::Materialized,
                                roster_certificate: request.roster.certificate.clone(),
                                subscriber: subscriber.clone(),
                            };
                            assert!(
                                engine
                                    .step(Event::AckObserved {
                                        evidence: Box::new(wrong)
                                    })
                                    .rejection
                                    .is_some()
                            );
                            enqueue(
                                &mut queue,
                                engine.step(Event::AckObserved {
                                    evidence: Box::new(AckEvidence {
                                        op,
                                        request_id: request.request_id.clone(),
                                        target: request.target.clone(),
                                        kind: request.kind,
                                        roster_certificate: request.roster.certificate.clone(),
                                        subscriber,
                                    }),
                                }),
                                seed,
                            );
                            seen[index] = true;
                        } else {
                            enqueue(&mut queue, engine.step(Event::AckChecked { op }), seed);
                        }
                    }
                    Effect::AckWaitFinished { op, outcome } => {
                        assert_eq!(op, wait_op);
                        assert!(
                            terminal.replace(outcome).is_none(),
                            "seed {seed}: repeated outcome"
                        );
                    }
                    Effect::ArmTimer(due) => {
                        assert!(due >= Time(u64::from(time)));
                        timers.push(due);
                    }
                    other => panic!("seed {seed}: unrelated effect {other:?}"),
                }
            }
            if terminal.is_some() {
                break;
            }
        }
        match terminal.as_ref().expect("finite wait outcome") {
            AckWaitOutcome::Satisfied => assert_eq!(seen, [true, true], "seed {seed}"),
            AckWaitOutcome::TimedOut(waiting) => {
                let expected: Vec<_> = req
                    .roster
                    .required
                    .iter()
                    .enumerate()
                    .filter_map(|(index, member)| (!seen[index]).then_some(member.clone()))
                    .collect();
                assert_eq!(*waiting, expected, "seed {seed}");
                assert_eq!(activation[1], 50, "seed {seed}");
            }
            AckWaitOutcome::Cancelled => assert!(cancel_at.is_some(), "seed {seed}"),
            AckWaitOutcome::AuthorityLost => assert!(lost_at.is_some(), "seed {seed}"),
            AckWaitOutcome::Pending(_) => panic!("seed {seed}: nonterminal result"),
        }
        if cancel_at.is_none() && lost_at.is_none() && activation[1] != 50 {
            assert_eq!(terminal, Some(AckWaitOutcome::Satisfied), "seed {seed}");
        }
        assert_eq!(engine.ack_wait_operation(), None, "seed {seed}");
    }
}
