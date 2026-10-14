//! Source/model-driven snapshot schedules with commits during cutover.

use std::collections::VecDeque;

use groupnet_core::Time;
use groupnet_core::replication::{
    ApplyReceipt, Batch, BoundComparison, ChunkReceipt, Comparison, Config, Coverage, Cursor,
    Effect, Event, HoldReceipt, Mode, ProofId, ReadDecision, Scope, SessionEngine, SnapshotConfig,
    SnapshotOffer, SourceHistory, SourceProof, Stage, Step, Stream,
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

fn pos(n: u8) -> Cursor {
    Cursor {
        scope: scope(),
        history: SourceHistory {
            source: "cas".into(),
            generation: 1,
        },
        position: vec![n],
    }
}

fn source_proof(head: u8) -> SourceProof {
    SourceProof {
        id: ProofId(vec![head, 1]),
        head: pos(head),
        retained_from: pos(1),
        read_authority: true,
    }
}

fn cmp(p: &SourceProof, left: u8, right: u8) -> BoundComparison {
    BoundComparison {
        left: pos(left),
        right: pos(right),
        proof: p.id.clone(),
        order: match left.cmp(&right) {
            std::cmp::Ordering::Less => Comparison::Before,
            std::cmp::Ordering::Equal => Comparison::Equal,
            std::cmp::Ordering::Greater => Comparison::After,
        },
    }
}

fn engine(session: u64) -> SessionEngine {
    SessionEngine::new(
        scope(),
        Mode::StateSync,
        Config {
            snapshot: Some(SnapshotConfig {
                max_metadata_bytes: 512,
                max_snapshot_bytes: 4,
                max_chunks: 2,
                max_chunk_bytes: 4,
                max_candidate_bytes: 16,
                max_total_ms: 500,
            }),
            max_batch_events: 16,
            attempt_timeout_ms: 30,
            ..Config::default()
        },
        session,
    )
    .unwrap()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Replica {
    Uninstalled,
    Deleted,
    Present(u8),
}

impl From<Option<u8>> for Replica {
    fn from(value: Option<u8>) -> Self {
        value.map_or(Self::Deleted, Self::Present)
    }
}

#[derive(Debug)]
struct Model {
    // Each source-native position is a committed replacement; None is delete.
    source: Vec<Option<u8>>,
    stage: Replica,
    live: Replica,
    cut: Option<u8>,
    barrier: Option<u8>,
    tail: Option<u8>,
    attach: Option<u8>,
    held: bool,
    cleanup: bool,
}

impl Model {
    fn new() -> Self {
        Self {
            source: vec![Some(1)],
            stage: Replica::Uninstalled,
            live: Replica::Uninstalled,
            cut: None,
            barrier: None,
            tail: None,
            attach: None,
            held: false,
            cleanup: false,
        }
    }

    fn head(&self) -> u8 {
        u8::try_from(self.source.len()).unwrap()
    }

    fn at(&self, n: u8) -> Option<u8> {
        self.source[usize::from(n - 1)]
    }

    fn commit(&mut self, value: Option<u8>) {
        self.source.push(value);
    }
}

fn enqueue(queue: &mut VecDeque<Effect>, step: Step, seed: u64) {
    assert!(step.rejection.is_none(), "seed {seed}: {step:?}");
    queue.extend(step.effects);
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one shaped seeded source/effect schedule keeps every cutover phase and its model assertion together"
)]
fn seeded_source_model_keeps_delete_and_every_commit_through_attach() {
    for seed in 0..96 {
        let mut rng = SplitMix64::new(seed);
        let mut e = engine(seed + 1);
        let mut m = Model::new();
        let mut queue = VecDeque::new();
        enqueue(&mut queue, e.step(Event::Authority(true)), seed);
        enqueue(&mut queue, e.step(Event::StartSnapshot), seed);
        let mut steps = 0;
        while let Some(effect) = queue.pop_front() {
            steps += 1;
            assert!(steps < 100, "seed {seed}: stalled at {:?}", e.state());
            if e.state().stage != Stage::Ready {
                assert!(
                    !matches!(e.read_decision(), ReadDecision::Serve(_)),
                    "seed {seed}"
                );
            }
            let next = match effect {
                Effect::AcquireSnapshotHold { op, total_due, .. } => {
                    assert_eq!(total_due, Time(500));
                    m.held = true;
                    e.step(Event::SnapshotHeld {
                        op,
                        receipt: HoldReceipt {
                            request: op,
                            history: pos(1).history,
                            retained_until: total_due,
                            certificate: vec![1],
                        },
                    })
                }
                Effect::OfferSnapshot { op, .. } => {
                    if rng.below(2) == 0 {
                        m.commit(Some(2));
                    }
                    let cut = m.head();
                    m.cut = Some(cut);
                    let p = source_proof(cut);
                    e.step(Event::SnapshotOffered {
                        op,
                        offer: Box::new(SnapshotOffer {
                            scope: scope(),
                            schema: vec![1],
                            cut: pos(cut),
                            proof: p.clone(),
                            cut_to_head: cmp(&p, cut, cut),
                            retained_to_cut: cmp(&p, 1, cut),
                            total_bytes: 1,
                            chunks: 1,
                            digest: vec![1],
                            certificate: vec![1],
                        }),
                    })
                }
                Effect::OpenSnapshotStage { op, offer, .. } => {
                    m.stage = Replica::from(m.at(offer.cut.position[0]));
                    e.step(Event::SnapshotOpened {
                        op,
                        charged_bytes: 1,
                    })
                }
                Effect::ReadSnapshotChunk {
                    op,
                    index,
                    offset,
                    max_bytes,
                } => {
                    assert_eq!((index, offset, max_bytes), (0, 0, 1));
                    e.step(Event::SnapshotRead {
                        op,
                        chunk: ChunkReceipt {
                            index,
                            offset,
                            bytes: 1,
                            payload_id: op.token,
                        },
                    })
                }
                Effect::WriteSnapshotChunk { op, chunk } => e.step(Event::SnapshotWritten {
                    op,
                    index: chunk.index,
                    through: 1,
                    charged_bytes: 1,
                }),
                Effect::VerifySnapshotImage { op, .. } => e.step(Event::SnapshotVerified {
                    op,
                    charged_bytes: 1,
                }),
                Effect::SnapshotReplayBarrier { op, from } => {
                    assert_eq!(from.position[0], m.cut.unwrap());
                    m.commit(None); // Delete strictly after image cut.
                    let head = m.head();
                    m.barrier = Some(head);
                    let p = source_proof(head);
                    e.step(Event::SnapshotBarrier {
                        op,
                        proof: p.clone(),
                        comparisons: vec![
                            cmp(&p, from.position[0], head),
                            cmp(&p, 1, from.position[0]),
                        ],
                    })
                }
                Effect::SnapshotScan {
                    op,
                    from,
                    max_events,
                    ..
                } => {
                    let head = m.barrier.unwrap();
                    let p = source_proof(head);
                    let count = usize::from(head - from.position[0]);
                    assert!(count <= max_events);
                    e.step(Event::SnapshotScanned {
                        op,
                        batch: Box::new(Batch {
                            coverage: Coverage {
                                from: from.clone(),
                                through: pos(head),
                                proof: p.id.clone(),
                                certificate: vec![1],
                            },
                            payload_id: op.token,
                            events: count,
                            bytes: count,
                            advance: cmp(&p, from.position[0], head),
                            end_to_head: cmp(&p, head, head),
                        }),
                    })
                }
                Effect::SnapshotApply { op, batch } => {
                    let head = batch.coverage.through.position[0];
                    m.stage = Replica::from(m.at(head));
                    e.step(Event::SnapshotApplied {
                        op,
                        through: pos(head),
                        charged_bytes: 1,
                    })
                }
                Effect::SealSnapshotStage { op, through } => {
                    assert_eq!(m.stage, Replica::from(m.at(through.position[0])));
                    assert_eq!(m.stage, Replica::Deleted); // The post-cut delete survived.
                    e.step(Event::SnapshotSealed {
                        op,
                        through,
                        payload_id: op.token,
                        charged_bytes: 1,
                    })
                }
                Effect::InstallSnapshot { op, cursor, .. } => {
                    m.live = m.stage;
                    e.step(Event::SnapshotInstalled {
                        op,
                        receipt: ApplyReceipt {
                            through: cursor,
                            durable: true,
                        },
                    })
                }
                Effect::AttachSnapshot { op, after } => {
                    assert_eq!(after.position[0], m.barrier.unwrap());
                    m.commit(if seed % 2 == 0 { None } else { Some(9) });
                    let head = m.head();
                    m.attach = Some(head);
                    let p = source_proof(head);
                    e.step(Event::SnapshotAttached {
                        op,
                        proof: p.clone(),
                        comparisons: vec![
                            cmp(&p, after.position[0], head),
                            cmp(&p, 1, after.position[0]),
                        ],
                    })
                }
                Effect::CheckTail { op, from, .. } => {
                    let from = from.expect("installed candidate").position[0];
                    if rng.below(3) == 0 {
                        m.commit(if seed % 2 == 0 { None } else { Some(10) });
                    }
                    let head = m.head();
                    m.tail = Some(head);
                    let p = source_proof(head);
                    let attach = m.attach.unwrap();
                    e.step(Event::Tail {
                        op,
                        proof: p.clone(),
                        comparisons: vec![
                            cmp(&p, from, head),
                            cmp(&p, from, 1),
                            cmp(&p, 1, head),
                            cmp(&p, from, attach),
                        ],
                    })
                }
                Effect::Scan { op, from, .. } => {
                    let head = m.tail.unwrap();
                    let p = source_proof(head);
                    let count = usize::from(head - from.position[0]);
                    e.step(Event::Scanned {
                        op,
                        batch: Box::new(Batch {
                            coverage: Coverage {
                                from: from.clone(),
                                through: pos(head),
                                proof: p.id.clone(),
                                certificate: vec![1],
                            },
                            payload_id: op.token,
                            events: count,
                            bytes: count,
                            advance: cmp(&p, from.position[0], head),
                            end_to_head: cmp(&p, head, head),
                        }),
                    })
                }
                Effect::Apply { op, batch } => {
                    let head = batch.coverage.through.position[0];
                    m.live = Replica::from(m.at(head));
                    e.step(Event::Applied {
                        op,
                        receipt: ApplyReceipt {
                            through: pos(head),
                            durable: false,
                        },
                    })
                }
                Effect::CleanupSnapshot { op, .. } => {
                    m.held = false;
                    m.cleanup = true;
                    e.step(Event::SnapshotCleaned { op })
                }
                Effect::ArmTimer(_) => continue,
                other => panic!("seed {seed}: unexpected effect {other:?}"),
            };
            enqueue(&mut queue, next, seed);
        }
        assert_eq!(e.state().stage, Stage::Ready, "seed {seed}");
        assert_eq!(e.state().materialized, Some(pos(m.head())), "seed {seed}");
        assert_eq!(m.live, Replica::from(m.at(m.head())), "seed {seed}");
        assert!(!m.held && m.cleanup, "seed {seed}");
        assert_eq!(
            e.read_decision(),
            ReadDecision::Serve(pos(m.head())),
            "seed {seed}"
        );
    }
}

#[test]
fn seeded_cancel_timeout_and_stale_reply_never_reopen_admission() {
    for seed in 0..96 {
        let mut e = engine(seed + 1);
        let first = e.step(Event::StartSnapshot);
        let hold = first
            .effects
            .iter()
            .find_map(|effect| match effect {
                Effect::AcquireSnapshotHold { op, .. } => Some(*op),
                _ => None,
            })
            .unwrap();
        let stopped = if seed % 2 == 0 {
            e.step(Event::Cancel)
        } else {
            e.step(Event::Tick(Time(30)))
        };
        assert!(
            stopped
                .effects
                .iter()
                .any(|effect| matches!(effect, Effect::CleanupSnapshot { .. }))
        );
        assert!(!e.accepts_operation(hold));
        assert!(
            e.step(Event::SnapshotHeld {
                op: hold,
                receipt: HoldReceipt {
                    request: hold,
                    history: pos(1).history,
                    retained_until: Time(500),
                    certificate: vec![1]
                }
            })
            .rejection
            .is_some()
        );
        assert!(!matches!(e.read_decision(), ReadDecision::Serve(_)));
        let mut restarted = engine(seed + 1000);
        let next = restarted.step(Event::StartSnapshot);
        assert!(next.rejection.is_none());
        assert!(
            restarted
                .step(Event::SnapshotHeld {
                    op: hold,
                    receipt: HoldReceipt {
                        request: hold,
                        history: pos(1).history,
                        retained_until: Time(500),
                        certificate: vec![1],
                    }
                })
                .rejection
                .is_some()
        );
    }
}
