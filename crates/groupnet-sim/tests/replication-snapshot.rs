//! Seeded virtual-time state-sync snapshot cutover schedules.

use groupnet_core::Time;
use groupnet_core::replication::{
    ApplyReceipt, Batch, BoundComparison, ChunkReceipt, Comparison, Config, Coverage, Cursor,
    Effect, Event, HoldReceipt, Mode, Operation, ProofId, ReadDecision, Refusal, Scope,
    SessionEngine, SnapshotConfig, SnapshotOffer, SourceHistory, SourceProof, Stage, Step, Stream,
};
use groupnet_sim::SplitMix64;

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

fn pos(n: u8) -> Cursor {
    Cursor {
        scope: scope(),
        history: SourceHistory {
            source: "cas".into(),
            generation: 2,
        },
        position: vec![n],
    }
}

fn proof(head: u8, low: u8) -> SourceProof {
    SourceProof {
        id: ProofId(vec![head, low]),
        head: pos(head),
        retained_from: pos(low),
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

fn op(step: &Step) -> Operation {
    step.effects
        .iter()
        .find_map(|effect| match effect {
            Effect::AcquireSnapshotHold { op, .. }
            | Effect::OfferSnapshot { op, .. }
            | Effect::OpenSnapshotStage { op, .. }
            | Effect::ReadSnapshotChunk { op, .. }
            | Effect::WriteSnapshotChunk { op, .. }
            | Effect::VerifySnapshotImage { op, .. }
            | Effect::SnapshotReplayBarrier { op, .. }
            | Effect::SnapshotScan { op, .. }
            | Effect::SnapshotApply { op, .. }
            | Effect::SealSnapshotStage { op, .. }
            | Effect::InstallSnapshot { op, .. }
            | Effect::AttachSnapshot { op, .. }
            | Effect::CheckTail { op, .. }
            | Effect::Scan { op, .. }
            | Effect::Apply { op, .. }
            | Effect::CleanupSnapshot { op, .. } => Some(*op),
            _ => None,
        })
        .expect("issued operation")
}

fn engine(session: u64) -> SessionEngine {
    SessionEngine::new(
        scope(),
        Mode::StateSync,
        Config {
            snapshot: Some(SnapshotConfig {
                max_metadata_bytes: 512,
                max_snapshot_bytes: 16,
                max_chunks: 4,
                max_chunk_bytes: 4,
                max_candidate_bytes: 64,
                max_total_ms: 80,
            }),
            attempt_timeout_ms: 12,
            ..Config::default()
        },
        session,
    )
    .unwrap()
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one cutover schedule asserts every core transition from source hold through attach"
)]
fn seeded_concurrent_commit_cut_barrier_attach_and_delayed_reply() {
    for seed in 0..128 {
        let mut rng = SplitMix64::new(seed);
        let mut e = engine(seed + 1);
        let hold = op(&e.step(Event::StartSnapshot));
        assert_eq!(e.read_decision(), ReadDecision::Refuse(Refusal::Unready));
        let delay = u64::from(rng.below(11));
        e.step(Event::Tick(Time(delay)));
        if delay >= 12 {
            unreachable!();
        }
        let offer_op = op(&e.step(Event::SnapshotHeld {
            op: hold,
            receipt: HoldReceipt {
                request: hold,
                history: pos(1).history,
                retained_until: Time(80),
                certificate: vec![1],
            },
        }));
        // The source image is a consistent cut at 1. A commit before barrier
        // and another between barrier and attach must both be replayed.
        let p = proof(2, 1);
        let offered = SnapshotOffer {
            scope: scope(),
            schema: vec![1],
            cut: pos(1),
            proof: p.clone(),
            cut_to_head: cmp(&p, 1, 2),
            retained_to_cut: cmp(&p, 1, 1),
            total_bytes: 1,
            chunks: 1,
            digest: vec![1],
            certificate: vec![1],
        };
        let open = op(&e.step(Event::SnapshotOffered {
            op: offer_op,
            offer: Box::new(offered),
        }));
        let read = op(&e.step(Event::SnapshotOpened {
            op: open,
            charged_bytes: 1,
        }));
        let chunk = ChunkReceipt {
            index: 0,
            offset: 0,
            bytes: 1,
            payload_id: read.token,
        };
        let write = op(&e.step(Event::SnapshotRead { op: read, chunk }));
        let verify = op(&e.step(Event::SnapshotWritten {
            op: write,
            index: 0,
            through: 1,
            charged_bytes: 2,
        }));
        let barrier_op = op(&e.step(Event::SnapshotVerified {
            op: verify,
            charged_bytes: 2,
        }));
        let barrier = proof(3, 1);
        let scan = op(&e.step(Event::SnapshotBarrier {
            op: barrier_op,
            proof: barrier.clone(),
            comparisons: vec![cmp(&barrier, 1, 3), cmp(&barrier, 1, 1)],
        }));
        let batch = Batch {
            coverage: Coverage {
                from: pos(1),
                through: pos(3),
                proof: barrier.id.clone(),
                certificate: vec![1],
            },
            payload_id: scan.token,
            events: 2,
            bytes: 2,
            advance: cmp(&barrier, 1, 3),
            end_to_head: cmp(&barrier, 3, 3),
        };
        let apply = op(&e.step(Event::SnapshotScanned {
            op: scan,
            batch: Box::new(batch),
        }));
        let seal = op(&e.step(Event::SnapshotApplied {
            op: apply,
            through: pos(3),
            charged_bytes: 3,
        }));
        let install = op(&e.step(Event::SnapshotSealed {
            op: seal,
            through: pos(3),
            payload_id: seal.token,
            charged_bytes: 3,
        }));
        assert_eq!(e.read_decision(), ReadDecision::Refuse(Refusal::Unready));
        let attach = op(&e.step(Event::SnapshotInstalled {
            op: install,
            receipt: ApplyReceipt {
                through: pos(3),
                durable: true,
            },
        }));
        assert_eq!(e.read_decision(), ReadDecision::Refuse(Refusal::Unready));
        let attached = proof(4, 3);
        let tail = op(&e.step(Event::SnapshotAttached {
            op: attach,
            proof: attached.clone(),
            comparisons: vec![cmp(&attached, 3, 4), cmp(&attached, 3, 3)],
        }));
        let source_tail = proof(4, 3);
        let scan = op(&e.step(Event::Tail {
            op: tail,
            proof: source_tail.clone(),
            comparisons: vec![
                cmp(&source_tail, 3, 4),
                cmp(&source_tail, 3, 3),
                cmp(&source_tail, 3, 4),
            ],
        }));
        let batch = Batch {
            coverage: Coverage {
                from: pos(3),
                through: pos(4),
                proof: source_tail.id.clone(),
                certificate: vec![1],
            },
            payload_id: scan.token,
            events: 1,
            bytes: 1,
            advance: cmp(&source_tail, 3, 4),
            end_to_head: cmp(&source_tail, 4, 4),
        };
        let apply = op(&e.step(Event::Scanned {
            op: scan,
            batch: Box::new(batch),
        }));
        let tail = op(&e.step(Event::Applied {
            op: apply,
            receipt: ApplyReceipt {
                through: pos(4),
                durable: false,
            },
        }));
        let done = e.step(Event::Tail {
            op: tail,
            proof: source_tail.clone(),
            comparisons: vec![
                cmp(&source_tail, 4, 4),
                cmp(&source_tail, 4, 3),
                cmp(&source_tail, 3, 4),
            ],
        });
        assert!(done.rejection.is_none(), "seed {seed}: {done:?}");
        assert_eq!(e.state().stage, Stage::Ready, "seed {seed}");
        assert_eq!(e.state().materialized, Some(pos(4)), "seed {seed}");
        assert!(
            done.effects
                .iter()
                .any(|effect| matches!(effect, Effect::CleanupSnapshot { .. }))
        );
        // The second commit is not dropped merely because the image and B
        // were individually valid; it is admitted only after the attach tail.
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one fault schedule checks chunk corruption, loss, duplication, cancellation, and restart correlation"
)]
fn seeded_chunk_faults_timeout_cancel_and_restart_remain_unready() {
    for seed in 0..96 {
        let mut e = engine(seed + 1);
        let hold = op(&e.step(Event::StartSnapshot));
        let offered = op(&e.step(Event::SnapshotHeld {
            op: hold,
            receipt: HoldReceipt {
                request: hold,
                history: pos(1).history,
                retained_until: Time(80),
                certificate: vec![1],
            },
        }));
        let p = proof(1, 1);
        let open = op(&e.step(Event::SnapshotOffered {
            op: offered,
            offer: Box::new(SnapshotOffer {
                scope: scope(),
                schema: vec![1],
                cut: pos(1),
                proof: p.clone(),
                cut_to_head: cmp(&p, 1, 1),
                retained_to_cut: cmp(&p, 1, 1),
                total_bytes: 1,
                chunks: 1,
                digest: vec![1],
                certificate: vec![1],
            }),
        }));
        let read = op(&e.step(Event::SnapshotOpened {
            op: open,
            charged_bytes: 1,
        }));
        let valid = ChunkReceipt {
            index: 0,
            offset: 0,
            bytes: 1,
            payload_id: read.token,
        };
        match seed % 4 {
            0 => {
                let mut corrupt = valid;
                corrupt.offset = 1;
                assert!(
                    e.step(Event::SnapshotRead {
                        op: read,
                        chunk: corrupt
                    })
                    .rejection
                    .is_some()
                );
                e.step(Event::Failed { op: read });
            }
            1 => {
                e.step(Event::Tick(Time(12)));
            }
            2 => {
                let write = op(&e.step(Event::SnapshotRead {
                    op: read,
                    chunk: valid,
                }));
                assert!(
                    e.step(Event::SnapshotRead {
                        op: read,
                        chunk: valid
                    })
                    .rejection
                    .is_some()
                );
                e.step(Event::Cancel);
                assert!(
                    e.step(Event::SnapshotWritten {
                        op: write,
                        index: 0,
                        through: 1,
                        charged_bytes: 1
                    })
                    .rejection
                    .is_some()
                );
            }
            _ => {
                e.step(Event::Cancel);
                assert!(
                    e.step(Event::SnapshotRead {
                        op: read,
                        chunk: valid
                    })
                    .rejection
                    .is_some()
                );
            }
        }
        assert!(
            !matches!(e.read_decision(), ReadDecision::Serve(_)),
            "seed {seed}"
        );
        assert!(e.state().checkpoint.is_none(), "seed {seed}");
        let mut restarted = engine(seed + 1000);
        restarted.step(Event::StartSnapshot);
        assert!(
            restarted
                .step(Event::SnapshotRead {
                    op: read,
                    chunk: valid
                })
                .rejection
                .is_some()
        );
    }
}
