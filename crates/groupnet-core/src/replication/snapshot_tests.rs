use super::*;
use crate::Time;

fn scope() -> Scope {
    Scope {
        stream: Stream {
            group: "g".into(),
            topic: "t".into(),
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
            generation: 4,
        },
        position: vec![n],
    }
}

fn proof(head: u8, low: u8) -> SourceProof {
    SourceProof {
        id: ProofId(vec![head, low]),
        head: cursor(head),
        retained_from: cursor(low),
        read_authority: true,
    }
}

fn compare(p: &SourceProof, left: u8, right: u8) -> BoundComparison {
    BoundComparison {
        left: cursor(left),
        right: cursor(right),
        proof: p.id.clone(),
        order: match left.cmp(&right) {
            std::cmp::Ordering::Less => Comparison::Before,
            std::cmp::Ordering::Equal => Comparison::Equal,
            std::cmp::Ordering::Greater => Comparison::After,
        },
    }
}

fn config() -> Config {
    Config {
        snapshot: Some(SnapshotConfig {
            max_metadata_bytes: 128,
            max_snapshot_bytes: 16,
            max_chunks: 4,
            max_chunk_bytes: 4,
            max_candidate_bytes: 32,
            max_total_ms: 100,
        }),
        attempt_timeout_ms: 20,
        ..Config::default()
    }
}

fn engine() -> SessionEngine {
    SessionEngine::new(scope(), Mode::StateSync, config(), 1).unwrap()
}

fn operation(step: &Step) -> Operation {
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
            | Effect::CleanupSnapshot { op, .. }
            | Effect::DiscardSnapshotResources { op, .. } => Some(*op),
            _ => None,
        })
        .expect("issued operation")
}

fn offer() -> SnapshotOffer {
    let p = proof(2, 0);
    SnapshotOffer {
        scope: scope(),
        schema: vec![1],
        cut: cursor(1),
        cut_to_head: compare(&p, 1, 2),
        retained_to_cut: compare(&p, 0, 1),
        proof: p,
        total_bytes: 5,
        chunks: 2,
        digest: vec![42],
        certificate: vec![7],
    }
}

fn through_offer(e: &mut SessionEngine) -> Operation {
    let hold = operation(&e.step(Event::StartSnapshot));
    assert_eq!(e.state().stage, Stage::SnapshotHolding);
    let next = e.step(Event::SnapshotHeld {
        op: hold,
        receipt: HoldReceipt {
            request: hold,
            history: cursor(1).history,
            retained_until: Time(100),
            certificate: vec![1],
        },
    });
    let offered = operation(&next);
    assert_ne!(hold, offered);
    offered
}

fn through_image(e: &mut SessionEngine) -> Operation {
    let offered = through_offer(e);
    let opened = operation(&e.step(Event::SnapshotOffered {
        op: offered,
        offer: Box::new(offer()),
    }));
    let mut step = e.step(Event::SnapshotOpened {
        op: opened,
        charged_bytes: 0,
    });
    for (index, offset, bytes) in [(0, 0, 4), (1, 4, 1)] {
        let read = operation(&step);
        let chunk = ChunkReceipt {
            index,
            offset,
            bytes,
            payload_id: read.token,
        };
        let write = operation(&e.step(Event::SnapshotRead { op: read, chunk }));
        step = e.step(Event::SnapshotWritten {
            op: write,
            index,
            through: offset + bytes as u64,
            charged_bytes: if index == 0 { 8 } else { 4 },
        });
    }
    let verify = operation(&step);
    operation(&e.step(Event::SnapshotVerified {
        op: verify,
        charged_bytes: 2,
    }))
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "the one successful private replay and attach schedule must keep its correlated operations together"
)]
fn snapshot_replays_private_suffix_then_attaches_before_ready() {
    let mut e = engine();
    let barrier = through_image(&mut e);
    let p = proof(3, 1);
    let scan = operation(&e.step(Event::SnapshotBarrier {
        op: barrier,
        proof: p.clone(),
        comparisons: vec![compare(&p, 1, 3), compare(&p, 1, 1)],
    }));
    let b = Batch {
        coverage: Coverage {
            from: cursor(1),
            through: cursor(3),
            proof: p.id.clone(),
            certificate: vec![1],
        },
        payload_id: scan.token,
        events: 2,
        bytes: 4,
        advance: compare(&p, 1, 3),
        end_to_head: compare(&p, 3, 3),
    };
    let apply = operation(&e.step(Event::SnapshotScanned {
        op: scan,
        batch: Box::new(b),
    }));
    assert_eq!(
        e.step(Event::SnapshotApplied {
            op: apply,
            through: cursor(3),
            charged_bytes: 33,
        })
        .rejection,
        Some(Reject::Discontinuity)
    );
    let seal = operation(&e.step(Event::SnapshotApplied {
        op: apply,
        through: cursor(3),
        charged_bytes: 1,
    }));
    assert_eq!(
        e.step(Event::SnapshotSealed {
            op: seal,
            through: cursor(3),
            payload_id: seal.token,
            charged_bytes: 33,
        })
        .rejection,
        Some(Reject::Discontinuity)
    );
    let install = operation(&e.step(Event::SnapshotSealed {
        op: seal,
        through: cursor(3),
        payload_id: seal.token,
        charged_bytes: 0,
    }));
    assert_eq!(e.read_decision(), ReadDecision::Refuse(Refusal::Unready));
    let attach = operation(&e.step(Event::SnapshotInstalled {
        op: install,
        receipt: ApplyReceipt {
            through: cursor(3),
            durable: true,
        },
    }));
    assert_eq!(e.state().checkpoint, Some(cursor(3)));
    assert_eq!(e.read_decision(), ReadDecision::Refuse(Refusal::Unready));
    let a = proof(4, 3);
    let tail = operation(&e.step(Event::SnapshotAttached {
        op: attach,
        proof: a.clone(),
        comparisons: vec![compare(&a, 3, 4), compare(&a, 3, 3)],
    }));
    let tail_proof = proof(4, 3);
    let scan = operation(&e.step(Event::Tail {
        op: tail,
        proof: tail_proof.clone(),
        comparisons: vec![
            compare(&tail_proof, 3, 4),
            compare(&tail_proof, 3, 3),
            compare(&tail_proof, 3, 4),
        ],
    }));
    let b = Batch {
        coverage: Coverage {
            from: cursor(3),
            through: cursor(4),
            proof: tail_proof.id.clone(),
            certificate: vec![1],
        },
        payload_id: scan.token,
        events: 1,
        bytes: 4,
        advance: compare(&tail_proof, 3, 4),
        end_to_head: compare(&tail_proof, 4, 4),
    };
    let apply = operation(&e.step(Event::Scanned {
        op: scan,
        batch: Box::new(b),
    }));
    let tail = operation(&e.step(Event::Applied {
        op: apply,
        receipt: ApplyReceipt {
            through: cursor(4),
            durable: false,
        },
    }));
    let done = e.step(Event::Tail {
        op: tail,
        proof: tail_proof.clone(),
        comparisons: vec![
            compare(&tail_proof, 4, 4),
            compare(&tail_proof, 4, 3),
            compare(&tail_proof, 3, 4),
        ],
    });
    assert!(done.rejection.is_none(), "{done:?}");
    assert!(done.effects.iter().any(|effect| matches!(
        effect,
        Effect::CleanupSnapshot {
            disposition: SnapshotCleanupDisposition::Completed,
            ..
        }
    )));
    assert_eq!(e.state().stage, Stage::Ready);
    assert_eq!(e.read_decision(), ReadDecision::Refuse(Refusal::Authority));
    let old_cleanup = operation(&done);
    let cancel = e.step(Event::Cancel);
    let discard = operation(&cancel);
    assert_ne!(discard, old_cleanup);
    assert!(cancel.effects.iter().any(|effect| matches!(effect,
        Effect::DiscardSnapshotResources { op, disposition: SnapshotCleanupDisposition::Aborted, .. }
        if *op == discard)));
    assert_eq!(
        e.step(Event::SnapshotCleaned { op: old_cleanup }).rejection,
        Some(Reject::StaleOperation)
    );
    assert_eq!(
        e.step(Event::SnapshotDiscarded {
            op: old_cleanup,
            disposition: SnapshotCleanupDisposition::Completed
        })
        .rejection,
        Some(Reject::StaleOperation)
    );
    assert!(
        e.step(Event::SnapshotDiscarded {
            op: discard,
            disposition: SnapshotCleanupDisposition::Aborted
        })
        .rejection
        .is_none()
    );
    assert_eq!(e.state().stage, Stage::Cancelled);
}

fn ready_without_suffix(e: &mut SessionEngine) -> Operation {
    let barrier = through_image(e);
    let p = proof(1, 1);
    let seal = operation(&e.step(Event::SnapshotBarrier {
        op: barrier,
        proof: p.clone(),
        comparisons: vec![compare(&p, 1, 1), compare(&p, 1, 1)],
    }));
    let install = operation(&e.step(Event::SnapshotSealed {
        op: seal,
        through: cursor(1),
        payload_id: seal.token,
        charged_bytes: 8,
    }));
    let attach = operation(&e.step(Event::SnapshotInstalled {
        op: install,
        receipt: ApplyReceipt {
            through: cursor(1),
            durable: true,
        },
    }));
    let tail = operation(&e.step(Event::SnapshotAttached {
        op: attach,
        proof: p.clone(),
        comparisons: vec![compare(&p, 1, 1), compare(&p, 1, 1)],
    }));
    let ready = e.step(Event::Tail {
        op: tail,
        proof: p.clone(),
        comparisons: vec![compare(&p, 1, 1), compare(&p, 1, 1), compare(&p, 1, 1)],
    });
    assert!(ready.rejection.is_none(), "{ready:?}");
    assert_eq!(e.state().stage, Stage::Ready);
    operation(&ready)
}

#[test]
fn supersede_after_completed_cleanup_discards_retained_attachment() {
    let mut e = engine();
    let cleanup = ready_without_suffix(&mut e);
    assert!(
        e.step(Event::SnapshotCleaned { op: cleanup })
            .rejection
            .is_none()
    );
    let supersede = e.step(Event::Supersede);
    let discard = operation(&supersede);
    assert!(supersede.effects.iter().any(|effect| matches!(effect,
        Effect::DiscardSnapshotResources { op, disposition: SnapshotCleanupDisposition::Aborted, .. }
        if *op == discard)));
    assert_eq!(e.step(Event::StartBootstrap).rejection, Some(Reject::Stage));
    assert!(
        e.step(Event::SnapshotDiscarded {
            op: discard,
            disposition: SnapshotCleanupDisposition::Aborted
        })
        .rejection
        .is_none()
    );
    assert!(e.step(Event::StartBootstrap).rejection.is_none());
}

#[test]
fn later_retention_gap_discards_old_attachment_before_new_snapshot() {
    let mut e = engine();
    let cleanup = ready_without_suffix(&mut e);
    assert!(
        e.step(Event::SnapshotCleaned { op: cleanup })
            .rejection
            .is_none()
    );
    let tail = operation(&e.step(Event::Hint));
    let jumped = proof(3, 2);
    let gap = e.step(Event::Tail {
        op: tail,
        proof: jumped.clone(),
        comparisons: vec![
            compare(&jumped, 1, 3),
            compare(&jumped, 1, 2),
            compare(&jumped, 2, 3),
        ],
    });
    let discard = operation(&gap);
    assert!(gap.effects.iter().any(|effect| matches!(effect,
        Effect::DiscardSnapshotResources { op, disposition: SnapshotCleanupDisposition::Aborted, .. }
        if *op == discard)));
    assert_eq!(e.state().stage, Stage::NeedsSnapshot);
    assert_eq!(e.step(Event::StartSnapshot).rejection, Some(Reject::Stage));
    let restarted = e.step(Event::SnapshotDiscarded {
        op: discard,
        disposition: SnapshotCleanupDisposition::Aborted,
    });
    assert!(
        restarted
            .effects
            .iter()
            .any(|effect| matches!(effect, Effect::AcquireSnapshotHold { .. }))
    );
    assert_eq!(e.state().stage, Stage::SnapshotHolding);
}

#[test]
fn bad_offer_and_chunk_do_not_advance_or_install() {
    let mut e = engine();
    let offered = through_offer(&mut e);
    let mut bad = offer();
    bad.cut.history.generation += 1;
    assert!(
        e.step(Event::SnapshotOffered {
            op: offered,
            offer: Box::new(bad)
        })
        .rejection
        .is_some()
    );
    assert_eq!(e.state().stage, Stage::SnapshotOffering);
    let opened = operation(&e.step(Event::SnapshotOffered {
        op: offered,
        offer: Box::new(offer()),
    }));
    let read = operation(&e.step(Event::SnapshotOpened {
        op: opened,
        charged_bytes: 0,
    }));
    assert_eq!(
        e.step(Event::SnapshotRead {
            op: read,
            chunk: ChunkReceipt {
                index: 1,
                offset: 0,
                bytes: 4,
                payload_id: read.token
            }
        })
        .rejection,
        Some(Reject::Discontinuity)
    );
    assert!(e.state().checkpoint.is_none());
    assert_eq!(e.read_decision(), ReadDecision::Refuse(Refusal::Unready));
}

#[test]
fn impossible_offer_and_cross_history_private_batch_are_rejected() {
    let mut e = engine();
    let offered = through_offer(&mut e);
    let mut impossible = offer();
    impossible.total_bytes = 9; // Two chunks of at most four bytes cannot cover it.
    assert_eq!(
        e.step(Event::SnapshotOffered {
            op: offered,
            offer: Box::new(impossible)
        })
        .rejection,
        Some(Reject::Backpressure)
    );
    let opened = operation(&e.step(Event::SnapshotOffered {
        op: offered,
        offer: Box::new(offer()),
    }));
    let mut step = e.step(Event::SnapshotOpened {
        op: opened,
        charged_bytes: 0,
    });
    for (index, offset, bytes) in [(0, 0, 4), (1, 4, 1)] {
        let read = operation(&step);
        let chunk = ChunkReceipt {
            index,
            offset,
            bytes,
            payload_id: read.token,
        };
        let write = operation(&e.step(Event::SnapshotRead { op: read, chunk }));
        step = e.step(Event::SnapshotWritten {
            op: write,
            index,
            through: offset + bytes as u64,
            charged_bytes: 8,
        });
    }
    let verify = operation(&step);
    let barrier = operation(&e.step(Event::SnapshotVerified {
        op: verify,
        charged_bytes: 8,
    }));
    let p = proof(3, 1);
    let scan = operation(&e.step(Event::SnapshotBarrier {
        op: barrier,
        proof: p.clone(),
        comparisons: vec![compare(&p, 1, 3), compare(&p, 1, 1)],
    }));
    let mut through = cursor(3);
    through.history.generation += 1;
    let cross = Batch {
        coverage: Coverage {
            from: cursor(1),
            through: through.clone(),
            proof: p.id.clone(),
            certificate: vec![1],
        },
        payload_id: scan.token,
        events: 2,
        bytes: 4,
        advance: BoundComparison {
            left: cursor(1),
            right: through.clone(),
            proof: p.id.clone(),
            order: Comparison::Before,
        },
        end_to_head: BoundComparison {
            left: through,
            right: cursor(3),
            proof: p.id.clone(),
            order: Comparison::Equal,
        },
    };
    assert_eq!(
        e.step(Event::SnapshotScanned {
            op: scan,
            batch: Box::new(cross)
        })
        .rejection,
        Some(Reject::Discontinuity)
    );
    assert!(e.state().checkpoint.is_none());
}

#[test]
fn total_deadline_cancels_live_work_and_late_hold_cannot_revive() {
    let mut e = engine();
    let hold = operation(&e.step(Event::StartSnapshot));
    let expired = e.step(Event::Tick(Time(100)));
    assert_eq!(e.state().stage, Stage::SnapshotAborted);
    assert!(expired.effects.iter().any(|effect| matches!(effect,
        Effect::CleanupSnapshot { attempt, disposition: SnapshotCleanupDisposition::Aborted, due: Time(120), .. } if *attempt == hold)));
    assert!(!e.accepts_operation(hold));
    assert_eq!(
        e.step(Event::SnapshotHeld {
            op: hold,
            receipt: HoldReceipt {
                request: hold,
                history: cursor(1).history,
                retained_until: Time(200),
                certificate: vec![1]
            }
        })
        .rejection,
        Some(Reject::StaleOperation)
    );
    assert!(e.state().checkpoint.is_none());
    let cleanup = operation(&expired);
    assert!(e.accepts_operation(cleanup));
    let discard = e.step(Event::Tick(Time(120)));
    assert!(discard.effects.iter().any(|effect| matches!(effect,
        Effect::DiscardSnapshotResources { op, attempt, disposition: SnapshotCleanupDisposition::Aborted }
        if *op == cleanup && *attempt == hold)));
    assert!(!e.accepts_operation(cleanup));
    assert_eq!(
        e.step(Event::SnapshotCleaned { op: cleanup }).rejection,
        Some(Reject::StaleOperation)
    );
    assert_eq!(e.step(Event::StartSnapshot).rejection, Some(Reject::Stage));
    assert_eq!(
        e.step(Event::SnapshotDiscarded {
            op: cleanup,
            disposition: SnapshotCleanupDisposition::Completed
        })
        .rejection,
        Some(Reject::StaleOperation)
    );
    assert!(
        e.step(Event::SnapshotDiscarded {
            op: cleanup,
            disposition: SnapshotCleanupDisposition::Aborted
        })
        .rejection
        .is_none()
    );
    assert!(!e.accepts_operation(cleanup));
    let restarted = e.step(Event::StartSnapshot);
    assert!(restarted.rejection.is_none());
    let next_hold = operation(&restarted);
    assert_ne!(next_hold, hold);
    assert_eq!(
        e.step(Event::SnapshotDiscarded {
            op: cleanup,
            disposition: SnapshotCleanupDisposition::Aborted
        })
        .rejection,
        Some(Reject::StaleOperation)
    );
    assert!(e.accepts_operation(next_hold));
}

#[test]
fn event_complete_and_replay_only_reject_snapshot() {
    let mut complete = SessionEngine::new(scope(), Mode::EventComplete, config(), 1).unwrap();
    assert_eq!(
        complete.step(Event::StartSnapshot).rejection,
        Some(Reject::Stage)
    );
    let mut replay = SessionEngine::new(scope(), Mode::StateSync, Config::default(), 1).unwrap();
    assert_eq!(
        replay.step(Event::StartSnapshot).rejection,
        Some(Reject::Stage)
    );
}

#[test]
fn volatile_install_and_post_attach_retention_gap_cannot_ready() {
    let mut volatile = engine();
    let barrier = through_image(&mut volatile);
    let p = proof(1, 1);
    let seal = operation(&volatile.step(Event::SnapshotBarrier {
        op: barrier,
        proof: p.clone(),
        comparisons: vec![compare(&p, 1, 1), compare(&p, 1, 1)],
    }));
    let install = operation(&volatile.step(Event::SnapshotSealed {
        op: seal,
        through: cursor(1),
        payload_id: seal.token,
        charged_bytes: 8,
    }));
    assert_eq!(
        volatile
            .step(Event::SnapshotInstalled {
                op: install,
                receipt: ApplyReceipt {
                    through: cursor(1),
                    durable: false
                }
            })
            .rejection,
        Some(Reject::Discontinuity)
    );
    assert!(volatile.state().checkpoint.is_none());
    let cancel = volatile.step(Event::Cancel);
    assert!(
        cancel
            .effects
            .iter()
            .any(|effect| matches!(effect, Effect::CleanupSnapshot { .. }))
    );
    assert_eq!(
        volatile
            .step(Event::SnapshotInstalled {
                op: install,
                receipt: ApplyReceipt {
                    through: cursor(1),
                    durable: true
                }
            })
            .rejection,
        Some(Reject::StaleOperation)
    );

    let mut gap = engine();
    let barrier = through_image(&mut gap);
    let p = proof(1, 1);
    let seal = operation(&gap.step(Event::SnapshotBarrier {
        op: barrier,
        proof: p.clone(),
        comparisons: vec![compare(&p, 1, 1), compare(&p, 1, 1)],
    }));
    let install = operation(&gap.step(Event::SnapshotSealed {
        op: seal,
        through: cursor(1),
        payload_id: seal.token,
        charged_bytes: 8,
    }));
    let attach = operation(&gap.step(Event::SnapshotInstalled {
        op: install,
        receipt: ApplyReceipt {
            through: cursor(1),
            durable: true,
        },
    }));
    let a = proof(2, 1);
    let tail = operation(&gap.step(Event::SnapshotAttached {
        op: attach,
        proof: a.clone(),
        comparisons: vec![compare(&a, 1, 2), compare(&a, 1, 1)],
    }));
    let jumped = proof(3, 2);
    let aborted = gap.step(Event::Tail {
        op: tail,
        proof: jumped.clone(),
        comparisons: vec![
            compare(&jumped, 1, 3),
            compare(&jumped, 1, 2),
            compare(&jumped, 2, 3),
        ],
    });
    assert_eq!(gap.state().stage, Stage::SnapshotAborted);
    assert!(
        aborted
            .effects
            .iter()
            .any(|effect| matches!(effect, Effect::CleanupSnapshot { .. }))
    );
    assert_eq!(gap.read_decision(), ReadDecision::Refuse(Refusal::Gap));
}

#[test]
fn emitted_post_attach_timer_expires_total_budget_before_operation_deadline() {
    let mut cfg = config();
    cfg.snapshot.as_mut().unwrap().max_total_ms = 15;
    let mut e = SessionEngine::new(scope(), Mode::StateSync, cfg, 1).unwrap();
    let barrier = through_image(&mut e);
    let p = proof(1, 1);
    let seal = operation(&e.step(Event::SnapshotBarrier {
        op: barrier,
        proof: p.clone(),
        comparisons: vec![compare(&p, 1, 1), compare(&p, 1, 1)],
    }));
    let install = operation(&e.step(Event::SnapshotSealed {
        op: seal,
        through: cursor(1),
        payload_id: seal.token,
        charged_bytes: 8,
    }));
    let attach = operation(&e.step(Event::SnapshotInstalled {
        op: install,
        receipt: ApplyReceipt {
            through: cursor(1),
            durable: true,
        },
    }));
    let a = proof(2, 1);
    let attached = e.step(Event::SnapshotAttached {
        op: attach,
        proof: a.clone(),
        comparisons: vec![compare(&a, 1, 2), compare(&a, 1, 1)],
    });
    let timer = attached
        .effects
        .iter()
        .find_map(|effect| match effect {
            Effect::ArmTimer(due) => Some(*due),
            _ => None,
        })
        .expect("timer effect");
    assert_eq!(timer, Time(15));
    let expired = e.step(Event::Tick(timer));
    assert_eq!(e.state().stage, Stage::SnapshotAborted);
    assert!(
        expired
            .effects
            .iter()
            .any(|effect| matches!(effect, Effect::CleanupSnapshot { .. }))
    );
    assert_eq!(e.read_decision(), ReadDecision::Refuse(Refusal::Gap));
}
