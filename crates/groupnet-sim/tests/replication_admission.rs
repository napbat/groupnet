//! Seeded virtual-time admission/intent races over the sans-IO cores.

use groupnet_core::Time;
use groupnet_core::replication::admission::{
    AckReceipt, AdmissionPolicy, AdmissionRef, ReaderCore, RecordBinding, RosterEntry,
    RosterReceipt, SourceReceipt, WriterCore,
};
use groupnet_core::replication::{
    BoundComparison, Comparison, Coverage, Cursor, ProofId, Scope, SourceHistory, SourceProof,
    Stream,
};
use groupnet_sim::SplitMix64;

fn scope() -> Scope {
    Scope {
        stream: Stream {
            group: "g".into(),
            topic: "control".into(),
            kind: "v1".into(),
        },
        partition: "bucket".into(),
    }
}

fn policy() -> AdmissionPolicy {
    AdmissionPolicy {
        history: SourceHistory {
            source: "cas".into(),
            generation: 1,
        },
        fingerprint: vec![1],
        max_duration_ms: 100,
        rate_numerator: 11,
        rate_denominator: 10,
        clock_margin_ms: 1,
        max_waiters: 8,
        max_cursor_bytes: 64,
    }
}

fn cursor(n: u8) -> Cursor {
    Cursor {
        scope: scope(),
        history: policy().history,
        position: vec![n],
    }
}

fn proof(head: u8) -> SourceProof {
    SourceProof {
        id: ProofId(vec![head]),
        head: cursor(head),
        retained_from: cursor(0),
        read_authority: false,
    }
}

fn cmp(p: &SourceProof, a: u8, b: u8) -> BoundComparison {
    BoundComparison {
        left: cursor(a),
        right: cursor(b),
        proof: p.id.clone(),
        order: match a.cmp(&b) {
            std::cmp::Ordering::Less => Comparison::Before,
            std::cmp::Ordering::Equal => Comparison::Equal,
            std::cmp::Ordering::Greater => Comparison::After,
        },
    }
}

fn receipt(n: u8, binding: RecordBinding) -> SourceReceipt {
    let p = proof(n);
    SourceReceipt {
        binding,
        policy_fingerprint: policy().fingerprint,
        cursor: cursor(n),
        record_to_head: cmp(&p, n, n),
        proof: p,
    }
}

fn reader_clock(real_deci_ms: u64, phase: u64) -> Time {
    Time((real_deci_ms * 10 + phase) / 100)
}

fn writer_clock(real_deci_ms: u64, phase: u64, rate: u64) -> Time {
    Time((real_deci_ms * rate + phase) / 100)
}

fn pending_key_through(cut: u8, key: &str) -> bool {
    // Independent source projection: slot 2 is the unclosed intent. Slot 3
    // admits the joining reader only after replaying that prior slot.
    [(1, None), (2, Some("touched")), (3, None)]
        .into_iter()
        .filter(|(slot, _)| *slot <= cut)
        .any(|(_, pending)| pending == Some(key))
}

#[test]
fn clock_quantization_margin_prevents_release_before_reader_expiry() {
    // The two source appends have different slots but occur in the same
    // logical millisecond. At real tick 991 (99.1 ms), the fast writer's
    // floored clock reaches 110 while the reader's is still 99. A wait of
    // only ceil(100 * 11/10) would therefore release too early.
    let mut reader = ReaderCore::new(scope(), policy(), "reader".into(), 1).unwrap();
    let admission = reader.begin(reader_clock(0, 0), 1).unwrap();
    reader
        .confirmed(
            reader_clock(0, 0),
            admission.token,
            &receipt(1, RecordBinding::Admission(admission.id.clone())),
        )
        .unwrap();
    let p1 = proof(1);
    reader
        .projected(
            reader_clock(0, 0),
            admission.token,
            &cursor(1),
            &p1,
            &cmp(&p1, 1, 1),
            &cmp(&p1, 1, 1),
        )
        .unwrap();
    let mut writer = WriterCore::new(scope(), policy(), 2, "mutation".into()).unwrap();
    let intent = writer.begin(writer_clock(0, 99, 11)).unwrap();
    writer
        .confirmed(
            writer_clock(0, 99, 11),
            intent.token,
            &receipt(2, RecordBinding::Intent("mutation".into())),
        )
        .unwrap();
    let p2 = proof(2);
    writer
        .roster(&RosterReceipt {
            proof: p2.clone(),
            coverage: Coverage {
                from: cursor(0),
                through: cursor(2),
                proof: p2.id.clone(),
                certificate: vec![1],
            },
            prefix_to_intent: cmp(&p2, 0, 2),
            intent_to_head: cmp(&p2, 2, 2),
            entries: vec![RosterEntry {
                admission: AdmissionRef {
                    id: admission.id,
                    cursor: cursor(1),
                    policy_fingerprint: policy().fingerprint,
                },
                before_intent: cmp(&p2, 1, 2),
            }],
        })
        .unwrap();
    assert_eq!(writer_clock(991, 99, 11), Time(110));
    assert_eq!(reader_clock(991, 0), Time(99));
    assert!(!writer.take_fence(writer_clock(991, 99, 11)).unwrap());
    assert!(reader.admitted_at(reader_clock(991, 0)).unwrap().is_some());
    let mut released = false;
    for real_tick in 992..=1_010 {
        if writer.take_fence(writer_clock(real_tick, 99, 11)).unwrap() {
            released = true;
            assert!(
                reader
                    .admitted_at(reader_clock(real_tick, 0))
                    .unwrap()
                    .is_none(),
                "writer released at real tick {real_tick} while reader remained live"
            );
            break;
        }
    }
    assert!(released);
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one seeded join and clock-rate schedule keeps the safety assertions beside its events"
)]
fn seeded_join_ack_loss_and_clock_skew_never_open_an_early_fence() {
    for seed in 0..128 {
        let mut rng = SplitMix64::new(seed);
        let ack_delivered = rng.below(2) == 0;
        let ack_at = 210 + u64::from(rng.below(750));
        let new_admission_at = 210 + u64::from(rng.below(450));
        let new_projection_delay = u64::from(rng.below(200));
        let reader_phase = u64::from(rng.below(100));
        let writer_phase = u64::from(rng.below(100));
        let writer_rate = 10 + u64::from(rng.below(2));

        let mut old = ReaderCore::new(scope(), policy(), "node".into(), 1001).unwrap();
        let old_append = old.begin(reader_clock(0, reader_phase), 41).unwrap();
        old.confirmed(
            reader_clock(10, reader_phase),
            old_append.token,
            &receipt(1, RecordBinding::Admission(old_append.id.clone())),
        )
        .unwrap();
        let p1 = proof(1);
        old.projected(
            reader_clock(20, reader_phase),
            old_append.token,
            &cursor(1),
            &p1,
            &cmp(&p1, 1, 1),
            &cmp(&p1, 1, 1),
        )
        .unwrap();
        // A queued duplicate append/readback reply cannot reactivate the
        // already-consumed admission operation.
        assert!(
            old.confirmed(
                reader_clock(20, reader_phase),
                old_append.token,
                &receipt(1, RecordBinding::Admission(old_append.id.clone())),
            )
            .is_err()
        );

        let mut writer = WriterCore::new(scope(), policy(), 3001, "mutation".into()).unwrap();
        let intent = writer
            .begin(writer_clock(100, writer_phase, writer_rate))
            .unwrap();
        writer
            .confirmed(
                writer_clock(200, writer_phase, writer_rate),
                intent.token,
                &receipt(2, RecordBinding::Intent("mutation".into())),
            )
            .unwrap();
        let p2 = proof(2);
        let old_ref = AdmissionRef {
            id: old_append.id.clone(),
            cursor: cursor(1),
            policy_fingerprint: policy().fingerprint,
        };
        let required = writer
            .roster(&RosterReceipt {
                proof: p2.clone(),
                coverage: Coverage {
                    from: cursor(0),
                    through: cursor(2),
                    proof: p2.id.clone(),
                    certificate: vec![1],
                },
                prefix_to_intent: cmp(&p2, 0, 2),
                intent_to_head: cmp(&p2, 2, 2),
                entries: vec![RosterEntry {
                    admission: old_ref.clone(),
                    before_intent: cmp(&p2, 1, 2),
                }],
            })
            .unwrap();
        assert_eq!(required, vec![old_ref.clone()]);

        // A second process can use the same stable node name, but needs its own
        // source-ordered admission and contiguous application projection.
        let mut joined = ReaderCore::new(scope(), policy(), "node".into(), 2002).unwrap();
        let mut join_op = None;
        let mut join_projected = false;
        let mut old_revoked = false;
        let mut released = false;
        for now in 200..=1_400 {
            let reader_now = reader_clock(now, reader_phase);
            let writer_now = writer_clock(now, writer_phase, writer_rate);
            old.tick(reader_now).unwrap();
            joined.tick(reader_now).unwrap();
            writer.tick(writer_now).unwrap();
            if now == new_admission_at {
                let append = joined.begin(reader_now, 42).unwrap();
                joined
                    .confirmed(
                        reader_now,
                        append.token,
                        &receipt(3, RecordBinding::Admission(append.id.clone())),
                    )
                    .unwrap();
                join_op = Some(append);
            }
            if now == new_admission_at + new_projection_delay {
                let append = join_op.as_ref().unwrap();
                let p3 = proof(3);
                joined
                    .projected(
                        reader_now,
                        append.token,
                        &cursor(3),
                        &p3,
                        &cmp(&p3, 3, 3),
                        &cmp(&p3, 3, 3),
                    )
                    .unwrap();
                assert!(
                    joined
                        .confirmed(
                            reader_now,
                            append.token,
                            &receipt(3, RecordBinding::Admission(append.id.clone())),
                        )
                        .is_err()
                );
                join_projected = true;
            }
            if ack_delivered && now == ack_at {
                old.cancel();
                old_revoked = true;
                writer
                    .acknowledge(&AckReceipt {
                        token: intent.token,
                        intent: cursor(2),
                        admission: old_ref.clone(),
                    })
                    .unwrap();
            }
            if !released && writer.take_fence(writer_now).unwrap() {
                released = true;
                assert!(
                    old_revoked || reader_now >= old_append.deadline,
                    "seed {seed}, real deci-ms {now}"
                );
                assert!(
                    old.admitted_at(reader_now).unwrap().is_none(),
                    "seed {seed}, real deci-ms {now}"
                );
            }
            if joined.admitted_at(reader_now).unwrap().is_some() {
                assert!(join_projected, "seed {seed}, time {now}");
                assert!(pending_key_through(3, "touched"));
                assert!(!pending_key_through(3, "unrelated"));
                let touched_allowed = !pending_key_through(3, "touched");
                let unrelated_allowed = !pending_key_through(3, "unrelated");
                assert!(!touched_allowed && unrelated_allowed, "seed {seed}");
            }
        }
        assert!(released, "seed {seed}");
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one seeded restart schedule keeps delayed replies and expiry assertions together"
)]
fn seeded_writer_restart_restarts_expiry_and_rejects_delayed_old_acks() {
    for seed in 128..192 {
        let mut rng = SplitMix64::new(seed);
        let deliver_new_ack = rng.below(2) == 0;
        let ack_at = 50 + u64::from(rng.below(40));
        let mut reader = ReaderCore::new(scope(), policy(), "node".into(), 10).unwrap();
        let admission = reader.begin(Time(0), 1).unwrap();
        reader
            .confirmed(
                Time(1),
                admission.token,
                &receipt(1, RecordBinding::Admission(admission.id.clone())),
            )
            .unwrap();
        let p1 = proof(1);
        reader
            .projected(
                Time(2),
                admission.token,
                &cursor(1),
                &p1,
                &cmp(&p1, 1, 1),
                &cmp(&p1, 1, 1),
            )
            .unwrap();
        let admission_ref = AdmissionRef {
            id: admission.id.clone(),
            cursor: cursor(1),
            policy_fingerprint: policy().fingerprint,
        };
        let p2 = proof(2);
        let roster = RosterReceipt {
            proof: p2.clone(),
            coverage: Coverage {
                from: cursor(0),
                through: cursor(2),
                proof: p2.id.clone(),
                certificate: vec![1],
            },
            prefix_to_intent: cmp(&p2, 0, 2),
            intent_to_head: cmp(&p2, 2, 2),
            entries: vec![RosterEntry {
                admission: admission_ref.clone(),
                before_intent: cmp(&p2, 1, 2),
            }],
        };
        let mut previous = WriterCore::new(scope(), policy(), 20, "op".into()).unwrap();
        let old_op = previous.begin(Time(10)).unwrap();
        previous
            .confirmed(
                Time(20),
                old_op.token,
                &receipt(2, RecordBinding::Intent("op".into())),
            )
            .unwrap();
        previous.roster(&roster).unwrap();
        previous.cancel();
        let mut restarted = WriterCore::new(scope(), policy(), 21, "op".into()).unwrap();
        let new_op = restarted.begin(Time(40)).unwrap();
        restarted
            .confirmed(
                Time(40),
                new_op.token,
                &receipt(2, RecordBinding::Intent("op".into())),
            )
            .unwrap();
        restarted.roster(&roster).unwrap();
        assert!(
            restarted
                .acknowledge(&AckReceipt {
                    token: old_op.token,
                    intent: cursor(2),
                    admission: admission_ref.clone(),
                })
                .is_err()
        );
        let mut released = false;
        for now in 40..=160 {
            reader.tick(Time(now)).unwrap();
            if deliver_new_ack && now == ack_at {
                reader.cancel();
                let ack = AckReceipt {
                    token: new_op.token,
                    intent: cursor(2),
                    admission: admission_ref.clone(),
                };
                restarted.acknowledge(&ack).unwrap();
                restarted.acknowledge(&ack).unwrap();
            }
            if !released && restarted.take_fence(Time(now)).unwrap() {
                released = true;
                assert!(
                    reader.admitted_at(Time(now)).unwrap().is_none(),
                    "seed {seed}"
                );
                if !deliver_new_ack {
                    assert!(now >= 151, "seed {seed}, time {now}");
                }
            }
        }
        assert!(released, "seed {seed}");
    }
}
