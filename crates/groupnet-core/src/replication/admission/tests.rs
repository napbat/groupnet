use crate::Time;
use crate::replication::{
    BoundComparison, Comparison, Coverage, Cursor, ProofId, Scope, SourceHistory, SourceProof,
    Stream,
};

use super::{
    AckReceipt, AdmissionId, AdmissionPolicy, AdmissionRef, ReaderCore, ReaderError, RecordBinding,
    RosterEntry, RosterReceipt, SourceReceipt, WriterCore, WriterError,
};

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
            source: "authority".into(),
            generation: 7,
        },
        fingerprint: vec![1, 2],
        max_duration_ms: 100,
        rate_numerator: 11,
        rate_denominator: 10,
        clock_margin_ms: 1,
        max_waiters: 4,
        max_cursor_bytes: 64,
    }
}

fn cursor(position: u8) -> Cursor {
    Cursor {
        scope: scope(),
        history: policy().history,
        position: vec![position],
    }
}

fn proof(head: u8) -> SourceProof {
    SourceProof {
        id: ProofId(vec![head, 7]),
        head: cursor(head),
        retained_from: cursor(0),
        read_authority: false,
    }
}

fn cmp(proof: &SourceProof, left: u8, right: u8) -> BoundComparison {
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

fn receipt(position: u8, head: u8, binding: RecordBinding) -> SourceReceipt {
    let proof = proof(head);
    SourceReceipt {
        binding,
        policy_fingerprint: policy().fingerprint,
        cursor: cursor(position),
        record_to_head: cmp(&proof, position, head),
        proof,
    }
}

fn projected(reader: &mut ReaderCore, token: (u64, u64), now: u64, admission: u8, head: u8) {
    let p = proof(head);
    reader
        .projected(
            Time(now),
            token,
            &cursor(head),
            &p,
            &cmp(&p, admission, head),
            &cmp(&p, head, head),
        )
        .unwrap();
}

fn roster(intent: u8, admissions: &[(u8, &str, u64)]) -> RosterReceipt {
    let p = proof(intent);
    RosterReceipt {
        proof: p.clone(),
        coverage: Coverage {
            from: cursor(0),
            through: cursor(intent),
            proof: p.id.clone(),
            certificate: vec![9],
        },
        prefix_to_intent: cmp(&p, 0, intent),
        intent_to_head: cmp(&p, intent, intent),
        entries: admissions
            .iter()
            .map(|(position, reader, incarnation)| RosterEntry {
                admission: AdmissionRef {
                    id: AdmissionId {
                        reader: (*reader).into(),
                        incarnation: *incarnation,
                    },
                    cursor: cursor(*position),
                    policy_fingerprint: policy().fingerprint,
                },
                before_intent: cmp(&p, *position, intent),
            })
            .collect(),
    }
}

#[test]
fn reader_late_append_cannot_extend_deadline_or_reopen() {
    let mut reader = ReaderCore::new(scope(), policy(), "node".into(), 3).unwrap();
    let append = reader.begin(Time(10), 55).unwrap();
    assert!(reader.admitted_at(Time(10)).unwrap().is_none());
    assert_eq!(append.deadline, Time(110));
    assert_eq!(
        reader.confirmed(
            Time(110),
            append.token,
            &receipt(1, 1, RecordBinding::Admission(append.id))
        ),
        Err(ReaderError::Stale)
    );
    assert!(reader.admitted_at(Time(110)).unwrap().is_none());
}

#[test]
fn renewal_does_not_extend_old_window_until_its_own_cut_is_applied() {
    let mut reader = ReaderCore::new(scope(), policy(), "node".into(), 3).unwrap();
    let first = reader.begin(Time(0), 1).unwrap();
    reader
        .confirmed(
            Time(1),
            first.token,
            &receipt(1, 1, RecordBinding::Admission(first.id.clone())),
        )
        .unwrap();
    projected(&mut reader, first.token, 2, 1, 1);
    let second = reader.begin(Time(50), 2).unwrap();
    assert_eq!(reader.admitted_at(Time(50)).unwrap(), Some(&first.id));
    reader
        .confirmed(
            Time(60),
            second.token,
            &receipt(3, 3, RecordBinding::Admission(second.id.clone())),
        )
        .unwrap();
    reader.tick(Time(100)).unwrap();
    assert!(reader.admitted_at(Time(100)).unwrap().is_none());
    projected(&mut reader, second.token, 101, 3, 3);
    assert_eq!(reader.admitted_at(Time(101)).unwrap(), Some(&second.id));
    reader.tick(Time(150)).unwrap();
    assert!(reader.admitted_at(Time(150)).unwrap().is_none());
}

#[test]
fn stale_process_and_wrong_native_proofs_are_rejected() {
    let mut reader = ReaderCore::new(scope(), policy(), "node".into(), 4).unwrap();
    let append = reader.begin(Time(0), 1).unwrap();
    assert_eq!(
        reader.confirmed(
            Time(1),
            (3, 1),
            &receipt(1, 1, RecordBinding::Admission(append.id.clone()))
        ),
        Err(ReaderError::Stale)
    );
    assert_eq!(
        reader.confirmed(
            Time(1),
            append.token,
            &receipt(
                1,
                1,
                RecordBinding::Admission(AdmissionId {
                    reader: "node".into(),
                    incarnation: 2,
                })
            )
        ),
        Err(ReaderError::Proof)
    );
    reader
        .confirmed(
            Time(2),
            append.token,
            &receipt(1, 1, RecordBinding::Admission(append.id.clone())),
        )
        .unwrap();
    let p = proof(1);
    assert_eq!(
        reader.projected(
            Time(3),
            append.token,
            &cursor(0),
            &p,
            &cmp(&p, 1, 0),
            &cmp(&p, 0, 1),
        ),
        Err(ReaderError::Proof)
    );
    reader.cancel();
    assert_eq!(
        reader.projected(
            Time(4),
            append.token,
            &cursor(1),
            &p,
            &cmp(&p, 1, 1),
            &cmp(&p, 1, 1),
        ),
        Err(ReaderError::Stale)
    );
}

#[test]
fn writer_requires_exact_acks_or_conservative_global_wait() {
    assert_eq!(policy().expiry_wait_ms().unwrap(), 111);
    let mut writer = WriterCore::new(scope(), policy(), 9, "op".into()).unwrap();
    let append = writer.begin(Time(0)).unwrap();
    writer
        .confirmed(
            Time(20),
            append.token,
            &receipt(3, 3, RecordBinding::Intent("op".into())),
        )
        .unwrap();
    assert_eq!(writer.take_fence(Time(20)), Err(WriterError::Stage));
    let required = writer
        .roster(&roster(3, &[(1, "same-node", 7), (2, "same-node", 8)]))
        .unwrap();
    assert_eq!(required.len(), 2);
    writer
        .acknowledge(&AckReceipt {
            token: append.token,
            intent: cursor(3),
            admission: required[0].clone(),
        })
        .unwrap();
    assert!(!writer.take_fence(Time(20)).unwrap());
    assert_eq!(
        writer.acknowledge(&AckReceipt {
            token: append.token,
            intent: cursor(3),
            admission: AdmissionRef {
                id: AdmissionId {
                    reader: "same-node".into(),
                    incarnation: 9,
                },
                ..required[1].clone()
            },
        }),
        Err(WriterError::Stale)
    );
    writer.tick(Time(129)).unwrap();
    assert!(!writer.take_fence(Time(129)).unwrap());
    writer.tick(Time(131)).unwrap();
    assert!(writer.take_fence(Time(131)).unwrap());
    assert_eq!(writer.take_fence(Time(131)), Err(WriterError::Stage));
}

#[test]
fn roster_overflow_and_mismatched_policy_fail_closed() {
    let mut p = policy();
    p.max_waiters = 1;
    let mut writer = WriterCore::new(scope(), p, 9, "op".into()).unwrap();
    let append = writer.begin(Time(0)).unwrap();
    writer
        .confirmed(
            Time(0),
            append.token,
            &receipt(3, 3, RecordBinding::Intent("op".into())),
        )
        .unwrap();
    assert_eq!(
        writer.roster(&roster(3, &[(1, "a", 1), (2, "b", 2)])),
        Err(WriterError::Backpressure)
    );
    writer.tick(Time(1_000)).unwrap();
    assert_eq!(writer.take_fence(Time(1_000)), Err(WriterError::Stage));
    let mut mismatched = roster(3, &[(1, "a", 1)]);
    mismatched.entries[0].admission.policy_fingerprint = vec![8];
    assert_eq!(writer.roster(&mismatched), Err(WriterError::Proof));
}

#[test]
fn policy_rejects_rate_overflow_instead_of_wrapping() {
    let mut p = policy();
    p.max_duration_ms = u64::MAX;
    p.rate_numerator = u64::MAX;
    p.rate_denominator = 1;
    assert!(p.validate().is_err());
    let mut zero = policy();
    zero.rate_denominator = 0;
    assert!(zero.expiry_wait_ms().is_err());
    let mut exact_rates = policy();
    exact_rates.rate_numerator = 1;
    exact_rates.rate_denominator = 1;
    assert_eq!(exact_rates.expiry_wait_ms().unwrap(), 101);
}
