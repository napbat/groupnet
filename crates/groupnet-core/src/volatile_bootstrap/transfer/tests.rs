use super::*;
use crate::NodeId;
use crate::Status;
use crate::Time;
use crate::volatile_bootstrap::journal::{
    AttachToken, BarrierReceipt, CaptureId, DeltaIdentity, JournalBatch, JournalCursor,
    JournalDelta, NativeCut, ReservationId,
};
use crate::volatile_bootstrap::{
    BootId, BootstrapMemberIdentity, BootstrapOperation, BootstrapScope, ClaimIdentity,
    PresenceIdentity,
};
use crate::volatile_recovery::RecoveryOperation;

fn recovery() -> RecoveryOperation {
    RecoveryOperation {
        session: 1,
        generation: 1,
        token: 1,
    }
}

fn identity(name: &str, session: u64) -> ClaimIdentity {
    ClaimIdentity {
        node: NodeId::from(name),
        incarnation: BootId(7),
        session,
        attempt: 1,
    }
}

fn member(name: &str, session: u64) -> BootstrapMemberIdentity {
    BootstrapMemberIdentity {
        node: NodeId::from(name),
        presence: Some(PresenceIdentity {
            node: NodeId::from(name),
            boot: BootId(7),
            session,
        }),
        member_incarnation: 1,
        status: Status::Alive,
    }
}

fn config() -> TransferConfig {
    TransferConfig {
        expected_schema: 1,
        max_metadata_bytes: 32,
        max_encoded_bytes: 16,
        max_decoded_bytes: 16,
        max_chunk_bytes: 8,
        max_chunks: 4,
        max_batch_bytes: 16,
        max_batch_events: 3,
        max_replay_events: 5,
        max_native_buffer_bytes: 8,
        max_members: 2,
        max_cuts: 1,
        coverage_poll_ms: 2,
    }
}

fn binding() -> TransferBinding {
    TransferBinding {
        parent: BootstrapOperation {
            session: 9,
            incarnation: BootId(7),
            generation: 1,
            token: 1,
        },
        scope: BootstrapScope {
            domain: "o".to_owned(),
            partition: "b".to_owned(),
        },
        donor: identity("donor", 1),
        follower: identity("peer", 9),
        due: Time(20),
    }
}

fn capture() -> CaptureId {
    CaptureId {
        scope: binding().scope,
        donor: identity("donor", 1),
        recovery_generation: 3,
        serial: 5,
    }
}

fn cursor(position: u64) -> JournalCursor {
    JournalCursor {
        capture: capture(),
        position,
    }
}

fn reservation() -> ReservationId {
    ReservationId {
        capture: capture(),
        follower: identity("peer", 9),
        serial: 1,
    }
}

fn cut(sequence: u64) -> NativeCut {
    NativeCut {
        writer: b"w".to_vec(),
        epoch: 1,
        sequence,
    }
}

fn offer() -> TransferOffer {
    TransferOffer {
        capture: capture(),
        image_cut: cursor(0),
        schema: 1,
        encoded_bytes: 2,
        decoded_bytes: 2,
        chunks: 2,
        commitment: [8; 32],
        members: vec![member("donor", 1), member("peer", 9)],
        cuts: vec![cut(0)],
    }
}

fn barrier(position: u64, sequence: u64, operation: u64) -> BarrierReceipt {
    BarrierReceipt {
        reservation: reservation(),
        attach_operation: 42,
        barrier_operation: operation,
        cursor: cursor(position),
        covered_cuts: vec![cut(sequence)],
        members: offer().members,
    }
}

fn op(token: u64) -> BootstrapOperation {
    BootstrapOperation {
        token,
        ..binding().parent
    }
}

fn accept(
    engine: &mut TransferSession,
    event: TransferEvent,
    allocator: &mut impl FnMut() -> Option<BootstrapOperation>,
) -> TransferEffect {
    let step = engine.step(event, allocator);
    assert_eq!(step.rejection, None);
    step.effects.into_iter().next().unwrap()
}

fn reject_repeated_first_chunk(
    engine: &mut TransferSession,
    chunk0: BootstrapOperation,
    allocator: &mut impl FnMut() -> Option<BootstrapOperation>,
) {
    assert_eq!(
        engine
            .step(
                TransferEvent::ChunkStored {
                    op: chunk0,
                    sequence: 0,
                    bytes: 1,
                    decoded_charge: 1,
                },
                allocator,
            )
            .rejection,
        Some(TransferError::Stale)
    );
}

fn through_image(engine: &mut TransferSession, next: &mut u64) -> BootstrapOperation {
    let mut allocate = || {
        *next += 1;
        Some(op(*next))
    };
    let TransferEffect::FetchOffer { op: first, .. } =
        accept(engine, TransferEvent::Start, &mut allocate)
    else {
        panic!("fetch offer")
    };
    let offered = offer();
    let expected_cut = offered.image_cut.clone();
    let expected_chunks = offered.chunks;
    let TransferEffect::ReserveStage {
        op: second,
        image_cut,
        chunks,
        ..
    } = accept(
        engine,
        TransferEvent::Offered {
            op: first,
            offer: offered,
        },
        &mut allocate,
    )
    else {
        panic!("reserve stage")
    };
    assert_eq!(image_cut, expected_cut);
    assert_eq!(chunks, expected_chunks);
    let TransferEffect::ReserveDonor { op: third, .. } = accept(
        engine,
        TransferEvent::StageReserved { op: second },
        &mut allocate,
    ) else {
        panic!("reserve donor")
    };
    let TransferEffect::FetchChunk {
        op: chunk0,
        sequence: 0,
        ..
    } = accept(
        engine,
        TransferEvent::DonorReserved {
            op: third,
            reservation: reservation(),
        },
        &mut allocate,
    )
    else {
        panic!("first chunk")
    };
    let TransferEffect::FetchChunk {
        op: chunk1,
        sequence: 1,
        ..
    } = accept(
        engine,
        TransferEvent::ChunkStored {
            op: chunk0,
            sequence: 0,
            bytes: 1,
            decoded_charge: 1,
        },
        &mut allocate,
    )
    else {
        panic!("second chunk")
    };
    reject_repeated_first_chunk(engine, chunk0, &mut allocate);
    let TransferEffect::VerifyImage { op: verify, .. } = accept(
        engine,
        TransferEvent::ChunkStored {
            op: chunk1,
            sequence: 1,
            bytes: 1,
            decoded_charge: 2,
        },
        &mut allocate,
    ) else {
        panic!("verify image")
    };
    let TransferEffect::AttachStream { op: attach, .. } = accept(
        engine,
        TransferEvent::ImageVerified {
            op: verify,
            commitment: [8; 32],
        },
        &mut allocate,
    ) else {
        panic!("attach stream")
    };
    attach
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one transfer walks two exact barriers and the associated private-stage callbacks"
)]
fn image_and_two_barriers_finish_without_granting_authority() {
    let mut engine = TransferSession::new(config(), binding(), Time(0)).unwrap();
    let mut next = 1;
    let attach = through_image(&mut engine, &mut next);
    let mut allocate = || {
        next += 1;
        Some(op(next))
    };
    let TransferEffect::FetchBarrier { op: first_b, .. } = accept(
        &mut engine,
        TransferEvent::StreamAttached {
            op: attach,
            token: AttachToken {
                reservation: reservation(),
                operation: 42,
            },
        },
        &mut allocate,
    ) else {
        panic!("first B")
    };
    let TransferEffect::FetchBatch { op: fetch, .. } = accept(
        &mut engine,
        TransferEvent::BarrierReceived {
            op: first_b,
            receipt: barrier(2, 1, 51),
        },
        &mut allocate,
    ) else {
        panic!("first batch")
    };
    let batch = JournalBatch {
        reservation: reservation(),
        operation: 60,
        from: cursor(0),
        through: cursor(2),
        deltas: vec![
            JournalDelta {
                position: 1,
                identity: DeltaIdentity::Native(cut(1)),
                effect: vec![1, 5],
            },
            JournalDelta {
                position: 2,
                identity: DeltaIdentity::Local(b"x".to_vec()),
                effect: vec![0],
            },
        ],
        bytes: 5,
    };
    let duplicate = batch.clone();
    let TransferEffect::AckBatch { op: ack, .. } = accept(
        &mut engine,
        TransferEvent::BatchStaged { op: fetch, batch },
        &mut allocate,
    ) else {
        panic!("ack batch")
    };
    assert_eq!(
        engine
            .step(
                TransferEvent::BatchStaged {
                    op: fetch,
                    batch: duplicate,
                },
                &mut allocate,
            )
            .rejection,
        Some(TransferError::Stale)
    );
    let TransferEffect::CheckNativeCoverage { op: cover, .. } = accept(
        &mut engine,
        TransferEvent::BatchAcknowledged {
            op: ack,
            through: cursor(2),
        },
        &mut allocate,
    ) else {
        panic!("check native")
    };
    assert_eq!(
        accept(
            &mut engine,
            TransferEvent::NativePending { op: cover },
            &mut allocate,
        ),
        TransferEffect::ArmTimer(Time(2))
    );
    let TransferEffect::AdvanceBarrier {
        op: next_b,
        expected,
    } = accept(&mut engine, TransferEvent::Tick(Time(2)), &mut allocate)
    else {
        panic!("next B")
    };
    assert_eq!(expected, barrier(2, 1, 51));
    let TransferEffect::FetchBatch { op: next_fetch, .. } = accept(
        &mut engine,
        TransferEvent::BarrierReceived {
            op: next_b,
            receipt: barrier(3, 2, 52),
        },
        &mut allocate,
    ) else {
        panic!("next batch")
    };
    let TransferEffect::AckBatch { op: next_ack, .. } = accept(
        &mut engine,
        TransferEvent::BatchStaged {
            op: next_fetch,
            batch: JournalBatch {
                reservation: reservation(),
                operation: 61,
                from: cursor(2),
                through: cursor(3),
                deltas: vec![JournalDelta {
                    position: 3,
                    identity: DeltaIdentity::Native(cut(2)),
                    effect: vec![1, 8],
                }],
                bytes: 3,
            },
        },
        &mut allocate,
    ) else {
        panic!("next ack")
    };
    let TransferEffect::CheckNativeCoverage {
        op: final_cover, ..
    } = accept(
        &mut engine,
        TransferEvent::BatchAcknowledged {
            op: next_ack,
            through: cursor(3),
        },
        &mut allocate,
    )
    else {
        panic!("final coverage")
    };
    let coverage = NativeCoverageReceipt {
        parent: op(1),
        barrier: barrier(3, 2, 52),
        staged_through: cursor(3),
        proven_cuts: vec![cut(2)],
        members: offer().members,
        buffered_bytes: 3,
    };
    let TransferEffect::InstallCandidate { op: install, .. } = accept(
        &mut engine,
        TransferEvent::NativeCovered {
            op: final_cover,
            coverage: coverage.clone(),
        },
        &mut allocate,
    ) else {
        panic!("install under app guard")
    };
    assert_eq!(engine.stage(), TransferStage::Installing);
    assert_eq!(
        accept(
            &mut engine,
            TransferEvent::Installed {
                op: install,
                handoff: Box::new(NativeHandoffReceipt {
                    recovery: recovery(),
                    install,
                    coverage,
                    attachment: AttachToken {
                        reservation: reservation(),
                        operation: 42,
                    },
                    schema: 1,
                    applier_generation: 1,
                    continued_cuts: vec![cut(2)],
                    buffered_bytes: 0,
                }),
            },
            &mut allocate,
        ),
        TransferEffect::ReleaseReservation(reservation())
    );
    assert_eq!(engine.stage(), TransferStage::Completed);
    assert_eq!(engine.next_deadline(), None);
}

/// An install that finds its live cuts moved since the coverage check keeps
/// its stage: the session samples a later barrier instead of aborting, and
/// the stale install reply can no longer complete the handoff.
#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one transfer walks image, barrier, batch, coverage, and install callbacks"
)]
fn install_reporting_moved_cuts_advances_the_barrier_instead_of_aborting() {
    let mut engine = TransferSession::new(config(), binding(), Time(0)).unwrap();
    let mut next = 1;
    let attach = through_image(&mut engine, &mut next);
    let mut allocate = || {
        next += 1;
        Some(op(next))
    };
    let TransferEffect::FetchBarrier { op: first_b, .. } = accept(
        &mut engine,
        TransferEvent::StreamAttached {
            op: attach,
            token: AttachToken {
                reservation: reservation(),
                operation: 42,
            },
        },
        &mut allocate,
    ) else {
        panic!("first B")
    };
    let TransferEffect::FetchBatch { op: fetch, .. } = accept(
        &mut engine,
        TransferEvent::BarrierReceived {
            op: first_b,
            receipt: barrier(2, 1, 51),
        },
        &mut allocate,
    ) else {
        panic!("batch")
    };
    let TransferEffect::AckBatch { op: ack, .. } = accept(
        &mut engine,
        TransferEvent::BatchStaged {
            op: fetch,
            batch: JournalBatch {
                reservation: reservation(),
                operation: 60,
                from: cursor(0),
                through: cursor(2),
                deltas: vec![
                    JournalDelta {
                        position: 1,
                        identity: DeltaIdentity::Native(cut(1)),
                        effect: vec![1, 5],
                    },
                    JournalDelta {
                        position: 2,
                        identity: DeltaIdentity::Local(b"x".to_vec()),
                        effect: vec![0],
                    },
                ],
                bytes: 5,
            },
        },
        &mut allocate,
    ) else {
        panic!("ack")
    };
    let TransferEffect::CheckNativeCoverage { op: cover, .. } = accept(
        &mut engine,
        TransferEvent::BatchAcknowledged {
            op: ack,
            through: cursor(2),
        },
        &mut allocate,
    ) else {
        panic!("coverage")
    };
    let coverage = NativeCoverageReceipt {
        parent: op(1),
        barrier: barrier(2, 1, 51),
        staged_through: cursor(2),
        proven_cuts: vec![cut(1)],
        members: offer().members,
        buffered_bytes: 0,
    };
    let TransferEffect::InstallCandidate { op: install, .. } = accept(
        &mut engine,
        TransferEvent::NativeCovered {
            op: cover,
            coverage: coverage.clone(),
        },
        &mut allocate,
    ) else {
        panic!("install")
    };
    assert_eq!(
        accept(
            &mut engine,
            TransferEvent::NativePending { op: install },
            &mut allocate,
        ),
        TransferEffect::ArmTimer(Time(2))
    );
    assert_eq!(engine.stage(), TransferStage::WaitingCoverage);
    let stale = engine.step(
        TransferEvent::Installed {
            op: install,
            handoff: Box::new(NativeHandoffReceipt {
                recovery: recovery(),
                install,
                coverage,
                attachment: AttachToken {
                    reservation: reservation(),
                    operation: 42,
                },
                schema: 1,
                applier_generation: 1,
                continued_cuts: vec![cut(1)],
                buffered_bytes: 0,
            }),
        },
        &mut allocate,
    );
    assert_eq!(stale.rejection, Some(TransferError::Stale));
    let TransferEffect::AdvanceBarrier { expected, .. } =
        accept(&mut engine, TransferEvent::Tick(Time(2)), &mut allocate)
    else {
        panic!("later B from the retained stage")
    };
    assert_eq!(expected, barrier(2, 1, 51));
}

#[test]
fn quiet_zero_cut_handoff_is_valid_but_mismatched_attachment_aborts() {
    for wrong_attachment in [false, true] {
        let mut engine = TransferSession::new(config(), binding(), Time(0)).unwrap();
        let mut next = 1;
        let attach = through_image(&mut engine, &mut next);
        let mut allocate = || {
            next += 1;
            Some(op(next))
        };
        let attachment = AttachToken {
            reservation: reservation(),
            operation: 42,
        };
        let TransferEffect::FetchBarrier { op: fetch, .. } = accept(
            &mut engine,
            TransferEvent::StreamAttached {
                op: attach,
                token: attachment.clone(),
            },
            &mut allocate,
        ) else {
            panic!("barrier")
        };
        let receipt = barrier(0, 0, 51);
        let TransferEffect::CheckNativeCoverage { op: cover, .. } = accept(
            &mut engine,
            TransferEvent::BarrierReceived {
                op: fetch,
                receipt: receipt.clone(),
            },
            &mut allocate,
        ) else {
            panic!("quiet native coverage")
        };
        let coverage = NativeCoverageReceipt {
            parent: op(1),
            staged_through: receipt.cursor.clone(),
            proven_cuts: receipt.covered_cuts.clone(),
            members: receipt.members.clone(),
            buffered_bytes: 0,
            barrier: receipt,
        };
        let TransferEffect::InstallCandidate { op: install, .. } = accept(
            &mut engine,
            TransferEvent::NativeCovered {
                op: cover,
                coverage: coverage.clone(),
            },
            &mut allocate,
        ) else {
            panic!("quiet install")
        };
        let handoff = NativeHandoffReceipt {
            recovery: recovery(),
            install,
            coverage,
            attachment: AttachToken {
                operation: if wrong_attachment { 43 } else { 42 },
                ..attachment
            },
            schema: 1,
            applier_generation: 1,
            continued_cuts: vec![cut(0)],
            buffered_bytes: 0,
        };
        let step = engine.step(
            TransferEvent::Installed {
                op: install,
                handoff: Box::new(handoff),
            },
            &mut allocate,
        );
        if wrong_attachment {
            assert_eq!(step.rejection, Some(TransferError::Continuity));
            assert_eq!(engine.stage(), TransferStage::Aborted);
            assert!(
                step.effects
                    .iter()
                    .any(|effect| matches!(effect, TransferEffect::ReleaseReservation(_)))
            );
        } else {
            assert_eq!(step.rejection, None);
            assert_eq!(engine.stage(), TransferStage::Completed);
        }
    }
}

#[test]
fn allocator_alias_and_exhaustion_abort_with_exact_cleanup() {
    for bad in [
        None,
        Some(op(1)),
        Some(BootstrapOperation {
            session: 10,
            ..op(2)
        }),
        Some(BootstrapOperation {
            incarnation: BootId(8),
            ..op(2)
        }),
        Some(BootstrapOperation {
            generation: 2,
            ..op(2)
        }),
    ] {
        let mut engine = TransferSession::new(config(), binding(), Time(0)).unwrap();
        let step = engine.step(TransferEvent::Start, &mut || bad);
        assert_eq!(step.rejection, Some(TransferError::Allocator));
        assert_eq!(engine.stage(), TransferStage::Aborted);
        assert_eq!(
            step.effects,
            vec![TransferEffect::DiscardStage { parent: op(1) },]
        );
    }
}

#[test]
fn local_identity_and_schema_must_match_before_stage_admission() {
    let mut wrong_local = binding();
    wrong_local.follower.session = 10;
    assert_eq!(
        TransferSession::new(config(), wrong_local, Time(0)).map(|_| ()),
        Err(TransferError::InvalidConfig)
    );
    let mut engine = TransferSession::new(config(), binding(), Time(0)).unwrap();
    let mut next = 1;
    let mut allocate = || {
        next += 1;
        Some(op(next))
    };
    let TransferEffect::FetchOffer { op: fetch, .. } =
        accept(&mut engine, TransferEvent::Start, &mut allocate)
    else {
        panic!("fetch offer")
    };
    let mut incompatible = offer();
    incompatible.schema = 2;
    let step = engine.step(
        TransferEvent::Offered {
            op: fetch,
            offer: incompatible,
        },
        &mut allocate,
    );
    assert_eq!(step.rejection, Some(TransferError::Schema));
    assert_eq!(engine.stage(), TransferStage::Aborted);
    assert_eq!(
        step.effects,
        vec![TransferEffect::DiscardStage { parent: op(1) }]
    );
}

#[test]
fn expiry_and_cancel_release_exact_reservation_and_reject_late_install() {
    let mut engine = TransferSession::new(config(), binding(), Time(0)).unwrap();
    let mut next = 1;
    let attach = through_image(&mut engine, &mut next);
    let step = engine.step(TransferEvent::Tick(Time(20)), &mut || None);
    assert_eq!(step.rejection, Some(TransferError::Expired));
    assert_eq!(
        step.effects,
        vec![
            TransferEffect::DiscardStage { parent: op(1) },
            TransferEffect::ReleaseReservation(reservation()),
        ]
    );
    assert_eq!(
        engine
            .step(
                TransferEvent::Installed {
                    op: attach,
                    handoff: Box::new(NativeHandoffReceipt {
                        recovery: recovery(),
                        install: attach,
                        coverage: NativeCoverageReceipt {
                            parent: op(1),
                            barrier: barrier(0, 0, 1),
                            staged_through: cursor(0),
                            proven_cuts: vec![cut(0)],
                            members: offer().members,
                            buffered_bytes: 0,
                        },
                        attachment: AttachToken {
                            reservation: reservation(),
                            operation: 42,
                        },
                        schema: 1,
                        applier_generation: 1,
                        continued_cuts: vec![cut(0)],
                        buffered_bytes: 0,
                    }),
                },
                &mut || None,
            )
            .rejection,
        Some(TransferError::Stale)
    );
    let mut cancelled = TransferSession::new(config(), binding(), Time(0)).unwrap();
    let _ = through_image(&mut cancelled, &mut next);
    let step = cancelled.step(TransferEvent::Cancel, &mut || None);
    assert_eq!(step.rejection, None);
    assert_eq!(cancelled.stage(), TransferStage::Aborted);
}

/// Replay one barrier covering `covered` through one batch of the native
/// `deltas`, from the offer's writer at epoch 1, sequence 0. Returns the
/// session's first effect after the batch.
fn replay_native(covered: NativeCut, deltas: &[NativeCut]) -> (TransferSession, TransferStep) {
    let mut engine = TransferSession::new(config(), binding(), Time(0)).unwrap();
    let mut next = 1;
    let attach = through_image(&mut engine, &mut next);
    let mut allocate = || {
        next += 1;
        Some(op(next))
    };
    let TransferEffect::FetchBarrier { op: first_b, .. } = accept(
        &mut engine,
        TransferEvent::StreamAttached {
            op: attach,
            token: AttachToken {
                reservation: reservation(),
                operation: 42,
            },
        },
        &mut allocate,
    ) else {
        panic!("first B")
    };
    let through = u64::try_from(deltas.len()).unwrap();
    let receipt = BarrierReceipt {
        covered_cuts: vec![covered],
        ..barrier(through, 0, 51)
    };
    let TransferEffect::FetchBatch { op: fetch, .. } = accept(
        &mut engine,
        TransferEvent::BarrierReceived {
            op: first_b,
            receipt,
        },
        &mut allocate,
    ) else {
        panic!("batch")
    };
    let deltas: Vec<_> = (1..)
        .zip(deltas)
        .map(|(position, cut)| JournalDelta {
            position,
            identity: DeltaIdentity::Native(cut.clone()),
            effect: vec![2],
        })
        .collect();
    let bytes = deltas.len() * 2;
    let step = engine.step(
        TransferEvent::BatchStaged {
            op: fetch,
            batch: JournalBatch {
                reservation: reservation(),
                operation: 60,
                from: cursor(0),
                through: cursor(through),
                deltas,
                bytes,
            },
        },
        &mut allocate,
    );
    (engine, step)
}

fn renewed(epoch: u64, sequence: u64) -> NativeCut {
    NativeCut {
        epoch,
        ..cut(sequence)
    }
}

/// The donor journal records a sealed renewal as a native delta at sequence
/// zero of the writer's newer epoch. A follower replaying the suffix crosses
/// with it, and a barrier in the new life covers the offer's older cut.
#[test]
fn a_sealed_renewal_in_the_suffix_replays_into_the_new_life() {
    let (engine, step) = replay_native(renewed(3, 1), &[cut(1), renewed(3, 0), renewed(3, 1)]);
    assert_eq!(step.rejection, None);
    assert!(matches!(
        step.effects.first(),
        Some(TransferEffect::AckBatch { .. })
    ));
    assert_ne!(engine.stage(), TransferStage::Aborted);
}

/// A crossing is only ever to sequence zero of a newer epoch and never past
/// the barrier; a native delta that skips into another life, or back into an
/// older one, breaks replay continuity and aborts the transfer.
#[test]
fn a_native_delta_that_is_not_a_contiguous_step_or_renewal_aborts_replay() {
    for (covered, deltas) in [
        (renewed(3, 1), vec![renewed(3, 1)]),
        (renewed(3, 0), vec![cut(1), renewed(3, 0), cut(2)]),
        (cut(1), vec![cut(1), renewed(3, 0)]),
        (renewed(3, 0), vec![renewed(3, 0), renewed(4, 0)]),
    ] {
        let (engine, step) = replay_native(covered.clone(), &deltas);
        assert_eq!(
            engine.stage(),
            TransferStage::Aborted,
            "{covered:?} {deltas:?} {step:?}"
        );
    }
}
