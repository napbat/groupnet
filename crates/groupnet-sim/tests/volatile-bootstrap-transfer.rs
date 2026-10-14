//! Queued donor transfer callbacks under seeded delay, duplication, and loss,
//! with the donor's native writer either continuing its life or sealing it
//! and renewing into its next life between the barrier sample and delivery.

use groupnet_core::volatile_bootstrap::journal::{
    AttachToken, BarrierReceipt, CaptureId, DeltaIdentity, DonorJournal, JournalConfig,
    JournalCursor, NativeCut, ReservationId,
};
use groupnet_core::volatile_bootstrap::transfer::{
    NativeCoverageReceipt, NativeHandoffReceipt, TransferConfig, TransferEffect, TransferEvent,
    TransferOffer,
};
use groupnet_core::volatile_bootstrap::{
    BootId, BootstrapClaim, BootstrapConfig, BootstrapEffect, BootstrapEvent, BootstrapMember,
    BootstrapMemberIdentity, BootstrapOperation, BootstrapScope, BootstrapStage, ClaimEngine,
    ClaimIdentity, ClaimPhase, PresenceIdentity,
};
use groupnet_core::volatile_recovery::RecoveryOperation;
use groupnet_core::{NodeId, Status, Time, placement};
use groupnet_sim::SplitMix64;

#[derive(Clone)]
struct Scheduled {
    at: u64,
    event: BootstrapEvent,
}

struct Fixture {
    parent: BootstrapOperation,
    donor: ClaimIdentity,
    follower: ClaimIdentity,
    offer: TransferOffer,
    journal: DonorJournal,
    reservation: Option<ReservationId>,
    barrier: Option<BarrierReceipt>,
    staged_image: Option<u8>,
    /// The follower's staged native position of writer `w`, epoch-major.
    staged_native: (u64, u64),
    reference_image: Option<u8>,
    barrier_advances: usize,
    batches: usize,
    renewal: u64,
    /// The writer seals and renews mid-transfer instead of writing once more.
    renews: bool,
}

impl Fixture {
    /// The journal position the advanced barrier reaches, and the writer's
    /// cut there.
    fn final_barrier(&self) -> (u64, (u64, u64)) {
        if self.renews {
            (5, (2, 1))
        } else {
            (3, (1, 2))
        }
    }
}

fn scope() -> BootstrapScope {
    BootstrapScope {
        domain: "o".into(),
        partition: "b".into(),
    }
}

fn claim_config() -> BootstrapConfig {
    BootstrapConfig {
        max_members: 2,
        max_member_bytes: 8,
        max_scope_bytes: 16,
        settle_ms: 3,
        renew_ms: 4,
        claim_ttl_ms: 12,
        observe_ms: 2,
        donor_wait_ms: 14,
        total_ms: 24,
    }
}

fn transfer_config() -> TransferConfig {
    TransferConfig {
        expected_schema: 1,
        max_metadata_bytes: 64,
        max_encoded_bytes: 8,
        max_decoded_bytes: 8,
        max_chunk_bytes: 8,
        max_chunks: 2,
        max_batch_bytes: 8,
        max_batch_events: 2,
        max_replay_events: 6,
        max_native_buffer_bytes: 8,
        max_members: 2,
        max_cuts: 1,
        coverage_poll_ms: 2,
    }
}

fn journal_config() -> JournalConfig {
    JournalConfig {
        max_encoded_bytes: 8,
        max_decoded_bytes: 8,
        max_events: 6,
        max_suffix_bytes: 16,
        max_event_bytes: 4,
        max_identity_bytes: 2,
        max_followers: 1,
        max_follower_id_bytes: 8,
        max_cuts: 1,
        max_cut_bytes: 2,
        max_members: 2,
        max_membership_bytes: 16,
        max_scope_bytes: 8,
        max_batch_events: 1,
        max_batch_bytes: 4,
        max_inflight_bytes: 4,
        max_total_ms: 24,
        max_follower_ms: 14,
    }
}

fn apply(image: &mut Option<u8>, effect: &[u8]) {
    match effect {
        [0] => *image = None,
        [1, value] => *image = Some(*value),
        [2] => {} // native no-op still advances its exact writer cut
        _ => panic!("invalid test mutation"),
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "one fixture binds the claim selection, real donor capture, and exact source identities"
)]
fn start(renews: bool) -> (ClaimEngine, Fixture, Vec<BootstrapEffect>) {
    let names = [NodeId::from("a"), NodeId::from("b")];
    let roster = names.iter().cloned().collect();
    let owner = placement::owner(&scope().placement_key(), &roster).unwrap();
    let follower_node = if owner.as_str() == "a" { "b" } else { "a" };
    let mut engine = ClaimEngine::new(
        claim_config(),
        scope(),
        NodeId::from(follower_node),
        BootId(1_u128 << 100),
        9,
    )
    .unwrap();
    engine.enable_transfer(transfer_config()).unwrap();
    let begin = engine.step(BootstrapEvent::Start);
    let local = begin
        .effects
        .into_iter()
        .find_map(|effect| match effect {
            BootstrapEffect::PublishClaim(claim) => Some(claim),
            _ => None,
        })
        .unwrap();
    let observed = engine.step(BootstrapEvent::Tick(Time(3)));
    let observe = observed
        .effects
        .iter()
        .find_map(|effect| match effect {
            BootstrapEffect::ObserveClaims { op, .. } => Some(*op),
            _ => None,
        })
        .unwrap();
    let donor = ClaimIdentity {
        node: owner,
        incarnation: BootId((1_u128 << 101) + 1),
        session: 2,
        attempt: 1,
    };
    let follower = local.identity.clone();
    let ready = BootstrapClaim {
        identity: donor.clone(),
        renewal: 2,
        phase: ClaimPhase::Ready,
        progress: 0,
        remaining_ms: 12,
    };
    let chosen = engine.step(BootstrapEvent::ClaimsObserved {
        op: observe,
        members: names
            .into_iter()
            .map(|node| BootstrapMember {
                node,
                eligible: true,
            })
            .collect(),
        claims: vec![local, ready],
    });
    let parent = chosen
        .effects
        .iter()
        .find_map(|effect| match effect {
            BootstrapEffect::DonorAvailable { op, .. } => Some(*op),
            _ => None,
        })
        .unwrap();
    let capture = CaptureId {
        scope: scope(),
        donor: donor.clone(),
        recovery_generation: 1,
        serial: 1,
    };
    let cursor = JournalCursor {
        capture: capture.clone(),
        position: 0,
    };
    let mut members = vec![donor.clone(), follower.clone()]
        .into_iter()
        .map(|claim| BootstrapMemberIdentity {
            node: claim.node.clone(),
            presence: Some(PresenceIdentity {
                node: claim.node,
                boot: claim.incarnation,
                session: claim.session,
            }),
            member_incarnation: 1,
            status: Status::Alive,
        })
        .collect::<Vec<_>>();
    members.sort_by(|a, b| a.node.cmp(&b.node));
    let cuts = vec![NativeCut {
        writer: b"w".to_vec(),
        epoch: 1,
        sequence: 0,
    }];
    let offer = TransferOffer {
        capture: capture.clone(),
        image_cut: cursor.clone(),
        schema: 1,
        encoded_bytes: 1,
        decoded_bytes: 1,
        chunks: 1,
        commitment: [7; 32],
        members: members.clone(),
        cuts: cuts.clone(),
    };
    let mut journal = DonorJournal::new(journal_config(), capture).unwrap();
    journal.begin_capture(Time(3), 1, 1, members, cuts).unwrap();
    assert_eq!(journal.finish_capture(Time(3), 1, 1).unwrap(), cursor);
    journal
        .append(
            Time(3),
            1,
            DeltaIdentity::Native(NativeCut {
                writer: b"w".to_vec(),
                epoch: 1,
                sequence: 1,
            }),
            vec![2],
        )
        .unwrap();
    journal
        .append(Time(3), 1, DeltaIdentity::Local(b"r".to_vec()), vec![1, 9])
        .unwrap();
    let effects = engine
        .step(BootstrapEvent::StartTransfer {
            op: parent,
            selected: donor.clone(),
        })
        .effects;
    (
        engine,
        Fixture {
            parent,
            donor,
            follower,
            offer,
            journal,
            reservation: None,
            barrier: None,
            staged_image: None,
            staged_native: (1, 0),
            reference_image: Some(9),
            barrier_advances: 0,
            batches: 0,
            renewal: 2,
            renews,
        },
        effects,
    )
}

#[expect(
    clippy::too_many_lines,
    reason = "the fake source executes every bounded donor journal operation in exact receipt order"
)]
fn reply(
    effect: TransferEffect,
    fixture: &mut Fixture,
    now: u64,
    corrupt: bool,
) -> Option<BootstrapEvent> {
    let event = match effect {
        TransferEffect::FetchOffer { op, .. } => {
            let mut offer = fixture.offer.clone();
            if corrupt {
                offer.schema = 2;
            }
            TransferEvent::Offered { op, offer }
        }
        TransferEffect::ReserveStage { op, .. } => TransferEvent::StageReserved { op },
        TransferEffect::ReserveDonor { op, capture, cut } => {
            assert_eq!(capture, fixture.offer.capture);
            assert_eq!(cut, fixture.offer.image_cut);
            // A donor journal operation after the reservation's own bound
            // (a faulted transfer that kept advancing) fails as the real
            // donor does, and the follower aborts.
            let Ok(reservation) =
                fixture
                    .journal
                    .reserve(Time(now), fixture.follower.clone(), &cut)
            else {
                return Some(failed(op));
            };
            fixture.reservation = Some(reservation.clone());
            TransferEvent::DonorReserved { op, reservation }
        }
        TransferEffect::FetchChunk { op, sequence, .. } => {
            assert_eq!(sequence, 0);
            fixture.staged_image = Some(7);
            TransferEvent::ChunkStored {
                op,
                sequence,
                bytes: 1,
                decoded_charge: 1,
            }
        }
        TransferEffect::VerifyImage { op, .. } => TransferEvent::ImageVerified {
            op,
            commitment: fixture.offer.commitment,
        },
        TransferEffect::AttachStream { op, reservation } => {
            let Ok(token) = fixture.journal.begin_attach(Time(now), &reservation) else {
                return Some(failed(op));
            };
            if fixture.journal.confirm_attach(Time(now), &token).is_err() {
                return Some(failed(op));
            }
            TransferEvent::StreamAttached { op, token }
        }
        TransferEffect::FetchBarrier { op, reservation } => {
            let Ok(receipt) = fixture.journal.barrier(Time(now), &reservation) else {
                return Some(failed(op));
            };
            assert_eq!(receipt.cursor.position, 2);
            fixture.barrier = Some(receipt.clone());
            // The donor's writer moves after B was sampled but before B is
            // delivered: one more write, or a seal, its renewal into the
            // next life, and that life's first write.
            let moved = if fixture.renews {
                let seal = NativeCut {
                    writer: b"w".to_vec(),
                    epoch: 1,
                    sequence: 2,
                };
                fixture
                    .journal
                    .append(Time(now), 1, DeltaIdentity::Native(seal.clone()), vec![2])
                    .and_then(|_| fixture.journal.renew(Time(now), 1, &seal, 2, vec![2]))
                    .and_then(|_| {
                        fixture.journal.append(
                            Time(now),
                            1,
                            DeltaIdentity::Native(NativeCut {
                                writer: b"w".to_vec(),
                                epoch: 2,
                                sequence: 1,
                            }),
                            vec![0],
                        )
                    })
            } else {
                fixture.journal.append(
                    Time(now),
                    1,
                    DeltaIdentity::Native(NativeCut {
                        writer: b"w".to_vec(),
                        epoch: 1,
                        sequence: 2,
                    }),
                    vec![0],
                )
            };
            if moved.is_err() {
                return Some(failed(op));
            }
            apply(&mut fixture.reference_image, &[0]);
            TransferEvent::BarrierReceived { op, receipt }
        }
        TransferEffect::AdvanceBarrier { op, expected } => {
            assert_eq!(fixture.barrier.as_ref(), Some(&expected));
            let reservation = fixture.reservation.as_ref().unwrap();
            let Ok(receipt) = fixture
                .journal
                .advance_barrier(Time(now), reservation, &expected)
            else {
                return Some(failed(op));
            };
            assert_eq!(receipt.cursor.position, fixture.final_barrier().0);
            fixture.barrier = Some(receipt.clone());
            fixture.barrier_advances += 1;
            TransferEvent::BarrierReceived { op, receipt }
        }
        TransferEffect::FetchBatch { op, receipt } => {
            let reservation = fixture.reservation.as_ref().unwrap();
            let Ok(batch) = fixture.journal.read_batch(Time(now), reservation, &receipt) else {
                return Some(failed(op));
            };
            let batch = batch.unwrap();
            for delta in &batch.deltas {
                if let DeltaIdentity::Native(cut) = &delta.identity {
                    let (epoch, sequence) = fixture.staged_native;
                    // Each native delta continues its life by one, or a
                    // sealed renewal opens the next life at zero.
                    assert!(
                        (cut.epoch, cut.sequence) == (epoch, sequence + 1)
                            || (fixture.renews && cut.epoch > epoch && cut.sequence == 0),
                        "native replay out of order: {cut:?} after {:?}",
                        fixture.staged_native
                    );
                    fixture.staged_native = (cut.epoch, cut.sequence);
                }
                apply(&mut fixture.staged_image, &delta.effect);
            }
            fixture.batches += 1;
            TransferEvent::BatchStaged { op, batch }
        }
        TransferEffect::AckBatch {
            op,
            reservation,
            batch_operation,
            through,
        } => {
            let Ok(confirmed) =
                fixture
                    .journal
                    .ack_batch(Time(now), &reservation, batch_operation, &through)
            else {
                return Some(failed(op));
            };
            TransferEvent::BatchAcknowledged {
                op,
                through: confirmed,
            }
        }
        TransferEffect::CheckNativeCoverage { op, receipt, .. } => {
            assert_eq!(
                fixture.staged_image,
                if receipt.cursor.position == 2 {
                    Some(9)
                } else {
                    None
                }
            );
            assert_eq!(
                fixture.staged_native,
                if receipt.cursor.position == 2 {
                    (1, 1)
                } else {
                    fixture.final_barrier().1
                }
            );
            if receipt.cursor.position == 2 {
                TransferEvent::NativePending { op }
            } else {
                assert_eq!(receipt.cursor.position, fixture.final_barrier().0);
                TransferEvent::NativeCovered {
                    op,
                    coverage: NativeCoverageReceipt {
                        parent: fixture.parent,
                        barrier: receipt.clone(),
                        staged_through: receipt.cursor.clone(),
                        proven_cuts: receipt.covered_cuts.clone(),
                        members: receipt.members.clone(),
                        buffered_bytes: 0,
                    },
                }
            }
        }
        TransferEffect::InstallCandidate { op, coverage } => {
            let receipt = coverage.barrier.clone();
            let (position, (epoch, sequence)) = fixture.final_barrier();
            assert_eq!(receipt.cursor.position, position);
            assert_eq!(
                (
                    receipt.covered_cuts[0].epoch,
                    receipt.covered_cuts[0].sequence
                ),
                (epoch, sequence)
            );
            assert_eq!(fixture.staged_native, (epoch, sequence));
            assert_eq!(fixture.staged_image, fixture.reference_image);
            TransferEvent::Installed {
                op,
                handoff: Box::new(NativeHandoffReceipt {
                    recovery: RecoveryOperation {
                        session: 1,
                        generation: 1,
                        token: 1,
                    },
                    install: op,
                    coverage: *coverage,
                    attachment: AttachToken {
                        reservation: receipt.reservation.clone(),
                        operation: receipt.attach_operation,
                    },
                    schema: 1,
                    applier_generation: 1,
                    continued_cuts: receipt.covered_cuts,
                    buffered_bytes: 0,
                }),
            }
        }
        TransferEffect::DiscardStage { .. } => {
            fixture.staged_image = None;
            return None;
        }
        TransferEffect::ReleaseReservation(reservation) => {
            let _ = fixture.journal.release(Time(now), &reservation);
            return None;
        }
        TransferEffect::ArmTimer(_) => return None,
    };
    Some(BootstrapEvent::Transfer(Box::new(event)))
}

/// The donor answered `op` with an error.
fn failed(op: BootstrapOperation) -> BootstrapEvent {
    BootstrapEvent::Transfer(Box::new(TransferEvent::Failed { op }))
}

#[derive(Default)]
struct Coverage {
    completed: usize,
    fallback: usize,
    stale: usize,
    dropped: usize,
    duplicated: usize,
    refreshes: usize,
    cleanup: usize,
    complete_batches: usize,
    complete_barriers: usize,
    restarts: usize,
    ack_drops: usize,
    ack_duplicates: usize,
}

#[expect(
    clippy::too_many_arguments,
    reason = "queued schedule explicitly carries source fixture, RNG, and fault ledger"
)]
fn enqueue(
    effects: Vec<BootstrapEffect>,
    now: u64,
    healthy: bool,
    seed: u64,
    fixture: &mut Fixture,
    queue: &mut Vec<Scheduled>,
    rng: &mut SplitMix64,
    coverage: &mut Coverage,
) {
    for effect in effects {
        let event = match effect {
            BootstrapEffect::Transfer(effect) => {
                if matches!(effect.as_ref(), TransferEffect::DiscardStage { .. }) {
                    coverage.cleanup += 1;
                }
                reply(*effect, fixture, now, !healthy && seed.is_multiple_of(7))
            }
            BootstrapEffect::ObserveSelectedClaim { op, selected } => {
                assert_eq!(selected, fixture.donor);
                fixture.renewal += 1;
                coverage.refreshes += 1;
                let claim = if !healthy && seed.is_multiple_of(17) {
                    None
                } else {
                    Some(BootstrapClaim {
                        identity: fixture.donor.clone(),
                        renewal: fixture.renewal,
                        phase: ClaimPhase::Ready,
                        progress: 0,
                        remaining_ms: 12,
                    })
                };
                Some(BootstrapEvent::SelectedClaimObserved { op, claim })
            }
            BootstrapEffect::FallbackOrigin => {
                coverage.fallback += 1;
                None
            }
            BootstrapEffect::CancelWork { .. }
            | BootstrapEffect::PublishPresence(_)
            | BootstrapEffect::WithdrawPresence(_)
            | BootstrapEffect::PublishClaim(_)
            | BootstrapEffect::WithdrawClaim(_)
            | BootstrapEffect::ObserveClaims { .. }
            | BootstrapEffect::BuildOrigin { .. }
            | BootstrapEffect::FollowBuilder { .. }
            | BootstrapEffect::BuilderProgressed
            | BootstrapEffect::Released { .. }
            | BootstrapEffect::DonorAvailable { .. }
            | BootstrapEffect::ArmTimer(_)
            | BootstrapEffect::RecaptureCurrent { .. } => None,
        };
        let Some(event) = event else { continue };
        let is_ack = matches!(&event, BootstrapEvent::Transfer(inner)
            if matches!(inner.as_ref(), TransferEvent::BatchAcknowledged { .. }));
        if !healthy && ((is_ack && seed.is_multiple_of(5)) || rng.below(9) == 0) {
            coverage.dropped += 1;
            coverage.ack_drops += usize::from(is_ack);
            continue;
        }
        let delay = if healthy { 0 } else { u64::from(rng.below(4)) };
        queue.push(Scheduled {
            at: now + delay,
            event: event.clone(),
        });
        if rng.below(4) == 0 {
            queue.push(Scheduled {
                at: now + delay + 1,
                event,
            });
            coverage.duplicated += 1;
            coverage.ack_duplicates += usize::from(is_ack);
        }
    }
}

fn run(seed: u64, healthy: bool, renews: bool, coverage: &mut Coverage) {
    let (mut engine, mut fixture, effects) = start(renews);
    assert_ne!(fixture.follower, fixture.donor);
    let mut rng = SplitMix64::new(seed);
    let mut queue = Vec::new();
    let mut completed = false;
    enqueue(
        effects,
        3,
        healthy,
        seed,
        &mut fixture,
        &mut queue,
        &mut rng,
        coverage,
    );
    let restarted = !healthy && seed.is_multiple_of(11);
    if restarted {
        coverage.restarts += 1;
        let superseded = engine.step(BootstrapEvent::Start);
        enqueue(
            superseded.effects,
            3,
            healthy,
            seed,
            &mut fixture,
            &mut queue,
            &mut rng,
            coverage,
        );
    }
    // Every transfer advance restarts the 14 ms stall bound, so a faulted run
    // that stalls late ends a stall bound after its last advance, past the
    // original 24 ms total; the horizon still bounds every run.
    for now in 3..=64 {
        let tick = engine.step(BootstrapEvent::Tick(Time(now)));
        enqueue(
            tick.effects,
            now,
            healthy,
            seed,
            &mut fixture,
            &mut queue,
            &mut rng,
            coverage,
        );
        let mut serviced = 0;
        while let Some(index) = queue.iter().position(|message| message.at <= now) {
            serviced += 1;
            assert!(serviced < 128, "finite effect queue per logical tick");
            let message = queue.swap_remove(index);
            let step = engine.step(message.event);
            if step.rejection.is_some() {
                coverage.stale += 1;
            }
            enqueue(
                step.effects,
                now,
                healthy,
                seed,
                &mut fixture,
                &mut queue,
                &mut rng,
                coverage,
            );
        }
        if engine.stage() == BootstrapStage::Transferred && !completed {
            completed = true;
            coverage.completed += 1;
            coverage.complete_batches += fixture.batches;
            coverage.complete_barriers += fixture.barrier_advances;
            // Completed transfer keeps the scoped participation heartbeat.
            assert!(engine.next_deadline().is_some());
        }
    }
    if healthy {
        assert_eq!(engine.stage(), BootstrapStage::Transferred, "seed {seed}");
    } else {
        if restarted {
            assert!(
                !completed,
                "old generation completed after restart, seed {seed}"
            );
        }
        assert!(
            matches!(
                engine.stage(),
                BootstrapStage::Transferred | BootstrapStage::Fallback
            ),
            "seed {seed}"
        );
    }
}

fn transfer_families(renews: bool) {
    let mut healthy = Coverage::default();
    for seed in 1..=48 {
        run(seed, true, renews, &mut healthy);
    }
    let batches = if renews { 5 } else { 3 };
    assert_eq!(healthy.completed, 48);
    assert_eq!(healthy.fallback, 0);
    assert_eq!(healthy.complete_batches, 48 * batches);
    assert_eq!(healthy.complete_barriers, 48);
    assert!(healthy.duplicated > 0);
    assert!(healthy.stale > 0);
    assert!(healthy.ack_duplicates > 0);

    let mut faulty = Coverage::default();
    for seed in 49..=96 {
        run(seed, false, renews, &mut faulty);
    }
    assert!(faulty.dropped > 0);
    assert!(faulty.ack_drops > 0);
    assert!(faulty.duplicated > 0);
    assert!(faulty.cleanup > 0);
    assert!(faulty.fallback > 0);
    assert!(faulty.restarts > 0);
}

#[test]
fn queued_transfer_healthy_and_faulted_families_preserve_deadlines_and_cleanup() {
    transfer_families(false);
}

/// A writer that seals and renews into its next life while a transfer is in
/// flight is followed across the restart: every healthy run installs at the
/// new life's cut with no fallback, and faulted runs still end installed or
/// on the origin with their stage discarded.
#[test]
fn queued_transfer_follows_a_sealed_renewal_in_the_middle_of_the_transfer() {
    transfer_families(true);
}
