//! The existing worker's exact effects traverse the reusable bulk adapter.

#![cfg(feature = "volatile-bootstrap-bulk")]

use std::sync::Arc;
use std::time::{Duration, Instant};

use groupnet_consistency::volatile_recovery::bootstrap::admission::{
    AdmissionClass, AdmissionLimits, Admitted, ByteAdmission,
};
use groupnet_consistency::volatile_recovery::bootstrap::bulk_adapter::{
    BootstrapStatePort, BulkDonorPort,
};
use groupnet_consistency::volatile_recovery::bootstrap::bulk_wire::{
    BootstrapBulkListener, BulkLimits, PhaseLimits, WireLimits,
};
use groupnet_consistency::volatile_recovery::bootstrap::inbox::DonorInbox;
use groupnet_consistency::volatile_recovery::bootstrap::ports::{
    DonorCapture, DonorPort, DonorReply, DonorRequest, JournalIngress, LocalCaptureOutcome,
    LocalCaptureRequest, TransferContext, TransferResources,
};
use groupnet_consistency::volatile_recovery::{AdapterError, BoxRecoveryFuture, PublicationPermit};
use groupnet_core::Time;
use groupnet_core::volatile_bootstrap::journal::{
    AttachToken, CaptureId, DeltaIdentity, DonorJournal, JournalBatch, JournalConfig,
    JournalCursor, ReservationId,
};
use groupnet_core::volatile_bootstrap::transfer::{
    TransferConfig, TransferEffect, TransferEvent, TransferOffer,
};
use groupnet_core::volatile_bootstrap::{
    BootId, BootstrapMemberIdentity, BootstrapOperation, BootstrapScope, ClaimIdentity,
    PresenceIdentity,
};
use groupnet_core::{NodeId, Status};
use groupnet_transport::bulk::DataPlane;
use groupnet_transport_mem::MemBulkNet;
use tokio::sync::{Notify, watch};

#[derive(Debug)]
struct PrivateState;

fn member(claim: ClaimIdentity) -> BootstrapMemberIdentity {
    BootstrapMemberIdentity {
        node: claim.node.clone(),
        presence: Some(PresenceIdentity {
            node: claim.node,
            boot: claim.incarnation,
            session: claim.session,
        }),
        member_incarnation: 1,
        status: Status::Alive,
    }
}

impl BootstrapStatePort for PrivateState {
    type Image = Vec<u8>;
    type Stage = Vec<u8>;
    type NativeBuffer = Vec<u8>;

    fn build_local_capture<'a>(
        &'a self,
        _request: LocalCaptureRequest,
        _admission: &'a ByteAdmission,
    ) -> BoxRecoveryFuture<'a, Result<LocalCaptureOutcome<Self::Image>, AdapterError>> {
        Box::pin(async { Err(AdapterError) })
    }

    fn retire_local_capture(&self, _capture: &DonorCapture<Self::Image>) {}

    fn image_offer(
        &self,
        capture: &DonorCapture<Self::Image>,
        _max_metadata_bytes: usize,
        admission: &ByteAdmission,
    ) -> Result<Admitted<TransferOffer>, AdapterError> {
        let charge = admission
            .reserve(AdmissionClass::Inflight, 512)
            .map_err(|_| AdapterError)?;
        let id = capture
            .ingress()
            .with_journal(|journal| journal.id().clone());
        let (_, context, _, _, _) = setup();
        Ok(charge.hold(TransferOffer {
            capture: id.clone(),
            image_cut: JournalCursor {
                capture: id,
                position: 0,
            },
            schema: 1,
            encoded_bytes: 1,
            decoded_bytes: 1,
            chunks: 1,
            commitment: [9; 32],
            members: vec![member(context.donor), member(context.follower)],
            cuts: vec![],
        }))
    }

    fn image_chunk(
        &self,
        capture: &DonorCapture<Self::Image>,
        _sequence: usize,
        _max_bytes: usize,
        admission: &ByteAdmission,
    ) -> Result<Admitted<Vec<u8>>, AdapterError> {
        let charge = admission
            .reserve(AdmissionClass::Inflight, 8)
            .map_err(|_| AdapterError)?;
        Ok(charge.hold(capture.image().clone()))
    }

    fn execute_local<'a>(
        &'a self,
        _context: &'a TransferContext,
        _effect: TransferEffect,
        _resources: &'a mut TransferResources<Self::Stage, AttachToken, Self::NativeBuffer>,
        _admission: &'a ByteAdmission,
        _permit: Option<PublicationPermit>,
        _deadline: Instant,
    ) -> BoxRecoveryFuture<'a, Result<Option<Admitted<TransferEvent>>, AdapterError>> {
        Box::pin(async { Err(AdapterError) })
    }

    fn store_chunk<'a>(
        &'a self,
        sequence: usize,
        chunk: &'a Admitted<Vec<u8>>,
        _resources: &'a mut TransferResources<Self::Stage, AttachToken, Self::NativeBuffer>,
    ) -> BoxRecoveryFuture<'a, Result<usize, AdapterError>> {
        Box::pin(async move {
            assert_eq!(sequence, 0);
            assert_eq!(chunk.get(), &[9]);
            Ok(1)
        })
    }

    fn stage_batch<'a>(
        &'a self,
        _batch: &'a Admitted<JournalBatch>,
        _resources: &'a mut TransferResources<Self::Stage, AttachToken, Self::NativeBuffer>,
    ) -> BoxRecoveryFuture<'a, Result<(), AdapterError>> {
        Box::pin(async { Ok(()) })
    }

    fn detach<'a>(
        &'a self,
        _token: AttachToken,
        _resources: &'a mut TransferResources<Self::Stage, AttachToken, Self::NativeBuffer>,
    ) -> BoxRecoveryFuture<'a, Result<(), AdapterError>> {
        Box::pin(async { Ok(()) })
    }
}

fn admission() -> ByteAdmission {
    ByteAdmission::new(AdmissionLimits {
        max_total_bytes: 100_000,
        max_encoded_bytes: 100_000,
        max_decoded_bytes: 100_000,
        max_suffix_bytes: 100_000,
        max_native_overlap_bytes: 100_000,
        max_inflight_bytes: 100_000,
        max_reservations: 100,
    })
    .unwrap()
}

fn limits() -> BulkLimits {
    BulkLimits {
        wire: WireLimits {
            max_frame_bytes: 4096,
            max_scope_bytes: 64,
            max_node_bytes: 32,
            max_payload_bytes: 2048,
        },
        phase: PhaseLimits {
            max_body_bytes: 2048,
            max_scope_bytes: 64,
            max_node_bytes: 32,
            max_writer_bytes: 32,
            max_cuts: 4,
            max_members: 4,
            max_events: 4,
            max_identity_bytes: 32,
            max_effect_bytes: 128,
        },
        server_request_ms: 1000,
    }
}

fn policy() -> TransferConfig {
    TransferConfig {
        expected_schema: 1,
        max_metadata_bytes: 512,
        max_encoded_bytes: 8,
        max_decoded_bytes: 8,
        max_chunk_bytes: 8,
        max_chunks: 2,
        max_batch_bytes: 8,
        max_batch_events: 2,
        max_replay_events: 8,
        max_native_buffer_bytes: 8,
        max_members: 2,
        max_cuts: 1,
        coverage_poll_ms: 5,
    }
}

fn setup() -> (
    BootstrapScope,
    TransferContext,
    CaptureId,
    ReservationId,
    AttachToken,
) {
    let scope = BootstrapScope {
        domain: "origin".into(),
        partition: "bucket".into(),
    };
    let donor = ClaimIdentity {
        node: NodeId::from("donor"),
        incarnation: BootId(11),
        session: 12,
        attempt: 13,
    };
    let follower = ClaimIdentity {
        node: NodeId::from("follower"),
        incarnation: BootId(21),
        session: 22,
        attempt: 23,
    };
    let capture = CaptureId {
        scope: scope.clone(),
        donor: donor.clone(),
        recovery_generation: 2,
        serial: 3,
    };
    let reservation = ReservationId {
        capture: capture.clone(),
        follower: follower.clone(),
        serial: 4,
    };
    let attachment = AttachToken {
        reservation: reservation.clone(),
        operation: 5,
    };
    let context = TransferContext {
        parent: BootstrapOperation {
            incarnation: BootId(21),
            session: 22,
            generation: 1,
            token: 31,
        },
        donor,
        follower,
    };
    (scope, context, capture, reservation, attachment)
}

fn op(token: u64) -> BootstrapOperation {
    BootstrapOperation {
        incarnation: BootId(21),
        session: 22,
        generation: 1,
        token,
    }
}

#[tokio::test]
async fn wrong_admission_pool_is_rejected_before_network_allocation() {
    let net = MemBulkNet::new();
    let plane = DataPlane::new(net.endpoint(NodeId::from("follower")));
    let owned = admission();
    let foreign = admission();
    let (scope, context, _, _, _) = setup();
    let adapter = BulkDonorPort::new(
        Arc::new(PrivateState),
        plane,
        owned.clone(),
        scope,
        policy(),
        limits(),
    )
    .unwrap();
    let mut resources = TransferResources::default();
    let result = adapter
        .execute(
            &context,
            TransferEffect::FetchOffer {
                op: op(32),
                max_metadata_bytes: 512,
            },
            &mut resources,
            &foreign,
            None,
            Instant::now() + Duration::from_secs(1),
        )
        .await;
    assert!(result.is_err());
    assert_eq!(owned.usage().0, 0);
    assert_eq!(foreign.usage().0, 0);
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one real transport schedule follows exact reservation and attachment ownership"
)]
async fn mapped_network_phases_keep_exact_resources_and_charges() {
    let net = MemBulkNet::new();
    let donor_plane = DataPlane::new(net.endpoint(NodeId::from("donor")));
    let follower_plane = DataPlane::new(net.endpoint(NodeId::from("follower")));
    let worker_budget = admission();
    let donor_budget = admission();
    let (scope, context, capture, reservation, attachment) = setup();
    let cut = JournalCursor {
        capture: capture.clone(),
        position: 0,
    };
    let offer = TransferOffer {
        capture: capture.clone(),
        image_cut: cut.clone(),
        schema: 1,
        encoded_bytes: 1,
        decoded_bytes: 1,
        chunks: 1,
        commitment: [9; 32],
        members: vec![
            member(context.donor.clone()),
            member(context.follower.clone()),
        ],
        cuts: vec![],
    };
    let (sender, mut inbox) = DonorInbox::new(5).unwrap();
    inbox.set_identity(Some(context.donor.clone()));
    let listener =
        BootstrapBulkListener::new(donor_plane, sender, donor_budget.clone(), limits()).unwrap();
    let (stop, stopped) = watch::channel(false);
    let listening = tokio::spawn(listener.run(stopped));
    let donor_reservation = reservation.clone();
    let donor_attachment = attachment.clone();
    let donor_worker = tokio::spawn(async move {
        for phase in 0..5 {
            let incoming = inbox.recv().await.unwrap();
            let reply = match (phase, incoming.request()) {
                (0, DonorRequest::Offer { .. }) => DonorReply::Offer(
                    donor_budget
                        .reserve(AdmissionClass::Inflight, 512)
                        .unwrap()
                        .hold(offer.clone()),
                ),
                (1, DonorRequest::Reserve { .. }) => DonorReply::Reserved(
                    donor_budget
                        .reserve(AdmissionClass::Inflight, 512)
                        .unwrap()
                        .hold(donor_reservation.clone()),
                ),
                (2, DonorRequest::Chunk { sequence: 0, .. }) => DonorReply::Chunk(
                    donor_budget
                        .reserve(AdmissionClass::Inflight, 8)
                        .unwrap()
                        .hold(vec![9]),
                ),
                (3, DonorRequest::Attach { .. }) => DonorReply::Attached(
                    donor_budget
                        .reserve(AdmissionClass::Inflight, 512)
                        .unwrap()
                        .hold(donor_attachment.clone()),
                ),
                (4, DonorRequest::Release { .. }) => DonorReply::Released,
                _ => panic!("unexpected donor phase"),
            };
            incoming.respond(Ok(reply));
        }
    });
    let adapter = BulkDonorPort::new(
        Arc::new(PrivateState),
        follower_plane,
        worker_budget.clone(),
        scope,
        policy(),
        limits(),
    )
    .unwrap();
    let mut resources = TransferResources::default();
    let due = Instant::now() + Duration::from_secs(3);
    let offer_event = adapter
        .execute(
            &context,
            TransferEffect::FetchOffer {
                op: op(32),
                max_metadata_bytes: 512,
            },
            &mut resources,
            &worker_budget,
            None,
            due,
        )
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(offer_event.get(), TransferEvent::Offered { .. }));
    drop(offer_event);
    let reserve_event = adapter
        .execute(
            &context,
            TransferEffect::ReserveDonor {
                op: op(33),
                capture,
                cut,
            },
            &mut resources,
            &worker_budget,
            None,
            due,
        )
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        reserve_event.get(),
        TransferEvent::DonorReserved { .. }
    ));
    drop(reserve_event);
    let chunk_event = adapter
        .execute(
            &context,
            TransferEffect::FetchChunk {
                op: op(34),
                sequence: 0,
                max_bytes: 8,
            },
            &mut resources,
            &worker_budget,
            None,
            due,
        )
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        chunk_event.get(),
        TransferEvent::ChunkStored { bytes: 1, .. }
    ));
    drop(chunk_event);
    let attach_event = adapter
        .execute(
            &context,
            TransferEffect::AttachStream {
                op: op(35),
                reservation: reservation.clone(),
            },
            &mut resources,
            &worker_budget,
            None,
            due,
        )
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        attach_event.get(),
        TransferEvent::StreamAttached { .. }
    ));
    drop(attach_event);
    assert_eq!(resources.attachment.as_ref(), Some(&attachment));
    assert!(
        adapter
            .execute(
                &context,
                TransferEffect::ReleaseReservation(reservation),
                &mut resources,
                &worker_budget,
                None,
                due
            )
            .await
            .unwrap()
            .is_none()
    );
    assert!(resources.attachment.is_none());
    donor_worker.await.unwrap();
    stop.send(true).unwrap();
    listening.await.unwrap().unwrap();
    assert_eq!(worker_budget.usage().0, 0);
}

fn journal_config_for_test() -> JournalConfig {
    JournalConfig {
        max_encoded_bytes: 8,
        max_decoded_bytes: 8,
        max_events: 4,
        max_suffix_bytes: 64,
        max_event_bytes: 8,
        max_identity_bytes: 8,
        max_followers: 1,
        max_follower_id_bytes: 32,
        max_cuts: 1,
        max_cut_bytes: 32,
        max_members: 2,
        max_membership_bytes: 64,
        max_scope_bytes: 64,
        max_batch_events: 2,
        max_batch_bytes: 8,
        max_inflight_bytes: 16,
        max_total_ms: 100,
        max_follower_ms: 50,
    }
}

fn capture_for_test(budget: &ByteAdmission) -> DonorCapture<Vec<u8>> {
    let (_, context, id, _, _) = setup();
    let config = journal_config_for_test();
    let storage = DonorJournal::storage_bound(config).unwrap();
    let mut journal = DonorJournal::new(config, id).unwrap();
    journal
        .begin_capture(
            Time(1),
            1,
            1,
            vec![member(context.donor), member(context.follower)],
            vec![],
        )
        .unwrap();
    journal.finish_capture(Time(2), 1, 1).unwrap();
    let suffix = budget.reserve(AdmissionClass::Suffix, storage).unwrap();
    let ingress = JournalIngress::new(journal, suffix, Arc::new(Notify::new())).unwrap();
    let encoded = budget.reserve(AdmissionClass::Encoded, 1).unwrap();
    let decoded = budget.reserve(AdmissionClass::Decoded, 1).unwrap();
    DonorCapture::new(vec![9], ingress, encoded, decoded).unwrap()
}

#[test]
fn capturing_ingress_records_live_effects_but_cannot_offer_until_finished() {
    let budget = admission();
    let (_, context, id, _, _) = setup();
    let config = journal_config_for_test();
    let mut journal = DonorJournal::new(config, id.clone()).unwrap();
    journal
        .begin_capture(
            Time(1),
            1,
            1,
            vec![member(context.donor), member(context.follower.clone())],
            vec![],
        )
        .unwrap();
    let suffix = budget
        .reserve(
            AdmissionClass::Suffix,
            DonorJournal::storage_bound(config).unwrap(),
        )
        .unwrap();
    let ingress = JournalIngress::new(journal, suffix, Arc::new(Notify::new())).unwrap();
    let c = JournalCursor {
        capture: id,
        position: 0,
    };
    ingress.with_journal(|journal| {
        assert!(journal.current_cursor().is_none());
        assert_eq!(
            journal.reserve(Time(2), context.follower.clone(), &c),
            Err(groupnet_core::volatile_bootstrap::journal::JournalError::Stage)
        );
        journal
            .append(
                Time(2),
                2,
                DeltaIdentity::Local(b"x".to_vec()),
                b"p".to_vec(),
            )
            .unwrap();
        assert_eq!(journal.finish_capture(Time(3), 1, 1).unwrap(), c);
    });
    let encoded = budget.reserve(AdmissionClass::Encoded, 1).unwrap();
    let decoded = budget.reserve(AdmissionClass::Decoded, 1).unwrap();
    let capture = DonorCapture::new(vec![9], ingress, encoded, decoded).unwrap();
    assert_eq!(
        capture
            .ingress()
            .with_journal(|journal| journal.current_cursor().unwrap().position),
        1
    );
    drop(capture);
    assert_eq!(budget.usage().0, 0);
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one complete journal schedule proves response-loss readback through B2 and release"
)]
fn generic_donor_journal_handles_nonempty_suffix_and_exact_ack_readback() {
    let net = MemBulkNet::new();
    let plane = DataPlane::new(net.endpoint(NodeId::from("follower")));
    let budget = admission();
    let (scope, context, id, _, _) = setup();
    let adapter = BulkDonorPort::new(
        Arc::new(PrivateState),
        plane,
        budget.clone(),
        scope,
        policy(),
        limits(),
    )
    .unwrap();
    let capture = capture_for_test(&budget);
    let cut = JournalCursor {
        capture: id,
        position: 0,
    };
    let offer = adapter
        .prepare_follower(
            &DonorRequest::Offer {
                max_metadata_bytes: 512,
            },
            &capture,
            Time(3),
            &budget,
        )
        .unwrap();
    assert!(matches!(offer, DonorReply::Offer(_)));
    drop(offer);
    let reserved = adapter
        .prepare_follower(
            &DonorRequest::Reserve {
                follower: context.follower.clone(),
                cut: cut.clone(),
            },
            &capture,
            Time(3),
            &budget,
        )
        .unwrap();
    let DonorReply::Reserved(reserved) = reserved else {
        panic!("reservation")
    };
    let reservation = reserved.get().clone();
    drop(reserved);
    let reread = adapter
        .prepare_follower(
            &DonorRequest::Reserve {
                follower: context.follower,
                cut,
            },
            &capture,
            Time(4),
            &budget,
        )
        .unwrap();
    assert!(matches!(reread, DonorReply::Reserved(ref id) if id.get() == &reservation));
    drop(reread);
    let chunk = adapter
        .prepare_follower(
            &DonorRequest::Chunk {
                reservation: reservation.clone(),
                sequence: 0,
                max_bytes: 8,
            },
            &capture,
            Time(4),
            &budget,
        )
        .unwrap();
    assert!(matches!(chunk, DonorReply::Chunk(ref bytes) if bytes.get() == &[9]));
    drop(chunk);
    let attached = adapter
        .prepare_follower(
            &DonorRequest::Attach {
                reservation: reservation.clone(),
            },
            &capture,
            Time(4),
            &budget,
        )
        .unwrap();
    let DonorReply::Attached(attached) = attached else {
        panic!("attachment")
    };
    let token = attached.get().clone();
    drop(attached);
    capture.ingress().with_journal(|journal| {
        journal
            .append(
                Time(5),
                2,
                DeltaIdentity::Local(b"repair".to_vec()),
                vec![7],
            )
            .unwrap();
    });
    let barrier = adapter
        .prepare_follower(
            &DonorRequest::Barrier {
                attachment: token,
                max_metadata_bytes: 512,
            },
            &capture,
            Time(5),
            &budget,
        )
        .unwrap();
    let DonorReply::Barrier(barrier) = barrier else {
        panic!("barrier")
    };
    let receipt = barrier.get().clone();
    assert_eq!(receipt.cursor.position, 1);
    drop(barrier);
    let batch = adapter
        .prepare_follower(
            &DonorRequest::Batch {
                barrier: receipt.clone(),
                max_bytes: 8,
                max_events: 2,
            },
            &capture,
            Time(5),
            &budget,
        )
        .unwrap();
    let DonorReply::Batch(batch) = batch else {
        panic!("batch")
    };
    assert_eq!(batch.get().deltas.len(), 1);
    let duplicate = adapter
        .prepare_follower(
            &DonorRequest::Batch {
                barrier: receipt.clone(),
                max_bytes: 8,
                max_events: 2,
            },
            &capture,
            Time(5),
            &budget,
        )
        .unwrap();
    assert!(matches!(duplicate, DonorReply::Batch(ref same) if same.get() == batch.get()));
    drop(duplicate);
    let operation = batch.get().operation;
    let through = batch.get().through.clone();
    drop(batch);
    let before_ack = capture
        .ingress()
        .with_journal(|journal| journal.acknowledged(Time(6), &reservation).unwrap());
    let held = budget
        .reserve(AdmissionClass::Inflight, 100_000 - budget.usage().0)
        .unwrap();
    assert!(
        adapter
            .prepare_follower(
                &DonorRequest::Ack {
                    reservation: reservation.clone(),
                    batch_operation: operation,
                    through: through.clone(),
                },
                &capture,
                Time(6),
                &budget,
            )
            .is_err()
    );
    assert_eq!(
        capture
            .ingress()
            .with_journal(|journal| journal.acknowledged(Time(6), &reservation).unwrap()),
        before_ack,
        "failed transient admission cannot ack the source"
    );
    drop(held);
    for _ in 0..2 {
        assert!(matches!(
            adapter.prepare_follower(
                &DonorRequest::Ack {
                    reservation: reservation.clone(),
                    batch_operation: operation,
                    through: through.clone(),
                },
                &capture,
                Time(6),
                &budget
            ),
            Ok(DonorReply::Acked)
        ));
    }
    assert!(
        adapter
            .prepare_follower(
                &DonorRequest::Ack {
                    reservation: reservation.clone(),
                    batch_operation: operation + 1,
                    through: through.clone(),
                },
                &capture,
                Time(6),
                &budget
            )
            .is_err()
    );
    capture.ingress().with_journal(|journal| {
        journal
            .append(Time(7), 2, DeltaIdentity::Local(b"new".to_vec()), vec![8])
            .unwrap();
    });
    let advanced = adapter
        .prepare_follower(
            &DonorRequest::AdvanceBarrier {
                expected: receipt,
                max_metadata_bytes: 512,
            },
            &capture,
            Time(7),
            &budget,
        )
        .unwrap();
    let DonorReply::Barrier(advanced) = advanced else {
        panic!("advanced barrier")
    };
    let second = advanced.get().clone();
    assert_eq!(second.cursor.position, 2);
    drop(advanced);
    let next_batch = adapter
        .prepare_follower(
            &DonorRequest::Batch {
                barrier: second,
                max_bytes: 8,
                max_events: 2,
            },
            &capture,
            Time(7),
            &budget,
        )
        .unwrap();
    let DonorReply::Batch(next_batch) = next_batch else {
        panic!("second batch")
    };
    assert_eq!(next_batch.get().deltas.len(), 1);
    let next_operation = next_batch.get().operation;
    let next_through = next_batch.get().through.clone();
    drop(next_batch);
    assert!(matches!(
        adapter.prepare_follower(
            &DonorRequest::Ack {
                reservation: reservation.clone(),
                batch_operation: next_operation,
                through: next_through,
            },
            &capture,
            Time(8),
            &budget
        ),
        Ok(DonorReply::Acked)
    ));
    assert!(matches!(
        adapter.prepare_follower(
            &DonorRequest::Release { reservation },
            &capture,
            Time(8),
            &budget
        ),
        Ok(DonorReply::Released)
    ));
    drop(capture);
    assert_eq!(budget.usage().0, 0);
}
