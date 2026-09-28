//! Real Ready-generation recapture through the production bulk port.

use super::*;
use groupnet_consistency::volatile_recovery::bootstrap::admission::Admitted;
use groupnet_consistency::volatile_recovery::bootstrap::bulk_adapter::{
    BootstrapStatePort, BulkDonorPort,
};
use groupnet_consistency::volatile_recovery::bootstrap::bulk_wire::{
    BulkLimits, PhaseLimits, WireLimits,
};
use groupnet_consistency::volatile_recovery::bootstrap::ports::ReadyCaptureRequest;
use groupnet_core::volatile_bootstrap::journal::{AttachToken, JournalBatch};
use groupnet_transport::bulk::DataPlane;
use groupnet_transport_mem::MemBulkNet;

#[derive(Debug, Default)]
struct RecaptureState {
    captures: AtomicUsize,
}

impl BootstrapStatePort for RecaptureState {
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

    fn recapture_current_index<'a>(
        &'a self,
        request: groupnet_consistency::volatile_recovery::bootstrap::ports::ReadyCaptureRequest,
        admission: &'a ByteAdmission,
    ) -> BoxRecoveryFuture<'a, Result<DonorCapture<Self::Image>, AdapterError>> {
        Box::pin(async move {
            let encoded = admission
                .reserve(AdmissionClass::Encoded, 1)
                .map_err(|_| AdapterError)?;
            let decoded = admission
                .reserve(AdmissionClass::Decoded, 1)
                .map_err(|_| AdapterError)?;
            let suffix = admission
                .reserve(
                    AdmissionClass::Suffix,
                    DonorJournal::storage_bound(journal_config()).map_err(|_| AdapterError)?,
                )
                .map_err(|_| AdapterError)?;
            let mut journal = request
                .guard
                .capture(|generation| {
                    assert_eq!(generation, request.recovery_generation);
                    let mut journal = DonorJournal::new(
                        journal_config(),
                        CaptureId {
                            scope: BootstrapScope {
                                domain: "o".into(),
                                partition: "b".into(),
                            },
                            donor: request.selected.clone(),
                            recovery_generation: generation,
                            serial: 2,
                        },
                    )
                    .map_err(|_| AdapterError)?;
                    journal
                        .begin_capture(request.now, 1, 1, request.members, Vec::new())
                        .map_err(|_| AdapterError)?;
                    self.captures.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, AdapterError>(journal)
                })
                .ok_or(AdapterError)??;
            journal
                .finish_capture(request.now, 1, 1)
                .map_err(|_| AdapterError)?;
            let ingress = JournalIngress::new(journal, suffix, request.wake)?;
            DonorCapture::new(vec![42], ingress, encoded, decoded)
        })
    }

    fn retire_local_capture(&self, _capture: &DonorCapture<Self::Image>) {}

    fn image_offer(
        &self,
        _capture: &DonorCapture<Self::Image>,
        _max_metadata_bytes: usize,
        _admission: &ByteAdmission,
    ) -> Result<Admitted<TransferOffer>, AdapterError> {
        Err(AdapterError)
    }

    fn image_chunk(
        &self,
        _capture: &DonorCapture<Self::Image>,
        _sequence: usize,
        _max_bytes: usize,
        _admission: &ByteAdmission,
    ) -> Result<Admitted<Vec<u8>>, AdapterError> {
        Err(AdapterError)
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
        _sequence: usize,
        _chunk: &'a Admitted<Vec<u8>>,
        _resources: &'a mut TransferResources<Self::Stage, AttachToken, Self::NativeBuffer>,
    ) -> BoxRecoveryFuture<'a, Result<usize, AdapterError>> {
        Box::pin(async { Err(AdapterError) })
    }

    fn stage_batch<'a>(
        &'a self,
        _batch: &'a Admitted<JournalBatch>,
        _resources: &'a mut TransferResources<Self::Stage, AttachToken, Self::NativeBuffer>,
    ) -> BoxRecoveryFuture<'a, Result<(), AdapterError>> {
        Box::pin(async { Err(AdapterError) })
    }

    fn detach<'a>(
        &'a self,
        _token: AttachToken,
        _resources: &'a mut TransferResources<Self::Stage, AttachToken, Self::NativeBuffer>,
    ) -> BoxRecoveryFuture<'a, Result<(), AdapterError>> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn bulk_port_recaptures_only_a_real_ready_generation() {
    let reads = Arc::new(ReadAdapter::default());
    let handle = RecoveryHandle::open(
        Arc::clone(&reads),
        RecoveryConfig {
            max_members: 2,
            max_member_bytes: 8,
            max_barrier_rounds: 2,
            total_ms: 1_000,
            attempt_ms: 500,
            settle_ms: 5,
            poll_ms: 5,
        },
        RecoveryMode::Unleased,
        NodeId::from("me"),
        12,
    )
    .unwrap();
    eventually_within("origin reaches Ready", SETTLE, || handle.status().may_serve).await;
    let original = reads.latest_origin_permit.lock().unwrap().clone().unwrap();
    let deadline = Instant::now() + Duration::from_secs(1);
    let guard = original.ready_capture(deadline).unwrap();
    let admission = admission();
    let state = Arc::new(RecaptureState::default());
    let net = MemBulkNet::new();
    let port = BulkDonorPort::new(
        Arc::clone(&state),
        DataPlane::new(net.endpoint(NodeId::from("me"))),
        admission.clone(),
        BootstrapScope {
            domain: "o".into(),
            partition: "b".into(),
        },
        bootstrap_config().transfer,
        BulkLimits {
            wire: WireLimits {
                max_frame_bytes: 4096,
                max_scope_bytes: 8,
                max_node_bytes: 8,
                max_payload_bytes: 2048,
            },
            phase: PhaseLimits {
                max_body_bytes: 2048,
                max_scope_bytes: 8,
                max_node_bytes: 8,
                max_writer_bytes: 8,
                max_cuts: 1,
                max_members: 2,
                max_events: 8,
                max_identity_bytes: 8,
                max_effect_bytes: 128,
            },
            server_request_ms: 1000,
        },
    )
    .unwrap();
    let selected = ClaimIdentity {
        node: NodeId::from("me"),
        incarnation: BootId(17),
        session: 7,
        attempt: 1,
    };
    let request = ReadyCaptureRequest {
        operation: BootstrapOperation {
            incarnation: BootId(17),
            session: 7,
            generation: 1,
            token: 5,
        },
        selected: selected.clone(),
        recovery_generation: original.operation().generation,
        members: vec![member_from_claim(&selected)],
        guard: guard.clone(),
        deadline,
        now: Time(1),
        wake: Arc::new(Notify::new()),
    };
    let capture = port
        .recapture_current_index(request.clone(), &admission)
        .await
        .unwrap();
    assert!(capture.is_active());
    assert_eq!(state.captures.load(Ordering::SeqCst), 1);
    assert_eq!(
        capture
            .ingress()
            .with_journal(|journal| journal.id().donor.clone()),
        selected
    );
    handle.cancel().unwrap();
    assert!(!guard.valid());
    assert!(
        port.recapture_current_index(request, &admission)
            .await
            .is_err()
    );
    assert_eq!(state.captures.load(Ordering::SeqCst), 1);
    drop(capture);
    assert_eq!(admission.usage().0, 0);
}
