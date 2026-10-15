//! Real Ready-generation recapture through the production bulk port.

use super::*;
use groupnet_consistency::DataPlane;
use groupnet_consistency::volatile_recovery::bootstrap::JournalBatch;
use groupnet_consistency::volatile_recovery::bootstrap::bulk_adapter::{
    BootstrapStatePort, BulkDonorOptions, BulkDonorPort,
};
use groupnet_consistency::volatile_recovery::bootstrap::bulk_wire::{
    BulkLimits, PhaseLimits, WireLimits,
};
use groupnet_consistency::volatile_recovery::bootstrap::ports::LogicalClock;
use groupnet_transport_mem::MemBulkNet;
use std::future::ready;

#[derive(Debug, Default)]
struct RecaptureState {
    captures: AtomicUsize,
}

impl BootstrapStatePort for RecaptureState {
    type Image = Vec<u8>;
    type Stage = Vec<u8>;
    type NativeBuffer = Vec<u8>;

    fn build_local_capture(
        &self,
        _request: LocalCaptureRequest,
        _admission: &ByteAdmission,
    ) -> impl Future<Output = Result<LocalCaptureOutcome<Self::Image>, AdapterError>> + Send {
        ready(Err(AdapterErrorKind::Unsupported.into()))
    }

    fn recapture_current_index(
        &self,
        request: ReadyCaptureRequest,
        admission: &ByteAdmission,
    ) -> impl Future<Output = Result<DonorCapture<Self::Image>, AdapterError>> + Send {
        ready(self.recapture(request, admission))
    }

    fn retire_local_capture(&self, _capture: &DonorCapture<Self::Image>) {}

    fn image_offer(
        &self,
        _capture: &DonorCapture<Self::Image>,
        _max_metadata_bytes: usize,
        _admission: &ByteAdmission,
    ) -> Result<Admitted<TransferOffer>, AdapterError> {
        Err(AdapterErrorKind::Unsupported.into())
    }

    fn image_chunk(
        &self,
        _capture: &DonorCapture<Self::Image>,
        _sequence: usize,
        _max_bytes: usize,
        _admission: &ByteAdmission,
    ) -> Result<Admitted<Vec<u8>>, AdapterError> {
        Err(AdapterErrorKind::Unsupported.into())
    }

    fn execute_local(
        &self,
        _effect: TransferEffect,
        _resources: &mut TransferResources<Self::Stage, AttachToken, Self::NativeBuffer>,
        _context: EffectContext<'_>,
    ) -> impl Future<Output = Result<Option<Admitted<TransferEvent>>, AdapterError>> + Send {
        ready(Err(AdapterErrorKind::Unsupported.into()))
    }

    fn store_chunk(
        &self,
        _sequence: usize,
        _chunk: &Admitted<Vec<u8>>,
        _resources: &mut TransferResources<Self::Stage, AttachToken, Self::NativeBuffer>,
    ) -> impl Future<Output = Result<usize, AdapterError>> + Send {
        ready(Err(AdapterErrorKind::Unsupported.into()))
    }

    fn stage_batch(
        &self,
        _batch: &Admitted<JournalBatch>,
        _resources: &mut TransferResources<Self::Stage, AttachToken, Self::NativeBuffer>,
    ) -> impl Future<Output = Result<(), AdapterError>> + Send {
        ready(Err(AdapterErrorKind::Unsupported.into()))
    }

    fn detach(
        &self,
        _token: AttachToken,
        _resources: &mut TransferResources<Self::Stage, AttachToken, Self::NativeBuffer>,
    ) -> impl Future<Output = Result<(), AdapterError>> + Send {
        ready(Ok(()))
    }
}

impl RecaptureState {
    /// One synchronous Ready recapture under the fresh generation guard.
    fn recapture(
        &self,
        request: ReadyCaptureRequest,
        admission: &ByteAdmission,
    ) -> Result<DonorCapture<Vec<u8>>, AdapterError> {
        let encoded = admission.reserve(AdmissionClass::Encoded, 1)?;
        let decoded = admission.reserve(AdmissionClass::Decoded, 1)?;
        let suffix = admission.reserve(
            AdmissionClass::Suffix,
            DonorJournal::storage_bound(journal_config()).map_err(fenced)?,
        )?;
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
                .map_err(fenced)?;
                journal
                    .begin_capture(request.clock.now(), 1, 1, request.members, Vec::new())
                    .map_err(fenced)?;
                self.captures.fetch_add(1, Ordering::SeqCst);
                Ok::<_, AdapterError>(journal)
            })
            .ok_or(AdapterErrorKind::Fenced)??;
        journal
            .finish_capture(request.clock.now(), 1, 1)
            .map_err(fenced)?;
        let ingress = JournalIngress::new(journal, suffix, request.wake)?;
        DonorCapture::new(vec![42], ingress, encoded, decoded)
    }
}

#[tokio::test]
async fn bulk_port_recaptures_only_a_real_ready_generation() {
    let reads = Arc::new(ReadAdapter::default());
    let handle = RecoveryHandle::builder(Arc::clone(&reads), NodeId::from("me"), 12)
        .config(RecoveryConfig {
            max_members: std::num::NonZeroUsize::new(2).unwrap(),
            max_member_bytes: std::num::NonZeroUsize::new(8).unwrap(),
            max_barrier_rounds: std::num::NonZeroU32::new(2).unwrap(),
            total_ms: 1_000,
            attempt_ms: 500,
            settle_ms: 5,
            poll_ms: 5,
        })
        .mode(RecoveryMode::Unleased)
        .open()
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
        BulkDonorOptions {
            scope: BootstrapScope {
                domain: "o".into(),
                partition: "b".into(),
            },
            policy: bootstrap_config().transfer,
            limits: BulkLimits {
                wire: WireLimits {
                    max_frame_bytes: std::num::NonZeroUsize::new(4096).unwrap(),
                    max_scope_bytes: std::num::NonZeroUsize::new(8).unwrap(),
                    max_node_bytes: std::num::NonZeroUsize::new(8).unwrap(),
                    max_payload_bytes: std::num::NonZeroUsize::new(2048).unwrap(),
                },
                phase: PhaseLimits {
                    max_body_bytes: std::num::NonZeroUsize::new(2048).unwrap(),
                    max_scope_bytes: std::num::NonZeroUsize::new(8).unwrap(),
                    max_node_bytes: std::num::NonZeroUsize::new(8).unwrap(),
                    max_writer_bytes: std::num::NonZeroUsize::new(8).unwrap(),
                    max_cuts: std::num::NonZeroUsize::new(1).unwrap(),
                    max_members: std::num::NonZeroUsize::new(2).unwrap(),
                    max_events: std::num::NonZeroUsize::new(8).unwrap(),
                    max_identity_bytes: std::num::NonZeroUsize::new(8).unwrap(),
                    max_effect_bytes: std::num::NonZeroUsize::new(128).unwrap(),
                },
                server_request: Duration::from_secs(1),
            },
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
        clock: LogicalClock::start(),
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
