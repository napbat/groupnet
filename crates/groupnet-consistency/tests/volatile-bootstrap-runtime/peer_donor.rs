//! A remote Ready donor serving one follower's peer transfer.

use super::*;
use groupnet_consistency::volatile_recovery::bootstrap::ports::StageResources;

#[derive(Debug)]
pub(super) struct PeerDonor {
    journal: Mutex<DonorJournal>,
    offer: TransferOffer,
    pub(super) live: Mutex<Option<Vec<u8>>>,
    pub(super) installed: AtomicUsize,
    pub(super) pause_install: std::sync::atomic::AtomicBool,
    pub(super) install_started: Notify,
    pub(super) resume_install: Notify,
    /// Each chunk fetch takes this long, as on a loaded follower.
    chunk_ms: u64,
    /// Origin scans this follower started as its own builder.
    pub(super) local_builds: AtomicUsize,
}

impl PeerDonor {
    pub(super) fn new(peer: &ClaimIdentity, follower: &ClaimIdentity) -> Self {
        Self::paced(peer, follower, NonZeroUsize::MIN, 0)
    }

    /// A donor whose image is `chunks` one-byte chunks, each served after
    /// `chunk_ms`.
    pub(super) fn paced(
        peer: &ClaimIdentity,
        follower: &ClaimIdentity,
        chunks: NonZeroUsize,
        chunk_ms: u64,
    ) -> Self {
        let scope = BootstrapScope {
            domain: "o".into(),
            partition: "b".into(),
        };
        let mut journal = DonorJournal::new(
            JournalConfig {
                max_members: std::num::NonZeroUsize::new(2).unwrap(),
                max_membership_bytes: std::num::NonZeroUsize::new(16).unwrap(),
                ..journal_config()
            },
            CaptureId {
                scope,
                donor: peer.clone(),
                recovery_generation: 1,
                serial: 1,
            },
        )
        .unwrap();
        let members = vec![member_from_claim(follower), member_from_claim(peer)];
        journal
            .begin_capture(Time(0), unit_capture(members.clone()))
            .unwrap();
        let image_cut = journal.finish_capture(Time(0), UNIT_IMAGE).unwrap();
        let offer = TransferOffer {
            capture: image_cut.capture.clone(),
            image_cut,
            schema: 1,
            encoded_bytes: chunks,
            decoded_bytes: chunks,
            chunks,
            commitment: [9; 32],
            members,
            cuts: Vec::new(),
        };
        Self {
            journal: Mutex::new(journal),
            offer,
            live: Mutex::new(None),
            installed: AtomicUsize::new(0),
            pause_install: std::sync::atomic::AtomicBool::new(false),
            install_started: Notify::new(),
            resume_install: Notify::new(),
            chunk_ms,
            local_builds: AtomicUsize::new(0),
        }
    }

    fn admitted(
        admission: &ByteAdmission,
        event: TransferEvent,
    ) -> Result<Option<Admitted<TransferEvent>>, AdapterError> {
        let charge = admission.reserve(AdmissionClass::Inflight, 256)?;
        Ok(Some(charge.hold(event)))
    }
}

/// A transfer effect inconsistent with this donor's single offer.
const INVALID: AdapterError = AdapterError::new(AdapterErrorKind::InvalidInput);

impl DonorPort for PeerDonor {
    type Image = Vec<u8>;
    type Stage = Vec<u8>;
    type Attachment = AttachToken;
    type NativeBuffer = Vec<u8>;

    fn build_local_capture(
        &self,
        _request: LocalCaptureRequest,
        _admission: &ByteAdmission,
    ) -> impl Future<Output = Result<LocalCaptureOutcome<Self::Image>, AdapterError>> + Send {
        self.local_builds.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Err(AdapterErrorKind::Unsupported.into()))
    }

    fn retire_local_capture(&self, _capture: &DonorCapture<Self::Image>) {}

    fn prepare_follower(
        &self,
        _request: &DonorRequest,
        _capture: &DonorCapture<Self::Image>,
        _now: Time,
        _admission: &ByteAdmission,
    ) -> Result<DonorReply, AdapterError> {
        Err(AdapterErrorKind::Unsupported.into())
    }

    #[expect(
        clippy::too_many_lines,
        reason = "the in-memory port maps each transfer phase to one exact test event"
    )]
    async fn execute(
        &self,
        effect: TransferEffect,
        resources: &mut TransferResources<Self::Stage, Self::Attachment, Self::NativeBuffer>,
        call: EffectContext<'_>,
    ) -> Result<Option<Admitted<TransferEvent>>, AdapterError> {
        let EffectContext {
            transfer: context,
            admission,
            permit,
            ..
        } = call;
        match effect {
            TransferEffect::FetchOffer { op, .. } => {
                let charge = admission.reserve(AdmissionClass::Inflight, 256)?;
                Ok(Some(charge.hold(TransferEvent::Offered {
                    op,
                    offer: self.offer.clone(),
                })))
            }
            TransferEffect::ReserveStage {
                op,
                encoded_bytes,
                decoded_bytes,
                ..
            } => {
                let encoded = admission.reserve(AdmissionClass::Encoded, encoded_bytes.get())?;
                let decoded = admission.reserve(AdmissionClass::Decoded, decoded_bytes.get())?;
                resources.stage = Some(StageResources::new(Vec::new(), encoded, decoded)?);
                Self::admitted(admission, TransferEvent::StageReserved { op })
            }
            TransferEffect::ReserveDonor { op, capture, cut } => {
                if capture != self.offer.capture || cut != self.offer.image_cut {
                    return Err(INVALID);
                }
                let reservation = self
                    .journal
                    .lock()
                    .unwrap()
                    .reserve(Time(1), context.follower.clone(), &cut)
                    .map_err(fenced)?;
                Self::admitted(admission, TransferEvent::DonorReserved { op, reservation })
            }
            TransferEffect::FetchChunk { op, sequence, .. } => {
                if sequence >= self.offer.chunks.get() {
                    return Err(INVALID);
                }
                tokio::time::sleep(Duration::from_millis(self.chunk_ms)).await;
                let stage = resources.stage.as_mut().ok_or(AdapterErrorKind::Fenced)?;
                stage.stage_mut().push(42);
                Self::admitted(
                    admission,
                    TransferEvent::ChunkStored {
                        op,
                        sequence,
                        bytes: NonZeroUsize::MIN,
                        decoded_charge: 1,
                    },
                )
            }
            TransferEffect::VerifyImage { op, commitment } => {
                if commitment != [9; 32]
                    || resources.stage.as_mut().is_none_or(|stage| {
                        stage.stage_mut().as_slice() != vec![42; self.offer.chunks.get()].as_slice()
                    })
                {
                    return Err(AdapterErrorKind::Malformed.into());
                }
                Self::admitted(admission, TransferEvent::ImageVerified { op, commitment })
            }
            TransferEffect::AttachStream { op, reservation } => {
                let mut journal = self.journal.lock().unwrap();
                let token = journal
                    .begin_attach(Time(1), &reservation)
                    .map_err(fenced)?;
                journal.confirm_attach(Time(1), &token).map_err(fenced)?;
                resources.attachment = Some(token.clone());
                Self::admitted(admission, TransferEvent::StreamAttached { op, token })
            }
            TransferEffect::FetchBarrier { op, reservation } => {
                let receipt = self
                    .journal
                    .lock()
                    .unwrap()
                    .barrier(Time(1), &reservation)
                    .map_err(fenced)?;
                Self::admitted(admission, TransferEvent::BarrierReceived { op, receipt })
            }
            TransferEffect::CheckNativeCoverage { op, receipt, .. } => {
                let coverage = NativeCoverageReceipt {
                    parent: context.parent,
                    staged_through: receipt.cursor.clone(),
                    proven_cuts: receipt.covered_cuts.clone(),
                    members: receipt.members.clone(),
                    barrier: receipt,
                    buffered_bytes: 0,
                };
                Self::admitted(admission, TransferEvent::NativeCovered { op, coverage })
            }
            TransferEffect::InstallCandidate { op, coverage } => {
                if self.pause_install.load(Ordering::SeqCst) {
                    self.install_started.notify_one();
                    self.resume_install.notified().await;
                }
                let permit = permit.ok_or(AdapterErrorKind::Fenced)?;
                let stage = resources.stage.take().ok_or(AdapterErrorKind::Fenced)?;
                let attachment = resources
                    .attachment
                    .clone()
                    .ok_or(AdapterErrorKind::Fenced)?;
                permit
                    .publish(|| {
                        stage.install(|image| *self.live.lock().unwrap() = Some(image));
                        self.installed.fetch_add(1, Ordering::SeqCst);
                    })
                    .ok_or(AdapterErrorKind::Fenced)?;
                let handoff = NativeHandoffReceipt {
                    recovery: permit.operation(),
                    install: op,
                    attachment,
                    schema: 1,
                    applier_generation: 1,
                    continued_cuts: coverage.proven_cuts.clone(),
                    buffered_bytes: 0,
                    coverage: *coverage,
                };
                Self::admitted(
                    admission,
                    TransferEvent::Installed {
                        op,
                        handoff: Box::new(handoff),
                    },
                )
            }
            TransferEffect::ReleaseReservation(reservation) => {
                let _ = self.journal.lock().unwrap().release(Time(1), &reservation);
                resources.attachment = None;
                Ok(None)
            }
            TransferEffect::DiscardStage { .. } => {
                resources.stage = None;
                resources.native_overlap = None;
                Ok(None)
            }
            _ => Err(AdapterErrorKind::Unsupported.into()),
        }
    }

    fn detach(
        &self,
        _token: AttachToken,
        resources: &mut TransferResources<Self::Stage, Self::Attachment, Self::NativeBuffer>,
    ) -> impl Future<Output = Result<(), AdapterError>> + Send {
        resources.attachment = None;
        std::future::ready(Ok(()))
    }
}
