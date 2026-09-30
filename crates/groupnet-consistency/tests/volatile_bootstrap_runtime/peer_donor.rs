//! A remote Ready donor serving one follower's peer transfer.

use super::*;

#[derive(Debug)]
pub(super) struct PeerDonor {
    journal: Mutex<DonorJournal>,
    offer: TransferOffer,
    pub(super) live: Mutex<Option<Vec<u8>>>,
    pub(super) installed: AtomicUsize,
    pub(super) pause_install: std::sync::atomic::AtomicBool,
    pub(super) install_started: Notify,
    pub(super) resume_install: Notify,
}

impl PeerDonor {
    pub(super) fn new(peer: &ClaimIdentity, follower: &ClaimIdentity) -> Self {
        let scope = BootstrapScope {
            domain: "o".into(),
            partition: "b".into(),
        };
        let mut journal = DonorJournal::new(
            JournalConfig {
                max_members: 2,
                max_membership_bytes: 16,
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
            .begin_capture(Time(0), 1, 1, members.clone(), Vec::new())
            .unwrap();
        let image_cut = journal.finish_capture(Time(0), 1, 1).unwrap();
        let offer = TransferOffer {
            capture: image_cut.capture.clone(),
            image_cut,
            schema: 1,
            encoded_bytes: 1,
            decoded_bytes: 1,
            chunks: 1,
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
        }
    }

    fn admitted(
        admission: &ByteAdmission,
        event: TransferEvent,
    ) -> Result<
        Option<
            groupnet_consistency::volatile_recovery::bootstrap::admission::Admitted<TransferEvent>,
        >,
        AdapterError,
    > {
        let charge = admission
            .reserve(AdmissionClass::Inflight, 256)
            .map_err(|_| AdapterError)?;
        Ok(Some(charge.hold(event)))
    }
}

impl DonorPort for PeerDonor {
    type Image = Vec<u8>;
    type Stage = Vec<u8>;
    type Attachment = groupnet_core::volatile_bootstrap::journal::AttachToken;
    type NativeBuffer = Vec<u8>;

    fn build_local_capture<'a>(
        &'a self,
        _request: LocalCaptureRequest,
        _admission: &'a ByteAdmission,
    ) -> BoxRecoveryFuture<'a, Result<LocalCaptureOutcome<Self::Image>, AdapterError>> {
        Box::pin(async { Err(AdapterError) })
    }

    fn retire_local_capture(&self, _capture: &DonorCapture<Self::Image>) {}

    fn prepare_follower(
        &self,
        _request: &DonorRequest,
        _capture: &DonorCapture<Self::Image>,
        _now: Time,
        _admission: &ByteAdmission,
    ) -> Result<DonorReply, AdapterError> {
        Err(AdapterError)
    }

    #[expect(
        clippy::too_many_lines,
        reason = "the in-memory port maps each transfer phase to one exact test event"
    )]
    fn execute<'a>(
        &'a self,
        context: &'a TransferContext,
        effect: TransferEffect,
        resources: &'a mut TransferResources<Self::Stage, Self::Attachment, Self::NativeBuffer>,
        admission: &'a ByteAdmission,
        permit: Option<PublicationPermit>,
        _deadline: Instant,
    ) -> BoxRecoveryFuture<
        'a,
        Result<
            Option<
                groupnet_consistency::volatile_recovery::bootstrap::admission::Admitted<
                    TransferEvent,
                >,
            >,
            AdapterError,
        >,
    > {
        Box::pin(async move {
            match effect {
                TransferEffect::FetchOffer { op, .. } => {
                    let charge = admission
                        .reserve(AdmissionClass::Inflight, 256)
                        .map_err(|_| AdapterError)?;
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
                    let encoded = admission
                        .reserve(AdmissionClass::Encoded, encoded_bytes)
                        .map_err(|_| AdapterError)?;
                    let decoded = admission
                        .reserve(AdmissionClass::Decoded, decoded_bytes)
                        .map_err(|_| AdapterError)?;
                    resources.stage = Some(groupnet_consistency::volatile_recovery::bootstrap::ports::StageResources::new(Vec::new(), encoded, decoded)?);
                    Self::admitted(admission, TransferEvent::StageReserved { op })
                }
                TransferEffect::ReserveDonor { op, capture, cut } => {
                    if capture != self.offer.capture || cut != self.offer.image_cut {
                        return Err(AdapterError);
                    }
                    let reservation = self
                        .journal
                        .lock()
                        .unwrap()
                        .reserve(Time(1), context.follower.clone(), &cut)
                        .map_err(|_| AdapterError)?;
                    Self::admitted(admission, TransferEvent::DonorReserved { op, reservation })
                }
                TransferEffect::FetchChunk { op, sequence, .. } => {
                    if sequence != 0 {
                        return Err(AdapterError);
                    }
                    let stage = resources.stage.as_mut().ok_or(AdapterError)?;
                    stage.stage_mut().push(42);
                    Self::admitted(
                        admission,
                        TransferEvent::ChunkStored {
                            op,
                            sequence,
                            bytes: 1,
                            decoded_charge: 1,
                        },
                    )
                }
                TransferEffect::VerifyImage { op, commitment } => {
                    if commitment != [9; 32]
                        || resources
                            .stage
                            .as_mut()
                            .is_none_or(|stage| stage.stage_mut().as_slice() != [42])
                    {
                        return Err(AdapterError);
                    }
                    Self::admitted(admission, TransferEvent::ImageVerified { op, commitment })
                }
                TransferEffect::AttachStream { op, reservation } => {
                    let mut journal = self.journal.lock().unwrap();
                    let token = journal
                        .begin_attach(Time(1), &reservation)
                        .map_err(|_| AdapterError)?;
                    journal
                        .confirm_attach(Time(1), &token)
                        .map_err(|_| AdapterError)?;
                    resources.attachment = Some(token.clone());
                    Self::admitted(admission, TransferEvent::StreamAttached { op, token })
                }
                TransferEffect::FetchBarrier { op, reservation } => {
                    let receipt = self
                        .journal
                        .lock()
                        .unwrap()
                        .barrier(Time(1), &reservation)
                        .map_err(|_| AdapterError)?;
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
                    let permit = permit.ok_or(AdapterError)?;
                    let stage = resources.stage.take().ok_or(AdapterError)?;
                    let attachment = resources.attachment.clone().ok_or(AdapterError)?;
                    permit
                        .publish(|| {
                            stage.install(|image| *self.live.lock().unwrap() = Some(image));
                            self.installed.fetch_add(1, Ordering::SeqCst);
                        })
                        .ok_or(AdapterError)?;
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
                _ => Err(AdapterError),
            }
        })
    }

    fn detach<'a>(
        &'a self,
        _token: groupnet_core::volatile_bootstrap::journal::AttachToken,
        resources: &'a mut TransferResources<Self::Stage, Self::Attachment, Self::NativeBuffer>,
    ) -> BoxRecoveryFuture<'a, Result<(), AdapterError>> {
        Box::pin(async move {
            resources.attachment = None;
            Ok(())
        })
    }
}
