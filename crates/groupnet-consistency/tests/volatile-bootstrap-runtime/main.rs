//! In-memory opt-in builder integration for the one recovery worker.
#![cfg(feature = "volatile-recovery")]
#![expect(
    clippy::unused_async_trait_impl,
    reason = "in-memory port fakes answer some asynchronous calls synchronously"
)]

use std::future::ready;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use groupnet_consistency::QueueCapacity;
use groupnet_consistency::volatile_recovery::bootstrap::admission::{
    AdmissionClass, AdmissionLimits, Admitted, ByteAdmission,
};
use groupnet_consistency::volatile_recovery::bootstrap::ports::{
    BootstrapCapabilities, ClaimObservationLimits, ClaimSnapshot, ClaimSource, DonorCapture,
    DonorPort, DonorReply, DonorRequest, EffectContext, JournalIngress, LocalCaptureOutcome,
    LocalCaptureRequest, ParticipationSnapshot, ReadyCaptureRequest, TimedClaim, TimedParticipant,
    TransferResources,
};
use groupnet_consistency::volatile_recovery::bootstrap::session::{
    BootstrapRuntimeConfig, BootstrapSession,
};
use groupnet_consistency::volatile_recovery::bootstrap::{
    AttachToken, BootId, BootstrapClaim, BootstrapConfig, BootstrapMember, BootstrapMemberIdentity,
    BootstrapOperation, BootstrapPresence, BootstrapScope, CaptureId, CapturePlan, ClaimIdentity,
    ClaimPhase, DonorJournal, ImageSize, JournalConfig, NativeHandoffReceipt, PresenceIdentity,
    TransferConfig, TransferOffer,
};
use groupnet_consistency::volatile_recovery::{
    AdapterError, AdapterErrorKind, Mark, Peer, PeerObservation, PublicationPermit,
    RecoveryAdapter, RecoveryConfig, RecoveryFallback, RecoveryHandle, RecoveryMode,
    RecoveryOperation, RecoveryRearm, RecoveryStage,
};
use groupnet_core::volatile_bootstrap::transfer::{
    NativeCoverageReceipt, TransferEffect, TransferEvent,
};
use groupnet_core::{NodeId, Status, Time};
use groupnet_testkit::cluster::eventually_within;
use tokio::sync::Notify;

const SETTLE: Duration = Duration::from_secs(2);

fn nz(bytes: usize) -> NonZeroUsize {
    NonZeroUsize::new(bytes).expect("a nonzero test size")
}

/// A one-byte encoded and decoded test image.
const UNIT_IMAGE: ImageSize = ImageSize {
    encoded_bytes: NonZeroUsize::MIN,
    decoded_bytes: NonZeroUsize::MIN,
};

/// A capture of the unit image over `members`, with no native writers.
fn unit_capture(members: Vec<BootstrapMemberIdentity>) -> CapturePlan {
    CapturePlan {
        image: UNIT_IMAGE,
        members,
        cuts: Vec::new(),
    }
}
fn member_from_claim(claim: &ClaimIdentity) -> BootstrapMemberIdentity {
    BootstrapMemberIdentity {
        node: claim.node.clone(),
        presence: Some(PresenceIdentity {
            node: claim.node.clone(),
            boot: claim.incarnation,
            session: claim.session,
        }),
        member_incarnation: 1,
        status: Status::Alive,
    }
}

#[derive(Debug, Default)]
struct Claims {
    local: Mutex<Option<BootstrapClaim>>,
    presence: Mutex<Option<BootstrapPresence>>,
    /// The remote node's claim, which a scenario may advance or end.
    peer: Mutex<Option<BootstrapClaim>>,
    joiner: Mutex<Option<(BootstrapMemberIdentity, bool)>>,
    pause_ready_renewal: std::sync::atomic::AtomicBool,
    ready_publishes: AtomicUsize,
    renewal_started: Notify,
    resume_renewal: Notify,
    /// Every claim publication takes this long, so a claim renewal is always
    /// due again by the worker's next engine tick.
    claim_publish_delay_ms: std::sync::atomic::AtomicU64,
}

impl ClaimSource for Claims {
    async fn observe_participation(
        &self,
        _op: BootstrapOperation,
        limits: ClaimObservationLimits,
        admission: &ByteAdmission,
    ) -> Result<Admitted<ParticipationSnapshot>, AdapterError> {
        let charge = admission.reserve(AdmissionClass::Inflight, limits.max_metadata_bytes)?;
        let presence = self
            .presence
            .lock()
            .unwrap()
            .clone()
            .ok_or(AdapterErrorKind::Unavailable)?;
        let claim = self.local.lock().unwrap().clone();
        let mut members = vec![BootstrapMember {
            node: presence.identity.node.clone(),
            eligible: true,
        }];
        let mut participants = vec![TimedParticipant {
            member: BootstrapMemberIdentity {
                node: presence.identity.node.clone(),
                presence: Some(presence.identity),
                member_incarnation: 1,
                status: Status::Alive,
            },
            renewal: presence.renewal,
            remaining_ms: presence.remaining_ms,
        }];
        let mut claims = claim.into_iter().collect::<Vec<_>>();
        if let Some(peer) = self.peer.lock().unwrap().clone() {
            members.push(BootstrapMember {
                node: peer.identity.node.clone(),
                eligible: true,
            });
            participants.push(TimedParticipant {
                member: BootstrapMemberIdentity {
                    node: peer.identity.node.clone(),
                    presence: Some(PresenceIdentity {
                        node: peer.identity.node.clone(),
                        boot: peer.identity.incarnation,
                        session: peer.identity.session,
                    }),
                    member_incarnation: 1,
                    status: Status::Alive,
                },
                // The peer renews its presence whenever it renews its claim.
                renewal: peer.renewal,
                remaining_ms: peer.remaining_ms,
            });
            claims.push(peer);
        }
        if let Some((member, present)) = self.joiner.lock().unwrap().clone() {
            members.push(BootstrapMember {
                node: member.node.clone(),
                eligible: true,
            });
            if present {
                participants.push(TimedParticipant {
                    member,
                    renewal: 1,
                    remaining_ms: 500,
                });
            }
        }
        let mut roster = participants
            .iter()
            .map(|participant| participant.member.clone())
            .collect::<Vec<_>>();
        roster.sort_by(|a, b| a.node.cmp(&b.node));
        Ok(charge.hold(ParticipationSnapshot {
            sampled_at: Instant::now(),
            roster,
            members,
            participants,
            claims,
        }))
    }

    fn publish_presence(
        &self,
        presence: BootstrapPresence,
    ) -> impl Future<Output = Result<(), AdapterError>> + Send {
        *self.presence.lock().unwrap() = Some(presence);
        ready(Ok(()))
    }

    fn withdraw_presence(
        &self,
        identity: PresenceIdentity,
    ) -> impl Future<Output = Result<(), AdapterError>> + Send {
        let mut presence = self.presence.lock().unwrap();
        if presence
            .as_ref()
            .is_some_and(|value| value.identity == identity)
        {
            *presence = None;
        }
        ready(Ok(()))
    }

    async fn publish_claim(&self, claim: BootstrapClaim) -> Result<(), AdapterError> {
        let delay = self.claim_publish_delay_ms.load(Ordering::SeqCst);
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        if claim.phase == ClaimPhase::Ready
            && self.ready_publishes.fetch_add(1, Ordering::SeqCst) > 0
            && self.pause_ready_renewal.load(Ordering::SeqCst)
        {
            self.renewal_started.notify_one();
            self.resume_renewal.notified().await;
        }
        *self.local.lock().unwrap() = Some(claim);
        Ok(())
    }

    fn withdraw_claim(
        &self,
        selected: ClaimIdentity,
    ) -> impl Future<Output = Result<(), AdapterError>> + Send {
        let mut claim = self.local.lock().unwrap();
        if claim
            .as_ref()
            .is_some_and(|claim| claim.identity == selected)
        {
            *claim = None;
        }
        ready(Ok(()))
    }

    async fn observe_claims(
        &self,
        _op: BootstrapOperation,
        _limits: ClaimObservationLimits,
        admission: &ByteAdmission,
    ) -> Result<Admitted<ClaimSnapshot>, AdapterError> {
        let charge = admission.reserve(AdmissionClass::Inflight, nz(64))?;
        let claim = self
            .local
            .lock()
            .unwrap()
            .clone()
            .ok_or(AdapterErrorKind::Unavailable)?;
        let mut claims = vec![claim];
        let mut members = vec![BootstrapMember {
            node: NodeId::from("me"),
            eligible: true,
        }];
        if let Some(peer) = self.peer.lock().unwrap().clone() {
            members.push(BootstrapMember {
                node: peer.identity.node.clone(),
                eligible: true,
            });
            claims.push(peer);
        }
        Ok(charge.hold(ClaimSnapshot {
            sampled_at: Instant::now(),
            members,
            claims,
        }))
    }

    async fn observe_selected_claim(
        &self,
        _op: BootstrapOperation,
        selected: ClaimIdentity,
        _limits: ClaimObservationLimits,
        admission: &ByteAdmission,
    ) -> Result<Option<Admitted<TimedClaim>>, AdapterError> {
        let peer = self.peer.lock().unwrap().clone();
        let Some(peer) = peer.filter(|peer| peer.identity == selected) else {
            return Ok(None);
        };
        let reservation = admission.reserve(AdmissionClass::Inflight, nz(64))?;
        Ok(Some(reservation.hold(TimedClaim {
            sampled_at: Instant::now(),
            claim: peer,
        })))
    }
}

#[derive(Debug, Default)]
struct OriginDonor {
    builds: AtomicUsize,
    recaptures: AtomicUsize,
    recapture_attempts: AtomicUsize,
    allow_recapture: std::sync::atomic::AtomicBool,
    local_only: std::sync::atomic::AtomicBool,
    follower_prepares: AtomicUsize,
    pause: std::sync::atomic::AtomicBool,
    started: Notify,
    resume: Notify,
    ingress: Mutex<Option<JournalIngress>>,
    latest_permit: Mutex<Option<PublicationPermit>>,
    capture_max_ms: std::sync::atomic::AtomicU64,
    /// A Ready recapture's clone at C blocks its worker this long, as a
    /// bucket-sized clone under the publication fence does.
    recapture_block_ms: std::sync::atomic::AtomicU64,
    /// A paged local origin scan: build starts, and this many pages each
    /// published after `page_ms` with a progress report.
    build_starts: AtomicUsize,
    build_pages: AtomicUsize,
    page_ms: std::sync::atomic::AtomicU64,
}

fn journal_config() -> JournalConfig {
    JournalConfig {
        max_encoded_bytes: std::num::NonZeroUsize::new(8).unwrap(),
        max_decoded_bytes: std::num::NonZeroUsize::new(8).unwrap(),
        max_events: std::num::NonZeroUsize::new(8).unwrap(),
        max_suffix_bytes: std::num::NonZeroUsize::new(64).unwrap(),
        max_event_bytes: std::num::NonZeroUsize::new(8).unwrap(),
        max_identity_bytes: std::num::NonZeroUsize::new(4).unwrap(),
        max_followers: std::num::NonZeroUsize::new(2).unwrap(),
        max_follower_id_bytes: std::num::NonZeroUsize::new(8).unwrap(),
        max_cuts: std::num::NonZeroUsize::new(1).unwrap(),
        max_cut_bytes: std::num::NonZeroUsize::new(8).unwrap(),
        max_members: std::num::NonZeroUsize::new(1).unwrap(),
        max_membership_bytes: std::num::NonZeroUsize::new(8).unwrap(),
        max_scope_bytes: std::num::NonZeroUsize::new(8).unwrap(),
        max_batch_events: std::num::NonZeroUsize::new(2).unwrap(),
        max_batch_bytes: std::num::NonZeroUsize::new(8).unwrap(),
        max_inflight_bytes: std::num::NonZeroUsize::new(8).unwrap(),
        max_total_ms: 1_000,
        max_follower_ms: 500,
    }
}

/// A refused journal or bound step, kept as a fenced adapter failure.
fn fenced<E>(_: E) -> AdapterError {
    AdapterErrorKind::Fenced.into()
}

impl DonorPort for OriginDonor {
    type Image = Vec<u8>;
    type Stage = Vec<u8>;
    type Attachment = ();
    type NativeBuffer = Vec<u8>;

    fn retire_local_capture(&self, capture: &DonorCapture<Self::Image>) {
        let mut current = self.ingress.lock().unwrap();
        if current
            .as_ref()
            .is_some_and(|ingress| ingress.same_candidate(capture.ingress()))
        {
            *current = None;
        }
    }

    async fn build_local_capture(
        &self,
        request: LocalCaptureRequest,
        admission: &ByteAdmission,
    ) -> Result<LocalCaptureOutcome<Self::Image>, AdapterError> {
        let LocalCaptureRequest {
            recovery,
            selected,
            permit,
            clock,
            wake,
            ..
        } = request;
        let encoded = admission.reserve(AdmissionClass::Encoded, nz(1))?;
        let decoded = admission.reserve(AdmissionClass::Decoded, nz(1))?;
        let mut config = journal_config();
        let capture_max_ms = self.capture_max_ms.load(Ordering::SeqCst);
        if capture_max_ms > 0 {
            config.max_total_ms = capture_max_ms;
            config.max_follower_ms = config.max_follower_ms.min(capture_max_ms);
        }
        let storage = DonorJournal::storage_bound(config).map_err(fenced)?;
        let suffix = admission.reserve(AdmissionClass::Suffix, storage)?;
        let mut journal = DonorJournal::new(
            config,
            CaptureId {
                scope: BootstrapScope {
                    domain: "o".into(),
                    partition: "b".into(),
                },
                donor: selected.clone(),
                recovery_generation: recovery.generation,
                serial: 1,
            },
        )
        .map_err(fenced)?;
        journal
            .begin_capture(
                clock.now(),
                unit_capture(vec![member_from_claim(&selected)]),
            )
            .map_err(fenced)?;
        self.build_starts.fetch_add(1, Ordering::SeqCst);
        let page = Duration::from_millis(self.page_ms.load(Ordering::SeqCst));
        for _ in 0..self.build_pages.load(Ordering::SeqCst) {
            tokio::time::sleep(page).await;
            permit.publish(|| ()).ok_or(AdapterErrorKind::Fenced)?;
            permit.progress();
        }
        if self.pause.load(Ordering::SeqCst) {
            *self.latest_permit.lock().unwrap() = Some(permit.clone());
            self.started.notify_one();
            self.resume.notified().await;
        }
        permit
            .publish(|| self.builds.fetch_add(1, Ordering::SeqCst))
            .ok_or(AdapterErrorKind::Fenced)?;
        if self.local_only.load(Ordering::SeqCst) {
            return Ok(LocalCaptureOutcome::LocalOnly);
        }
        journal
            .finish_capture(clock.now(), UNIT_IMAGE)
            .map_err(fenced)?;
        let ingress = JournalIngress::new(journal, suffix, wake)?;
        *self.ingress.lock().unwrap() = Some(ingress.clone());
        DonorCapture::new(vec![42], ingress, encoded, decoded).map(LocalCaptureOutcome::Ready)
    }

    async fn recapture_current_index(
        &self,
        request: ReadyCaptureRequest,
        admission: &ByteAdmission,
    ) -> Result<DonorCapture<Self::Image>, AdapterError> {
        self.recapture_attempts.fetch_add(1, Ordering::SeqCst);
        if !self.allow_recapture.load(Ordering::SeqCst) {
            return Err(AdapterErrorKind::Unsupported.into());
        }
        let encoded = admission.reserve(AdmissionClass::Encoded, nz(1))?;
        let decoded = admission.reserve(AdmissionClass::Decoded, nz(1))?;
        let config = JournalConfig {
            max_members: std::num::NonZeroUsize::new(2).expect("nonzero"),
            max_membership_bytes: std::num::NonZeroUsize::new(128).expect("nonzero"),
            ..journal_config()
        };
        let suffix = admission.reserve(
            AdmissionClass::Suffix,
            DonorJournal::storage_bound(config).map_err(fenced)?,
        )?;
        let mut journal = request
            .guard
            .capture(|generation| {
                if generation != request.recovery_generation {
                    return Err(AdapterError::new(AdapterErrorKind::Fenced));
                }
                let mut journal = DonorJournal::new(
                    config,
                    CaptureId {
                        scope: BootstrapScope {
                            domain: "o".into(),
                            partition: "b".into(),
                        },
                        donor: request.selected.clone(),
                        recovery_generation: generation,
                        serial: self.recaptures.fetch_add(1, Ordering::SeqCst) as u64 + 2,
                    },
                )
                .map_err(fenced)?;
                let block = self.recapture_block_ms.load(Ordering::SeqCst);
                if block > 0 {
                    std::thread::sleep(Duration::from_millis(block));
                }
                journal
                    .begin_capture(request.clock.now(), unit_capture(request.members))
                    .map_err(fenced)?;
                Ok(journal)
            })
            .ok_or(AdapterErrorKind::Fenced)??;
        journal
            .finish_capture(request.clock.now(), UNIT_IMAGE)
            .map_err(fenced)?;
        let ingress = JournalIngress::new(journal, suffix, request.wake)?;
        *self.ingress.lock().unwrap() = Some(ingress.clone());
        DonorCapture::new(vec![42], ingress, encoded, decoded)
    }

    fn prepare_follower(
        &self,
        _request: &DonorRequest,
        _capture: &DonorCapture<Self::Image>,
        _now: Time,
        _admission: &ByteAdmission,
    ) -> Result<DonorReply, AdapterError> {
        self.follower_prepares.fetch_add(1, Ordering::SeqCst);
        Err(AdapterErrorKind::Unsupported.into())
    }

    fn execute(
        &self,
        _effect: TransferEffect,
        _resources: &mut TransferResources<Self::Stage, Self::Attachment, Self::NativeBuffer>,
        _context: EffectContext<'_>,
    ) -> impl Future<Output = Result<Option<Admitted<TransferEvent>>, AdapterError>> + Send {
        ready(Err(AdapterErrorKind::Unsupported.into()))
    }

    fn detach(
        &self,
        _token: AttachToken,
        _resources: &mut TransferResources<Self::Stage, Self::Attachment, Self::NativeBuffer>,
    ) -> impl Future<Output = Result<(), AdapterError>> + Send {
        ready(Ok(()))
    }
}

#[derive(Debug, Default)]
struct ReadAdapter {
    old_origin_builds: AtomicUsize,
    lapse_sequences: AtomicUsize,
    lapse_hold: AtomicBool,
    peer: Option<ClaimIdentity>,
    latest_origin_permit: Mutex<Option<PublicationPermit>>,
    /// Every recovery fallback the worker reported, in order.
    fallbacks: Mutex<Vec<(RecoveryStage, RecoveryFallback)>>,
}

impl RecoveryAdapter for ReadAdapter {
    fn revoke_serving(&self) {}

    fn invalidate(
        &self,
        _op: RecoveryOperation,
        _distrust_bodies: bool,
        _permit: PublicationPermit,
    ) -> impl Future<Output = Result<(), AdapterError>> + Send {
        ready(Ok(()))
    }

    fn rebuild_origin(
        &self,
        _op: RecoveryOperation,
        permit: PublicationPermit,
    ) -> impl Future<Output = Result<(), AdapterError>> + Send {
        *self.latest_origin_permit.lock().unwrap() = Some(permit);
        self.old_origin_builds.fetch_add(1, Ordering::SeqCst);
        ready(Ok(()))
    }

    async fn observe_peers(
        &self,
        _op: RecoveryOperation,
        _limits: RecoveryConfig,
    ) -> PeerObservation {
        if self.lapse_sequences.load(Ordering::SeqCst) == 0 {
            // Post-handoff check only: the bootstrap child supplies the
            // member identities, this adapter just the quiet peer's head.
            let peer = self.peer.as_ref().ok_or(AdapterErrorKind::Unavailable)?;
            return Ok((
                vec![Peer {
                    node: peer.node.clone(),
                    alive: true,
                    grants_lease: false,
                    old_nonlive: false,
                    grant: None,
                    head: None,
                    renewal: None,
                    sealed: None,
                }],
                None,
            ));
        }
        let sequence = if self.lapse_hold.load(Ordering::SeqCst) {
            1
        } else {
            self.lapse_sequences.fetch_add(1, Ordering::SeqCst)
        };
        let mark = Mark {
            epoch: 1,
            sequence: u64::try_from(sequence)
                .map_err(|error| AdapterError::with_source(AdapterErrorKind::Capacity, error))?,
        };
        Ok((
            vec![Peer {
                node: NodeId::from("peer"),
                alive: true,
                grants_lease: true,
                old_nonlive: false,
                grant: Some(mark),
                head: Some(Mark {
                    epoch: 1,
                    sequence: 1,
                }),
                renewal: None,
                sealed: None,
            }],
            Some(mark),
        ))
    }

    fn wait_frontiers(
        &self,
        _op: RecoveryOperation,
        _heads: Vec<(NodeId, Mark)>,
    ) -> impl Future<Output = Result<(), AdapterError>> + Send {
        ready(Ok(()))
    }

    fn affirm(&self, _op: RecoveryOperation) -> bool {
        true
    }

    fn operation_failed(&self, _stage: RecoveryStage, _error: AdapterError) {}

    fn fell_back(&self, from: RecoveryStage, reason: RecoveryFallback) {
        self.fallbacks.lock().unwrap().push((from, reason));
    }
}

fn bootstrap_config() -> BootstrapRuntimeConfig {
    BootstrapRuntimeConfig {
        claim: BootstrapConfig {
            max_members: std::num::NonZeroUsize::new(2).unwrap(),
            max_member_bytes: std::num::NonZeroUsize::new(8).unwrap(),
            max_scope_bytes: std::num::NonZeroUsize::new(8).unwrap(),
            settle_ms: 3,
            renew_ms: 100,
            claim_ttl_ms: 500,
            observe_ms: 100,
            donor_wait_ms: 500,
            total_ms: 800,
        },
        transfer: TransferConfig {
            expected_schema: std::num::NonZeroU32::new(1).unwrap(),
            max_metadata_bytes: std::num::NonZeroUsize::new(128).unwrap(),
            max_encoded_bytes: std::num::NonZeroUsize::new(8).unwrap(),
            max_decoded_bytes: std::num::NonZeroUsize::new(8).unwrap(),
            max_chunk_bytes: std::num::NonZeroUsize::new(8).unwrap(),
            max_chunks: std::num::NonZeroUsize::new(2).unwrap(),
            max_batch_bytes: std::num::NonZeroUsize::new(8).unwrap(),
            max_batch_events: std::num::NonZeroUsize::new(2).unwrap(),
            max_replay_events: std::num::NonZeroUsize::new(8).unwrap(),
            max_native_buffer_bytes: std::num::NonZeroUsize::new(8).unwrap(),
            max_members: std::num::NonZeroUsize::new(2).unwrap(),
            max_cuts: std::num::NonZeroUsize::new(1).unwrap(),
            coverage_poll_ms: 5,
        },
        max_claim_metadata_bytes: std::num::NonZeroUsize::new(128).unwrap(),
        donor_inbox_capacity: QueueCapacity::of(2),
        require_participation: false,
    }
}

fn admission() -> ByteAdmission {
    ByteAdmission::new(AdmissionLimits {
        max_total_bytes: std::num::NonZeroUsize::new(100_000).unwrap(),
        max_encoded_bytes: 1_024,
        max_decoded_bytes: 1_024,
        max_suffix_bytes: 50_000,
        max_native_overlap_bytes: 1_024,
        max_inflight_bytes: 8_192,
        max_reservations: std::num::NonZeroUsize::new(32).unwrap(),
    })
    .unwrap()
}

mod scenarios;

#[cfg(feature = "volatile-bootstrap-bulk")]
mod bulk_recapture;

mod roster_maintenance;

mod scheduling;

mod slow_build;

mod peer_donor;
use peer_donor::PeerDonor;

mod origin_presence;

mod follower_progress;

mod session_open;
