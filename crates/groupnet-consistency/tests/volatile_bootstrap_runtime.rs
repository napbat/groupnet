//! In-memory opt-in builder integration for the one recovery worker.
#![cfg(feature = "volatile-recovery")]

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use groupnet_consistency::volatile_recovery::bootstrap::admission::{
    AdmissionClass, AdmissionLimits, ByteAdmission,
};
use groupnet_consistency::volatile_recovery::bootstrap::ports::{
    BootstrapCapabilities, ClaimObservationLimits, ClaimSnapshot, ClaimSource, DonorCapture,
    DonorPort, DonorReply, DonorRequest, JournalIngress, LocalCaptureOutcome, LocalCaptureRequest,
    ParticipationSnapshot, TimedClaim, TimedParticipant, TransferContext, TransferResources,
};
use groupnet_consistency::volatile_recovery::bootstrap::session::{
    BootstrapRuntimeConfig, BootstrapSession,
};
use groupnet_consistency::volatile_recovery::{
    AdapterError, BoxRecoveryFuture, Mark, PeerObservation, PublicationPermit, RecoveryAdapter,
    RecoveryConfig, RecoveryFallback, RecoveryHandle, RecoveryMode, RecoveryOperation,
    RecoveryRearm, RecoveryStage,
};
use groupnet_core::volatile_bootstrap::journal::{CaptureId, DonorJournal, JournalConfig};
use groupnet_core::volatile_bootstrap::transfer::{
    NativeCoverageReceipt, NativeHandoffReceipt, TransferConfig, TransferEffect, TransferEvent,
    TransferOffer,
};
use groupnet_core::volatile_bootstrap::{
    BootId, BootstrapClaim, BootstrapConfig, BootstrapMember, BootstrapMemberIdentity,
    BootstrapOperation, BootstrapPresence, BootstrapScope, ClaimIdentity, PresenceIdentity,
};
use groupnet_core::{NodeId, Status, Time};
use groupnet_testkit::cluster::eventually_within;
use tokio::sync::Notify;

const SETTLE: Duration = Duration::from_secs(2);

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
    fn observe_participation<'a>(
        &'a self,
        _op: BootstrapOperation,
        limits: ClaimObservationLimits,
        admission: &'a ByteAdmission,
    ) -> BoxRecoveryFuture<
        'a,
        Result<
            groupnet_consistency::volatile_recovery::bootstrap::admission::Admitted<
                ParticipationSnapshot,
            >,
            AdapterError,
        >,
    > {
        Box::pin(async move {
            let charge = admission
                .reserve(AdmissionClass::Inflight, limits.max_metadata_bytes)
                .map_err(|_| AdapterError)?;
            let presence = self.presence.lock().unwrap().clone().ok_or(AdapterError)?;
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
        })
    }

    fn publish_presence(
        &self,
        presence: BootstrapPresence,
    ) -> BoxRecoveryFuture<'_, Result<(), AdapterError>> {
        Box::pin(async move {
            *self.presence.lock().unwrap() = Some(presence);
            Ok(())
        })
    }

    fn withdraw_presence(
        &self,
        identity: PresenceIdentity,
    ) -> BoxRecoveryFuture<'_, Result<(), AdapterError>> {
        Box::pin(async move {
            let mut presence = self.presence.lock().unwrap();
            if presence
                .as_ref()
                .is_some_and(|value| value.identity == identity)
            {
                *presence = None;
            }
            Ok(())
        })
    }

    fn publish_claim(
        &self,
        claim: BootstrapClaim,
    ) -> BoxRecoveryFuture<'_, Result<(), AdapterError>> {
        Box::pin(async move {
            let delay = self.claim_publish_delay_ms.load(Ordering::SeqCst);
            if delay > 0 {
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
            if claim.phase == groupnet_core::volatile_bootstrap::ClaimPhase::Ready
                && self.ready_publishes.fetch_add(1, Ordering::SeqCst) > 0
                && self.pause_ready_renewal.load(Ordering::SeqCst)
            {
                self.renewal_started.notify_one();
                self.resume_renewal.notified().await;
            }
            *self.local.lock().unwrap() = Some(claim);
            Ok(())
        })
    }

    fn withdraw_claim(
        &self,
        selected: ClaimIdentity,
    ) -> BoxRecoveryFuture<'_, Result<(), AdapterError>> {
        Box::pin(async move {
            let mut claim = self.local.lock().unwrap();
            if claim
                .as_ref()
                .is_some_and(|claim| claim.identity == selected)
            {
                *claim = None;
            }
            Ok(())
        })
    }

    fn observe_claims<'a>(
        &'a self,
        _op: BootstrapOperation,
        _limits: ClaimObservationLimits,
        admission: &'a ByteAdmission,
    ) -> BoxRecoveryFuture<
        'a,
        Result<
            groupnet_consistency::volatile_recovery::bootstrap::admission::Admitted<ClaimSnapshot>,
            AdapterError,
        >,
    > {
        Box::pin(async move {
            let charge = admission
                .reserve(AdmissionClass::Inflight, 64)
                .map_err(|_| AdapterError)?;
            let claim = self.local.lock().unwrap().clone().ok_or(AdapterError)?;
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
        })
    }

    fn observe_selected_claim<'a>(
        &'a self,
        _op: BootstrapOperation,
        selected: ClaimIdentity,
        _limits: ClaimObservationLimits,
        admission: &'a ByteAdmission,
    ) -> BoxRecoveryFuture<
        'a,
        Result<
            Option<
                groupnet_consistency::volatile_recovery::bootstrap::admission::Admitted<TimedClaim>,
            >,
            AdapterError,
        >,
    > {
        Box::pin(async move {
            let peer = self.peer.lock().unwrap().clone();
            peer.filter(|peer| peer.identity == selected)
                .map(|peer| {
                    admission
                        .reserve(AdmissionClass::Inflight, 64)
                        .map(|reservation| {
                            reservation.hold(TimedClaim {
                                sampled_at: Instant::now(),
                                claim: peer,
                            })
                        })
                        .map_err(|_| AdapterError)
                })
                .transpose()
        })
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
        max_encoded_bytes: 8,
        max_decoded_bytes: 8,
        max_events: 8,
        max_suffix_bytes: 64,
        max_event_bytes: 8,
        max_identity_bytes: 4,
        max_followers: 2,
        max_follower_id_bytes: 8,
        max_cuts: 1,
        max_cut_bytes: 8,
        max_members: 1,
        max_membership_bytes: 8,
        max_scope_bytes: 8,
        max_batch_events: 2,
        max_batch_bytes: 8,
        max_inflight_bytes: 8,
        max_total_ms: 1_000,
        max_follower_ms: 500,
    }
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

    fn build_local_capture<'a>(
        &'a self,
        request: LocalCaptureRequest,
        admission: &'a ByteAdmission,
    ) -> BoxRecoveryFuture<'a, Result<LocalCaptureOutcome<Self::Image>, AdapterError>> {
        Box::pin(async move {
            let LocalCaptureRequest {
                recovery,
                selected,
                permit,
                clock,
                wake,
                ..
            } = request;
            let encoded = admission
                .reserve(AdmissionClass::Encoded, 1)
                .map_err(|_| AdapterError)?;
            let decoded = admission
                .reserve(AdmissionClass::Decoded, 1)
                .map_err(|_| AdapterError)?;
            let mut config = journal_config();
            let capture_max_ms = self.capture_max_ms.load(Ordering::SeqCst);
            if capture_max_ms > 0 {
                config.max_total_ms = capture_max_ms;
                config.max_follower_ms = config.max_follower_ms.min(capture_max_ms);
            }
            let storage = DonorJournal::storage_bound(config).map_err(|_| AdapterError)?;
            let suffix = admission
                .reserve(AdmissionClass::Suffix, storage)
                .map_err(|_| AdapterError)?;
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
            .map_err(|_| AdapterError)?;
            journal
                .begin_capture(
                    clock.now(),
                    1,
                    1,
                    vec![member_from_claim(&selected)],
                    Vec::new(),
                )
                .map_err(|_| AdapterError)?;
            self.build_starts.fetch_add(1, Ordering::SeqCst);
            let page = Duration::from_millis(self.page_ms.load(Ordering::SeqCst));
            for _ in 0..self.build_pages.load(Ordering::SeqCst) {
                tokio::time::sleep(page).await;
                permit.publish(|| ()).ok_or(AdapterError)?;
                permit.progress();
            }
            if self.pause.load(Ordering::SeqCst) {
                *self.latest_permit.lock().unwrap() = Some(permit.clone());
                self.started.notify_one();
                self.resume.notified().await;
            }
            permit
                .publish(|| self.builds.fetch_add(1, Ordering::SeqCst))
                .ok_or(AdapterError)?;
            if self.local_only.load(Ordering::SeqCst) {
                return Ok(LocalCaptureOutcome::LocalOnly);
            }
            journal
                .finish_capture(clock.now(), 1, 1)
                .map_err(|_| AdapterError)?;
            let ingress = JournalIngress::new(journal, suffix, wake)?;
            *self.ingress.lock().unwrap() = Some(ingress.clone());
            DonorCapture::new(vec![42], ingress, encoded, decoded).map(LocalCaptureOutcome::Ready)
        })
    }

    fn recapture_current_index<'a>(
        &'a self,
        request: groupnet_consistency::volatile_recovery::bootstrap::ports::ReadyCaptureRequest,
        admission: &'a ByteAdmission,
    ) -> BoxRecoveryFuture<'a, Result<DonorCapture<Self::Image>, AdapterError>> {
        Box::pin(async move {
            self.recapture_attempts.fetch_add(1, Ordering::SeqCst);
            if !self.allow_recapture.load(Ordering::SeqCst) {
                return Err(AdapterError);
            }
            let encoded = admission
                .reserve(AdmissionClass::Encoded, 1)
                .map_err(|_| AdapterError)?;
            let decoded = admission
                .reserve(AdmissionClass::Decoded, 1)
                .map_err(|_| AdapterError)?;
            let config = JournalConfig {
                max_members: 2,
                max_membership_bytes: 128,
                ..journal_config()
            };
            let suffix = admission
                .reserve(
                    AdmissionClass::Suffix,
                    DonorJournal::storage_bound(config).map_err(|_| AdapterError)?,
                )
                .map_err(|_| AdapterError)?;
            let journal = request
                .guard
                .capture(|generation| {
                    if generation != request.recovery_generation {
                        return Err(AdapterError);
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
                    .map_err(|_| AdapterError)?;
                    let block = self.recapture_block_ms.load(Ordering::SeqCst);
                    if block > 0 {
                        std::thread::sleep(Duration::from_millis(block));
                    }
                    journal
                        .begin_capture(request.clock.now(), 1, 1, request.members, Vec::new())
                        .map_err(|_| AdapterError)?;
                    Ok(journal)
                })
                .ok_or(AdapterError)??;
            let mut journal = journal;
            journal
                .finish_capture(request.clock.now(), 1, 1)
                .map_err(|_| AdapterError)?;
            let ingress = JournalIngress::new(journal, suffix, request.wake)?;
            *self.ingress.lock().unwrap() = Some(ingress.clone());
            DonorCapture::new(vec![42], ingress, encoded, decoded)
        })
    }

    fn prepare_follower(
        &self,
        _request: &DonorRequest,
        _capture: &DonorCapture<Self::Image>,
        _now: Time,
        _admission: &ByteAdmission,
    ) -> Result<DonorReply, AdapterError> {
        self.follower_prepares.fetch_add(1, Ordering::SeqCst);
        Err(AdapterError)
    }

    fn execute<'a>(
        &'a self,
        _context: &'a TransferContext,
        _effect: TransferEffect,
        _resources: &'a mut TransferResources<Self::Stage, Self::Attachment, Self::NativeBuffer>,
        _admission: &'a ByteAdmission,
        _permit: Option<PublicationPermit>,
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
        Box::pin(async { Err(AdapterError) })
    }

    fn detach<'a>(
        &'a self,
        _token: groupnet_core::volatile_bootstrap::journal::AttachToken,
        _resources: &'a mut TransferResources<Self::Stage, Self::Attachment, Self::NativeBuffer>,
    ) -> BoxRecoveryFuture<'a, Result<(), AdapterError>> {
        Box::pin(async { Ok(()) })
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
    ) -> BoxRecoveryFuture<'_, Result<(), AdapterError>> {
        Box::pin(async { Ok(()) })
    }

    fn rebuild_origin(
        &self,
        _op: RecoveryOperation,
        permit: PublicationPermit,
    ) -> BoxRecoveryFuture<'_, Result<(), AdapterError>> {
        Box::pin(async move {
            *self.latest_origin_permit.lock().unwrap() = Some(permit);
            self.old_origin_builds.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }

    fn observe_peers(
        &self,
        _op: RecoveryOperation,
        _limits: RecoveryConfig,
    ) -> BoxRecoveryFuture<'_, PeerObservation> {
        Box::pin(async move {
            if self.lapse_sequences.load(Ordering::SeqCst) == 0 {
                // Post-handoff check only: the bootstrap child supplies the
                // member identities, this adapter just the quiet peer's head.
                let peer = self.peer.as_ref().ok_or(AdapterError)?;
                return Ok((
                    vec![groupnet_consistency::volatile_recovery::Peer {
                        node: peer.node.clone(),
                        alive: true,
                        grants_lease: false,
                        old_nonlive: false,
                        grant: None,
                        head: None,
                        renewal: None,
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
                sequence: u64::try_from(sequence).map_err(|_| AdapterError)?,
            };
            Ok((
                vec![groupnet_consistency::volatile_recovery::Peer {
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
                }],
                Some(mark),
            ))
        })
    }

    fn wait_frontiers(
        &self,
        _op: RecoveryOperation,
        _heads: Vec<(NodeId, Mark)>,
    ) -> BoxRecoveryFuture<'_, Result<(), AdapterError>> {
        Box::pin(async { Ok(()) })
    }

    fn affirm(&self, _op: RecoveryOperation) -> bool {
        true
    }

    fn fell_back(&self, from: RecoveryStage, reason: RecoveryFallback) {
        self.fallbacks.lock().unwrap().push((from, reason));
    }
}

fn bootstrap_config() -> BootstrapRuntimeConfig {
    BootstrapRuntimeConfig {
        claim: BootstrapConfig {
            max_members: 2,
            max_member_bytes: 8,
            max_scope_bytes: 8,
            settle_ms: 3,
            renew_ms: 100,
            claim_ttl_ms: 500,
            observe_ms: 100,
            donor_wait_ms: 500,
            total_ms: 800,
        },
        transfer: TransferConfig {
            expected_schema: 1,
            max_metadata_bytes: 128,
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
        },
        max_claim_metadata_bytes: 128,
        donor_inbox_capacity: 2,
        require_participation: false,
    }
}

fn admission() -> ByteAdmission {
    ByteAdmission::new(AdmissionLimits {
        max_total_bytes: 100_000,
        max_encoded_bytes: 1_024,
        max_decoded_bytes: 1_024,
        max_suffix_bytes: 50_000,
        max_native_overlap_bytes: 1_024,
        max_inflight_bytes: 8_192,
        max_reservations: 32,
    })
    .unwrap()
}

#[path = "volatile_bootstrap_runtime/scenarios.rs"]
mod scenarios;

#[cfg(feature = "volatile-bootstrap-bulk")]
#[path = "volatile_bootstrap_runtime/bulk_recapture.rs"]
mod bulk_recapture;

#[path = "volatile_bootstrap_runtime/roster_maintenance.rs"]
mod roster_maintenance;

#[path = "volatile_bootstrap_runtime/scheduling.rs"]
mod scheduling;

#[path = "volatile_bootstrap_runtime/slow_build.rs"]
mod slow_build;

#[path = "volatile_bootstrap_runtime/peer_donor.rs"]
mod peer_donor;
use peer_donor::PeerDonor;

#[path = "volatile_bootstrap_runtime/origin_presence.rs"]
mod origin_presence;

#[path = "volatile_bootstrap_runtime/follower_progress.rs"]
mod follower_progress;
