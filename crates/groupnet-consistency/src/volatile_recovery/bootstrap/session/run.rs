//! Serial effect execution under the outer recovery episode deadline.

use super::{
    Arc, BootstrapEffect, BootstrapEvent, BootstrapOutcome, BootstrapSession, BootstrapStage,
    ClaimSource, DonorPort, Duration, Instant, PublicationPermit, RecoveryOperation,
    TransferContext, TransferEffect, TransferEvent, TransferResources,
};
use crate::volatile_recovery::bootstrap::ports::{
    LocalCaptureOutcome, LocalCaptureRequest, ReadyCaptureRequest,
};
use groupnet_core::volatile_bootstrap::{
    BootstrapMemberIdentity, BootstrapParticipant, ClaimIdentity,
};

/// Where one participation cut is sampled and how it is verified.
#[derive(Clone, Copy, PartialEq, Eq)]
enum RosterCheck {
    /// Pin or recheck the roster of a candidate this acquisition drives.
    Acquisition,
    /// Recheck a completed candidate's roster on a maintenance turn.
    Maintenance,
    /// Return the roster for the recovery core's post-handoff peer check.
    PeerHandoff,
}

impl<C: ClaimSource, D: DonorPort> BootstrapSession<C, D> {
    /// The active acquisition's complete participation roster.
    pub(super) async fn acquisition_participation(
        &mut self,
        due: Instant,
    ) -> Option<Vec<BootstrapMemberIdentity>> {
        self.sample_participation(due, RosterCheck::Acquisition)
            .await
    }

    /// A completed candidate's participation roster on a maintenance turn.
    pub(super) async fn current_participation(
        &mut self,
        due: Instant,
    ) -> Option<Vec<BootstrapMemberIdentity>> {
        self.sample_participation(due, RosterCheck::Maintenance)
            .await
    }

    /// One complete current participation cut for the recovery core's
    /// post-handoff peer check, after this child's candidate has retired.
    pub(super) async fn observe_peer_roster(
        &mut self,
        due: Instant,
    ) -> Option<Vec<BootstrapMemberIdentity>> {
        self.sample_participation(due, RosterCheck::PeerHandoff)
            .await
    }

    /// Sample the native participation cut and verify it against this
    /// worker's own claim and presence sequence. Every renewal the engine
    /// has scheduled is published before the source is read back, and the
    /// engine is not ticked again until the cut is verified, so the cut
    /// reflects its current renewal. The operation deadline is checked on
    /// the wall clock.
    async fn sample_participation(
        &mut self,
        due: Instant,
        check: RosterCheck,
    ) -> Option<Vec<BootstrapMemberIdentity>> {
        self.tick_publishing_renewals(due, check).await?;
        let op = self.engine.begin_roster_observation().ok()?;
        let operation_due = self.operation_due(op, due)?;
        let limits = self.claim_limits(
            self.config.claim.max_members,
            self.config.claim.max_member_bytes,
        );
        let snapshot = tokio::time::timeout_at(
            tokio::time::Instant::from_std(operation_due),
            self.claims
                .observe_participation(op, limits, &self.admission),
        )
        .await
        .ok()?
        .ok()?;
        if Instant::now() >= operation_due {
            return None;
        }
        snapshot.consume(|mut snapshot| {
            let age_ms = observation_age_ms(snapshot.sampled_at, Instant::now())?;
            for claim in &mut snapshot.claims {
                claim.remaining_ms = claim.remaining_ms.saturating_sub(age_ms);
            }
            snapshot.claims.retain(|claim| claim.remaining_ms > 0);
            let participants: Vec<_> = snapshot
                .participants
                .into_iter()
                .map(|participant| BootstrapParticipant {
                    member: participant.member,
                    renewal: participant.renewal,
                    remaining_ms: participant.remaining_ms.saturating_sub(age_ms),
                })
                .collect();
            match check {
                RosterCheck::Acquisition | RosterCheck::Maintenance => {
                    self.engine
                        .verify_participant_roster(
                            op,
                            &snapshot.members,
                            &snapshot.roster,
                            &participants,
                            &snapshot.claims,
                        )
                        .ok()?;
                    self.engine
                        .participant_roster()
                        .map(<[BootstrapMemberIdentity]>::to_vec)
                }
                RosterCheck::PeerHandoff => self
                    .engine
                    .verify_peer_roster(
                        op,
                        &snapshot.members,
                        &snapshot.roster,
                        &participants,
                        &snapshot.claims,
                    )
                    .ok(),
            }
        })
    }

    /// Advance the engine's clock and publish every claim or presence renewal
    /// still queued, including one an earlier tick scheduled, before the
    /// caller reads the native source back: the cut is verified against this
    /// worker's latest own sequence, so an unpublished renewal would refute
    /// it. Other scheduled work stays queued in order. `None` if the tick or a
    /// publication failed.
    async fn tick_publishing_renewals(&mut self, due: Instant, check: RosterCheck) -> Option<()> {
        if !self.accept(BootstrapEvent::Tick(self.now())) {
            return None;
        }
        self.publish_scheduled(0, due, check).await
    }

    /// Whether this worker's own claim is live: advertised by a capture, held
    /// by a running build or recapture, or kept for a pending recapture.
    /// Outside an acquisition only a live claim's renewal is published.
    fn claim_live(&self) -> bool {
        self.capture.is_some()
            || self.engine.stage() == BootstrapStage::Building
            || self.engine.ready_recapture_pending()
    }

    /// Publish the claim and presence renewals queued after `queued`,
    /// keeping every other effect queued in order.
    async fn publish_scheduled(
        &mut self,
        queued: usize,
        due: Instant,
        check: RosterCheck,
    ) -> Option<()> {
        let scheduled = self.effects.split_off(queued);
        for effect in scheduled {
            match effect {
                // Outside an acquisition a withdrawn donor claim stays
                // withdrawn until its recapture, as in `drain_maintenance`.
                BootstrapEffect::PublishClaim(_)
                    if check != RosterCheck::Acquisition && !self.claim_live() => {}
                BootstrapEffect::PublishClaim(claim) => {
                    let published = tokio::time::timeout_at(
                        tokio::time::Instant::from_std(due),
                        self.claims.publish_claim(claim),
                    )
                    .await;
                    if !matches!(published, Ok(Ok(()))) {
                        return None;
                    }
                }
                BootstrapEffect::PublishPresence(presence) => {
                    let published = tokio::time::timeout_at(
                        tokio::time::Instant::from_std(due),
                        self.claims.publish_presence(presence.clone()),
                    )
                    .await;
                    if !matches!(published, Ok(Ok(()))) {
                        let _ = self.accept(BootstrapEvent::PresenceFailed {
                            identity: presence.identity,
                            renewal: presence.renewal,
                        });
                        return None;
                    }
                }
                BootstrapEffect::ArmTimer(_) => {}
                other => self.effects.push_back(other),
            }
        }
        Some(())
    }

    /// Drive local work, an origin build or a Ready recapture, while keeping
    /// this node's claim and presence renewed on their cadence, so neither
    /// lapses however long the work runs. For a build, each renewal turn
    /// converts progress the build reported on the parent permit into
    /// `BuildProgressed`, which restarts the build's stall bound and
    /// advertises the advance to followers. `outer` is the work's current
    /// outer deadline. `None` once the operation's bound, `outer`, or a
    /// failed renewal ends the work first; dropping the future stops it
    /// before any later publication.
    async fn await_renewing<T>(
        &mut self,
        op: groupnet_core::volatile_bootstrap::BootstrapOperation,
        build: Option<(&ClaimIdentity, &PublicationPermit)>,
        outer: impl Fn() -> Option<Instant>,
        mut work: crate::volatile_recovery::BoxRecoveryFuture<'_, T>,
    ) -> Option<T> {
        let mut reported = match build {
            Some((_, permit)) => Some(permit.progress_reports()?),
            None => None,
        };
        loop {
            let operation_due = self.operation_due(op, outer()?)?;
            if Instant::now() >= operation_due {
                return None;
            }
            let wake = self
                .engine
                .next_deadline()
                .and_then(|due| self.absolute(due))
                .map_or(operation_due, |due| due.min(operation_due));
            tokio::select! {
                biased;
                value = &mut work => return Some(value),
                () = tokio::time::sleep_until(tokio::time::Instant::from_std(wake)) => {}
            }
            let queued = self.effects.len();
            if let Some((selected, permit)) = build
                && let Some(progress) = permit.progress_reports()
                && Some(progress) != reported
            {
                reported = Some(progress);
                // Applied before this turn's tick, so progress reported just
                // before the stall bound still renews it. A refused report
                // leaves the tick below to end the build.
                let _ = self.accept(BootstrapEvent::BuildProgressed {
                    op,
                    selected: selected.clone(),
                });
            }
            if !self.accept(BootstrapEvent::Tick(self.now())) {
                return None;
            }
            self.publish_scheduled(queued, outer()?, RosterCheck::Acquisition)
                .await?;
            if self.engine.current_operation() != Some(op) {
                return None;
            }
        }
    }

    /// One bounded recapture of the completed local image. Any failure only
    /// makes the recapture pending again in the core; it retries under a
    /// different participation cut and never scans the origin.
    async fn recapture_current(
        &mut self,
        op: groupnet_core::volatile_bootstrap::BootstrapOperation,
        selected: ClaimIdentity,
        outer: Instant,
    ) {
        let Some(deadline) = self.operation_due(op, outer) else {
            return;
        };
        let Some(guard) = self
            .ready_guard
            .take()
            .map(|guard| guard.restricted_to(deadline))
        else {
            let _ = self.accept(BootstrapEvent::BuildFailed { op, selected });
            return;
        };
        let Some(recovery_generation) = self.recovery.map(|recovery| recovery.generation) else {
            let _ = self.accept(BootstrapEvent::BuildFailed { op, selected });
            return;
        };
        let Some(members) = self.current_participation(deadline).await else {
            let _ = self.accept(BootstrapEvent::BuildFailed { op, selected });
            return;
        };
        let donor = Arc::clone(&self.donor);
        let admission = self.admission.clone();
        let capture = donor.recapture_current_index(
            ReadyCaptureRequest {
                operation: op,
                selected: selected.clone(),
                recovery_generation,
                members: members.clone(),
                guard: guard.clone(),
                deadline,
                clock: self.clock,
                wake: Arc::clone(&self.wake),
            },
            &admission,
        );
        let built = self
            .await_renewing(op, None, || Some(deadline), capture)
            .await;
        let _ = self.tick_after_io();
        if let Some(Ok(capture)) = built {
            if guard.valid()
                && self.engine.current_operation() == Some(op)
                && capture.is_active()
                && self.current_participation(deadline).await.as_deref() == Some(members.as_slice())
                && guard.valid()
            {
                self.capture = Some(capture);
                if self.accept(BootstrapEvent::Built {
                    op,
                    selected: selected.clone(),
                }) {
                    self.inbox.set_identity(Some(selected));
                    self.drain_ready_publication(deadline).await;
                    return;
                }
                self.drop_capture();
            } else {
                self.donor.retire_local_capture(&capture);
                drop(capture);
            }
        }
        if self.engine.current_operation() == Some(op) {
            let _ = self.accept(BootstrapEvent::BuildFailed { op, selected });
        }
    }

    pub(super) async fn run_acquisition(
        &mut self,
        recovery: RecoveryOperation,
        permit: PublicationPermit,
    ) -> BootstrapOutcome {
        if permit.deadline().is_none() {
            return BootstrapOutcome::Declined;
        }
        if self.engine.stage() == BootstrapStage::Cancelled && !self.reset_engine() {
            return BootstrapOutcome::Declined;
        }
        self.drop_capture();
        self.resources = TransferResources::default();
        self.child_parent = None;
        self.transfer_context = None;
        self.recovery = Some(recovery);
        self.suspended_recovery = None;
        self.permit = Some(permit.clone());
        self.ready_guard = None;
        if !self.accept(BootstrapEvent::Tick(self.now())) || !self.accept(BootstrapEvent::Start) {
            return BootstrapOutcome::Declined;
        }
        loop {
            // The parent's deadline is the permit's current one: it moves
            // later while the selected build reports progress.
            if permit.deadline().is_none() {
                return BootstrapOutcome::Declined;
            }
            if !self.accept(BootstrapEvent::Tick(self.now())) {
                return BootstrapOutcome::Declined;
            }
            // Every accepted step re-arms its timer. Drain the queued work
            // before the next Tick; re-ticking after a no-op effect queues a
            // fresh `ArmTimer` forever and never yields to the runtime.
            while let Some(effect) = self.effects.pop_front() {
                let Some(due) = permit.deadline() else {
                    return BootstrapOutcome::Declined;
                };
                if let Some(outcome) = self.execute_claim_effect(effect, due).await {
                    return outcome;
                }
            }
            if matches!(
                self.engine.stage(),
                BootstrapStage::Fallback | BootstrapStage::Cancelled
            ) {
                return BootstrapOutcome::Declined;
            }
            let (Some(next), Some(due)) = (
                self.engine
                    .next_deadline()
                    .and_then(|time| self.absolute(time)),
                permit.deadline(),
            ) else {
                return BootstrapOutcome::Declined;
            };
            let wake = Arc::clone(&self.wake);
            tokio::select! {
                () = tokio::time::sleep_until(tokio::time::Instant::from_std(next.min(due))) => {},
                () = wake.notified() => {
                    self.service_pending().await;
                    self.retire_capture_if_invalid();
                }
            }
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one serial dispatch for the finite claim effects"
    )]
    async fn execute_claim_effect(
        &mut self,
        effect: BootstrapEffect,
        due: Instant,
    ) -> Option<BootstrapOutcome> {
        match effect {
            BootstrapEffect::ArmTimer(_)
            | BootstrapEffect::CancelWork { .. }
            | BootstrapEffect::FollowBuilder { .. } => {}
            BootstrapEffect::BuilderProgressed => {
                // A followed build that keeps advancing keeps this follower's
                // parent recovery alive past any fixed wait.
                if let Some(permit) = &self.permit {
                    permit.progress();
                }
            }
            BootstrapEffect::PublishClaim(claim) => {
                let result = tokio::time::timeout_at(
                    tokio::time::Instant::from_std(due),
                    self.claims.publish_claim(claim),
                )
                .await;
                if !matches!(result, Ok(Ok(()))) {
                    return Some(BootstrapOutcome::Declined);
                }
            }
            BootstrapEffect::PublishPresence(presence) => {
                let result = tokio::time::timeout_at(
                    tokio::time::Instant::from_std(due),
                    self.claims.publish_presence(presence.clone()),
                )
                .await;
                if !matches!(result, Ok(Ok(()))) {
                    let _ = self.accept(BootstrapEvent::PresenceFailed {
                        identity: presence.identity,
                        renewal: presence.renewal,
                    });
                    return Some(BootstrapOutcome::Declined);
                }
            }
            BootstrapEffect::WithdrawClaim(identity) => {
                let _ = tokio::time::timeout_at(
                    tokio::time::Instant::from_std(due),
                    self.claims.withdraw_claim(identity),
                )
                .await;
            }
            BootstrapEffect::WithdrawPresence(identity) => {
                let _ = tokio::time::timeout_at(
                    tokio::time::Instant::from_std(due),
                    self.claims.withdraw_presence(identity),
                )
                .await;
            }
            BootstrapEffect::ObserveClaims {
                op,
                max_members,
                max_member_bytes,
            } => {
                let operation_due = self.operation_due(op, due)?;
                let limits = self.claim_limits(max_members, max_member_bytes);
                if self.config.require_participation {
                    let snapshot = tokio::time::timeout_at(
                        tokio::time::Instant::from_std(operation_due),
                        self.claims
                            .observe_participation(op, limits, &self.admission),
                    )
                    .await;
                    // No engine tick between the read and its verification: a
                    // renewal scheduled by that tick is not in the cut yet and
                    // would contradict this worker's own claim sequence. The
                    // next loop tick publishes it; the deadline is wall time.
                    let Ok(Ok(snapshot)) = snapshot else {
                        return Some(BootstrapOutcome::Declined);
                    };
                    if Instant::now() < operation_due && self.engine.current_operation() == Some(op)
                    {
                        let accepted = snapshot.consume(|mut snapshot| {
                            let Some(age_ms) =
                                observation_age_ms(snapshot.sampled_at, Instant::now())
                            else {
                                return false;
                            };
                            for participant in &mut snapshot.participants {
                                participant.remaining_ms =
                                    participant.remaining_ms.saturating_sub(age_ms);
                            }
                            for claim in &mut snapshot.claims {
                                claim.remaining_ms = claim.remaining_ms.saturating_sub(age_ms);
                            }
                            snapshot.claims.retain(|claim| claim.remaining_ms > 0);
                            self.accept(BootstrapEvent::ParticipantsObserved {
                                op,
                                members: snapshot.members,
                                roster: snapshot.roster,
                                participants: snapshot
                                    .participants
                                    .into_iter()
                                    .map(|participant| BootstrapParticipant {
                                        member: participant.member,
                                        renewal: participant.renewal,
                                        remaining_ms: participant.remaining_ms,
                                    })
                                    .collect(),
                                claims: snapshot.claims,
                            })
                        });
                        if !accepted {
                            return Some(BootstrapOutcome::Declined);
                        }
                    }
                    return None;
                }
                let snapshot = tokio::time::timeout_at(
                    tokio::time::Instant::from_std(operation_due),
                    self.claims.observe_claims(op, limits, &self.admission),
                )
                .await;
                let Ok(Ok(snapshot)) = snapshot else {
                    return Some(BootstrapOutcome::Declined);
                };
                if Instant::now() < operation_due && self.engine.current_operation() == Some(op) {
                    let accepted = snapshot.consume(|mut snapshot| {
                        let Some(age_ms) = observation_age_ms(snapshot.sampled_at, Instant::now())
                        else {
                            return false;
                        };
                        for claim in &mut snapshot.claims {
                            claim.remaining_ms = claim.remaining_ms.saturating_sub(age_ms);
                        }
                        snapshot.claims.retain(|claim| claim.remaining_ms > 0);
                        self.accept(BootstrapEvent::ClaimsObserved {
                            op,
                            members: snapshot.members,
                            claims: snapshot.claims,
                        })
                    });
                    if !accepted {
                        return Some(BootstrapOutcome::Declined);
                    }
                }
            }
            BootstrapEffect::BuildOrigin { op, selected } => {
                self.operation_due(op, due)?;
                let members = if self.config.require_participation {
                    let Some(members) = self.acquisition_participation(due).await else {
                        return Some(BootstrapOutcome::Declined);
                    };
                    members
                } else {
                    Vec::new()
                };
                let Some(recovery) = self.recovery else {
                    return Some(BootstrapOutcome::Declined);
                };
                let Some(permit) = self.permit.clone() else {
                    return Some(BootstrapOutcome::Declined);
                };
                let donor = Arc::clone(&self.donor);
                let admission = self.admission.clone();
                let scan = donor.build_local_capture(
                    LocalCaptureRequest {
                        recovery,
                        build: op,
                        selected: selected.clone(),
                        members: members.clone(),
                        permit: permit.clone(),
                        clock: self.clock,
                        wake: Arc::clone(&self.wake),
                    },
                    &admission,
                );
                let built = self
                    .await_renewing(op, Some((&selected, &permit)), || permit.deadline(), scan)
                    .await;
                let _ = self.tick_after_io();
                if let Some(Ok(LocalCaptureOutcome::LocalOnly)) = &built {
                    if permit.valid()
                        && self.engine.current_operation() == Some(op)
                        && self.accept(BootstrapEvent::LocalOnlyBuilt {
                            op,
                            selected: selected.clone(),
                        })
                    {
                        self.drain_maintenance().await;
                        return Some(BootstrapOutcome::LocalBuilt);
                    }
                    return Some(BootstrapOutcome::Declined);
                }
                if let Some(Ok(LocalCaptureOutcome::Ready(capture))) = built {
                    // The build may have outlived the deadline sampled before
                    // it; continue under the parent's renewed one.
                    let due = permit.deadline().unwrap_or(due);
                    if permit.valid()
                        && self.engine.current_operation() == Some(op)
                        && capture.is_active()
                    {
                        if self.config.require_participation {
                            // An image encoded across a membership change is
                            // never advertised as a complete donor capture.
                            if self.acquisition_participation(due).await.as_deref()
                                != Some(members.as_slice())
                            {
                                self.donor.retire_local_capture(&capture);
                                drop(capture);
                                if self.accept(BootstrapEvent::LocalOnlyBuilt {
                                    op,
                                    selected: selected.clone(),
                                }) {
                                    self.drain_maintenance().await;
                                    return Some(BootstrapOutcome::LocalBuilt);
                                }
                                return Some(BootstrapOutcome::Declined);
                            }
                        }
                        self.capture = Some(capture);
                        if !self.accept(BootstrapEvent::Built {
                            op,
                            selected: selected.clone(),
                        }) {
                            self.drop_capture();
                            return Some(BootstrapOutcome::Declined);
                        }
                        // The same worker that owns the captured image
                        // updates the listener's exact admitted claim. A
                        // later capture retirement clears it synchronously.
                        self.inbox.set_identity(Some(selected));
                        // A complete captured image exists before the Ready
                        // claim is advertised. Local recovery uses this scan.
                        self.drain_ready_publication(due).await;
                        return Some(BootstrapOutcome::LocalBuilt);
                    }
                    self.donor.retire_local_capture(&capture);
                    drop(capture);
                }
                if self.engine.current_operation() == Some(op) {
                    let _ = self.accept(BootstrapEvent::BuildFailed { op, selected });
                }
            }
            BootstrapEffect::RecaptureCurrent { op, selected } => {
                self.recapture_current(op, selected, due).await;
            }
            BootstrapEffect::DonorAvailable { op, selected } => {
                if self.config.require_participation
                    && self.acquisition_participation(due).await.is_none()
                {
                    let _ = self.accept(BootstrapEvent::PeerTransferDeclined { op, selected });
                    return Some(BootstrapOutcome::Declined);
                }
                self.child_parent = Some(op);
                self.transfer_context = Some(TransferContext {
                    parent: op,
                    donor: selected.clone(),
                    follower: ClaimIdentity {
                        node: self.me.clone(),
                        incarnation: self.boot,
                        session: self.session,
                        attempt: self.engine.generation(),
                    },
                });
                if !self.accept(BootstrapEvent::StartTransfer { op, selected }) {
                    return Some(BootstrapOutcome::Declined);
                }
            }
            BootstrapEffect::ObserveSelectedClaim { op, selected } => {
                let operation_due = self.operation_due(op, due)?;
                let limits = self.claim_limits(
                    self.config.claim.max_members,
                    self.config.claim.max_member_bytes,
                );
                let observed = tokio::time::timeout_at(
                    tokio::time::Instant::from_std(operation_due),
                    self.claims
                        .observe_selected_claim(op, selected, limits, &self.admission),
                )
                .await;
                let _ = self.tick_after_io();
                self.engine.operation_deadline(op)?;
                match observed {
                    Ok(Ok(Some(claim))) => {
                        let accepted = claim.consume(|mut observed| {
                            let Some(age_ms) =
                                observation_age_ms(observed.sampled_at, Instant::now())
                            else {
                                return false;
                            };
                            observed.claim.remaining_ms =
                                observed.claim.remaining_ms.saturating_sub(age_ms);
                            self.accept(BootstrapEvent::SelectedClaimObserved {
                                op,
                                claim: (observed.claim.remaining_ms > 0).then_some(observed.claim),
                            })
                        });
                        if !accepted {
                            return Some(BootstrapOutcome::Declined);
                        }
                    }
                    Ok(Ok(None)) => {
                        let _ =
                            self.accept(BootstrapEvent::SelectedClaimObserved { op, claim: None });
                    }
                    _ => return Some(BootstrapOutcome::Declined),
                }
            }
            BootstrapEffect::Transfer(effect) => {
                return self.execute_transfer_effect(*effect, due).await;
            }
            BootstrapEffect::FallbackOrigin => return Some(BootstrapOutcome::Declined),
        }
        None
    }

    async fn drain_ready_publication(&mut self, due: Instant) {
        while let Some(effect) = self.effects.pop_front() {
            match effect {
                BootstrapEffect::PublishPresence(presence) => {
                    if !matches!(
                        tokio::time::timeout_at(
                            tokio::time::Instant::from_std(due),
                            self.claims.publish_presence(presence.clone()),
                        )
                        .await,
                        Ok(Ok(()))
                    ) {
                        let _ = self.accept(BootstrapEvent::PresenceFailed {
                            identity: presence.identity,
                            renewal: presence.renewal,
                        });
                        self.drop_capture();
                        break;
                    }
                }
                BootstrapEffect::PublishClaim(claim) => {
                    let selected = claim.identity.clone();
                    if !matches!(
                        tokio::time::timeout_at(
                            tokio::time::Instant::from_std(due),
                            self.claims.publish_claim(claim),
                        )
                        .await,
                        Ok(Ok(()))
                    ) {
                        // Donor availability is optional. Withdraw the exact
                        // claim and retire capture, keeping the local scan.
                        self.drop_capture();
                        let _ = self.accept(BootstrapEvent::DonorPublicationFailed { selected });
                        break;
                    }
                }
                BootstrapEffect::ArmTimer(_) => {}
                other => {
                    self.effects.push_front(other);
                    break;
                }
            }
        }
    }

    pub(super) async fn retire_cancel_effects(&mut self) {
        let retire_due = Instant::now()
            .checked_add(Duration::from_millis(self.config.claim.observe_ms))
            .unwrap_or_else(Instant::now);
        while let Some(effect) = self.effects.pop_front() {
            match effect {
                BootstrapEffect::WithdrawPresence(identity) => {
                    let _ = tokio::time::timeout_at(
                        tokio::time::Instant::from_std(retire_due),
                        self.claims.withdraw_presence(identity),
                    )
                    .await;
                }
                BootstrapEffect::WithdrawClaim(identity) => {
                    let _ = tokio::time::timeout_at(
                        tokio::time::Instant::from_std(retire_due),
                        self.claims.withdraw_claim(identity),
                    )
                    .await;
                }
                BootstrapEffect::Transfer(effect) => {
                    let Some(context) = self.transfer_context.as_ref() else {
                        continue;
                    };
                    let _ = tokio::time::timeout_at(
                        tokio::time::Instant::from_std(retire_due),
                        self.donor.execute(
                            context,
                            *effect,
                            &mut self.resources,
                            &self.admission,
                            None,
                            retire_due,
                        ),
                    )
                    .await;
                }
                _ => {}
            }
        }
        self.drop_capture();
        self.resources = TransferResources::default();
    }

    pub(super) async fn drain_maintenance(&mut self) {
        while let Some(effect) = self.effects.pop_front() {
            // Each publication gets its own observation bound from when it
            // runs: a Ready recapture earlier in this drain may have held the
            // worker far past any bound sampled before it.
            let due = Instant::now()
                .checked_add(Duration::from_millis(self.config.claim.observe_ms))
                .unwrap_or_else(Instant::now);
            match effect {
                BootstrapEffect::PublishPresence(presence) => {
                    if !matches!(
                        tokio::time::timeout_at(
                            tokio::time::Instant::from_std(due),
                            self.claims.publish_presence(presence.clone()),
                        )
                        .await,
                        Ok(Ok(()))
                    ) {
                        let _ = self.accept(BootstrapEvent::PresenceFailed {
                            identity: presence.identity,
                            renewal: presence.renewal,
                        });
                        self.drop_capture();
                    }
                }
                BootstrapEffect::WithdrawPresence(identity) => {
                    let _ = tokio::time::timeout_at(
                        tokio::time::Instant::from_std(due),
                        self.claims.withdraw_presence(identity),
                    )
                    .await;
                }
                BootstrapEffect::PublishClaim(claim) if self.claim_live() => {
                    let selected = claim.identity.clone();
                    if !matches!(
                        tokio::time::timeout_at(
                            tokio::time::Instant::from_std(due),
                            self.claims.publish_claim(claim),
                        )
                        .await,
                        Ok(Ok(()))
                    ) {
                        self.drop_capture();
                        if self.engine.stage() == BootstrapStage::Building {
                            if let Some(op) = self.engine.current_operation() {
                                let _ = self.accept(BootstrapEvent::BuildFailed { op, selected });
                            }
                        } else {
                            let _ =
                                self.accept(BootstrapEvent::DonorPublicationFailed { selected });
                        }
                    }
                }
                BootstrapEffect::WithdrawClaim(identity) => {
                    let _ = tokio::time::timeout_at(
                        tokio::time::Instant::from_std(due),
                        self.claims.withdraw_claim(identity),
                    )
                    .await;
                }
                BootstrapEffect::RecaptureCurrent { op, selected } => {
                    let outer = Instant::now()
                        .checked_add(Duration::from_millis(self.config.claim.donor_wait_ms))
                        .unwrap_or(due);
                    self.recapture_current(op, selected, outer).await;
                }
                _ => {}
            }
        }
    }
}

/// Rounds actor-to-worker transit upward and adds a tick of source-clock
/// quantization margin. The two actors' logical origins are never compared.
fn observation_age_ms(sampled_at: Instant, now: Instant) -> Option<u64> {
    let elapsed = now.checked_duration_since(sampled_at)?;
    let rounded = elapsed.as_nanos().checked_add(999_999)? / 1_000_000;
    u64::try_from(rounded).ok()?.checked_add(1)
}

mod transfer;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn actor_ttl_ages_upward_with_one_quantization_tick() {
        let sampled_at = Instant::now();
        assert_eq!(observation_age_ms(sampled_at, sampled_at), Some(1));
        assert_eq!(
            observation_age_ms(sampled_at, sampled_at + Duration::from_nanos(1)),
            Some(2)
        );
        assert_eq!(
            observation_age_ms(sampled_at, sampled_at + Duration::from_micros(1_001)),
            Some(3)
        );
        assert_eq!(
            observation_age_ms(sampled_at + Duration::from_millis(1), sampled_at),
            None
        );
    }
}
