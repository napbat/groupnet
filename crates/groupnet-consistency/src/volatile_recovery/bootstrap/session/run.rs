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

impl<C: ClaimSource, D: DonorPort> BootstrapSession<C, D> {
    pub(super) async fn current_participation(
        &mut self,
        due: Instant,
    ) -> Option<Vec<BootstrapMemberIdentity>> {
        if !self.accept(BootstrapEvent::Tick(self.now())) {
            return None;
        }
        let parent = self.engine.current_operation();
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
        if !self.tick_after_io() {
            return None;
        }
        if self.engine.current_operation() != parent {
            return None;
        }
        snapshot.consume(|mut snapshot| {
            let age_ms = observation_age_ms(snapshot.sampled_at, Instant::now())?;
            for participant in &mut snapshot.participants {
                participant.remaining_ms = participant.remaining_ms.saturating_sub(age_ms);
            }
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
                    remaining_ms: participant.remaining_ms,
                })
                .collect();
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
        })
    }

    /// One complete current participation cut for the recovery core's
    /// post-handoff peer check, after this child's candidate has retired.
    pub(super) async fn observe_peer_roster(
        &mut self,
        due: Instant,
    ) -> Option<Vec<BootstrapMemberIdentity>> {
        if !self.accept(BootstrapEvent::Tick(self.now())) {
            return None;
        }
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
        if !self.tick_after_io() {
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
            self.engine
                .verify_peer_roster(
                    op,
                    &snapshot.members,
                    &snapshot.roster,
                    &participants,
                    &snapshot.claims,
                )
                .ok()
        })
    }

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
        let built = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            self.donor.recapture_current_index(
                ReadyCaptureRequest {
                    operation: op,
                    selected: selected.clone(),
                    recovery_generation,
                    members: members.clone(),
                    guard: guard.clone(),
                    deadline,
                    now: self.now(),
                    wake: Arc::clone(&self.wake),
                },
                &self.admission,
            ),
        )
        .await;
        let _ = self.tick_after_io();
        if let Ok(Ok(capture)) = built {
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
        due: Instant,
    ) -> BootstrapOutcome {
        if Instant::now() >= due || !permit.valid() {
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
        self.due = Some(due);
        if !self.accept(BootstrapEvent::Tick(self.now())) || !self.accept(BootstrapEvent::Start) {
            return BootstrapOutcome::Declined;
        }
        loop {
            if Instant::now() >= due || !permit.valid() {
                return BootstrapOutcome::Declined;
            }
            if !self.accept(BootstrapEvent::Tick(self.now())) {
                return BootstrapOutcome::Declined;
            }
            // Every accepted step re-arms its timer. Drain the queued work
            // before the next Tick; re-ticking after a no-op effect queues a
            // fresh `ArmTimer` forever and never yields to the runtime.
            while let Some(effect) = self.effects.pop_front() {
                if Instant::now() >= due || !permit.valid() {
                    return BootstrapOutcome::Declined;
                }
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
            let Some(next) = self
                .engine
                .next_deadline()
                .and_then(|time| self.absolute(time))
            else {
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
                    let _ = self.tick_after_io();
                    let Ok(Ok(snapshot)) = snapshot else {
                        return Some(BootstrapOutcome::Declined);
                    };
                    if self.engine.current_operation() == Some(op) {
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
                let _ = self.tick_after_io();
                let Ok(Ok(snapshot)) = snapshot else {
                    return Some(BootstrapOutcome::Declined);
                };
                if self.engine.current_operation() == Some(op) {
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
                let operation_due = self.operation_due(op, due)?;
                let members = if self.config.require_participation {
                    let Some(members) = self.current_participation(due).await else {
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
                let permit = permit.restricted_to(operation_due);
                let built = tokio::time::timeout_at(
                    tokio::time::Instant::from_std(operation_due),
                    self.donor.build_local_capture(
                        LocalCaptureRequest {
                            recovery,
                            build: op,
                            selected: selected.clone(),
                            members: members.clone(),
                            permit: permit.clone(),
                            now: self.now(),
                            wake: Arc::clone(&self.wake),
                        },
                        &self.admission,
                    ),
                )
                .await;
                let _ = self.tick_after_io();
                if let Ok(Ok(LocalCaptureOutcome::LocalOnly)) = &built {
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
                if let Ok(Ok(LocalCaptureOutcome::Ready(capture))) = built {
                    if permit.valid()
                        && self.engine.current_operation() == Some(op)
                        && capture.is_active()
                    {
                        if self.config.require_participation {
                            // An image encoded across a membership change is
                            // never advertised as a complete donor capture.
                            if self.current_participation(due).await.as_deref()
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
                    && self.current_participation(due).await.is_none()
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
        let due = Instant::now()
            .checked_add(Duration::from_millis(self.config.claim.observe_ms))
            .unwrap_or_else(Instant::now);
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
                    }
                }
                BootstrapEffect::WithdrawPresence(identity) => {
                    let _ = tokio::time::timeout_at(
                        tokio::time::Instant::from_std(due),
                        self.claims.withdraw_presence(identity),
                    )
                    .await;
                }
                BootstrapEffect::PublishClaim(claim)
                    if self.capture.is_some()
                        || self.engine.stage() == BootstrapStage::Building =>
                {
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
