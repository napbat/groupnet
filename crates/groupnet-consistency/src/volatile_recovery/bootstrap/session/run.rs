//! Serial effect execution under the outer recovery episode deadline.

use super::{
    Arc, BootstrapEffect, BootstrapEvent, BootstrapOutcome, BootstrapSession, BootstrapStage,
    ClaimSource, DonorPort, Duration, Instant, PublicationPermit, RecoveryOperation,
    TransferContext, TransferEffect, TransferEvent, TransferResources,
};
use crate::volatile_recovery::bootstrap::ports::LocalCaptureRequest;
use groupnet_core::volatile_bootstrap::ClaimIdentity;

impl<C: ClaimSource, D: DonorPort> BootstrapSession<C, D> {
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
        self.permit = Some(permit.clone());
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
            if let Some(effect) = self.effects.pop_front() {
                if let Some(outcome) = self.execute_claim_effect(effect, due).await {
                    return outcome;
                }
                continue;
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
                    self.service_pending();
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
            BootstrapEffect::WithdrawClaim(identity) => {
                let _ = tokio::time::timeout_at(
                    tokio::time::Instant::from_std(due),
                    self.claims.withdraw_claim(identity),
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
                            permit: permit.clone(),
                            now: self.now(),
                            wake: Arc::clone(&self.wake),
                        },
                        &self.admission,
                    ),
                )
                .await;
                let _ = self.tick_after_io();
                if let Ok(Ok(capture)) = built {
                    if permit.valid()
                        && self.engine.current_operation() == Some(op)
                        && capture.is_active()
                    {
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
            BootstrapEffect::DonorAvailable { op, selected } => {
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
                BootstrapEffect::PublishClaim(claim) => {
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
                        let _ = self.accept(BootstrapEvent::Cancel);
                        self.drop_capture();
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
                BootstrapEffect::PublishClaim(claim) if self.capture.is_some() => {
                    if !matches!(
                        tokio::time::timeout_at(
                            tokio::time::Instant::from_std(due),
                            self.claims.publish_claim(claim),
                        )
                        .await,
                        Ok(Ok(()))
                    ) {
                        self.drop_capture();
                        let _ = self.accept(BootstrapEvent::Cancel);
                    }
                }
                BootstrapEffect::WithdrawClaim(identity) => {
                    let _ = tokio::time::timeout_at(
                        tokio::time::Instant::from_std(due),
                        self.claims.withdraw_claim(identity),
                    )
                    .await;
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
