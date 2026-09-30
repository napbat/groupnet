//! Composite claim/transfer transitions using the claim session allocator.

use crate::Time;

use super::super::transfer::{
    TransferBinding, TransferConfig, TransferEffect, TransferEvent, TransferSession, TransferStage,
    TransferStep,
};
use super::super::types::{
    BootstrapClaim, BootstrapEffect, BootstrapError, BootstrapOperation, BootstrapStage,
    BootstrapStep, ClaimIdentity, ClaimPhase,
};
use super::ClaimEngine;

impl ClaimEngine {
    /// Enable bounded peer transfer before the first selection event.
    ///
    /// # Errors
    /// Rejects invalid transfer limits or an already-started session.
    pub fn enable_transfer(&mut self, config: TransferConfig) -> Result<(), BootstrapError> {
        if self.stage != BootstrapStage::Unready {
            return Err(BootstrapError::Stage);
        }
        self.transfer_config = Some(
            config
                .validate()
                .map_err(|_| BootstrapError::InvalidConfig)?,
        );
        Ok(())
    }

    pub(super) fn cancel_transfer(&mut self) -> Vec<BootstrapEffect> {
        self.claim_refresh_due = None;
        self.claim_poll = None;
        self.claim_poll_due = None;
        let Some(mut child) = self.transfer.take() else {
            return Vec::new();
        };
        child
            .step(TransferEvent::Cancel, &mut || None)
            .effects
            .into_iter()
            .filter(|effect| !matches!(effect, TransferEffect::ArmTimer(_)))
            .map(|effect| BootstrapEffect::Transfer(Box::new(effect)))
            .collect()
    }

    pub(super) fn start_transfer(
        &mut self,
        op: BootstrapOperation,
        selected: ClaimIdentity,
    ) -> BootstrapStep {
        if self.stage != BootstrapStage::DonorAvailable
            || self.operation != Some(op)
            || self.selected.as_ref() != Some(&selected)
            || selected.node == self.me
        {
            return Self::reject(BootstrapError::StaleOperation);
        }
        let Some(config) = self.transfer_config else {
            return Self::reject(BootstrapError::Stage);
        };
        let Some(due) = self
            .operation_due
            .zip(self.total_due)
            .map(|(a, b)| a.min(b))
        else {
            return self.terminate();
        };
        if self.now >= due {
            return self.terminate();
        }
        if self
            .observed
            .get(&selected)
            .is_none_or(|claim| claim.expires <= self.now)
        {
            return self.terminate();
        }
        let binding = TransferBinding {
            parent: op,
            scope: self.scope.clone(),
            donor: selected,
            follower: self.identity(),
            due,
        };
        let Ok(child) = TransferSession::new(config, binding, self.now) else {
            return self.terminate();
        };
        self.transfer = Some(child);
        let Some(refresh_due) = self.now.0.checked_add(self.config.observe_ms).map(Time) else {
            return self.terminate();
        };
        self.claim_refresh_due = Some(refresh_due);
        self.stage = BootstrapStage::Transferring;
        self.transfer_event(TransferEvent::Start)
    }

    fn abandon_transfer(&mut self) -> BootstrapStep {
        let mut effects = Vec::new();
        if let Some(op) = self.operation {
            effects.push(BootstrapEffect::CancelWork { op });
        }
        self.operation = None;
        self.operation_due = None;
        effects.extend(self.cancel_transfer());
        if let Some(selected) = self.selected.clone() {
            self.observed.remove(&selected);
            self.excluded.insert(selected);
        }
        if self.total_due.is_some_and(|due| self.now >= due)
            || self.excluded.len() > self.config.max_members
        {
            let fallback = self.terminate();
            effects.extend(fallback.effects);
        } else {
            let next = self.observe();
            effects.extend(next.effects);
        }
        self.ok(effects
            .into_iter()
            .filter(|effect| !matches!(effect, BootstrapEffect::ArmTimer(_)))
            .collect())
    }

    pub(super) fn selected_claim_observed(
        &mut self,
        op: BootstrapOperation,
        claim: Option<BootstrapClaim>,
    ) -> BootstrapStep {
        if self.stage != BootstrapStage::Transferring || self.claim_poll != Some(op) {
            return Self::reject(BootstrapError::StaleOperation);
        }
        self.claim_poll = None;
        self.claim_poll_due = None;
        let Some(claim) = claim else {
            return self.abandon_transfer();
        };
        if self.selected.as_ref() != Some(&claim.identity)
            || claim.phase != ClaimPhase::Ready
            || claim.remaining_ms == 0
            || claim.remaining_ms > self.config.claim_ttl_ms
            || !matches!(self.track_claim(&claim), Ok(true))
        {
            return self.abandon_transfer();
        }
        let Some(due) = self.now.0.checked_add(self.config.observe_ms).map(Time) else {
            return self.abandon_transfer();
        };
        self.claim_refresh_due = Some(due);
        self.ok(Vec::new())
    }

    pub(super) fn transfer_event(&mut self, event: TransferEvent) -> BootstrapStep {
        if self.stage != BootstrapStage::Transferring {
            return Self::reject(BootstrapError::Stage);
        }
        if matches!(event, TransferEvent::Start)
            && self
                .transfer
                .as_ref()
                .is_some_and(|child| child.stage() != TransferStage::Unready)
        {
            return Self::reject(BootstrapError::Stage);
        }
        let Some(mut child) = self.transfer.take() else {
            return Self::reject(BootstrapError::Stage);
        };
        let session = self.session;
        let incarnation = self.boot_incarnation;
        let generation = self.generation;
        let next_token = &mut self.next_token;
        let step = child.step(event, &mut || {
            let token = *next_token;
            *next_token = token.checked_add(1)?;
            Some(BootstrapOperation {
                session,
                incarnation,
                generation,
                token,
            })
        });
        self.finish_transfer(child, step)
    }

    fn finish_transfer(&mut self, child: TransferSession, step: TransferStep) -> BootstrapStep {
        let mut effects: Vec<_> = step
            .effects
            .into_iter()
            .filter(|effect| !matches!(effect, TransferEffect::ArmTimer(_)))
            .map(|effect| BootstrapEffect::Transfer(Box::new(effect)))
            .collect();
        match child.stage() {
            TransferStage::Aborted => {
                if let Some(op) = self.operation.take() {
                    effects.insert(0, BootstrapEffect::CancelWork { op });
                }
                self.operation_due = None;
                self.transfer = None;
                self.claim_refresh_due = None;
                self.claim_poll = None;
                self.claim_poll_due = None;
                if let Some(selected) = self.selected.clone() {
                    self.observed.remove(&selected);
                    self.excluded.insert(selected);
                }
                if self.total_due.is_some_and(|due| self.now >= due)
                    || self.excluded.len() > self.config.max_members
                {
                    let fallback = self.terminate();
                    effects.extend(fallback.effects);
                } else {
                    let next = self.observe();
                    effects.extend(next.effects);
                }
                self.ok(effects
                    .into_iter()
                    .filter(|effect| !matches!(effect, BootstrapEffect::ArmTimer(_)))
                    .collect())
            }
            TransferStage::Completed => {
                self.transfer = None;
                self.claim_refresh_due = None;
                self.claim_poll = None;
                self.claim_poll_due = None;
                self.stage = BootstrapStage::Transferred;
                self.operation = None;
                self.operation_due = None;
                self.follow_due = None;
                self.total_due = None;
                self.renew_due = None;
                effects.push(BootstrapEffect::WithdrawClaim(self.identity()));
                self.ok(effects)
            }
            _ => {
                self.transfer = Some(child);
                if step.rejection.is_some() {
                    return Self::reject(BootstrapError::StaleOperation);
                }
                self.ok(effects)
            }
        }
    }

    pub(super) fn tick_transfer(&mut self, now: Time) -> BootstrapStep {
        if self.total_due.is_some_and(|due| now >= due)
            || self.claim_poll_due.is_some_and(|due| now >= due)
            || self
                .selected
                .as_ref()
                .and_then(|id| self.observed.get(id))
                .is_none_or(|claim| claim.expires <= now)
        {
            return self.abandon_transfer();
        }
        let mut renewal = Vec::new();
        if self.renew_due.is_some_and(|due| now >= due) {
            match self.publish_renewal() {
                Ok(claim) => renewal.push(claim),
                Err(_) => return self.terminate(),
            }
        }
        let mut step = self.transfer_event(TransferEvent::Tick(now));
        if step.rejection.is_none() {
            if self.stage == BootstrapStage::Transferring
                && self.claim_refresh_due.is_some_and(|due| now >= due)
                && self.claim_poll.is_none()
            {
                let Some(op) = self.allocate_token() else {
                    return self.abandon_transfer();
                };
                let Some(due) = now.0.checked_add(self.config.observe_ms).map(Time) else {
                    return self.abandon_transfer();
                };
                self.claim_poll = Some(op);
                self.claim_poll_due = Some(due);
                self.claim_refresh_due = None;
                renewal.push(BootstrapEffect::ObserveSelectedClaim {
                    op,
                    selected: self.selected.clone().expect("transfer selected donor"),
                });
            }
            renewal.append(&mut step.effects);
            return self.ok(renewal
                .into_iter()
                .filter(|effect| !matches!(effect, BootstrapEffect::ArmTimer(_)))
                .collect());
        }
        step
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::types::{BootId, BootstrapConfig, BootstrapEvent, BootstrapScope};
    use super::super::ObservedRenewal;
    use super::*;
    use crate::NodeId;

    #[test]
    fn child_allocator_exhaustion_fences_parent_and_falls_back() {
        let config = BootstrapConfig {
            max_members: 2,
            max_member_bytes: 8,
            max_scope_bytes: 8,
            settle_ms: 2,
            renew_ms: 3,
            claim_ttl_ms: 10,
            observe_ms: 2,
            donor_wait_ms: 9,
            total_ms: 20,
        };
        let mut engine = ClaimEngine::new(
            config,
            BootstrapScope {
                domain: "o".into(),
                partition: "b".into(),
            },
            NodeId::from("me"),
            BootId(1),
            1,
        )
        .unwrap();
        engine
            .enable_transfer(TransferConfig {
                expected_schema: 1,
                max_metadata_bytes: 64,
                max_encoded_bytes: 8,
                max_decoded_bytes: 8,
                max_chunk_bytes: 8,
                max_chunks: 2,
                max_batch_bytes: 8,
                max_batch_events: 2,
                max_replay_events: 4,
                max_native_buffer_bytes: 8,
                max_members: 2,
                max_cuts: 1,
                coverage_poll_ms: 2,
            })
            .unwrap();
        let donor = ClaimIdentity {
            node: NodeId::from("peer"),
            incarnation: BootId(2),
            session: 2,
            attempt: 1,
        };
        let parent = BootstrapOperation {
            session: 1,
            incarnation: BootId(1),
            generation: 1,
            token: 1,
        };
        engine.generation = 1;
        engine.stage = BootstrapStage::DonorAvailable;
        engine.now = Time(1);
        engine.total_due = Some(Time(20));
        engine.operation_due = Some(Time(10));
        engine.operation = Some(parent);
        engine.selected = Some(donor.clone());
        engine.observed.insert(
            donor.clone(),
            ObservedRenewal {
                sequence: 1,
                phase: ClaimPhase::Ready,
                progress: 0,
                expires: Time(10),
            },
        );
        engine.next_token = u64::MAX;
        let failed = engine.step(BootstrapEvent::StartTransfer {
            op: parent,
            selected: donor,
        });
        assert_eq!(failed.rejection, None);
        assert_eq!(engine.stage(), BootstrapStage::Fallback);
        assert!(
            matches!(failed.effects.first(), Some(BootstrapEffect::CancelWork { op }) if *op == parent)
        );
        assert!(failed.effects.iter().any(|effect| matches!(effect,
            BootstrapEffect::Transfer(inner) if matches!(inner.as_ref(), TransferEffect::DiscardStage { .. }))));
        assert!(
            failed
                .effects
                .iter()
                .any(|effect| matches!(effect, BootstrapEffect::FallbackOrigin))
        );
    }
}
