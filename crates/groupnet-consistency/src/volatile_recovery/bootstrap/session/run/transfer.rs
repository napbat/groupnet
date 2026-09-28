//! Exact transfer-effect dispatch without another scheduler or hidden stage map.

use super::super::{AcquisitionBinding, AdmissionClass, BootstrapOperation};
use super::{
    BootstrapEvent, BootstrapOutcome, BootstrapSession, BootstrapStage, ClaimSource, DonorPort,
    Instant, TransferEffect, TransferEvent,
};

impl<C: ClaimSource, D: DonorPort> BootstrapSession<C, D> {
    #[expect(
        clippy::too_many_lines,
        reason = "one serial dispatch preserves transfer event correlation"
    )]
    pub(super) async fn execute_transfer_effect(
        &mut self,
        effect: TransferEffect,
        due: Instant,
    ) -> Option<BootstrapOutcome> {
        if matches!(effect, TransferEffect::ArmTimer(_)) {
            return None;
        }
        let op = effect_operation(&effect);
        let installing = matches!(effect, TransferEffect::InstallCandidate { .. });
        let operation_due = match op {
            Some(op) => self.operation_due(op, due)?,
            None => due,
        };
        // The installed receipt is cloned only after a second, conservative
        // reservation. Reserve before the application could swap a candidate.
        let handoff_charge = if installing {
            let bytes = self
                .config
                .transfer
                .max_metadata_bytes
                .checked_mul(4)
                .and_then(|bytes| bytes.checked_add(256));
            let Some(bytes) = bytes else {
                return Some(BootstrapOutcome::Declined);
            };
            let Ok(reservation) = self.admission.reserve(AdmissionClass::Inflight, bytes) else {
                if let Some(op) = op {
                    let _ =
                        self.accept(BootstrapEvent::Transfer(Box::new(TransferEvent::Failed {
                            op,
                        })));
                }
                return None;
            };
            Some(reservation)
        } else {
            None
        };
        let permit = if installing {
            self.permit
                .as_ref()
                .map(|permit| permit.restricted_to(operation_due))
        } else {
            None
        };
        if installing && permit.as_ref().is_none_or(|permit| !permit.valid()) {
            return Some(BootstrapOutcome::Declined);
        }
        let Some(context) = self.transfer_context.as_ref() else {
            return Some(BootstrapOutcome::Declined);
        };
        let response = tokio::time::timeout_at(
            tokio::time::Instant::from_std(operation_due),
            self.donor.execute(
                context,
                effect,
                &mut self.resources,
                &self.admission,
                permit,
            ),
        )
        .await;
        let _ = self.tick_after_io();
        if op.is_some_and(|op| self.engine.operation_deadline(op).is_none()) {
            return None;
        }
        match response {
            Ok(Ok(Some(event))) => {
                let (accepted, installed) = event.consume(|event| {
                    let installed = match &event {
                        TransferEvent::Installed { handoff, .. } => {
                            let Some(recovery) = self.recovery else {
                                return (false, None);
                            };
                            let Some(parent) = self.child_parent else {
                                return (false, None);
                            };
                            if !(AcquisitionBinding {
                                recovery,
                                child: parent,
                            })
                            .accepts(recovery, handoff)
                            {
                                return (false, None);
                            }
                            Some(handoff.clone())
                        }
                        _ => None,
                    };
                    let accepted = self.accept(BootstrapEvent::Transfer(Box::new(event)));
                    (accepted, installed)
                });
                if !accepted {
                    return Some(BootstrapOutcome::Declined);
                }
                if let Some(handoff) = installed {
                    if self.engine.stage() != BootstrapStage::Transferred {
                        return Some(BootstrapOutcome::Declined);
                    }
                    let Some(charge) = handoff_charge else {
                        return Some(BootstrapOutcome::Declined);
                    };
                    return Some(BootstrapOutcome::PeerInstalled(charge.hold(handoff)));
                }
                None
            }
            Ok(Ok(None)) => None,
            _ => {
                if let Some(op) = op {
                    let _ =
                        self.accept(BootstrapEvent::Transfer(Box::new(TransferEvent::Failed {
                            op,
                        })));
                }
                None
            }
        }
    }
}

fn effect_operation(effect: &TransferEffect) -> Option<BootstrapOperation> {
    match effect {
        TransferEffect::FetchOffer { op, .. }
        | TransferEffect::ReserveStage { op, .. }
        | TransferEffect::ReserveDonor { op, .. }
        | TransferEffect::FetchChunk { op, .. }
        | TransferEffect::VerifyImage { op, .. }
        | TransferEffect::AttachStream { op, .. }
        | TransferEffect::FetchBarrier { op, .. }
        | TransferEffect::AdvanceBarrier { op, .. }
        | TransferEffect::FetchBatch { op, .. }
        | TransferEffect::AckBatch { op, .. }
        | TransferEffect::CheckNativeCoverage { op, .. }
        | TransferEffect::InstallCandidate { op, .. } => Some(*op),
        TransferEffect::DiscardStage { .. }
        | TransferEffect::ReleaseReservation(_)
        | TransferEffect::ArmTimer(_) => None,
    }
}
