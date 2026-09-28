//! Composite claim and child-transfer correlation tests.

use super::journal::{
    AttachToken, BarrierReceipt, CaptureId, JournalCursor, NativeCut, ReservationId,
};
use super::transfer::{
    NativeCoverageReceipt, NativeHandoffReceipt, TransferConfig, TransferEffect, TransferEvent,
};
use super::*;
use crate::volatile_recovery::RecoveryOperation;
use crate::{NodeId, Status, Time, placement};

fn config() -> BootstrapConfig {
    BootstrapConfig {
        max_members: 2,
        max_member_bytes: 16,
        max_scope_bytes: 32,
        settle_ms: 3,
        renew_ms: 4,
        claim_ttl_ms: 12,
        observe_ms: 3,
        donor_wait_ms: 8,
        total_ms: 30,
    }
}

fn transfer_config() -> TransferConfig {
    TransferConfig {
        expected_schema: 1,
        max_metadata_bytes: 128,
        max_encoded_bytes: 16,
        max_decoded_bytes: 16,
        max_chunk_bytes: 8,
        max_chunks: 4,
        max_batch_bytes: 16,
        max_batch_events: 4,
        max_replay_events: 8,
        max_native_buffer_bytes: 16,
        max_members: 2,
        max_cuts: 2,
        coverage_poll_ms: 2,
    }
}

fn scope() -> BootstrapScope {
    BootstrapScope {
        domain: "o".to_owned(),
        partition: "b".to_owned(),
    }
}

fn ready_follower_with_wait(wait: u64) -> (ClaimEngine, BootstrapOperation, ClaimIdentity) {
    let names = [NodeId::from("a"), NodeId::from("b")];
    let donor = placement::owner(&scope().placement_key(), &names.into_iter().collect()).unwrap();
    let follower = if donor.as_str() == "a" { "b" } else { "a" };
    let boot = BootId((1_u128 << 100) + 5);
    let mut limits = config();
    limits.donor_wait_ms = wait;
    let mut engine = ClaimEngine::new(limits, scope(), NodeId::from(follower), boot, 9).unwrap();
    engine.enable_transfer(transfer_config()).unwrap();
    let start = engine.step(BootstrapEvent::Start);
    let local = start
        .effects
        .iter()
        .find_map(|effect| match effect {
            BootstrapEffect::PublishClaim(claim) => Some(claim.clone()),
            _ => None,
        })
        .unwrap();
    let tick = engine.step(BootstrapEvent::Tick(Time(3)));
    let observe = tick
        .effects
        .iter()
        .find_map(|effect| match effect {
            BootstrapEffect::ObserveClaims { op, .. } => Some(*op),
            _ => None,
        })
        .unwrap();
    let selected = ClaimIdentity {
        node: donor.clone(),
        incarnation: BootId((1_u128 << 101) + 7),
        session: 2,
        attempt: 1,
    };
    let donor_claim = BootstrapClaim {
        identity: selected.clone(),
        renewal: 2,
        phase: ClaimPhase::Ready,
        remaining_ms: 9,
    };
    let decision = engine.step(BootstrapEvent::ClaimsObserved {
        op: observe,
        members: ["a", "b"]
            .into_iter()
            .map(|name| BootstrapMember {
                node: NodeId::from(name),
                eligible: true,
            })
            .collect(),
        claims: vec![local, donor_claim],
    });
    let parent = decision
        .effects
        .iter()
        .find_map(|effect| match effect {
            BootstrapEffect::DonorAvailable { op, .. } => Some(*op),
            _ => None,
        })
        .unwrap();
    (engine, parent, selected)
}

fn ready_follower() -> (ClaimEngine, BootstrapOperation, ClaimIdentity) {
    ready_follower_with_wait(8)
}

#[test]
fn transfer_child_preserves_parent_operation_and_original_deadline() {
    let (mut engine, parent, selected) = ready_follower();
    assert_eq!(engine.stage(), BootstrapStage::DonorAvailable);
    let begin = engine.step(BootstrapEvent::StartTransfer {
        op: parent,
        selected,
    });
    assert_eq!(begin.rejection, None);
    let fetch = begin
        .effects
        .iter()
        .find_map(|effect| match effect {
            BootstrapEffect::Transfer(effect) => match effect.as_ref() {
                TransferEffect::FetchOffer { op, .. } => Some(*op),
                _ => None,
            },
            _ => None,
        })
        .unwrap();
    assert_eq!(engine.current_operation(), Some(parent));
    assert_eq!(engine.stage(), BootstrapStage::Transferring);
    assert_eq!(fetch.incarnation, parent.incarnation);
    assert!(fetch.token > parent.token);
    assert_eq!(engine.next_deadline(), Some(Time(4))); // renewal precedes donor due 11
    let tick = engine.step(BootstrapEvent::Tick(Time(7)));
    assert_eq!(tick.rejection, None);
    assert_eq!(engine.current_operation(), Some(parent));
    let expired = engine.step(BootstrapEvent::Tick(Time(11)));
    assert_eq!(expired.rejection, None);
    assert!(
        expired.effects.iter().any(|effect| matches!(effect,
        BootstrapEffect::Transfer(inner) if matches!(inner.as_ref(), TransferEffect::DiscardStage { parent: old } if *old == parent)))
    );
    assert!(expired.effects.iter().any(|effect| matches!(effect,
        BootstrapEffect::CancelWork { op } if *op == parent)));
    assert!(
        expired
            .effects
            .iter()
            .any(|effect| matches!(effect, BootstrapEffect::ObserveClaims { .. }))
    );
    assert_ne!(engine.stage(), BootstrapStage::Transferring);
}

#[test]
fn stale_transfer_reply_after_supersession_cannot_reopen_candidate() {
    let (mut engine, parent, selected) = ready_follower();
    let begin = engine.step(BootstrapEvent::StartTransfer {
        op: parent,
        selected,
    });
    let fetch = begin
        .effects
        .iter()
        .find_map(|effect| match effect {
            BootstrapEffect::Transfer(effect) => match effect.as_ref() {
                TransferEffect::FetchOffer { op, .. } => Some(*op),
                _ => None,
            },
            _ => None,
        })
        .unwrap();
    let restart = engine.step(BootstrapEvent::Start);
    assert!(
        matches!(restart.effects.first(), Some(BootstrapEffect::CancelWork { op }) if *op == parent)
    );
    assert!(
        restart.effects.iter().any(|effect| matches!(effect,
        BootstrapEffect::Transfer(inner) if matches!(inner.as_ref(), TransferEffect::DiscardStage { parent: old } if *old == parent)))
    );
    assert!(restart.effects.iter().any(|effect| matches!(effect,
        BootstrapEffect::CancelWork { op } if *op == parent)));
    let stale = engine.step(BootstrapEvent::Transfer(Box::new(TransferEvent::Failed {
        op: fetch,
    })));
    assert_eq!(stale.rejection, Some(BootstrapError::Stage));
    assert_eq!(engine.stage(), BootstrapStage::Settling);
}

#[test]
fn selected_claim_refresh_is_correlated_and_does_not_replace_transfer_parent() {
    let (mut engine, parent, selected) = ready_follower();
    engine.step(BootstrapEvent::StartTransfer {
        op: parent,
        selected: selected.clone(),
    });
    let poll = engine.step(BootstrapEvent::Tick(Time(7)));
    let refresh = poll
        .effects
        .iter()
        .find_map(|effect| match effect {
            BootstrapEffect::ObserveSelectedClaim { op, selected: id } if id == &selected => {
                Some(*op)
            }
            _ => None,
        })
        .unwrap();
    assert_ne!(refresh, parent);
    assert_eq!(engine.current_operation(), Some(parent));
    let accepted = engine.step(BootstrapEvent::SelectedClaimObserved {
        op: refresh,
        claim: Some(BootstrapClaim {
            identity: selected.clone(),
            renewal: 3,
            phase: ClaimPhase::Ready,
            remaining_ms: 9,
        }),
    });
    assert_eq!(accepted.rejection, None);
    assert_eq!(engine.stage(), BootstrapStage::Transferring);
    let stale = engine.step(BootstrapEvent::SelectedClaimObserved {
        op: refresh,
        claim: None,
    });
    assert_eq!(stale.rejection, Some(BootstrapError::StaleOperation));
    assert_eq!(engine.stage(), BootstrapStage::Transferring);
    let next = engine.step(BootstrapEvent::Tick(Time(10)));
    assert!(
        next.effects
            .iter()
            .any(|effect| matches!(effect, BootstrapEffect::ObserveSelectedClaim { .. }))
    );
}

#[test]
fn repeated_old_renewal_cannot_extend_selected_claim_lifetime() {
    let (mut engine, parent, selected) = ready_follower_with_wait(20);
    engine.step(BootstrapEvent::StartTransfer {
        op: parent,
        selected: selected.clone(),
    });
    let poll = engine.step(BootstrapEvent::Tick(Time(7)));
    let refresh = poll
        .effects
        .iter()
        .find_map(|effect| match effect {
            BootstrapEffect::ObserveSelectedClaim { op, .. } => Some(*op),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        engine
            .step(BootstrapEvent::SelectedClaimObserved {
                op: refresh,
                claim: Some(BootstrapClaim {
                    identity: selected,
                    renewal: 2,
                    phase: ClaimPhase::Ready,
                    remaining_ms: 9,
                }),
            })
            .rejection,
        None
    );
    let expired = engine.step(BootstrapEvent::Tick(Time(12)));
    assert_eq!(expired.rejection, None);
    assert!(expired.effects.iter().any(|effect| matches!(effect,
        BootstrapEffect::Transfer(inner) if matches!(inner.as_ref(), TransferEffect::DiscardStage { .. }))));
    assert_ne!(engine.stage(), BootstrapStage::Transferring);
}

fn child_effect(engine: &mut ClaimEngine, event: TransferEvent) -> TransferEffect {
    let step = engine.step(BootstrapEvent::Transfer(Box::new(event)));
    assert_eq!(step.rejection, None);
    step.effects
        .into_iter()
        .find_map(|effect| match effect {
            BootstrapEffect::Transfer(inner)
                if !matches!(inner.as_ref(), TransferEffect::ArmTimer(_)) =>
            {
                Some(*inner)
            }
            _ => None,
        })
        .unwrap()
}

#[expect(
    clippy::too_many_lines,
    reason = "the helper advances each exact private transfer callback to an outstanding install"
)]
fn installing() -> (
    ClaimEngine,
    BootstrapOperation,
    BootstrapOperation,
    NativeHandoffReceipt,
) {
    let (mut engine, parent, donor) = ready_follower_with_wait(20);
    let follower = ClaimIdentity {
        node: NodeId::from(if donor.node.as_str() == "a" { "b" } else { "a" }),
        incarnation: parent.incarnation,
        session: parent.session,
        attempt: parent.generation,
    };
    let capture = CaptureId {
        scope: scope(),
        donor: donor.clone(),
        recovery_generation: 1,
        serial: 1,
    };
    let cursor = JournalCursor {
        capture: capture.clone(),
        position: 0,
    };
    let reservation = ReservationId {
        capture: capture.clone(),
        follower: follower.clone(),
        serial: 1,
    };
    let mut members = vec![donor.clone(), follower]
        .into_iter()
        .map(|claim| BootstrapMemberIdentity {
            node: claim.node.clone(),
            presence: Some(PresenceIdentity {
                node: claim.node,
                boot: claim.incarnation,
                session: claim.session,
            }),
            member_incarnation: 1,
            status: Status::Alive,
        })
        .collect::<Vec<_>>();
    members.sort_by(|a, b| a.node.cmp(&b.node));
    let cuts = vec![NativeCut {
        writer: b"w".to_vec(),
        epoch: 1,
        sequence: 0,
    }];
    let offer = super::transfer::TransferOffer {
        capture,
        image_cut: cursor.clone(),
        schema: 1,
        encoded_bytes: 1,
        decoded_bytes: 1,
        chunks: 1,
        commitment: [8; 32],
        members: members.clone(),
        cuts: cuts.clone(),
    };
    let begin = engine.step(BootstrapEvent::StartTransfer {
        op: parent,
        selected: donor,
    });
    let fetch = begin
        .effects
        .into_iter()
        .find_map(|effect| match effect {
            BootstrapEffect::Transfer(inner) => match *inner {
                TransferEffect::FetchOffer { op, .. } => Some(op),
                _ => None,
            },
            _ => None,
        })
        .unwrap();
    let TransferEffect::ReserveStage { op: stage, .. } =
        child_effect(&mut engine, TransferEvent::Offered { op: fetch, offer })
    else {
        panic!("stage")
    };
    let TransferEffect::ReserveDonor { op: reserve, .. } =
        child_effect(&mut engine, TransferEvent::StageReserved { op: stage })
    else {
        panic!("reserve")
    };
    let TransferEffect::FetchChunk { op: chunk, .. } = child_effect(
        &mut engine,
        TransferEvent::DonorReserved {
            op: reserve,
            reservation: reservation.clone(),
        },
    ) else {
        panic!("chunk")
    };
    let TransferEffect::VerifyImage { op: verify, .. } = child_effect(
        &mut engine,
        TransferEvent::ChunkStored {
            op: chunk,
            sequence: 0,
            bytes: 1,
            decoded_charge: 1,
        },
    ) else {
        panic!("verify")
    };
    let TransferEffect::AttachStream { op: attach, .. } = child_effect(
        &mut engine,
        TransferEvent::ImageVerified {
            op: verify,
            commitment: [8; 32],
        },
    ) else {
        panic!("attach")
    };
    let TransferEffect::FetchBarrier { op: barrier_op, .. } = child_effect(
        &mut engine,
        TransferEvent::StreamAttached {
            op: attach,
            token: AttachToken {
                reservation: reservation.clone(),
                operation: 1,
            },
        },
    ) else {
        panic!("barrier")
    };
    let barrier = BarrierReceipt {
        reservation,
        attach_operation: 1,
        barrier_operation: 2,
        cursor,
        covered_cuts: cuts,
        members,
    };
    let TransferEffect::CheckNativeCoverage { op: cover, .. } = child_effect(
        &mut engine,
        TransferEvent::BarrierReceived {
            op: barrier_op,
            receipt: barrier.clone(),
        },
    ) else {
        panic!("native coverage")
    };
    let coverage = NativeCoverageReceipt {
        parent,
        staged_through: barrier.cursor.clone(),
        proven_cuts: barrier.covered_cuts.clone(),
        members: barrier.members.clone(),
        buffered_bytes: 0,
        barrier,
    };
    let TransferEffect::InstallCandidate { op: install, .. } = child_effect(
        &mut engine,
        TransferEvent::NativeCovered {
            op: cover,
            coverage: coverage.clone(),
        },
    ) else {
        panic!("install")
    };
    let handoff = NativeHandoffReceipt {
        recovery: RecoveryOperation {
            session: 1,
            generation: 1,
            token: 1,
        },
        install,
        attachment: AttachToken {
            reservation: coverage.barrier.reservation.clone(),
            operation: 1,
        },
        schema: 1,
        applier_generation: 1,
        continued_cuts: coverage.proven_cuts.clone(),
        buffered_bytes: 0,
        coverage,
    };
    (engine, parent, install, handoff)
}

#[test]
fn cancel_or_restart_during_install_fences_old_callback_before_cleanup() {
    for terminal in [false, true] {
        let (mut engine, parent, install, handoff) = installing();
        let event = if terminal {
            BootstrapEvent::Cancel
        } else {
            BootstrapEvent::Start
        };
        let stopped = engine.step(event);
        assert!(
            matches!(stopped.effects.first(), Some(BootstrapEffect::CancelWork { op }) if *op == parent)
        );
        assert!(stopped.effects.iter().any(|effect| matches!(effect,
            BootstrapEffect::Transfer(inner) if matches!(inner.as_ref(), TransferEffect::DiscardStage { .. }))));
        assert!(stopped.effects.iter().any(|effect| matches!(effect,
            BootstrapEffect::Transfer(inner) if matches!(inner.as_ref(), TransferEffect::ReleaseReservation(_)))));
        let late = engine.step(BootstrapEvent::Transfer(Box::new(
            TransferEvent::Installed {
                op: install,
                handoff: Box::new(handoff),
            },
        )));
        assert_eq!(late.rejection, Some(BootstrapError::Stage));
        assert_eq!(
            engine.stage(),
            if terminal {
                BootstrapStage::Cancelled
            } else {
                BootstrapStage::Settling
            }
        );
    }
}
