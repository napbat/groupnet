//! Exact peer-candidate continuity stays separate from serving authority.

use super::*;
use crate::volatile_bootstrap::journal::{
    AttachToken, BarrierReceipt, CaptureId, JournalCursor, NativeCut, ReservationId,
};
use crate::volatile_bootstrap::transfer::{NativeCoverageReceipt, NativeHandoffReceipt};
use crate::volatile_bootstrap::{
    BootId, BootstrapMemberIdentity, BootstrapOperation, BootstrapScope, ClaimIdentity,
    PresenceIdentity,
};
use crate::{NodeId, Status, Time};

fn config() -> RecoveryConfig {
    RecoveryConfig {
        max_members: 2,
        max_member_bytes: 16,
        max_barrier_rounds: 2,
        total_ms: 30,
        attempt_ms: 5,
        settle_ms: 2,
        poll_ms: 1,
    }
}

fn identity(node: &str, boot: u128, session: u64) -> ClaimIdentity {
    ClaimIdentity {
        node: NodeId::from(node),
        incarnation: BootId(boot),
        session,
        attempt: 1,
    }
}

fn members() -> Vec<BootstrapMemberIdentity> {
    [identity("a", 8, 5), identity("me", 7, 9)]
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
        .collect()
}

fn handoff(recovery: RecoveryOperation) -> NativeHandoffReceipt {
    let donor = identity("a", 8, 5);
    let follower = identity("me", 7, 9);
    let capture = CaptureId {
        scope: BootstrapScope {
            domain: "o".to_owned(),
            partition: "b".to_owned(),
        },
        donor,
        recovery_generation: 1,
        serial: 1,
    };
    let reservation = ReservationId {
        capture: capture.clone(),
        follower,
        serial: 1,
    };
    let barrier = BarrierReceipt {
        reservation: reservation.clone(),
        attach_operation: 1,
        barrier_operation: 2,
        cursor: JournalCursor {
            capture,
            position: 0,
        },
        covered_cuts: vec![NativeCut {
            writer: b"w".to_vec(),
            epoch: 1,
            sequence: 0,
        }],
        members: members(),
    };
    let parent = BootstrapOperation {
        session: 9,
        incarnation: BootId(7),
        generation: 1,
        token: 1,
    };
    let coverage = NativeCoverageReceipt {
        parent,
        barrier: barrier.clone(),
        staged_through: barrier.cursor.clone(),
        proven_cuts: barrier.covered_cuts.clone(),
        members: members(),
        buffered_bytes: 0,
    };
    NativeHandoffReceipt {
        recovery,
        install: BootstrapOperation { token: 2, ..parent },
        coverage,
        attachment: AttachToken {
            reservation,
            operation: 1,
        },
        schema: 1,
        applier_generation: 1,
        continued_cuts: barrier.covered_cuts,
        buffered_bytes: 0,
    }
}

fn operation(step: &RecoveryStep) -> RecoveryOperation {
    step.effects
        .iter()
        .find_map(|effect| match effect {
            RecoveryEffect::Invalidate { op, .. }
            | RecoveryEffect::AcquireBaseline { op }
            | RecoveryEffect::RebuildOrigin { op }
            | RecoveryEffect::ObservePeers { op }
            | RecoveryEffect::ObservePeerHeads { op }
            | RecoveryEffect::WaitFrontiers { op, .. }
            | RecoveryEffect::Affirm { op } => Some(*op),
            RecoveryEffect::CloseGate { .. }
            | RecoveryEffect::CancelBaseline { .. }
            | RecoveryEffect::SuspendLocalBaseline { .. }
            | RecoveryEffect::ResumeLocalBaseline { .. }
            | RecoveryEffect::FellBack { .. }
            | RecoveryEffect::ArmTimer(_) => None,
        })
        .expect("one current operation")
}

fn bootstrap() -> (RecoveryEngine, RecoveryOperation) {
    let mut engine = RecoveryEngine::new(config(), RecoveryMode::Leased, NodeId::from("me"), 7)
        .unwrap()
        .with_bootstrap()
        .unwrap();
    let started = engine.step(RecoveryEvent::Start);
    let acquire = engine.step(RecoveryEvent::Invalidated {
        op: operation(&started),
    });
    assert_eq!(engine.state().stage, RecoveryStage::AcquiringBaseline);
    assert_eq!(engine.next_deadline(), Some(Time(30)));
    (engine, operation(&acquire))
}

fn quiet_peer() -> Peer {
    Peer {
        node: NodeId::from("a"),
        alive: true,
        grants_lease: true,
        old_nonlive: false,
        grant: None,
        head: None,
        renewal: None,
    }
}

fn renewing_peer(sequence: u64) -> Peer {
    Peer {
        node: NodeId::from("a"),
        alive: true,
        grants_lease: true,
        old_nonlive: false,
        grant: Some(Mark { epoch: 1, sequence }),
        head: Some(Mark {
            epoch: 1,
            sequence: 1,
        }),
        renewal: None,
    }
}

#[test]
fn affirmed_local_lapse_resumes_only_the_exact_suspended_baseline() {
    let (mut engine, acquire) = bootstrap();
    let initial = engine.step(RecoveryEvent::LocalBaselineBuilt { op: acquire });
    engine.step(RecoveryEvent::Affirmed {
        op: operation(&initial),
        accepted: true,
    });
    let lapse = engine.step(RecoveryEvent::LeaseLapse { count: 1 });
    assert!(lapse.effects.iter().any(|effect| matches!(
        effect,
        RecoveryEffect::SuspendLocalBaseline { op } if *op == acquire
    )));
    assert!(!lapse.effects.iter().any(|effect| matches!(
        effect,
        RecoveryEffect::CancelBaseline { .. } | RecoveryEffect::RebuildOrigin { .. }
    )));
    let first = engine.step(RecoveryEvent::Invalidated {
        op: operation(&lapse),
    });
    let renew = engine.step(RecoveryEvent::PeersObserved {
        op: operation(&first),
        peers: vec![renewing_peer(1)],
        confirmed: Some(Mark {
            epoch: 1,
            sequence: 1,
        }),
    });
    let settled = engine.step(RecoveryEvent::PeersObserved {
        op: operation(&renew),
        peers: vec![renewing_peer(2)],
        confirmed: Some(Mark {
            epoch: 1,
            sequence: 2,
        }),
    });
    assert!(
        settled
            .effects
            .iter()
            .any(|effect| matches!(effect, RecoveryEffect::ArmTimer(Time(2))))
    );
    let heads = engine.step(RecoveryEvent::Tick(Time(2)));
    let barrier = engine.step(RecoveryEvent::PeersObserved {
        op: operation(&heads),
        peers: vec![renewing_peer(2)],
        confirmed: Some(Mark {
            epoch: 1,
            sequence: 2,
        }),
    });
    let recheck = engine.step(RecoveryEvent::FrontiersReached {
        op: operation(&barrier),
    });
    let affirm = engine.step(RecoveryEvent::PeersObserved {
        op: operation(&recheck),
        peers: vec![renewing_peer(2)],
        confirmed: Some(Mark {
            epoch: 1,
            sequence: 2,
        }),
    });
    assert_eq!(engine.state().stage, RecoveryStage::Affirming);
    assert_eq!(
        engine
            .step(RecoveryEvent::Affirmed {
                op: acquire,
                accepted: true,
            })
            .rejection,
        Some(RecoveryError::StaleOperation)
    );
    let resumed = engine.step(RecoveryEvent::Affirmed {
        op: operation(&affirm),
        accepted: true,
    });
    assert!(resumed.effects.iter().any(|effect| matches!(
        effect,
        RecoveryEffect::ResumeLocalBaseline { previous, current }
            if *previous == acquire && current.generation == 2 && current.token > acquire.token
    )));
    assert!(engine.state().recovered);
    assert!(!resumed.effects.iter().any(|effect| matches!(
        effect,
        RecoveryEffect::RebuildOrigin { .. } | RecoveryEffect::AcquireBaseline { .. }
    )));
    let gap = engine.step(RecoveryEvent::FeedGap { lapses: 1 });
    assert!(gap.effects.iter().any(|effect| matches!(
        effect,
        RecoveryEffect::CancelBaseline { op } if op.generation == 2
    )));
}

#[test]
fn interrupted_lapse_cancels_suspended_child_without_resume() {
    let (mut engine, acquire) = bootstrap();
    let initial = engine.step(RecoveryEvent::LocalBaselineBuilt { op: acquire });
    engine.step(RecoveryEvent::Affirmed {
        op: operation(&initial),
        accepted: true,
    });
    engine.step(RecoveryEvent::LeaseLapse { count: 1 });
    let full = engine.step(RecoveryEvent::FeedGap { lapses: 1 });
    assert!(full.effects.iter().any(|effect| matches!(
        effect,
        RecoveryEffect::CancelBaseline { op } if *op == acquire
    )));
    assert!(
        !full
            .effects
            .iter()
            .any(|effect| matches!(effect, RecoveryEffect::ResumeLocalBaseline { .. }))
    );
}

#[test]
fn empty_native_feed_handoff_waits_peer_barrier_then_affirms() {
    let (mut engine, acquire) = bootstrap();
    let sample = engine.step(RecoveryEvent::PeerBaselineInstalled {
        op: acquire,
        handoff: Box::new(handoff(acquire)),
    });
    assert_eq!(engine.state().stage, RecoveryStage::PeerSamplingHeads);
    assert!(!engine.state().recovered);
    let barrier = engine.step(RecoveryEvent::PeerHeadsObserved {
        op: operation(&sample),
        peers: vec![quiet_peer()],
        identities: members(),
    });
    assert_eq!(engine.state().stage, RecoveryStage::PeerWaitingFrontiers);
    assert!(barrier.effects.iter().any(|effect| matches!(
        effect,
        RecoveryEffect::WaitFrontiers { heads, .. } if heads.is_empty()
    )));
    let recheck = engine.step(RecoveryEvent::FrontiersReached {
        op: operation(&barrier),
    });
    let affirm = engine.step(RecoveryEvent::PeerHeadsObserved {
        op: operation(&recheck),
        peers: vec![quiet_peer()],
        identities: members(),
    });
    assert_eq!(engine.state().stage, RecoveryStage::Affirming);
    assert!(!engine.state().recovered);
    engine.step(RecoveryEvent::Affirmed {
        op: operation(&affirm),
        accepted: true,
    });
    assert_eq!(engine.state().stage, RecoveryStage::Ready);
}

#[test]
fn stale_or_mismatched_handoff_never_releases_peer_to_serving() {
    for variant in 0..3 {
        let (mut engine, acquire) = bootstrap();
        let mut invalid = handoff(acquire);
        match variant {
            0 => invalid.continued_cuts[0].writer = b"v".to_vec(),
            1 => {
                invalid.coverage.proven_cuts.clear();
                invalid.continued_cuts.clear();
            }
            _ => invalid.recovery.token -= 1,
        }
        let refused = engine.step(RecoveryEvent::PeerBaselineInstalled {
            op: acquire,
            handoff: Box::new(invalid),
        });
        assert!(matches!(
            refused.effects.first(),
            Some(RecoveryEffect::CancelBaseline { op }) if *op == acquire
        ));
        assert!(
            refused
                .effects
                .iter()
                .any(|effect| matches!(effect, RecoveryEffect::RebuildOrigin { .. }))
        );
        assert_eq!(engine.state().stage, RecoveryStage::Rebuilding);
        assert!(!engine.state().recovered);
    }
}

#[test]
fn old_child_receipt_repackaged_under_new_acquisition_is_rejected() {
    let (mut engine, first) = bootstrap();
    let old = handoff(first);
    let supersede = engine.step(RecoveryEvent::FeedGap { lapses: 1 });
    let next = engine.step(RecoveryEvent::Invalidated {
        op: operation(&supersede),
    });
    let current = operation(&next);
    assert_ne!(first, current);
    let refused = engine.step(RecoveryEvent::PeerBaselineInstalled {
        op: current,
        handoff: Box::new(old),
    });
    assert_eq!(engine.state().stage, RecoveryStage::Rebuilding);
    assert!(!engine.state().recovered);
    assert!(matches!(
        refused.effects.first(),
        Some(RecoveryEffect::CancelBaseline { op }) if *op == current
    ));
    assert!(
        refused
            .effects
            .iter()
            .any(|effect| matches!(effect, RecoveryEffect::RebuildOrigin { .. }))
    );
}

#[test]
fn changed_boot_identity_or_lost_peer_falls_back_with_original_deadline() {
    let (mut engine, acquire) = bootstrap();
    let sample = engine.step(RecoveryEvent::PeerBaselineInstalled {
        op: acquire,
        handoff: Box::new(handoff(acquire)),
    });
    let barrier = engine.step(RecoveryEvent::PeerHeadsObserved {
        op: operation(&sample),
        peers: vec![quiet_peer()],
        identities: members(),
    });
    let recheck = engine.step(RecoveryEvent::FrontiersReached {
        op: operation(&barrier),
    });
    let mut changed = members();
    changed[0].presence.as_mut().unwrap().boot = BootId(10);
    let fallback = engine.step(RecoveryEvent::PeerHeadsObserved {
        op: operation(&recheck),
        peers: vec![quiet_peer()],
        identities: changed,
    });
    assert_eq!(engine.state().stage, RecoveryStage::Rebuilding);
    assert_eq!(engine.state().generation, acquire.generation);
    assert_eq!(engine.next_deadline(), Some(Time(5)));
    assert!(
        fallback
            .effects
            .iter()
            .any(|effect| matches!(effect, RecoveryEffect::RebuildOrigin { .. }))
    );
    let old = engine.step(RecoveryEvent::PeerHeadsObserved {
        op: operation(&recheck),
        peers: vec![quiet_peer()],
        identities: members(),
    });
    assert_eq!(old.rejection, Some(RecoveryError::StaleOperation));
}

#[test]
fn local_builder_affirms_without_second_scan_and_cancels_on_next_gap() {
    let (mut engine, acquire) = bootstrap();
    let built = engine.step(RecoveryEvent::LocalBaselineBuilt { op: acquire });
    assert_eq!(engine.state().stage, RecoveryStage::Affirming);
    assert!(
        built
            .effects
            .iter()
            .any(|effect| matches!(effect, RecoveryEffect::Affirm { .. }))
    );
    assert!(!built.effects.iter().any(|effect| matches!(
        effect,
        RecoveryEffect::RebuildOrigin { .. } | RecoveryEffect::CancelBaseline { .. }
    )));
    engine.step(RecoveryEvent::Affirmed {
        op: operation(&built),
        accepted: true,
    });
    assert_eq!(engine.state().stage, RecoveryStage::Ready);
    let gap = engine.step(RecoveryEvent::FeedGap { lapses: 1 });
    assert!(matches!(
        gap.effects.first(),
        Some(RecoveryEffect::CloseGate { .. })
    ));
    assert!(matches!(
        gap.effects.get(1),
        Some(RecoveryEffect::CancelBaseline { op }) if *op == acquire
    ));
    assert!(!engine.state().recovered);
}

#[test]
fn cancelled_acquisition_revokes_child_before_any_late_completion() {
    let (mut engine, acquire) = bootstrap();
    let cancel = engine.step(RecoveryEvent::Cancel);
    assert!(matches!(
        cancel.effects.first(),
        Some(RecoveryEffect::CloseGate { .. })
    ));
    assert!(matches!(
        cancel.effects.get(1),
        Some(RecoveryEffect::CancelBaseline { op }) if *op == acquire
    ));
    assert_eq!(
        engine
            .step(RecoveryEvent::LocalBaselineBuilt { op: acquire })
            .rejection,
        Some(RecoveryError::StaleOperation)
    );
    assert_eq!(engine.state().stage, RecoveryStage::Cancelled);
}

#[test]
fn failed_claim_cancels_child_and_origin_fallback_keeps_original_total() {
    let (mut engine, acquire) = bootstrap();
    engine.step(RecoveryEvent::Tick(Time(5)));
    let failed = engine.step(RecoveryEvent::Failed { op: acquire });
    assert!(matches!(
        failed.effects.first(),
        Some(RecoveryEffect::CancelBaseline { op }) if *op == acquire
    ));
    assert!(
        failed
            .effects
            .iter()
            .any(|effect| matches!(effect, RecoveryEffect::RebuildOrigin { .. }))
    );
    assert!(failed.effects.contains(&RecoveryEffect::FellBack {
        from: RecoveryStage::AcquiringBaseline,
        reason: RecoveryFallback::OperationFailed,
    }));
    assert_eq!(engine.state().stage, RecoveryStage::Rebuilding);
    assert_eq!(engine.state().generation, acquire.generation);
    assert_eq!(engine.next_deadline(), Some(Time(10)));
    assert_eq!(
        engine
            .step(RecoveryEvent::LocalBaselineBuilt { op: acquire })
            .rejection,
        Some(RecoveryError::StaleOperation)
    );
    engine.step(RecoveryEvent::Tick(Time(30)));
    assert_eq!(engine.state().stage, RecoveryStage::OriginOnly);
    assert_eq!(engine.next_deadline(), None);
}

#[test]
fn total_deadline_cancels_slow_transfer_without_starting_another_episode() {
    let (mut engine, acquire) = bootstrap();
    let expired = engine.step(RecoveryEvent::Tick(Time(30)));
    assert!(matches!(
        expired.effects.first(),
        Some(RecoveryEffect::CancelBaseline { op }) if *op == acquire
    ));
    assert!(!expired.effects.iter().any(|effect| matches!(
        effect,
        RecoveryEffect::AcquireBaseline { .. } | RecoveryEffect::RebuildOrigin { .. }
    )));
    assert!(expired.effects.contains(&RecoveryEffect::FellBack {
        from: RecoveryStage::AcquiringBaseline,
        reason: RecoveryFallback::EpisodeExpired,
    }));
    assert_eq!(engine.state().stage, RecoveryStage::OriginOnly);
    assert_eq!(engine.state().generation, acquire.generation);
    assert!(!engine.state().recovered);
}
