//! Seeded peer-head and native-handoff schedules for volatile recovery.

use std::collections::VecDeque;

use groupnet_core::volatile_bootstrap::journal::{
    AttachToken, BarrierReceipt, CaptureId, JournalCursor, NativeCut, ReservationId,
};
use groupnet_core::volatile_bootstrap::transfer::{NativeCoverageReceipt, NativeHandoffReceipt};
use groupnet_core::volatile_bootstrap::{
    BootId, BootstrapOperation, BootstrapScope, ClaimIdentity,
};
use groupnet_core::volatile_recovery::{
    Mark, Peer, RecoveryConfig, RecoveryEffect, RecoveryEngine, RecoveryEvent, RecoveryMode,
    RecoveryOperation, RecoveryStage,
};
use groupnet_core::{NodeId, Time};
use groupnet_sim::SplitMix64;

fn identity(node: &str, boot: u128, session: u64) -> ClaimIdentity {
    ClaimIdentity {
        node: NodeId::from(node),
        incarnation: BootId(boot),
        session,
        attempt: 1,
    }
}

fn members() -> Vec<ClaimIdentity> {
    vec![identity("a", 8, 5), identity("me", 7, 9)]
}

fn handoff(recovery: RecoveryOperation) -> NativeHandoffReceipt {
    let capture = CaptureId {
        scope: BootstrapScope {
            domain: "o".to_owned(),
            partition: "b".to_owned(),
        },
        donor: identity("a", 8, 5),
        recovery_generation: 1,
        serial: 1,
    };
    let reservation = ReservationId {
        capture: capture.clone(),
        follower: identity("me", 7, 9),
        serial: 1,
    };
    let cut = NativeCut {
        writer: b"w".to_vec(),
        epoch: 1,
        sequence: 0,
    };
    let barrier = BarrierReceipt {
        reservation: reservation.clone(),
        attach_operation: 1,
        barrier_operation: 2,
        cursor: JournalCursor {
            capture,
            position: 0,
        },
        covered_cuts: vec![cut.clone()],
        members: members(),
    };
    let parent = BootstrapOperation {
        session: 9,
        incarnation: BootId(7),
        generation: 1,
        token: 1,
    };
    NativeHandoffReceipt {
        recovery,
        install: BootstrapOperation { token: 2, ..parent },
        coverage: NativeCoverageReceipt {
            parent,
            staged_through: barrier.cursor.clone(),
            proven_cuts: vec![cut.clone()],
            members: members(),
            buffered_bytes: 0,
            barrier,
        },
        attachment: AttachToken {
            reservation,
            operation: 1,
        },
        schema: 1,
        applier_generation: 1,
        continued_cuts: vec![cut],
        buffered_bytes: 0,
    }
}

fn peer(sequence: Option<u64>) -> Peer {
    Peer {
        node: NodeId::from("a"),
        alive: true,
        grants_lease: true,
        old_nonlive: false,
        grant: None,
        head: sequence.map(|sequence| Mark { epoch: 1, sequence }),
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one shaped peer-barrier schedule keeps its transition assertions together"
)]
fn peer_handoff_shaped_barrier_faults_keep_reads_closed_until_independent_affirmation() {
    let mut peer_ready = 0;
    let mut origin_fallback = 0;
    let mut changed_boots = 0;
    let mut timed_out_barriers = 0;
    let mut head_moves = 0;
    for seed in 1..=48 {
        let mut rng = SplitMix64::new(seed);
        let fault = seed % 3;
        let move_head = fault == 0 && rng.below(2) == 0;
        let mut engine = RecoveryEngine::new(
            RecoveryConfig {
                max_members: 2,
                max_member_bytes: 16,
                max_barrier_rounds: 3,
                total_ms: 30,
                attempt_ms: 5,
                settle_ms: 1,
                poll_ms: 1,
            },
            RecoveryMode::Leased,
            NodeId::from("me"),
            seed,
        )
        .unwrap()
        .with_bootstrap()
        .unwrap();
        let mut effects = VecDeque::from(engine.step(RecoveryEvent::Start).effects);
        let mut sample_count = 0;
        let mut applied_head = 0;
        let mut head = u64::from(seed % 4 != 0);
        let mut steps = 0;
        while let Some(effect) = effects.pop_front() {
            steps += 1;
            assert!(steps < 30, "seed {seed}: unbounded work");
            let response = match effect {
                RecoveryEffect::CloseGate { .. } => {
                    assert!(!engine.state().recovered, "seed {seed}");
                    None
                }
                RecoveryEffect::CancelBaseline { .. } => None,
                RecoveryEffect::Invalidate { op, .. } => Some(RecoveryEvent::Invalidated { op }),
                RecoveryEffect::AcquireBaseline { op } => {
                    Some(RecoveryEvent::PeerBaselineInstalled {
                        op,
                        handoff: Box::new(handoff(op)),
                    })
                }
                RecoveryEffect::ObservePeerHeads { op } => {
                    sample_count += 1;
                    let mut identities = members();
                    if fault == 1 && sample_count == 2 {
                        identities[0].incarnation = BootId(10);
                        changed_boots += 1;
                    }
                    Some(RecoveryEvent::PeerHeadsObserved {
                        op,
                        peers: vec![peer((head != 0).then_some(head))],
                        identities,
                    })
                }
                RecoveryEffect::WaitFrontiers { op, heads } => {
                    if fault == 2 {
                        timed_out_barriers += 1;
                        Some(RecoveryEvent::Tick(Time(5)))
                    } else {
                        for (_, mark) in heads {
                            applied_head = applied_head.max(mark.sequence);
                        }
                        if move_head && sample_count == 1 {
                            head = 2;
                            head_moves += 1;
                        }
                        Some(RecoveryEvent::FrontiersReached { op })
                    }
                }
                RecoveryEffect::RebuildOrigin { op } => {
                    origin_fallback += 1;
                    Some(RecoveryEvent::Materialized { op })
                }
                RecoveryEffect::Affirm { op } => {
                    assert!(
                        !engine.state().recovered,
                        "seed {seed}: premature read gate"
                    );
                    if fault == 0 {
                        assert!(applied_head >= head, "seed {seed}: missed head");
                        peer_ready += 1;
                    }
                    Some(RecoveryEvent::Affirmed { op, accepted: true })
                }
                RecoveryEffect::ArmTimer(due) => {
                    assert!(due <= Time(30), "seed {seed}: extended total deadline");
                    None
                }
                RecoveryEffect::ObservePeers { .. } => {
                    panic!("seed {seed}: peer path took cheap-lapse observation")
                }
            };
            if let Some(response) = response {
                let step = engine.step(response);
                assert_eq!(step.rejection, None, "seed {seed}");
                effects.extend(step.effects);
            }
        }
        assert_eq!(engine.state().stage, RecoveryStage::Ready, "seed {seed}");
    }
    assert_eq!(peer_ready, 16);
    assert_eq!(origin_fallback, 32);
    assert_eq!(changed_boots, 16);
    assert_eq!(timed_out_barriers, 16);
    assert!(head_moves > 0);
}

#[derive(Clone)]
struct Scheduled {
    at: u64,
    event: RecoveryEvent,
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one queued callback fault schedule keeps its safety and liveness floors together"
)]
fn delayed_duplicate_and_lost_peer_callbacks_cannot_reopen_a_superseded_image() {
    let mut healed = 0;
    let mut cancelled = 0;
    let mut stale = 0;
    let mut dropped = 0;
    let mut fallback = 0;
    let mut stale_binding = 0;
    for seed in 1..=60 {
        let family = seed % 6;
        let mut rng = SplitMix64::new(seed);
        let mut engine = RecoveryEngine::new(
            RecoveryConfig {
                max_members: 2,
                max_member_bytes: 16,
                max_barrier_rounds: 2,
                total_ms: 30,
                attempt_ms: 5,
                settle_ms: 1,
                poll_ms: 1,
            },
            RecoveryMode::Leased,
            NodeId::from("me"),
            seed,
        )
        .unwrap()
        .with_bootstrap()
        .unwrap();
        let mut effects = VecDeque::from(engine.step(RecoveryEvent::Start).effects);
        let mut scheduled = Vec::<Scheduled>::new();
        let mut origin_used = false;
        let mut cancelled_this_seed = false;
        let mut first_acquire = None;
        for now in 0..=35 {
            if now == 2 && matches!(family, 1 | 2 | 5) {
                let event = if family == 2 {
                    cancelled_this_seed = true;
                    RecoveryEvent::Cancel
                } else {
                    RecoveryEvent::FeedGap { lapses: 1 }
                };
                effects.extend(engine.step(event).effects);
            }
            effects.extend(engine.step(RecoveryEvent::Tick(Time(now))).effects);
            let mut due = Vec::new();
            scheduled.retain(|item| {
                if item.at <= now {
                    due.push(item.event.clone());
                    false
                } else {
                    true
                }
            });
            if rng.below(2) == 0 {
                due.reverse();
            }
            for event in due {
                let result = engine.step(event);
                if result.rejection.is_some() {
                    stale += 1;
                }
                effects.extend(result.effects);
            }
            let mut immediate = 0;
            while let Some(effect) = effects.pop_front() {
                immediate += 1;
                assert!(immediate < 20, "seed {seed}: unbounded immediate work");
                let event = match effect {
                    RecoveryEffect::CloseGate { .. } => {
                        assert!(!engine.state().recovered, "seed {seed}");
                        None
                    }
                    RecoveryEffect::Invalidate { op, .. } => {
                        Some(RecoveryEvent::Invalidated { op })
                    }
                    RecoveryEffect::AcquireBaseline { op } => {
                        let original = *first_acquire.get_or_insert(op);
                        if op.generation > 1 {
                            if family == 5 {
                                stale_binding += 1;
                                Some(RecoveryEvent::PeerBaselineInstalled {
                                    op,
                                    handoff: Box::new(handoff(original)),
                                })
                            } else {
                                Some(RecoveryEvent::BootstrapDeclined { op })
                            }
                        } else {
                            Some(RecoveryEvent::PeerBaselineInstalled {
                                op,
                                handoff: Box::new(handoff(op)),
                            })
                        }
                    }
                    RecoveryEffect::CancelBaseline { .. } => None,
                    RecoveryEffect::ObservePeerHeads { op } => {
                        let mut identities = members();
                        if family == 4 && engine.state().stage == RecoveryStage::PeerRecheckingHeads
                        {
                            identities[0].incarnation = BootId(11);
                        }
                        Some(RecoveryEvent::PeerHeadsObserved {
                            op,
                            peers: vec![peer(Some(1))],
                            identities,
                        })
                    }
                    RecoveryEffect::WaitFrontiers { op, .. } => {
                        if family == 3 {
                            dropped += 1;
                            None
                        } else {
                            Some(RecoveryEvent::FrontiersReached { op })
                        }
                    }
                    RecoveryEffect::RebuildOrigin { op } => {
                        origin_used = true;
                        Some(RecoveryEvent::Materialized { op })
                    }
                    RecoveryEffect::Affirm { op } => {
                        assert!(!engine.state().recovered, "seed {seed}");
                        Some(RecoveryEvent::Affirmed { op, accepted: true })
                    }
                    RecoveryEffect::ArmTimer(due) => {
                        assert!(due <= Time(32), "seed {seed}: extended original turn");
                        None
                    }
                    RecoveryEffect::ObservePeers { .. } => {
                        panic!("seed {seed}: peer path took lapse proof")
                    }
                };
                if let Some(event) = event {
                    let delay = if family == 5
                        && matches!(event, RecoveryEvent::PeerBaselineInstalled { .. })
                    {
                        4
                    } else {
                        1 + u64::from(rng.below(2))
                    };
                    scheduled.push(Scheduled {
                        at: now + delay,
                        event: event.clone(),
                    });
                    if rng.below(5) == 0 {
                        scheduled.push(Scheduled {
                            at: now + delay + 1,
                            event,
                        });
                    }
                }
            }
            assert!(
                !engine.state().recovered || engine.state().stage == RecoveryStage::Ready,
                "seed {seed}: serving before ready"
            );
        }
        if cancelled_this_seed {
            assert_eq!(
                engine.state().stage,
                RecoveryStage::Cancelled,
                "seed {seed}"
            );
            cancelled += 1;
        } else {
            assert_eq!(engine.state().stage, RecoveryStage::Ready, "seed {seed}");
            healed += 1;
        }
        if origin_used {
            fallback += 1;
        }
    }
    assert_eq!(healed, 50);
    assert_eq!(cancelled, 10);
    assert!(stale >= 10);
    assert!(dropped >= 10);
    assert!(fallback >= 30);
    assert_eq!(stale_binding, 10);
}
