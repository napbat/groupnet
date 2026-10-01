use super::*;
use crate::{NodeId, Time};

fn config() -> RecoveryConfig {
    RecoveryConfig {
        max_members: 4,
        max_member_bytes: 32,
        max_barrier_rounds: 3,
        total_ms: 100,
        attempt_ms: 20,
        settle_ms: 5,
        poll_ms: 2,
    }
}

fn engine() -> RecoveryEngine {
    RecoveryEngine::new(config(), RecoveryMode::Leased, NodeId::from("me"), 7).unwrap()
}

fn operation(step: &RecoveryStep) -> RecoveryOperation {
    step.effects
        .iter()
        .find_map(|effect| match effect {
            RecoveryEffect::Invalidate { op, .. }
            | RecoveryEffect::RebuildOrigin { op }
            | RecoveryEffect::AcquireBaseline { op }
            | RecoveryEffect::ObservePeers { op }
            | RecoveryEffect::ObservePeerHeads { op }
            | RecoveryEffect::WaitFrontiers { op, .. }
            | RecoveryEffect::Affirm { op } => Some(*op),
            RecoveryEffect::CloseGate { .. }
            | RecoveryEffect::CancelBaseline { .. }
            | RecoveryEffect::SuspendLocalBaseline { .. }
            | RecoveryEffect::AdoptLocalBaseline { .. }
            | RecoveryEffect::ResumeLocalBaseline { .. }
            | RecoveryEffect::FellBack { .. }
            | RecoveryEffect::ArmTimer(_) => None,
        })
        .expect("current operation effect")
}

fn peer(name: &str, grant: u64, head: u64) -> Peer {
    Peer {
        node: NodeId::from(name),
        alive: true,
        grants_lease: true,
        old_nonlive: false,
        grant: Some(Mark {
            epoch: 1,
            sequence: grant,
        }),
        head: Some(Mark {
            epoch: 1,
            sequence: head,
        }),
        renewal: None,
        sealed: None,
    }
}

#[expect(
    clippy::unnecessary_wraps,
    reason = "test fixture mirrors the optional native confirmation in every observation"
)]
fn confirmed(sequence: u64) -> Option<Mark> {
    Some(Mark { epoch: 1, sequence })
}

#[test]
fn rearm_policy_validates_before_the_first_event() {
    assert_eq!(
        RecoveryRearm {
            initial_ms: 0,
            max_ms: 5
        }
        .validate(),
        Err(RecoveryError::InvalidConfig)
    );
    assert_eq!(
        engine()
            .with_rearm(RecoveryRearm {
                initial_ms: 9,
                max_ms: 8
            })
            .err(),
        Some(RecoveryError::InvalidConfig)
    );
    let mut used = engine();
    used.step(RecoveryEvent::Start);
    assert_eq!(
        used.with_rearm(RecoveryRearm {
            initial_ms: 5,
            max_ms: 20
        })
        .err(),
        Some(RecoveryError::Stage)
    );
    let mut clocked = engine();
    clocked.step(RecoveryEvent::Tick(Time(0)));
    assert_eq!(
        clocked
            .with_rearm(RecoveryRearm {
                initial_ms: 5,
                max_ms: 20,
            })
            .err(),
        Some(RecoveryError::Stage),
        "even a tick at the initial logical time is an event"
    );
}

#[test]
fn cancel_is_terminal_for_all_restart_signals() {
    let mut recovery = engine()
        .with_rearm(RecoveryRearm {
            initial_ms: 5,
            max_ms: 20,
        })
        .unwrap();
    recovery.step(RecoveryEvent::Start);
    recovery.step(RecoveryEvent::Cancel);
    for event in [
        RecoveryEvent::Start,
        RecoveryEvent::StartWithLapses { lapses: 1 },
        RecoveryEvent::FeedGap { lapses: 1 },
        RecoveryEvent::LeaseLapse { count: 1 },
    ] {
        let rejected = recovery.step(event);
        assert_eq!(rejected.rejection, Some(RecoveryError::Stage));
        assert_eq!(
            rejected.effects,
            [] as [crate::volatile_recovery::types::RecoveryEffect; 0]
        );
        assert_eq!(recovery.state().stage, RecoveryStage::Cancelled);
        assert_eq!(recovery.next_deadline(), None);
    }
}

#[test]
fn explicit_restart_coalesces_new_lapses_and_overrides_rearm_cooldown() {
    let mut recovery = engine()
        .with_rearm(RecoveryRearm {
            initial_ms: 10,
            max_ms: 40,
        })
        .unwrap();
    recovery.step(RecoveryEvent::Start);
    recovery.step(RecoveryEvent::Tick(Time(100)));
    assert_eq!(recovery.next_deadline(), Some(Time(110)));
    let restarted = recovery.step(RecoveryEvent::StartWithLapses { lapses: 7 });
    assert!(restarted.rejection.is_none());
    assert_eq!(recovery.state().covered_lapses, 7);
    assert_eq!(recovery.state().stage, RecoveryStage::Invalidating);
    assert_eq!(recovery.next_deadline(), Some(Time(120)));
    assert!(restarted.effects.iter().any(|effect| matches!(
        effect,
        RecoveryEffect::Invalidate {
            distrust_bodies: true,
            ..
        }
    )));
}

#[test]
fn start_clock_exhaustion_disables_rearm_without_scheduling_a_timer() {
    let mut recovery = engine()
        .with_rearm(RecoveryRearm {
            initial_ms: 5,
            max_ms: 20,
        })
        .unwrap();
    recovery.step(RecoveryEvent::Tick(Time(u64::MAX - 1)));
    let exhausted = recovery.step(RecoveryEvent::Start);
    assert_eq!(exhausted.rejection, Some(RecoveryError::Exhausted));
    assert_eq!(recovery.state().stage, RecoveryStage::OriginOnly);
    assert_eq!(recovery.next_deadline(), None);
    assert_eq!(
        recovery.step(RecoveryEvent::Start).rejection,
        Some(RecoveryError::Exhausted)
    );
}

#[test]
fn exhausted_episodes_rearm_at_capped_intervals_and_coalesce_signals() {
    let mut recovery = engine()
        .with_rearm(RecoveryRearm {
            initial_ms: 5,
            max_ms: 10,
        })
        .unwrap();
    let first = recovery.step(RecoveryEvent::Start);
    let old = operation(&first);
    let expired = recovery.step(RecoveryEvent::Tick(Time(100)));
    assert_eq!(recovery.state().stage, RecoveryStage::OriginOnly);
    assert_eq!(recovery.next_deadline(), Some(Time(105)));
    assert!(
        expired
            .effects
            .contains(&RecoveryEffect::ArmTimer(Time(105)))
    );
    let generation = recovery.state().generation;
    for count in 1..=8 {
        let pending = recovery.step(RecoveryEvent::FeedGap { lapses: count });
        assert!(pending.rejection.is_none());
        assert_eq!(recovery.state().generation, generation);
        assert_eq!(recovery.next_deadline(), Some(Time(105)));
    }
    assert_eq!(recovery.state().covered_lapses, 8);
    assert_eq!(
        recovery.step(RecoveryEvent::Tick(Time(104))).effects,
        [] as [crate::volatile_recovery::types::RecoveryEffect; 0]
    );
    let second = recovery.step(RecoveryEvent::Tick(Time(105)));
    assert_eq!(recovery.state().generation, generation + 1);
    assert_eq!(recovery.state().stage, RecoveryStage::Invalidating);
    assert_eq!(recovery.next_deadline(), Some(Time(125)));
    assert!(!recovery.accepts_operation(old));
    assert_ne!(operation(&second), old);

    recovery.step(RecoveryEvent::Tick(Time(205)));
    assert_eq!(recovery.next_deadline(), Some(Time(215)));
    recovery.step(RecoveryEvent::Tick(Time(215)));
    recovery.step(RecoveryEvent::Tick(Time(315)));
    assert_eq!(recovery.next_deadline(), Some(Time(325)));
    let cancelled = recovery.step(RecoveryEvent::Cancel);
    assert!(cancelled.rejection.is_none());
    assert_eq!(recovery.next_deadline(), None);
    assert_eq!(recovery.state().stage, RecoveryStage::Cancelled);
}

#[test]
fn successful_rearm_resets_delay_and_explicit_restart_supersedes_timer() {
    let mut recovery = engine()
        .with_rearm(RecoveryRearm {
            initial_ms: 5,
            max_ms: 40,
        })
        .unwrap();
    recovery.step(RecoveryEvent::Start);
    recovery.step(RecoveryEvent::Tick(Time(100)));
    assert_eq!(recovery.next_deadline(), Some(Time(105)));
    let restarted = recovery.step(RecoveryEvent::Start);
    assert_eq!(recovery.next_deadline(), Some(Time(120)));
    assert_eq!(recovery.state().stage, RecoveryStage::Invalidating);
    let invalid = operation(&restarted);
    let rebuild = recovery.step(RecoveryEvent::Invalidated { op: invalid });
    let affirm = recovery.step(RecoveryEvent::Materialized {
        op: operation(&rebuild),
    });
    let ready = recovery.step(RecoveryEvent::Affirmed {
        op: operation(&affirm),
        accepted: true,
    });
    assert!(ready.rejection.is_none());
    assert_eq!(recovery.next_deadline(), None);
    assert!(recovery.state().recovered);
    recovery.step(RecoveryEvent::FeedGap { lapses: 0 });
    recovery.step(RecoveryEvent::Tick(Time(200)));
    assert_eq!(recovery.next_deadline(), Some(Time(205)));
}

#[test]
fn rearm_clock_exhaustion_stays_closed_and_terminal() {
    let config = RecoveryConfig {
        total_ms: 5,
        attempt_ms: 5,
        ..config()
    };
    let mut recovery = RecoveryEngine::new(config, RecoveryMode::Unleased, NodeId::from("me"), 1)
        .unwrap()
        .with_rearm(RecoveryRearm {
            initial_ms: 10,
            max_ms: 10,
        })
        .unwrap();
    recovery.step(RecoveryEvent::Tick(Time(u64::MAX - 6)));
    recovery.step(RecoveryEvent::Start);
    let exhausted = recovery.step(RecoveryEvent::Tick(Time(u64::MAX - 1)));
    assert_eq!(exhausted.rejection, Some(RecoveryError::Exhausted));
    assert_eq!(recovery.state().stage, RecoveryStage::OriginOnly);
    assert!(!recovery.state().recovered);
    assert_eq!(recovery.next_deadline(), None);
    assert_eq!(
        recovery
            .step(RecoveryEvent::FeedGap { lapses: 3 })
            .rejection,
        Some(RecoveryError::Exhausted)
    );
}

#[test]
fn limits_and_local_identity_are_checked_before_work() {
    assert_eq!(
        RecoveryEngine::new(
            RecoveryConfig {
                max_members: usize::MAX,
                max_member_bytes: 2,
                ..config()
            },
            RecoveryMode::Leased,
            NodeId::from("me"),
            1,
        )
        .err(),
        Some(RecoveryError::InvalidConfig)
    );
    assert_eq!(
        RecoveryEngine::new(config(), RecoveryMode::Leased, NodeId::from(""), 1).err(),
        Some(RecoveryError::InvalidConfig)
    );
}

fn full_ready(engine: &mut RecoveryEngine) {
    let started = engine.step(RecoveryEvent::Start);
    let invalidated = engine.step(RecoveryEvent::Invalidated {
        op: operation(&started),
    });
    let rebuilt = engine.step(RecoveryEvent::Materialized {
        op: operation(&invalidated),
    });
    let affirmed = engine.step(RecoveryEvent::Affirmed {
        op: operation(&rebuilt),
        accepted: true,
    });
    assert!(affirmed.rejection.is_none());
    assert!(engine.state().recovered);
}

#[test]
fn cold_and_origin_only_lapses_cannot_downgrade_full_rebuild() {
    let mut recovery = engine();
    let cold = recovery.step(RecoveryEvent::LeaseLapse { count: 1 });
    assert!(cold.effects.iter().any(|effect| matches!(
        effect,
        RecoveryEffect::Invalidate {
            distrust_bodies: true,
            ..
        }
    )));
    let first = operation(&cold);
    let gap = recovery.step(RecoveryEvent::FeedGap { lapses: 1 });
    let rebuilding = recovery.step(RecoveryEvent::Invalidated {
        op: operation(&gap),
    });
    assert_eq!(recovery.state().stage, RecoveryStage::Rebuilding);
    let newer = recovery.step(RecoveryEvent::LeaseLapse { count: 2 });
    assert!(newer.effects.iter().any(|effect| matches!(
        effect,
        RecoveryEffect::Invalidate {
            distrust_bodies: true,
            ..
        }
    )));
    assert_eq!(
        recovery
            .step(RecoveryEvent::Invalidated { op: first })
            .rejection,
        Some(RecoveryError::StaleOperation)
    );
    assert_eq!(
        recovery
            .step(RecoveryEvent::Materialized {
                op: operation(&rebuilding)
            })
            .rejection,
        Some(RecoveryError::StaleOperation)
    );
    assert!(!recovery.state().recovered);
    let failed = recovery.step(RecoveryEvent::Failed {
        op: operation(&newer),
    });
    assert!(failed.rejection.is_none());
    assert_eq!(recovery.state().stage, RecoveryStage::Invalidating);
    recovery.step(RecoveryEvent::Tick(Time(100)));
    assert_eq!(recovery.state().stage, RecoveryStage::OriginOnly);
    let restart = recovery.step(RecoveryEvent::LeaseLapse { count: 3 });
    assert!(restart.effects.iter().any(|effect| matches!(
        effect,
        RecoveryEffect::Invalidate {
            distrust_bodies: true,
            ..
        }
    )));
}

#[test]
fn lapse_proof_requires_each_granter_and_both_vanished_checks() {
    let mut recovery = engine();
    full_ready(&mut recovery);
    let lapse = recovery.step(RecoveryEvent::LeaseLapse { count: 1 });
    assert!(lapse.effects.iter().any(|effect| matches!(
        effect,
        RecoveryEffect::Invalidate {
            distrust_bodies: false,
            ..
        }
    )));
    let initial = recovery.step(RecoveryEvent::Invalidated {
        op: operation(&lapse),
    });
    let peers = vec![peer("a", 1, 1), peer("b", 1, 1)];
    let renew = recovery.step(RecoveryEvent::PeersObserved {
        op: operation(&initial),
        peers: peers.clone(),
        confirmed: confirmed(1),
    });
    let frozen = recovery.step(RecoveryEvent::PeersObserved {
        op: operation(&renew),
        peers: vec![peer("a", 2, 1), peer("b", 1, 1)],
        confirmed: confirmed(2),
    });
    assert_eq!(recovery.state().stage, RecoveryStage::WaitingRenewals);
    assert!(
        frozen
            .effects
            .iter()
            .any(|effect| matches!(effect, RecoveryEffect::ArmTimer(Time(2))))
    );
    let retry = recovery.step(RecoveryEvent::Tick(Time(2)));
    let advanced = recovery.step(RecoveryEvent::PeersObserved {
        op: operation(&retry),
        peers: vec![peer("a", 2, 1), peer("b", 2, 1)],
        confirmed: confirmed(2),
    });
    assert_eq!(recovery.state().stage, RecoveryStage::Settling);
    assert!(
        advanced
            .effects
            .iter()
            .any(|effect| matches!(effect, RecoveryEffect::ArmTimer(Time(7))))
    );
    let heads = recovery.step(RecoveryEvent::Tick(Time(7)));
    let before_barrier = recovery.step(RecoveryEvent::PeersObserved {
        op: operation(&heads),
        peers: peers.clone(),
        confirmed: confirmed(2),
    });
    assert_eq!(recovery.state().stage, RecoveryStage::WaitingFrontiers);
    let recheck = recovery.step(RecoveryEvent::FrontiersReached {
        op: operation(&before_barrier),
    });
    let mut dead_but_present = peers;
    dead_but_present[1].alive = false;
    let affirm = recovery.step(RecoveryEvent::PeersObserved {
        op: operation(&recheck),
        peers: dead_but_present,
        confirmed: confirmed(2),
    });
    assert_eq!(recovery.state().stage, RecoveryStage::Affirming);
    let done = recovery.step(RecoveryEvent::Affirmed {
        op: operation(&affirm),
        accepted: true,
    });
    assert!(done.rejection.is_none());
    assert!(recovery.state().recovered);
}

#[test]
fn vanished_member_and_cancel_fail_closed() {
    let mut recovery = engine();
    full_ready(&mut recovery);
    let lapse = recovery.step(RecoveryEvent::LeaseLapse { count: 1 });
    let initial = recovery.step(RecoveryEvent::Invalidated {
        op: operation(&lapse),
    });
    let renew = recovery.step(RecoveryEvent::PeersObserved {
        op: operation(&initial),
        peers: vec![peer("a", 1, 1)],
        confirmed: confirmed(1),
    });
    recovery.step(RecoveryEvent::PeersObserved {
        op: operation(&renew),
        peers: vec![peer("a", 2, 1)],
        confirmed: confirmed(2),
    });
    let heads = recovery.step(RecoveryEvent::Tick(Time(5)));
    let vanished = recovery.step(RecoveryEvent::PeersObserved {
        op: operation(&heads),
        peers: Vec::new(),
        confirmed: confirmed(2),
    });
    assert!(vanished.effects.iter().any(|effect| matches!(
        effect,
        RecoveryEffect::Invalidate {
            distrust_bodies: true,
            ..
        }
    )));
    assert!(!recovery.state().recovered);
    recovery.step(RecoveryEvent::Cancel);
    assert_eq!(
        recovery
            .step(RecoveryEvent::FeedGap { lapses: 2 })
            .rejection,
        Some(RecoveryError::Stage)
    );
    assert_eq!(
        recovery
            .step(RecoveryEvent::LeaseLapse { count: 2 })
            .rejection,
        Some(RecoveryError::Stage)
    );
    let explicit = recovery.step(RecoveryEvent::Start);
    assert_eq!(explicit.rejection, Some(RecoveryError::Stage));
    assert_eq!(
        explicit.effects,
        [] as [crate::volatile_recovery::types::RecoveryEffect; 0]
    );
}

#[test]
fn stalled_lapse_budget_falls_back_to_fenced_origin_rebuild() {
    let mut recovery = engine();
    full_ready(&mut recovery);
    let lapse = recovery.step(RecoveryEvent::LeaseLapse { count: 1 });
    let initial = recovery.step(RecoveryEvent::Invalidated {
        op: operation(&lapse),
    });
    let stale = operation(&initial);
    let timeout = recovery.step(RecoveryEvent::Tick(Time(100)));
    assert!(timeout.effects.iter().any(|effect| matches!(
        effect,
        RecoveryEffect::Invalidate {
            distrust_bodies: true,
            ..
        }
    )));
    assert_eq!(recovery.state().stage, RecoveryStage::Invalidating);
    assert_eq!(recovery.state().generation, 3);
    assert_eq!(
        recovery
            .step(RecoveryEvent::PeersObserved {
                op: stale,
                peers: vec![peer("a", 2, 1)],
                confirmed: confirmed(2),
            })
            .rejection,
        Some(RecoveryError::StaleOperation)
    );
}

#[test]
fn oversized_or_contradictory_roster_cannot_advance_lapse() {
    let mut recovery = engine();
    full_ready(&mut recovery);
    let lapse = recovery.step(RecoveryEvent::LeaseLapse { count: 1 });
    let initial = recovery.step(RecoveryEvent::Invalidated {
        op: operation(&lapse),
    });
    let op = operation(&initial);
    let too_many = recovery.step(RecoveryEvent::PeersObserved {
        op,
        peers: (0..5)
            .map(|index| peer(&format!("peer-{index}"), 1, 1))
            .collect(),
        confirmed: confirmed(1),
    });
    assert_eq!(too_many.rejection, Some(RecoveryError::Capacity));
    assert!(recovery.accepts_operation(op));
    let duplicate = recovery.step(RecoveryEvent::PeersObserved {
        op,
        peers: vec![peer("a", 1, 1), peer("a", 1, 1)],
        confirmed: confirmed(1),
    });
    assert_eq!(duplicate.rejection, Some(RecoveryError::InvalidEvidence));
    assert!(recovery.accepts_operation(op));
}

#[test]
fn quiet_present_peer_has_no_frontier_target_but_still_counts_for_grants() {
    let mut recovery = engine();
    full_ready(&mut recovery);
    let lapse = recovery.step(RecoveryEvent::LeaseLapse { count: 1 });
    let initial = recovery.step(RecoveryEvent::Invalidated {
        op: operation(&lapse),
    });
    let mut quiet = peer("quiet", 1, 1);
    quiet.head = None;
    let renewal = recovery.step(RecoveryEvent::PeersObserved {
        op: operation(&initial),
        peers: vec![quiet.clone()],
        confirmed: confirmed(1),
    });
    let frozen = recovery.step(RecoveryEvent::PeersObserved {
        op: operation(&renewal),
        peers: vec![quiet.clone()],
        confirmed: confirmed(2),
    });
    assert_eq!(recovery.state().stage, RecoveryStage::WaitingRenewals);
    assert!(
        frozen
            .effects
            .iter()
            .any(|effect| matches!(effect, RecoveryEffect::ArmTimer(Time(2))))
    );
    let retry = recovery.step(RecoveryEvent::Tick(Time(2)));
    quiet.grant = confirmed(2);
    let advanced = recovery.step(RecoveryEvent::PeersObserved {
        op: operation(&retry),
        peers: vec![quiet.clone()],
        confirmed: confirmed(2),
    });
    assert_eq!(recovery.state().stage, RecoveryStage::Settling);
    assert!(advanced.rejection.is_none());
    let sample = recovery.step(RecoveryEvent::Tick(Time(7)));
    let wait = recovery.step(RecoveryEvent::PeersObserved {
        op: operation(&sample),
        peers: vec![quiet.clone()],
        confirmed: confirmed(2),
    });
    assert!(wait.effects.iter().any(
        |effect| matches!(effect, RecoveryEffect::WaitFrontiers { heads, .. } if heads.is_empty())
    ));
    let recheck = recovery.step(RecoveryEvent::FrontiersReached {
        op: operation(&wait),
    });
    let affirm = recovery.step(RecoveryEvent::PeersObserved {
        op: operation(&recheck),
        peers: vec![quiet],
        confirmed: confirmed(2),
    });
    recovery.step(RecoveryEvent::Affirmed {
        op: operation(&affirm),
        accepted: true,
    });
    assert!(recovery.state().recovered);
}

#[test]
fn previously_advertised_head_cannot_disappear_into_empty_feed() {
    let mut recovery = engine();
    full_ready(&mut recovery);
    let lapse = recovery.step(RecoveryEvent::LeaseLapse { count: 1 });
    let initial = recovery.step(RecoveryEvent::Invalidated {
        op: operation(&lapse),
    });
    let renewal = recovery.step(RecoveryEvent::PeersObserved {
        op: operation(&initial),
        peers: vec![peer("a", 1, 3)],
        confirmed: confirmed(1),
    });
    let mut disappeared = peer("a", 2, 3);
    disappeared.head = None;
    let fallback = recovery.step(RecoveryEvent::PeersObserved {
        op: operation(&renewal),
        peers: vec![disappeared],
        confirmed: confirmed(2),
    });
    assert!(fallback.effects.iter().any(|effect| matches!(
        effect,
        RecoveryEffect::Invalidate {
            distrust_bodies: true,
            ..
        }
    )));
    assert!(!recovery.state().recovered);
}

/// Drives a lapse arm to its first recheck with `a` advertising `(1, 3)`,
/// then reports `restarted` for it and returns the step.
fn lapse_recheck_after(recovery: &mut RecoveryEngine, restarted: Peer) -> RecoveryStep {
    full_ready(recovery);
    let lapse = recovery.step(RecoveryEvent::LeaseLapse { count: 1 });
    let initial = recovery.step(RecoveryEvent::Invalidated {
        op: operation(&lapse),
    });
    let renewal = recovery.step(RecoveryEvent::PeersObserved {
        op: operation(&initial),
        peers: vec![peer("a", 1, 3)],
        confirmed: confirmed(1),
    });
    recovery.step(RecoveryEvent::PeersObserved {
        op: operation(&renewal),
        peers: vec![peer("a", 2, 3)],
        confirmed: confirmed(2),
    });
    let sample = recovery.step(RecoveryEvent::Tick(Time(5)));
    let wait = recovery.step(RecoveryEvent::PeersObserved {
        op: operation(&sample),
        peers: vec![peer("a", 2, 3)],
        confirmed: confirmed(2),
    });
    let recheck = recovery.step(RecoveryEvent::FrontiersReached {
        op: operation(&wait),
    });
    recovery.step(RecoveryEvent::PeersObserved {
        op: operation(&recheck),
        peers: vec![restarted],
        confirmed: confirmed(2),
    })
}

fn restarted(renewal: Option<Renewal>) -> Peer {
    let mut restarted = peer("a", 2, 1);
    restarted.head = Some(Mark {
        epoch: 2,
        sequence: 1,
    });
    restarted.renewal = renewal;
    restarted
}

fn sealed_at(sequence: u64) -> Renewal {
    Renewal {
        sealed: Mark { epoch: 1, sequence },
        epoch: 2,
    }
}

fn distrusts(step: &RecoveryStep) -> bool {
    step.effects.iter().any(|effect| {
        matches!(
            effect,
            RecoveryEffect::Invalidate {
                distrust_bodies: true,
                ..
            }
        )
    })
}

#[test]
fn a_lapse_barrier_follows_a_delivered_seal_into_the_writers_next_life() {
    let mut recovery = engine();
    let next = lapse_recheck_after(&mut recovery, restarted(Some(sealed_at(4))));
    assert!(!distrusts(&next), "a delivered seal is not lost evidence");
    let new_head = Mark {
        epoch: 2,
        sequence: 1,
    };
    assert!(next.effects.iter().any(|effect| matches!(
        effect,
        RecoveryEffect::WaitFrontiers { heads, .. } if heads == &vec![(NodeId::from("a"), new_head)]
    )));
    let recheck = recovery.step(RecoveryEvent::FrontiersReached {
        op: operation(&next),
    });
    let affirm = recovery.step(RecoveryEvent::PeersObserved {
        op: operation(&recheck),
        peers: vec![restarted(Some(sealed_at(4)))],
        confirmed: confirmed(2),
    });
    recovery.step(RecoveryEvent::Affirmed {
        op: operation(&affirm),
        accepted: true,
    });
    assert!(recovery.state().recovered);
}

#[test]
fn a_sealed_writer_that_restarts_with_nothing_written_yet_still_affirms() {
    let mut recovery = engine();
    let mut announced = restarted(Some(sealed_at(4)));
    announced.head = None;
    let next = lapse_recheck_after(&mut recovery, announced.clone());
    assert!(!distrusts(&next));
    let recheck = recovery.step(RecoveryEvent::FrontiersReached {
        op: operation(&next),
    });
    let affirm = recovery.step(RecoveryEvent::PeersObserved {
        op: operation(&recheck),
        peers: vec![announced],
        confirmed: confirmed(2),
    });
    recovery.step(RecoveryEvent::Affirmed {
        op: operation(&affirm),
        accepted: true,
    });
    assert!(recovery.state().recovered);
}

#[test]
fn a_restart_without_a_covering_delivered_seal_still_falls_back() {
    for (case, renewal) in [
        ("no seal was delivered", None),
        ("the seal precedes the sampled head", Some(sealed_at(3))),
        (
            "the renewal is into another life",
            Some(Renewal {
                sealed: Mark {
                    epoch: 1,
                    sequence: 4,
                },
                epoch: 3,
            }),
        ),
    ] {
        let mut recovery = engine();
        let next = lapse_recheck_after(&mut recovery, restarted(renewal));
        assert!(distrusts(&next), "{case}");
        assert!(!recovery.state().recovered, "{case}");
    }
}

#[test]
fn newly_observed_nonlive_writer_cannot_vanish_before_head_sample() {
    let mut recovery = engine();
    full_ready(&mut recovery);
    let lapse = recovery.step(RecoveryEvent::LeaseLapse { count: 1 });
    let initial = recovery.step(RecoveryEvent::Invalidated {
        op: operation(&lapse),
    });
    let renewal = recovery.step(RecoveryEvent::PeersObserved {
        op: operation(&initial),
        peers: vec![peer("a", 1, 1)],
        confirmed: confirmed(1),
    });
    let mut late = peer("late", 1, 4);
    late.alive = false;
    late.grants_lease = false;
    let settle = recovery.step(RecoveryEvent::PeersObserved {
        op: operation(&renewal),
        peers: vec![peer("a", 2, 1), late],
        confirmed: confirmed(2),
    });
    assert_eq!(recovery.state().stage, RecoveryStage::Settling);
    assert!(settle.rejection.is_none());
    let sample = recovery.step(RecoveryEvent::Tick(Time(5)));
    let fallback = recovery.step(RecoveryEvent::PeersObserved {
        op: operation(&sample),
        peers: vec![peer("a", 2, 1)],
        confirmed: confirmed(2),
    });
    assert!(fallback.effects.iter().any(|effect| matches!(
        effect,
        RecoveryEffect::Invalidate {
            distrust_bodies: true,
            ..
        }
    )));
    assert!(!recovery.state().recovered);
}

#[test]
fn newly_observed_nonlive_writer_cannot_vanish_during_barrier() {
    let mut recovery = engine();
    full_ready(&mut recovery);
    let lapse = recovery.step(RecoveryEvent::LeaseLapse { count: 1 });
    let initial = recovery.step(RecoveryEvent::Invalidated {
        op: operation(&lapse),
    });
    let renewal = recovery.step(RecoveryEvent::PeersObserved {
        op: operation(&initial),
        peers: vec![peer("a", 1, 1)],
        confirmed: confirmed(1),
    });
    let settle = recovery.step(RecoveryEvent::PeersObserved {
        op: operation(&renewal),
        peers: vec![peer("a", 2, 1)],
        confirmed: confirmed(2),
    });
    assert!(settle.rejection.is_none());
    assert_eq!(recovery.state().stage, RecoveryStage::Settling);
    let sample = recovery.step(RecoveryEvent::Tick(Time(5)));
    let mut late = peer("late", 1, 4);
    late.alive = false;
    late.grants_lease = false;
    let barrier = recovery.step(RecoveryEvent::PeersObserved {
        op: operation(&sample),
        peers: vec![peer("a", 2, 1), late],
        confirmed: confirmed(2),
    });
    assert_eq!(recovery.state().stage, RecoveryStage::WaitingFrontiers);
    let recheck = recovery.step(RecoveryEvent::FrontiersReached {
        op: operation(&barrier),
    });
    let fallback = recovery.step(RecoveryEvent::PeersObserved {
        op: operation(&recheck),
        peers: vec![peer("a", 2, 1)],
        confirmed: confirmed(2),
    });
    assert!(fallback.effects.iter().any(|effect| matches!(
        effect,
        RecoveryEffect::Invalidate {
            distrust_bodies: true,
            ..
        }
    )));
    assert!(!recovery.state().recovered);
}

#[test]
fn full_rebuild_failure_retries_with_new_operation_before_original_total_deadline() {
    let mut recovery = engine();
    let started = recovery.step(RecoveryEvent::Start);
    let invalidated = recovery.step(RecoveryEvent::Invalidated {
        op: operation(&started),
    });
    let first_rebuild = operation(&invalidated);
    let failed = recovery.step(RecoveryEvent::Failed { op: first_rebuild });
    assert_eq!(recovery.state().stage, RecoveryStage::Rebuilding);
    assert!(
        failed
            .effects
            .iter()
            .any(|effect| matches!(effect, RecoveryEffect::ArmTimer(Time(2))))
    );
    let retry = recovery.step(RecoveryEvent::Tick(Time(2)));
    let second_rebuild = operation(&retry);
    assert_ne!(first_rebuild, second_rebuild);
    assert_eq!(
        recovery
            .step(RecoveryEvent::Materialized { op: first_rebuild })
            .rejection,
        Some(RecoveryError::StaleOperation)
    );
    let materialized = recovery.step(RecoveryEvent::Materialized { op: second_rebuild });
    let affirmed = recovery.step(RecoveryEvent::Affirmed {
        op: operation(&materialized),
        accepted: true,
    });
    assert!(affirmed.rejection.is_none());
    assert!(recovery.state().recovered);
}

#[test]
fn full_operation_timeout_retries_but_total_deadline_still_ends_origin_only() {
    let mut recovery = engine();
    let started = recovery.step(RecoveryEvent::Start);
    let first = operation(&started);
    let timed_out = recovery.step(RecoveryEvent::Tick(Time(20)));
    assert_eq!(recovery.state().stage, RecoveryStage::Invalidating);
    assert!(!recovery.accepts_operation(first));
    assert!(
        timed_out
            .effects
            .iter()
            .any(|effect| matches!(effect, RecoveryEffect::ArmTimer(Time(22))))
    );
    let retry = recovery.step(RecoveryEvent::Tick(Time(22)));
    assert_ne!(operation(&retry), first);
    let exhausted = recovery.step(RecoveryEvent::Tick(Time(100)));
    assert!(exhausted.rejection.is_none());
    assert_eq!(recovery.state().stage, RecoveryStage::OriginOnly);
    assert!(!recovery.state().recovered);
}
