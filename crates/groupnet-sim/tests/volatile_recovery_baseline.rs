//! Seeded local-baseline lapse handoff and interruption fences.

use groupnet_core::volatile_recovery::{
    Mark, Peer, RecoveryConfig, RecoveryEffect, RecoveryEngine, RecoveryEvent, RecoveryMode,
    RecoveryOperation, RecoveryStage, RecoveryStep,
};
use groupnet_core::{NodeId, Time};
use groupnet_sim::SplitMix64;

fn operation(step: &RecoveryStep) -> RecoveryOperation {
    step.effects
        .iter()
        .find_map(|effect| match effect {
            RecoveryEffect::AcquireBaseline { op }
            | RecoveryEffect::Invalidate { op, .. }
            | RecoveryEffect::ObservePeers { op }
            | RecoveryEffect::Affirm { op }
            | RecoveryEffect::WaitFrontiers { op, .. } => Some(*op),
            _ => None,
        })
        .expect("one current operation")
}

fn peer(sequence: u64) -> Peer {
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
        sealed: None,
    }
}

fn ready_local(seed: u64) -> (RecoveryEngine, RecoveryOperation) {
    let mut engine = RecoveryEngine::new(
        RecoveryConfig {
            max_members: 2,
            max_member_bytes: 8,
            max_barrier_rounds: 2,
            total_ms: 30,
            attempt_ms: 10,
            settle_ms: 2,
            poll_ms: 2,
        },
        RecoveryMode::Leased,
        NodeId::from("me"),
        seed + 1,
    )
    .unwrap()
    .with_bootstrap()
    .unwrap();
    let start = engine.step(RecoveryEvent::Start);
    let acquire = engine.step(RecoveryEvent::Invalidated {
        op: operation(&start),
    });
    let baseline = operation(&acquire);
    let affirm = engine.step(RecoveryEvent::LocalBaselineBuilt { op: baseline });
    engine.step(RecoveryEvent::Affirmed {
        op: operation(&affirm),
        accepted: true,
    });
    assert!(engine.state().recovered);
    (engine, baseline)
}

#[test]
fn retained_local_baseline_only_resumes_after_exact_affirmation() {
    let mut resumed = 0;
    let mut interrupted = 0;
    for seed in 0..48 {
        let mut rng = SplitMix64::new(seed);
        let (mut engine, baseline) = ready_local(seed);
        let lapse = engine.step(RecoveryEvent::LeaseLapse { count: 1 });
        assert!(lapse.effects.iter().any(|effect| matches!(effect,
            RecoveryEffect::SuspendLocalBaseline { op } if *op == baseline)));
        assert!(!engine.state().recovered);
        if rng.below(2) == 0 {
            let interrupted_step = engine.step(RecoveryEvent::FeedGap { lapses: 1 });
            assert!(
                interrupted_step
                    .effects
                    .iter()
                    .any(|effect| matches!(effect,
                RecoveryEffect::CancelBaseline { op } if *op == baseline))
            );
            assert!(
                !interrupted_step
                    .effects
                    .iter()
                    .any(|effect| matches!(effect, RecoveryEffect::ResumeLocalBaseline { .. }))
            );
            interrupted += 1;
            continue;
        }
        let invalid = engine.step(RecoveryEvent::Invalidated {
            op: operation(&lapse),
        });
        let first = engine.step(RecoveryEvent::PeersObserved {
            op: operation(&invalid),
            peers: vec![peer(1)],
            confirmed: Some(Mark {
                epoch: 1,
                sequence: 1,
            }),
        });
        let second = engine.step(RecoveryEvent::PeersObserved {
            op: operation(&first),
            peers: vec![peer(2)],
            confirmed: Some(Mark {
                epoch: 1,
                sequence: 2,
            }),
        });
        assert!(second.rejection.is_none());
        let tick = engine.step(RecoveryEvent::Tick(Time(2)));
        let barrier = engine.step(RecoveryEvent::PeersObserved {
            op: operation(&tick),
            peers: vec![peer(2)],
            confirmed: Some(Mark {
                epoch: 1,
                sequence: 2,
            }),
        });
        let reached = engine.step(RecoveryEvent::FrontiersReached {
            op: operation(&barrier),
        });
        let final_heads = engine.step(RecoveryEvent::PeersObserved {
            op: operation(&reached),
            peers: vec![peer(2)],
            confirmed: Some(Mark {
                epoch: 1,
                sequence: 2,
            }),
        });
        assert_eq!(engine.state().stage, RecoveryStage::Affirming);
        let stale = engine.step(RecoveryEvent::Affirmed {
            op: baseline,
            accepted: true,
        });
        assert!(stale.rejection.is_some());
        let complete = engine.step(RecoveryEvent::Affirmed {
            op: operation(&final_heads),
            accepted: true,
        });
        assert!(engine.state().recovered);
        assert!(complete.effects.iter().any(|effect| matches!(effect,
            RecoveryEffect::ResumeLocalBaseline { previous, current }
                if *previous == baseline && current.generation == 2 && current.token > baseline.token)));
        assert!(!complete.effects.iter().any(|effect| matches!(
            effect,
            RecoveryEffect::AcquireBaseline { .. } | RecoveryEffect::RebuildOrigin { .. }
        )));
        resumed += 1;
    }
    assert!(resumed > 0 && interrupted > 0);
}
