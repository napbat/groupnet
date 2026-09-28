//! Seeded virtual-time outages for optional volatile origin recovery rearm.

use groupnet_core::volatile_recovery::{
    RecoveryConfig, RecoveryEffect, RecoveryEngine, RecoveryEvent, RecoveryMode, RecoveryOperation,
    RecoveryRearm, RecoveryStage, RecoveryStep,
};
use groupnet_core::{NodeId, Time};
use groupnet_sim::SplitMix64;

fn config() -> RecoveryConfig {
    RecoveryConfig {
        max_members: 4,
        max_member_bytes: 32,
        max_barrier_rounds: 2,
        total_ms: 20,
        attempt_ms: 5,
        settle_ms: 2,
        poll_ms: 1,
    }
}

fn op(step: &RecoveryStep) -> RecoveryOperation {
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
            | RecoveryEffect::ArmTimer(_) => None,
        })
        .expect("the current full turn emits one operation")
}

#[test]
fn bounded_rearms_remain_closed_under_noise_and_heal_after_failed_episodes() {
    let mut failed_responses = 0;
    let mut lost_responses = 0;
    let mut noisy_cooldowns = 0;
    let mut healed = 0;
    for seed in 0..64 {
        let mut rng = SplitMix64::new(seed);
        let initial = 2 + u64::from(rng.below(5));
        let cap = initial * 4;
        let failures = 1 + rng.below(4);
        let mut recovery =
            RecoveryEngine::new(config(), RecoveryMode::Leased, NodeId::from("me"), seed + 1)
                .unwrap()
                .with_rearm(RecoveryRearm {
                    initial_ms: initial,
                    max_ms: cap,
                })
                .unwrap();
        let mut start_time = 0;
        let mut turn = recovery.step(RecoveryEvent::Start);
        let mut expected_delay = initial;
        for episode in 0..failures {
            let old_op = op(&turn);
            if (seed + u64::from(episode)) % 2 == 0 {
                let rebuild = recovery.step(RecoveryEvent::Invalidated { op: old_op });
                let failed = recovery.step(RecoveryEvent::Failed { op: op(&rebuild) });
                assert!(failed.rejection.is_none(), "seed {seed}");
                failed_responses += 1;
            } else {
                lost_responses += 1;
            }
            let exhausted_at = start_time + config().total_ms;
            recovery.step(RecoveryEvent::Tick(Time(exhausted_at)));
            assert_eq!(
                recovery.state().stage,
                RecoveryStage::OriginOnly,
                "seed {seed}"
            );
            assert!(!recovery.state().recovered, "seed {seed}");
            let due = exhausted_at + expected_delay;
            assert_eq!(recovery.next_deadline(), Some(Time(due)), "seed {seed}");
            let generation = recovery.state().generation;
            for noise in 1..=3 {
                let count = u64::from(episode) * 3 + noise;
                recovery.step(RecoveryEvent::FeedGap { lapses: count });
                recovery.step(RecoveryEvent::LeaseLapse { count: count + 1 });
                assert_eq!(recovery.state().generation, generation, "seed {seed}");
                assert_eq!(recovery.next_deadline(), Some(Time(due)), "seed {seed}");
                noisy_cooldowns += 1;
            }
            recovery.step(RecoveryEvent::Tick(Time(due - 1)));
            assert_eq!(
                recovery.state().stage,
                RecoveryStage::OriginOnly,
                "seed {seed}"
            );
            turn = recovery.step(RecoveryEvent::Tick(Time(due)));
            assert_eq!(
                recovery.state().stage,
                RecoveryStage::Invalidating,
                "seed {seed}"
            );
            assert_eq!(recovery.state().generation, generation + 1, "seed {seed}");
            assert!(!recovery.accepts_operation(old_op), "seed {seed}");
            start_time = due;
            expected_delay = expected_delay.saturating_mul(2).min(cap);
        }
        let invalid = op(&turn);
        let rebuild = recovery.step(RecoveryEvent::Invalidated { op: invalid });
        let affirm = recovery.step(RecoveryEvent::Materialized { op: op(&rebuild) });
        let ready = recovery.step(RecoveryEvent::Affirmed {
            op: op(&affirm),
            accepted: true,
        });
        assert!(ready.rejection.is_none(), "seed {seed}");
        assert!(recovery.state().recovered, "seed {seed}");
        assert_eq!(recovery.state().stage, RecoveryStage::Ready, "seed {seed}");
        assert_eq!(recovery.next_deadline(), None, "seed {seed}");
        healed += 1;
    }
    assert!(failed_responses >= 40);
    assert!(lost_responses >= 40);
    assert!(noisy_cooldowns >= 180);
    assert_eq!(healed, 64);
}
