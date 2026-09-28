//! Seeded full-rescan retry schedules under one absolute recovery budget.

use std::collections::VecDeque;

use groupnet_core::NodeId;
use groupnet_core::Time;
use groupnet_core::volatile_recovery::{
    RecoveryConfig, RecoveryEffect, RecoveryEngine, RecoveryEvent, RecoveryMode, RecoveryOperation,
    RecoveryStage,
};
use groupnet_sim::SplitMix64;

#[test]
fn transient_full_scan_failures_retry_without_reviving_old_operations() {
    let mut recovered = 0;
    let mut exhausted = 0;
    for seed in 0..48 {
        let mut rng = SplitMix64::new(seed);
        let failures = rng.below(16);
        let config = RecoveryConfig {
            max_members: 2,
            max_member_bytes: 16,
            max_barrier_rounds: 2,
            total_ms: 35,
            attempt_ms: 8,
            settle_ms: 2,
            poll_ms: 3,
        };
        let mut engine =
            RecoveryEngine::new(config, RecoveryMode::Unleased, NodeId::from("me"), seed + 1)
                .unwrap();
        let mut effects: VecDeque<_> = engine.step(RecoveryEvent::Start).effects.into();
        let mut stale: Option<RecoveryOperation> = None;
        let mut attempts = 0;
        for time in 0..=35 {
            let mut immediate = 0;
            while let Some(effect) = effects.pop_front() {
                immediate += 1;
                assert!(immediate < 12, "seed {seed}: unbounded immediate work");
                match effect {
                    RecoveryEffect::CloseGate { .. } => assert!(!engine.state().recovered),
                    RecoveryEffect::Invalidate { op, .. } => {
                        effects.extend(engine.step(RecoveryEvent::Invalidated { op }).effects);
                    }
                    RecoveryEffect::RebuildOrigin { op } => {
                        attempts += 1;
                        if attempts <= failures {
                            stale = Some(op);
                            effects.extend(engine.step(RecoveryEvent::Failed { op }).effects);
                        } else {
                            if let Some(old) = stale {
                                assert_ne!(old, op);
                                assert!(!engine.accepts_operation(old));
                                assert!(
                                    engine
                                        .step(RecoveryEvent::Materialized { op: old })
                                        .rejection
                                        .is_some()
                                );
                            }
                            effects.extend(engine.step(RecoveryEvent::Materialized { op }).effects);
                        }
                    }
                    RecoveryEffect::Affirm { op } => {
                        effects.extend(
                            engine
                                .step(RecoveryEvent::Affirmed { op, accepted: true })
                                .effects,
                        );
                    }
                    RecoveryEffect::ArmTimer(_) => {}
                    RecoveryEffect::AcquireBaseline { .. }
                    | RecoveryEffect::ObservePeerHeads { .. }
                    | RecoveryEffect::CancelBaseline { .. }
                    | RecoveryEffect::ObservePeers { .. }
                    | RecoveryEffect::WaitFrontiers { .. } => {
                        panic!("seed {seed}: full plan requested lapse proof")
                    }
                }
            }
            if engine.state().stage == RecoveryStage::Ready {
                recovered += 1;
                break;
            }
            if engine.state().stage == RecoveryStage::OriginOnly {
                exhausted += 1;
                break;
            }
            effects.extend(engine.step(RecoveryEvent::Tick(Time(time + 1))).effects);
        }
    }
    assert!(recovered > 0);
    assert!(exhausted > 0);
}
