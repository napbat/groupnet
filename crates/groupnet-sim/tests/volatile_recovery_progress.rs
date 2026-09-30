//! Seeded paged-scan schedules: progress keeps one scan alive past every fixed
//! bound, and only a stall longer than the attempt bound retries it.

use std::collections::VecDeque;

use groupnet_core::NodeId;
use groupnet_core::Time;
use groupnet_core::volatile_recovery::{
    RecoveryConfig, RecoveryEffect, RecoveryEngine, RecoveryError, RecoveryEvent, RecoveryMode,
    RecoveryOperation, RecoveryStage,
};
use groupnet_sim::SplitMix64;

const CONFIG: RecoveryConfig = RecoveryConfig {
    max_members: 2,
    max_member_bytes: 16,
    max_barrier_rounds: 2,
    total_ms: 40,
    attempt_ms: 8,
    settle_ms: 2,
    poll_ms: 3,
};

/// One origin scan bound to its exact operation. A retried operation is a
/// fresh scan from page zero, as the adapter contract has no resume cursor.
struct Scan {
    op: RecoveryOperation,
    done: u32,
    next_page: u64,
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one seeded scan schedule keeps its effect loop and floors together"
)]
fn progressing_scan_outlives_every_fixed_bound_and_only_a_stall_retries() {
    let (mut one_pass, mut retried) = (0, 0);
    for seed in 0..96 {
        let mut rng = SplitMix64::new(seed);
        let pages = 50 + rng.below(40);
        let stall_at = (seed % 3 == 0).then(|| rng.below(pages));
        let stall_ms = CONFIG.attempt_ms + u64::from(rng.below(5));
        let mut stalled = false;
        let mut engine =
            RecoveryEngine::new(CONFIG, RecoveryMode::Unleased, NodeId::from("me"), seed + 1)
                .unwrap();
        let mut effects: VecDeque<_> = engine.step(RecoveryEvent::Start).effects.into();
        let mut scan: Option<Scan> = None;
        let mut rebuilds = 0;
        let mut started_at = None;
        let mut ready_at = None;
        for now in 0..20_000_u64 {
            // Tick first so work the tick issues starts at the same instant.
            effects.extend(engine.step(RecoveryEvent::Tick(Time(now))).effects);
            while let Some(effect) = effects.pop_front() {
                match effect {
                    RecoveryEffect::CloseGate { .. } | RecoveryEffect::ArmTimer(_) => {}
                    RecoveryEffect::Invalidate { op, .. } => {
                        effects.extend(engine.step(RecoveryEvent::Invalidated { op }).effects);
                    }
                    RecoveryEffect::RebuildOrigin { op } => {
                        rebuilds += 1;
                        started_at.get_or_insert(now);
                        if let Some(old) = scan.take() {
                            assert!(!engine.accepts_operation(old.op), "seed {seed}");
                        }
                        scan = Some(Scan {
                            op,
                            done: 0,
                            next_page: now + 1 + u64::from(rng.below(7)),
                        });
                    }
                    RecoveryEffect::Affirm { op } => {
                        effects.extend(
                            engine
                                .step(RecoveryEvent::Affirmed { op, accepted: true })
                                .effects,
                        );
                    }
                    other => panic!("seed {seed}: full plan requested {other:?}"),
                }
            }
            if engine.state().stage == RecoveryStage::Ready {
                ready_at = Some(now);
                break;
            }
            assert_ne!(
                engine.state().stage,
                RecoveryStage::OriginOnly,
                "seed {seed}: a scan that progresses or stalls once never exhausts"
            );
            let Some(current) = scan.as_mut().filter(|scan| scan.next_page == now) else {
                continue;
            };
            let op = current.op;
            if !engine.accepts_operation(op) {
                // The stalled page lands after its attempt expired: its
                // report is fenced and a fresh operation rescans.
                assert_eq!(
                    engine.step(RecoveryEvent::Progressed { op }).rejection,
                    Some(RecoveryError::StaleOperation),
                    "seed {seed}"
                );
                assert!(
                    engine
                        .step(RecoveryEvent::Materialized { op })
                        .rejection
                        .is_some()
                );
                scan = None;
                continue;
            }
            current.done += 1;
            if current.done == pages {
                effects.extend(engine.step(RecoveryEvent::Materialized { op }).effects);
                scan = None;
                continue;
            }
            let step = engine.step(RecoveryEvent::Progressed { op });
            assert_eq!(step.rejection, None, "seed {seed}");
            effects.extend(step.effects);
            let gap = if stall_at == Some(current.done) && !stalled {
                stalled = true;
                stall_ms
            } else {
                1 + u64::from(rng.below(u32::try_from(CONFIG.attempt_ms - 1).unwrap()))
            };
            current.next_page = now + gap;
        }
        let ready_at = ready_at.unwrap_or_else(|| panic!("seed {seed}: scan never completed"));
        if stalled {
            assert_eq!(rebuilds, 2, "seed {seed}: one stall costs one rescan");
            retried += 1;
        } else {
            assert_eq!(
                rebuilds, 1,
                "seed {seed}: a progressing scan is never restarted"
            );
            assert!(
                ready_at - started_at.unwrap() > CONFIG.total_ms,
                "seed {seed}: schedule must outlast the fixed episode budget"
            );
            one_pass += 1;
        }
    }
    assert!(one_pass >= 48, "one-pass schedules {one_pass}");
    assert!(retried >= 16, "stalled schedules {retried}");
}

#[test]
fn progress_is_refused_outside_a_live_rebuild_or_baseline() {
    let mut engine =
        RecoveryEngine::new(CONFIG, RecoveryMode::Unleased, NodeId::from("me"), 1).unwrap();
    let start = engine.step(RecoveryEvent::Start);
    let invalidate = start
        .effects
        .iter()
        .find_map(|effect| match effect {
            RecoveryEffect::Invalidate { op, .. } => Some(*op),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        engine
            .step(RecoveryEvent::Progressed { op: invalidate })
            .rejection,
        Some(RecoveryError::Stage),
        "invalidation is not a long-running scan"
    );
    let rebuild = engine
        .step(RecoveryEvent::Invalidated { op: invalidate })
        .effects
        .iter()
        .find_map(|effect| match effect {
            RecoveryEffect::RebuildOrigin { op } => Some(*op),
            _ => None,
        })
        .unwrap();
    let foreign = RecoveryOperation {
        token: rebuild.token + 1,
        ..rebuild
    };
    assert_eq!(
        engine
            .step(RecoveryEvent::Progressed { op: foreign })
            .rejection,
        Some(RecoveryError::StaleOperation)
    );
    // Renewal is measured from the reported time and never shortens a bound.
    engine.step(RecoveryEvent::Tick(Time(5)));
    let renewed = engine.step(RecoveryEvent::Progressed { op: rebuild });
    assert_eq!(renewed.rejection, None);
    assert_eq!(
        engine.next_deadline(),
        Some(Time(5 + CONFIG.attempt_ms)),
        "stall bound restarts at the report"
    );
    // A gap fences the old scan: its later report cannot revive it.
    engine.step(RecoveryEvent::FeedGap { lapses: 0 });
    assert_eq!(
        engine
            .step(RecoveryEvent::Progressed { op: rebuild })
            .rejection,
        Some(RecoveryError::StaleOperation)
    );
}
