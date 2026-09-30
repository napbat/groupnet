//! Seeded volatile-lease recovery with bounded grants, heads, and fallback.

use std::collections::VecDeque;

use groupnet_core::NodeId;
use groupnet_core::Time;
use groupnet_core::volatile_recovery::{
    Mark, Peer, RecoveryConfig, RecoveryEffect, RecoveryEngine, RecoveryEvent, RecoveryMode,
    RecoveryOperation, RecoveryStage, RecoveryStep,
};
use groupnet_sim::SplitMix64;

fn mark(sequence: u64) -> Mark {
    Mark { epoch: 1, sequence }
}

fn peer(name: &str, grant: u64, head: u64, alive: bool) -> Peer {
    Peer {
        node: NodeId::from(name),
        alive,
        grants_lease: true,
        old_nonlive: false,
        grant: Some(mark(grant)),
        head: Some(mark(head)),
    }
}

fn op(step: &RecoveryStep) -> RecoveryOperation {
    step.effects
        .iter()
        .find_map(|effect| match effect {
            RecoveryEffect::Invalidate { op, .. }
            | RecoveryEffect::RebuildOrigin { op }
            | RecoveryEffect::ObservePeers { op }
            | RecoveryEffect::WaitFrontiers { op, .. }
            | RecoveryEffect::Affirm { op } => Some(*op),
            RecoveryEffect::CloseGate { .. }
            | RecoveryEffect::CancelBaseline { .. }
            | RecoveryEffect::SuspendLocalBaseline { .. }
            | RecoveryEffect::ResumeLocalBaseline { .. }
            | RecoveryEffect::FellBack { .. }
            | RecoveryEffect::ArmTimer(_) => None,
            RecoveryEffect::AcquireBaseline { .. } | RecoveryEffect::ObservePeerHeads { .. } => {
                panic!("default recovery cannot request peer bootstrap")
            }
        })
        .expect("operation effect")
}

fn queue(effects: &mut VecDeque<RecoveryEffect>, step: RecoveryStep, seed: u64) {
    assert!(step.rejection.is_none(), "seed {seed}: {step:?}");
    effects.extend(step.effects);
}

fn warm(engine: &mut RecoveryEngine) {
    let start = engine.step(RecoveryEvent::Start);
    let invalid = engine.step(RecoveryEvent::Invalidated { op: op(&start) });
    let materialized = engine.step(RecoveryEvent::Materialized { op: op(&invalid) });
    let affirmed = engine.step(RecoveryEvent::Affirmed {
        op: op(&materialized),
        accepted: true,
    });
    assert!(affirmed.rejection.is_none());
    assert!(engine.state().recovered);
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one seeded lease-recovery schedule varies renewal delay, head movement, disappearance, and failure"
)]
fn volatile_recovery_reaches_applied_heads_or_fences_full_fallback() {
    let mut cheap = 0;
    let mut fallback = 0;
    let mut delayed_grant = 0;
    let mut moved_head = 0;
    let mut vanished = 0;
    for seed in 0..64 {
        let mut rng = SplitMix64::new(seed);
        let heal_at = 2 + u64::from(rng.below(6));
        let fail_observation = seed % 5 == 0;
        let disappear = seed % 7 == 0;
        let advance_once = seed % 3 == 0;
        let config = RecoveryConfig {
            max_members: 4,
            max_member_bytes: 32,
            max_barrier_rounds: 3,
            total_ms: 60,
            attempt_ms: 12,
            settle_ms: 3,
            poll_ms: 2,
        };
        let mut engine =
            RecoveryEngine::new(config, RecoveryMode::Leased, NodeId::from("me"), seed + 1)
                .expect("valid config");
        warm(&mut engine);
        let mut effects = VecDeque::new();
        let mut timers = Vec::new();
        let mut source_head = 1;
        let mut applied_head = 1;
        let mut moved = false;
        let mut fell_back = false;
        let mut reported_fallback = false;
        let mut observed_failure = false;
        let mut observed_disappearance = false;
        queue(
            &mut effects,
            engine.step(RecoveryEvent::LeaseLapse { count: 1 }),
            seed,
        );
        for time in 0..=120 {
            if timers.contains(&Time(time)) {
                timers.retain(|due| *due != Time(time));
                queue(
                    &mut effects,
                    engine.step(RecoveryEvent::Tick(Time(time))),
                    seed,
                );
            }
            let mut immediate = 0;
            while let Some(effect) = effects.pop_front() {
                immediate += 1;
                assert!(immediate < 24, "seed {seed}: unbounded immediate work");
                match effect {
                    RecoveryEffect::CloseGate { .. } => {
                        assert!(!engine.state().recovered);
                    }
                    RecoveryEffect::Invalidate {
                        op,
                        distrust_bodies,
                    } => {
                        fell_back |= distrust_bodies;
                        queue(
                            &mut effects,
                            engine.step(RecoveryEvent::Invalidated { op }),
                            seed,
                        );
                    }
                    RecoveryEffect::RebuildOrigin { op } => {
                        applied_head = source_head;
                        queue(
                            &mut effects,
                            engine.step(RecoveryEvent::Materialized { op }),
                            seed,
                        );
                    }
                    RecoveryEffect::ObservePeers { op } => {
                        let stage = engine.state().stage;
                        if fail_observation
                            && !observed_failure
                            && stage == RecoveryStage::WaitingRenewals
                        {
                            observed_failure = true;
                            queue(
                                &mut effects,
                                engine.step(RecoveryEvent::Failed { op }),
                                seed,
                            );
                            continue;
                        }
                        let advanced = time >= heal_at;
                        delayed_grant +=
                            usize::from(!advanced && stage == RecoveryStage::WaitingRenewals);
                        let grant = if advanced { 2 } else { 1 };
                        let mut peers = vec![
                            peer("a", grant, source_head, true),
                            peer("b", grant, source_head, seed % 4 != 0),
                        ];
                        if disappear
                            && stage == RecoveryStage::SamplingHeads
                            && !observed_disappearance
                        {
                            peers.pop();
                            observed_disappearance = true;
                            vanished += 1;
                        }
                        let confirmed = Some(mark(grant));
                        queue(
                            &mut effects,
                            engine.step(RecoveryEvent::PeersObserved {
                                op,
                                peers,
                                confirmed,
                            }),
                            seed,
                        );
                    }
                    RecoveryEffect::WaitFrontiers { op, heads } => {
                        for (_, head) in heads {
                            applied_head = applied_head.max(head.sequence);
                        }
                        if advance_once && !moved {
                            source_head = 2;
                            moved = true;
                            moved_head += 1;
                        }
                        queue(
                            &mut effects,
                            engine.step(RecoveryEvent::FrontiersReached { op }),
                            seed,
                        );
                    }
                    RecoveryEffect::Affirm { op } => {
                        queue(
                            &mut effects,
                            engine.step(RecoveryEvent::Affirmed { op, accepted: true }),
                            seed,
                        );
                    }
                    RecoveryEffect::ArmTimer(due) => {
                        assert!(due >= Time(time), "seed {seed}: backward timer");
                        timers.push(due);
                    }
                    RecoveryEffect::FellBack { .. } => {
                        reported_fallback = true;
                    }
                    RecoveryEffect::AcquireBaseline { .. }
                    | RecoveryEffect::ObservePeerHeads { .. }
                    | RecoveryEffect::CancelBaseline { .. }
                    | RecoveryEffect::SuspendLocalBaseline { .. }
                    | RecoveryEffect::ResumeLocalBaseline { .. } => {
                        panic!("seed {seed}: default recovery attempted peer bootstrap")
                    }
                }
            }
            if engine.state().stage == RecoveryStage::Ready {
                assert!(engine.state().recovered);
                assert!(applied_head >= source_head, "seed {seed}: stale index");
                if fell_back {
                    assert!(
                        reported_fallback,
                        "seed {seed}: a full fallback was not reported"
                    );
                    fallback += 1;
                } else {
                    cheap += 1;
                }
                break;
            }
            assert!(time < 120, "seed {seed}: no finite recovery");
        }
    }
    assert!(cheap > 0 && fallback > 0);
    assert!(delayed_grant > 0 && moved_head > 0 && vanished > 0);
}
