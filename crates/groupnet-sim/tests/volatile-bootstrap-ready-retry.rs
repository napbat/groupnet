//! A temporarily unavailable Ready donor cannot trigger a duplicate origin scan early.

use groupnet_core::volatile_bootstrap::{
    BootId, BootstrapClaim, BootstrapConfig, BootstrapEffect, BootstrapEvent, BootstrapMember,
    BootstrapScope, BootstrapStep, ClaimEngine, ClaimIdentity, ClaimPhase,
};
use groupnet_core::{NodeId, Time};
use groupnet_sim::SplitMix64;

fn claim(attempt: u64) -> BootstrapClaim {
    BootstrapClaim {
        identity: ClaimIdentity {
            node: NodeId::from("peer"),
            incarnation: BootId(8),
            session: 1,
            attempt,
        },
        renewal: 1,
        phase: ClaimPhase::Ready,
        progress: 0,
        remaining_ms: 10,
    }
}

fn observe(
    engine: &mut ClaimEngine,
    step: &BootstrapStep,
    claims: Vec<BootstrapClaim>,
) -> BootstrapStep {
    let op = step
        .effects
        .iter()
        .find_map(|effect| match effect {
            BootstrapEffect::ObserveClaims { op, .. } => Some(*op),
            _ => None,
        })
        .expect("fresh bounded source observation");
    engine.step(BootstrapEvent::ClaimsObserved {
        op,
        members: vec![
            BootstrapMember {
                node: NodeId::from("me"),
                eligible: true,
            },
            BootstrapMember {
                node: NodeId::from("peer"),
                eligible: true,
            },
        ],
        claims,
    })
}

fn unavailable(engine: &mut ClaimEngine) -> BootstrapStep {
    engine.step(BootstrapEvent::DonorUnavailable {
        op: engine.current_operation().unwrap(),
        selected: engine.selected().unwrap().clone(),
    })
}

#[test]
fn new_ready_attempt_can_win_but_never_extends_the_first_wait_budget() {
    let mut replacements = 0;
    for seed in 0..48 {
        let mut rng = SplitMix64::new(seed);
        let mut engine = ClaimEngine::new(
            BootstrapConfig {
                max_members: 2,
                max_member_bytes: 8,
                max_scope_bytes: 16,
                settle_ms: 2,
                renew_ms: 3,
                claim_ttl_ms: 10,
                observe_ms: 2,
                donor_wait_ms: 5,
                total_ms: 20,
            },
            BootstrapScope {
                domain: "o".into(),
                partition: "b".into(),
            },
            NodeId::from("me"),
            BootId(7),
            1,
        )
        .unwrap();
        let started = engine.step(BootstrapEvent::Start);
        let mut me = started
            .effects
            .iter()
            .find_map(|effect| match effect {
                BootstrapEffect::PublishClaim(claim) => Some(claim.clone()),
                _ => None,
            })
            .unwrap();
        let tick = engine.step(BootstrapEvent::Tick(Time(2)));
        let first = observe(&mut engine, &tick, vec![me.clone(), claim(1)]);
        assert!(first.effects.iter().any(|effect| matches!(effect,
            BootstrapEffect::DonorAvailable { selected, .. } if *selected == claim(1).identity)));
        let refused = unavailable(&mut engine);
        let deferred = observe(&mut engine, &refused, vec![me.clone(), claim(1)]);
        assert!(
            !deferred
                .effects
                .iter()
                .any(|effect| matches!(effect, BootstrapEffect::BuildOrigin { .. }))
        );
        let sample_at = 4 + u64::from(rng.below(2));
        let tick = engine.step(BootstrapEvent::Tick(Time(sample_at)));
        if let Some(renewed) = tick.effects.iter().find_map(|effect| match effect {
            BootstrapEffect::PublishClaim(claim) => Some(claim.clone()),
            _ => None,
        }) {
            me = renewed;
        }
        let replacement = observe(&mut engine, &tick, vec![me.clone(), claim(2)]);
        assert!(replacement.effects.iter().any(|effect| matches!(effect,
            BootstrapEffect::DonorAvailable { selected, .. } if *selected == claim(2).identity)));
        assert_eq!(
            engine.operation_deadline(engine.current_operation().unwrap()),
            Some(Time(7))
        );
        replacements += 1;
        let refused = unavailable(&mut engine);
        let deferred = observe(&mut engine, &refused, vec![me.clone(), claim(2)]);
        assert!(
            !deferred
                .effects
                .iter()
                .any(|effect| matches!(effect, BootstrapEffect::BuildOrigin { .. }))
        );
        let due = engine.step(BootstrapEvent::Tick(Time(7)));
        if let Some(renewed) = due.effects.iter().find_map(|effect| match effect {
            BootstrapEffect::PublishClaim(claim) => Some(claim.clone()),
            _ => None,
        }) {
            me = renewed;
        }
        let local = observe(&mut engine, &due, vec![me]);
        assert!(
            local
                .effects
                .iter()
                .any(|effect| matches!(effect, BootstrapEffect::BuildOrigin { .. }))
        );
    }
    assert_eq!(replacements, 48);
}
