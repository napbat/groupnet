//! Seeded virtual-time provisional builder and partition schedules.

use groupnet_core::placement;
use groupnet_core::volatile_bootstrap::{
    BootId, BootstrapClaim, BootstrapConfig, BootstrapEffect, BootstrapEvent, BootstrapMember,
    BootstrapOperation, BootstrapScope, BootstrapStage, ClaimEngine,
};
use groupnet_core::{NodeId, Time};
use groupnet_sim::SplitMix64;

fn config() -> BootstrapConfig {
    BootstrapConfig {
        max_members: 8,
        max_member_bytes: 32,
        max_scope_bytes: 64,
        settle_ms: 3,
        renew_ms: 4,
        claim_ttl_ms: 12,
        observe_ms: 5,
        donor_wait_ms: 8,
        total_ms: 30,
    }
}

fn scope() -> BootstrapScope {
    BootstrapScope {
        domain: "origin".to_owned(),
        partition: "bucket".to_owned(),
    }
}

fn claim(step: &groupnet_core::volatile_bootstrap::BootstrapStep) -> BootstrapClaim {
    step.effects
        .iter()
        .find_map(|effect| match effect {
            BootstrapEffect::PublishClaim(claim) => Some(claim.clone()),
            _ => None,
        })
        .expect("local claim")
}

fn observation(step: &groupnet_core::volatile_bootstrap::BootstrapStep) -> BootstrapOperation {
    step.effects
        .iter()
        .find_map(|effect| match effect {
            BootstrapEffect::ObserveClaims { op, .. } => Some(*op),
            _ => None,
        })
        .expect("roster observation")
}

fn decide(engines: &mut [ClaimEngine], names: &[NodeId], claims: &[BootstrapClaim]) -> usize {
    let members: Vec<_> = names
        .iter()
        .map(|node| BootstrapMember {
            node: node.clone(),
            eligible: true,
        })
        .collect();
    let mut builders = 0;
    for engine in engines {
        let tick = engine.step(BootstrapEvent::Tick(Time(3)));
        let decision = engine.step(BootstrapEvent::ClaimsObserved {
            op: observation(&tick),
            members: members.clone(),
            claims: claims.to_vec(),
        });
        assert!(decision.rejection.is_none());
        builders += usize::from(engine.stage() == BootstrapStage::Building);
    }
    builders
}

fn expected_builder(names: &[NodeId]) -> NodeId {
    placement::owner(&scope().placement_key(), &names.iter().cloned().collect()).unwrap()
}

#[test]
fn converged_claims_take_over_and_partitions_have_safe_duplicate_builders() {
    let mut connected = 0;
    let mut takeovers = 0;
    let mut partitions = 0;
    for seed in 0..64 {
        let mut rng = SplitMix64::new(seed);
        let count = usize::try_from(3 + rng.below(4)).unwrap();
        let names: Vec<_> = (0..count)
            .map(|index| NodeId::from(format!("n{index}")))
            .collect();
        let mut engines: Vec<_> = names
            .iter()
            .enumerate()
            .map(|(index, name)| {
                ClaimEngine::new(
                    config(),
                    scope(),
                    name.clone(),
                    BootId(u128::try_from(index + 1).unwrap()),
                    seed + 1,
                )
                .unwrap()
            })
            .collect();
        let claims: Vec<_> = engines
            .iter_mut()
            .map(|engine| claim(&engine.step(BootstrapEvent::Start)))
            .collect();
        assert_eq!(decide(&mut engines, &names, &claims), 1, "seed {seed}");
        connected += 1;
        let expected = expected_builder(&names);
        let builder = engines
            .iter()
            .position(|engine| engine.stage() == BootstrapStage::Building)
            .unwrap();
        assert_eq!(names[builder], expected, "seed {seed}");

        let identity = engines[builder].selected().unwrap().clone();
        let built_op = engines[builder].current_operation().unwrap();
        engines[builder].step(BootstrapEvent::BuildFailed {
            op: built_op,
            selected: identity.clone(),
        });
        let survivors: Vec<_> = claims
            .iter()
            .filter(|claim| claim.identity != identity)
            .cloned()
            .collect();
        let members: Vec<_> = names
            .iter()
            .map(|node| BootstrapMember {
                node: node.clone(),
                eligible: true,
            })
            .collect();
        let mut replacements = 0;
        for (index, engine) in engines.iter_mut().enumerate() {
            if index == builder {
                continue;
            }
            let follow_op = engine.current_operation().unwrap();
            let next = engine.step(BootstrapEvent::DonorUnavailable {
                op: follow_op,
                selected: identity.clone(),
            });
            let decision = engine.step(BootstrapEvent::ClaimsObserved {
                op: observation(&next),
                members: members.clone(),
                claims: survivors.clone(),
            });
            assert!(decision.rejection.is_none(), "seed {seed}");
            replacements += usize::from(engine.stage() == BootstrapStage::Building);
        }
        assert_eq!(replacements, 1, "seed {seed}");
        takeovers += 1;

        let split = 1 + usize::try_from(rng.below(u32::try_from(count - 1).unwrap())).unwrap();
        for part in [&names[..split], &names[split..]] {
            let mut local: Vec<_> = part
                .iter()
                .enumerate()
                .map(|(index, name)| {
                    ClaimEngine::new(
                        config(),
                        scope(),
                        name.clone(),
                        BootId(u128::try_from(index + 101).unwrap()),
                        seed + 100,
                    )
                    .unwrap()
                })
                .collect();
            let local_claims: Vec<_> = local
                .iter_mut()
                .map(|engine| claim(&engine.step(BootstrapEvent::Start)))
                .collect();
            assert_eq!(decide(&mut local, part, &local_claims), 1, "seed {seed}");
        }
        partitions += 1;
    }
    assert_eq!(connected, 64);
    assert_eq!(takeovers, 64);
    assert_eq!(partitions, 64);
}
