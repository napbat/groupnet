//! Shaped virtual-time claim callback and takeover scenarios.

use groupnet_core::placement;
use groupnet_core::volatile_bootstrap::{
    BootstrapClaim, BootstrapConfig, BootstrapEffect, BootstrapEvent, BootstrapMember,
    BootstrapOperation, BootstrapScope, BootstrapStage, ClaimEngine, ClaimPhase,
};
use groupnet_core::{NodeId, Time};
use groupnet_sim::SplitMix64;

fn config() -> BootstrapConfig {
    BootstrapConfig {
        max_members: 3,
        max_member_bytes: 8,
        max_scope_bytes: 16,
        settle_ms: 3,
        renew_ms: 4,
        claim_ttl_ms: 12,
        observe_ms: 2,
        donor_wait_ms: 8,
        total_ms: 25,
    }
}

fn scope() -> BootstrapScope {
    BootstrapScope {
        domain: "o".to_owned(),
        partition: "b".to_owned(),
    }
}

fn claim(effects: &[BootstrapEffect]) -> BootstrapClaim {
    effects
        .iter()
        .find_map(|effect| match effect {
            BootstrapEffect::PublishClaim(claim) => Some(claim.clone()),
            _ => None,
        })
        .expect("published claim")
}

fn observe(effects: &[BootstrapEffect]) -> BootstrapOperation {
    effects
        .iter()
        .find_map(|effect| match effect {
            BootstrapEffect::ObserveClaims { op, .. } => Some(*op),
            _ => None,
        })
        .expect("queued observation")
}

fn members(names: &[NodeId]) -> Vec<BootstrapMember> {
    names
        .iter()
        .map(|node| BootstrapMember {
            node: node.clone(),
            eligible: true,
        })
        .collect()
}

fn initialized(
    seed: u64,
) -> (
    [NodeId; 3],
    Vec<ClaimEngine>,
    Vec<BootstrapClaim>,
    usize,
    usize,
) {
    let names = [NodeId::from("a"), NodeId::from("b"), NodeId::from("c")];
    let mut engines: Vec<_> = names
        .iter()
        .enumerate()
        .map(|(index, node)| {
            ClaimEngine::new(
                config(),
                scope(),
                node.clone(),
                groupnet_core::volatile_bootstrap::BootId(u128::try_from(index + 1).unwrap()),
                seed + 1,
            )
            .unwrap()
        })
        .collect();
    let claims: Vec<_> = engines
        .iter_mut()
        .map(|engine| claim(&engine.step(BootstrapEvent::Start).effects))
        .collect();
    for engine in &mut engines {
        let due = engine.step(BootstrapEvent::Tick(Time(3)));
        let result = engine.step(BootstrapEvent::ClaimsObserved {
            op: observe(&due.effects),
            members: members(&names),
            claims: claims.clone(),
        });
        assert!(result.rejection.is_none(), "seed {seed}");
    }
    let builder = engines
        .iter()
        .position(|engine| engine.stage() == BootstrapStage::Building)
        .unwrap();
    assert_eq!(
        engines
            .iter()
            .filter(|engine| engine.stage() == BootstrapStage::Building)
            .count(),
        1
    );
    assert_eq!(
        names[builder],
        placement::owner(&scope().placement_key(), &names.iter().cloned().collect()).unwrap()
    );
    let follower = (0..3).find(|index| *index != builder).unwrap();
    (names, engines, claims, builder, follower)
}

#[test]
fn scripted_lost_and_reordered_claim_replies_preserve_selection_and_heal() {
    let mut lost_polls = 0;
    let mut stale_replies = 0;
    let mut ready_donors = 0;
    let mut takeovers = 0;
    for seed in 0..64 {
        let mut rng = SplitMix64::new(seed);
        let (names, mut engines, mut claims, builder, follower) = initialized(seed);
        let donor_identity = engines[builder].selected().unwrap().clone();
        if rng.below(4) == 0 {
            let build_op = engines[builder].current_operation().unwrap();
            engines[builder].step(BootstrapEvent::BuildFailed {
                op: build_op,
                selected: donor_identity.clone(),
            });
            claims.remove(builder);
            let follow_op = engines[follower].current_operation().unwrap();
            let due = engines[follower].step(BootstrapEvent::DonorUnavailable {
                op: follow_op,
                selected: donor_identity.clone(),
            });
            let replacement = engines[follower].step(BootstrapEvent::ClaimsObserved {
                op: observe(&due.effects),
                members: members(&names),
                claims,
            });
            assert!(replacement.rejection.is_none());
            assert_ne!(engines[follower].selected(), Some(&donor_identity));
            takeovers += 1;
            continue;
        }
        engines[builder].step(BootstrapEvent::Tick(Time(4)));
        let build_op = engines[builder].current_operation().unwrap();
        let built = engines[builder].step(BootstrapEvent::Built {
            op: build_op,
            selected: donor_identity,
        });
        let ready = claim(&built.effects);
        assert_eq!(ready.phase, ClaimPhase::Ready);
        claims[builder] = ready.clone();
        let first_poll = engines[follower].step(BootstrapEvent::Tick(Time(5)));
        let first_op = observe(&first_poll.effects);
        claims[follower] = claim(&first_poll.effects);
        let dropped = rng.below(2) == 0;
        if dropped {
            // The first source reply is lost; a finite core timer reissues it.
            lost_polls += 1;
            let retry = engines[follower].step(BootstrapEvent::Tick(Time(7)));
            let result = engines[follower].step(BootstrapEvent::ClaimsObserved {
                op: observe(&retry.effects),
                members: members(&names),
                claims: claims.clone(),
            });
            assert!(result.rejection.is_none());
        } else {
            let result = engines[follower].step(BootstrapEvent::ClaimsObserved {
                op: first_op,
                members: members(&names),
                claims: claims.clone(),
            });
            assert!(result.rejection.is_none());
        }
        assert_eq!(engines[follower].stage(), BootstrapStage::DonorAvailable);
        assert_eq!(engines[follower].selected(), Some(&ready.identity));
        ready_donors += 1;
        if dropped {
            let duplicate = engines[follower].step(BootstrapEvent::ClaimsObserved {
                op: first_op,
                members: members(&names),
                claims,
            });
            assert!(duplicate.rejection.is_some());
            stale_replies += 1;
        }
    }
    assert!(lost_polls >= 10);
    assert!(stale_replies >= 10);
    assert!(ready_donors >= 35);
    assert!(takeovers >= 5);
}
