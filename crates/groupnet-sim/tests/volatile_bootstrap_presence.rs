//! Delayed local capture retirement must not affect a replacement donor or participation.

use groupnet_core::volatile_bootstrap::{
    BootId, BootstrapClaim, BootstrapConfig, BootstrapEffect, BootstrapEvent, BootstrapMember,
    BootstrapOperation, BootstrapScope, BootstrapStage, ClaimEngine, ClaimIdentity,
};
use groupnet_core::{NodeId, Time};
use groupnet_sim::SplitMix64;

fn config() -> BootstrapConfig {
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
    }
}

fn published_claim(effects: &[BootstrapEffect]) -> BootstrapClaim {
    effects
        .iter()
        .find_map(|effect| match effect {
            BootstrapEffect::PublishClaim(claim) => Some(claim.clone()),
            _ => None,
        })
        .expect("a new episode publishes its exact claim")
}

fn observe_operation(effects: &[BootstrapEffect]) -> BootstrapOperation {
    effects
        .iter()
        .find_map(|effect| match effect {
            BootstrapEffect::ObserveClaims { op, .. } => Some(*op),
            _ => None,
        })
        .expect("settle produces an observation")
}

fn build_operation(effects: &[BootstrapEffect]) -> BootstrapOperation {
    effects
        .iter()
        .find_map(|effect| match effect {
            BootstrapEffect::BuildOrigin { op, .. } => Some(*op),
            _ => None,
        })
        .expect("the sole eligible member builds")
}

fn build_local(engine: &mut ClaimEngine, claim: &BootstrapClaim, now: u64) {
    let tick = engine.step(BootstrapEvent::Tick(Time(now)));
    let decision = engine.step(BootstrapEvent::ClaimsObserved {
        op: observe_operation(&tick.effects),
        members: vec![BootstrapMember {
            node: NodeId::from("me"),
            eligible: true,
        }],
        claims: vec![claim.clone()],
    });
    let built = engine.step(BootstrapEvent::Built {
        op: build_operation(&decision.effects),
        selected: claim.identity.clone(),
    });
    assert!(built.rejection.is_none());
    assert_eq!(engine.stage(), BootstrapStage::DonorAvailable);
}

#[test]
fn delayed_old_capture_events_cannot_retire_replacement_or_presence() {
    let mut stale_deliveries = 0;
    let mut replacement_donors = 0;
    for seed in 0..48 {
        let mut rng = SplitMix64::new(seed);
        let mut engine = ClaimEngine::new(
            config(),
            BootstrapScope {
                domain: "o".to_owned(),
                partition: "b".to_owned(),
            },
            NodeId::from("me"),
            BootId(1),
            1,
        )
        .unwrap();
        let first = published_claim(&engine.step(BootstrapEvent::Start).effects);
        build_local(&mut engine, &first, 2);
        let retired = engine.step(BootstrapEvent::CaptureRetired {
            selected: first.identity.clone(),
        });
        assert!(retired.rejection.is_none());
        assert!(retired.effects.iter().any(|effect| {
            matches!(effect, BootstrapEffect::WithdrawClaim(identity) if *identity == first.identity)
        }));
        assert!(
            !retired
                .effects
                .iter()
                .any(|effect| { matches!(effect, BootstrapEffect::WithdrawPresence(_)) })
        );

        let replacement = published_claim(&engine.step(BootstrapEvent::Start).effects);
        assert_ne!(replacement.identity, first.identity);
        // A queued old-capture callback can arrive before or after the new
        // candidate becomes available, with duplicate delivery.
        let early = rng.below(2) == 0;
        if early {
            reject_old(&mut engine, &first.identity);
            stale_deliveries += 1;
        }
        build_local(&mut engine, &replacement, 4);
        replacement_donors += 1;
        let duplicates = 1 + rng.below(3);
        for _ in 0..duplicates {
            reject_old(&mut engine, &first.identity);
            stale_deliveries += 1;
        }
        assert_eq!(engine.selected(), Some(&replacement.identity));
        assert_eq!(engine.stage(), BootstrapStage::DonorAvailable);
        let cancel = engine.step(BootstrapEvent::Cancel);
        assert!(
            cancel
                .effects
                .iter()
                .any(|effect| { matches!(effect, BootstrapEffect::WithdrawPresence(_)) })
        );
    }
    assert_eq!(replacement_donors, 48);
    assert!(stale_deliveries >= 48);
}

fn reject_old(engine: &mut ClaimEngine, old: &ClaimIdentity) {
    let stale = engine.step(BootstrapEvent::CaptureRetired {
        selected: old.clone(),
    });
    assert!(stale.rejection.is_some());
    assert!(stale.effects.is_empty());
}

#[test]
fn retired_peer_candidate_keeps_presence_and_rejects_old_capture_over_seeded_restarts() {
    for seed in 0..48 {
        let mut rng = SplitMix64::new(seed);
        let mut engine = ClaimEngine::new(
            config(),
            BootstrapScope {
                domain: "o".into(),
                partition: "b".into(),
            },
            NodeId::from("me"),
            BootId(1),
            1,
        )
        .unwrap();
        let first = published_claim(&engine.step(BootstrapEvent::Start).effects);
        build_local(&mut engine, &first, 2);
        let retired = engine.step(BootstrapEvent::RetireCandidate);
        assert_eq!(engine.stage(), BootstrapStage::Participating);
        assert!(
            retired
                .effects
                .contains(&BootstrapEffect::WithdrawClaim(first.identity.clone()))
        );
        assert!(
            !retired
                .effects
                .iter()
                .any(|effect| matches!(effect, BootstrapEffect::WithdrawPresence(_)))
        );
        let before = engine.step(BootstrapEvent::Tick(Time(3 + u64::from(rng.below(2)))));
        assert!(
            before
                .effects
                .iter()
                .any(|effect| matches!(effect, BootstrapEffect::PublishPresence(_)))
        );
        if rng.below(2) == 0 {
            reject_old(&mut engine, &first.identity);
        }
        let second = published_claim(&engine.step(BootstrapEvent::Start).effects);
        assert_ne!(first.identity, second.identity);
        reject_old(&mut engine, &first.identity);
        let fallback = engine.step(BootstrapEvent::RetireCandidate);
        assert!(
            fallback
                .effects
                .contains(&BootstrapEffect::WithdrawClaim(second.identity))
        );
        assert_eq!(engine.stage(), BootstrapStage::Participating);
        assert!(
            !fallback
                .effects
                .iter()
                .any(|effect| matches!(effect, BootstrapEffect::WithdrawPresence(_)))
        );
        let terminal = engine.step(BootstrapEvent::Cancel);
        assert!(
            terminal
                .effects
                .iter()
                .any(|effect| matches!(effect, BootstrapEffect::WithdrawPresence(_)))
        );
    }
}
