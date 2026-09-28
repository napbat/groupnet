use super::*;
use crate::NodeId;
use crate::placement;

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

fn node(name: &str, boot: u64) -> ClaimEngine {
    ClaimEngine::new(
        config(),
        scope(),
        NodeId::from(name),
        BootId(u128::from(boot)),
        1,
    )
    .unwrap()
}

fn published(step: &BootstrapStep) -> BootstrapClaim {
    step.effects
        .iter()
        .find_map(|effect| match effect {
            BootstrapEffect::PublishClaim(claim) => Some(claim.clone()),
            _ => None,
        })
        .expect("claim publication")
}

fn observed_op(step: &BootstrapStep) -> BootstrapOperation {
    step.effects
        .iter()
        .find_map(|effect| match effect {
            BootstrapEffect::ObserveClaims { op, .. } => Some(*op),
            _ => None,
        })
        .expect("observation operation")
}

fn roster(names: &[&str]) -> Vec<BootstrapMember> {
    names
        .iter()
        .map(|name| BootstrapMember {
            node: NodeId::from(*name),
            eligible: true,
        })
        .collect()
}

#[test]
fn scope_encoding_and_constructor_bounds_are_unambiguous() {
    assert_ne!(
        BootstrapScope {
            domain: "a".to_owned(),
            partition: "bc".to_owned()
        }
        .placement_key(),
        BootstrapScope {
            domain: "ab".to_owned(),
            partition: "c".to_owned()
        }
        .placement_key()
    );
    assert_eq!(
        ClaimEngine::new(config(), scope(), NodeId::from("me"), BootId(0), 1).err(),
        Some(BootstrapError::InvalidConfig)
    );
    assert_eq!(
        ClaimEngine::new(
            BootstrapConfig {
                max_members: usize::MAX,
                ..config()
            },
            scope(),
            NodeId::from("me"),
            BootId(1),
            1
        )
        .err(),
        Some(BootstrapError::InvalidConfig)
    );
}

#[test]
fn converged_claims_select_exactly_one_provisional_builder() {
    let names = ["a", "b", "c"];
    let mut engines: Vec<_> = names
        .iter()
        .enumerate()
        .map(|(index, name)| node(name, u64::try_from(index + 1).unwrap()))
        .collect();
    let claims: Vec<_> = engines
        .iter_mut()
        .map(|engine| published(&engine.step(BootstrapEvent::Start)))
        .collect();
    let expected = placement::owner(
        &scope().placement_key(),
        &names.iter().map(|name| NodeId::from(*name)).collect(),
    )
    .unwrap();
    let mut builders = 0;
    for (index, engine) in engines.iter_mut().enumerate() {
        let tick = engine.step(BootstrapEvent::Tick(crate::Time(3)));
        let decision = engine.step(BootstrapEvent::ClaimsObserved {
            op: observed_op(&tick),
            members: roster(&names),
            claims: claims.clone(),
        });
        assert!(decision.rejection.is_none());
        assert_eq!(engine.selected().unwrap().node, expected);
        if names[index] == expected.as_str() {
            assert_eq!(engine.stage(), BootstrapStage::Building);
            assert!(
                decision
                    .effects
                    .iter()
                    .any(|effect| matches!(effect, BootstrapEffect::BuildOrigin { .. }))
            );
            builders += 1;
        } else {
            assert_eq!(engine.stage(), BootstrapStage::Following);
            assert!(
                decision
                    .effects
                    .iter()
                    .any(|effect| matches!(effect, BootstrapEffect::FollowBuilder { .. }))
            );
        }
    }
    assert_eq!(builders, 1);
}

#[test]
fn exact_operation_deadline_excludes_earlier_renewal_timer() {
    let mut engine = node("me", 7);
    engine.step(BootstrapEvent::Start);
    let tick = engine.step(BootstrapEvent::Tick(crate::Time(3)));
    let op = observed_op(&tick);
    assert_eq!(engine.next_deadline(), Some(crate::Time(4)));
    assert_eq!(engine.operation_deadline(op), Some(crate::Time(8)));
    let stale = BootstrapOperation {
        token: op.token + 1,
        ..op
    };
    assert_eq!(engine.operation_deadline(stale), None);
}

#[test]
fn failed_builder_withdraws_and_exact_followers_choose_one_takeover() {
    let names = ["a", "b", "c"];
    let mut engines: Vec<_> = names
        .iter()
        .enumerate()
        .map(|(index, name)| node(name, u64::try_from(index + 1).unwrap()))
        .collect();
    let claims: Vec<_> = engines
        .iter_mut()
        .map(|engine| published(&engine.step(BootstrapEvent::Start)))
        .collect();
    for engine in &mut engines {
        let tick = engine.step(BootstrapEvent::Tick(crate::Time(3)));
        engine.step(BootstrapEvent::ClaimsObserved {
            op: observed_op(&tick),
            members: roster(&names),
            claims: claims.clone(),
        });
    }
    let builder = engines
        .iter()
        .position(|engine| engine.stage() == BootstrapStage::Building)
        .unwrap();
    let failed_identity = engines[builder].selected().unwrap().clone();
    let failed_op = engines[builder].current_operation().unwrap();
    let failed = engines[builder].step(BootstrapEvent::BuildFailed {
        op: failed_op,
        selected: failed_identity.clone(),
    });
    assert_eq!(engines[builder].stage(), BootstrapStage::Fallback);
    assert!(failed.effects.iter().any(|effect| matches!(
        effect,
        BootstrapEffect::WithdrawClaim(id) if *id == failed_identity
    )));

    let remaining: Vec<_> = claims
        .into_iter()
        .filter(|claim| claim.identity != failed_identity)
        .collect();
    let mut takeovers = 0;
    for (index, engine) in engines.iter_mut().enumerate() {
        if index == builder {
            continue;
        }
        let follow_op = engine.current_operation().unwrap();
        let wrong = ClaimIdentity {
            attempt: failed_identity.attempt + 1,
            ..failed_identity.clone()
        };
        assert_eq!(
            engine
                .step(BootstrapEvent::DonorUnavailable {
                    op: follow_op,
                    selected: wrong,
                })
                .rejection,
            Some(BootstrapError::StaleOperation)
        );
        let next = engine.step(BootstrapEvent::DonorUnavailable {
            op: follow_op,
            selected: failed_identity.clone(),
        });
        assert!(
            matches!(next.effects.first(), Some(BootstrapEffect::CancelWork { op }) if *op == follow_op)
        );
        let decision = engine.step(BootstrapEvent::ClaimsObserved {
            op: observed_op(&next),
            members: roster(&names),
            claims: remaining.clone(),
        });
        assert!(decision.rejection.is_none());
        takeovers += usize::from(engine.stage() == BootstrapStage::Building);
    }
    assert_eq!(takeovers, 1);
}

#[test]
fn retained_old_boot_claim_and_late_callback_cannot_reopen_new_boot() {
    let mut old = node("me", 1);
    let old_claim = published(&old.step(BootstrapEvent::Start));
    let old_observe = old.step(BootstrapEvent::Tick(crate::Time(3)));
    let old_decision = old.step(BootstrapEvent::ClaimsObserved {
        op: observed_op(&old_observe),
        members: roster(&["me"]),
        claims: vec![old_claim.clone()],
    });
    assert!(old_decision.rejection.is_none());
    let old_op = old.current_operation().unwrap();

    let mut fresh = node("me", 2);
    let fresh_claim = published(&fresh.step(BootstrapEvent::Start));
    let fresh_observe = fresh.step(BootstrapEvent::Tick(crate::Time(3)));
    let fresh_decision = fresh.step(BootstrapEvent::ClaimsObserved {
        op: observed_op(&fresh_observe),
        members: roster(&["me"]),
        claims: vec![old_claim.clone(), fresh_claim],
    });
    assert!(fresh_decision.rejection.is_none());
    assert_eq!(fresh.stage(), BootstrapStage::Building);
    assert_eq!(
        fresh
            .step(BootstrapEvent::Built {
                op: old_op,
                selected: old_claim.identity.clone()
            })
            .rejection,
        Some(BootstrapError::StaleOperation)
    );
    assert_eq!(fresh.stage(), BootstrapStage::Building);
}

#[test]
fn fresh_same_boot_session_rejects_old_claim_and_completion() {
    let mut old = node("me", 7);
    let old_claim = published(&old.step(BootstrapEvent::Start));
    let old_tick = old.step(BootstrapEvent::Tick(crate::Time(3)));
    old.step(BootstrapEvent::ClaimsObserved {
        op: observed_op(&old_tick),
        members: roster(&["me"]),
        claims: vec![old_claim.clone()],
    });
    let old_op = old.current_operation().unwrap();

    let mut fresh = ClaimEngine::new(config(), scope(), NodeId::from("me"), BootId(7), 2).unwrap();
    let fresh_claim = published(&fresh.step(BootstrapEvent::Start));
    assert_ne!(old_claim.identity, fresh_claim.identity);
    let fresh_tick = fresh.step(BootstrapEvent::Tick(crate::Time(3)));
    let result = fresh.step(BootstrapEvent::ClaimsObserved {
        op: observed_op(&fresh_tick),
        members: roster(&["me"]),
        claims: vec![old_claim.clone(), fresh_claim],
    });
    assert!(result.rejection.is_none());
    assert_eq!(fresh.stage(), BootstrapStage::Building);
    assert_eq!(
        fresh
            .step(BootstrapEvent::Built {
                op: old_op,
                selected: old_claim.identity.clone()
            })
            .rejection,
        Some(BootstrapError::StaleOperation)
    );
}

#[test]
fn builder_timeout_falls_back_without_duplicate_origin_build() {
    let mut engine = node("me", 7);
    let local = published(&engine.step(BootstrapEvent::Start));
    let tick = engine.step(BootstrapEvent::Tick(crate::Time(3)));
    let decision = engine.step(BootstrapEvent::ClaimsObserved {
        op: observed_op(&tick),
        members: roster(&["me"]),
        claims: vec![local],
    });
    assert_eq!(engine.stage(), BootstrapStage::Building);
    let build_op = engine.current_operation().unwrap();
    assert_eq!(
        decision
            .effects
            .iter()
            .filter(|effect| matches!(effect, BootstrapEffect::BuildOrigin { .. }))
            .count(),
        1
    );
    let selected = engine.selected().unwrap().clone();
    let timed_out = engine.step(BootstrapEvent::Tick(crate::Time(11)));
    assert_eq!(engine.stage(), BootstrapStage::Fallback);
    assert!(
        timed_out
            .effects
            .iter()
            .any(|effect| matches!(effect, BootstrapEffect::CancelWork { op } if *op == build_op))
    );
    assert!(
        timed_out
            .effects
            .iter()
            .any(|effect| matches!(effect, BootstrapEffect::FallbackOrigin))
    );
    assert!(
        !timed_out
            .effects
            .iter()
            .any(|effect| matches!(effect, BootstrapEffect::BuildOrigin { .. }))
    );
    assert_eq!(
        engine
            .step(BootstrapEvent::Built {
                op: build_op,
                selected
            })
            .rejection,
        Some(BootstrapError::StaleOperation)
    );
}

#[test]
fn follower_observes_ready_donor_before_wait_budget_expires() {
    let names = [NodeId::from("a"), NodeId::from("b")];
    let winner =
        placement::owner(&scope().placement_key(), &names.iter().cloned().collect()).unwrap();
    let follower_name = names.iter().find(|name| **name != winner).unwrap();
    let mut follower =
        ClaimEngine::new(config(), scope(), follower_name.clone(), BootId(7), 1).unwrap();
    let self_claim = published(&follower.step(BootstrapEvent::Start));
    let donor_claim = BootstrapClaim {
        identity: ClaimIdentity {
            node: winner,
            incarnation: BootId(8),
            session: 1,
            attempt: 1,
        },
        renewal: 1,
        phase: ClaimPhase::Willing,
        remaining_ms: config().claim_ttl_ms,
    };
    let tick = follower.step(BootstrapEvent::Tick(crate::Time(3)));
    follower.step(BootstrapEvent::ClaimsObserved {
        op: observed_op(&tick),
        members: names
            .iter()
            .map(|node| BootstrapMember {
                node: node.clone(),
                eligible: true,
            })
            .collect(),
        claims: vec![self_claim, donor_claim.clone()],
    });
    assert_eq!(follower.stage(), BootstrapStage::Following);
    let poll = follower.step(BootstrapEvent::Tick(crate::Time(8)));
    let fresh_self = published(&poll);
    let ready = BootstrapClaim {
        phase: ClaimPhase::Ready,
        renewal: 2,
        ..donor_claim
    };
    let answer = follower.step(BootstrapEvent::ClaimsObserved {
        op: observed_op(&poll),
        members: names
            .iter()
            .map(|node| BootstrapMember {
                node: node.clone(),
                eligible: true,
            })
            .collect(),
        claims: vec![fresh_self, ready.clone()],
    });
    assert!(answer.rejection.is_none());
    assert_eq!(follower.stage(), BootstrapStage::DonorAvailable);
    assert!(answer.effects.iter().any(|effect| matches!(effect, BootstrapEffect::DonorAvailable { selected, .. } if *selected == ready.identity)));
}

#[test]
fn cancellation_never_grants_authority_or_accepts_restart() {
    let mut engine = node("me", 7);
    engine.step(BootstrapEvent::Start);
    let cancelled = engine.step(BootstrapEvent::Cancel);
    assert!(
        cancelled
            .effects
            .iter()
            .any(|effect| matches!(effect, BootstrapEffect::WithdrawClaim(_)))
    );
    assert_eq!(engine.stage(), BootstrapStage::Cancelled);
    assert_eq!(engine.next_deadline(), None);
    assert_eq!(
        engine.step(BootstrapEvent::Start).rejection,
        Some(BootstrapError::Stage)
    );
}
