//! Participation and stale-callback claim regressions.

use super::*;

fn engine() -> ClaimEngine {
    ClaimEngine::new(
        BootstrapConfig {
            max_members: 2,
            max_member_bytes: 8,
            max_scope_bytes: 16,
            settle_ms: 2,
            renew_ms: 3,
            claim_ttl_ms: 10,
            observe_ms: 3,
            donor_wait_ms: 5,
            total_ms: 20,
        },
        BootstrapScope {
            domain: "o".to_owned(),
            partition: "b".to_owned(),
        },
        NodeId::from("me"),
        BootId(7),
        1,
    )
    .unwrap()
}

#[test]
fn presence_renews_after_fallback_and_stale_failure_cannot_end_it() {
    let mut engine = engine();
    let start = engine.step(BootstrapEvent::Start);
    let first = start.effects.iter().find_map(|effect| match effect {
        BootstrapEffect::PublishPresence(value) => Some(value.clone()),
        _ => None,
    });
    let first = first.expect("start publishes participation");
    assert_eq!(first.renewal, 1);
    let fallback = engine.step(BootstrapEvent::Tick(Time(20)));
    assert_eq!(engine.stage(), BootstrapStage::Fallback);
    assert!(fallback.effects.contains(&BootstrapEffect::FallbackOrigin));
    let renewal = fallback.effects.iter().find_map(|effect| match effect {
        BootstrapEffect::PublishPresence(value) => Some(value.clone()),
        _ => None,
    });
    let renewal = renewal.expect("presence remains scheduled across fallback");
    assert_eq!(renewal.renewal, 2);
    let old = engine.step(BootstrapEvent::PresenceFailed {
        identity: first.identity.clone(),
        renewal: 1,
    });
    assert_eq!(old.rejection, Some(BootstrapError::StaleOperation));
    let cancel = engine.step(BootstrapEvent::Cancel);
    assert!(
        cancel
            .effects
            .contains(&BootstrapEffect::WithdrawPresence(first.identity))
    );
    assert_eq!(engine.next_deadline(), None);
}

#[test]
fn local_capture_retirement_preserves_presence_but_cancel_withdraws_it() {
    let mut engine = engine();
    let start = engine.step(BootstrapEvent::Start);
    let claim = start.effects.iter().find_map(|effect| match effect {
        BootstrapEffect::PublishClaim(claim) => Some(claim.clone()),
        _ => None,
    });
    let claim = claim.unwrap();
    let observe = engine.step(BootstrapEvent::Tick(Time(2)));
    let op = observe.effects.iter().find_map(|effect| match effect {
        BootstrapEffect::ObserveClaims { op, .. } => Some(*op),
        _ => None,
    });
    let decision = engine.step(BootstrapEvent::ClaimsObserved {
        op: op.unwrap(),
        members: vec![BootstrapMember {
            node: NodeId::from("me"),
            eligible: true,
        }],
        claims: vec![claim.clone()],
    });
    let build = decision.effects.iter().find_map(|effect| match effect {
        BootstrapEffect::BuildOrigin { op, .. } => Some(*op),
        _ => None,
    });
    assert!(
        engine
            .step(BootstrapEvent::Built {
                op: build.unwrap(),
                selected: claim.identity.clone(),
            })
            .rejection
            .is_none()
    );
    let retired = engine.step(BootstrapEvent::CaptureRetired {
        selected: claim.identity.clone(),
    });
    assert_eq!(engine.stage(), BootstrapStage::Fallback);
    assert!(
        retired
            .effects
            .contains(&BootstrapEffect::WithdrawClaim(claim.identity.clone()))
    );
    assert!(
        !retired
            .effects
            .iter()
            .any(|effect| matches!(effect, BootstrapEffect::WithdrawPresence(_)))
    );
    let restart = engine.step(BootstrapEvent::Start);
    let replacement = restart.effects.iter().find_map(|effect| match effect {
        BootstrapEffect::PublishClaim(claim) => Some(claim.identity.clone()),
        _ => None,
    });
    assert_ne!(replacement.unwrap(), claim.identity);
    assert_eq!(
        engine
            .step(BootstrapEvent::CaptureRetired {
                selected: claim.identity,
            })
            .rejection,
        Some(BootstrapError::StaleOperation)
    );
    assert!(
        engine
            .step(BootstrapEvent::Cancel)
            .effects
            .iter()
            .any(|effect| matches!(effect, BootstrapEffect::WithdrawPresence(_)))
    );
}

#[test]
fn repeated_stale_claim_never_refreshes_its_original_expiry() {
    let mut engine = engine();
    let mut claim = BootstrapClaim {
        identity: ClaimIdentity {
            node: NodeId::from("peer"),
            incarnation: BootId(9),
            session: 1,
            attempt: 1,
        },
        renewal: 1,
        phase: ClaimPhase::Building,
        remaining_ms: 10,
    };
    assert!(engine.track_claim(&claim).unwrap());
    assert_eq!(engine.observed[&claim.identity].expires, Time(10));
    engine.now = Time(6);
    assert!(engine.track_claim(&claim).unwrap());
    assert_eq!(engine.observed[&claim.identity].expires, Time(10));
    engine.now = Time(10);
    assert!(!engine.track_claim(&claim).unwrap());
    let local = BootstrapClaim {
        identity: ClaimIdentity {
            node: NodeId::from("me"),
            incarnation: BootId(7),
            session: 1,
            attempt: 1,
        },
        renewal: 1,
        phase: ClaimPhase::Willing,
        remaining_ms: 10,
    };
    engine.generation = 1;
    engine.local_renewal = 1;
    let roster = vec![
        BootstrapMember {
            node: NodeId::from("me"),
            eligible: true,
        },
        BootstrapMember {
            node: NodeId::from("peer"),
            eligible: true,
        },
    ];
    let result = engine.choose(&roster, vec![local, claim.clone()]);
    assert!(result.rejection.is_none());
    assert_eq!(engine.observed[&claim.identity].expires, Time(10));
    claim.renewal = 2;
    assert!(engine.track_claim(&claim).unwrap());
    assert_eq!(engine.observed[&claim.identity].expires, Time(20));
}
