//! Complete TTL participation cuts under delayed, lost, and malformed replies.

use groupnet_core::volatile_bootstrap::{
    BootId, BootstrapClaim, BootstrapConfig, BootstrapEffect, BootstrapEvent, BootstrapMember,
    BootstrapMemberIdentity, BootstrapOperation, BootstrapParticipant, BootstrapScope,
    BootstrapStage, ClaimEngine, ClaimIdentity, ClaimPhase, PresenceIdentity,
};
use groupnet_core::{NodeId, Status, Time};
use groupnet_sim::SplitMix64;

fn engine() -> ClaimEngine {
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
            domain: "o".to_owned(),
            partition: "b".to_owned(),
        },
        NodeId::from("me"),
        BootId(7),
        1,
    )
    .unwrap();
    engine.require_participation().unwrap();
    engine
}

fn participant(node: &str, boot: u128, session: u64) -> BootstrapParticipant {
    BootstrapParticipant {
        member: BootstrapMemberIdentity {
            node: NodeId::from(node),
            presence: Some(PresenceIdentity {
                node: NodeId::from(node),
                boot: BootId(boot),
                session,
            }),
            member_incarnation: 0,
            status: Status::Alive,
        },
        renewal: 1,
        remaining_ms: 8,
    }
}

fn claim(node: &str, boot: u128, session: u64, phase: ClaimPhase) -> BootstrapClaim {
    BootstrapClaim {
        identity: ClaimIdentity {
            node: NodeId::from(node),
            incarnation: BootId(boot),
            session,
            attempt: 1,
        },
        renewal: 1,
        phase,
        remaining_ms: 8,
    }
}

fn observation(op: BootstrapOperation, variant: u32) -> BootstrapEvent {
    let mut participants = vec![participant("me", 7, 1), participant("peer", 9, 2)];
    let roster = participants
        .iter()
        .map(|entry| entry.member.clone())
        .collect();
    if variant == 1 {
        participants.pop();
    } else if variant == 2 {
        participants[1].member.presence.as_mut().unwrap().boot = BootId(10);
    }
    BootstrapEvent::ParticipantsObserved {
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
        roster,
        participants,
        claims: vec![
            BootstrapClaim {
                renewal: 2,
                ..claim("me", 7, 1, ClaimPhase::Willing)
            },
            claim("peer", 9, 2, ClaimPhase::Ready),
        ],
    }
}

#[test]
fn complete_source_cut_or_finite_origin_fallback_under_faults() {
    let mut healthy = 0;
    let mut refused = 0;
    let mut lost = 0;
    let mut duplicate_rejections = 0;
    for seed in 0..48 {
        let mut rng = SplitMix64::new(seed);
        let mut engine = engine();
        let start = engine.step(BootstrapEvent::Start);
        assert!(
            start
                .effects
                .iter()
                .any(|effect| { matches!(effect, BootstrapEffect::PublishPresence(_)) })
        );
        let observe = engine.step(BootstrapEvent::Tick(Time(2)));
        let op = observe.effects.iter().find_map(|effect| match effect {
            BootstrapEffect::ObserveClaims { op, .. } => Some(*op),
            _ => None,
        });
        let op = op.unwrap();
        let variant = u32::try_from(seed % 4).unwrap();
        if variant == 3 {
            lost += 1;
        } else {
            let _ = engine.step(BootstrapEvent::Tick(Time(3)));
            let delivered = engine.step(observation(op, variant));
            if variant == 0 {
                assert!(delivered.rejection.is_none());
                assert_eq!(engine.stage(), BootstrapStage::DonorAvailable);
                assert_eq!(engine.participant_roster().unwrap().len(), 2);
                healthy += 1;
                let follow = delivered.effects.iter().find_map(|effect| match effect {
                    BootstrapEffect::DonorAvailable { op, .. } => Some(*op),
                    _ => None,
                });
                let decline = engine.step(BootstrapEvent::PeerTransferDeclined {
                    op: follow.unwrap(),
                    selected: claim("peer", 9, 2, ClaimPhase::Ready).identity,
                });
                assert!(decline.rejection.is_none());
                assert!(
                    !decline
                        .effects
                        .iter()
                        .any(|effect| { matches!(effect, BootstrapEffect::WithdrawPresence(_)) })
                );
            } else {
                assert!(delivered.rejection.is_some());
                refused += 1;
            }
            // A duplicate delayed answer cannot advance a replaced operation.
            if rng.below(2) == 0 {
                let duplicate = engine.step(observation(op, variant));
                if duplicate.rejection.is_some() {
                    duplicate_rejections += 1;
                }
            }
        }
        let terminal = engine.step(BootstrapEvent::Tick(Time(20)));
        assert_eq!(engine.stage(), BootstrapStage::Fallback);
        assert!(
            !terminal
                .effects
                .iter()
                .any(|effect| { matches!(effect, BootstrapEffect::WithdrawPresence(_)) })
        );
        assert!(engine.next_deadline().is_some());
    }
    assert_eq!(healthy, 12);
    assert_eq!(refused, 24);
    assert_eq!(lost, 12);
    assert!(duplicate_rejections > 0);
}
