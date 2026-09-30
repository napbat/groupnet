//! A follower's failed samples and the `Released` report of every wait it
//! gives up.

use super::*;
use crate::{NodeId, Time};

const GRACE_MS: u64 = 8 + 12 + 5;

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
        total_ms: 60,
    }
}

/// Node `b`, the follower, and the source view of builder `a`.
struct Follower {
    engine: ClaimEngine,
    mine: Option<BootstrapClaim>,
    builder: BootstrapClaim,
    op: Option<BootstrapOperation>,
}

impl Follower {
    fn new(phase: ClaimPhase) -> Self {
        let engine = ClaimEngine::new(
            config(),
            BootstrapScope {
                domain: "origin".to_owned(),
                partition: "bucket".to_owned(),
            },
            NodeId::from("b"),
            BootId(2),
            1,
        )
        .unwrap();
        let mut follower = Self {
            engine,
            mine: None,
            builder: BootstrapClaim {
                identity: ClaimIdentity {
                    node: NodeId::from("a"),
                    incarnation: BootId(1),
                    session: 1,
                    attempt: 1,
                },
                renewal: 1,
                phase,
                progress: 1,
                remaining_ms: 12,
            },
            op: None,
        };
        follower.step(BootstrapEvent::Start);
        follower
    }

    /// Step, keeping this node's latest claim and observation operation.
    fn step(&mut self, event: BootstrapEvent) -> BootstrapStep {
        let step = self.engine.step(event);
        for effect in &step.effects {
            match effect {
                BootstrapEffect::PublishClaim(claim) => self.mine = Some(claim.clone()),
                BootstrapEffect::ObserveClaims { op, .. } => self.op = Some(*op),
                _ => {}
            }
        }
        step
    }

    fn tick(&mut self, now: u64) -> BootstrapStep {
        self.step(BootstrapEvent::Tick(Time(now)))
    }

    /// Answer the pending observation. `builder_eligible` false models the
    /// membership layer suspecting `a`; `with_claim` false, `a` without a
    /// claim in the cut.
    fn observe(&mut self, builder_eligible: bool, with_claim: bool) -> BootstrapStep {
        self.builder.renewal += 1;
        let mut claims = vec![self.mine.clone().expect("own claim")];
        if builder_eligible && with_claim {
            claims.push(self.builder.clone());
        }
        let op = self.op.take().expect("pending observation");
        self.step(BootstrapEvent::ClaimsObserved {
            op,
            members: vec![
                BootstrapMember {
                    node: NodeId::from("a"),
                    eligible: builder_eligible,
                },
                BootstrapMember {
                    node: NodeId::from("b"),
                    eligible: true,
                },
            ],
            claims,
        })
    }

    /// Settle and select `a`.
    fn selected(phase: ClaimPhase) -> Self {
        let mut follower = Self::new(phase);
        follower.tick(3);
        let chosen = follower.observe(true, true);
        assert_eq!(chosen.rejection, None);
        assert_eq!(follower.engine.selected(), Some(&follower.builder.identity));
        follower
    }
}

fn released(step: &BootstrapStep) -> Vec<(ClaimIdentity, ReleaseReason)> {
    step.effects
        .iter()
        .filter_map(|effect| match effect {
            BootstrapEffect::Released { builder, reason } => Some((builder.clone(), *reason)),
            _ => None,
        })
        .collect()
}

fn follows(step: &BootstrapStep, builder: &ClaimIdentity) -> bool {
    step.effects.iter().any(
        |effect| matches!(effect, BootstrapEffect::FollowBuilder { selected, .. } if selected == builder),
    )
}

fn builds(step: &BootstrapStep) -> bool {
    step.effects
        .iter()
        .any(|effect| matches!(effect, BootstrapEffect::BuildOrigin { .. }))
}

#[test]
fn failed_sample_inside_the_grace_keeps_following_the_builder() {
    let mut follower = Follower::selected(ClaimPhase::Building);
    follower.tick(8);
    let op = follower.op.expect("follow interval elapsed");
    let failed = follower.step(BootstrapEvent::ObservationFailed { op });
    assert_eq!(failed.rejection, None);
    assert!(follows(&failed, &follower.builder.identity));
    assert!(!builds(&failed));
    assert!(released(&failed).is_empty());
    assert_eq!(follower.engine.stage(), BootstrapStage::Following);
    // A duplicate or late failure for the replaced operation changes nothing.
    assert_eq!(
        follower
            .step(BootstrapEvent::ObservationFailed { op })
            .rejection,
        Some(BootstrapError::StaleOperation)
    );
    // The resample neither renewed the grace nor lost the builder.
    follower.tick(13);
    let seen = follower.observe(true, true);
    assert!(follows(&seen, &follower.builder.identity));
}

#[test]
fn failed_sample_before_any_selection_falls_back_without_release() {
    let mut follower = Follower::new(ClaimPhase::Building);
    follower.tick(3);
    let op = follower.op.expect("settled");
    let failed = follower.step(BootstrapEvent::ObservationFailed { op });
    assert_eq!(failed.rejection, None);
    assert_eq!(follower.engine.stage(), BootstrapStage::Fallback);
    assert!(released(&failed).is_empty());
}

#[test]
fn suspected_builder_is_resampled_not_replaced() {
    let mut follower = Follower::selected(ClaimPhase::Building);
    follower.tick(8);
    let unseen = follower.observe(false, false);
    assert_eq!(unseen.rejection, None);
    assert!(follows(&unseen, &follower.builder.identity));
    assert!(!builds(&unseen));
    assert!(released(&unseen).is_empty());
}

#[test]
fn builder_withdrawn_while_live_is_released_and_taken_over() {
    let mut follower = Follower::selected(ClaimPhase::Building);
    follower.tick(8);
    let withdrawn = follower.observe(true, false);
    assert_eq!(withdrawn.rejection, None);
    assert_eq!(
        released(&withdrawn),
        vec![(follower.builder.identity.clone(), ReleaseReason::Withdrawn)]
    );
    assert!(matches!(
        withdrawn.effects.first(),
        Some(BootstrapEffect::Released { .. })
    ));
    assert!(builds(&withdrawn));
}

#[test]
fn stalled_builder_is_released_once() {
    let mut follower = Follower::selected(ClaimPhase::Building);
    let mut releases = Vec::new();
    let mut now = 3;
    while releases.is_empty() {
        now += 1;
        assert!(now <= 3 + GRACE_MS, "no release within the grace");
        let tick = follower.tick(now);
        releases.extend(released(&tick));
        if releases.is_empty() && follower.op.is_some() {
            // Visible and eligible, but its progress never advances.
            releases.extend(released(&follower.observe(true, true)));
        }
    }
    assert_eq!(
        releases,
        vec![(follower.builder.identity.clone(), ReleaseReason::Stalled)]
    );
    assert_eq!(now, 3 + GRACE_MS);
    // The excluded attempt is not reported again when the episode ends.
    let op = follower.op.expect("resampled after the release");
    let ended = follower.step(BootstrapEvent::ObservationFailed { op });
    assert_eq!(follower.engine.stage(), BootstrapStage::Fallback);
    assert!(released(&ended).is_empty());
}

#[test]
fn unverified_ready_donor_is_released() {
    let mut follower = Follower::new(ClaimPhase::Ready);
    follower.tick(3);
    let op = follower.op.take().expect("settled");
    let mine = follower.mine.clone().unwrap();
    let chosen = follower.step(BootstrapEvent::ClaimsObserved {
        op,
        members: vec![
            BootstrapMember {
                node: NodeId::from("a"),
                eligible: true,
            },
            BootstrapMember {
                node: NodeId::from("b"),
                eligible: true,
            },
        ],
        claims: vec![mine, follower.builder.clone()],
    });
    let donor_op = chosen
        .effects
        .iter()
        .find_map(|effect| match effect {
            BootstrapEffect::DonorAvailable { op, .. } => Some(*op),
            _ => None,
        })
        .expect("ready donor");
    let declined = follower.step(BootstrapEvent::PeerTransferDeclined {
        op: donor_op,
        selected: follower.builder.identity.clone(),
    });
    assert_eq!(declined.rejection, None);
    assert_eq!(
        released(&declined),
        vec![(follower.builder.identity.clone(), ReleaseReason::Unverified)]
    );
    assert_eq!(follower.engine.stage(), BootstrapStage::Fallback);
}

#[test]
fn failed_sample_outside_the_grace_ends_the_wait_with_release() {
    let mut follower = Follower::selected(ClaimPhase::Ready);
    // A Ready donor has no follow grace: once its donor wait lapses without
    // a transfer, the next sample decides, and a failed one ends the episode.
    let mut now = 3;
    while follower.op.is_none() {
        now += 1;
        assert!(now <= 3 + GRACE_MS, "no resample after the donor wait");
        follower.tick(now);
    }
    let op = follower.op.take().unwrap();
    let ended = follower.step(BootstrapEvent::ObservationFailed { op });
    assert_eq!(ended.rejection, None);
    assert_eq!(
        released(&ended),
        vec![(follower.builder.identity.clone(), ReleaseReason::Ended)]
    );
    assert_eq!(follower.engine.stage(), BootstrapStage::Fallback);
}
