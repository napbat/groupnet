//! Seeded follower schedules under membership churn: a follower of a builder
//! that keeps advancing waits through the builder's SWIM refutations
//! (incarnation bumps), Suspect windows and failed samples, never starts its
//! own origin build or falls back, and finds the builder once it is Ready.

use groupnet_core::volatile_bootstrap::{
    BootId, BootstrapClaim, BootstrapConfig, BootstrapEffect, BootstrapEvent, BootstrapMember,
    BootstrapMemberIdentity, BootstrapOperation, BootstrapParticipant, BootstrapPresence,
    BootstrapScope, BootstrapStage, ClaimEngine, ClaimIdentity, ClaimPhase, PresenceIdentity,
};
use groupnet_core::{NodeId, Status, Time};
use groupnet_sim::SplitMix64;

const CONFIG: BootstrapConfig = BootstrapConfig {
    max_members: 4,
    max_member_bytes: 16,
    max_scope_bytes: 32,
    settle_ms: 3,
    renew_ms: 2,
    claim_ttl_ms: 6,
    observe_ms: 3,
    donor_wait_ms: 10,
    total_ms: 30,
};

/// The builder's view as the follower's source samples it.
struct Builder {
    presence: PresenceIdentity,
    member_incarnation: u64,
    suspect_until: u64,
    renewal: u64,
    renewed_at: u64,
    progress: u64,
    ready: bool,
}

impl Builder {
    fn claim(&self, now: u64) -> BootstrapClaim {
        BootstrapClaim {
            identity: ClaimIdentity {
                node: self.presence.node.clone(),
                incarnation: self.presence.boot,
                session: self.presence.session,
                attempt: 1,
            },
            renewal: self.renewal,
            phase: if self.ready {
                ClaimPhase::Ready
            } else {
                ClaimPhase::Building
            },
            progress: self.progress,
            remaining_ms: CONFIG.claim_ttl_ms - (now - self.renewed_at),
        }
    }

    fn member(&self, now: u64) -> BootstrapMemberIdentity {
        let suspect = now < self.suspect_until;
        BootstrapMemberIdentity {
            node: self.presence.node.clone(),
            presence: (!suspect).then(|| self.presence.clone()),
            member_incarnation: self.member_incarnation,
            status: if suspect {
                Status::Suspect
            } else {
                Status::Alive
            },
        }
    }
}

/// One complete source cut: the follower's own presence and claim as last
/// published, and the builder as its membership status allows.
fn sample(
    op: BootstrapOperation,
    builder: &Builder,
    me: &(BootstrapPresence, u64),
    my_claim: Option<&(BootstrapClaim, u64)>,
    now: u64,
) -> BootstrapEvent {
    let mine = BootstrapParticipant {
        member: BootstrapMemberIdentity {
            node: me.0.identity.node.clone(),
            presence: Some(me.0.identity.clone()),
            member_incarnation: 0,
            status: Status::Alive,
        },
        renewal: me.0.renewal,
        remaining_ms: me.0.remaining_ms - (now - me.1),
    };
    let theirs = builder.member(now);
    let eligible = theirs.eligible();
    let mut participants = vec![mine.clone()];
    let mut claims: Vec<_> = my_claim
        .map(|(claim, at)| BootstrapClaim {
            remaining_ms: claim.remaining_ms - (now - at),
            ..claim.clone()
        })
        .into_iter()
        .collect();
    if eligible {
        participants.push(BootstrapParticipant {
            member: theirs.clone(),
            renewal: builder.renewal,
            remaining_ms: CONFIG.claim_ttl_ms - (now - builder.renewed_at),
        });
        claims.push(builder.claim(now));
    }
    BootstrapEvent::ParticipantsObserved {
        op,
        members: vec![
            BootstrapMember {
                node: mine.member.node.clone(),
                eligible: true,
            },
            BootstrapMember {
                node: theirs.node.clone(),
                eligible,
            },
        ],
        roster: vec![mine.member, theirs],
        participants,
        claims,
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one seeded schedule keeps the builder model, the sample queue and the invariants together"
)]
fn follower_waits_out_builder_churn_and_finds_the_ready_donor() {
    let (mut bumps, mut suspect_windows, mut failed_samples, mut held) = (0, 0, 0, 0);
    for seed in 0..96_u64 {
        let mut rng = SplitMix64::new(seed);
        let me = NodeId::from("a-follower");
        let mut engine = ClaimEngine::new(
            CONFIG,
            BootstrapScope {
                domain: "origin".to_owned(),
                partition: "bucket".to_owned(),
            },
            me.clone(),
            BootId(7),
            1,
        )
        .unwrap();
        engine.require_participation().unwrap();
        // Far longer than the follower's grace and episode budget: only the
        // builder's advertised progress keeps the follower waiting.
        let build_ms = 150 + u64::from(rng.below(150));
        let mut builder = Builder {
            presence: PresenceIdentity {
                node: NodeId::from("b-builder"),
                boot: BootId(9),
                session: 2,
            },
            member_incarnation: 0,
            suspect_until: 0,
            renewal: 1,
            renewed_at: 0,
            progress: 1,
            ready: false,
        };
        let mut my_presence: Option<(BootstrapPresence, u64)> = None;
        let mut my_claim: Option<(BootstrapClaim, u64)> = None;
        let mut pending: Vec<(u64, BootstrapOperation)> = Vec::new();
        let mut saw_builder = false;
        let mut donor_at = None;
        let mut effects = engine.step(BootstrapEvent::Start).effects;
        for now in 0..(build_ms + 60) {
            // The builder: renews and advances every other millisecond, and
            // under load is suspected for a while and then refutes with a
            // higher incarnation, or refutes a suspicion no sample saw. A
            // window opens only after a sample saw the builder, so the
            // builder stays visible within the follower's grace.
            if now >= 20 && saw_builder && now >= builder.suspect_until + 4 {
                match rng.below(40) {
                    0 => {
                        builder.suspect_until = now + 1 + u64::from(rng.below(8));
                        builder.member_incarnation += 1;
                        suspect_windows += 1;
                        bumps += 1;
                    }
                    1 => {
                        builder.member_incarnation += 1;
                        bumps += 1;
                    }
                    _ => {}
                }
            }
            if now % 2 == 0 {
                builder.renewal += 1;
                builder.renewed_at = now;
                if now >= build_ms {
                    builder.ready = true;
                } else {
                    builder.progress += 1;
                }
            }
            // The engine's own publications from this tick land before this
            // millisecond's samples read the source.
            for sampling in [false, true] {
                if !sampling && now > 0 {
                    effects.extend(engine.step(BootstrapEvent::Tick(Time(now))).effects);
                }
                let mut due = Vec::new();
                if sampling {
                    pending.retain(|(at, op)| {
                        if *at > now {
                            return true;
                        }
                        due.push(*op);
                        false
                    });
                }
                for op in due {
                    // The source read failed or timed out under load. Churn
                    // starts once the follower follows (a node that selected
                    // nothing yet still falls back), and a failure follows a
                    // sample that saw the builder: the builder stays visible
                    // within the follower's grace, which a longer blackout
                    // rightly releases as a stall.
                    let step = if now >= 20 && saw_builder && rng.below(4) == 0 {
                        failed_samples += 1;
                        saw_builder = false;
                        engine.step(BootstrapEvent::ObservationFailed { op })
                    } else {
                        saw_builder = now >= builder.suspect_until;
                        let step = engine.step(sample(
                            op,
                            &builder,
                            my_presence.as_ref().expect("presence precedes sampling"),
                            my_claim.as_ref(),
                            now,
                        ));
                        assert_eq!(
                            step.rejection, None,
                            "seed {seed}: a complete cut at {now} ms was refused"
                        );
                        step
                    };
                    effects.extend(step.effects);
                }
                for effect in effects.drain(..) {
                    match effect {
                        BootstrapEffect::PublishPresence(presence) => {
                            my_presence = Some((presence, now));
                        }
                        BootstrapEffect::PublishClaim(claim) => my_claim = Some((claim, now)),
                        BootstrapEffect::ObserveClaims { op, .. } => {
                            pending.push((now + u64::from(rng.below(2)), op));
                        }
                        BootstrapEffect::FollowBuilder { selected, .. } => {
                            assert_eq!(selected.node, builder.presence.node, "seed {seed}");
                            if now < builder.suspect_until {
                                held += 1;
                            }
                        }
                        BootstrapEffect::DonorAvailable { selected, .. } => {
                            assert_eq!(selected.node, builder.presence.node, "seed {seed}");
                            assert!(builder.ready, "seed {seed}");
                            donor_at.get_or_insert(now);
                        }
                        BootstrapEffect::BuildOrigin { .. } => {
                            panic!("seed {seed}: the follower started its own build at {now} ms")
                        }
                        BootstrapEffect::Released { builder, reason } => {
                            panic!("seed {seed}: released {builder:?} ({reason:?}) at {now} ms")
                        }
                        _ => {}
                    }
                }
            }
            assert_ne!(
                engine.stage(),
                BootstrapStage::Fallback,
                "seed {seed}: the follower fell back to origin at {now} ms"
            );
            // The transfer from the Ready donor is covered elsewhere.
            if donor_at.is_some() {
                break;
            }
        }
        let donor_at = donor_at.unwrap_or_else(|| panic!("seed {seed}: no Ready donor found"));
        // Found by the first sample that sees the Ready claim: within one
        // Suspect window, one failed sample and two observation intervals.
        assert!(
            donor_at >= build_ms && donor_at <= build_ms + 8 + 3 * (CONFIG.observe_ms + 1),
            "seed {seed}: the Ready donor was found at {donor_at} ms, built at {build_ms} ms"
        );
    }
    assert!(bumps > 96);
    assert!(suspect_windows > 48);
    assert!(failed_samples > 96);
    assert!(held > 0);
}
