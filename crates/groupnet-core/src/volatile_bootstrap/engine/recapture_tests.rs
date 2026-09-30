//! A completed local image's Ready recapture under SWIM churn and failure.
//!
//! The donor's worker verifies one complete cut at C and another after the
//! image is encoded. On a loaded node both this node's own refutations and a
//! suspected peer change member incarnation and status between the two, with
//! every presence unchanged: the capture must still complete. A failed
//! attempt is retried only after a doubling backoff, and only inside the
//! claim window unless the membership changes. A Ready capture retired before
//! it has stayed Ready for a claim window is such a failed attempt.

use super::*;
use crate::Status;
use crate::volatile_bootstrap::BootstrapParticipant;

const CONFIG: BootstrapConfig = BootstrapConfig {
    max_members: 4,
    max_member_bytes: 16,
    max_scope_bytes: 32,
    settle_ms: 2,
    renew_ms: 2,
    claim_ttl_ms: 6,
    observe_ms: 2,
    donor_wait_ms: 40,
    total_ms: 80,
};

/// One donor engine and the native view it samples: itself, and one peer
/// whose presence, status and incarnation the test moves.
struct Donor {
    engine: ClaimEngine,
    now: u64,
    me_incarnation: u64,
    peer_boot: u128,
    peer_status: Status,
    peer_incarnation: u64,
    peer_renewal: u64,
}

impl Donor {
    /// A donor whose local origin build completed: its Ready recapture is
    /// pending under a claim window of `donor_wait_ms` from now.
    fn pending() -> Self {
        let mut engine = ClaimEngine::new(
            CONFIG,
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
        let mut donor = Self {
            engine,
            now: 0,
            me_incarnation: 1,
            peer_boot: 9,
            peer_status: Status::Alive,
            peer_incarnation: 1,
            peer_renewal: 0,
        };
        let _ = donor.engine.step(BootstrapEvent::Start);
        let observe = donor.advance(CONFIG.settle_ms);
        let op = observe
            .iter()
            .find_map(|effect| match effect {
                BootstrapEffect::ObserveClaims { op, .. } => Some(*op),
                _ => None,
            })
            .unwrap();
        let (members, roster, participants, claims) = donor.cut();
        let chosen = donor.engine.step(BootstrapEvent::ParticipantsObserved {
            op,
            members,
            roster,
            participants,
            claims,
        });
        let build = chosen
            .effects
            .iter()
            .find_map(|effect| match effect {
                BootstrapEffect::BuildOrigin { op, .. } => Some(*op),
                _ => None,
            })
            .unwrap();
        let selected = donor.engine.selected().unwrap().clone();
        let local_only = donor.engine.step(BootstrapEvent::LocalOnlyBuilt {
            op: build,
            selected,
        });
        assert_eq!(local_only.rejection, None);
        assert!(donor.engine.ready_recapture_due());
        donor
    }

    /// Tick the engine through `ms` milliseconds, one at a time, returning
    /// every effect.
    fn advance(&mut self, ms: u64) -> Vec<BootstrapEffect> {
        let mut effects = Vec::new();
        for _ in 0..ms {
            self.now += 1;
            let step = self.engine.step(BootstrapEvent::Tick(Time(self.now)));
            assert_eq!(step.rejection, None);
            effects.extend(step.effects);
        }
        effects
    }

    /// One complete native cut as this donor's actor reports it: its own
    /// current claim and presence, and the peer's renewed presence. A peer
    /// that is not Alive keeps its presence but its claim is not reported.
    fn cut(
        &mut self,
    ) -> (
        Vec<BootstrapMember>,
        Vec<BootstrapMemberIdentity>,
        Vec<BootstrapParticipant>,
        Vec<BootstrapClaim>,
    ) {
        self.peer_renewal += 1;
        let me = BootstrapMemberIdentity {
            node: NodeId::from("me"),
            presence: Some(self.engine.presence_identity()),
            member_incarnation: self.me_incarnation,
            status: Status::Alive,
        };
        let peer = BootstrapMemberIdentity {
            node: NodeId::from("peer"),
            presence: Some(PresenceIdentity {
                node: NodeId::from("peer"),
                boot: BootId(self.peer_boot),
                session: 2,
            }),
            member_incarnation: self.peer_incarnation,
            status: self.peer_status,
        };
        let members = [&me, &peer]
            .map(|member| BootstrapMember {
                node: member.node.clone(),
                eligible: member.eligible(),
            })
            .to_vec();
        let participants = vec![
            BootstrapParticipant {
                member: me.clone(),
                renewal: self.engine.presence_renewal,
                remaining_ms: CONFIG.claim_ttl_ms,
            },
            BootstrapParticipant {
                member: peer.clone(),
                renewal: self.peer_renewal,
                remaining_ms: CONFIG.claim_ttl_ms,
            },
        ];
        let claims = if self.engine.local_renewal > 0 && self.engine.renew_due.is_some() {
            vec![self.engine.claim()]
        } else {
            Vec::new()
        };
        (members, vec![me, peer], participants, claims)
    }

    /// Sample and verify one cut, as the worker's `current_participation`.
    fn verify(&mut self) -> Result<(), BootstrapError> {
        let op = self.engine.begin_roster_observation()?;
        let (members, roster, participants, claims) = self.cut();
        self.engine
            .verify_participant_roster(op, &members, &roster, &participants, &claims)
    }

    /// Start one recapture under a freshly verified cut, verify its cut at
    /// C, and return its operation, or `None` if nothing started.
    fn start(&mut self) -> Option<BootstrapOperation> {
        self.verify().unwrap();
        let step = self.engine.step(BootstrapEvent::StartReadyRecapture);
        assert_eq!(step.rejection, None);
        let op = step.effects.iter().find_map(|effect| match effect {
            BootstrapEffect::RecaptureCurrent { op, .. } => Some(*op),
            _ => None,
        })?;
        self.verify().unwrap();
        Some(op)
    }

    fn fail(&mut self, op: BootstrapOperation) -> Vec<BootstrapEffect> {
        let selected = self.engine.selected().unwrap().clone();
        let step = self
            .engine
            .step(BootstrapEvent::BuildFailed { op, selected });
        assert_eq!(step.rejection, None);
        step.effects
    }
}

fn withdraws(effects: &[BootstrapEffect]) -> bool {
    effects
        .iter()
        .any(|effect| matches!(effect, BootstrapEffect::WithdrawClaim(_)))
}

fn publishes(effects: &[BootstrapEffect]) -> bool {
    effects
        .iter()
        .any(|effect| matches!(effect, BootstrapEffect::PublishClaim(_)))
}

/// While C is encoded, this node refutes a suspicion of itself and the peer
/// is suspected and then declared Dead, all with unchanged presence: the
/// post-encode cut still verifies against C and the image goes Ready. The
/// Ready capture's maintenance rechecks survive the refutation that follows.
/// A peer restart, a new boot under the same node, is still a change.
#[test]
fn swim_churn_while_encoding_keeps_the_ready_capture() {
    let mut donor = Donor::pending();
    let op = donor.start().expect("the first recapture starts");
    donor.me_incarnation += 1;
    donor.peer_status = Status::Suspect;
    donor.peer_incarnation += 1;
    let _ = donor.advance(1);
    donor.peer_status = Status::Dead;
    let _ = donor.advance(1);
    assert_eq!(donor.verify(), Ok(()), "the post-encode cut binds C");
    let selected = donor.engine.selected().unwrap().clone();
    let built = donor.engine.step(BootstrapEvent::Built { op, selected });
    assert_eq!(built.rejection, None);
    assert!(built.effects.iter().any(|effect| matches!(
        effect,
        BootstrapEffect::PublishClaim(claim) if claim.phase == ClaimPhase::Ready
    )));

    donor.peer_status = Status::Alive;
    donor.peer_incarnation += 1;
    donor.me_incarnation += 1;
    let _ = donor.advance(CONFIG.renew_ms);
    assert_eq!(
        donor.verify(),
        Ok(()),
        "a refutation keeps the Ready capture"
    );
    donor.peer_boot += 1;
    let _ = donor.advance(CONFIG.renew_ms);
    assert_eq!(
        donor.verify(),
        Err(BootstrapError::InvalidObservation),
        "a restarted peer retires it"
    );
}

/// Every attempt fails. Retries wait one observation interval, doubling to a
/// cap of a quarter of the stall bound; the claim is renewed only until the
/// window that opened with the pending image closes, which no failure
/// extends; after it an unchanged membership starts nothing, however many
/// maintenance turns sample it, and a restarted peer starts exactly one
/// attempt under a fresh claim window.
#[test]
fn failed_recaptures_back_off_inside_the_claim_window() {
    let mut donor = Donor::pending();
    let window = donor.now + CONFIG.donor_wait_ms;
    let mut starts = Vec::new();
    let mut withdrawn = None;
    while donor.now < window + 3 * CONFIG.donor_wait_ms {
        if donor.engine.ready_recapture_due() {
            if let Some(op) = donor.start() {
                starts.push(donor.now);
                let _ = donor.advance(1);
                let failed = donor.fail(op);
                if withdraws(&failed) {
                    withdrawn.get_or_insert(donor.now);
                }
            } else {
                assert!(donor.now >= window, "an attempt is refused in the window");
            }
        }
        let effects = donor.advance(1);
        if withdraws(&effects) {
            withdrawn.get_or_insert(donor.now);
        }
        if withdrawn.is_some() {
            assert!(!publishes(&effects), "a withdrawn claim stays withdrawn");
        }
    }
    let gaps: Vec<_> = starts.windows(2).map(|pair| pair[1] - pair[0]).collect();
    for (failures, gap) in (1_u32..).zip(&gaps) {
        let backoff = (CONFIG.observe_ms << (failures - 1)).min(CONFIG.donor_wait_ms / 4);
        assert!(
            *gap > backoff,
            "retry {failures} after {gap} ms: {starts:?}"
        );
    }
    assert!(gaps.iter().any(|gap| *gap > CONFIG.donor_wait_ms / 4));
    assert!(starts.iter().all(|start| *start < window), "{starts:?}");
    // 2, 4, 8, then 10 ms apart, plus the failed attempt's 1 ms: 6 attempts.
    assert_eq!(starts.len(), 6, "{starts:?}");
    let withdrawn = withdrawn.expect("the claim is withdrawn once the window closes");
    assert!((window..=window + 1).contains(&withdrawn), "{withdrawn}");

    donor.peer_boot += 1;
    let limit = donor.now + CONFIG.donor_wait_ms;
    let mut after = None;
    while after.is_none() && donor.now < limit {
        let _ = donor.advance(1);
        if donor.engine.ready_recapture_due() {
            after = donor.start();
        }
    }
    let op = after.expect("a membership change retries");
    assert!(donor.engine.recapture_due.is_some(), "a fresh claim window");
    let failed = donor.fail(op);
    assert!(
        !withdraws(&failed),
        "its claim is renewed for the new window"
    );
    assert!(
        donor.engine.recapture_retry_due.unwrap().0 >= donor.now + CONFIG.donor_wait_ms / 4,
        "the backoff carries on across the membership change"
    );
}

/// Build a Ready capture for the running recapture `op`.
fn build(donor: &mut Donor, op: BootstrapOperation) -> Vec<BootstrapEffect> {
    let selected = donor.engine.selected().unwrap().clone();
    let built = donor.engine.step(BootstrapEvent::Built { op, selected });
    assert_eq!(built.rejection, None);
    built.effects
}

/// Retire the Ready capture, as a lapse or an expiry does.
fn retire(donor: &mut Donor) -> Vec<BootstrapEffect> {
    let selected = donor.engine.selected().unwrap().clone();
    let retired = donor
        .engine
        .step(BootstrapEvent::CaptureRetired { selected });
    assert_eq!(retired.rejection, None);
    retired.effects
}

/// Every capture stalls the node past its lease, so each Ready capture is
/// retired by a lapse moments after it is built. Each retirement supersedes
/// the Ready claim with a Building one for a fresh window, so a joiner waits
/// through the lapse, but each such capture is also a failed attempt: retries
/// back off as failures do, and once the backoffs add up to a claim window
/// the unchanged membership starts nothing more, however long the node runs.
/// The last window then runs out. A restarted peer still starts one attempt,
/// with the backoff carried on, and a lapse of that capture starts nothing.
#[test]
fn a_capture_that_costs_its_own_lapse_cannot_cycle() {
    let mut donor = Donor::pending();
    let mut starts = Vec::new();
    let mut last_retired = 0;
    let mut withdrawn = None;
    let limit = donor.now + 6 * CONFIG.donor_wait_ms;
    while donor.now < limit {
        if donor.engine.ready_recapture_due()
            && let Some(op) = donor.start()
        {
            assert_eq!(withdrawn, None, "{starts:?}");
            starts.push(donor.now);
            let _ = donor.advance(1);
            assert!(publishes(&build(&mut donor, op)), "the capture goes Ready");
            let _ = donor.advance(2);
            let retired = retire(&mut donor);
            assert!(
                retired.iter().any(|effect| matches!(
                    effect,
                    BootstrapEffect::PublishClaim(claim) if claim.phase == ClaimPhase::Building
                )),
                "a Building claim keeps a joiner waiting through the lapse"
            );
            last_retired = donor.now;
        }
        let effects = donor.advance(1);
        if withdraws(&effects) {
            withdrawn.get_or_insert(donor.now);
        }
        if withdrawn.is_some() {
            assert!(!publishes(&effects), "a withdrawn claim stays withdrawn");
        }
    }
    let gaps: Vec<_> = starts.windows(2).map(|pair| pair[1] - pair[0]).collect();
    for (failures, gap) in (1_u32..).zip(&gaps) {
        let backoff = (CONFIG.observe_ms << (failures - 1)).min(CONFIG.donor_wait_ms / 4);
        assert!(
            *gap > backoff,
            "retry {failures} after {gap} ms: {starts:?}"
        );
    }
    // Backoffs of 2, 4, 8, 10 and 10 ms stay under the 40 ms window; the
    // sixth failure's 10 ms reaches it.
    assert_eq!(starts.len(), 6, "{starts:?}");
    assert_eq!(
        withdrawn,
        Some(last_retired + CONFIG.donor_wait_ms),
        "the last lapse's claim window runs out"
    );
    assert!(
        donor.engine.ready_recapture_pending(),
        "the image still awaits one"
    );

    donor.peer_boot += 1;
    let limit = donor.now + CONFIG.donor_wait_ms;
    let mut after = None;
    while after.is_none() && donor.now < limit {
        let _ = donor.advance(1);
        if donor.engine.ready_recapture_due() {
            after = donor.start();
        }
    }
    let op = after.expect("a membership change retries");
    let _ = build(&mut donor, op);
    let _ = retire(&mut donor);
    assert!(
        donor.engine.recapture_retry_due.unwrap().0 >= donor.now + CONFIG.donor_wait_ms / 4,
        "the backoff carries on across the membership change"
    );
    let limit = donor.now + CONFIG.donor_wait_ms;
    while donor.now < limit {
        let _ = donor.advance(1);
        if donor.engine.ready_recapture_due() {
            assert_eq!(
                donor.start(),
                None,
                "an unchanged membership retries nothing"
            );
        }
    }
}

/// A capture that stays Ready for a whole claim window proves the attempts
/// behind it: its retirement is recaptured without waiting, and the next
/// failure backs off from one observation interval again. One retired a
/// millisecond sooner failed instead.
#[test]
fn a_capture_that_survives_its_claim_window_clears_the_backoff() {
    let mut short = Donor::pending();
    let op = short.start().unwrap();
    let _ = build(&mut short, op);
    let _ = short.advance(CONFIG.donor_wait_ms - 1);
    let _ = retire(&mut short);
    assert!(
        !short.engine.ready_recapture_due(),
        "retired one millisecond short of its window, the capture failed"
    );
    assert_eq!(short.engine.recapture_failures, 1);

    let mut donor = Donor::pending();
    let first = donor.start().unwrap();
    let _ = donor.fail(first);
    let _ = donor.advance(CONFIG.observe_ms);
    let second = donor.start().unwrap();
    let _ = donor.fail(second);
    let _ = donor.advance(2 * CONFIG.observe_ms);
    let op = donor.start().unwrap();
    let _ = build(&mut donor, op);
    let _ = donor.advance(CONFIG.donor_wait_ms);
    let retired = retire(&mut donor);
    assert!(publishes(&retired), "a Building claim for the next attempt");
    assert!(donor.engine.ready_recapture_due());
    let op = donor.start().expect("recaptured without waiting");
    let _ = donor.fail(op);
    assert_eq!(
        donor.engine.recapture_retry_due,
        Some(Time(donor.now + CONFIG.observe_ms)),
        "the backoff starts over"
    );
}
