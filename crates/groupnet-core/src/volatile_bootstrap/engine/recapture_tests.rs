//! A completed local image's Ready recapture under SWIM churn and failure.
//!
//! The donor's worker verifies one complete cut at C and another after the
//! image is encoded. On a loaded node both this node's own refutations and a
//! suspected peer change member incarnation and status between the two, with
//! every presence unchanged: the capture must still complete. A failed
//! attempt is retried only after a doubling backoff, and only inside the
//! claim window unless the membership changes. A Ready capture retired before
//! it has stayed Ready for a claim window is such a failed attempt. The
//! backoff paces only the participants the failures were taken under: a
//! joiner or a restarted peer starts its capture at once.

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
/// whose membership, presence, status and incarnation the test moves.
struct Donor {
    engine: ClaimEngine,
    now: u64,
    me_incarnation: u64,
    /// The peer is a member at all; a reaped one is not.
    peer_listed: bool,
    /// The peer's presence is live. Without it the peer is listed only as a
    /// suspected member, as a peer whose presence lapsed is.
    peer_present: bool,
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
            peer_listed: true,
            peer_present: true,
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
        assert!(donor.engine.ready_recapture_pending());
        assert!(!donor.engine.ready_recapture_backing_off());
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
        let mut roster = vec![me];
        if self.peer_listed {
            roster.push(BootstrapMemberIdentity {
                node: NodeId::from("peer"),
                presence: self.peer_present.then(|| PresenceIdentity {
                    node: NodeId::from("peer"),
                    boot: BootId(self.peer_boot),
                    session: 2,
                }),
                member_incarnation: self.peer_incarnation,
                status: if self.peer_present {
                    self.peer_status
                } else {
                    Status::Suspect
                },
            });
        }
        let members = roster
            .iter()
            .map(|member| BootstrapMember {
                node: member.node.clone(),
                eligible: member.eligible(),
            })
            .collect();
        let participants = roster
            .iter()
            .filter(|member| member.presence.is_some())
            .map(|member| BootstrapParticipant {
                member: member.clone(),
                renewal: if member.node == self.engine.me {
                    self.engine.presence_renewal
                } else {
                    self.peer_renewal
                },
                remaining_ms: CONFIG.claim_ttl_ms,
            })
            .collect();
        let claims = if self.engine.local_renewal > 0 && self.engine.renew_due.is_some() {
            vec![self.engine.claim()]
        } else {
            Vec::new()
        };
        (members, roster, participants, claims)
    }

    /// Sample and verify one cut, as the worker's `current_participation`.
    fn verify(&mut self) -> Result<(), BootstrapError> {
        let op = self.engine.begin_roster_observation()?;
        let (members, roster, participants, claims) = self.cut();
        self.engine
            .verify_participant_roster(op, &members, &roster, &participants, &claims)
    }

    /// Offer one freshly verified cut to the pending recapture, as every
    /// maintenance turn does, and verify the cut at C of an attempt that
    /// started. Returns its operation, or `None` if nothing started.
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

    /// A node that installed a peer's image under the current cut, retired
    /// its candidate, and adopted the image once its recovery went Ready.
    fn installed() -> Self {
        let mut donor = Self::pending();
        let (members, roster, participants, claims) = donor.cut();
        let op = donor.engine.begin_roster_observation().unwrap();
        donor
            .engine
            .verify_participant_roster(op, &members, &roster, &participants, &claims)
            .unwrap();
        // As after a completed transfer: the image was installed under the
        // verified cut, and no capture of this node's own is running.
        donor.engine.stage = BootstrapStage::Transferred;
        donor.engine.ready_capture = None;
        let retired = donor.engine.step(BootstrapEvent::RetireCandidate);
        assert_eq!(retired.rejection, None);
        assert!(withdraws(&retired.effects));
        assert_eq!(donor.engine.stage(), BootstrapStage::Participating);
        let adopted = donor.engine.step(BootstrapEvent::AdoptInstalled);
        assert_eq!(adopted.rejection, None);
        assert!(
            publishes_phase(&adopted.effects, ClaimPhase::Building),
            "adoption advertises the pending recapture"
        );
        donor
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

fn publishes_phase(effects: &[BootstrapEffect], phase: ClaimPhase) -> bool {
    effects.iter().any(
        |effect| matches!(effect, BootstrapEffect::PublishClaim(claim) if claim.phase == phase),
    )
}

/// Build a Ready capture for the running recapture `op`.
fn build(donor: &mut Donor, op: BootstrapOperation) -> Vec<BootstrapEffect> {
    let selected = donor.engine.selected().unwrap().clone();
    let built = donor.engine.step(BootstrapEvent::Built { op, selected });
    assert_eq!(built.rejection, None);
    built.effects
}

/// Retire the Ready capture, as a lapse, an expiry or a changed roster does.
fn retire(donor: &mut Donor) -> Vec<BootstrapEffect> {
    let selected = donor.engine.selected().unwrap().clone();
    let retired = donor
        .engine
        .step(BootstrapEvent::CaptureRetired { selected });
    assert_eq!(retired.rejection, None);
    retired.effects
}

/// A node Ready on a peer's installed image is a donor like an origin
/// build's: adoption advertises a pending recapture, the very next verified
/// cut captures the image, unchanged membership and all, and the Ready capture
/// is retired and replaced on a membership change like any other.
#[test]
fn an_adopted_image_offers_its_own_ready_capture_at_once() {
    let mut donor = Donor::installed();
    assert_eq!(donor.engine.stage(), BootstrapStage::DonorAvailable);
    assert!(donor.engine.ready_recapture_pending());
    let op = donor
        .start()
        .expect("the membership it installed under captures it");
    assert!(publishes_phase(&build(&mut donor, op), ClaimPhase::Ready));

    donor.peer_boot += 1;
    let _ = donor.advance(CONFIG.renew_ms);
    assert_eq!(
        donor.verify(),
        Err(BootstrapError::InvalidObservation),
        "a restarted peer changes the roster"
    );
    assert!(publishes_phase(&retire(&mut donor), ClaimPhase::Building));
    assert!(
        donor.start().is_some(),
        "the restarted peer's capture starts at once"
    );
}

/// Only an installed image is adopted: a retired origin builder, or a node
/// never selected, has nothing to adopt.
#[test]
fn only_an_installed_candidate_is_adopted() {
    let mut builder = Donor::pending();
    assert_eq!(
        builder
            .engine
            .step(BootstrapEvent::AdoptInstalled)
            .rejection,
        Some(BootstrapError::Stage)
    );
    let _ = builder.engine.step(BootstrapEvent::RetireCandidate);
    assert_eq!(builder.engine.stage(), BootstrapStage::Participating);
    assert_eq!(
        builder
            .engine
            .step(BootstrapEvent::AdoptInstalled)
            .rejection,
        Some(BootstrapError::Stage)
    );
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
    assert!(publishes_phase(&build(&mut donor, op), ClaimPhase::Ready));

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
/// cap of a quarter of the stall bound, however many maintenance turns offer
/// the unchanged cut meanwhile; the claim is renewed only until the window
/// that opened with the pending image closes, which no failure extends; after
/// it an unchanged membership starts nothing. A restarted peer is a new
/// participant: it starts its attempt at once, under a fresh claim window,
/// and its failure backs off from one interval again.
#[test]
fn failed_recaptures_back_off_inside_the_claim_window() {
    let mut donor = Donor::pending();
    let window = donor.now + CONFIG.donor_wait_ms;
    let mut starts = Vec::new();
    let mut withdrawn = None;
    while donor.now < window + 3 * CONFIG.donor_wait_ms {
        if donor.engine.ready_recapture_pending() {
            let backing_off = donor.engine.ready_recapture_backing_off();
            if let Some(op) = donor.start() {
                assert!(!backing_off, "started at {} inside a backoff", donor.now);
                starts.push(donor.now);
                let _ = donor.advance(1);
                let failed = donor.fail(op);
                if withdraws(&failed) {
                    withdrawn.get_or_insert(donor.now);
                }
            } else {
                assert!(
                    backing_off || donor.now >= window,
                    "an attempt is refused in the window"
                );
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
    let op = donor
        .start()
        .expect("a restarted peer starts its attempt at once");
    assert!(donor.engine.recapture_due.is_some(), "a fresh claim window");
    let failed = donor.fail(op);
    assert!(
        !withdraws(&failed),
        "its claim is renewed for the new window"
    );
    assert_eq!(
        donor.engine.recapture_retry_due,
        Some(Time(donor.now + CONFIG.observe_ms)),
        "the failures started over for the new participant"
    );
}

/// Run a donor whose every capture goes Ready and is retired by a lapse
/// moments later, for `ms`. Returns when each attempt started, and when the
/// claim was withdrawn, if it was.
fn self_lapsing(donor: &mut Donor, ms: u64) -> (Vec<u64>, Option<u64>, u64) {
    let mut starts = Vec::new();
    let mut last_retired = 0;
    let mut withdrawn = None;
    let limit = donor.now + ms;
    while donor.now < limit {
        if donor.engine.ready_recapture_pending()
            && let Some(op) = donor.start()
        {
            assert_eq!(withdrawn, None, "{starts:?}");
            starts.push(donor.now);
            let _ = donor.advance(1);
            assert!(publishes(&build(donor, op)), "the capture goes Ready");
            let _ = donor.advance(2);
            assert!(
                publishes_phase(&retire(donor), ClaimPhase::Building),
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
    (starts, withdrawn, last_retired)
}

/// Every capture stalls the node past its lease, so each Ready capture is
/// retired by a lapse moments after it is built. Each retirement supersedes
/// the Ready claim with a Building one for a fresh window, so a joiner waits
/// through the lapse, but each such capture is also a failed attempt: retries
/// back off as failures do, and once the backoffs add up to a claim window
/// the unchanged membership starts nothing more, however long the node runs
/// and however many turns offer it. The last window then runs out. A
/// restarted peer starts its own capture at once, and its own lapses are
/// bounded the same way.
#[test]
fn a_capture_that_costs_its_own_lapse_cannot_cycle() {
    let mut donor = Donor::pending();
    let (starts, withdrawn, last_retired) = self_lapsing(&mut donor, 6 * CONFIG.donor_wait_ms);
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
    let restarted = donor.now;
    let (starts, _, _) = self_lapsing(&mut donor, 6 * CONFIG.donor_wait_ms);
    assert_eq!(
        starts.first(),
        Some(&restarted),
        "the restarted peer's capture starts at once"
    );
    assert_eq!(
        starts.len(),
        6,
        "the restarted peer's lapses are bounded alike: {starts:?}"
    );
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
        short.engine.ready_recapture_backing_off(),
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
    assert!(!donor.engine.ready_recapture_backing_off());
    let op = donor.start().expect("recaptured without waiting");
    let _ = donor.fail(op);
    assert_eq!(
        donor.engine.recapture_retry_due,
        Some(Time(donor.now + CONFIG.observe_ms)),
        "the backoff starts over"
    );
}

/// A planned restart of the peer, as a rolling update makes it. Its leave
/// lapses this node's lease and fails the running attempt; its reap and its
/// seed re-registration each change the roster and retire the capture taken
/// in between, each a failure, so the backoff doubles. None of them names a
/// new participant, so each waits out its backoff. The replacement's presence
/// does: its capture starts on the first cut that names it, through the
/// backoff, which starts over.
#[test]
fn a_joiner_starts_its_capture_through_the_backoff_its_arrival_caused() {
    let mut donor = Donor::pending();
    let op = donor.start().unwrap();
    let _ = donor.fail(op);
    donor.peer_listed = false;
    assert_eq!(donor.start(), None, "the reap names no new participant");
    assert!(donor.engine.ready_recapture_backing_off());
    let _ = donor.advance(CONFIG.observe_ms);
    let op = donor
        .start()
        .expect("the changed membership starts once the backoff passed");
    let _ = build(&mut donor, op);
    let _ = donor.advance(1);
    donor.peer_listed = true;
    donor.peer_present = false;
    assert_eq!(donor.verify(), Err(BootstrapError::InvalidObservation));
    let _ = retire(&mut donor);
    assert_eq!(donor.engine.recapture_failures, 2);
    let backoff = donor.engine.recapture_retry_due.unwrap();
    assert_eq!(backoff, Time(donor.now + 2 * CONFIG.observe_ms));
    assert_eq!(
        donor.start(),
        None,
        "the re-registered peer without presence is no new participant"
    );

    donor.peer_present = true;
    donor.peer_boot += 1;
    let op = donor
        .start()
        .expect("the replacement's presence starts its capture at once");
    assert!(donor.now < backoff.0, "inside the backoff");
    assert_eq!(donor.engine.recapture_failures, 0);
    let selected = donor.engine.selected().unwrap().clone();
    let built = donor.engine.step(BootstrapEvent::Built { op, selected });
    assert!(publishes_phase(&built.effects, ClaimPhase::Ready));
}

/// A capture that stalls this node can make a suspected peer's presence
/// lapse in its view and return. That peer stays the participant it was, so
/// neither the lapse nor the return resets the backoff: only a new boot or
/// session does. The participants kept are at most one per listed node,
/// however often the peer restarts.
#[test]
fn a_presence_that_lapses_and_returns_names_no_new_participant() {
    let mut donor = Donor::pending();
    let op = donor.start().unwrap();
    let _ = donor.fail(op);
    donor.peer_present = false;
    assert_eq!(donor.start(), None);
    let _ = donor.advance(CONFIG.observe_ms);
    let op = donor
        .start()
        .expect("the lapsed presence changed the membership");
    let _ = donor.fail(op);
    assert_eq!(donor.engine.recapture_failures, 2);
    donor.peer_present = true;
    assert_eq!(
        donor.start(),
        None,
        "the returning presence is no new participant"
    );
    assert!(donor.engine.ready_recapture_backing_off());

    for _ in 0..5 {
        donor.peer_boot += 1;
        let op = donor.start().expect("a restarted peer starts at once");
        let _ = donor.fail(op);
        assert!(donor.engine.failed_participants.len() <= 2);
    }
}
