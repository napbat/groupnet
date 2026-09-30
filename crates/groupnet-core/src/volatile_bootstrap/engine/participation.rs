//! Complete source participation proof for an opted-in selection episode.

use std::collections::BTreeMap;

use crate::{NodeId, Time};

use super::{ClaimEngine, ObservedPresence};
use crate::volatile_bootstrap::{
    BootstrapClaim, BootstrapError, BootstrapMember, BootstrapMemberIdentity, BootstrapParticipant,
    BootstrapStep, PresenceIdentity, same_membership,
};

impl ClaimEngine {
    /// Allocate one finite fresh-roster read under the existing session token
    /// allocator. The worker performs no source I/O while holding a core lock.
    /// A transferred or retired candidate still participates, so the recovery
    /// core's post-handoff peer check reads the roster through the same path.
    ///
    /// # Errors
    /// Rejects a non-participating or inactive candidate, or exhausted time
    /// or token space.
    pub fn begin_roster_observation(
        &mut self,
    ) -> Result<crate::volatile_bootstrap::BootstrapOperation, BootstrapError> {
        if !self.participation_required
            || !matches!(
                self.stage,
                crate::volatile_bootstrap::BootstrapStage::Building
                    | crate::volatile_bootstrap::BootstrapStage::DonorAvailable
                    | crate::volatile_bootstrap::BootstrapStage::Transferring
                    | crate::volatile_bootstrap::BootstrapStage::Transferred
                    | crate::volatile_bootstrap::BootstrapStage::Participating
            )
        {
            return Err(BootstrapError::Stage);
        }
        let due = Time(
            self.now
                .0
                .checked_add(self.config.observe_ms)
                .ok_or(BootstrapError::Exhausted)?,
        );
        let op = self.allocate_token().ok_or(BootstrapError::Exhausted)?;
        self.roster_poll = Some(op);
        self.roster_poll_due = Some(due);
        Ok(op)
    }

    /// Recheck an already selected candidate against one complete source cut.
    /// This never chooses a new builder or grants local serving permission.
    /// The first verified cut is pinned; every later one must bind the same
    /// membership until the candidate's next selection or recapture.
    ///
    /// # Errors
    /// Rejects stale, incomplete, or changed participation before capture or
    /// barrier publication.
    pub fn verify_participant_roster(
        &mut self,
        op: crate::volatile_bootstrap::BootstrapOperation,
        members: &[BootstrapMember],
        roster: &[BootstrapMemberIdentity],
        participants: &[BootstrapParticipant],
        claims: &[BootstrapClaim],
    ) -> Result<(), BootstrapError> {
        if self.roster_poll != Some(op)
            || self.roster_poll_due.is_none_or(|due| self.now >= due)
            || !self.participation_required
            || !matches!(
                self.stage,
                crate::volatile_bootstrap::BootstrapStage::Building
                    | crate::volatile_bootstrap::BootstrapStage::DonorAvailable
                    | crate::volatile_bootstrap::BootstrapStage::Transferring
            )
        {
            return Err(BootstrapError::Stage);
        }
        let (roster, observed) =
            self.validate_participants(members, roster, participants, claims)?;
        self.matches_pinned_roster(&roster)?;
        if self.participant_roster.is_none() {
            self.participant_roster = Some(roster);
        }
        self.roster_poll = None;
        self.roster_poll_due = None;
        self.observed_presence = observed;
        Ok(())
    }

    /// A join, a leave, or a boot or session change since the pinned cut
    /// invalidates the decision that cut was verified for. A member's SWIM
    /// status or incarnation changing alone does not: see
    /// [`same_membership`].
    fn matches_pinned_roster(
        &self,
        roster: &[BootstrapMemberIdentity],
    ) -> Result<(), BootstrapError> {
        if self
            .participant_roster
            .as_deref()
            .is_some_and(|pinned| !same_membership(pinned, roster))
        {
            return Err(BootstrapError::InvalidObservation);
        }
        Ok(())
    }

    /// Validate one complete source cut for the recovery core's post-handoff
    /// peer check and return its exact roster. Unlike
    /// [`Self::verify_participant_roster`] it pins nothing: the recovery core
    /// compares the result with the handoff's covered members itself.
    ///
    /// # Errors
    /// Rejects a stale read, a candidate that has not finished transfer, or an
    /// incomplete or malformed participation cut.
    pub fn verify_peer_roster(
        &mut self,
        op: crate::volatile_bootstrap::BootstrapOperation,
        members: &[BootstrapMember],
        roster: &[BootstrapMemberIdentity],
        participants: &[BootstrapParticipant],
        claims: &[BootstrapClaim],
    ) -> Result<Vec<BootstrapMemberIdentity>, BootstrapError> {
        if self.roster_poll != Some(op)
            || self.roster_poll_due.is_none_or(|due| self.now >= due)
            || !matches!(
                self.stage,
                crate::volatile_bootstrap::BootstrapStage::Transferred
                    | crate::volatile_bootstrap::BootstrapStage::Participating
            )
        {
            return Err(BootstrapError::Stage);
        }
        let (roster, _) = self.validate_participants(members, roster, participants, claims)?;
        self.roster_poll = None;
        self.roster_poll_due = None;
        Ok(roster)
    }

    /// Choose from one complete cut, and pin it for the decision taken now.
    /// Membership may change between two selection samples, for example a
    /// member refuting a suspicion under load: each sample is a fresh
    /// decision, so it is not compared with the previous sample's pin. Only
    /// [`Self::verify_participant_roster`] holds a decision to its cut.
    pub(super) fn choose_participants(
        &mut self,
        members: &[BootstrapMember],
        roster: &[BootstrapMemberIdentity],
        participants: &[BootstrapParticipant],
        claims: Vec<BootstrapClaim>,
    ) -> BootstrapStep {
        let Ok((roster, observed)) =
            self.validate_participants(members, roster, participants, &claims)
        else {
            return Self::reject(BootstrapError::InvalidObservation);
        };
        let step = self.choose(members, claims);
        if step.rejection.is_none() {
            self.participant_roster = Some(roster);
            self.observed_presence = observed;
        }
        step
    }

    fn validate_participants(
        &self,
        members: &[BootstrapMember],
        roster: &[BootstrapMemberIdentity],
        participants: &[BootstrapParticipant],
        claims: &[BootstrapClaim],
    ) -> Result<
        (
            Vec<BootstrapMemberIdentity>,
            BTreeMap<PresenceIdentity, ObservedPresence>,
        ),
        BootstrapError,
    > {
        self.validate_roster(members, claims)?;
        if participants.len() > self.config.max_members || roster.len() != members.len() {
            return Err(BootstrapError::InvalidObservation);
        }
        for (index, (member, exact)) in members.iter().zip(roster).enumerate() {
            if member.node != exact.node
                || member.eligible != exact.eligible()
                || (index > 0 && roster[index - 1].node >= exact.node)
                || exact.presence.as_ref().is_some_and(|presence| {
                    presence.node != exact.node || presence.boot.0 == 0 || presence.session == 0
                })
                || (exact.eligible() && exact.presence.is_none())
            {
                return Err(BootstrapError::InvalidObservation);
            }
        }
        let mut by_node = BTreeMap::<NodeId, BootstrapMemberIdentity>::new();
        // The source cut contains at most one entry per native member node.
        // Retain renewal high-water only for identities still in this complete
        // cut; old boot identities cannot consume capacity for this session's
        // entire (potentially long-lived) donor tenure.
        let mut observed = BTreeMap::new();
        for participant in participants {
            let Some(identity) = participant.member.presence.as_ref() else {
                return Err(BootstrapError::InvalidObservation);
            };
            let node = &participant.member.node;
            if node.as_str().is_empty()
                || node.as_str().len() > self.config.max_member_bytes
                || identity.boot.0 == 0
                || identity.session == 0
                || participant.renewal == 0
                || participant.remaining_ms == 0
                || participant.remaining_ms > self.config.claim_ttl_ms
                || !roster.contains(&participant.member)
                || by_node
                    .insert(node.clone(), participant.member.clone())
                    .is_some()
            {
                return Err(BootstrapError::InvalidObservation);
            }
            let expiry = Time(
                self.now
                    .0
                    .checked_add(participant.remaining_ms)
                    .ok_or(BootstrapError::InvalidObservation)?,
            );
            let expiry = match self.observed_presence.get(identity) {
                Some(previous) if participant.renewal < previous.sequence => {
                    return Err(BootstrapError::InvalidObservation);
                }
                Some(previous) if participant.renewal == previous.sequence => {
                    expiry.min(previous.expires)
                }
                _ => expiry,
            };
            if expiry <= self.now || observed.len() >= self.config.max_members {
                return Err(BootstrapError::InvalidObservation);
            }
            observed.insert(
                identity.clone(),
                ObservedPresence {
                    sequence: participant.renewal,
                    expires: expiry,
                },
            );
        }
        if members
            .iter()
            .any(|member| member.eligible && !by_node.contains_key(&member.node))
            || roster.iter().any(|member| {
                member.presence.is_some() && by_node.get(&member.node) != Some(member)
            })
            || !by_node.get(&self.me).is_some_and(|member| {
                member.presence.as_ref().is_some_and(|presence| {
                    presence.boot == self.boot_incarnation && presence.session == self.session
                }) && member.eligible()
            })
            || claims.iter().any(|claim| {
                !by_node.get(&claim.identity.node).is_some_and(|member| {
                    member.presence.as_ref().is_some_and(|presence| {
                        presence.boot == claim.identity.incarnation
                            && presence.session == claim.identity.session
                    })
                })
            })
        {
            return Err(BootstrapError::InvalidObservation);
        }
        Ok((roster.to_vec(), observed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Status;
    use crate::volatile_bootstrap::{
        BootId, BootstrapConfig, BootstrapEffect, BootstrapEvent, BootstrapOperation,
        BootstrapScope, BootstrapStage, ClaimIdentity, ClaimPhase, PresenceIdentity,
    };

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

    fn facts() -> (
        Vec<BootstrapMember>,
        Vec<BootstrapParticipant>,
        Vec<BootstrapClaim>,
    ) {
        let me = NodeId::from("me");
        (
            vec![BootstrapMember {
                node: me.clone(),
                eligible: true,
            }],
            vec![BootstrapParticipant {
                member: BootstrapMemberIdentity {
                    node: me.clone(),
                    presence: Some(PresenceIdentity {
                        node: me.clone(),
                        boot: BootId(7),
                        session: 1,
                    }),
                    member_incarnation: 0,
                    status: Status::Alive,
                },
                renewal: 1,
                remaining_ms: 10,
            }],
            vec![BootstrapClaim {
                identity: ClaimIdentity {
                    node: me,
                    incarnation: BootId(7),
                    session: 1,
                    attempt: 1,
                },
                renewal: 1,
                phase: ClaimPhase::Willing,
                progress: 0,
                remaining_ms: 10,
            }],
        )
    }

    fn observe(engine: &mut ClaimEngine) -> BootstrapOperation {
        let _ = engine.step(BootstrapEvent::Start);
        engine
            .step(BootstrapEvent::Tick(Time(2)))
            .effects
            .iter()
            .find_map(|effect| match effect {
                BootstrapEffect::ObserveClaims { op, .. } => Some(*op),
                _ => None,
            })
            .unwrap()
    }

    fn roster(participants: &[BootstrapParticipant]) -> Vec<BootstrapMemberIdentity> {
        let mut exact = participants
            .iter()
            .map(|participant| participant.member.clone())
            .collect::<Vec<_>>();
        exact.sort_by(|a, b| a.node.cmp(&b.node));
        exact
    }

    #[test]
    fn complete_presence_is_required_and_claim_must_bind_same_worker() {
        let mut engine = engine();
        let op = observe(&mut engine);
        let (members, mut participants, claims) = facts();
        let exact = roster(&participants);
        let missing = engine.step(BootstrapEvent::ParticipantsObserved {
            op,
            members: members.clone(),
            roster: exact.clone(),
            participants: Vec::new(),
            claims: claims.clone(),
        });
        assert_eq!(missing.rejection, Some(BootstrapError::InvalidObservation));
        participants[0].member.presence.as_mut().unwrap().boot = BootId(9);
        let wrong_boot = engine.step(BootstrapEvent::ParticipantsObserved {
            op,
            members: members.clone(),
            roster: exact,
            participants,
            claims: claims.clone(),
        });
        assert_eq!(
            wrong_boot.rejection,
            Some(BootstrapError::InvalidObservation)
        );
        let (members, participants, claims) = facts();
        let ready = engine.step(BootstrapEvent::ParticipantsObserved {
            op,
            members,
            roster: roster(&participants),
            participants,
            claims,
        });
        assert!(ready.rejection.is_none());
        assert_eq!(engine.participant_roster().unwrap().len(), 1);
        let build = ready.effects.iter().find_map(|effect| match effect {
            BootstrapEffect::BuildOrigin { op, .. } => Some(*op),
            _ => None,
        });
        let selected = engine.selected().unwrap().clone();
        let local_only = engine.step(BootstrapEvent::LocalOnlyBuilt {
            op: build.unwrap(),
            selected,
        });
        assert_eq!(engine.stage(), BootstrapStage::DonorAvailable);
        assert!(engine.ready_recapture_pending());
        // The Building claim stays for the recapture to supersede.
        assert!(!local_only.effects.iter().any(|effect| matches!(
            effect,
            BootstrapEffect::WithdrawClaim(_)
                | BootstrapEffect::WithdrawPresence(_)
                | BootstrapEffect::PublishClaim(_)
        )));
        // A recapture starts only under a freshly verified complete cut.
        assert_eq!(
            engine.step(BootstrapEvent::StartReadyRecapture).rejection,
            Some(BootstrapError::Stage)
        );
        let (members, participants, _) = facts();
        let cut = engine.begin_roster_observation().unwrap();
        let claims = vec![engine.claim()];
        engine
            .verify_participant_roster(
                cut,
                &members,
                &roster(&participants),
                &participants,
                &claims,
            )
            .unwrap();
        let recapture = engine.step(BootstrapEvent::StartReadyRecapture);
        assert!(recapture.rejection.is_none());
        assert!(
            recapture
                .effects
                .iter()
                .any(|effect| { matches!(effect, BootstrapEffect::RecaptureCurrent { .. }) })
        );
        assert!(!engine.ready_recapture_pending());
    }

    /// A second live participant and the two-member roster that includes it.
    fn second_participant() -> (BootstrapParticipant, Vec<BootstrapMember>) {
        let peer = BootstrapParticipant {
            member: BootstrapMemberIdentity {
                node: NodeId::from("peer"),
                presence: Some(PresenceIdentity {
                    node: NodeId::from("peer"),
                    boot: BootId(9),
                    session: 2,
                }),
                member_incarnation: 1,
                status: Status::Alive,
            },
            renewal: 1,
            remaining_ms: 10,
        };
        let members = ["me", "peer"]
            .map(|node| BootstrapMember {
                node: NodeId::from(node),
                eligible: true,
            })
            .to_vec();
        (peer, members)
    }

    #[test]
    fn retired_ready_waits_for_complete_new_roster_before_one_recapture() {
        let mut engine = engine();
        let observe = observe(&mut engine);
        let (members, participants, claims) = facts();
        let built = engine.step(BootstrapEvent::ParticipantsObserved {
            op: observe,
            members,
            roster: roster(&participants),
            participants: participants.clone(),
            claims,
        });
        let op = built.effects.iter().find_map(|effect| match effect {
            BootstrapEffect::BuildOrigin { op, .. } => Some(*op),
            _ => None,
        });
        let old = engine.selected().unwrap().clone();
        assert!(
            engine
                .step(BootstrapEvent::Built {
                    op: op.unwrap(),
                    selected: old.clone(),
                })
                .rejection
                .is_none()
        );
        let retired = engine.step(BootstrapEvent::CaptureRetired {
            selected: old.clone(),
        });
        assert_eq!(engine.stage(), BootstrapStage::DonorAvailable);
        assert!(engine.ready_recapture_pending());
        assert_eq!(engine.participant_roster(), None);
        // The Ready claim is superseded by a fresh attempt's Building claim,
        // never withdrawn, so a joiner keeps waiting for the recapture.
        let superseding: Vec<_> = retired
            .effects
            .iter()
            .filter_map(|effect| match effect {
                BootstrapEffect::PublishClaim(claim) => Some(claim),
                _ => None,
            })
            .collect();
        assert_eq!(superseding.len(), 1);
        assert_eq!(superseding[0].phase, ClaimPhase::Building);
        assert_eq!(superseding[0].identity.node, old.node);
        assert!(superseding[0].identity.attempt > old.attempt);
        assert!(!retired.effects.iter().any(|effect| matches!(
            effect,
            BootstrapEffect::WithdrawClaim(_)
                | BootstrapEffect::BuildOrigin { .. }
                | BootstrapEffect::RecaptureCurrent { .. }
        )));
        assert!(engine.next_deadline().is_some()); // Presence renews while donation waits.

        let (peer, members) = second_participant();
        let mut exact = roster(&participants);
        exact.push(peer.member.clone());
        let incomplete = engine.begin_roster_observation().unwrap();
        assert_eq!(
            engine.verify_participant_roster(incomplete, &members, &exact, &participants, &[],),
            Err(BootstrapError::InvalidObservation)
        );
        assert!(engine.ready_recapture_pending());
        let complete = engine.begin_roster_observation().unwrap();
        let mut present = participants;
        present.push(peer);
        assert_eq!(
            engine.verify_participant_roster(complete, &members, &exact, &present, &[]),
            Ok(())
        );
        let recapture = engine.step(BootstrapEvent::StartReadyRecapture);
        assert!(!engine.ready_recapture_pending());
        assert!(
            recapture
                .effects
                .iter()
                .any(|effect| matches!(effect, BootstrapEffect::RecaptureCurrent { .. }))
        );
        assert!(
            !recapture
                .effects
                .iter()
                .any(|effect| matches!(effect, BootstrapEffect::BuildOrigin { .. }))
        );
    }

    /// A noneligible member without presence belongs to the roster, so its
    /// leaving, or its presence appearing, is a membership change. A SWIM
    /// suspicion, its refutation, or a Dead verdict changes only the
    /// observer's status and incarnation for the same process: the pinned
    /// decision stands.
    #[test]
    fn noneligible_native_member_without_presence_is_part_of_exact_roster() {
        let mut engine = engine();
        let _ = engine.step(BootstrapEvent::Start);
        let (mut members, participants, claims) = facts();
        members.push(BootstrapMember {
            node: NodeId::from("peer"),
            eligible: false,
        });
        let mut exact = roster(&participants);
        exact.push(BootstrapMemberIdentity {
            node: NodeId::from("peer"),
            presence: None,
            member_incarnation: 4,
            status: Status::Suspect,
        });
        let accepted = engine
            .validate_participants(&members, &exact, &participants, &claims)
            .expect("complete source cut");
        engine.participant_roster = Some(accepted.0);
        let mut changed = exact.clone();
        changed[1].member_incarnation = 5;
        changed[1].status = Status::Dead;
        let dead = engine
            .validate_participants(&members, &changed, &participants, &claims)
            .expect("a dead member is still a complete cut");
        assert_eq!(engine.matches_pinned_roster(&dead.0), Ok(()));
        let left = engine
            .validate_participants(&members[..1], &exact[..1], &participants, &claims)
            .expect("a cut without the member is complete");
        assert_eq!(
            engine.matches_pinned_roster(&left.0),
            Err(BootstrapError::InvalidObservation)
        );
        changed[1].presence = Some(PresenceIdentity {
            node: NodeId::from("peer"),
            boot: BootId(9),
            session: 2,
        });
        assert_eq!(
            engine.matches_pinned_roster(&changed),
            Err(BootstrapError::InvalidObservation)
        );
    }

    #[test]
    fn duplicate_presence_ttl_cannot_reacquire_expired_source_lifetime() {
        let mut engine = engine();
        let _ = observe(&mut engine);
        let (members, participants, claims) = facts();
        let exact = roster(&participants);
        let accepted = engine.choose_participants(&members, &exact, &participants, claims);
        assert!(accepted.rejection.is_none());
        let identity = participants[0].member.presence.clone().unwrap();
        assert_eq!(engine.observed_presence[&identity].expires, Time(12));
        engine.now = Time(8);
        let claims = vec![engine.claim()];
        let repeated = engine.validate_participants(&members, &exact, &participants, &claims);
        assert!(repeated.is_ok());
        assert_eq!(repeated.unwrap().1[&identity].expires, Time(12));
        engine.now = Time(12);
        assert_eq!(
            engine
                .validate_participants(&members, &exact, &participants, &claims)
                .unwrap_err(),
            BootstrapError::InvalidObservation
        );
    }

    #[test]
    fn complete_cuts_replace_old_boot_marks_without_losing_current_renewal_fence() {
        let mut engine = engine();
        let _ = engine.step(BootstrapEvent::Start);
        let (mut members, mut participants, mut claims) = facts();
        members.push(BootstrapMember {
            node: NodeId::from("peer"),
            eligible: true,
        });
        for boot in 8_u64..16 {
            participants.push(BootstrapParticipant {
                member: BootstrapMemberIdentity {
                    node: NodeId::from("peer"),
                    presence: Some(PresenceIdentity {
                        node: NodeId::from("peer"),
                        boot: BootId(u128::from(boot)),
                        session: 2,
                    }),
                    member_incarnation: boot,
                    status: Status::Alive,
                },
                renewal: if boot == 15 { 2 } else { 1 },
                remaining_ms: 10,
            });
            claims.push(BootstrapClaim {
                identity: ClaimIdentity {
                    node: NodeId::from("peer"),
                    incarnation: BootId(u128::from(boot)),
                    session: 2,
                    attempt: 1,
                },
                renewal: 1,
                phase: ClaimPhase::Willing,
                progress: 0,
                remaining_ms: 10,
            });
            let (_, current) = engine
                .validate_participants(&members, &roster(&participants), &participants, &claims)
                .expect("complete current cut");
            assert_eq!(current.len(), 2);
            engine.observed_presence = current;
            participants.pop();
            claims.pop();
        }
        participants.push(BootstrapParticipant {
            member: BootstrapMemberIdentity {
                node: NodeId::from("peer"),
                presence: Some(PresenceIdentity {
                    node: NodeId::from("peer"),
                    boot: BootId(15),
                    session: 2,
                }),
                member_incarnation: 15,
                status: Status::Alive,
            },
            renewal: 1,
            remaining_ms: 10,
        });
        claims.push(BootstrapClaim {
            identity: ClaimIdentity {
                node: NodeId::from("peer"),
                incarnation: BootId(15),
                session: 2,
                attempt: 1,
            },
            renewal: 1,
            phase: ClaimPhase::Willing,
            progress: 0,
            remaining_ms: 10,
        });
        assert_eq!(
            engine
                .validate_participants(&members, &roster(&participants), &participants, &claims)
                .unwrap_err(),
            BootstrapError::InvalidObservation
        );
    }

    #[test]
    fn delayed_old_roster_cut_cannot_replace_a_newer_observation() {
        let mut engine = engine();
        let _ = engine.step(BootstrapEvent::Start);
        engine.stage = BootstrapStage::Building;
        let old = engine.begin_roster_observation().unwrap();
        let current = engine.begin_roster_observation().unwrap();
        let (members, participants, _) = facts();
        let claims = vec![engine.claim()];
        assert_eq!(
            engine.verify_participant_roster(
                old,
                &members,
                &roster(&participants),
                &participants,
                &claims
            ),
            Err(BootstrapError::Stage)
        );
        assert!(engine.participant_roster().is_none());
        engine
            .verify_participant_roster(
                current,
                &members,
                &roster(&participants),
                &participants,
                &claims,
            )
            .unwrap();
        assert_eq!(engine.participant_roster().unwrap().len(), 1);
        assert_eq!(
            engine.verify_participant_roster(
                old,
                &members,
                &roster(&participants),
                &participants,
                &claims
            ),
            Err(BootstrapError::Stage)
        );
    }

    #[test]
    fn failed_ready_publication_stops_donation_without_restarting_capture() {
        let mut engine = engine();
        let op = observe(&mut engine);
        let (members, participants, claims) = facts();
        let decision = engine.step(BootstrapEvent::ParticipantsObserved {
            op,
            members,
            roster: roster(&participants),
            participants,
            claims,
        });
        let build = decision.effects.iter().find_map(|effect| match effect {
            BootstrapEffect::BuildOrigin { op, .. } => Some(*op),
            _ => None,
        });
        let selected = engine.selected().unwrap().clone();
        assert!(
            engine
                .step(BootstrapEvent::Built {
                    op: build.unwrap(),
                    selected: selected.clone(),
                })
                .rejection
                .is_none()
        );
        let stopped = engine.step(BootstrapEvent::DonorPublicationFailed {
            selected: selected.clone(),
        });
        assert_eq!(engine.stage(), BootstrapStage::Fallback);
        assert!(
            stopped
                .effects
                .contains(&BootstrapEffect::WithdrawClaim(selected.clone()))
        );
        assert!(!stopped.effects.iter().any(|effect| {
            matches!(
                effect,
                BootstrapEffect::RecaptureCurrent { .. } | BootstrapEffect::WithdrawPresence(_)
            )
        }));
        let later = engine.step(BootstrapEvent::Tick(Time(10)));
        assert!(
            later
                .effects
                .iter()
                .any(|effect| matches!(effect, BootstrapEffect::PublishPresence(_)))
        );
        assert!(!later.effects.iter().any(|effect| matches!(
            effect,
            BootstrapEffect::PublishClaim(_) | BootstrapEffect::RecaptureCurrent { .. }
        )));
        assert_eq!(
            engine
                .step(BootstrapEvent::DonorPublicationFailed { selected })
                .rejection,
            Some(BootstrapError::StaleOperation)
        );
    }

    #[test]
    fn unsupported_peer_roster_retires_claim_without_withdrawing_presence() {
        let mut engine = engine();
        let op = observe(&mut engine);
        let (mut members, mut participants, mut claims) = facts();
        let peer = NodeId::from("peer");
        members.push(BootstrapMember {
            node: peer.clone(),
            eligible: true,
        });
        participants.push(BootstrapParticipant {
            member: BootstrapMemberIdentity {
                node: peer.clone(),
                presence: Some(PresenceIdentity {
                    node: peer.clone(),
                    boot: BootId(8),
                    session: 2,
                }),
                member_incarnation: 0,
                status: Status::Alive,
            },
            renewal: 1,
            remaining_ms: 10,
        });
        let selected = ClaimIdentity {
            node: peer,
            incarnation: BootId(8),
            session: 2,
            attempt: 1,
        };
        claims.push(BootstrapClaim {
            identity: selected.clone(),
            renewal: 1,
            phase: ClaimPhase::Ready,
            progress: 0,
            remaining_ms: 10,
        });
        let following = engine.step(BootstrapEvent::ParticipantsObserved {
            op,
            members,
            roster: roster(&participants),
            participants,
            claims,
        });
        let follow_op = following.effects.iter().find_map(|effect| match effect {
            BootstrapEffect::DonorAvailable { op, .. } => Some(*op),
            _ => None,
        });
        let declined = engine.step(BootstrapEvent::PeerTransferDeclined {
            op: follow_op.unwrap(),
            selected,
        });
        assert!(declined.rejection.is_none());
        assert_eq!(engine.stage(), BootstrapStage::Fallback);
        assert!(
            declined
                .effects
                .iter()
                .any(|effect| matches!(effect, BootstrapEffect::WithdrawClaim(_)))
        );
        assert!(
            !declined
                .effects
                .iter()
                .any(|effect| matches!(effect, BootstrapEffect::WithdrawPresence(_)))
        );
    }
}
