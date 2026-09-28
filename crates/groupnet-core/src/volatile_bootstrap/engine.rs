//! Sans-IO provisional builder selection. Claims are liveness hints only.

use std::collections::{BTreeMap, BTreeSet};

use crate::placement;
use crate::{NodeId, Time};

use super::types::{
    BootstrapClaim, BootstrapConfig, BootstrapEffect, BootstrapError, BootstrapEvent,
    BootstrapMember, BootstrapOperation, BootstrapScope, BootstrapStage, BootstrapStep,
    ClaimIdentity, ClaimPhase,
};

#[derive(Clone, Copy, Debug)]
struct ObservedRenewal {
    sequence: u64,
    phase: ClaimPhase,
    expires: Time,
}

/// Deterministic, bounded decision engine for a provisional origin builder.
/// It has no serving-authority effect and makes no source-completeness claim.
#[derive(Debug)]
pub struct ClaimEngine {
    config: BootstrapConfig,
    scope: BootstrapScope,
    me: NodeId,
    boot_incarnation: u64,
    session: u64,
    stage: BootstrapStage,
    now: Time,
    generation: u64,
    next_token: u64,
    local_phase: ClaimPhase,
    local_renewal: u64,
    settle_due: Option<Time>,
    renew_due: Option<Time>,
    operation_due: Option<Time>,
    follow_due: Option<Time>,
    total_due: Option<Time>,
    operation: Option<BootstrapOperation>,
    selected: Option<ClaimIdentity>,
    observed: BTreeMap<ClaimIdentity, ObservedRenewal>,
    excluded: BTreeSet<ClaimIdentity>,
}

impl ClaimEngine {
    /// Construct an unstarted selection session. `boot_incarnation` must be
    /// fresh across process restarts for the same node and nonzero.
    ///
    /// # Errors
    /// Rejects invalid limits, scope/identity bytes, or zero correlation IDs.
    pub fn new(
        config: BootstrapConfig,
        scope: BootstrapScope,
        me: NodeId,
        boot_incarnation: u64,
        session: u64,
    ) -> Result<Self, BootstrapError> {
        let config = config.validate()?;
        let names = scope
            .domain
            .len()
            .checked_add(scope.partition.len())
            .ok_or(BootstrapError::InvalidConfig)?;
        if scope.domain.is_empty()
            || scope.partition.is_empty()
            || names > config.max_scope_bytes
            || me.as_str().is_empty()
            || me.as_str().len() > config.max_member_bytes
            || boot_incarnation == 0
            || session == 0
        {
            return Err(BootstrapError::InvalidConfig);
        }
        Ok(Self {
            config,
            scope,
            me,
            boot_incarnation,
            session,
            stage: BootstrapStage::Unready,
            now: Time(0),
            generation: 0,
            next_token: 1,
            local_phase: ClaimPhase::Willing,
            local_renewal: 0,
            settle_due: None,
            renew_due: None,
            operation_due: None,
            follow_due: None,
            total_due: None,
            operation: None,
            selected: None,
            observed: BTreeMap::new(),
            excluded: BTreeSet::new(),
        })
    }

    /// Current non-authoritative selection stage.
    #[must_use]
    pub fn stage(&self) -> BootstrapStage {
        self.stage
    }

    /// Current local generation, incremented on explicit restart.
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Exact outstanding observation, builder, or follower operation.
    #[must_use]
    pub fn current_operation(&self) -> Option<BootstrapOperation> {
        self.operation
    }

    /// Exact selected donor or builder identity, if any.
    #[must_use]
    pub fn selected(&self) -> Option<&ClaimIdentity> {
        self.selected.as_ref()
    }

    /// Earliest finite local deadline; the runtime must drive `Tick` at it.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Time> {
        [
            self.settle_due,
            self.renew_due,
            self.operation_due,
            self.follow_due,
            self.total_due,
        ]
        .into_iter()
        .flatten()
        .min()
    }

    fn identity(&self) -> ClaimIdentity {
        ClaimIdentity {
            node: self.me.clone(),
            incarnation: self.boot_incarnation,
            session: self.session,
            attempt: self.generation,
        }
    }

    fn claim(&self) -> BootstrapClaim {
        BootstrapClaim {
            identity: self.identity(),
            renewal: self.local_renewal,
            phase: self.local_phase,
            remaining_ms: self.config.claim_ttl_ms,
        }
    }

    fn ok(&self, mut effects: Vec<BootstrapEffect>) -> BootstrapStep {
        if let Some(due) = self.next_deadline() {
            effects.push(BootstrapEffect::ArmTimer(due));
        }
        BootstrapStep {
            effects,
            rejection: None,
        }
    }

    fn reject(error: BootstrapError) -> BootstrapStep {
        BootstrapStep {
            effects: Vec::new(),
            rejection: Some(error),
        }
    }

    fn terminate(&mut self) -> BootstrapStep {
        let previous = self.operation.take();
        self.stage = BootstrapStage::Fallback;
        self.settle_due = None;
        self.renew_due = None;
        self.operation_due = None;
        self.follow_due = None;
        self.total_due = None;
        self.selected = None;
        let mut effects = Vec::new();
        if let Some(op) = previous {
            effects.push(BootstrapEffect::CancelWork { op });
        }
        effects.push(BootstrapEffect::WithdrawClaim(self.identity()));
        effects.push(BootstrapEffect::FallbackOrigin);
        self.ok(effects)
    }

    fn operation(&mut self, due_ms: u64) -> Result<BootstrapOperation, BootstrapError> {
        let due = Time(
            self.now
                .0
                .checked_add(due_ms)
                .ok_or(BootstrapError::Exhausted)?,
        );
        let token = self.next_token;
        self.next_token = token.checked_add(1).ok_or(BootstrapError::Exhausted)?;
        let op = BootstrapOperation {
            session: self.session,
            incarnation: self.boot_incarnation,
            generation: self.generation,
            token,
        };
        self.operation = Some(op);
        self.operation_due = Some(due.min(self.total_due.unwrap_or(due)));
        Ok(op)
    }

    fn observe(&mut self) -> BootstrapStep {
        let previous = self.operation;
        let Ok(op) = self.operation(self.config.observe_ms) else {
            return self.terminate();
        };
        self.stage = BootstrapStage::Observing;
        self.settle_due = None;
        let mut effects = Vec::new();
        if let Some(op) = previous {
            effects.push(BootstrapEffect::CancelWork { op });
        }
        effects.push(BootstrapEffect::ObserveClaims {
            op,
            max_members: self.config.max_members,
            max_member_bytes: self.config.max_member_bytes,
        });
        self.ok(effects)
    }

    fn publish_renewal(&mut self) -> Result<BootstrapEffect, BootstrapError> {
        self.local_renewal = self
            .local_renewal
            .checked_add(1)
            .ok_or(BootstrapError::Exhausted)?;
        self.renew_due = Some(Time(
            self.now
                .0
                .checked_add(self.config.renew_ms)
                .ok_or(BootstrapError::Exhausted)?,
        ));
        Ok(BootstrapEffect::PublishClaim(self.claim()))
    }

    fn start(&mut self) -> BootstrapStep {
        if self.stage == BootstrapStage::Cancelled {
            return Self::reject(BootstrapError::Stage);
        }
        let Some(generation) = self.generation.checked_add(1) else {
            return Self::reject(BootstrapError::Exhausted);
        };
        let Some(total_due) = self.now.0.checked_add(self.config.total_ms).map(Time) else {
            return Self::reject(BootstrapError::Exhausted);
        };
        let Some(settle_due) = self.now.0.checked_add(self.config.settle_ms).map(Time) else {
            return Self::reject(BootstrapError::Exhausted);
        };
        let Some(renew_due) = self.now.0.checked_add(self.config.renew_ms).map(Time) else {
            return Self::reject(BootstrapError::Exhausted);
        };
        let previous = (self.generation > 0).then(|| self.identity());
        let previous_operation = self.operation;
        self.generation = generation;
        self.stage = BootstrapStage::Settling;
        self.local_phase = ClaimPhase::Willing;
        self.local_renewal = 1;
        self.renew_due = Some(renew_due);
        self.total_due = Some(total_due);
        self.settle_due = Some(settle_due);
        self.operation = None;
        self.operation_due = None;
        self.follow_due = None;
        self.selected = None;
        self.observed.clear();
        self.excluded.clear();
        let mut effects = Vec::new();
        if let Some(op) = previous_operation {
            effects.push(BootstrapEffect::CancelWork { op });
        }
        if let Some(previous) = previous {
            effects.push(BootstrapEffect::WithdrawClaim(previous));
        }
        effects.push(BootstrapEffect::PublishClaim(self.claim()));
        self.ok(effects)
    }

    fn validate_roster(
        &self,
        members: &[BootstrapMember],
        claims: &[BootstrapClaim],
    ) -> Result<(), BootstrapError> {
        if members.is_empty()
            || members.len() > self.config.max_members
            || claims.len() > self.config.max_members
        {
            return Err(BootstrapError::InvalidObservation);
        }
        let mut eligible = BTreeSet::new();
        let mut all = BTreeSet::new();
        for member in members {
            if member.node.as_str().is_empty()
                || member.node.as_str().len() > self.config.max_member_bytes
                || !all.insert(member.node.clone())
            {
                return Err(BootstrapError::InvalidObservation);
            }
            if member.eligible {
                eligible.insert(member.node.clone());
            }
        }
        if !eligible.contains(&self.me) {
            return Err(BootstrapError::InvalidObservation);
        }
        let mut identities = BTreeSet::new();
        let mut nodes = BTreeMap::new();
        for claim in claims {
            let id = &claim.identity;
            if id.node.as_str().is_empty()
                || id.node.as_str().len() > self.config.max_member_bytes
                || id.incarnation == 0
                || id.session == 0
                || id.attempt == 0
                || claim.renewal == 0
                || claim.remaining_ms > self.config.claim_ttl_ms
                || !eligible.contains(&id.node)
                || !identities.insert(id.clone())
            {
                return Err(BootstrapError::InvalidObservation);
            }
            if id.node == self.me
                && (id.incarnation != self.boot_incarnation || id.session != self.session)
            {
                continue; // a retained claim from this node's previous session
            }
            if id.node == self.me
                && (id.attempt != self.generation
                    || claim.phase != self.local_phase
                    || claim.renewal != self.local_renewal)
            {
                return Err(BootstrapError::InvalidObservation);
            }
            if nodes.insert(id.node.clone(), id.clone()).is_some() {
                return Err(BootstrapError::InvalidObservation);
            }
        }
        Ok(())
    }

    fn track_claim(&mut self, claim: &BootstrapClaim) -> Result<bool, BootstrapError> {
        let id = &claim.identity;
        if id.node == self.me
            && (id.incarnation != self.boot_incarnation || id.session != self.session)
        {
            return Ok(false);
        }
        let new_expiry = self
            .now
            .0
            .checked_add(claim.remaining_ms)
            .map(Time)
            .ok_or(BootstrapError::Exhausted)?;
        let expiry = match self.observed.get(id) {
            Some(previous) if claim.renewal < previous.sequence => {
                return Err(BootstrapError::InvalidObservation);
            }
            Some(previous) if claim.renewal == previous.sequence => {
                if claim.phase != previous.phase {
                    return Err(BootstrapError::InvalidObservation);
                }
                previous.expires.min(new_expiry)
            }
            Some(previous) => {
                if claim.phase < previous.phase {
                    return Err(BootstrapError::InvalidObservation);
                }
                new_expiry
            }
            None => new_expiry,
        };
        if !self.observed.contains_key(id) && self.observed.len() >= self.config.max_members {
            return Err(BootstrapError::InvalidObservation);
        }
        self.observed.insert(
            id.clone(),
            ObservedRenewal {
                sequence: claim.renewal,
                phase: claim.phase,
                expires: expiry,
            },
        );
        Ok(expiry > self.now && claim.remaining_ms > 0)
    }

    fn choose(
        &mut self,
        members: &[BootstrapMember],
        claims: Vec<BootstrapClaim>,
    ) -> BootstrapStep {
        if self.validate_roster(members, &claims).is_err() {
            return Self::reject(BootstrapError::InvalidObservation);
        }
        if self.follow_due.is_some_and(|due| self.now >= due)
            && let Some(selected) = self.selected.take()
        {
            self.excluded.insert(selected);
            self.follow_due = None;
        }
        // Keep the bounded renewal high-water marks until the episode ends.
        // Forgetting an expired mark would let a stale source record revive it.
        let mut candidates = BTreeMap::new();
        for claim in claims {
            match self.track_claim(&claim) {
                Ok(true) if !self.excluded.contains(&claim.identity) => {
                    candidates.insert(claim.identity.node.clone(), claim);
                }
                Ok(_) => {}
                Err(error) => return Self::reject(error),
            }
        }
        if candidates.len() > self.config.max_members || !candidates.contains_key(&self.me) {
            return Self::reject(BootstrapError::InvalidObservation);
        }
        let incumbents: BTreeSet<_> = candidates
            .iter()
            .filter(|(_, claim)| claim.phase != ClaimPhase::Willing)
            .map(|(node, _)| node.clone())
            .collect();
        let roster = if incumbents.is_empty() {
            candidates.keys().cloned().collect()
        } else {
            incumbents
        };
        let Some(winner) = placement::owner(&self.scope.placement_key(), &roster) else {
            return Self::reject(BootstrapError::InvalidObservation);
        };
        let selected = candidates.remove(&winner).expect("winner was in roster");
        let same_selection = self.selected.as_ref() == Some(&selected.identity);
        self.selected = Some(selected.identity.clone());
        self.operation = None;
        self.operation_due = None;
        if winner == self.me {
            self.follow_due = None;
            if selected.phase == ClaimPhase::Ready {
                self.stage = BootstrapStage::DonorAvailable;
                self.total_due = None;
                return self.ok(Vec::new());
            }
            self.local_phase = ClaimPhase::Building;
            let Ok(claim) = self.publish_renewal() else {
                return self.terminate();
            };
            let Ok(op) = self.operation(self.config.donor_wait_ms) else {
                return self.terminate();
            };
            self.stage = BootstrapStage::Building;
            self.ok(vec![
                claim,
                BootstrapEffect::BuildOrigin {
                    op,
                    selected: selected.identity,
                },
            ])
        } else {
            if selected.phase == ClaimPhase::Ready {
                self.follow_due = None;
            } else if !same_selection || self.follow_due.is_none() {
                let Some(due) = self.now.0.checked_add(self.config.donor_wait_ms).map(Time) else {
                    return self.terminate();
                };
                self.follow_due = Some(due.min(self.total_due.unwrap_or(due)));
            }
            let Ok(op) = self.operation(if selected.phase == ClaimPhase::Ready {
                self.config.donor_wait_ms
            } else {
                self.config.observe_ms
            }) else {
                return self.terminate();
            };
            self.stage = if selected.phase == ClaimPhase::Ready {
                BootstrapStage::DonorAvailable
            } else {
                BootstrapStage::Following
            };
            let effect = if selected.phase == ClaimPhase::Ready {
                BootstrapEffect::DonorAvailable {
                    op,
                    selected: selected.identity,
                }
            } else {
                BootstrapEffect::FollowBuilder {
                    op,
                    selected: selected.identity,
                }
            };
            self.ok(vec![effect])
        }
    }

    fn tick(&mut self, now: Time) -> BootstrapStep {
        if now < self.now {
            return Self::reject(BootstrapError::BackwardTime);
        }
        self.now = now;
        if self.total_due.is_some_and(|due| now >= due) {
            return self.terminate();
        }
        let mut effects = Vec::new();
        if self.renew_due.is_some_and(|due| now >= due) {
            match self.publish_renewal() {
                Ok(claim) => effects.push(claim),
                Err(_) => return self.terminate(),
            }
        }
        if self.stage == BootstrapStage::Building
            && self.operation_due.is_some_and(|due| now >= due)
        {
            return self.terminate();
        }
        if self.settle_due.is_some_and(|due| now >= due)
            || self.operation_due.is_some_and(|due| now >= due)
            || self.follow_due.is_some_and(|due| now >= due)
        {
            if self.follow_due.is_some_and(|due| now >= due)
                && let Some(selected) = self.selected.clone()
            {
                self.excluded.insert(selected);
                self.follow_due = None;
                if self.excluded.len() > self.config.max_members {
                    return self.terminate();
                }
            }
            let observation = self.observe();
            effects.extend(
                observation
                    .effects
                    .into_iter()
                    .filter(|effect| !matches!(effect, BootstrapEffect::ArmTimer(_))),
            );
        }
        self.ok(effects)
    }

    fn cancel(&mut self) -> BootstrapStep {
        if self.stage == BootstrapStage::Cancelled {
            return self.ok(Vec::new());
        }
        let identity = (self.generation > 0).then(|| self.identity());
        let previous_operation = self.operation;
        self.stage = BootstrapStage::Cancelled;
        self.settle_due = None;
        self.renew_due = None;
        self.operation_due = None;
        self.follow_due = None;
        self.total_due = None;
        self.operation = None;
        self.selected = None;
        let mut effects = Vec::new();
        if let Some(op) = previous_operation {
            effects.push(BootstrapEffect::CancelWork { op });
        }
        if let Some(id) = identity {
            effects.push(BootstrapEffect::WithdrawClaim(id));
        }
        self.ok(effects)
    }

    /// Consume one event and emit only correlated, bounded decisions.
    pub fn step(&mut self, event: BootstrapEvent) -> BootstrapStep {
        match event {
            BootstrapEvent::Start => self.start(),
            BootstrapEvent::ClaimsObserved {
                op,
                members,
                claims,
            } => {
                if self.stage != BootstrapStage::Observing || self.operation != Some(op) {
                    return Self::reject(BootstrapError::StaleOperation);
                }
                self.choose(&members, claims)
            }
            BootstrapEvent::Built { op, selected } => {
                if self.stage != BootstrapStage::Building
                    || self.operation != Some(op)
                    || self.selected.as_ref() != Some(&selected)
                {
                    return Self::reject(BootstrapError::StaleOperation);
                }
                self.operation = None;
                self.operation_due = None;
                self.total_due = None;
                self.stage = BootstrapStage::DonorAvailable;
                self.local_phase = ClaimPhase::Ready;
                let Ok(claim) = self.publish_renewal() else {
                    return self.terminate();
                };
                self.ok(vec![claim])
            }
            BootstrapEvent::BuildFailed { op, selected } => {
                if self.stage != BootstrapStage::Building
                    || self.operation != Some(op)
                    || self.selected.as_ref() != Some(&selected)
                {
                    return Self::reject(BootstrapError::StaleOperation);
                }
                self.terminate()
            }
            BootstrapEvent::DonorUnavailable { op, selected } => {
                if !matches!(
                    self.stage,
                    BootstrapStage::Following | BootstrapStage::DonorAvailable
                ) || self.operation != Some(op)
                    || self.selected.as_ref() != Some(&selected)
                {
                    return Self::reject(BootstrapError::StaleOperation);
                }
                self.excluded.insert(selected);
                self.follow_due = None;
                if self.excluded.len() > self.config.max_members {
                    return self.terminate();
                }
                self.observe()
            }
            BootstrapEvent::Tick(now) => self.tick(now),
            BootstrapEvent::Cancel => self.cancel(),
        }
    }
}

#[cfg(test)]
mod renewal_tests {
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
            7,
            1,
        )
        .unwrap()
    }

    #[test]
    fn repeated_stale_claim_never_refreshes_its_original_expiry() {
        let mut engine = engine();
        let mut claim = BootstrapClaim {
            identity: ClaimIdentity {
                node: NodeId::from("peer"),
                incarnation: 9,
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
                incarnation: 7,
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
}
