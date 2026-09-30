//! Sans-IO provisional builder selection. Claims are liveness hints only.

use std::collections::{BTreeMap, BTreeSet};

use crate::{NodeId, Time};

use super::transfer::{TransferConfig, TransferSession};

use super::types::{
    BootId, BootstrapClaim, BootstrapConfig, BootstrapEffect, BootstrapError, BootstrapEvent,
    BootstrapMember, BootstrapMemberIdentity, BootstrapOperation, BootstrapPresence,
    BootstrapScope, BootstrapStage, BootstrapStep, ClaimIdentity, ClaimPhase, PresenceIdentity,
};

mod participation;
mod selection;
mod transfer;

#[derive(Clone, Copy, Debug)]
struct ObservedRenewal {
    sequence: u64,
    phase: ClaimPhase,
    progress: u64,
    expires: Time,
}

#[derive(Clone, Copy, Debug)]
struct ObservedPresence {
    sequence: u64,
    expires: Time,
}

/// Deterministic, bounded decision engine for a provisional origin builder.
/// It has no serving-authority effect and makes no source-completeness claim.
#[derive(Debug)]
pub struct ClaimEngine {
    config: BootstrapConfig,
    scope: BootstrapScope,
    me: NodeId,
    boot_incarnation: BootId,
    session: u64,
    stage: BootstrapStage,
    now: Time,
    generation: u64,
    next_token: u64,
    local_phase: ClaimPhase,
    local_renewal: u64,
    local_progress: u64,
    follow_progress: Option<u64>,
    presence_renewal: u64,
    presence_due: Option<Time>,
    participation_required: bool,
    participant_roster: Option<Vec<BootstrapMemberIdentity>>,
    observed_presence: BTreeMap<PresenceIdentity, ObservedPresence>,
    settle_due: Option<Time>,
    renew_due: Option<Time>,
    operation_due: Option<Time>,
    follow_due: Option<Time>,
    ready_retry_due: Option<Time>,
    total_due: Option<Time>,
    operation: Option<BootstrapOperation>,
    selected: Option<ClaimIdentity>,
    observed: BTreeMap<ClaimIdentity, ObservedRenewal>,
    excluded: BTreeSet<ClaimIdentity>,
    transfer_config: Option<TransferConfig>,
    transfer: Option<TransferSession>,
    claim_refresh_due: Option<Time>,
    claim_poll: Option<BootstrapOperation>,
    claim_poll_due: Option<Time>,
    roster_poll: Option<BootstrapOperation>,
    roster_poll_due: Option<Time>,
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
        boot_incarnation: BootId,
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
            || boot_incarnation.0 == 0
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
            local_progress: 0,
            follow_progress: None,
            presence_renewal: 0,
            presence_due: None,
            participation_required: false,
            participant_roster: None,
            observed_presence: BTreeMap::new(),
            settle_due: None,
            renew_due: None,
            operation_due: None,
            follow_due: None,
            ready_retry_due: None,
            total_due: None,
            operation: None,
            selected: None,
            observed: BTreeMap::new(),
            excluded: BTreeSet::new(),
            transfer_config: None,
            transfer: None,
            claim_refresh_due: None,
            claim_poll: None,
            claim_poll_due: None,
            roster_poll: None,
            roster_poll_due: None,
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

    /// Deadline for one exact source, builder, selected-claim, or transfer
    /// operation. Renewal and unrelated timers never shorten its I/O budget.
    #[must_use]
    pub fn operation_deadline(&self, op: BootstrapOperation) -> Option<Time> {
        if self.operation == Some(op) {
            return self.operation_due;
        }
        if self.claim_poll == Some(op) {
            return self.claim_poll_due;
        }
        if self.roster_poll == Some(op) {
            return self.roster_poll_due;
        }
        self.transfer.as_ref().and_then(|transfer| {
            (transfer.current_operation() == Some(op))
                .then(|| transfer.next_deadline())
                .flatten()
        })
    }

    /// Exact selected donor or builder identity, if any.
    #[must_use]
    pub fn selected(&self) -> Option<&ClaimIdentity> {
        self.selected.as_ref()
    }

    /// Opt into source-certified participation before the first episode.
    /// Existing claim-only sessions retain their previous behavior.
    ///
    /// # Errors
    /// Rejects enabling after a selection has started.
    pub fn require_participation(&mut self) -> Result<(), BootstrapError> {
        if self.stage != BootstrapStage::Unready {
            return Err(BootstrapError::Stage);
        }
        self.participation_required = true;
        Ok(())
    }

    /// Exact sorted participant identities from the last accepted actor cut.
    /// They are source evidence only, never read or donor authority.
    #[must_use]
    pub fn participant_roster(&self) -> Option<&[BootstrapMemberIdentity]> {
        self.participant_roster.as_deref()
    }

    /// A completed local origin image needs one fresh Ready donor recapture.
    #[must_use]
    pub fn ready_recapture_pending(&self) -> bool {
        self.participation_required
            && self.stage == BootstrapStage::DonorAvailable
            && self.local_phase == ClaimPhase::Building
            && self.selected.as_ref() == Some(&self.identity())
    }

    /// Earliest finite local deadline; the runtime must drive `Tick` at it.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Time> {
        [
            self.settle_due,
            self.renew_due,
            self.presence_due,
            self.operation_due,
            self.follow_due,
            self.total_due,
            self.transfer
                .as_ref()
                .and_then(TransferSession::next_deadline),
            self.claim_refresh_due,
            self.claim_poll_due,
            (self.stage == BootstrapStage::Transferring)
                .then(|| {
                    self.selected
                        .as_ref()
                        .and_then(|id| self.observed.get(id))
                        .map(|claim| claim.expires)
                })
                .flatten(),
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

    fn presence_identity(&self) -> PresenceIdentity {
        PresenceIdentity {
            node: self.me.clone(),
            boot: self.boot_incarnation,
            session: self.session,
        }
    }

    fn presence(&self) -> BootstrapPresence {
        BootstrapPresence {
            identity: self.presence_identity(),
            renewal: self.presence_renewal,
            remaining_ms: self.config.claim_ttl_ms,
        }
    }

    fn publish_presence(&mut self) -> Result<BootstrapEffect, BootstrapError> {
        self.presence_renewal = self
            .presence_renewal
            .checked_add(1)
            .ok_or(BootstrapError::Exhausted)?;
        self.presence_due = Some(Time(
            self.now
                .0
                .checked_add(self.config.renew_ms)
                .ok_or(BootstrapError::Exhausted)?,
        ));
        Ok(BootstrapEffect::PublishPresence(self.presence()))
    }

    fn claim(&self) -> BootstrapClaim {
        BootstrapClaim {
            identity: self.identity(),
            renewal: self.local_renewal,
            phase: self.local_phase,
            progress: self.local_progress,
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
        let mut effects = Vec::new();
        if let Some(op) = previous {
            effects.push(BootstrapEffect::CancelWork { op });
        }
        effects.extend(self.cancel_transfer());
        self.stage = BootstrapStage::Fallback;
        self.settle_due = None;
        self.renew_due = None;
        self.operation_due = None;
        self.follow_due = None;
        self.ready_retry_due = None;
        self.total_due = None;
        self.selected = None;
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
        let op = self.allocate_token().ok_or(BootstrapError::Exhausted)?;
        self.operation = Some(op);
        self.operation_due = Some(due.min(self.total_due.unwrap_or(due)));
        Ok(op)
    }

    fn allocate_token(&mut self) -> Option<BootstrapOperation> {
        let token = self.next_token;
        self.next_token = token.checked_add(1)?;
        Some(BootstrapOperation {
            session: self.session,
            incarnation: self.boot_incarnation,
            generation: self.generation,
            token,
        })
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
        let transfer_cleanup = self.cancel_transfer();
        let previous_operation = self.operation;
        self.generation = generation;
        self.stage = BootstrapStage::Settling;
        self.local_phase = ClaimPhase::Willing;
        self.local_renewal = 1;
        self.local_progress = 0;
        self.follow_progress = None;
        self.renew_due = Some(renew_due);
        self.total_due = Some(total_due);
        self.settle_due = Some(settle_due);
        self.operation = None;
        self.operation_due = None;
        self.follow_due = None;
        self.ready_retry_due = None;
        self.selected = None;
        self.observed.clear();
        self.observed_presence.clear();
        self.participant_roster = None;
        self.roster_poll = None;
        self.roster_poll_due = None;
        self.excluded.clear();
        let mut effects = Vec::new();
        if let Some(op) = previous_operation {
            effects.push(BootstrapEffect::CancelWork { op });
        }
        effects.extend(transfer_cleanup);
        if let Some(previous) = previous {
            effects.push(BootstrapEffect::WithdrawClaim(previous));
        }
        if self.presence_due.is_none() {
            match self.publish_presence() {
                Ok(presence) => effects.push(presence),
                Err(_) => return self.presence_failure(),
            }
        }
        effects.push(BootstrapEffect::PublishClaim(self.claim()));
        self.ok(effects)
    }

    fn presence_failure(&mut self) -> BootstrapStep {
        self.presence_due = None;
        let mut step = self.terminate();
        let before_fallback = step
            .effects
            .iter()
            .position(|effect| matches!(effect, BootstrapEffect::FallbackOrigin))
            .unwrap_or(step.effects.len());
        step.effects.insert(
            before_fallback,
            BootstrapEffect::WithdrawPresence(self.presence_identity()),
        );
        step
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
                || id.incarnation.0 == 0
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
                    || claim.renewal != self.local_renewal
                    || claim.progress != self.local_progress)
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
                if claim.phase != previous.phase || claim.progress != previous.progress {
                    return Err(BootstrapError::InvalidObservation);
                }
                previous.expires.min(new_expiry)
            }
            Some(previous) => {
                if claim.phase < previous.phase || claim.progress < previous.progress {
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
                progress: claim.progress,
                expires: expiry,
            },
        );
        Ok(expiry > self.now && claim.remaining_ms > 0)
    }

    fn tick(&mut self, now: Time) -> BootstrapStep {
        if now < self.now {
            return Self::reject(BootstrapError::BackwardTime);
        }
        self.now = now;
        let mut presence = Vec::new();
        if self.presence_due.is_some_and(|due| now >= due) {
            match self.publish_presence() {
                Ok(effect) => presence.push(effect),
                Err(_) => return self.presence_failure(),
            }
        }
        let mut step = self.tick_claim(now);
        if step.rejection.is_none() && !presence.is_empty() {
            presence.extend(
                step.effects
                    .into_iter()
                    .filter(|effect| !matches!(effect, BootstrapEffect::ArmTimer(_))),
            );
            step = self.ok(presence);
        }
        step
    }

    fn tick_claim(&mut self, now: Time) -> BootstrapStep {
        if self.stage == BootstrapStage::Transferring {
            return self.tick_transfer(now);
        }
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

    fn retire_candidate(&mut self) -> BootstrapStep {
        if matches!(
            self.stage,
            BootstrapStage::Cancelled | BootstrapStage::Participating
        ) {
            return self.ok(Vec::new());
        }
        let identity = (self.generation > 0).then(|| self.identity());
        let previous_operation = self.operation.take();
        let roster_poll = self.roster_poll.take();
        let mut effects = Vec::new();
        if let Some(op) = previous_operation {
            effects.push(BootstrapEffect::CancelWork { op });
        }
        if let Some(op) = roster_poll.filter(|op| Some(*op) != previous_operation) {
            effects.push(BootstrapEffect::CancelWork { op });
        }
        effects.extend(self.cancel_transfer());
        if let Some(id) = identity {
            effects.push(BootstrapEffect::WithdrawClaim(id));
        }
        self.stage = BootstrapStage::Participating;
        self.settle_due = None;
        self.renew_due = None;
        self.operation_due = None;
        self.roster_poll_due = None;
        self.follow_due = None;
        self.ready_retry_due = None;
        self.total_due = None;
        self.selected = None;
        self.participant_roster = None;
        self.ok(effects)
    }

    fn cancel(&mut self) -> BootstrapStep {
        if self.stage == BootstrapStage::Cancelled {
            return self.ok(Vec::new());
        }
        let identity = (self.generation > 0).then(|| self.identity());
        let transfer_cleanup = self.cancel_transfer();
        let previous_operation = self.operation;
        self.stage = BootstrapStage::Cancelled;
        self.settle_due = None;
        self.renew_due = None;
        self.presence_due = None;
        self.operation_due = None;
        self.follow_due = None;
        self.ready_retry_due = None;
        self.total_due = None;
        self.operation = None;
        self.selected = None;
        let mut effects = Vec::new();
        if let Some(op) = previous_operation {
            effects.push(BootstrapEffect::CancelWork { op });
        }
        effects.extend(transfer_cleanup);
        if let Some(id) = identity {
            effects.push(BootstrapEffect::WithdrawClaim(id));
        }
        if self.presence_renewal > 0 {
            effects.push(BootstrapEffect::WithdrawPresence(self.presence_identity()));
        }
        self.ok(effects)
    }

    /// Consume one event and emit only correlated, bounded decisions.
    #[expect(
        clippy::too_many_lines,
        reason = "one finite sans-IO event dispatch keeps operation correlation visible"
    )]
    pub fn step(&mut self, event: BootstrapEvent) -> BootstrapStep {
        match event {
            BootstrapEvent::Start => self.start(),
            BootstrapEvent::ClaimsObserved {
                op,
                members,
                claims,
            } => {
                if self.participation_required
                    || self.stage != BootstrapStage::Observing
                    || self.operation != Some(op)
                {
                    return Self::reject(BootstrapError::StaleOperation);
                }
                self.choose(&members, claims)
            }
            BootstrapEvent::ParticipantsObserved {
                op,
                members,
                roster,
                participants,
                claims,
            } => {
                if !self.participation_required
                    || self.stage != BootstrapStage::Observing
                    || self.operation != Some(op)
                {
                    return Self::reject(BootstrapError::StaleOperation);
                }
                self.choose_participants(&members, &roster, &participants, claims)
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
            BootstrapEvent::BuildProgressed { op, selected } => {
                self.build_progressed(op, &selected)
            }
            BootstrapEvent::LocalOnlyBuilt { op, selected } => self.local_only_built(op, selected),
            BootstrapEvent::StartReadyRecapture => self.start_ready_recapture(),
            BootstrapEvent::PeerTransferDeclined { op, selected } => {
                if self.stage != BootstrapStage::DonorAvailable
                    || self.operation != Some(op)
                    || self.selected.as_ref() != Some(&selected)
                    || selected.node == self.me
                {
                    Self::reject(BootstrapError::StaleOperation)
                } else {
                    self.terminate()
                }
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
                if self.stage == BootstrapStage::Transferring
                    && self.operation == Some(op)
                    && self.selected.as_ref() == Some(&selected)
                {
                    let mut effects = vec![BootstrapEffect::CancelWork { op }];
                    effects.extend(self.cancel_transfer());
                    self.operation = None;
                    self.operation_due = None;
                    self.observed.remove(&selected);
                    self.excluded.insert(selected);
                    let next = self.observe();
                    effects.extend(
                        next.effects
                            .into_iter()
                            .filter(|effect| !matches!(effect, BootstrapEffect::ArmTimer(_))),
                    );
                    return self.ok(effects);
                }
                if !matches!(
                    self.stage,
                    BootstrapStage::Following | BootstrapStage::DonorAvailable
                ) || self.operation != Some(op)
                    || self.selected.as_ref() != Some(&selected)
                {
                    return Self::reject(BootstrapError::StaleOperation);
                }
                self.observed.remove(&selected);
                self.excluded.insert(selected);
                self.follow_due = None;
                if self.excluded.len() > self.config.max_members {
                    return self.terminate();
                }
                self.observe()
            }
            BootstrapEvent::StartTransfer { op, selected } => self.start_transfer(op, selected),
            BootstrapEvent::SelectedClaimObserved { op, claim } => {
                self.selected_claim_observed(op, claim)
            }
            BootstrapEvent::Transfer(event) => {
                if matches!(
                    event.as_ref(),
                    super::transfer::TransferEvent::Start | super::transfer::TransferEvent::Tick(_)
                ) {
                    Self::reject(BootstrapError::Stage)
                } else {
                    self.transfer_event(*event)
                }
            }
            BootstrapEvent::Tick(now) => self.tick(now),
            BootstrapEvent::PresenceFailed { identity, renewal } => {
                if self.stage == BootstrapStage::Cancelled
                    || self.presence_due.is_none()
                    || identity != self.presence_identity()
                    || renewal != self.presence_renewal
                {
                    Self::reject(BootstrapError::StaleOperation)
                } else {
                    self.presence_failure()
                }
            }
            BootstrapEvent::CaptureRetired { selected } => {
                if self.stage != BootstrapStage::DonorAvailable
                    || self.selected.as_ref() != Some(&selected)
                    || selected != self.identity()
                {
                    Self::reject(BootstrapError::StaleOperation)
                } else if self.participation_required {
                    let old = self.identity();
                    self.local_phase = ClaimPhase::Building;
                    // Withdraw stale C immediately. Wait for a complete fresh
                    // native cut before starting the one bounded recapture;
                    // a newly Alive member may not have presence yet.
                    self.participant_roster = None;
                    self.renew_due = None;
                    self.ok(vec![BootstrapEffect::WithdrawClaim(old)])
                } else {
                    self.terminate()
                }
            }
            BootstrapEvent::DonorPublicationFailed { selected } => {
                if self.stage != BootstrapStage::DonorAvailable
                    || self.selected.as_ref() != Some(&selected)
                    || selected != self.identity()
                {
                    Self::reject(BootstrapError::StaleOperation)
                } else {
                    self.terminate()
                }
            }
            BootstrapEvent::Cancel => self.cancel(),
            BootstrapEvent::RetireCandidate => self.retire_candidate(),
        }
    }
}

#[cfg(test)]
mod renewal_tests;
