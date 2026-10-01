//! Sans-IO decisions for a bounded volatile-feed recovery turn.

use std::collections::{BTreeMap, BTreeSet};

use crate::volatile_bootstrap::BootstrapMemberIdentity;
use crate::{NodeId, Time};

use super::types::{
    Mark, Peer, RecoveryConfig, RecoveryEffect, RecoveryError, RecoveryEvent, RecoveryFallback,
    RecoveryMode, RecoveryOperation, RecoveryRearm, RecoveryStage, RecoveryState, RecoveryStep,
};

mod evidence;
mod fallback;
mod peer;

use evidence::{crossed, regressed};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Plan {
    Full,
    Lapse,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Baseline {
    Origin,
    Peer,
}

/// One bounded recovery turn. The shell synchronously intersects `recovered`
/// with its lease and application gates before returning from a public signal.
#[derive(Clone, Debug)]
pub struct RecoveryEngine {
    config: RecoveryConfig,
    mode: RecoveryMode,
    me: NodeId,
    session: u64,
    next_token: u64,
    now: Time,
    state: RecoveryState,
    plan: Plan,
    baseline: Baseline,
    bootstrap: bool,
    baseline_op: Option<RecoveryOperation>,
    suspended_local: Option<RecoveryOperation>,
    operation: Option<RecoveryOperation>,
    operation_due: Option<Time>,
    wait_due: Option<Time>,
    total_due: Option<Time>,
    rearm: Option<RecoveryRearm>,
    rearm_due: Option<Time>,
    next_rearm_ms: u64,
    rearm_exhausted: bool,
    stepped: bool,
    seen: BTreeSet<NodeId>,
    exempt: BTreeSet<NodeId>,
    grants: BTreeMap<NodeId, Option<Mark>>,
    confirmed_before: Option<Mark>,
    known_heads: BTreeMap<NodeId, Mark>,
    seals: BTreeMap<NodeId, Mark>,
    heads: BTreeMap<NodeId, Mark>,
    barrier_rounds: u32,
    peer_members: Vec<BootstrapMemberIdentity>,
}

impl RecoveryEngine {
    /// Creates a closed recovery view with a unique local session incarnation.
    ///
    /// # Errors
    /// Rejects invalid finite bounds, empty/oversized local identity, or zero
    /// session incarnation before any effect is emitted.
    pub fn new(
        config: RecoveryConfig,
        mode: RecoveryMode,
        me: NodeId,
        session: u64,
    ) -> Result<Self, RecoveryError> {
        let config = config.validate()?;
        if session == 0 || me.as_str().is_empty() || me.as_str().len() > config.max_member_bytes {
            return Err(RecoveryError::InvalidConfig);
        }
        Ok(Self {
            config,
            mode,
            me,
            session,
            next_token: 1,
            now: Time::ZERO,
            state: RecoveryState {
                generation: 0,
                stage: RecoveryStage::Unready,
                recovered: false,
                covered_lapses: 0,
            },
            plan: Plan::Full,
            baseline: Baseline::Origin,
            bootstrap: false,
            baseline_op: None,
            suspended_local: None,
            operation: None,
            operation_due: None,
            wait_due: None,
            total_due: None,
            rearm: None,
            rearm_due: None,
            next_rearm_ms: 0,
            rearm_exhausted: false,
            stepped: false,
            seen: BTreeSet::new(),
            exempt: BTreeSet::new(),
            grants: BTreeMap::new(),
            confirmed_before: None,
            known_heads: BTreeMap::new(),
            seals: BTreeMap::new(),
            heads: BTreeMap::new(),
            barrier_rounds: 0,
            peer_members: Vec::new(),
        })
    }

    /// Enables capped automatic full recovery after an exhausted episode.
    /// This may be selected only before the first event.
    ///
    /// # Errors
    /// Rejects an invalid policy or an engine already used for recovery.
    pub fn with_rearm(mut self, policy: RecoveryRearm) -> Result<Self, RecoveryError> {
        let policy = policy.validate()?;
        if self.stepped {
            return Err(RecoveryError::Stage);
        }
        self.rearm = Some(policy);
        self.next_rearm_ms = policy.initial_ms;
        Ok(self)
    }

    /// Enables an opt-in, authority-free peer baseline attempt before the
    /// guarded origin fallback. It must be selected before the first event.
    ///
    /// # Errors
    /// Rejects an engine already used for recovery.
    pub fn with_bootstrap(mut self) -> Result<Self, RecoveryError> {
        if self.stepped {
            return Err(RecoveryError::Stage);
        }
        self.bootstrap = true;
        Ok(self)
    }

    /// Current local recovery permission, which cannot override lease or app policy.
    #[must_use]
    pub fn state(&self) -> RecoveryState {
        self.state
    }

    /// Exact operation the shell may still execute or report.
    #[must_use]
    pub fn accepts_operation(&self, op: RecoveryOperation) -> bool {
        self.operation == Some(op)
            && op.session == self.session
            && op.generation == self.state.generation
            && self.operation_due.is_some_and(|due| self.now < due)
    }

    /// Earliest finite timer across current I/O, settle/poll wait, and total budget.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Time> {
        [
            self.operation_due,
            self.wait_due,
            self.total_due,
            self.rearm_due,
        ]
        .into_iter()
        .flatten()
        .min()
    }

    fn step_ok(effects: Vec<RecoveryEffect>) -> RecoveryStep {
        RecoveryStep {
            effects,
            rejection: None,
        }
    }

    fn reject(reason: RecoveryError) -> RecoveryStep {
        RecoveryStep {
            effects: Vec::new(),
            rejection: Some(reason),
        }
    }

    fn clear_work(&mut self) {
        self.operation = None;
        self.operation_due = None;
        self.wait_due = None;
        self.total_due = None;
        self.rearm_due = None;
        self.seen.clear();
        self.exempt.clear();
        self.grants.clear();
        self.confirmed_before = None;
        self.known_heads.clear();
        self.seals.clear();
        self.heads.clear();
        self.barrier_rounds = 0;
        self.peer_members.clear();
    }

    fn cancel_baseline(&mut self) -> Vec<RecoveryEffect> {
        self.baseline_op
            .take()
            .into_iter()
            .chain(self.suspended_local.take())
            .map(|op| RecoveryEffect::CancelBaseline { op })
            .collect()
    }

    fn issue(&mut self, stage: RecoveryStage) -> Result<RecoveryOperation, RecoveryError> {
        let total_due = self.total_due.ok_or(RecoveryError::Stage)?;
        // The baseline child has its own finite source-operation deadlines.
        // Its parent spans the original recovery episode; charging it to one
        // ordinary adapter attempt would abort a healthy multi-step transfer.
        let operation_due = if stage == RecoveryStage::AcquiringBaseline {
            total_due
        } else {
            Time(
                self.now
                    .0
                    .checked_add(self.config.attempt_ms)
                    .ok_or(RecoveryError::Exhausted)?,
            )
            .min(total_due)
        };
        if self.now >= operation_due || self.next_token == 0 {
            return Err(RecoveryError::Exhausted);
        }
        let op = RecoveryOperation {
            session: self.session,
            generation: self.state.generation,
            token: self.next_token,
        };
        self.next_token = self.next_token.checked_add(1).unwrap_or(0);
        self.state.stage = stage;
        self.operation = Some(op);
        self.operation_due = Some(operation_due);
        self.wait_due = None;
        Ok(op)
    }

    fn with_timer(&self, mut effects: Vec<RecoveryEffect>) -> RecoveryStep {
        if let Some(due) = self.next_deadline() {
            effects.push(RecoveryEffect::ArmTimer(due));
        }
        Self::step_ok(effects)
    }

    fn begin(&mut self, plan: Plan, lapses: u64) -> RecoveryStep {
        if self.state.stage == RecoveryStage::Cancelled {
            return Self::reject(RecoveryError::Stage);
        }
        if self.rearm_exhausted {
            return Self::reject(RecoveryError::Exhausted);
        }
        // A lapse keeps a local image the child still holds, whether an origin
        // build or an adopted peer install, for one recapture once affirmed.
        let cancel = if plan == Plan::Lapse {
            if let Some(op) = self.baseline_op.take() {
                self.suspended_local = Some(op);
                vec![RecoveryEffect::SuspendLocalBaseline { op }]
            } else {
                self.cancel_baseline()
            }
        } else {
            self.cancel_baseline()
        };
        self.clear_work();
        self.state.recovered = false;
        self.state.covered_lapses = self.state.covered_lapses.max(lapses);
        let Some(next) = self.state.generation.checked_add(1) else {
            let cancel = self
                .suspended_local
                .take()
                .map_or(cancel, |op| vec![RecoveryEffect::CancelBaseline { op }]);
            self.state.stage = RecoveryStage::OriginOnly;
            self.rearm_exhausted = true;
            let mut effects = vec![RecoveryEffect::CloseGate {
                generation: self.state.generation,
            }];
            effects.extend(cancel);
            return RecoveryStep {
                effects,
                rejection: Some(RecoveryError::Exhausted),
            };
        };
        self.state.generation = next;
        self.plan = plan;
        self.baseline = Baseline::Origin;
        let Some(due) = self.now.0.checked_add(self.config.total_ms).map(Time) else {
            let cancel = self
                .suspended_local
                .take()
                .map_or(cancel, |op| vec![RecoveryEffect::CancelBaseline { op }]);
            self.state.stage = RecoveryStage::OriginOnly;
            self.rearm_exhausted = true;
            let mut effects = vec![RecoveryEffect::CloseGate { generation: next }];
            effects.extend(cancel);
            return RecoveryStep {
                effects,
                rejection: Some(RecoveryError::Exhausted),
            };
        };
        self.total_due = Some(due);
        let Ok(op) = self.issue(RecoveryStage::Invalidating) else {
            let cancel = self
                .suspended_local
                .take()
                .map_or(cancel, |op| vec![RecoveryEffect::CancelBaseline { op }]);
            self.clear_work();
            self.state.stage = RecoveryStage::OriginOnly;
            self.rearm_exhausted = true;
            let mut effects = vec![RecoveryEffect::CloseGate { generation: next }];
            effects.extend(cancel);
            return RecoveryStep {
                effects,
                rejection: Some(RecoveryError::Exhausted),
            };
        };
        let mut effects = vec![RecoveryEffect::CloseGate { generation: next }];
        effects.extend(cancel);
        effects.push(RecoveryEffect::Invalidate {
            op,
            distrust_bodies: plan == Plan::Full,
        });
        self.with_timer(effects)
    }

    fn expected(&self, op: RecoveryOperation, stage: RecoveryStage) -> bool {
        self.state.stage == stage && self.accepts_operation(op)
    }

    fn observe_peers(&mut self, stage: RecoveryStage) -> RecoveryStep {
        let Ok(op) = self.issue(stage) else {
            return self.fallback_or_origin(RecoveryFallback::Exhausted);
        };
        let effect = if matches!(
            stage,
            RecoveryStage::PeerSamplingHeads | RecoveryStage::PeerRecheckingHeads
        ) {
            RecoveryEffect::ObservePeerHeads { op }
        } else {
            RecoveryEffect::ObservePeers { op }
        };
        self.with_timer(vec![effect])
    }

    fn acquire_baseline(&mut self) -> RecoveryStep {
        let Ok(op) = self.issue(RecoveryStage::AcquiringBaseline) else {
            return self.origin_only(RecoveryFallback::Exhausted);
        };
        self.baseline_op = Some(op);
        self.with_timer(vec![RecoveryEffect::AcquireBaseline { op }])
    }

    /// A long-running operation that keeps committing work stays alive: its
    /// stall bound and the episode budget both restart now. Neither deadline
    /// can move earlier, and a fenced or expired operation is refused.
    fn progressed(&mut self, op: RecoveryOperation) -> RecoveryStep {
        if !self.accepts_operation(op) {
            return Self::reject(RecoveryError::StaleOperation);
        }
        let baseline = self.state.stage == RecoveryStage::AcquiringBaseline;
        if !baseline && self.state.stage != RecoveryStage::Rebuilding {
            return Self::reject(RecoveryError::Stage);
        }
        let (Some(total_due), Some(attempt_due)) = (
            self.now.0.checked_add(self.config.total_ms).map(Time),
            self.now.0.checked_add(self.config.attempt_ms).map(Time),
        ) else {
            return Self::reject(RecoveryError::Exhausted);
        };
        self.total_due = Some(total_due);
        self.operation_due = Some(if baseline {
            total_due
        } else {
            attempt_due.min(total_due)
        });
        self.with_timer(Vec::new())
    }

    fn wait_for(&mut self, stage: RecoveryStage, delay: u64) -> RecoveryStep {
        let Some(total_due) = self.total_due else {
            return self.origin_only(RecoveryFallback::Exhausted);
        };
        let Some(due) = self.now.0.checked_add(delay).map(Time) else {
            return self.fallback_or_origin(RecoveryFallback::Exhausted);
        };
        self.state.stage = stage;
        self.operation = None;
        self.operation_due = None;
        self.wait_due = Some(due.min(total_due));
        self.with_timer(Vec::new())
    }

    fn validate_peers(&self, peers: &[Peer]) -> Result<(), RecoveryError> {
        if peers.len() > self.config.max_members {
            return Err(RecoveryError::Capacity);
        }
        let mut unique = BTreeSet::new();
        for peer in peers {
            if peer.node == self.me
                || peer.node.as_str().is_empty()
                || peer.node.as_str().len() > self.config.max_member_bytes
                || peer.grant.is_some_and(|mark| mark.sequence == 0)
                || peer.head.is_some_and(|mark| mark.sequence == 0)
                || peer.renewal.is_some_and(|renewal| {
                    renewal.sealed.sequence == 0 || renewal.epoch <= renewal.sealed.epoch
                })
                || peer.sealed.is_some_and(|sealed| sealed.sequence == 0)
                || !unique.insert(&peer.node)
            {
                return Err(RecoveryError::InvalidEvidence);
            }
        }
        Ok(())
    }

    fn initial_peers(&mut self, peers: &[Peer], confirmed: Option<Mark>) -> RecoveryStep {
        self.seen.clear();
        self.exempt.clear();
        self.grants.clear();
        for peer in peers {
            if peer.old_nonlive && !peer.alive {
                self.exempt.insert(peer.node.clone());
            } else {
                self.seen.insert(peer.node.clone());
                if peer.grants_lease {
                    self.grants.insert(peer.node.clone(), peer.grant);
                }
            }
        }
        self.confirmed_before = confirmed;
        self.observe_peers(RecoveryStage::WaitingRenewals)
    }

    fn renewed_peers(&mut self, peers: &[Peer], confirmed: Option<Mark>) -> RecoveryStep {
        let present: BTreeMap<&NodeId, &Peer> =
            peers.iter().map(|peer| (&peer.node, peer)).collect();
        for peer in peers {
            if peer.alive {
                self.exempt.remove(&peer.node);
            }
            if !self.exempt.contains(&peer.node) {
                self.seen.insert(peer.node.clone());
            }
            if peer.grants_lease
                && !self.exempt.contains(&peer.node)
                && !self.grants.contains_key(&peer.node)
            {
                self.grants.insert(peer.node.clone(), peer.grant);
            }
        }
        if self.seen.len().saturating_add(self.exempt.len()) > self.config.max_members
            || self.grants.len() > self.config.max_members
        {
            return self.fallback_or_origin(RecoveryFallback::MembershipChanged);
        }
        let all_advanced = self.grants.iter().all(|(node, before)| {
            present.get(node).is_none_or(|peer| !peer.grants_lease)
                || present.get(node).is_some_and(|peer| {
                    peer.grant
                        .is_some_and(|now| before.is_none_or(|before| now > before))
                })
        });
        let confirmed_advanced =
            confirmed.is_some_and(|now| self.confirmed_before.is_none_or(|before| now > before));
        if all_advanced && confirmed_advanced {
            self.wait_for(RecoveryStage::Settling, self.config.settle_ms)
        } else {
            self.wait_for(RecoveryStage::WaitingRenewals, self.config.poll_ms)
        }
    }

    fn observed_heads(&mut self, peers: &[Peer], recheck: bool) -> RecoveryStep {
        for peer in peers {
            if peer.alive {
                self.exempt.remove(&peer.node);
            }
            if !self.exempt.contains(&peer.node) {
                self.seen.insert(peer.node.clone());
            }
        }
        if self.seen.len().saturating_add(self.exempt.len()) > self.config.max_members {
            return self.fallback_or_origin(RecoveryFallback::MembershipChanged);
        }
        let present: BTreeSet<&NodeId> = peers.iter().map(|peer| &peer.node).collect();
        // A writer that left after the observer delivered its seal took no
        // unapplied write with it; any other vanished writer may have.
        if self
            .seen
            .iter()
            .any(|node| !present.contains(node) && !self.departed_sealed(node))
        {
            return self.fallback_or_origin(RecoveryFallback::MembershipChanged);
        }
        let mut heads = BTreeMap::new();
        for peer in peers {
            if self.exempt.contains(&peer.node) {
                continue;
            }
            if let Some(head) = peer.head {
                heads.insert(peer.node.clone(), head);
            }
        }
        if recheck
            && peers.iter().any(|peer| {
                self.heads.get(&peer.node).is_some_and(|before| {
                    heads
                        .get(&peer.node)
                        .is_some_and(|head| regressed(*before, *head))
                        && !crossed(*before, peer)
                })
            })
        {
            return self.fallback_or_origin(RecoveryFallback::EvidenceRejected);
        }
        if recheck && heads == self.heads {
            return self.affirm();
        }
        if recheck {
            self.barrier_rounds += 1;
            if self.barrier_rounds >= self.config.max_barrier_rounds {
                return self.fallback_or_origin(RecoveryFallback::BarrierExhausted);
            }
        }
        self.heads = heads;
        let waiting = if self.baseline == Baseline::Peer {
            RecoveryStage::PeerWaitingFrontiers
        } else {
            RecoveryStage::WaitingFrontiers
        };
        let Ok(op) = self.issue(waiting) else {
            return self.fallback_or_origin(RecoveryFallback::Exhausted);
        };
        self.with_timer(vec![RecoveryEffect::WaitFrontiers {
            op,
            heads: self
                .heads
                .iter()
                .map(|(node, head)| (node.clone(), *head))
                .collect(),
        }])
    }

    fn affirm(&mut self) -> RecoveryStep {
        let Ok(op) = self.issue(RecoveryStage::Affirming) else {
            return self.origin_only(RecoveryFallback::Exhausted);
        };
        self.with_timer(vec![RecoveryEffect::Affirm { op }])
    }

    /// Consumes one explicit event and returns bounded, correlated work.
    #[expect(
        clippy::too_many_lines,
        reason = "one auditable sans-IO transition table; bounded proof handlers are split into helpers"
    )]
    pub fn step(&mut self, event: RecoveryEvent) -> RecoveryStep {
        self.stepped = true;
        match event {
            RecoveryEvent::Start => self.begin(Plan::Full, self.state.covered_lapses),
            RecoveryEvent::StartWithLapses { lapses } => self.begin(Plan::Full, lapses),
            RecoveryEvent::FeedGap { lapses } => {
                if self.state.stage == RecoveryStage::Cancelled {
                    return Self::reject(RecoveryError::Stage);
                }
                if self.state.stage == RecoveryStage::OriginOnly && self.rearm_due.is_some() {
                    self.state.covered_lapses = self.state.covered_lapses.max(lapses);
                    return self.with_timer(Vec::new());
                }
                self.begin(Plan::Full, lapses)
            }
            RecoveryEvent::LeaseLapse { count } => {
                if self.state.stage == RecoveryStage::Cancelled {
                    return Self::reject(RecoveryError::Stage);
                }
                if self.mode != RecoveryMode::Leased {
                    return Self::reject(RecoveryError::Stage);
                }
                if count <= self.state.covered_lapses {
                    return Self::step_ok(Vec::new());
                }
                if self.state.stage == RecoveryStage::OriginOnly && self.rearm_due.is_some() {
                    self.state.covered_lapses = count;
                    return self.with_timer(Vec::new());
                }
                if self.plan == Plan::Full
                    && !matches!(
                        self.state.stage,
                        RecoveryStage::Unready | RecoveryStage::OriginOnly | RecoveryStage::Ready
                    )
                {
                    return self.begin(Plan::Full, count);
                }
                if self.state.stage == RecoveryStage::Ready && self.state.recovered {
                    self.begin(Plan::Lapse, count)
                } else {
                    self.begin(Plan::Full, count)
                }
            }
            RecoveryEvent::Invalidated { op } => {
                if !self.expected(op, RecoveryStage::Invalidating) {
                    return Self::reject(RecoveryError::StaleOperation);
                }
                self.operation = None;
                self.operation_due = None;
                if self.plan == Plan::Lapse {
                    self.observe_peers(RecoveryStage::SamplingInitial)
                } else if self.bootstrap {
                    self.acquire_baseline()
                } else {
                    self.rebuild_origin()
                }
            }
            RecoveryEvent::Materialized { op } => {
                if !self.expected(op, RecoveryStage::Rebuilding) {
                    return Self::reject(RecoveryError::StaleOperation);
                }
                self.operation = None;
                self.operation_due = None;
                self.affirm()
            }
            RecoveryEvent::BootstrapDeclined { op } => {
                if !self.expected(op, RecoveryStage::AcquiringBaseline) {
                    return Self::reject(RecoveryError::StaleOperation);
                }
                self.recover_origin(RecoveryFallback::BaselineDeclined)
            }
            RecoveryEvent::LocalBaselineBuilt { op } => {
                if !self.expected(op, RecoveryStage::AcquiringBaseline) {
                    return Self::reject(RecoveryError::StaleOperation);
                }
                self.baseline = Baseline::Origin;
                self.affirm()
            }
            RecoveryEvent::Progressed { op } => self.progressed(op),
            RecoveryEvent::PeerBaselineInstalled { op, handoff } => {
                if !self.expected(op, RecoveryStage::AcquiringBaseline) {
                    return Self::reject(RecoveryError::StaleOperation);
                }
                if !self.valid_handoff(op, &handoff) {
                    return self.recover_origin(RecoveryFallback::HandoffRejected);
                }
                self.baseline = Baseline::Peer;
                self.peer_members = handoff.coverage.members;
                let mut cancel = self.cancel_baseline();
                let mut observed = self.observe_peers(RecoveryStage::PeerSamplingHeads);
                cancel.append(&mut observed.effects);
                observed.effects = cancel;
                observed
            }
            RecoveryEvent::PeersObserved {
                op,
                peers,
                confirmed,
            } => {
                if !self.accepts_operation(op) {
                    return Self::reject(RecoveryError::StaleOperation);
                }
                if !matches!(
                    self.state.stage,
                    RecoveryStage::SamplingInitial
                        | RecoveryStage::WaitingRenewals
                        | RecoveryStage::SamplingHeads
                        | RecoveryStage::RecheckingHeads
                ) {
                    return Self::reject(RecoveryError::Stage);
                }
                if let Err(error) = self.validate_peers(&peers) {
                    return Self::reject(error);
                }
                if confirmed.is_some_and(|mark| mark.sequence == 0) {
                    return Self::reject(RecoveryError::InvalidEvidence);
                }
                if self.record_known_heads(&peers).is_err() {
                    return self.fallback_or_origin(RecoveryFallback::EvidenceRejected);
                }
                self.operation = None;
                self.operation_due = None;
                match self.state.stage {
                    RecoveryStage::SamplingInitial => self.initial_peers(&peers, confirmed),
                    RecoveryStage::WaitingRenewals => self.renewed_peers(&peers, confirmed),
                    RecoveryStage::SamplingHeads => self.observed_heads(&peers, false),
                    RecoveryStage::RecheckingHeads => self.observed_heads(&peers, true),
                    _ => Self::reject(RecoveryError::Stage),
                }
            }
            RecoveryEvent::PeerHeadsObserved {
                op,
                peers,
                identities,
            } => {
                if !self.accepts_operation(op)
                    || !matches!(
                        self.state.stage,
                        RecoveryStage::PeerSamplingHeads | RecoveryStage::PeerRecheckingHeads
                    )
                {
                    return Self::reject(RecoveryError::StaleOperation);
                }
                if let Err(error) = self.validate_peers(&peers) {
                    return Self::reject(error);
                }
                if !self.valid_peer_roster(&peers, &identities)
                    || self.record_known_heads(&peers).is_err()
                {
                    return self.recover_origin(RecoveryFallback::EvidenceRejected);
                }
                self.operation = None;
                self.operation_due = None;
                let recheck = self.state.stage == RecoveryStage::PeerRecheckingHeads;
                self.observed_heads(&peers, recheck)
            }
            RecoveryEvent::FrontiersReached { op } => {
                if !self.expected(op, RecoveryStage::WaitingFrontiers)
                    && !self.expected(op, RecoveryStage::PeerWaitingFrontiers)
                {
                    return Self::reject(RecoveryError::StaleOperation);
                }
                self.operation = None;
                self.operation_due = None;
                let rechecking = if self.baseline == Baseline::Peer {
                    RecoveryStage::PeerRecheckingHeads
                } else {
                    RecoveryStage::RecheckingHeads
                };
                self.observe_peers(rechecking)
            }
            RecoveryEvent::Affirmed { op, accepted } => {
                if !self.expected(op, RecoveryStage::Affirming) {
                    return Self::reject(RecoveryError::StaleOperation);
                }
                self.operation = None;
                self.operation_due = None;
                if accepted {
                    self.state.stage = RecoveryStage::Ready;
                    self.state.recovered = true;
                    self.total_due = None;
                    self.rearm_due = None;
                    if let Some(policy) = self.rearm {
                        self.next_rearm_ms = policy.initial_ms;
                    }
                    if let Some(previous) = self.suspended_local.take() {
                        if self.next_token == 0 {
                            return Self::step_ok(vec![RecoveryEffect::CancelBaseline {
                                op: previous,
                            }]);
                        }
                        let current = RecoveryOperation {
                            session: self.session,
                            generation: self.state.generation,
                            token: self.next_token,
                        };
                        self.next_token = self.next_token.checked_add(1).unwrap_or(0);
                        self.baseline_op = Some(current);
                        Self::step_ok(vec![RecoveryEffect::ResumeLocalBaseline {
                            previous,
                            current,
                        }])
                    } else if self.baseline == Baseline::Peer && self.baseline_op.is_none() {
                        if self.next_token == 0 {
                            return Self::step_ok(Vec::new());
                        }
                        let op = RecoveryOperation {
                            session: self.session,
                            generation: self.state.generation,
                            token: self.next_token,
                        };
                        self.next_token = self.next_token.checked_add(1).unwrap_or(0);
                        self.baseline_op = Some(op);
                        Self::step_ok(vec![RecoveryEffect::AdoptLocalBaseline { op }])
                    } else {
                        Self::step_ok(Vec::new())
                    }
                } else {
                    self.wait_for(RecoveryStage::Affirming, self.config.poll_ms)
                }
            }
            RecoveryEvent::Failed { op } => {
                if !self.accepts_operation(op) {
                    return Self::reject(RecoveryError::StaleOperation);
                }
                if self.state.stage == RecoveryStage::AcquiringBaseline
                    || self.baseline == Baseline::Peer
                {
                    self.recover_origin(RecoveryFallback::OperationFailed)
                } else if self.plan == Plan::Full {
                    self.retry_full(RecoveryFallback::OperationFailed)
                } else {
                    self.fallback(RecoveryFallback::OperationFailed)
                }
            }
            RecoveryEvent::Tick(now) => {
                if now < self.now {
                    return Self::reject(RecoveryError::BackwardTime);
                }
                self.now = now;
                if self.total_due.is_some_and(|due| now >= due) {
                    return self.fallback_or_origin(RecoveryFallback::EpisodeExpired);
                }
                if self.operation_due.is_some_and(|due| now >= due) {
                    return if self.state.stage == RecoveryStage::AcquiringBaseline
                        || self.baseline == Baseline::Peer
                    {
                        self.recover_origin(RecoveryFallback::OperationExpired)
                    } else if self.plan == Plan::Full {
                        self.retry_full(RecoveryFallback::OperationExpired)
                    } else {
                        self.fallback(RecoveryFallback::OperationExpired)
                    };
                }
                if self.wait_due.is_some_and(|due| now >= due) {
                    self.wait_due = None;
                    return match self.state.stage {
                        RecoveryStage::Settling => self.observe_peers(RecoveryStage::SamplingHeads),
                        RecoveryStage::WaitingRenewals => {
                            self.observe_peers(RecoveryStage::WaitingRenewals)
                        }
                        RecoveryStage::Affirming => self.affirm(),
                        RecoveryStage::Invalidating if self.plan == Plan::Full => {
                            self.retry_invalidation()
                        }
                        RecoveryStage::Rebuilding if self.plan == Plan::Full => {
                            self.retry_rebuild()
                        }
                        _ => Self::reject(RecoveryError::Stage),
                    };
                }
                if self.rearm_due.is_some_and(|due| now >= due) {
                    return self.begin(Plan::Full, self.state.covered_lapses);
                }
                Self::step_ok(Vec::new())
            }
            RecoveryEvent::Cancel => {
                let cancel = self.cancel_baseline();
                self.clear_work();
                self.state.recovered = false;
                self.state.stage = RecoveryStage::Cancelled;
                let Some(next) = self.state.generation.checked_add(1) else {
                    let mut effects = vec![RecoveryEffect::CloseGate {
                        generation: self.state.generation,
                    }];
                    effects.extend(cancel);
                    return RecoveryStep {
                        effects,
                        rejection: Some(RecoveryError::Exhausted),
                    };
                };
                self.state.generation = next;
                let mut effects = vec![RecoveryEffect::CloseGate { generation: next }];
                effects.extend(cancel);
                Self::step_ok(effects)
            }
        }
    }
}
