//! Sans-IO decisions for a bounded volatile-feed recovery turn.

use std::collections::{BTreeMap, BTreeSet};

use crate::{NodeId, Time};

use super::types::{
    Mark, Peer, RecoveryConfig, RecoveryEffect, RecoveryError, RecoveryEvent, RecoveryMode,
    RecoveryOperation, RecoveryStage, RecoveryState, RecoveryStep,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Plan {
    Full,
    Lapse,
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
    operation: Option<RecoveryOperation>,
    operation_due: Option<Time>,
    wait_due: Option<Time>,
    total_due: Option<Time>,
    seen: BTreeSet<NodeId>,
    exempt: BTreeSet<NodeId>,
    grants: BTreeMap<NodeId, Option<Mark>>,
    confirmed_before: Option<Mark>,
    known_heads: BTreeMap<NodeId, Mark>,
    heads: BTreeMap<NodeId, Mark>,
    barrier_rounds: u32,
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
            operation: None,
            operation_due: None,
            wait_due: None,
            total_due: None,
            seen: BTreeSet::new(),
            exempt: BTreeSet::new(),
            grants: BTreeMap::new(),
            confirmed_before: None,
            known_heads: BTreeMap::new(),
            heads: BTreeMap::new(),
            barrier_rounds: 0,
        })
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
        [self.operation_due, self.wait_due, self.total_due]
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
        self.seen.clear();
        self.exempt.clear();
        self.grants.clear();
        self.confirmed_before = None;
        self.known_heads.clear();
        self.heads.clear();
        self.barrier_rounds = 0;
    }

    fn origin_only(&mut self) -> RecoveryStep {
        self.clear_work();
        self.state.stage = RecoveryStage::OriginOnly;
        self.state.recovered = false;
        Self::step_ok(Vec::new())
    }

    fn issue(&mut self, stage: RecoveryStage) -> Result<RecoveryOperation, RecoveryError> {
        let total_due = self.total_due.ok_or(RecoveryError::Stage)?;
        let operation_due = Time(
            self.now
                .0
                .checked_add(self.config.attempt_ms)
                .ok_or(RecoveryError::Exhausted)?,
        )
        .min(total_due);
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
        self.clear_work();
        self.state.recovered = false;
        self.state.covered_lapses = self.state.covered_lapses.max(lapses);
        let Some(next) = self.state.generation.checked_add(1) else {
            self.state.stage = RecoveryStage::OriginOnly;
            return RecoveryStep {
                effects: vec![RecoveryEffect::CloseGate {
                    generation: self.state.generation,
                }],
                rejection: Some(RecoveryError::Exhausted),
            };
        };
        self.state.generation = next;
        self.plan = plan;
        let Some(due) = self.now.0.checked_add(self.config.total_ms).map(Time) else {
            self.state.stage = RecoveryStage::OriginOnly;
            return RecoveryStep {
                effects: vec![RecoveryEffect::CloseGate { generation: next }],
                rejection: Some(RecoveryError::Exhausted),
            };
        };
        self.total_due = Some(due);
        let Ok(op) = self.issue(RecoveryStage::Invalidating) else {
            self.origin_only();
            return RecoveryStep {
                effects: vec![RecoveryEffect::CloseGate { generation: next }],
                rejection: Some(RecoveryError::Exhausted),
            };
        };
        self.with_timer(vec![
            RecoveryEffect::CloseGate { generation: next },
            RecoveryEffect::Invalidate {
                op,
                distrust_bodies: plan == Plan::Full,
            },
        ])
    }

    fn fallback(&mut self) -> RecoveryStep {
        let lapses = self.state.covered_lapses;
        self.begin(Plan::Full, lapses)
    }

    fn expected(&self, op: RecoveryOperation, stage: RecoveryStage) -> bool {
        self.state.stage == stage && self.accepts_operation(op)
    }

    fn observe_peers(&mut self, stage: RecoveryStage) -> RecoveryStep {
        let Ok(op) = self.issue(stage) else {
            return self.fallback_or_origin();
        };
        self.with_timer(vec![RecoveryEffect::ObservePeers { op }])
    }

    fn fallback_or_origin(&mut self) -> RecoveryStep {
        if self.plan == Plan::Lapse {
            self.fallback()
        } else {
            self.origin_only()
        }
    }

    fn retry_full(&mut self) -> RecoveryStep {
        match self.state.stage {
            RecoveryStage::Invalidating | RecoveryStage::Rebuilding | RecoveryStage::Affirming => {
                self.wait_for(self.state.stage, self.config.poll_ms)
            }
            _ => self.origin_only(),
        }
    }

    fn retry_invalidation(&mut self) -> RecoveryStep {
        let Ok(op) = self.issue(RecoveryStage::Invalidating) else {
            return self.origin_only();
        };
        self.with_timer(vec![RecoveryEffect::Invalidate {
            op,
            distrust_bodies: true,
        }])
    }

    fn retry_rebuild(&mut self) -> RecoveryStep {
        let Ok(op) = self.issue(RecoveryStage::Rebuilding) else {
            return self.origin_only();
        };
        self.with_timer(vec![RecoveryEffect::RebuildOrigin { op }])
    }

    fn wait_for(&mut self, stage: RecoveryStage, delay: u64) -> RecoveryStep {
        let Some(total_due) = self.total_due else {
            return self.origin_only();
        };
        let Some(due) = self.now.0.checked_add(delay).map(Time) else {
            return self.fallback_or_origin();
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
                || !unique.insert(&peer.node)
            {
                return Err(RecoveryError::InvalidEvidence);
            }
        }
        Ok(())
    }

    fn record_known_heads(&mut self, peers: &[Peer]) -> Result<(), RecoveryError> {
        for peer in peers {
            match (self.known_heads.get(&peer.node), peer.head) {
                (Some(_), None) => return Err(RecoveryError::InvalidEvidence),
                (Some(before), Some(now))
                    if now.epoch != before.epoch || now.sequence < before.sequence =>
                {
                    return Err(RecoveryError::InvalidEvidence);
                }
                (_, Some(now)) => {
                    self.known_heads.insert(peer.node.clone(), now);
                }
                (None, None) => {}
            }
        }
        if self.known_heads.len() > self.config.max_members {
            return Err(RecoveryError::Capacity);
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
            return self.fallback();
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
            return self.fallback();
        }
        let present: BTreeSet<&NodeId> = peers.iter().map(|peer| &peer.node).collect();
        if self.seen.iter().any(|node| !present.contains(node)) {
            return self.fallback();
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
            && heads.iter().any(|(node, head)| {
                self.heads.get(node).is_some_and(|before| {
                    head.epoch != before.epoch || head.sequence < before.sequence
                })
            })
        {
            return self.fallback();
        }
        if recheck && heads == self.heads {
            return self.affirm();
        }
        if recheck {
            self.barrier_rounds += 1;
            if self.barrier_rounds >= self.config.max_barrier_rounds {
                return self.fallback();
            }
        }
        self.heads = heads;
        let Ok(op) = self.issue(RecoveryStage::WaitingFrontiers) else {
            return self.fallback();
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
            return self.origin_only();
        };
        self.with_timer(vec![RecoveryEffect::Affirm { op }])
    }

    /// Consumes one explicit event and returns bounded, correlated work.
    #[expect(
        clippy::too_many_lines,
        reason = "one auditable sans-IO transition table; bounded proof handlers are split into helpers"
    )]
    pub fn step(&mut self, event: RecoveryEvent) -> RecoveryStep {
        match event {
            RecoveryEvent::Start => self.begin(Plan::Full, self.state.covered_lapses),
            RecoveryEvent::FeedGap { lapses } => {
                if self.state.stage == RecoveryStage::Cancelled {
                    return Self::reject(RecoveryError::Stage);
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
                } else {
                    let Ok(op) = self.issue(RecoveryStage::Rebuilding) else {
                        return self.origin_only();
                    };
                    self.with_timer(vec![RecoveryEffect::RebuildOrigin { op }])
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
                    return self.fallback_or_origin();
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
            RecoveryEvent::FrontiersReached { op } => {
                if !self.expected(op, RecoveryStage::WaitingFrontiers) {
                    return Self::reject(RecoveryError::StaleOperation);
                }
                self.operation = None;
                self.operation_due = None;
                self.observe_peers(RecoveryStage::RecheckingHeads)
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
                    Self::step_ok(Vec::new())
                } else {
                    self.wait_for(RecoveryStage::Affirming, self.config.poll_ms)
                }
            }
            RecoveryEvent::Failed { op } => {
                if !self.accepts_operation(op) {
                    return Self::reject(RecoveryError::StaleOperation);
                }
                if self.plan == Plan::Full {
                    self.retry_full()
                } else {
                    self.fallback()
                }
            }
            RecoveryEvent::Tick(now) => {
                if now < self.now {
                    return Self::reject(RecoveryError::BackwardTime);
                }
                self.now = now;
                if self.total_due.is_some_and(|due| now >= due) {
                    return self.fallback_or_origin();
                }
                if self.operation_due.is_some_and(|due| now >= due) {
                    return if self.plan == Plan::Full {
                        self.retry_full()
                    } else {
                        self.fallback()
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
                Self::step_ok(Vec::new())
            }
            RecoveryEvent::Cancel => {
                self.clear_work();
                self.state.recovered = false;
                self.state.stage = RecoveryStage::Cancelled;
                let Some(next) = self.state.generation.checked_add(1) else {
                    return RecoveryStep {
                        effects: vec![RecoveryEffect::CloseGate {
                            generation: self.state.generation,
                        }],
                        rejection: Some(RecoveryError::Exhausted),
                    };
                };
                self.state.generation = next;
                Self::step_ok(vec![RecoveryEffect::CloseGate { generation: next }])
            }
        }
    }
}
