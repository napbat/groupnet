//! One fair, bounded worker per registered scope. Global semaphores admit one
//! operation turn at a time in FIFO order across scopes.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use groupnet_core::replication::{
    Batch, Coverage, Cursor, Effect, Event, Operation, ReadDecision, Reject, SessionEngine,
    SourceProof, Stage, Step,
};
use tokio::sync::{OwnedSemaphorePermit, mpsc, watch};

use super::{Command, Manager, Published, SessionShared};
use crate::replication::api::{
    ApplicationAdapter, Checkpoint, CheckpointLimit, FailureClass, ScanLimit, SourceAdapter,
    SourceBatch, TailLimit,
};

struct Payload<B> {
    native: B,
    _bytes: OwnedSemaphorePermit,
}

struct PrivateCheckpoint<P, R> {
    candidate: Checkpoint<P, R>,
    _bytes: OwnedSemaphorePermit,
}

fn discard_stale<P, B, R>(
    op: Operation,
    batch_id: Option<u64>,
    checkpoint_id: Option<u64>,
    deadlines: &mut HashMap<Operation, Instant>,
    payloads: &mut HashMap<u64, Payload<B>>,
    checkpoints: &mut HashMap<u64, PrivateCheckpoint<P, R>>,
) {
    deadlines.remove(&op);
    if let Some(id) = batch_id {
        payloads.remove(&id);
    }
    if let Some(id) = checkpoint_id {
        checkpoints.remove(&id);
    }
}

struct Driver<S, A>
where
    S: SourceAdapter,
    A: ApplicationAdapter<S::Position, S::Batch>,
{
    manager: Arc<Manager<S, A>>,
    shared: Arc<SessionShared>,
    engine: SessionEngine,
    updates: watch::Sender<Published>,
    effects: VecDeque<Effect>,
    deadlines: HashMap<Operation, Instant>,
    payloads: HashMap<u64, Payload<S::Batch>>,
    checkpoints: HashMap<u64, PrivateCheckpoint<S::Position, A::Recovery>>,
    deferred_floors: VecDeque<Cursor>,
    started: Instant,
    proof: Option<SourceProof>,
    tail_checked_at: Option<Instant>,
    tail_request_started: Option<Instant>,
    tail_authority_epoch: u64,
    failure: Option<FailureClass>,
    cancel_reply: Option<tokio::sync::oneshot::Sender<Option<FailureClass>>>,
    cancel_processed: bool,
}

impl<S, A> Driver<S, A>
where
    S: SourceAdapter,
    A: ApplicationAdapter<S::Position, S::Batch>,
{
    fn timeout(&self) -> Duration {
        Duration::from_millis(self.manager.limits.core.attempt_timeout_ms)
    }

    fn logical_now(&self) -> groupnet_core::Time {
        groupnet_core::Time(u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX))
    }

    fn publish(&self) {
        let decision = self.engine.read_decision();
        let fresh = self.tail_checked_at.is_some_and(|when| {
            when.elapsed() < Duration::from_millis(self.manager.limits.core.tail_check_ms)
        });
        let permitted = matches!(decision, ReadDecision::Serve(_))
            && fresh
            && self.failure.is_none()
            && self.shared.external_authority.load(Ordering::Acquire)
            && !self.shared.authority_revalidation.load(Ordering::Acquire)
            && !self.shared.cancelled.load(Ordering::Acquire)
            && self.shared.alive.load(Ordering::Acquire);
        self.shared.local_gate.store(permitted, Ordering::Release);
        self.updates.send_replace(Published {
            state: self.engine.state().clone(),
            decision,
            proof: self.proof.clone(),
            tail_checked_at: self.tail_checked_at,
            failure: self.failure,
        });
    }

    fn step(&mut self, event: Event) {
        let completed = match &event {
            Event::CheckpointLoaded { op, .. }
            | Event::CheckpointInstalled { op, .. }
            | Event::Tail { op, .. }
            | Event::Scanned { op, .. }
            | Event::Applied { op, .. }
            | Event::Invalidated { op }
            | Event::Failed { op } => Some(*op),
            _ => None,
        };
        let tail = match &event {
            Event::Tail { proof, .. } => Some(proof.clone()),
            _ => None,
        };
        let ignorable = matches!(
            &event,
            Event::Hint | Event::Demand { .. } | Event::Authority(_) | Event::Cancel
        );
        let Step { effects, rejection } = self.engine.step(event);
        if let Some(op) = completed {
            self.deadlines.remove(&op);
        }
        if let Some(reason) = rejection {
            if reason != Reject::StaleOperation
                && !(ignorable && reason == Reject::Stage)
                && !(ignorable && reason == Reject::History)
            {
                self.stop(FailureClass::Terminal);
            }
            return;
        }
        if let Some(proof) = tail {
            self.proof = Some(proof);
            self.tail_checked_at = self.tail_request_started.take();
            if self.shared.external_authority.load(Ordering::Acquire)
                && self.shared.authority_epoch.load(Ordering::Acquire) == self.tail_authority_epoch
            {
                self.shared
                    .authority_revalidation
                    .store(false, Ordering::Release);
                if !self.shared.external_authority.load(Ordering::Acquire)
                    || self.shared.authority_epoch.load(Ordering::Acquire)
                        != self.tail_authority_epoch
                {
                    self.shared
                        .authority_revalidation
                        .store(true, Ordering::Release);
                }
            }
        }
        for effect in &effects {
            let operation = match effect {
                Effect::LoadCheckpoint { op, .. }
                | Effect::InstallCheckpoint { op, .. }
                | Effect::CheckTail { op, .. }
                | Effect::Scan { op, .. }
                | Effect::Apply { op, .. }
                | Effect::RevokeServing { op } => Some(*op),
                _ => None,
            };
            if let Some(op) = operation {
                let deadline = if self.engine.current_operation() == Some(op) {
                    self.engine
                        .next_deadline()
                        .and_then(|due| self.started.checked_add(Duration::from_millis(due.0)))
                } else {
                    Instant::now().checked_add(self.timeout())
                };
                let Some(deadline) = deadline else {
                    return self.stop(FailureClass::Terminal);
                };
                self.deadlines.insert(op, deadline);
            }
        }
        self.effects.extend(effects);
        self.publish();
    }

    fn stop(&mut self, class: FailureClass) {
        if self.failure.is_some() {
            return;
        }
        self.failure = Some(class);
        self.shared.local_gate.store(false, Ordering::Release);
        self.shared.fence.invalidate();
        self.payloads.clear();
        self.checkpoints.clear();
        self.deadlines.clear();
        let Step { effects, .. } = self.engine.step(Event::Cancel);
        for effect in &effects {
            if let Effect::RevokeServing { op } = effect {
                if let Some(deadline) = Instant::now().checked_add(self.timeout()) {
                    self.deadlines.insert(*op, deadline);
                }
            }
        }
        self.effects.extend(effects);
        self.publish();
    }

    fn adapter_failure(&mut self, op: Operation, class: FailureClass) {
        match class {
            FailureClass::Retryable => {
                self.shared.fence.invalidate();
                self.step(Event::Failed { op });
            }
            other => self.stop(other),
        }
    }

    async fn operation_permit(
        manager: &Manager<S, A>,
        deadline: Instant,
    ) -> Option<OwnedSemaphorePermit> {
        tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            Arc::clone(&manager.operations).acquire_owned(),
        )
        .await
        .ok()?
        .ok()
    }

    async fn byte_permit(
        manager: &Manager<S, A>,
        deadline: Instant,
    ) -> Option<OwnedSemaphorePermit> {
        let count = u32::try_from(manager.limits.core.max_batch_bytes).ok()?;
        tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            Arc::clone(&manager.bytes).acquire_many_owned(count),
        )
        .await
        .ok()?
        .ok()
    }

    async fn checkpoint_permit(
        manager: &Manager<S, A>,
        deadline: Instant,
    ) -> Option<OwnedSemaphorePermit> {
        let count = u32::try_from(manager.limits.max_checkpoint_bytes).ok()?;
        tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            Arc::clone(&manager.checkpoint_bytes).acquire_many_owned(count),
        )
        .await
        .ok()?
        .ok()
    }

    fn comparisons(
        &self,
        proof: &SourceProof,
    ) -> Result<Vec<groupnet_core::replication::BoundComparison>, FailureClass> {
        let Some(materialized) = self.engine.state().materialized.as_ref() else {
            return Ok(Vec::new());
        };
        let mut pairs = vec![
            (materialized, &proof.head),
            (materialized, &proof.retained_from),
            (&proof.retained_from, &proof.head),
        ];
        if let Some(target) = self.engine.state().target.as_ref() {
            pairs.push((materialized, target));
        }
        pairs
            .into_iter()
            .map(|(left, right)| {
                self.manager
                    .source
                    .compare(left, right, proof)
                    .map_err(|error| error.class())
            })
            .collect()
    }

    async fn load_checkpoint(&mut self, op: Operation, scope: groupnet_core::replication::Scope) {
        let Some(deadline) = self.deadlines.get(&op).copied() else {
            return self.stop(FailureClass::Terminal);
        };
        let Some(bytes) = Self::checkpoint_permit(&self.manager, deadline).await else {
            return self.step(Event::Failed { op });
        };
        let Some(_permit) = Self::operation_permit(&self.manager, deadline).await else {
            return self.step(Event::Failed { op });
        };
        let loaded = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            self.manager.app.load_checkpoint(
                scope,
                CheckpointLimit {
                    bytes: self.manager.limits.max_checkpoint_bytes,
                },
            ),
        )
        .await;
        self.tick();
        match loaded {
            Ok(Ok(Some(checkpoint))) => {
                if checkpoint.bytes > self.manager.limits.max_checkpoint_bytes {
                    return self.stop(FailureClass::Terminal);
                }
                let cursor = match self
                    .manager
                    .source
                    .cursor(&self.shared.scope, &checkpoint.position)
                {
                    Ok(cursor) => cursor,
                    Err(error) => return self.adapter_failure(op, error.class()),
                };
                let payload_id = op.token;
                self.checkpoints.insert(
                    payload_id,
                    PrivateCheckpoint {
                        candidate: checkpoint,
                        _bytes: bytes,
                    },
                );
                self.step(Event::CheckpointLoaded {
                    op,
                    cursor: Some(cursor),
                    payload_id: Some(payload_id),
                });
                if self.engine.state().stage != Stage::InstallingCheckpoint {
                    self.checkpoints.remove(&payload_id);
                }
            }
            Ok(Ok(None)) => self.step(Event::CheckpointLoaded {
                op,
                cursor: None,
                payload_id: None,
            }),
            Ok(Err(error)) => self.adapter_failure(op, error.class()),
            Err(_) => self.step(Event::Failed { op }),
        }
    }

    async fn install_checkpoint(&mut self, op: Operation, cursor: Cursor, payload_id: u64) {
        let Some(deadline) = self.deadlines.get(&op).copied() else {
            return self.stop(FailureClass::Terminal);
        };
        let Some(checkpoint) = self.checkpoints.remove(&payload_id) else {
            return self.stop(FailureClass::Terminal);
        };
        let PrivateCheckpoint { candidate, _bytes } = checkpoint;
        let Some(_operation) = Self::operation_permit(&self.manager, deadline).await else {
            return self.step(Event::Failed { op });
        };
        if self.shared.cancelled.load(Ordering::Acquire) {
            return;
        }
        let Some(permit) = self.shared.fence.issue(op, deadline) else {
            return self.stop(FailureClass::Terminal);
        };
        if self.shared.cancelled.load(Ordering::Acquire) {
            self.shared.fence.invalidate();
            return;
        }
        let installed = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            self.manager
                .app
                .install_checkpoint(self.shared.scope.clone(), candidate, permit),
        )
        .await;
        self.shared.fence.invalidate();
        self.tick();
        match installed {
            Ok(Ok(receipt)) => {
                let actual = match self
                    .manager
                    .source
                    .cursor(&self.shared.scope, &receipt.position)
                {
                    Ok(value) => value,
                    Err(error) => return self.adapter_failure(op, error.class()),
                };
                if actual != cursor {
                    return self.stop(FailureClass::Terminal);
                }
                self.step(Event::CheckpointInstalled {
                    op,
                    receipt: groupnet_core::replication::ApplyReceipt {
                        through: actual,
                        durable: receipt.durable,
                    },
                });
            }
            Ok(Err(error)) => self.adapter_failure(op, error.class()),
            Err(_) => self.step(Event::Failed { op }),
        }
    }

    async fn check_tail(
        &mut self,
        op: Operation,
        scope: groupnet_core::replication::Scope,
        from: Option<Cursor>,
    ) {
        let Some(deadline) = self.deadlines.get(&op).copied() else {
            return self.stop(FailureClass::Terminal);
        };
        let from_native = match from
            .as_ref()
            .map(|cursor| self.manager.source.position(cursor))
            .transpose()
        {
            Ok(value) => value,
            Err(error) => return self.adapter_failure(op, error.class()),
        };
        let Some(_bytes) = Self::byte_permit(&self.manager, deadline).await else {
            return self.step(Event::Failed { op });
        };
        let Some(_permit) = Self::operation_permit(&self.manager, deadline).await else {
            return self.step(Event::Failed { op });
        };
        let limit = TailLimit {
            events: self.manager.limits.core.max_batch_events,
            bytes: self.manager.limits.core.max_batch_bytes,
        };
        self.tail_request_started = Some(Instant::now());
        self.tail_authority_epoch = self.shared.authority_epoch.load(Ordering::Acquire);
        let result = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            self.manager.source.tail(scope, from_native, limit),
        )
        .await;
        self.tick();
        match result {
            Ok(Ok(proof)) => match self.comparisons(&proof) {
                Ok(comparisons) => self.step(Event::Tail {
                    op,
                    proof,
                    comparisons,
                }),
                Err(class) => self.stop(class),
            },
            Ok(Err(error)) => self.adapter_failure(op, error.class()),
            Err(_) => self.step(Event::Failed { op }),
        }
        self.tail_request_started = None;
        self.flush_deferred();
    }

    async fn scan(&mut self, op: Operation, from: Cursor, max_events: usize, max_bytes: usize) {
        let Some(deadline) = self.deadlines.get(&op).copied() else {
            return self.stop(FailureClass::Terminal);
        };
        let Some(proof) = self.proof.clone() else {
            return self.stop(FailureClass::Terminal);
        };
        let native_from = match self.manager.source.position(&from) {
            Ok(value) => value,
            Err(error) => return self.adapter_failure(op, error.class()),
        };
        let Some(bytes) = Self::byte_permit(&self.manager, deadline).await else {
            return self.step(Event::Failed { op });
        };
        let Some(_operation) = Self::operation_permit(&self.manager, deadline).await else {
            return self.step(Event::Failed { op });
        };
        let scope = self.shared.scope.clone();
        let result = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            self.manager.source.scan_after(
                scope,
                native_from,
                proof.clone(),
                ScanLimit {
                    events: max_events,
                    bytes: max_bytes,
                },
            ),
        )
        .await;
        self.tick();
        let source_batch = match result {
            Ok(Ok(batch)) => batch,
            Ok(Err(error)) => return self.adapter_failure(op, error.class()),
            Err(_) => return self.step(Event::Failed { op }),
        };
        self.accept_batch(op, &from, &proof, source_batch, bytes);
    }

    fn accept_batch(
        &mut self,
        op: Operation,
        from: &Cursor,
        proof: &SourceProof,
        source_batch: SourceBatch<S::Position, S::Batch>,
        bytes: OwnedSemaphorePermit,
    ) {
        let SourceBatch {
            from: reported_from,
            through,
            proof: reported_proof,
            certificate,
            events,
            bytes: encoded_bytes,
            native,
        } = source_batch;
        let from_cursor = match self
            .manager
            .source
            .cursor(&self.shared.scope, &reported_from)
        {
            Ok(value) => value,
            Err(error) => return self.adapter_failure(op, error.class()),
        };
        let through_cursor = match self.manager.source.cursor(&self.shared.scope, &through) {
            Ok(value) => value,
            Err(error) => return self.adapter_failure(op, error.class()),
        };
        let advance = match self.manager.source.compare(from, &through_cursor, proof) {
            Ok(value) => value,
            Err(error) => return self.adapter_failure(op, error.class()),
        };
        let end_to_head = match self
            .manager
            .source
            .compare(&through_cursor, &proof.head, proof)
        {
            Ok(value) => value,
            Err(error) => return self.adapter_failure(op, error.class()),
        };
        let meta = Batch {
            coverage: Coverage {
                from: from_cursor,
                through: through_cursor,
                proof: reported_proof,
                certificate,
            },
            payload_id: op.token,
            events,
            bytes: encoded_bytes,
            advance,
            end_to_head,
        };
        self.payloads.insert(
            op.token,
            Payload {
                native,
                _bytes: bytes,
            },
        );
        self.tick();
        self.step(Event::Scanned {
            op,
            batch: Box::new(meta),
        });
        if self.engine.state().stage != Stage::Applying {
            self.payloads.remove(&op.token);
        }
    }

    async fn apply(&mut self, op: Operation, batch: Box<Batch>) {
        let Some(deadline) = self.deadlines.get(&op).copied() else {
            return self.stop(FailureClass::Terminal);
        };
        let Some(payload) = self.payloads.remove(&batch.payload_id) else {
            return self.stop(FailureClass::Terminal);
        };
        let from = match self.manager.source.position(&batch.coverage.from) {
            Ok(value) => value,
            Err(error) => return self.adapter_failure(op, error.class()),
        };
        let through = match self.manager.source.position(&batch.coverage.through) {
            Ok(value) => value,
            Err(error) => return self.adapter_failure(op, error.class()),
        };
        let Some(_operation) = Self::operation_permit(&self.manager, deadline).await else {
            return self.step(Event::Failed { op });
        };
        if self.shared.cancelled.load(Ordering::Acquire) {
            return;
        }
        let Some(permit) = self.shared.fence.issue(op, deadline) else {
            return self.stop(FailureClass::Terminal);
        };
        if self.shared.cancelled.load(Ordering::Acquire) {
            self.shared.fence.invalidate();
            return;
        }
        let result = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            self.manager.app.apply(
                self.shared.scope.clone(),
                from,
                through,
                payload.native,
                permit,
            ),
        )
        .await;
        self.shared.fence.invalidate();
        self.tick();
        match result {
            Ok(Ok(receipt)) => {
                let cursor = match self
                    .manager
                    .source
                    .cursor(&self.shared.scope, &receipt.position)
                {
                    Ok(value) => value,
                    Err(error) => return self.adapter_failure(op, error.class()),
                };
                self.tick();
                self.step(Event::Applied {
                    op,
                    receipt: groupnet_core::replication::ApplyReceipt {
                        through: cursor,
                        durable: receipt.durable,
                    },
                });
            }
            Ok(Err(error)) => self.adapter_failure(op, error.class()),
            Err(_) => self.step(Event::Failed { op }),
        }
    }

    async fn revoke(&mut self, op: Option<Operation>) {
        self.shared.local_gate.store(false, Ordering::Release);
        self.shared.fence.invalidate();
        let deadline = op
            .and_then(|operation| self.deadlines.get(&operation).copied())
            .or_else(|| Instant::now().checked_add(self.timeout()));
        let Some(deadline) = deadline else {
            return self.stop(FailureClass::Terminal);
        };
        let Some(_permit) = Self::operation_permit(&self.manager, deadline).await else {
            return self.stop(FailureClass::Terminal);
        };
        let Some(permit) = self.shared.fence.issue_revocation(op, deadline) else {
            return self.stop(FailureClass::Terminal);
        };
        let result = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            self.manager
                .app
                .revoke_serving(self.shared.scope.clone(), permit),
        )
        .await;
        self.shared.fence.invalidate();
        self.tick();
        match result {
            Ok(Ok(())) => {
                if let Some(op) = op {
                    self.step(Event::Invalidated { op });
                }
            }
            Ok(Err(error)) => self.stop(error.class()),
            Err(_) => self.stop(FailureClass::Terminal),
        }
    }

    async fn effect(&mut self, effect: Effect) {
        let (batch_id, checkpoint_id) = match &effect {
            Effect::Apply { batch, .. } => (Some(batch.payload_id), None),
            Effect::InstallCheckpoint { payload_id, .. } => (None, Some(*payload_id)),
            _ => (None, None),
        };
        let op = match &effect {
            Effect::LoadCheckpoint { op, .. }
            | Effect::InstallCheckpoint { op, .. }
            | Effect::CheckTail { op, .. }
            | Effect::Scan { op, .. }
            | Effect::Apply { op, .. }
            | Effect::RevokeServing { op } => Some(*op),
            _ => None,
        };
        if op.is_some_and(|operation| {
            !self.deadlines.contains_key(&operation) || !self.engine.accepts_operation(operation)
        }) {
            if let Some(operation) = op {
                discard_stale(
                    operation,
                    batch_id,
                    checkpoint_id,
                    &mut self.deadlines,
                    &mut self.payloads,
                    &mut self.checkpoints,
                );
            }
            return;
        }
        match effect {
            Effect::LoadCheckpoint { op, scope } => self.load_checkpoint(op, scope).await,
            Effect::InstallCheckpoint {
                op,
                cursor,
                payload_id,
            } => {
                self.install_checkpoint(op, cursor, payload_id).await;
            }
            Effect::CheckTail { op, scope, from } => self.check_tail(op, scope, from).await,
            Effect::Scan {
                op,
                from,
                max_events,
                max_bytes,
            } => self.scan(op, from, max_events, max_bytes).await,
            Effect::Apply { op, batch } => self.apply(op, batch).await,
            Effect::RevokeServing { op } => self.revoke(Some(op)).await,
            Effect::RevokeServingUnconfirmed => self.revoke(None).await,
            Effect::ArmTimer(_) => {}
            Effect::NeedsSnapshot | Effect::IrrecoverableGap => self.publish(),
        }
    }

    fn demand(&mut self, cursor: Cursor) -> bool {
        let comparison = if let Some(existing) = self.engine.state().target.as_ref() {
            let Some(proof) = self.proof.as_ref() else {
                if self.deferred_floors.len() < self.manager.limits.queue_depth {
                    self.deferred_floors.push_back(cursor);
                    return true;
                }
                return false;
            };
            match self.manager.source.compare(existing, &cursor, proof) {
                Ok(value) => Some(value),
                Err(error) => {
                    self.stop(error.class());
                    return true;
                }
            }
        } else {
            None
        };
        self.step(Event::Demand { cursor, comparison });
        true
    }

    fn flush_deferred(&mut self) {
        if self.proof.is_none() {
            return;
        }
        while let Some(cursor) = self.deferred_floors.pop_front() {
            let _ = self.demand(cursor);
        }
    }

    fn command(&mut self, command: Command) {
        match command {
            Command::Floor(cursor, reply) => {
                let accepted =
                    !self.shared.cancelled.load(Ordering::Acquire) && self.demand(cursor);
                let _ = reply.send(accepted);
            }
            Command::Authority(allowed) => self.step(Event::Authority(allowed)),
            Command::Cancel(reply) => {
                self.shared.local_gate.store(false, Ordering::Release);
                self.shared.fence.invalidate();
                self.payloads.clear();
                self.checkpoints.clear();
                if !self.cancel_processed {
                    self.effects.clear();
                    self.step(Event::Cancel);
                    self.cancel_processed = true;
                }
                self.cancel_reply = Some(reply);
            }
        }
    }

    fn next_timer(&self) -> Option<tokio::time::Instant> {
        self.engine
            .next_deadline()
            .and_then(|time| self.started.checked_add(Duration::from_millis(time.0)))
            .map(tokio::time::Instant::from_std)
    }

    fn tick(&mut self) {
        let now = self.logical_now();
        self.step(Event::Tick(now));
    }
}

pub(super) async fn worker<S, A>(
    manager: Arc<Manager<S, A>>,
    shared: Arc<SessionShared>,
    engine: SessionEngine,
    mut receiver: mpsc::Receiver<Command>,
    updates: watch::Sender<Published>,
) where
    S: SourceAdapter,
    A: ApplicationAdapter<S::Position, S::Batch>,
{
    let mut driver = Driver {
        manager,
        shared,
        engine,
        updates,
        effects: VecDeque::new(),
        deadlines: HashMap::new(),
        payloads: HashMap::new(),
        checkpoints: HashMap::new(),
        deferred_floors: VecDeque::new(),
        started: Instant::now(),
        proof: None,
        tail_checked_at: None,
        tail_request_started: None,
        tail_authority_epoch: 0,
        failure: None,
        cancel_reply: None,
        cancel_processed: false,
    };
    driver.step(Event::StartBootstrap);
    loop {
        if driver.shared.cancelled.load(Ordering::Acquire) && !driver.cancel_processed {
            driver.shared.local_gate.store(false, Ordering::Release);
            driver.shared.fence.invalidate();
            driver.effects.clear();
            driver.payloads.clear();
            driver.checkpoints.clear();
            driver.step(Event::Cancel);
            driver.cancel_processed = true;
        }
        if driver
            .engine
            .next_deadline()
            .is_some_and(|due| due <= driver.logical_now())
        {
            driver.tick();
        }
        if let Ok(command) = receiver.try_recv() {
            driver.command(command);
        }
        if let Some(effect) = driver.effects.pop_front() {
            driver.effect(effect).await;
            continue;
        }
        if let Some(reply) = driver.cancel_reply.take() {
            let _ = reply.send(driver.failure);
        }
        let next_timer = driver.next_timer();
        let shared = Arc::clone(&driver.shared);
        tokio::select! {
            command = receiver.recv() => {
                let Some(command) = command else { break; };
                driver.command(command);
            }
            () = shared.hints.notified() => {
                if shared.hinted.swap(false, Ordering::AcqRel) {
                    driver.step(Event::Hint);
                }
            }
            () = async {
                if let Some(instant) = next_timer {
                    tokio::time::sleep_until(instant).await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => driver.tick(),
        }
    }
    driver.shared.alive.store(false, Ordering::Release);
    driver.shared.local_gate.store(false, Ordering::Release);
    driver.shared.fence.invalidate();
}

#[cfg(test)]
mod tests {
    use super::{Checkpoint, Payload, PrivateCheckpoint, discard_stale};
    use groupnet_core::replication::Operation;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;
    use tokio::sync::Semaphore;

    struct DropCount(Arc<AtomicUsize>);

    impl Drop for DropCount {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Release);
        }
    }

    #[test]
    fn stale_effect_releases_native_batch_reserve_and_private_checkpoint() {
        let bytes = Arc::new(Semaphore::new(8));
        let permit = Arc::clone(&bytes)
            .try_acquire_many_owned(8)
            .expect("reserve");
        let checkpoint_bytes = Arc::new(Semaphore::new(8));
        let checkpoint_permit = Arc::clone(&checkpoint_bytes)
            .try_acquire_many_owned(8)
            .expect("checkpoint reserve");
        let released = Arc::new(AtomicUsize::new(0));
        let mut payloads = HashMap::from([(
            7,
            Payload {
                native: (),
                _bytes: permit,
            },
        )]);
        let mut checkpoints = HashMap::from([(
            8,
            PrivateCheckpoint {
                candidate: Checkpoint {
                    position: (),
                    native: DropCount(Arc::clone(&released)),
                    bytes: 8,
                },
                _bytes: checkpoint_permit,
            },
        )]);
        let apply = Operation {
            session: 1,
            generation: 1,
            token: 2,
        };
        let install = Operation {
            session: 1,
            generation: 1,
            token: 3,
        };
        let mut deadlines = HashMap::from([(apply, Instant::now()), (install, Instant::now())]);
        discard_stale(
            apply,
            Some(7),
            None,
            &mut deadlines,
            &mut payloads,
            &mut checkpoints,
        );
        assert_eq!(bytes.available_permits(), 8);
        assert!(payloads.is_empty());
        discard_stale(
            install,
            None,
            Some(8),
            &mut deadlines,
            &mut payloads,
            &mut checkpoints,
        );
        assert_eq!(released.load(Ordering::Acquire), 1);
        assert_eq!(checkpoint_bytes.available_permits(), 8);
        assert!(checkpoints.is_empty());
        assert!(deadlines.is_empty());
    }
}
