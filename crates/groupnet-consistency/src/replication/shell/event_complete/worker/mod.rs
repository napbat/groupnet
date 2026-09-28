//! Thin async driver for the shared sans-IO named-subscription session.

use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use groupnet_core::Time;
use groupnet_core::replication::{
    Effect, Event, Operation, Reject, SessionEngine, Stage, SubscriptionError,
};
use tokio::sync::{OwnedSemaphorePermit, mpsc, watch};

use super::{NamedCommand, NamedPublished, NamedShared};
use crate::replication::ApplicationAdapter;
use crate::replication::api::{AdapterFailure, FailureClass};
use crate::replication::shell::Manager;
use crate::replication::subscription_api::{
    DurableEventSink, DurableSubscriptionSource, SubscriptionSourceResult,
};

mod sink;
mod source;

struct NativePayload<B> {
    id: u64,
    native: B,
    bytes_permit: OwnedSemaphorePermit,
}

struct Driver<S, A, D, M>
where
    S: DurableSubscriptionSource,
    A: ApplicationAdapter<S::Position, S::Batch>,
    D: DurableEventSink<S::Position, S::Batch>,
{
    manager: Arc<Manager<S, A, M>>,
    sink: Arc<D>,
    shared: Arc<NamedShared>,
    engine: SessionEngine,
    updates: watch::Sender<NamedPublished>,
    effects: VecDeque<Effect>,
    payload: Option<NativePayload<S::Batch>>,
    proof: Option<groupnet_core::replication::SourceProof>,
    started: Instant,
    failure: Option<FailureClass>,
}

impl<S, A, D, M> Drop for Driver<S, A, D, M>
where
    S: DurableSubscriptionSource,
    A: ApplicationAdapter<S::Position, S::Batch>,
    D: DurableEventSink<S::Position, S::Batch>,
{
    fn drop(&mut self) {
        self.shared.cancelled.store(true, Ordering::Release);
        self.shared.fence.retire();
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "worker bootstrap passes its initial core state and original deadline anchor explicitly"
)]
pub(super) async fn run<S, A, D, M>(
    manager: Arc<Manager<S, A, M>>,
    sink: Arc<D>,
    shared: Arc<NamedShared>,
    engine: SessionEngine,
    commands: mpsc::Receiver<NamedCommand>,
    updates: watch::Sender<NamedPublished>,
    initial: Vec<Effect>,
    started: Instant,
) where
    S: DurableSubscriptionSource,
    A: ApplicationAdapter<S::Position, S::Batch>,
    D: DurableEventSink<S::Position, S::Batch>,
    M: Send + Sync + 'static,
{
    let mut driver = Driver {
        manager,
        sink,
        shared,
        engine,
        updates,
        effects: initial.into(),
        payload: None,
        proof: None,
        started,
        failure: None,
    };
    driver.run(commands).await;
}

impl<S, A, D, M> Driver<S, A, D, M>
where
    S: DurableSubscriptionSource,
    A: ApplicationAdapter<S::Position, S::Batch>,
    D: DurableEventSink<S::Position, S::Batch>,
    M: Send + Sync + 'static,
{
    fn logical_now(&self) -> Time {
        Time(u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX))
    }

    fn publish(&self) {
        self.updates.send_replace(NamedPublished {
            state: self.engine.state().clone(),
            source_ack: self.engine.subscription_acknowledged().cloned(),
            failure: self.failure,
        });
    }

    fn step(&mut self, event: Event) -> bool {
        let result = self.engine.step(event);
        let accepted = result.rejection.is_none();
        if let Some(rejection) = result.rejection {
            if matches!(rejection, Reject::Exhausted) {
                self.failure = Some(FailureClass::Terminal);
            }
        }
        for effect in result.effects {
            if !matches!(effect, Effect::ArmTimer(_)) {
                self.effects.push_back(effect);
            }
        }
        if self.engine.state().stage != Stage::ApplyingSubscriber {
            self.payload = None;
        }
        self.publish();
        accepted
    }

    fn tick(&mut self) {
        let _ = self.step(Event::Tick(self.logical_now()));
    }

    fn reply(&mut self, op: Operation, event: Event) {
        let _ = self.reply_accepted(op, event);
    }

    fn reply_accepted(&mut self, op: Operation, event: Event) -> bool {
        self.tick();
        if self.engine.accepts_operation(op) {
            return self.step(event);
        }
        false
    }

    fn due(&self, op: Operation) -> Option<Instant> {
        self.engine
            .operation_deadline(op)
            .and_then(|due| self.started.checked_add(Duration::from_millis(due.0)))
    }

    async fn source_call<T, F>(
        &mut self,
        due: Instant,
        future: F,
    ) -> Option<SubscriptionSourceResult<T, S::Error>>
    where
        F: Future<Output = SubscriptionSourceResult<T, S::Error>> + Send,
    {
        tokio::time::timeout_at(tokio::time::Instant::from_std(due), async {
            let _permit = self.manager.operations.clone().acquire_owned().await.ok()?;
            Some(future.await)
        })
        .await
        .ok()
        .flatten()
    }

    async fn source_call_charged<T, F>(
        &mut self,
        due: Instant,
        max_bytes: usize,
        future: F,
    ) -> Option<SubscriptionSourceResult<T, S::Error>>
    where
        F: Future<Output = SubscriptionSourceResult<T, S::Error>> + Send,
    {
        let charge = u32::try_from(max_bytes).ok()?;
        tokio::time::timeout_at(tokio::time::Instant::from_std(due), async {
            let _bytes = self
                .manager
                .bytes
                .clone()
                .acquire_many_owned(charge)
                .await
                .ok()?;
            let _operation = self.manager.operations.clone().acquire_owned().await.ok()?;
            Some(future.await)
        })
        .await
        .ok()
        .flatten()
    }

    async fn sink_call<T, F>(
        &mut self,
        due: Instant,
        future: F,
    ) -> Option<Result<T, AdapterFailure<D::Error>>>
    where
        F: Future<Output = Result<T, AdapterFailure<D::Error>>> + Send,
    {
        tokio::time::timeout_at(tokio::time::Instant::from_std(due), async {
            let _permit = self.manager.operations.clone().acquire_owned().await.ok()?;
            Some(future.await)
        })
        .await
        .ok()
        .flatten()
    }

    fn source_failure(&mut self, op: Operation, error: &AdapterFailure<S::Error>) {
        match error.class() {
            FailureClass::Retryable => self.reply(op, Event::Failed { op }),
            FailureClass::Terminal => {
                self.failure = Some(FailureClass::Terminal);
                self.reply(
                    op,
                    Event::SubscriberRejected {
                        op,
                        error: SubscriptionError::Unsupported,
                    },
                );
            }
            FailureClass::AuthorityLost => {
                self.failure = Some(FailureClass::AuthorityLost);
                self.reply(
                    op,
                    Event::SubscriberRejected {
                        op,
                        error: SubscriptionError::FenceLost,
                    },
                );
            }
        }
    }

    fn sink_failure(&mut self, op: Operation, error: &AdapterFailure<D::Error>) {
        if error.class() != FailureClass::Retryable {
            self.failure = Some(error.class());
        }
        match error.class() {
            FailureClass::Retryable => {
                // Ambiguous durable sink results reconcile on idempotent replay.
                self.reply(op, Event::Failed { op });
            }
            FailureClass::Terminal => self.reply(
                op,
                Event::SubscriberRejected {
                    op,
                    error: SubscriptionError::Unsupported,
                },
            ),
            FailureClass::AuthorityLost => self.reply(
                op,
                Event::SubscriberRejected {
                    op,
                    error: SubscriptionError::FenceLost,
                },
            ),
        }
    }

    async fn run(&mut self, mut commands: mpsc::Receiver<NamedCommand>) {
        self.publish();
        loop {
            if self.shared.cancelled.load(Ordering::Acquire) {
                self.step(Event::Cancel);
                break;
            }
            if let Ok(NamedCommand::Cancel) = commands.try_recv() {
                self.step(Event::Cancel);
                break;
            }
            if let Some(effect) = self.effects.pop_front() {
                self.execute(effect).await;
                continue;
            }
            let due = self
                .engine
                .next_deadline()
                .and_then(|time| self.started.checked_add(Duration::from_millis(time.0)));
            if let Some(due) = due {
                tokio::select! {
                    command = commands.recv() => {
                        if command.is_none() || matches!(command, Some(NamedCommand::Cancel)) {
                            self.shared.fence.retire();
                            self.step(Event::Cancel);
                            break;
                        }
                    }
                    () = self.shared.hints.notified() => {
                        self.tick();
                        self.step(Event::Hint);
                    }
                    () = tokio::time::sleep_until(tokio::time::Instant::from_std(due)) => self.tick(),
                }
            } else {
                tokio::select! {
                    command = commands.recv() => {
                        if command.is_none() || matches!(command, Some(NamedCommand::Cancel)) {
                            self.shared.fence.retire();
                            self.step(Event::Cancel);
                            break;
                        }
                    }
                    () = self.shared.hints.notified() => {
                        self.tick();
                        self.step(Event::Hint);
                    }
                }
            }
        }
        self.payload = None;
        self.publish();
    }

    async fn execute(&mut self, effect: Effect) {
        self.tick();
        let op = match &effect {
            Effect::ReadCurrentSubscriber { op, .. }
            | Effect::RegisterSubscriber { op, .. }
            | Effect::ReadSubscriberRegistration { op, .. }
            | Effect::BindSinkEpoch { op, .. }
            | Effect::CheckSubscriberTail { op, .. }
            | Effect::ScanSubscriber { op, .. }
            | Effect::ApplySubscriberBatch { op, .. }
            | Effect::CommitSubscriberAck { op, .. }
            | Effect::ReadSubscriberAck { op, .. } => *op,
            Effect::IrrecoverableGap | Effect::ArmTimer(_) => return,
            _ => {
                self.failure = Some(FailureClass::Terminal);
                self.step(Event::Cancel);
                return;
            }
        };
        if !self.engine.accepts_operation(op) {
            return;
        }
        let Some(due) = self.due(op) else {
            self.step(Event::Failed { op });
            return;
        };
        match effect {
            Effect::ReadCurrentSubscriber { key, .. } => self.read_current(op, key, due).await,
            Effect::RegisterSubscriber { request, .. } => self.register(op, *request, due).await,
            Effect::ReadSubscriberRegistration {
                key, request_id, ..
            } => {
                self.read_registration(op, key, request_id, due).await;
            }
            Effect::BindSinkEpoch { registration, .. } => {
                self.bind_sink(op, *registration, due).await;
            }
            Effect::CheckSubscriberTail {
                registration, from, ..
            } => {
                self.check_tail(op, *registration, from, due).await;
            }
            Effect::ScanSubscriber {
                registration,
                from,
                max_events,
                max_bytes,
                ..
            } => {
                self.scan(op, *registration, from, max_events, max_bytes, due)
                    .await;
            }
            Effect::ApplySubscriberBatch {
                registration,
                batch,
                previous_sink,
                ..
            } => {
                self.apply(op, *registration, *batch, previous_sink, due)
                    .await;
            }
            Effect::CommitSubscriberAck { request, .. } => {
                self.commit_ack(op, *request, due).await;
            }
            Effect::ReadSubscriberAck { request, .. } => {
                self.read_ack(op, *request, due).await;
            }
            _ => {}
        }
    }
}
