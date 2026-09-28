//! Sink-independent conditional terminal reconciliation for a detached name.

use std::collections::{HashSet, VecDeque};
use std::future::Future;
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use groupnet_core::Time;
use groupnet_core::replication::{
    Effect, Event, Mode, Operation, Reject, SessionEngine, Stage, SubscriberKey, SubscriptionError,
    TerminalReceipt,
};
use tokio::sync::Semaphore;

use super::{
    ApplicationAdapter, DurableEventSink, DurableSubscriptionSource, EventSubscriptions, OpenError,
    SnapshotMode, lock,
};
use crate::replication::SubscriptionSourceResult;

/// Outcome when a source-only detached unsubscribe cannot be confirmed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DetachedUnsubscribeError {
    /// Invalid scope, request ID, deadline, or native engine configuration.
    InvalidRequest,
    /// Shared session or operation admission is full.
    Backpressured,
    /// The supplied process incarnation is already active.
    DuplicateSession,
    /// A live named worker or another detached attempt owns this key.
    ActiveSession,
    /// The source conclusively rejected the exact conditional tombstone.
    Rejected(SubscriptionError),
    /// No exact durable receipt was confirmed by the original deadline.
    Unconfirmed,
}

struct DetachedKeyReservation<'a> {
    registry: &'a Mutex<HashSet<SubscriberKey>>,
    key: SubscriberKey,
}

impl Drop for DetachedKeyReservation<'_> {
    fn drop(&mut self) {
        assert!(lock(self.registry).remove(&self.key));
    }
}

async fn source_call<T, E, F>(
    operations: Arc<Semaphore>,
    deadline: Instant,
    future: F,
) -> Option<SubscriptionSourceResult<T, E>>
where
    F: Future<Output = SubscriptionSourceResult<T, E>>,
{
    tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), async {
        let _admission = operations.acquire_owned().await.ok()?;
        Some(future.await)
    })
    .await
    .ok()
    .flatten()
}

fn source_event<T, E>(
    result: Option<SubscriptionSourceResult<T, E>>,
    op: Operation,
    accepted: impl FnOnce(T) -> Event,
) -> Event
where
    E: std::error::Error + Send + Sync + 'static,
{
    match result {
        Some(SubscriptionSourceResult::Accepted(value)) => accepted(value),
        Some(SubscriptionSourceResult::Rejected(error)) => Event::SubscriberRejected { op, error },
        // A failed response can follow a committed source mutation regardless
        // of its adapter class. Only an explicit Rejected result proves no
        // write; every Failed result must take the core's exact-ID readback.
        Some(SubscriptionSourceResult::Failed(_)) | None => Event::Failed { op },
    }
}

fn enqueue(queue: &mut VecDeque<Effect>, effects: Vec<Effect>) {
    queue.extend(
        effects
            .into_iter()
            .filter(|effect| !matches!(effect, Effect::ArmTimer(_))),
    );
}

#[expect(
    private_bounds,
    reason = "the snapshot mode bridge is sealed; public constructors return only supported modes"
)]
impl<S, A, D, M> EventSubscriptions<S, A, D, M>
where
    S: DurableSubscriptionSource,
    A: ApplicationAdapter<S::Position, S::Batch>,
    D: DurableEventSink<S::Position, S::Batch>,
    M: SnapshotMode<S, A>,
{
    /// Releases a detached name's source retention without binding a sink.
    /// The core reads source-current ack, writes a conditional tombstone, and
    /// reads back unknown results. Caller cancellation can leave an ambiguous
    /// source write; inspect the exact terminal ledger before reset or reuse.
    /// A confirmed tombstone releases source retention, but does not undo or
    /// quiesce a previously issued remote sink transaction; the sink must
    /// enforce its durable epoch and previous-cursor fence.
    /// `session_id` must be fresh across process restarts and delayed replies.
    ///
    /// # Errors
    /// Returns invalid input, shared admission failure, conclusive source
    /// rejection, or an unconfirmed original deadline.
    #[expect(
        clippy::too_many_lines,
        reason = "one bounded source-only driver interprets the core current/tombstone/readback effects"
    )]
    pub async fn unsubscribe_detached(
        &self,
        key: SubscriberKey,
        session_id: NonZeroU64,
        request_id: Vec<u8>,
        deadline: Instant,
    ) -> Result<TerminalReceipt, DetachedUnsubscribeError> {
        let started = Instant::now();
        let due_ms = deadline
            .checked_duration_since(started)
            .and_then(|duration| u64::try_from(duration.as_millis()).ok())
            .filter(|millis| *millis > 0)
            .ok_or(DetachedUnsubscribeError::InvalidRequest)?;
        if key.scope.stream.group != self.replication.inner.group.id().as_str() {
            return Err(DetachedUnsubscribeError::InvalidRequest);
        }
        let mut config = self.replication.inner.limits.core;
        config.snapshot = None;
        let mut engine = SessionEngine::new(
            key.scope.clone(),
            Mode::EventComplete,
            config,
            session_id.get(),
        )
        .map_err(|_| DetachedUnsubscribeError::InvalidRequest)?;
        let initial = engine.step(Event::StartDetachedSubscriberTerminal {
            key: key.clone(),
            request_id,
            due: Time(due_ms),
            limits: self.limits,
        });
        if initial.rejection.is_some() {
            return Err(DetachedUnsubscribeError::InvalidRequest);
        }
        let _reservation = {
            let named = lock(&self.named);
            let mut detached = lock(&self.detached_terminal);
            if named.contains_key(&key) || detached.contains(&key) {
                return Err(DetachedUnsubscribeError::ActiveSession);
            }
            let reservation = self
                .replication
                .inner
                .reserve_registration(session_id)
                .map_err(|error| match error {
                    OpenError::DuplicateSession => DetachedUnsubscribeError::DuplicateSession,
                    OpenError::Backpressured => DetachedUnsubscribeError::Backpressured,
                    _ => DetachedUnsubscribeError::InvalidRequest,
                })?;
            if !detached.insert(key.clone()) {
                return Err(DetachedUnsubscribeError::ActiveSession);
            }
            reservation
        };
        let _key_reservation = DetachedKeyReservation {
            registry: &self.detached_terminal,
            key,
        };
        let mut effects = VecDeque::new();
        enqueue(&mut effects, initial.effects);
        loop {
            if Instant::now() >= deadline {
                return Err(DetachedUnsubscribeError::Unconfirmed);
            }
            let now = Time(u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX));
            enqueue(&mut effects, engine.step(Event::Tick(now)).effects);
            if let Some(receipt) = engine.subscription_terminal() {
                return if Instant::now() < deadline {
                    Ok(receipt.clone())
                } else {
                    Err(DetachedUnsubscribeError::Unconfirmed)
                };
            }
            if matches!(
                engine.state().stage,
                Stage::RetryExhausted | Stage::IrrecoverableGap
            ) {
                return Err(DetachedUnsubscribeError::Unconfirmed);
            }
            let Some(effect) = effects.pop_front() else {
                let Some(next) = engine.next_deadline() else {
                    return Err(DetachedUnsubscribeError::Unconfirmed);
                };
                let Some(wake) = started.checked_add(Duration::from_millis(next.0)) else {
                    return Err(DetachedUnsubscribeError::Unconfirmed);
                };
                tokio::time::sleep_until(tokio::time::Instant::from_std(wake.min(deadline))).await;
                continue;
            };
            let op = match &effect {
                Effect::ReadCurrentSubscriber { op, .. }
                | Effect::CommitSubscriberTerminal { op, .. }
                | Effect::ReadSubscriberTerminal { op, .. } => *op,
                _ => return Err(DetachedUnsubscribeError::Unconfirmed),
            };
            if !engine.accepts_operation(op) {
                continue;
            }
            let Some(operation_due) = engine
                .operation_deadline(op)
                .and_then(|logical| started.checked_add(Duration::from_millis(logical.0)))
            else {
                return Err(DetachedUnsubscribeError::Unconfirmed);
            };
            let operation_due = operation_due.min(deadline);
            let operations = Arc::clone(&self.replication.inner.operations);
            let source = Arc::clone(&self.replication.inner.source);
            let event = match effect {
                Effect::ReadCurrentSubscriber { key, .. } => source_event(
                    source_call(
                        operations,
                        operation_due,
                        source.read_current_subscriber(key),
                    )
                    .await,
                    op,
                    |state| Event::CurrentSubscriberRead {
                        op,
                        state: state.map(Box::new),
                    },
                ),
                Effect::CommitSubscriberTerminal { request, .. } => source_event(
                    source_call(
                        operations,
                        operation_due,
                        source.commit_subscriber_terminal(*request),
                    )
                    .await,
                    op,
                    |receipt| Event::SubscriberTerminalCommitted {
                        op,
                        receipt: Box::new(receipt),
                    },
                ),
                Effect::ReadSubscriberTerminal { request, .. } => source_event(
                    source_call(
                        operations,
                        operation_due,
                        source.read_subscriber_terminal(*request),
                    )
                    .await,
                    op,
                    |receipt| Event::SubscriberTerminalRead {
                        op,
                        receipt: receipt.map(Box::new),
                    },
                ),
                _ => unreachable!("effect classified above"),
            };
            let now = Time(u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX));
            enqueue(&mut effects, engine.step(Event::Tick(now)).effects);
            if Instant::now() >= deadline || !engine.accepts_operation(op) {
                continue;
            }
            let outcome = engine.step(event);
            if let Some(Reject::Subscription(error)) = outcome.rejection {
                return Err(DetachedUnsubscribeError::Rejected(error));
            }
            enqueue(&mut effects, outcome.effects);
        }
    }
}
