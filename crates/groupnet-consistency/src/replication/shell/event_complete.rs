//! Bounded named-subscription registry sharing `StateSync` operation admission.

use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use groupnet_core::replication::{
    Cursor, Event, Mode, RegisterSubscriber, ResumeSubscriber, RetentionPolicy, Scope,
    SessionEngine, Stage, State, SubscriberId, SubscriberKey, SubscriptionLimits,
};
use tokio::sync::{Notify, mpsc, watch};
use tokio::task::JoinHandle;

use super::{OpenError, Replication, lock};
use crate::replication::snapshot_runtime::SnapshotMode;
use crate::replication::{
    ApplicationAdapter, DurableEventSink, DurableSubscriptionSource, FailureClass, OperationFence,
};

mod worker;

/// Explicit durable subscriber start. Recovery never silently attaches at head.
#[derive(Clone, Debug)]
pub enum SubscriptionStart<P> {
    /// Atomically claim an absent stable name and protect exactly this cursor.
    StartAt {
        /// Source-native requested start, including native batch position.
        position: P,
        /// Finite source-enforced retention policy.
        policy: RetentionPolicy,
        /// Stable registration request ID for ambiguous response readback.
        request_id: Vec<u8>,
    },
    /// Read source-current durable ack and replace its incarnation conditionally.
    ResumeExisting {
        /// Expected source-enforced retention policy.
        policy: RetentionPolicy,
        /// Stable replacement request ID for ambiguous response readback.
        request_id: Vec<u8>,
    },
}

/// Progress for one source-protected named subscriber.
#[derive(Clone, Debug)]
pub struct NamedSubscriptionStatus<P> {
    /// Core stage, including terminal gap and bounded retry exhaustion.
    pub stage: Stage,
    /// Source-protected durable ack, independent of the sink cursor.
    pub source_ack: Option<P>,
    /// Last durably recoverable sink effect cursor.
    pub sink_cursor: Option<P>,
    /// Terminal classified adapter failure, if any.
    pub failure: Option<FailureClass>,
}

#[derive(Clone, Debug)]
struct NamedPublished {
    state: State,
    source_ack: Option<Cursor>,
    failure: Option<FailureClass>,
}

#[derive(Debug)]
enum NamedCommand {
    Cancel,
}

struct NamedShared {
    key: SubscriberKey,
    session_id: NonZeroU64,
    commands: mpsc::Sender<NamedCommand>,
    hints: Notify,
    published: watch::Receiver<NamedPublished>,
    cancelled: AtomicBool,
    fence: OperationFence,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl fmt::Debug for NamedShared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NamedShared")
            .field("key", &self.key)
            .field("session_id", &self.session_id)
            .field("cancelled", &self.cancelled.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

/// Opt-in manager for durable named subscribers over the same native source.
/// State-sync and named subscribers share one bounded registration count and
/// the same global operation, replay-byte, and checkpoint admission pools.
pub struct EventSubscriptions<S, A, D, M>
where
    S: DurableSubscriptionSource,
    A: ApplicationAdapter<S::Position, S::Batch>,
    D: DurableEventSink<S::Position, S::Batch>,
{
    replication: Replication<S, A, M>,
    sink: Arc<D>,
    limits: SubscriptionLimits,
    named: Mutex<HashMap<SubscriberKey, Arc<NamedShared>>>,
}

impl<S, A, D, M> fmt::Debug for EventSubscriptions<S, A, D, M>
where
    S: DurableSubscriptionSource,
    A: ApplicationAdapter<S::Position, S::Batch>,
    D: DurableEventSink<S::Position, S::Batch>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventSubscriptions")
            .field("replication", &self.replication)
            .field("named", &lock(&self.named).len())
            .finish_non_exhaustive()
    }
}

#[expect(
    private_bounds,
    reason = "the snapshot mode bridge is sealed; public constructors return only supported modes"
)]
impl<S, A, M> Replication<S, A, M>
where
    S: DurableSubscriptionSource,
    A: ApplicationAdapter<S::Position, S::Batch>,
    M: SnapshotMode<S, A>,
{
    /// Adds source-backed named event delivery before opening sessions.
    ///
    /// # Errors
    /// Returns invalid configuration for zero or inconsistent identity bounds,
    /// or when `StateSync` sessions are already open.
    pub fn with_event_complete<D: DurableEventSink<S::Position, S::Batch>>(
        self,
        sink: D,
        limits: SubscriptionLimits,
    ) -> Result<EventSubscriptions<S, A, D, M>, OpenError> {
        if !limits.valid() || !lock(&self.inner.sessions).is_empty() {
            return Err(OpenError::InvalidConfig);
        }
        Ok(EventSubscriptions {
            replication: self,
            sink: Arc::new(sink),
            limits,
            named: Mutex::new(HashMap::new()),
        })
    }
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
    /// Existing state-sync manager with shared global capacity.
    #[must_use]
    pub fn state_sync(&self) -> &Replication<S, A, M> {
        &self.replication
    }

    /// Opens one stable subscriber identity under an exact fresh incarnation.
    ///
    /// # Errors
    /// Returns duplicate, wrong-group, invalid identity, or shared-capacity
    /// backpressure before source or sink work begins.
    #[expect(
        clippy::too_many_lines,
        reason = "registration validation, admission, and atomic worker publication stay in one lifecycle operation"
    )]
    pub fn open_named(
        &self,
        scope: &Scope,
        subscriber: SubscriberId,
        session_id: NonZeroU64,
        start: SubscriptionStart<S::Position>,
    ) -> Result<NamedSubscriptionHandle<S>, OpenError> {
        if scope.stream.group != self.replication.inner.group.id().as_str() {
            return Err(OpenError::WrongGroup);
        }
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| OpenError::NoRuntime)?;
        // The first core operation is issued before the worker is scheduled.
        // Its runtime deadline must include any time spent waiting to poll.
        let started = std::time::Instant::now();
        let key = SubscriberKey {
            scope: scope.clone(),
            subscriber,
        };
        let mut registry = lock(&self.named);
        if registry.contains_key(&key) {
            return Err(OpenError::AlreadyOpen);
        }
        // Event-complete recovery cannot replace missing events with a state
        // snapshot, even when the shared StateSync manager supports snapshots.
        let mut core_config = self.replication.inner.limits.core;
        core_config.snapshot = None;
        let mut engine = SessionEngine::new(
            scope.clone(),
            Mode::EventComplete,
            core_config,
            session_id.get(),
        )
        .map_err(|_| OpenError::InvalidConfig)?;
        let initial = match start {
            SubscriptionStart::StartAt {
                position,
                policy,
                request_id,
            } => {
                let cursor = self
                    .replication
                    .inner
                    .source
                    .cursor(scope, &position)
                    .map_err(|_| OpenError::InvalidConfig)?;
                engine.step(Event::StartSubscription {
                    request: Box::new(RegisterSubscriber {
                        key: key.clone(),
                        incarnation: session_id,
                        start: cursor,
                        policy,
                        request_id,
                        expected_prior_ordinal: None,
                    }),
                    limits: self.limits,
                })
            }
            SubscriptionStart::ResumeExisting { policy, request_id } => {
                engine.step(Event::ResumeSubscription {
                    request: Box::new(ResumeSubscriber {
                        key: key.clone(),
                        incarnation: session_id,
                        policy,
                        request_id,
                    }),
                    limits: self.limits,
                })
            }
        };
        if initial.rejection.is_some() {
            return Err(OpenError::InvalidConfig);
        }
        let reservation = self.replication.inner.reserve_registration(session_id)?;
        let (commands, receiver) = mpsc::channel(self.replication.inner.limits.queue_depth);
        let (updates, published) = watch::channel(NamedPublished {
            state: engine.state().clone(),
            source_ack: None,
            failure: None,
        });
        let shared = Arc::new(NamedShared {
            key: key.clone(),
            session_id,
            commands,
            hints: Notify::new(),
            published,
            cancelled: AtomicBool::new(false),
            fence: OperationFence::default(),
            task: Mutex::new(None),
        });
        let handle = NamedSubscriptionHandle {
            source: Arc::clone(&self.replication.inner.source),
            shared: Arc::clone(&shared),
            _position: PhantomData,
        };
        registry.insert(key, Arc::clone(&shared));
        reservation.commit();
        let manager = Arc::clone(&self.replication.inner);
        let sink = Arc::clone(&self.sink);
        let task_shared = Arc::clone(&shared);
        let task = runtime.spawn(async move {
            worker::run(
                manager,
                sink,
                task_shared,
                engine,
                receiver,
                updates,
                initial.effects,
                started,
            )
            .await;
        });
        *lock(&shared.task) = Some(task);
        Ok(handle)
    }

    /// Closes only this exact subscriber incarnation, preserving the durable
    /// source registration for a future `ResumeExisting` until policy expiry.
    #[must_use]
    pub fn close_named_if(&self, key: &SubscriberKey, session_id: NonZeroU64) -> bool {
        let removed = {
            let mut registry = lock(&self.named);
            if registry
                .get(key)
                .is_none_or(|entry| entry.session_id != session_id)
            {
                return false;
            }
            if let Some(shared) = registry.get(key) {
                retire_named(shared);
            }
            let removed = registry.remove(key);
            if let Some(shared) = &removed {
                self.replication
                    .inner
                    .release_registration(shared.session_id);
            }
            removed
        };
        if let Some(shared) = removed {
            if let Some(task) = lock(&shared.task).take() {
                task.abort();
            }
            true
        } else {
            false
        }
    }
}

impl<S, A, D, M> Drop for EventSubscriptions<S, A, D, M>
where
    S: DurableSubscriptionSource,
    A: ApplicationAdapter<S::Position, S::Batch>,
    D: DurableEventSink<S::Position, S::Batch>,
{
    fn drop(&mut self) {
        for (_, shared) in lock(&self.named).drain() {
            retire_named(&shared);
            self.replication
                .inner
                .release_registration(shared.session_id);
            if let Some(task) = lock(&shared.task).take() {
                task.abort();
            }
        }
    }
}

fn retire_named(shared: &NamedShared) {
    shared.cancelled.store(true, Ordering::Release);
    shared.fence.retire();
    shared.hints.notify_waiters();
}

/// Local handle to one durable named subscriber, without read-serving claims.
pub struct NamedSubscriptionHandle<S: DurableSubscriptionSource> {
    source: Arc<S>,
    shared: Arc<NamedShared>,
    _position: PhantomData<S::Position>,
}

impl<S: DurableSubscriptionSource> fmt::Debug for NamedSubscriptionHandle<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NamedSubscriptionHandle")
            .field("key", &self.shared.key)
            .field("session_id", &self.shared.session_id)
            .finish_non_exhaustive()
    }
}

impl<S: DurableSubscriptionSource> NamedSubscriptionHandle<S> {
    /// Source scope and stable subscriber name for this local session.
    #[must_use]
    pub fn key(&self) -> &SubscriberKey {
        &self.shared.key
    }

    /// Best-effort wake; source timers discover commits without this hint.
    pub fn hint(&self) {
        self.shared.hints.notify_one();
    }

    /// Current durable sink and source-protected positions, kept separate.
    #[must_use]
    pub fn status(&self) -> NamedSubscriptionStatus<S::Position> {
        let published = self.shared.published.borrow().clone();
        let decode = |cursor: &Cursor| self.source.position(cursor).ok();
        NamedSubscriptionStatus {
            stage: if self.shared.cancelled.load(Ordering::Acquire) {
                Stage::Cancelled
            } else {
                published.state.stage
            },
            source_ack: published.source_ack.as_ref().and_then(decode),
            sink_cursor: published.state.checkpoint.as_ref().and_then(decode),
            failure: published.failure,
        }
    }

    /// Stops local work immediately. Durable source retention remains until
    /// explicit terminal unsubscribe or its finite expiry policy.
    pub fn cancel(&self) {
        self.shared.cancelled.store(true, Ordering::Release);
        self.shared.fence.retire();
        let _ = self.shared.commands.try_send(NamedCommand::Cancel);
        self.shared.hints.notify_waiters();
    }
}
