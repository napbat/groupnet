//! Bounded Tokio driver around the sans-IO replication session.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::marker::PhantomData;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use groupnet_core::replication::{
    AckWaitLimits, AckWaitOutcome, Comparison, Cursor, Mode, ReadDecision as CoreReadDecision,
    Refusal, Scope, SessionEngine, SourceProof, Stage, State,
};
use groupnet_runtime::Group;
use tokio::sync::{Notify, Semaphore, mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use super::ack_api::{AckEvidenceSource, AckWaitStartError, NamedAckRequest};
use super::api::{ApplicationAdapter, CatchUp, FailureClass, Limits, ReadVerdict, SourceAdapter};
use super::fence::OperationFence;
use super::snapshot_api::{
    NativeSnapshot, ReplayOnly, SnapshotApplicationAdapter, SnapshotSourceAdapter,
};
use super::snapshot_runtime::SnapshotMode;

mod ack;
mod event_complete;
use ack::AckSlot;
pub use event_complete::{
    DetachedUnsubscribeError, EventSubscriptions, NamedSubscriptionHandle, NamedSubscriptionStatus,
    SubscriptionStart, TerminalInspectError, UnsubscribeError,
};

fn lock<T>(value: &Mutex<T>) -> MutexGuard<'_, T> {
    value.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Failure to register a local state-sync scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpenError {
    /// Configuration is zero or exceeds the global reserve.
    InvalidConfig,
    /// Scope belongs to another Groupnet group.
    WrongGroup,
    /// A session for the scope already exists.
    AlreadyOpen,
    /// A session incarnation is already active in this manager.
    DuplicateSession,
    /// The configured local scope registry is full.
    Backpressured,
    /// No Tokio executor is running.
    NoRuntime,
}

/// Typed progress visible to readers and observers.
#[derive(Clone, Debug)]
pub struct SessionStatus<P> {
    /// Current recovery stage.
    pub stage: Stage,
    /// Latest application-visible native position.
    pub materialized: Option<P>,
    /// Latest durably recoverable native position.
    pub checkpoint: Option<P>,
    /// Last checked source head.
    pub source_head: Option<P>,
    /// Terminal source/application error, if any.
    pub failure: Option<FailureClass>,
}

#[derive(Clone, Debug)]
struct Published {
    state: State,
    decision: CoreReadDecision,
    proof: Option<SourceProof>,
    tail_checked_at: Option<Instant>,
    failure: Option<FailureClass>,
}

enum Command {
    Floor(Cursor, oneshot::Sender<bool>),
    Authority(bool),
    Cancel(oneshot::Sender<Option<FailureClass>>),
    StartAck {
        generation: u64,
        request: Box<NamedAckRequest>,
        deadline: Instant,
        reply: oneshot::Sender<Result<AckWaitOutcome, AckWaitStartError>>,
    },
    CancelAck {
        generation: u64,
        request_id: Vec<u8>,
    },
}

impl fmt::Debug for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Floor(cursor, _) => f.debug_tuple("Floor").field(cursor).finish(),
            Self::Authority(allowed) => f.debug_tuple("Authority").field(allowed).finish(),
            Self::Cancel(_) => f.write_str("Cancel(..)"),
            Self::StartAck {
                generation,
                request,
                deadline,
                ..
            } => f
                .debug_struct("StartAck")
                .field("generation", generation)
                .field("request", request)
                .field("deadline", deadline)
                .finish_non_exhaustive(),
            Self::CancelAck {
                generation,
                request_id,
            } => f
                .debug_struct("CancelAck")
                .field("generation", generation)
                .field("request_id", request_id)
                .finish(),
        }
    }
}

struct AckCapability {
    source: Arc<dyn AckEvidenceSource>,
    limits: AckWaitLimits,
}

impl fmt::Debug for AckCapability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AckCapability")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

struct SessionShared {
    scope: Scope,
    session_id: NonZeroU64,
    commands: mpsc::Sender<Command>,
    hints: Notify,
    hinted: AtomicBool,
    activity: AtomicBool,
    stale_activity: AtomicBool,
    idle_enabled: bool,
    published: watch::Receiver<Published>,
    signals: watch::Sender<()>,
    local_gate: AtomicBool,
    external_authority: AtomicBool,
    authority_revalidation: AtomicBool,
    authority_epoch: AtomicU64,
    cancelled: AtomicBool,
    alive: AtomicBool,
    fence: OperationFence,
    task: Mutex<Option<JoinHandle<()>>>,
    ack_limits: Option<AckWaitLimits>,
    ack_slot: Mutex<AckSlot>,
}

impl fmt::Debug for SessionShared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionShared")
            .field("scope", &self.scope)
            .field("alive", &self.alive.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

struct Manager<S, A, M>
where
    S: SourceAdapter,
    A: ApplicationAdapter<S::Position, S::Batch>,
{
    group: Group,
    source: Arc<S>,
    app: Arc<A>,
    limits: Limits,
    sessions: Mutex<HashMap<Scope, Arc<SessionShared>>>,
    admission: Mutex<RegistrationAdmission>,
    operations: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    checkpoint_bytes: Arc<Semaphore>,
    snapshot_ops: Arc<Semaphore>,
    snapshot_bytes: Arc<Semaphore>,
    snapshot_checkpoint_bytes: Arc<Semaphore>,
    ack: Option<AckCapability>,
    _mode: PhantomData<M>,
}

#[derive(Debug, Default)]
struct RegistrationAdmission {
    active: usize,
    session_ids: HashSet<NonZeroU64>,
}

struct RegistrationReservation<'a> {
    admission: &'a Mutex<RegistrationAdmission>,
    session_id: NonZeroU64,
    committed: bool,
}

impl RegistrationReservation<'_> {
    fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for RegistrationReservation<'_> {
    fn drop(&mut self) {
        if !self.committed {
            let mut admission = lock(self.admission);
            assert!(admission.session_ids.remove(&self.session_id));
            admission.active -= 1;
        }
    }
}

impl<S, A, M> Manager<S, A, M>
where
    S: SourceAdapter,
    A: ApplicationAdapter<S::Position, S::Batch>,
{
    fn reserve_registration(
        &self,
        session_id: NonZeroU64,
    ) -> Result<RegistrationReservation<'_>, OpenError> {
        let mut admission = lock(&self.admission);
        if admission.session_ids.contains(&session_id) {
            return Err(OpenError::DuplicateSession);
        }
        if admission.active >= self.limits.max_scopes {
            return Err(OpenError::Backpressured);
        }
        admission.active += 1;
        admission.session_ids.insert(session_id);
        Ok(RegistrationReservation {
            admission: &self.admission,
            session_id,
            committed: false,
        })
    }

    fn release_registration(&self, session_id: NonZeroU64) {
        let mut admission = lock(&self.admission);
        assert!(admission.session_ids.remove(&session_id));
        admission.active -= 1;
    }
}

impl<S, A, M> fmt::Debug for Manager<S, A, M>
where
    S: SourceAdapter,
    A: ApplicationAdapter<S::Position, S::Batch>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Manager")
            .field("group", &self.group.id())
            .field("limits", &self.limits)
            .field("sessions", &lock(&self.sessions).len())
            .finish_non_exhaustive()
    }
}

/// Registry of bounded source-backed state-sync sessions for one group.
pub struct Replication<S, A, M = ReplayOnly>
where
    S: SourceAdapter,
    A: ApplicationAdapter<S::Position, S::Batch>,
{
    inner: Arc<Manager<S, A, M>>,
}

impl<S, A, M> fmt::Debug for Replication<S, A, M>
where
    S: SourceAdapter,
    A: ApplicationAdapter<S::Position, S::Batch>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Replication").field(&self.inner).finish()
    }
}

impl<S, A> Replication<S, A, ReplayOnly>
where
    S: SourceAdapter,
    A: ApplicationAdapter<S::Position, S::Batch>,
{
    /// Builds a bounded manager. It starts no source work until [`Self::open`].
    ///
    /// # Errors
    /// Returns [`OpenError::InvalidConfig`] for zero or inconsistent limits.
    pub fn new(group: Group, source: S, app: A, limits: Limits) -> Result<Self, OpenError> {
        Self::build(group, source, app, limits)
    }
}

impl<S, A> Replication<S, A, NativeSnapshot>
where
    S: SnapshotSourceAdapter,
    A: SnapshotApplicationAdapter<S::Position, S::Batch>,
{
    /// Builds a manager with source-native snapshot recovery enabled.
    ///
    /// # Errors
    /// Returns [`OpenError::InvalidConfig`] unless finite snapshot limits are set.
    pub fn new_native(group: Group, source: S, app: A, limits: Limits) -> Result<Self, OpenError> {
        Self::build(group, source, app, limits)
    }
}

#[expect(
    private_bounds,
    reason = "snapshot modes are sealed implementation choices; callers construct only ReplayOnly or NativeSnapshot"
)]
impl<S, A, M> Replication<S, A, M>
where
    S: SourceAdapter,
    A: ApplicationAdapter<S::Position, S::Batch>,
    M: SnapshotMode<S, A>,
{
    fn build(group: Group, source: S, app: A, limits: Limits) -> Result<Self, OpenError> {
        let limits = limits.validate().map_err(|_| OpenError::InvalidConfig)?;
        if M::ENABLED != limits.core.snapshot.is_some() {
            return Err(OpenError::InvalidConfig);
        }
        Ok(Self {
            inner: Arc::new(Manager {
                group,
                source: Arc::new(source),
                app: Arc::new(app),
                limits,
                sessions: Mutex::new(HashMap::new()),
                admission: Mutex::new(RegistrationAdmission::default()),
                operations: Arc::new(Semaphore::new(limits.max_parallel_ops)),
                bytes: Arc::new(Semaphore::new(limits.max_inflight_bytes)),
                checkpoint_bytes: Arc::new(Semaphore::new(limits.max_checkpoint_inflight_bytes)),
                snapshot_ops: Arc::new(Semaphore::new(
                    limits.max_parallel_ops - usize::from(M::ENABLED),
                )),
                snapshot_bytes: Arc::new(Semaphore::new(
                    limits.max_inflight_bytes
                        - if M::ENABLED {
                            limits.core.max_batch_bytes
                        } else {
                            0
                        },
                )),
                snapshot_checkpoint_bytes: Arc::new(Semaphore::new(
                    limits.max_checkpoint_inflight_bytes
                        - if M::ENABLED {
                            limits.max_checkpoint_bytes
                        } else {
                            0
                        },
                )),
                ack: None,
                _mode: PhantomData,
            }),
        })
    }

    /// Adds source-certified named acknowledgement waits before any session
    /// opens. Existing source, application, and snapshot adapter types stay
    /// unchanged; no lease-holder roster is inferred from gossip.
    ///
    /// # Errors
    /// Returns an invalid-configuration error if the manager is already
    /// shared/open, configured twice, or given zero/inconsistent bounds.
    pub fn with_ack_evidence<E: AckEvidenceSource>(
        mut self,
        source: E,
        limits: AckWaitLimits,
    ) -> Result<Self, OpenError> {
        if limits.max_required == 0
            || limits.max_identity_bytes == 0
            || limits.max_certificate_bytes == 0
            || limits.max_metadata_bytes == 0
            || limits.max_wait_ms == 0
            || limits.poll_ms == 0
            || limits.poll_ms > limits.max_wait_ms
            || limits.max_identity_bytes > limits.max_metadata_bytes
            || limits.max_certificate_bytes > limits.max_metadata_bytes
        {
            return Err(OpenError::InvalidConfig);
        }
        let manager = Arc::get_mut(&mut self.inner).ok_or(OpenError::InvalidConfig)?;
        if manager.ack.is_some() {
            return Err(OpenError::InvalidConfig);
        }
        manager.ack = Some(AckCapability {
            source: Arc::new(source),
            limits,
        });
        Ok(self)
    }

    /// Registers one state-sync scope. `session_id` must be unique for this
    /// scope across restarts while any old reply may arrive. The caller should
    /// allocate it from a durable incarnation or another collision-proof
    /// source; a process-local atomic counter alone is insufficient.
    ///
    /// # Errors
    /// Returns an admission error for a wrong group, duplicate session/scope,
    /// full registry, or missing Tokio executor.
    pub fn open(
        &self,
        scope: Scope,
        session_id: NonZeroU64,
    ) -> Result<SessionHandle<S, A>, OpenError> {
        if scope.stream.group != self.inner.group.id().as_str() {
            return Err(OpenError::WrongGroup);
        }
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| OpenError::NoRuntime)?;
        let mut registry = lock(&self.inner.sessions);
        if registry.contains_key(&scope) {
            return Err(OpenError::AlreadyOpen);
        }
        let engine = SessionEngine::new(
            scope.clone(),
            Mode::StateSync,
            self.inner.limits.core,
            session_id.get(),
        )
        .map_err(|_| OpenError::InvalidConfig)?;
        let reservation = self.inner.reserve_registration(session_id)?;
        let (commands, receiver) = mpsc::channel(self.inner.limits.queue_depth);
        let initial = Published {
            state: engine.state().clone(),
            decision: CoreReadDecision::Refuse(Refusal::Unready),
            proof: None,
            tail_checked_at: None,
            failure: None,
        };
        let (updates, published) = watch::channel(initial);
        let (signals, _) = watch::channel(());
        let shared = Arc::new(SessionShared {
            scope: scope.clone(),
            session_id,
            commands,
            hints: Notify::new(),
            hinted: AtomicBool::new(false),
            activity: AtomicBool::new(false),
            stale_activity: AtomicBool::new(false),
            idle_enabled: self.inner.limits.core.idle.is_some(),
            published,
            signals,
            local_gate: AtomicBool::new(false),
            external_authority: AtomicBool::new(false),
            authority_revalidation: AtomicBool::new(true),
            authority_epoch: AtomicU64::new(0),
            cancelled: AtomicBool::new(false),
            alive: AtomicBool::new(true),
            fence: OperationFence::default(),
            task: Mutex::new(None),
            ack_limits: self.inner.ack.as_ref().map(|ack| ack.limits),
            ack_slot: Mutex::new(AckSlot::default()),
        });
        let handle = SessionHandle {
            source: Arc::clone(&self.inner.source),
            app: Arc::clone(&self.inner.app),
            shared: Arc::clone(&shared),
            tail_interval: Duration::from_millis(self.inner.limits.core.tail_check_ms),
        };
        registry.insert(scope, Arc::clone(&shared));
        reservation.commit();
        let manager = Arc::clone(&self.inner);
        let task_shared = Arc::clone(&shared);
        let task = runtime.spawn(async move {
            worker(manager, task_shared, engine, receiver, updates).await;
        });
        *lock(&shared.task) = Some(task);
        Ok(handle)
    }

    /// Removes one scope and immediately closes its local read gate.
    pub fn close(&self, scope: &Scope) {
        let removed = {
            let mut registry = lock(&self.inner.sessions);
            if let Some(shared) = registry.get(scope) {
                Self::retire_session(shared);
            }
            let removed = registry.remove(scope);
            if let Some(shared) = &removed {
                self.inner.release_registration(shared.session_id);
            }
            removed
        };
        if let Some(shared) = removed {
            Self::abort_session(&shared);
        }
    }

    /// Closes only the exact local incarnation, so a delayed old owner cannot
    /// evict a replacement registered under the same native scope.
    /// Returns whether that incarnation was still registered and closed.
    #[must_use]
    pub fn close_if(&self, scope: &Scope, session_id: NonZeroU64) -> bool {
        let shared = {
            let mut registry = lock(&self.inner.sessions);
            if registry
                .get(scope)
                .is_none_or(|entry| entry.session_id != session_id)
            {
                return false;
            }
            if let Some(shared) = registry.get(scope) {
                Self::retire_session(shared);
            }
            let removed = registry.remove(scope);
            if let Some(shared) = &removed {
                self.inner.release_registration(shared.session_id);
            }
            removed
        };
        if let Some(shared) = shared {
            Self::abort_session(&shared);
            true
        } else {
            false
        }
    }

    fn retire_session(shared: &SessionShared) {
        shared.local_gate.store(false, Ordering::Release);
        shared.cancelled.store(true, Ordering::Release);
        shared.alive.store(false, Ordering::Release);
        shared.fence.retire();
        shared.signals.send_replace(());
    }

    fn abort_session(shared: &SessionShared) {
        if let Some(task) = lock(&shared.task).take() {
            task.abort();
        }
    }
}

impl<S, A, M> Drop for Replication<S, A, M>
where
    S: SourceAdapter,
    A: ApplicationAdapter<S::Position, S::Batch>,
{
    fn drop(&mut self) {
        for (_, shared) in lock(&self.inner.sessions).drain() {
            self.inner.release_registration(shared.session_id);
            shared.local_gate.store(false, Ordering::Release);
            shared.cancelled.store(true, Ordering::Release);
            shared.alive.store(false, Ordering::Release);
            shared.fence.retire();
            shared.signals.send_replace(());
            if let Some(task) = lock(&shared.task).take() {
                task.abort();
            }
        }
    }
}

/// Handle for one native-position state-sync session.
pub struct SessionHandle<S, A>
where
    S: SourceAdapter,
    A: ApplicationAdapter<S::Position, S::Batch>,
{
    source: Arc<S>,
    app: Arc<A>,
    shared: Arc<SessionShared>,
    tail_interval: Duration,
}

impl<S, A> Clone for SessionHandle<S, A>
where
    S: SourceAdapter,
    A: ApplicationAdapter<S::Position, S::Batch>,
{
    fn clone(&self) -> Self {
        Self {
            source: Arc::clone(&self.source),
            app: Arc::clone(&self.app),
            shared: Arc::clone(&self.shared),
            tail_interval: self.tail_interval,
        }
    }
}

impl<S, A> fmt::Debug for SessionHandle<S, A>
where
    S: SourceAdapter,
    A: ApplicationAdapter<S::Position, S::Batch>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionHandle")
            .field("scope", &self.shared.scope)
            .field("alive", &self.shared.alive.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl<S, A> SessionHandle<S, A>
where
    S: SourceAdapter,
    A: ApplicationAdapter<S::Position, S::Batch>,
{
    fn record_activity(&self) {
        if self.shared.idle_enabled
            && self.shared.alive.load(Ordering::Acquire)
            && !self.shared.cancelled.load(Ordering::Acquire)
            && let Some(checked_at) = self.shared.published.borrow().tail_checked_at
        {
            if checked_at.elapsed() >= self.tail_interval {
                // The shell dates freshness at request start. A delayed
                // response can expire here before the core's response-
                // time logical deadline, so force a prompt source check.
                self.shared.stale_activity.store(true, Ordering::Release);
            } else {
                self.shared.activity.store(true, Ordering::Release);
            }
            self.shared.hints.notify_one();
        }
    }

    /// Coalesces a best-effort feed hint into one source-tail wakeup.
    pub fn hint(&self) {
        self.shared.hinted.store(true, Ordering::Release);
        self.shared.hints.notify_one();
    }

    /// Updates mode/lease authority. Revocation closes the local gate before
    /// the worker's next async operation or source response.
    ///
    /// # Errors
    /// Returns cancellation, backpressure, or terminal epoch exhaustion.
    pub fn set_authority(&self, allowed: bool) -> Result<(), CatchUp<S::Position>> {
        if self.shared.cancelled.load(Ordering::Acquire) {
            return Err(CatchUp::Cancelled);
        }
        self.shared
            .external_authority
            .store(allowed, Ordering::Release);
        if !allowed {
            self.shared
                .authority_revalidation
                .store(true, Ordering::Release);
            self.shared.local_gate.store(false, Ordering::Release);
            self.shared.fence.invalidate();
            if self
                .shared
                .authority_epoch
                .try_update(Ordering::AcqRel, Ordering::Acquire, |epoch| {
                    epoch.checked_add(1)
                })
                .is_err()
            {
                self.shared.cancelled.store(true, Ordering::Release);
                self.shared.signals.send_replace(());
                return Err(CatchUp::Failed(FailureClass::Terminal));
            }
        }
        self.shared.signals.send_replace(());
        let queued = self
            .shared
            .commands
            .try_send(Command::Authority(allowed))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Closed(_) => CatchUp::Cancelled,
                mpsc::error::TrySendError::Full(_) => CatchUp::Backpressured,
            });
        if allowed
            && self.shared.authority_revalidation.load(Ordering::Acquire)
            && self.shared.published.borrow().tail_checked_at.is_some()
        {
            // A false command may have been backpressured while the shell
            // synchronously revoked serving. Even if the core still sees
            // true -> true, the next source check must be prompt.
            self.hint();
        }
        queued
    }

    /// Current typed positions and stage, independent of whether a local read
    /// is presently permitted.
    ///
    /// # Errors
    /// Returns an adapter failure if a published cursor cannot be decoded.
    pub fn status(&self) -> Result<SessionStatus<S::Position>, CatchUp<S::Position>> {
        let published = self.shared.published.borrow().clone();
        let decode = |cursor: Option<Cursor>| -> Result<Option<S::Position>, CatchUp<S::Position>> {
            cursor
                .map(|c| {
                    self.source
                        .position(&c)
                        .map_err(|error| CatchUp::Failed(error.class()))
                })
                .transpose()
        };
        Ok(SessionStatus {
            stage: published.state.stage,
            materialized: decode(published.state.materialized)?,
            checkpoint: decode(published.state.checkpoint)?,
            source_head: decode(published.state.head)?,
            failure: published.failure,
        })
    }

    /// Checks the immediate local gate, a native floor, current authority, and
    /// the application's final domain read predicate. The wall-time expiry is
    /// checked here even if the worker is blocked or has stopped. With idle
    /// polling enabled, a read coalesces an asynchronous activity wake.
    #[must_use]
    pub fn read_decision(
        &self,
        floor: Option<&S::Position>,
        authority_now: bool,
    ) -> ReadVerdict<S::Position> {
        self.record_activity();
        if !self.shared.alive.load(Ordering::Acquire)
            || self.shared.cancelled.load(Ordering::Acquire)
            || !self.shared.local_gate.load(Ordering::Acquire)
            || !self.shared.external_authority.load(Ordering::Acquire)
            || self.shared.authority_revalidation.load(Ordering::Acquire)
            || !authority_now
        {
            return ReadVerdict::Fallback(Refusal::Authority);
        }
        let published = self.shared.published.borrow().clone();
        let Some(checked_at) = published.tail_checked_at else {
            return ReadVerdict::Fallback(Refusal::TailCheckDue);
        };
        if checked_at.elapsed() >= self.tail_interval {
            return ReadVerdict::Fallback(Refusal::TailCheckDue);
        }
        let through = match published.decision {
            CoreReadDecision::Serve(through) => through,
            CoreReadDecision::Refuse(reason) => return ReadVerdict::Fallback(reason),
        };
        let Some(proof) = published.proof else {
            return ReadVerdict::Fallback(Refusal::Authority);
        };
        if let Some(floor) = floor {
            let Ok(target) = self.source.cursor(&self.shared.scope, floor) else {
                return ReadVerdict::Fallback(Refusal::Floor);
            };
            let Ok(comparison) = self.source.compare(&through, &target, &proof) else {
                return ReadVerdict::Fallback(Refusal::Floor);
            };
            if !matches!(
                comparison.for_operands(&through, &target, &proof.id),
                Some(Comparison::Equal | Comparison::After)
            ) {
                return ReadVerdict::Fallback(Refusal::Floor);
            }
        }
        let Ok(position) = self.source.position(&through) else {
            return ReadVerdict::Fallback(Refusal::Unready);
        };
        if !self.app.may_serve(&self.shared.scope, &position) {
            return ReadVerdict::Fallback(Refusal::Unready);
        }
        ReadVerdict::Serve(position)
    }

    /// Waits for a native source floor with a caller deadline. Timeout reports
    /// incomplete catch-up and never rolls back an external source commit.
    /// With idle polling enabled, it coalesces activity before entering the
    /// bounded worker queue; the demand itself resets the core's idle backoff.
    #[expect(
        clippy::too_many_lines,
        reason = "the waiter checks the complete published evidence and all terminal outcomes in one loop"
    )]
    pub async fn catch_up(&self, floor: S::Position, deadline: Instant) -> CatchUp<S::Position> {
        if Instant::now() >= deadline {
            return CatchUp::TimedOut;
        }
        // A fresh local proof that already covers the floor needs no new
        // source request. This also records coalesced idle activity.
        if let ReadVerdict::Serve(position) = self.read_decision(Some(&floor), true) {
            return if Instant::now() < deadline {
                CatchUp::Ready(position)
            } else {
                CatchUp::TimedOut
            };
        }
        let cursor = match self.source.cursor(&self.shared.scope, &floor) {
            Ok(cursor) => cursor,
            Err(error) => return CatchUp::Failed(error.class()),
        };
        if self.shared.cancelled.load(Ordering::Acquire) {
            return CatchUp::Cancelled;
        }
        let (admitted, reply) = oneshot::channel();
        if self
            .shared
            .commands
            .try_send(Command::Floor(cursor.clone(), admitted))
            .is_err()
        {
            return if self.shared.alive.load(Ordering::Acquire) {
                CatchUp::Backpressured
            } else {
                CatchUp::Cancelled
            };
        }
        match tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), reply).await {
            Ok(Ok(true)) => {}
            Ok(Ok(false)) => return CatchUp::Backpressured,
            Ok(Err(_)) => return CatchUp::Cancelled,
            Err(_) => return CatchUp::TimedOut,
        }
        let mut updates = self.shared.published.clone();
        let mut signals = self.shared.signals.subscribe();
        loop {
            if !self.shared.alive.load(Ordering::Acquire) {
                return CatchUp::Cancelled;
            }
            if self.shared.cancelled.load(Ordering::Acquire) {
                return CatchUp::Cancelled;
            }
            if !self.shared.external_authority.load(Ordering::Acquire) {
                return CatchUp::AuthorityLost;
            }
            let published = updates.borrow_and_update().clone();
            if let Some(failure) = published.failure {
                return match failure {
                    FailureClass::AuthorityLost => CatchUp::AuthorityLost,
                    other => CatchUp::Failed(other),
                };
            }
            match published.state.stage {
                Stage::NeedsSnapshot => return CatchUp::NeedsSnapshot,
                Stage::Cancelled => return CatchUp::Cancelled,
                Stage::RetryExhausted | Stage::IrrecoverableGap => {
                    return CatchUp::Failed(FailureClass::Terminal);
                }
                _ => {}
            }
            if let (Some(materialized), Some(proof)) = (
                published.state.materialized.as_ref(),
                published.proof.as_ref(),
            ) {
                if materialized.scope != cursor.scope || materialized.history != cursor.history {
                    return CatchUp::InvalidFloor;
                }
                if let Ok(comparison) = self.source.compare(materialized, &cursor, proof)
                    && matches!(
                        comparison.for_operands(materialized, &cursor, &proof.id),
                        Some(Comparison::After | Comparison::Equal)
                    )
                    && matches!(published.decision, CoreReadDecision::Serve(_))
                    && self.shared.local_gate.load(Ordering::Acquire)
                    && self.shared.external_authority.load(Ordering::Acquire)
                    && !self.shared.authority_revalidation.load(Ordering::Acquire)
                    && published
                        .tail_checked_at
                        .is_some_and(|at| at.elapsed() < self.tail_interval)
                    && !self.shared.cancelled.load(Ordering::Acquire)
                {
                    return match self.source.position(materialized) {
                        Ok(position) if self.app.may_serve(&self.shared.scope, &position) => {
                            CatchUp::Ready(position)
                        }
                        Ok(_) => CatchUp::ReadPolicyBlocked,
                        Err(error) => CatchUp::Failed(error.class()),
                    };
                }
            }
            let until = tokio::time::Instant::from_std(deadline);
            let changed = tokio::time::timeout_at(until, async {
                tokio::select! {
                    result = updates.changed() => result,
                    result = signals.changed() => result,
                }
            })
            .await;
            if changed.is_err() {
                return CatchUp::TimedOut;
            }
            if matches!(changed, Ok(Err(_))) {
                return CatchUp::Cancelled;
            }
            signals.borrow_and_update();
        }
    }

    /// Closes the shell gate and operation fence immediately, then asks the
    /// worker to revoke application serving. A stalled worker cannot leave a
    /// caller waiting past `deadline`.
    pub async fn cancel(&self, deadline: Instant) -> CatchUp<S::Position> {
        self.shared.local_gate.store(false, Ordering::Release);
        self.shared.cancelled.store(true, Ordering::Release);
        self.shared.fence.invalidate();
        self.shared.signals.send_replace(());
        let (reply, received) = oneshot::channel();
        if self
            .shared
            .commands
            .try_send(Command::Cancel(reply))
            .is_err()
        {
            return CatchUp::Cancelled;
        }
        match tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), received).await {
            Ok(Ok(None)) => CatchUp::Cancelled,
            Ok(Ok(Some(FailureClass::AuthorityLost))) => CatchUp::AuthorityLost,
            Ok(Ok(Some(class))) => CatchUp::Failed(class),
            _ => CatchUp::TimedOut,
        }
    }
}

// The worker and effect driver follow in `driver.rs` to keep the public handle
// and the event loop independently reviewable.
mod driver;
use driver::worker;
