//! Synchronous safety gate and bounded asynchronous effect driver.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use groupnet_core::volatile_recovery::{
    Mark, Peer, RecoveryConfig, RecoveryEffect, RecoveryEngine, RecoveryError, RecoveryEvent,
    RecoveryMode, RecoveryOperation, RecoveryState,
};
use groupnet_core::{NodeId, Time};
use tokio::sync::Notify;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// One adapter operation, bounded by the current core operation deadline.
pub type BoxRecoveryFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A failed source or application operation. The core chooses its fallback.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdapterError;

/// Bounded native membership, lease confirmation, and feed-head sample.
pub type PeerObservation = Result<(Vec<Peer>, Option<Mark>), AdapterError>;

/// Consumer-owned facts and application effects for volatile coherence.
///
/// Implementations must honor the supplied operation and publication permit.
/// They must bound roster allocation *before* building a vector, and must use
/// [`PublicationPermit::publish`] for every index page/swap (not only the final
/// receipt). An adapter must not turn an unreadable feed into an empty head.
/// `revoke_serving` and `affirm` run while the shell holds its short control
/// lock: they must not call this handle's status/signal methods or a permit's
/// methods, and they must not block on I/O.
pub trait RecoveryAdapter: Send + Sync + 'static {
    /// Immediately revoke any independent lease or application serve grant.
    /// Called under the same short lock that closes the shell's read gate.
    fn revoke_serving(&self);

    /// Make old index/body state unservable before later reconstruction.
    fn invalidate(
        &self,
        op: RecoveryOperation,
        distrust_bodies: bool,
        permit: PublicationPermit,
    ) -> BoxRecoveryFuture<'_, Result<(), AdapterError>>;

    /// Build an origin index privately and publish through the permit.
    fn rebuild_origin(
        &self,
        op: RecoveryOperation,
        permit: PublicationPermit,
    ) -> BoxRecoveryFuture<'_, Result<(), AdapterError>>;

    /// Observe at most `limits.max_members` complete native peer facts.
    fn observe_peers(
        &self,
        op: RecoveryOperation,
        limits: RecoveryConfig,
    ) -> BoxRecoveryFuture<'_, PeerObservation>;

    /// Wait for every sampled native writer head to be applied locally.
    fn wait_frontiers(
        &self,
        op: RecoveryOperation,
        heads: Vec<(NodeId, Mark)>,
    ) -> BoxRecoveryFuture<'_, Result<(), AdapterError>>;

    /// Atomically attempt the independent lease/application affirmation.
    /// This method is synchronous and must not block on network or origin I/O.
    /// It runs under the same short lock as public gate closure.
    fn affirm(&self, op: RecoveryOperation) -> bool;
}

#[derive(Clone, Copy, Debug)]
struct Control {
    version: u64,
    open: bool,
    state: RecoveryState,
    operation: Option<RecoveryOperation>,
    pending: Pending,
    terminal: bool,
}

#[derive(Clone, Copy, Debug, Default)]
struct Pending {
    start: bool,
    gap: bool,
    lapse: u64,
    cancel: bool,
}

struct Shared<A> {
    adapter: Arc<A>,
    mode: RecoveryMode,
    control: Arc<Mutex<Control>>,
    handles: AtomicUsize,
    notify: Notify,
}

impl<A: RecoveryAdapter> Shared<A> {
    fn close(&self, expected: u64) -> Option<u64> {
        let mut control = lock(&self.control);
        if control.version != expected || control.terminal {
            return None;
        }
        control.open = false;
        control.operation = None;
        self.adapter.revoke_serving();
        let Some(version) = control.version.checked_add(1) else {
            control.terminal = true;
            control.pending.cancel = true;
            self.notify.notify_one();
            return None;
        };
        control.version = version;
        Some(control.version)
    }

    fn signal(&self, kind: Signal) -> Result<(), RecoveryError> {
        if matches!(kind, Signal::Lapse(_)) && self.mode != RecoveryMode::Leased {
            return Err(RecoveryError::Stage);
        }
        let result = {
            let mut control = lock(&self.control);
            if control.terminal {
                return if matches!(kind, Signal::Cancel) {
                    Ok(())
                } else {
                    Err(RecoveryError::Stage)
                };
            }
            if let Signal::Lapse(count) = kind
                && count <= control.state.covered_lapses
            {
                return Ok(());
            }
            control.open = false;
            control.operation = None;
            self.adapter.revoke_serving();
            if let Some(version) = control.version.checked_add(1) {
                control.version = version;
                if !control.pending.cancel {
                    match kind {
                        Signal::Gap(lapses) => {
                            control.pending.gap = true;
                            control.pending.lapse = control.pending.lapse.max(lapses);
                        }
                        Signal::Lapse(count) => {
                            control.pending.lapse = control.pending.lapse.max(count);
                        }
                        Signal::Cancel => {
                            control.terminal = true;
                            control.pending = Pending {
                                cancel: true,
                                ..Pending::default()
                            };
                        }
                        Signal::Start => control.pending.start = true,
                    }
                }
                Ok(())
            } else {
                control.terminal = true;
                control.pending = Pending {
                    cancel: true,
                    ..Pending::default()
                };
                Err(RecoveryError::Exhausted)
            }
        };
        self.notify.notify_one();
        result
    }

    fn permit(
        &self,
        op: RecoveryOperation,
        version: u64,
        deadline: Instant,
    ) -> Option<PublicationPermit> {
        let mut control = lock(&self.control);
        if control.terminal || control.version != version || Instant::now() >= deadline {
            return None;
        }
        control.operation = Some(op);
        Some(PublicationPermit {
            control: Arc::clone(&self.control),
            version: control.version,
            operation: op,
            deadline,
        })
    }

    fn disarm(&self, op: RecoveryOperation) {
        let mut control = lock(&self.control);
        if control.operation == Some(op) {
            control.operation = None;
        }
    }

    fn force_close(&self, terminal: bool) {
        let mut control = lock(&self.control);
        control.open = false;
        control.operation = None;
        control.terminal |= terminal;
        self.adapter.revoke_serving();
    }
}

#[derive(Clone, Copy, Debug)]
enum Signal {
    Start,
    Gap(u64),
    Lapse(u64),
    Cancel,
}

/// A generation-fenced permission to publish a rebuilt index page or swap.
/// A gap, lapse, or cancellation revokes all previously issued permits before
/// its public signal returns. A rejected closure is never invoked.
#[derive(Debug)]
pub struct PublicationPermit {
    control: Arc<Mutex<Control>>,
    version: u64,
    operation: RecoveryOperation,
    deadline: Instant,
}

impl Clone for PublicationPermit {
    fn clone(&self) -> Self {
        Self {
            control: Arc::clone(&self.control),
            version: self.version,
            operation: self.operation,
            deadline: self.deadline,
        }
    }
}

impl PublicationPermit {
    /// Runs one bounded index mutation only if this exact permit still owns
    /// the publication generation. The closure runs under the short gate lock
    /// so a concurrent public signal cannot return before it has finished.
    pub fn publish<T>(&self, apply: impl FnOnce() -> T) -> Option<T> {
        let control = lock(&self.control);
        (Instant::now() < self.deadline
            && !control.terminal
            && control.version == self.version
            && control.operation == Some(self.operation))
        .then(apply)
    }

    /// Whether this publication generation remains current.
    #[must_use]
    pub fn valid(&self) -> bool {
        let control = lock(&self.control);
        Instant::now() < self.deadline
            && !control.terminal
            && control.version == self.version
            && control.operation == Some(self.operation)
    }
}

/// Synchronously published recovery state and local serving permission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoveryStatus {
    /// Pure recovery state; lease and application authority remain separate.
    pub state: RecoveryState,
    /// The shell's recovery gate, closed before any public signal returns.
    pub may_serve: bool,
}

/// Failure opening the bounded recovery driver.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryOpenError {
    /// Invalid configuration or local identity.
    Core(RecoveryError),
    /// The configured total deadline cannot be represented by this clock.
    ClockRange,
    /// No Tokio executor is active to drive the bounded worker.
    NoRuntime,
}

/// Public handle to one per-domain recovery driver.
pub struct RecoveryHandle<A: RecoveryAdapter> {
    shared: Arc<Shared<A>>,
}

impl<A: RecoveryAdapter> std::fmt::Debug for RecoveryHandle<A> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RecoveryHandle")
            .finish_non_exhaustive()
    }
}

impl<A: RecoveryAdapter> Clone for RecoveryHandle<A> {
    fn clone(&self) -> Self {
        self.shared.handles.fetch_add(1, Ordering::AcqRel);
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<A: RecoveryAdapter> Drop for RecoveryHandle<A> {
    fn drop(&mut self) {
        if self.shared.handles.fetch_sub(1, Ordering::AcqRel) == 1 {
            let _ = self.shared.signal(Signal::Cancel);
        }
    }
}

impl<A: RecoveryAdapter> RecoveryHandle<A> {
    /// Opens a closed driver and schedules the mandatory cold origin rebuild.
    /// A unique nonzero session incarnation is required for every process life.
    ///
    /// # Errors
    /// Returns a core configuration or platform clock-range error before a
    /// worker is spawned.
    ///
    pub fn open(
        adapter: Arc<A>,
        config: RecoveryConfig,
        mode: RecoveryMode,
        me: NodeId,
        session: u64,
    ) -> Result<Self, RecoveryOpenError> {
        let engine =
            RecoveryEngine::new(config, mode, me, session).map_err(RecoveryOpenError::Core)?;
        if Instant::now()
            .checked_add(Duration::from_millis(config.total_ms))
            .is_none()
        {
            return Err(RecoveryOpenError::ClockRange);
        }
        let runtime =
            tokio::runtime::Handle::try_current().map_err(|_| RecoveryOpenError::NoRuntime)?;
        let shared = Arc::new(Shared {
            adapter,
            mode,
            control: Arc::new(Mutex::new(Control {
                version: 1,
                open: false,
                state: engine.state(),
                operation: None,
                pending: Pending {
                    start: true,
                    ..Pending::default()
                },
                terminal: false,
            })),
            handles: AtomicUsize::new(1),
            notify: Notify::new(),
        });
        shared.force_close(false);
        runtime.spawn(run(Arc::clone(&shared), engine, config));
        Ok(Self { shared })
    }

    /// Immediately revokes serving and retains a full origin-rescan obligation.
    /// A full gap dominates all coalesced lapse signals.
    ///
    /// # Errors
    /// Fails closed if the local publication fence cannot advance.
    pub fn feed_gap(&self, lapses: u64) -> Result<(), RecoveryError> {
        self.shared.signal(Signal::Gap(lapses))
    }

    /// Explicitly starts a fresh full origin recovery after `OriginOnly` or
    /// another operational intervention. It does not reopen a cancelled handle;
    /// cancellation ends the worker, so that requires a new handle/session.
    ///
    /// # Errors
    /// Rejects a cancelled handle or exhausted local publication fence.
    pub fn restart(&self) -> Result<(), RecoveryError> {
        self.shared.signal(Signal::Start)
    }

    /// Immediately revokes serving and retains the highest lapse counter.
    /// The core decides whether a cheap proof or full rescan is mandatory.
    ///
    /// # Errors
    /// Fails closed if the local publication fence cannot advance.
    pub fn lease_lapse(&self, count: u64) -> Result<(), RecoveryError> {
        self.shared.signal(Signal::Lapse(count))
    }

    /// Stops all automatic recovery and immediately revokes publication.
    ///
    /// # Errors
    /// Fails closed if the local publication fence cannot advance.
    pub fn cancel(&self) -> Result<(), RecoveryError> {
        self.shared.signal(Signal::Cancel)
    }

    /// Current synchronously published recovery gate and stage.
    #[must_use]
    pub fn status(&self) -> RecoveryStatus {
        let gate = lock(&self.shared.control);
        RecoveryStatus {
            state: gate.state,
            may_serve: gate.open,
        }
    }
}

fn logical_now(start: Instant) -> Time {
    Time(u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX))
}

struct WorkerExitGuard<A: RecoveryAdapter>(Arc<Shared<A>>);

impl<A: RecoveryAdapter> Drop for WorkerExitGuard<A> {
    fn drop(&mut self) {
        self.0.force_close(true);
    }
}

fn absolute_deadline(started: Instant, due: Time) -> Option<Instant> {
    started.checked_add(Duration::from_millis(due.0))
}

fn affirm_effect<A: RecoveryAdapter>(
    shared: &Shared<A>,
    engine: &mut RecoveryEngine,
    started: Instant,
    active_version: u64,
    op: RecoveryOperation,
    now: impl Fn() -> Instant,
) -> Vec<RecoveryEffect> {
    if !engine.accepts_operation(op) {
        return Vec::new();
    }
    let Some(due) = engine.next_deadline() else {
        return Vec::new();
    };
    let Some(deadline) = absolute_deadline(started, due) else {
        return Vec::new();
    };
    let mut control = lock(&shared.control);
    if now() >= deadline {
        drop(control);
        return engine.step(RecoveryEvent::Tick(due)).effects;
    }
    let accepted =
        !control.terminal && control.version == active_version && shared.adapter.affirm(op);
    if accepted && now() >= deadline {
        control.open = false;
        shared.adapter.revoke_serving();
        drop(control);
        return engine.step(RecoveryEvent::Tick(due)).effects;
    }
    let result = engine.step(RecoveryEvent::Affirmed { op, accepted });
    control.state = engine.state();
    control.open = accepted && control.state.recovered;
    result.effects
}

#[expect(
    clippy::too_many_lines,
    reason = "one auditable shell effect dispatch; all protocol decisions remain in the sans-IO engine"
)]
async fn run<A: RecoveryAdapter>(
    shared: Arc<Shared<A>>,
    mut engine: RecoveryEngine,
    config: RecoveryConfig,
) {
    let _exit_guard = WorkerExitGuard(Arc::clone(&shared));
    let started = Instant::now();
    let mut effects = std::collections::VecDeque::new();
    let mut active_version = 1;
    loop {
        let (pending, version) = {
            let mut control = lock(&shared.control);
            let pending = std::mem::take(&mut control.pending);
            (pending, control.version)
        };
        if pending.start || pending.gap || pending.lapse > 0 || pending.cancel {
            active_version = version;
            // External signals can arrive after an arbitrarily long ready
            // interval. Start their finite deadlines at this actual time.
            let _ = engine.step(RecoveryEvent::Tick(logical_now(started)));
            let event = if pending.cancel {
                RecoveryEvent::Cancel
            } else if pending.gap || (pending.start && pending.lapse > 0) {
                RecoveryEvent::FeedGap {
                    lapses: pending.lapse,
                }
            } else if pending.start {
                RecoveryEvent::Start
            } else {
                RecoveryEvent::LeaseLapse {
                    count: pending.lapse,
                }
            };
            effects.clear();
            effects.extend(engine.step(event).effects);
            lock(&shared.control).state = engine.state();
            if pending.cancel {
                lock(&shared.control).pending.cancel = true;
                return;
            }
        }

        if let Some(effect) = effects.pop_front() {
            let elapsed = engine.step(RecoveryEvent::Tick(logical_now(started)));
            if !elapsed.effects.is_empty() {
                effects.clear();
                effects.extend(elapsed.effects);
                lock(&shared.control).state = engine.state();
                continue;
            }
            match effect {
                RecoveryEffect::CloseGate { .. } => {
                    let Some(version) = shared.close(active_version) else {
                        continue;
                    };
                    active_version = version;
                }
                RecoveryEffect::ArmTimer(_) => {}
                RecoveryEffect::Invalidate {
                    op,
                    distrust_bodies,
                } => {
                    if !engine.accepts_operation(op) {
                        continue;
                    }
                    let Some(due) = engine
                        .next_deadline()
                        .and_then(|due| absolute_deadline(started, due))
                    else {
                        continue;
                    };
                    let Some(permit) = shared.permit(op, active_version, due) else {
                        continue;
                    };
                    let response = await_operation(
                        &shared,
                        &engine,
                        started,
                        active_version,
                        shared.adapter.invalidate(op, distrust_bodies, permit),
                    )
                    .await;
                    shared.disarm(op);
                    if let Some(result) = response {
                        effects.extend(
                            engine
                                .step(RecoveryEvent::Tick(logical_now(started)))
                                .effects,
                        );
                        if engine.accepts_operation(op) {
                            effects.extend(
                                engine
                                    .step(match result {
                                        Ok(()) => RecoveryEvent::Invalidated { op },
                                        Err(_) => RecoveryEvent::Failed { op },
                                    })
                                    .effects,
                            );
                        }
                    }
                }
                RecoveryEffect::RebuildOrigin { op } => {
                    if !engine.accepts_operation(op) {
                        continue;
                    }
                    let Some(due) = engine
                        .next_deadline()
                        .and_then(|due| absolute_deadline(started, due))
                    else {
                        continue;
                    };
                    let Some(permit) = shared.permit(op, active_version, due) else {
                        continue;
                    };
                    let response = await_operation(
                        &shared,
                        &engine,
                        started,
                        active_version,
                        shared.adapter.rebuild_origin(op, permit),
                    )
                    .await;
                    shared.disarm(op);
                    if let Some(result) = response {
                        effects.extend(
                            engine
                                .step(RecoveryEvent::Tick(logical_now(started)))
                                .effects,
                        );
                        if engine.accepts_operation(op) {
                            effects.extend(
                                engine
                                    .step(match result {
                                        Ok(()) => RecoveryEvent::Materialized { op },
                                        Err(_) => RecoveryEvent::Failed { op },
                                    })
                                    .effects,
                            );
                        }
                    }
                }
                RecoveryEffect::ObservePeers { op } => {
                    if !engine.accepts_operation(op) {
                        continue;
                    }
                    let response = await_operation(
                        &shared,
                        &engine,
                        started,
                        active_version,
                        shared.adapter.observe_peers(op, config),
                    )
                    .await;
                    if let Some(result) = response {
                        effects.extend(
                            engine
                                .step(RecoveryEvent::Tick(logical_now(started)))
                                .effects,
                        );
                        if engine.accepts_operation(op) {
                            effects.extend(
                                engine
                                    .step(match result {
                                        Ok((peers, confirmed)) => RecoveryEvent::PeersObserved {
                                            op,
                                            peers,
                                            confirmed,
                                        },
                                        Err(_) => RecoveryEvent::Failed { op },
                                    })
                                    .effects,
                            );
                        }
                    }
                }
                RecoveryEffect::WaitFrontiers { op, heads } => {
                    if !engine.accepts_operation(op) {
                        continue;
                    }
                    let response = await_operation(
                        &shared,
                        &engine,
                        started,
                        active_version,
                        shared.adapter.wait_frontiers(op, heads),
                    )
                    .await;
                    if let Some(result) = response {
                        effects.extend(
                            engine
                                .step(RecoveryEvent::Tick(logical_now(started)))
                                .effects,
                        );
                        if engine.accepts_operation(op) {
                            effects.extend(
                                engine
                                    .step(match result {
                                        Ok(()) => RecoveryEvent::FrontiersReached { op },
                                        Err(_) => RecoveryEvent::Failed { op },
                                    })
                                    .effects,
                            );
                        }
                    }
                }
                RecoveryEffect::Affirm { op } => {
                    effects.extend(affirm_effect(
                        &shared,
                        &mut engine,
                        started,
                        active_version,
                        op,
                        Instant::now,
                    ));
                }
            }
            lock(&shared.control).state = engine.state();
            continue;
        }

        let Some(due) = engine.next_deadline() else {
            shared.notify.notified().await;
            continue;
        };
        let delay = due.0.saturating_sub(logical_now(started).0);
        tokio::select! {
            () = shared.notify.notified() => {}
            () = tokio::time::sleep(Duration::from_millis(delay)) => {
                effects.extend(engine.step(RecoveryEvent::Tick(logical_now(started))).effects);
                lock(&shared.control).state = engine.state();
            }
        }
    }
}

async fn await_operation<A: RecoveryAdapter, T>(
    shared: &Arc<Shared<A>>,
    engine: &RecoveryEngine,
    started: Instant,
    active_version: u64,
    future: BoxRecoveryFuture<'_, T>,
) -> Option<T> {
    let due = engine.next_deadline()?;
    let deadline = absolute_deadline(started, due)?;
    tokio::pin!(future);
    loop {
        let delay = deadline.saturating_duration_since(Instant::now());
        tokio::select! {
            biased;
            () = shared.notify.notified() => {
                let control = lock(&shared.control);
                if control.version != active_version || control.pending.cancel {
                    return None;
                }
            }
            () = tokio::time::sleep(delay) => return None,
            value = &mut future => return Some(value),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug, Default)]
    struct Adapter(AtomicUsize);

    impl RecoveryAdapter for Adapter {
        fn revoke_serving(&self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }

        fn invalidate(
            &self,
            _op: RecoveryOperation,
            _distrust_bodies: bool,
            _permit: PublicationPermit,
        ) -> BoxRecoveryFuture<'_, Result<(), AdapterError>> {
            Box::pin(async { Ok(()) })
        }

        fn rebuild_origin(
            &self,
            _op: RecoveryOperation,
            _permit: PublicationPermit,
        ) -> BoxRecoveryFuture<'_, Result<(), AdapterError>> {
            Box::pin(async { Ok(()) })
        }

        fn observe_peers(
            &self,
            _op: RecoveryOperation,
            _limits: RecoveryConfig,
        ) -> BoxRecoveryFuture<'_, PeerObservation> {
            Box::pin(async { Err(AdapterError) })
        }

        fn wait_frontiers(
            &self,
            _op: RecoveryOperation,
            _heads: Vec<(NodeId, Mark)>,
        ) -> BoxRecoveryFuture<'_, Result<(), AdapterError>> {
            Box::pin(async { Err(AdapterError) })
        }

        fn affirm(&self, _op: RecoveryOperation) -> bool {
            true
        }
    }

    fn shared() -> Arc<Shared<Adapter>> {
        let config = RecoveryConfig {
            max_members: 2,
            max_member_bytes: 8,
            max_barrier_rounds: 2,
            total_ms: 100,
            attempt_ms: 20,
            settle_ms: 1,
            poll_ms: 1,
        };
        let state = RecoveryEngine::new(config, RecoveryMode::Unleased, NodeId::from("me"), 1)
            .unwrap()
            .state();
        Arc::new(Shared {
            adapter: Arc::new(Adapter::default()),
            mode: RecoveryMode::Unleased,
            control: Arc::new(Mutex::new(Control {
                version: 1,
                open: true,
                state,
                operation: None,
                pending: Pending::default(),
                terminal: false,
            })),
            handles: AtomicUsize::new(1),
            notify: Notify::new(),
        })
    }

    #[test]
    fn later_signal_cannot_be_adopted_by_older_close_gate() {
        let shared = shared();
        let drained_version = lock(&shared.control).version;
        shared.signal(Signal::Gap(3)).unwrap();
        assert_eq!(shared.close(drained_version), None);
        let control = lock(&shared.control);
        assert_eq!(control.version, 2);
        assert!(control.pending.gap);
        assert_eq!(control.pending.lapse, 3);
        assert!(!control.open);
        assert_eq!(shared.adapter.0.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn expired_publication_permit_refuses_page_before_worker_tick() {
        let shared = shared();
        let op = RecoveryOperation {
            session: 1,
            generation: 1,
            token: 1,
        };
        let expired = PublicationPermit {
            control: Arc::clone(&shared.control),
            version: 1,
            operation: op,
            deadline: Instant::now(),
        };
        lock(&shared.control).operation = Some(op);
        assert!(!expired.valid());
        assert_eq!(expired.publish(|| 7), None);
    }

    #[test]
    fn cancel_stays_terminal_after_worker_drains_pending_slot() {
        let shared = shared();
        shared.signal(Signal::Cancel).unwrap();
        let drained = std::mem::take(&mut lock(&shared.control).pending);
        assert!(drained.cancel);
        assert_eq!(shared.signal(Signal::Start), Err(RecoveryError::Stage));
        assert_eq!(shared.signal(Signal::Gap(1)), Err(RecoveryError::Stage));
        assert!(!lock(&shared.control).open);
    }

    #[test]
    fn unexpected_worker_exit_refuses_new_signals() {
        let shared = shared();
        shared.force_close(true);
        assert_eq!(shared.signal(Signal::Gap(1)), Err(RecoveryError::Stage));
        assert!(!lock(&shared.control).open);
    }

    #[test]
    fn close_gate_counter_exhaustion_terminally_rejects_old_work() {
        let shared = shared();
        lock(&shared.control).version = u64::MAX;
        assert_eq!(shared.close(u64::MAX), None);
        let op = RecoveryOperation {
            session: 1,
            generation: 1,
            token: 1,
        };
        assert!(
            shared
                .permit(op, u64::MAX, Instant::now() + Duration::from_secs(1))
                .is_none()
        );
        let control = lock(&shared.control);
        assert!(control.terminal && control.pending.cancel);
        assert!(!control.open);
    }

    #[test]
    fn open_without_executor_returns_typed_error() {
        let adapter = Arc::new(Adapter::default());
        let config = RecoveryConfig {
            max_members: 2,
            max_member_bytes: 8,
            max_barrier_rounds: 2,
            total_ms: 100,
            attempt_ms: 20,
            settle_ms: 1,
            poll_ms: 1,
        };
        assert_eq!(
            RecoveryHandle::open(
                adapter,
                config,
                RecoveryMode::Unleased,
                NodeId::from("me"),
                1
            )
            .err(),
            Some(RecoveryOpenError::NoRuntime)
        );
    }

    #[test]
    fn affirmation_expiring_during_callback_never_opens_gate() {
        let shared = shared();
        let config = RecoveryConfig {
            max_members: 2,
            max_member_bytes: 8,
            max_barrier_rounds: 2,
            total_ms: 100,
            attempt_ms: 20,
            settle_ms: 1,
            poll_ms: 1,
        };
        let mut engine =
            RecoveryEngine::new(config, RecoveryMode::Unleased, NodeId::from("me"), 1).unwrap();
        let start = engine.step(RecoveryEvent::Start);
        let invalidation = start
            .effects
            .iter()
            .find_map(|effect| match effect {
                RecoveryEffect::Invalidate { op, .. } => Some(*op),
                _ => None,
            })
            .unwrap();
        let rebuild = engine.step(RecoveryEvent::Invalidated { op: invalidation });
        let rebuild_op = rebuild
            .effects
            .iter()
            .find_map(|effect| match effect {
                RecoveryEffect::RebuildOrigin { op } => Some(*op),
                _ => None,
            })
            .unwrap();
        let affirm = engine.step(RecoveryEvent::Materialized { op: rebuild_op });
        let affirm_op = affirm
            .effects
            .iter()
            .find_map(|effect| match effect {
                RecoveryEffect::Affirm { op } => Some(*op),
                _ => None,
            })
            .unwrap();
        let started = Instant::now();
        let deadline = absolute_deadline(started, engine.next_deadline().unwrap()).unwrap();
        let calls = std::cell::Cell::new(0);
        let effects = affirm_effect(&shared, &mut engine, started, 1, affirm_op, || {
            let call = calls.get();
            calls.set(call + 1);
            if call == 0 {
                deadline.checked_sub(Duration::from_millis(1)).unwrap()
            } else {
                deadline
            }
        });
        assert!(calls.get() >= 2);
        assert!(!lock(&shared.control).open);
        assert!(!engine.state().recovered);
        assert!(shared.adapter.0.load(Ordering::SeqCst) >= 1);
        assert!(
            effects
                .iter()
                .any(|effect| matches!(effect, RecoveryEffect::ArmTimer(_)))
        );
    }
}
