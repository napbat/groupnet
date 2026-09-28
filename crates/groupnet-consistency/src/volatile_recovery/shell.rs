//! Synchronous safety gate and bounded asynchronous effect driver.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use groupnet_core::NodeId;
use groupnet_core::volatile_recovery::{
    Mark, Peer, RecoveryConfig, RecoveryEffect, RecoveryEngine, RecoveryError, RecoveryEvent,
    RecoveryMode, RecoveryOperation, RecoveryRearm, RecoveryState, RecoveryStep,
};
use tokio::sync::Notify;

use super::bootstrap::driver::BootstrapDriver;

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

/// Complete exact-incarnation peer observation after private peer handoff.
pub type PeerHeadObservation = Result<
    (
        Vec<Peer>,
        Vec<groupnet_core::volatile_bootstrap::BootstrapMemberIdentity>,
    ),
    AdapterError,
>;

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

    /// Observe bounded current peer identities and native feed heads after
    /// a private peer handoff. Replay-only adapters can decline this optional
    /// path; the core then falls back to its origin recovery policy.
    fn observe_peer_heads(
        &self,
        _op: RecoveryOperation,
        _limits: RecoveryConfig,
    ) -> BoxRecoveryFuture<'_, PeerHeadObservation> {
        Box::pin(async { Err(AdapterError) })
    }

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

fn pending_event(pending: Pending) -> RecoveryEvent {
    if pending.cancel {
        RecoveryEvent::Cancel
    } else if pending.start {
        RecoveryEvent::StartWithLapses {
            lapses: pending.lapse,
        }
    } else if pending.gap {
        RecoveryEvent::FeedGap {
            lapses: pending.lapse,
        }
    } else {
        RecoveryEvent::LeaseLapse {
            count: pending.lapse,
        }
    }
}

fn signal_effects(tick: RecoveryStep, transition: RecoveryStep) -> Vec<RecoveryEffect> {
    // A no-op signal must not erase a newly due timer's recovery work.
    if transition.effects.is_empty() {
        tick.effects
    } else {
        transition.effects
    }
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
        if terminal {
            control.pending.cancel = true;
            self.notify.notify_one();
        }
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
    /// Derive a fresh, finite read-only capture guard after this exact
    /// recovery generation reached Ready. The expired build deadline is not
    /// extended: this permit can only clone an already-serving index under
    /// the current control lock, never publish recovery state.
    #[must_use]
    pub fn ready_capture(&self, deadline: Instant) -> Option<ReadyCapturePermit> {
        let control = lock(&self.control);
        (Instant::now() < deadline
            && !control.terminal
            && control.version == self.version
            && control.open
            && control.state.recovered
            && control.state.generation == self.operation.generation
            && control.operation.is_none())
        .then(|| ReadyCapturePermit {
            control: Arc::clone(&self.control),
            version: self.version,
            generation: control.state.generation,
            deadline,
        })
    }

    /// Exact outer recovery operation this permit can publish for.
    #[must_use]
    pub const fn operation(&self) -> RecoveryOperation {
        self.operation
    }

    /// Narrows this exact generation's publication deadline for a child
    /// operation. It can never extend the parent recovery permission.
    #[must_use]
    pub fn restricted_to(&self, deadline: Instant) -> Self {
        let mut restricted = self.clone();
        restricted.deadline = restricted.deadline.min(deadline);
        restricted
    }
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

/// One finite synchronous clone/ingress attachment against an already-Ready
/// recovery generation. A gap, lapse, or restart revokes it before returning.
#[derive(Clone, Debug)]
pub struct ReadyCapturePermit {
    control: Arc<Mutex<Control>>,
    version: u64,
    generation: u64,
    deadline: Instant,
}

impl ReadyCapturePermit {
    /// Runs one short application critical section only while the original
    /// Ready generation remains current. No await may occur inside `capture`.
    pub fn capture<T>(&self, capture: impl FnOnce(u64) -> T) -> Option<T> {
        let control = lock(&self.control);
        (Instant::now() < self.deadline
            && !control.terminal
            && control.version == self.version
            && control.state.generation == self.generation
            && control.open
            && control.state.recovered
            && control.operation.is_none())
        .then(|| capture(control.state.generation))
    }

    /// Whether this exact guard remains current before an off-lock encode.
    #[must_use]
    pub fn valid(&self) -> bool {
        self.capture(|_| ()).is_some()
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
        Self::open_configured(adapter, config, mode, me, session, None)
    }

    /// Opens a closed driver with capped automatic full recovery after an
    /// exhausted episode. The policy does not change any individual attempt's
    /// `RecoveryConfig::total_ms` or source authority.
    ///
    /// # Errors
    /// Rejects invalid policy bounds, platform clock range, or no executor.
    pub fn open_with_rearm(
        adapter: Arc<A>,
        config: RecoveryConfig,
        mode: RecoveryMode,
        me: NodeId,
        session: u64,
        rearm: RecoveryRearm,
    ) -> Result<Self, RecoveryOpenError> {
        Self::open_configured(adapter, config, mode, me, session, Some(rearm))
    }

    /// Opens one recovery worker with an optional source-backed peer bootstrap
    /// child. The child only supplies a private baseline; independent lease
    /// and application authority still decide whether reads may be served.
    ///
    /// # Errors
    /// Rejects invalid recovery configuration, clock range, or no executor.
    pub fn open_with_bootstrap(
        adapter: Arc<A>,
        config: RecoveryConfig,
        mode: RecoveryMode,
        me: NodeId,
        session: u64,
        bootstrap: Box<dyn BootstrapDriver>,
    ) -> Result<Self, RecoveryOpenError> {
        Self::open_inner(adapter, config, mode, me, session, None, Some(bootstrap))
    }

    /// Opens the same worker with both a peer bootstrap child and a bounded
    /// rearm policy for later exhausted recovery episodes.
    ///
    /// # Errors
    /// Rejects invalid recovery or rearm bounds, clock range, or no executor.
    pub fn open_with_bootstrap_and_rearm(
        adapter: Arc<A>,
        config: RecoveryConfig,
        mode: RecoveryMode,
        me: NodeId,
        session: u64,
        rearm: RecoveryRearm,
        bootstrap: Box<dyn BootstrapDriver>,
    ) -> Result<Self, RecoveryOpenError> {
        Self::open_inner(
            adapter,
            config,
            mode,
            me,
            session,
            Some(rearm),
            Some(bootstrap),
        )
    }

    fn open_configured(
        adapter: Arc<A>,
        config: RecoveryConfig,
        mode: RecoveryMode,
        me: NodeId,
        session: u64,
        rearm: Option<RecoveryRearm>,
    ) -> Result<Self, RecoveryOpenError> {
        Self::open_inner(adapter, config, mode, me, session, rearm, None)
    }

    fn open_inner(
        adapter: Arc<A>,
        config: RecoveryConfig,
        mode: RecoveryMode,
        me: NodeId,
        session: u64,
        rearm: Option<RecoveryRearm>,
        bootstrap: Option<Box<dyn BootstrapDriver>>,
    ) -> Result<Self, RecoveryOpenError> {
        let mut engine =
            RecoveryEngine::new(config, mode, me, session).map_err(RecoveryOpenError::Core)?;
        if let Some(policy) = rearm {
            engine = engine.with_rearm(policy).map_err(RecoveryOpenError::Core)?;
        }
        if bootstrap.is_some() {
            engine = engine.with_bootstrap().map_err(RecoveryOpenError::Core)?;
        }
        let now = Instant::now();
        if now
            .checked_add(Duration::from_millis(config.total_ms))
            .is_none()
            || rearm.is_some_and(|policy| {
                now.checked_add(Duration::from_millis(policy.max_ms))
                    .is_none()
            })
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
        runtime.spawn(run(Arc::clone(&shared), engine, config, bootstrap));
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

mod worker;
use worker::run;
#[cfg(test)]
use worker::{absolute_deadline, affirm_effect, terminal_deadline};

#[cfg(test)]
mod tests;
