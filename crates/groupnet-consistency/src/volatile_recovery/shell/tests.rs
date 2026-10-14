use super::*;
use crate::volatile_recovery::AdapterErrorKind;
use groupnet_core::Time;
use groupnet_testkit::cluster::eventually_within;
use std::error::Error as _;
use std::future::ready;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

const SETTLE: Duration = Duration::from_secs(2);

#[derive(Debug, Default)]
struct Adapter {
    revokes: AtomicUsize,
    origin_attempts: AtomicUsize,
    fail_origin: AtomicBool,
    /// Stage, class, and source text of each failure the core consumed.
    failures: Mutex<Vec<(RecoveryStage, AdapterErrorKind, Option<String>)>>,
}

#[derive(Debug)]
struct OriginOutage;

impl std::fmt::Display for OriginOutage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("origin outage")
    }
}

impl std::error::Error for OriginOutage {}

impl RecoveryAdapter for Adapter {
    fn revoke_serving(&self) {
        self.revokes.fetch_add(1, Ordering::SeqCst);
    }

    fn invalidate(
        &self,
        _op: RecoveryOperation,
        _distrust_bodies: bool,
        _permit: PublicationPermit,
    ) -> impl Future<Output = Result<(), AdapterError>> + Send {
        ready(Ok(()))
    }

    fn rebuild_origin(
        &self,
        _op: RecoveryOperation,
        _permit: PublicationPermit,
    ) -> impl Future<Output = Result<(), AdapterError>> + Send {
        self.origin_attempts.fetch_add(1, Ordering::SeqCst);
        ready(if self.fail_origin.load(Ordering::SeqCst) {
            Err(AdapterError::with_source(
                AdapterErrorKind::Unavailable,
                OriginOutage,
            ))
        } else {
            Ok(())
        })
    }

    fn observe_peers(
        &self,
        _op: RecoveryOperation,
        _limits: RecoveryConfig,
    ) -> impl Future<Output = PeerObservation> + Send {
        ready(Err(AdapterErrorKind::Unsupported.into()))
    }

    fn wait_frontiers(
        &self,
        _op: RecoveryOperation,
        _heads: Vec<(NodeId, Mark)>,
    ) -> impl Future<Output = Result<(), AdapterError>> + Send {
        ready(Err(AdapterErrorKind::Unsupported.into()))
    }

    fn affirm(&self, _op: RecoveryOperation) -> bool {
        true
    }

    fn operation_failed(&self, stage: RecoveryStage, error: AdapterError) {
        lock(&self.failures).push((stage, error.kind(), error.source().map(ToString::to_string)));
    }

    fn fell_back(&self, _from: RecoveryStage, _reason: RecoveryFallback) {}
}

fn shared() -> Arc<Shared<Adapter>> {
    let config = RecoveryConfig {
        max_members: std::num::NonZeroUsize::new(2).unwrap(),
        max_member_bytes: std::num::NonZeroUsize::new(8).unwrap(),
        max_barrier_rounds: std::num::NonZeroU32::new(2).unwrap(),
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
            deadline: None,
            progress: 0,
            pending: Pending::default(),
            terminal: false,
        })),
        handles: AtomicUsize::new(1),
        notify: Arc::new(Notify::new()),
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
    assert_eq!(shared.adapter.revokes.load(Ordering::SeqCst), 1);
}

#[test]
fn expired_publication_permit_refuses_page_before_worker_tick() {
    let shared = shared();
    let op = RecoveryOperation {
        session: 1,
        generation: 1,
        token: 1,
    };
    let expired = shared
        .permit(op, 1, Instant::now() + Duration::from_secs(1))
        .expect("current operation");
    lock(&shared.control).deadline = Some(Instant::now());
    assert!(!expired.valid());
    assert_eq!(expired.publish(|| 7), None);
    assert!(
        !expired.progress(),
        "an expired operation cannot report progress"
    );
}

#[test]
fn progress_renews_only_the_current_operation_and_never_a_restriction() {
    let shared = shared();
    let op = RecoveryOperation {
        session: 1,
        generation: 1,
        token: 1,
    };
    let permit = shared
        .permit(op, 1, Instant::now() + Duration::from_secs(1))
        .expect("current operation");
    assert!(permit.progress());
    assert!(permit.progress());
    assert_eq!(shared.progress(op, 1), Some(2));
    assert_eq!(permit.progress_reports(), Some(2));

    let child = permit.restricted_to(Instant::now());
    let renewed = Instant::now() + Duration::from_secs(60);
    shared.extend(op, 1, renewed);
    assert_eq!(permit.deadline(), Some(renewed));
    assert!(permit.publish(|| ()).is_some());
    assert!(
        child.deadline().is_none(),
        "a narrowed child stays narrowed"
    );
    assert!(!child.progress());

    shared.signal(Signal::Gap(1)).unwrap();
    assert!(!permit.progress(), "a fenced operation discards its report");
    assert_eq!(permit.progress_reports(), None);
    shared.extend(op, 1, renewed + Duration::from_secs(60));
    assert!(!permit.valid(), "no renewal revives a fenced permit");
}

#[test]
fn ready_recapture_uses_a_fresh_finite_guard_and_generation_change_revokes_it() {
    let shared = shared();
    {
        let mut control = lock(&shared.control);
        control.state.stage = groupnet_core::volatile_recovery::RecoveryStage::Ready;
        control.state.recovered = true;
        control.state.generation = 1;
    }
    let old_op = RecoveryOperation {
        session: 1,
        generation: 1,
        token: 1,
    };
    let old = shared
        .permit(old_op, 1, Instant::now() + Duration::from_secs(1))
        .expect("current operation");
    shared.disarm(old_op);
    assert!(old.publish(|| ()).is_none());
    let guard = old
        .ready_capture(Instant::now() + Duration::from_secs(1))
        .expect("same Ready generation may recapture a healthy index");
    assert_eq!(guard.capture(|generation| generation), Some(1));
    assert_eq!(guard.capture(|generation| generation + 1), Some(2));
    assert!(old.ready_capture(Instant::now()).is_none());
    shared.signal(Signal::Gap(1)).unwrap();
    assert!(guard.capture(|_| 3).is_none());
    assert!(
        shared
            .ready_capture(Instant::now() + Duration::from_secs(1))
            .is_none()
    );

    // Only a newly affirmed outer owner can mint the replacement. The old
    // generation's guard stays revoked even after the read gate reopens.
    {
        let mut control = lock(&shared.control);
        control.state.generation = 2;
        control.state.stage = groupnet_core::volatile_recovery::RecoveryStage::Ready;
        control.state.recovered = true;
        control.open = true;
    }
    let fresh = shared
        .ready_capture(Instant::now() + Duration::from_secs(1))
        .expect("newly affirmed owner may issue a finite capture guard");
    assert!(guard.capture(|_| ()).is_none());
    assert_eq!(fresh.capture(|generation| generation), Some(2));
    assert!(
        fresh
            .restricted_to(Instant::now())
            .capture(|_| ())
            .is_none()
    );
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
        max_members: std::num::NonZeroUsize::new(2).unwrap(),
        max_member_bytes: std::num::NonZeroUsize::new(8).unwrap(),
        max_barrier_rounds: std::num::NonZeroU32::new(2).unwrap(),
        total_ms: 100,
        attempt_ms: 20,
        settle_ms: 1,
        poll_ms: 1,
    };
    assert_eq!(
        RecoveryHandle::builder(adapter, NodeId::from("me"), 1)
            .config(config)
            .mode(RecoveryMode::Unleased)
            .open()
            .err(),
        Some(RecoveryOpenError::NoRuntime)
    );
}

#[test]
fn the_builder_starts_unleased_with_the_default_limits() {
    let builder = RecoveryHandle::builder(Arc::new(Adapter::default()), NodeId::from("me"), 1);
    assert_eq!(builder.mode, RecoveryMode::Unleased);
    assert_eq!(
        builder.config,
        crate::volatile_recovery::defaults::recovery()
    );
    assert!(builder.rearm.is_none() && builder.bootstrap.is_none());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let handle = runtime
        .block_on(async { builder.open() })
        .expect("the defaults open");
    assert!(!handle.status().may_serve, "a driver opens closed");
}

#[test]
fn rejected_core_config_is_the_open_error_source() {
    let adapter = Arc::new(Adapter::default());
    let config = RecoveryConfig {
        max_members: std::num::NonZeroUsize::new(2).unwrap(),
        max_member_bytes: std::num::NonZeroUsize::new(8).unwrap(),
        max_barrier_rounds: std::num::NonZeroU32::new(2).unwrap(),
        total_ms: 0,
        attempt_ms: 20,
        settle_ms: 1,
        poll_ms: 1,
    };
    let error = RecoveryHandle::builder(adapter, NodeId::from("me"), 1)
        .config(config)
        .mode(RecoveryMode::Unleased)
        .open()
        .expect_err("zero total budget is rejected");
    let RecoveryOpenError::Core(core) = error else {
        panic!("expected the core's rejection, got {error:?}");
    };
    assert_eq!(error.to_string(), "recovery core rejected the open");
    assert_eq!(
        error
            .source()
            .and_then(|source| source.downcast_ref::<RecoveryError>()),
        Some(&core)
    );
}

#[test]
fn affirmation_expiring_during_callback_never_opens_gate() {
    let shared = shared();
    let config = RecoveryConfig {
        max_members: std::num::NonZeroUsize::new(2).unwrap(),
        max_member_bytes: std::num::NonZeroUsize::new(8).unwrap(),
        max_barrier_rounds: std::num::NonZeroU32::new(2).unwrap(),
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
    assert!(shared.adapter.revokes.load(Ordering::SeqCst) >= 1);
    assert!(
        effects
            .iter()
            .any(|effect| matches!(effect, RecoveryEffect::ArmTimer(_)))
    );
}

#[test]
fn coalesced_restart_precedes_gap_and_retains_its_lapse_counter() {
    assert_eq!(
        pending_event(Pending {
            start: true,
            gap: true,
            lapse: 9,
            cancel: false,
        }),
        RecoveryEvent::StartWithLapses { lapses: 9 }
    );
    assert_eq!(
        pending_event(Pending {
            start: true,
            gap: true,
            lapse: 9,
            cancel: true,
        }),
        RecoveryEvent::Cancel
    );
}

#[test]
fn no_op_signal_retains_a_newly_due_rearm_invalidation() {
    let mut engine = RecoveryEngine::new(
        RecoveryConfig {
            max_members: std::num::NonZeroUsize::new(2).unwrap(),
            max_member_bytes: std::num::NonZeroUsize::new(8).unwrap(),
            max_barrier_rounds: std::num::NonZeroU32::new(2).unwrap(),
            total_ms: 20,
            attempt_ms: 5,
            settle_ms: 1,
            poll_ms: 1,
        },
        RecoveryMode::Leased,
        NodeId::from("me"),
        1,
    )
    .unwrap()
    .with_rearm(RecoveryRearm {
        initial_ms: 5,
        max_ms: 10,
    })
    .unwrap();
    engine.step(RecoveryEvent::Start);
    engine.step(RecoveryEvent::Tick(Time(20)));
    let tick = engine.step(RecoveryEvent::Tick(Time(25)));
    let no_op = engine.step(RecoveryEvent::LeaseLapse { count: 0 });
    let queued = signal_effects(tick, no_op);
    assert!(
        queued
            .iter()
            .any(|effect| matches!(effect, RecoveryEffect::Invalidate { .. }))
    );
}

#[test]
fn unrepresentable_later_deadline_terminally_closes_worker() {
    let shared = shared();
    assert_eq!(terminal_deadline(&shared, None), None);
    let control = lock(&shared.control);
    assert!(control.terminal && control.pending.cancel);
    assert!(!control.open);
    assert_eq!(shared.adapter.revokes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancel_or_last_drop_during_cooldown_prevents_another_origin_attempt() {
    let config = RecoveryConfig {
        max_members: std::num::NonZeroUsize::new(2).unwrap(),
        max_member_bytes: std::num::NonZeroUsize::new(8).unwrap(),
        max_barrier_rounds: std::num::NonZeroU32::new(2).unwrap(),
        total_ms: 30,
        attempt_ms: 10,
        settle_ms: 1,
        poll_ms: 2,
    };
    for cancel in [true, false] {
        let adapter = Arc::new(Adapter::default());
        adapter.fail_origin.store(true, Ordering::SeqCst);
        let handle = RecoveryHandle::builder(
            Arc::clone(&adapter),
            NodeId::from("me"),
            100 + u64::from(cancel),
        )
        .config(config)
        .mode(RecoveryMode::Unleased)
        .rearm(RecoveryRearm {
            initial_ms: 80,
            max_ms: 80,
        })
        .open()
        .unwrap();
        eventually_within("failed episode enters cooldown", SETTLE, || {
            handle.status().state.stage
                == groupnet_core::volatile_recovery::RecoveryStage::OriginOnly
        })
        .await;
        let before = adapter.origin_attempts.load(Ordering::SeqCst);
        if cancel {
            handle.cancel().unwrap();
            eventually_within("cancel becomes terminal", SETTLE, || {
                handle.status().state.stage
                    == groupnet_core::volatile_recovery::RecoveryStage::Cancelled
            })
            .await;
        }
        drop(handle);
        let after_cooldown = Instant::now() + Duration::from_millis(120);
        eventually_within("closed worker makes no new origin attempt", SETTLE, || {
            Instant::now() >= after_cooldown
                && adapter.origin_attempts.load(Ordering::SeqCst) == before
        })
        .await;
    }
}

#[tokio::test]
async fn optional_rearm_recovers_after_a_long_origin_outage_without_a_restart_signal() {
    let adapter = Arc::new(Adapter::default());
    adapter.fail_origin.store(true, Ordering::SeqCst);
    let config = RecoveryConfig {
        max_members: std::num::NonZeroUsize::new(2).unwrap(),
        max_member_bytes: std::num::NonZeroUsize::new(8).unwrap(),
        max_barrier_rounds: std::num::NonZeroU32::new(2).unwrap(),
        total_ms: 80,
        attempt_ms: 15,
        settle_ms: 1,
        poll_ms: 5,
    };
    let handle = RecoveryHandle::builder(Arc::clone(&adapter), NodeId::from("me"), 11)
        .config(config)
        .mode(RecoveryMode::Unleased)
        .rearm(RecoveryRearm {
            initial_ms: 40,
            max_ms: 80,
        })
        .open()
        .unwrap();
    eventually_within("the first finite episode exhausts", SETTLE, || {
        handle.status().state.stage == groupnet_core::volatile_recovery::RecoveryStage::OriginOnly
    })
    .await;
    assert!(!handle.status().may_serve);
    assert!(adapter.origin_attempts.load(Ordering::SeqCst) >= 2);
    assert!(lock(&adapter.failures).contains(&(
        RecoveryStage::Rebuilding,
        AdapterErrorKind::Unavailable,
        Some("origin outage".to_owned()),
    )));

    adapter.fail_origin.store(false, Ordering::SeqCst);
    eventually_within("automatic rearm reaches Ready", SETTLE, || {
        handle.status().may_serve
    })
    .await;
    assert_eq!(
        handle.status().state.stage,
        groupnet_core::volatile_recovery::RecoveryStage::Ready
    );
    let before_cancel = adapter.origin_attempts.load(Ordering::SeqCst);
    handle.cancel().unwrap();
    assert!(!handle.status().may_serve);
    eventually_within("cancel reaches the worker", SETTLE, || {
        handle.status().state.stage == groupnet_core::volatile_recovery::RecoveryStage::Cancelled
    })
    .await;
    assert_eq!(
        adapter.origin_attempts.load(Ordering::SeqCst),
        before_cancel
    );
}
