use super::*;
use groupnet_core::Time;
use groupnet_testkit::cluster::eventually_within;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

const SETTLE: Duration = Duration::from_secs(2);

#[derive(Debug, Default)]
struct Adapter {
    revokes: AtomicUsize,
    origin_attempts: AtomicUsize,
    fail_origin: AtomicBool,
}

impl RecoveryAdapter for Adapter {
    fn revoke_serving(&self) {
        self.revokes.fetch_add(1, Ordering::SeqCst);
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
        Box::pin(async move {
            self.origin_attempts.fetch_add(1, Ordering::SeqCst);
            if self.fail_origin.load(Ordering::SeqCst) {
                Err(AdapterError)
            } else {
                Ok(())
            }
        })
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
            max_members: 2,
            max_member_bytes: 8,
            max_barrier_rounds: 2,
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
        max_members: 2,
        max_member_bytes: 8,
        max_barrier_rounds: 2,
        total_ms: 30,
        attempt_ms: 10,
        settle_ms: 1,
        poll_ms: 2,
    };
    for cancel in [true, false] {
        let adapter = Arc::new(Adapter::default());
        adapter.fail_origin.store(true, Ordering::SeqCst);
        let handle = RecoveryHandle::open_with_rearm(
            Arc::clone(&adapter),
            config,
            RecoveryMode::Unleased,
            NodeId::from("me"),
            100 + u64::from(cancel),
            RecoveryRearm {
                initial_ms: 80,
                max_ms: 80,
            },
        )
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
        max_members: 2,
        max_member_bytes: 8,
        max_barrier_rounds: 2,
        total_ms: 80,
        attempt_ms: 15,
        settle_ms: 1,
        poll_ms: 5,
    };
    let handle = RecoveryHandle::open_with_rearm(
        Arc::clone(&adapter),
        config,
        RecoveryMode::Unleased,
        NodeId::from("me"),
        11,
        RecoveryRearm {
            initial_ms: 40,
            max_ms: 80,
        },
    )
    .unwrap();
    eventually_within("the first finite episode exhausts", SETTLE, || {
        handle.status().state.stage == groupnet_core::volatile_recovery::RecoveryStage::OriginOnly
    })
    .await;
    assert!(!handle.status().may_serve);
    assert!(adapter.origin_attempts.load(Ordering::SeqCst) >= 2);

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
