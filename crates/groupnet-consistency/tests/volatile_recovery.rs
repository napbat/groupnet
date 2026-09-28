//! In-memory driver checks for volatile coherence recovery.
#![cfg(feature = "volatile-recovery")]

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use groupnet_consistency::volatile_recovery::{
    AdapterError, BoxRecoveryFuture, Mark, PeerObservation, PublicationPermit, RecoveryAdapter,
    RecoveryConfig, RecoveryHandle, RecoveryMode, RecoveryOperation,
};
use groupnet_core::NodeId;
use groupnet_testkit::cluster::eventually_within;

const SETTLE: Duration = Duration::from_secs(2);

#[derive(Debug, Default)]
struct MemoryAdapter {
    revoked: AtomicUsize,
    invalidations: AtomicUsize,
    rebuilds: AtomicUsize,
    published: AtomicUsize,
    fail_first: AtomicBool,
    fail_all: AtomicBool,
    first_permit: Mutex<Option<PublicationPermit>>,
}

impl RecoveryAdapter for MemoryAdapter {
    fn revoke_serving(&self) {
        self.revoked.fetch_add(1, Ordering::SeqCst);
    }

    fn invalidate(
        &self,
        _op: RecoveryOperation,
        _distrust_bodies: bool,
        _permit: PublicationPermit,
    ) -> BoxRecoveryFuture<'_, Result<(), AdapterError>> {
        self.invalidations.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }

    fn rebuild_origin(
        &self,
        _op: RecoveryOperation,
        permit: PublicationPermit,
    ) -> BoxRecoveryFuture<'_, Result<(), AdapterError>> {
        Box::pin(async move {
            self.rebuilds.fetch_add(1, Ordering::SeqCst);
            if self.fail_first.swap(false, Ordering::SeqCst) {
                *self.first_permit.lock().unwrap() = Some(permit);
                return Err(AdapterError);
            }
            if self.fail_all.load(Ordering::SeqCst) {
                return Err(AdapterError);
            }
            let applied = permit.publish(|| self.published.fetch_add(1, Ordering::SeqCst));
            applied.map_or(Err(AdapterError), |_| Ok(()))
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

fn config() -> RecoveryConfig {
    RecoveryConfig {
        max_members: 4,
        max_member_bytes: 32,
        max_barrier_rounds: 3,
        total_ms: 500,
        attempt_ms: 100,
        settle_ms: 5,
        poll_ms: 5,
    }
}

#[tokio::test]
async fn failed_full_scan_retries_and_old_page_cannot_publish() {
    let adapter = Arc::new(MemoryAdapter {
        fail_first: AtomicBool::new(true),
        ..MemoryAdapter::default()
    });
    let handle = RecoveryHandle::open(
        Arc::clone(&adapter),
        config(),
        RecoveryMode::Unleased,
        NodeId::from("me"),
        1,
    )
    .unwrap();
    assert!(adapter.revoked.load(Ordering::SeqCst) >= 1);
    eventually_within("full rescan retry affirmed", SETTLE, || {
        handle.status().may_serve
    })
    .await;
    assert_eq!(adapter.rebuilds.load(Ordering::SeqCst), 2);
    assert_eq!(adapter.published.load(Ordering::SeqCst), 1);
    let stale = adapter.first_permit.lock().unwrap().take().unwrap();
    assert!(
        stale
            .publish(|| adapter.published.fetch_add(1, Ordering::SeqCst))
            .is_none()
    );
    assert_eq!(adapter.published.load(Ordering::SeqCst), 1);
    handle.cancel().unwrap();
}

#[tokio::test]
async fn public_gap_closes_read_gate_before_worker_handles_coalesced_signals() {
    let adapter = Arc::new(MemoryAdapter::default());
    let handle = RecoveryHandle::open(
        Arc::clone(&adapter),
        config(),
        RecoveryMode::Unleased,
        NodeId::from("me"),
        2,
    )
    .unwrap();
    eventually_within("cold origin rescan affirmed", SETTLE, || {
        handle.status().may_serve
    })
    .await;
    for lapse in 1..=100 {
        handle.feed_gap(lapse).unwrap();
        assert!(!handle.status().may_serve);
    }
    eventually_within("coalesced gap origin rescan affirmed", SETTLE, || {
        handle.status().may_serve
    })
    .await;
    assert!(adapter.rebuilds.load(Ordering::SeqCst) >= 2);
    assert!(adapter.revoked.load(Ordering::SeqCst) >= 100);
    handle.cancel().unwrap();
    assert!(!handle.status().may_serve);
}

#[tokio::test]
async fn last_handle_drop_revokes_serving_without_waiting_for_worker_poll() {
    let adapter = Arc::new(MemoryAdapter::default());
    let handle = RecoveryHandle::open(
        Arc::clone(&adapter),
        config(),
        RecoveryMode::Unleased,
        NodeId::from("me"),
        3,
    )
    .unwrap();
    eventually_within("cold recovery ready before drop", SETTLE, || {
        handle.status().may_serve
    })
    .await;
    let clone = handle.clone();
    let before = adapter.revoked.load(Ordering::SeqCst);
    drop(handle);
    assert!(clone.status().may_serve);
    assert_eq!(adapter.revoked.load(Ordering::SeqCst), before);
    drop(clone);
    assert!(adapter.revoked.load(Ordering::SeqCst) > before);
}

#[tokio::test]
async fn duplicate_zero_and_old_lapses_do_not_close_an_affirmed_gate() {
    let adapter = Arc::new(MemoryAdapter::default());
    let handle = RecoveryHandle::open(
        Arc::clone(&adapter),
        config(),
        RecoveryMode::Leased,
        NodeId::from("me"),
        4,
    )
    .unwrap();
    eventually_within("cold leased recovery ready", SETTLE, || {
        handle.status().may_serve
    })
    .await;
    let before = adapter.revoked.load(Ordering::SeqCst);
    handle.lease_lapse(0).unwrap();
    assert!(handle.status().may_serve);
    assert_eq!(adapter.revoked.load(Ordering::SeqCst), before);

    handle.lease_lapse(2).unwrap();
    assert!(!handle.status().may_serve);
    eventually_within("lapse fallback affirmed", SETTLE, || {
        handle.status().may_serve && handle.status().state.covered_lapses == 2
    })
    .await;
    let after = adapter.revoked.load(Ordering::SeqCst);
    handle.lease_lapse(1).unwrap();
    handle.lease_lapse(2).unwrap();
    assert!(handle.status().may_serve);
    assert_eq!(adapter.revoked.load(Ordering::SeqCst), after);
}

#[tokio::test]
async fn gap_after_long_ready_idle_receives_a_fresh_total_budget() {
    let adapter = Arc::new(MemoryAdapter::default());
    let handle = RecoveryHandle::open(
        Arc::clone(&adapter),
        config(),
        RecoveryMode::Unleased,
        NodeId::from("me"),
        5,
    )
    .unwrap();
    eventually_within("initial recovery ready", SETTLE, || {
        handle.status().may_serve
    })
    .await;
    let idle_until = Instant::now() + Duration::from_millis(config().total_ms + 50);
    eventually_within("idle beyond prior total budget", SETTLE, || {
        Instant::now() >= idle_until
    })
    .await;
    handle.feed_gap(1).unwrap();
    assert!(!handle.status().may_serve);
    eventually_within("fresh gap recovered after idle", SETTLE, || {
        handle.status().may_serve
    })
    .await;
    assert!(adapter.rebuilds.load(Ordering::SeqCst) >= 2);
}

#[tokio::test]
async fn unsupported_lapse_does_not_revoke_unleased_recovery() {
    let adapter = Arc::new(MemoryAdapter::default());
    let handle = RecoveryHandle::open(
        Arc::clone(&adapter),
        config(),
        RecoveryMode::Unleased,
        NodeId::from("me"),
        6,
    )
    .unwrap();
    eventually_within("unleased recovery ready", SETTLE, || {
        handle.status().may_serve
    })
    .await;
    let before = adapter.revoked.load(Ordering::SeqCst);
    assert_eq!(
        handle.lease_lapse(1),
        Err(groupnet_consistency::volatile_recovery::RecoveryError::Stage)
    );
    assert!(handle.status().may_serve);
    assert_eq!(adapter.revoked.load(Ordering::SeqCst), before);
}

#[tokio::test(flavor = "current_thread")]
async fn prequeued_signal_notify_does_not_cancel_its_own_invalidation() {
    let adapter = Arc::new(MemoryAdapter::default());
    let handle = RecoveryHandle::open(
        Arc::clone(&adapter),
        config(),
        RecoveryMode::Unleased,
        NodeId::from("me"),
        7,
    )
    .unwrap();
    // This synchronous call runs before the worker's first poll on a
    // current-thread executor, leaving a stored Notify permit.
    handle.feed_gap(1).unwrap();
    eventually_within("prequeued gap recovered", SETTLE, || {
        handle.status().may_serve
    })
    .await;
    assert_eq!(adapter.invalidations.load(Ordering::SeqCst), 1);
    assert_eq!(adapter.rebuilds.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn origin_only_can_explicitly_restart_after_origin_recovers() {
    let adapter = Arc::new(MemoryAdapter {
        fail_all: AtomicBool::new(true),
        ..MemoryAdapter::default()
    });
    let limits = RecoveryConfig {
        total_ms: 40,
        attempt_ms: 10,
        ..config()
    };
    let handle = RecoveryHandle::open(
        Arc::clone(&adapter),
        limits,
        RecoveryMode::Unleased,
        NodeId::from("me"),
        8,
    )
    .unwrap();
    eventually_within("origin-only after finite failed scans", SETTLE, || {
        handle.status().state.stage
            == groupnet_consistency::volatile_recovery::RecoveryStage::OriginOnly
    })
    .await;
    assert!(!handle.status().may_serve);
    adapter.fail_all.store(false, Ordering::SeqCst);
    handle.restart().unwrap();
    eventually_within("origin recovery after explicit restart", SETTLE, || {
        handle.status().may_serve
    })
    .await;
    handle.cancel().unwrap();
    assert_eq!(
        handle.restart(),
        Err(groupnet_consistency::volatile_recovery::RecoveryError::Stage)
    );
}
