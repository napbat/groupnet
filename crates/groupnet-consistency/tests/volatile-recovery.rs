//! In-memory driver checks for volatile coherence recovery.
#![cfg(feature = "volatile-recovery")]

use std::error::Error as _;
use std::future::ready;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use groupnet_consistency::volatile_recovery::{
    AdapterError, AdapterErrorKind, Mark, PeerObservation, PublicationPermit, RecoveryAdapter,
    RecoveryConfig, RecoveryFallback, RecoveryHandle, RecoveryMode, RecoveryOperation,
    RecoveryStage,
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
    /// A paged scan: this many pages, each published after `page_ms`.
    pages: AtomicUsize,
    page_ms: AtomicU64,
    report_progress: AtomicBool,
    /// Every failure the core consumed: its stage, class, and source text.
    failures: Mutex<Vec<(RecoveryStage, AdapterErrorKind, Option<String>)>>,
}

/// The statically dispatched adapter needs no boxing: its asynchronous
/// operations are plain `async fn`s or ready futures.
impl RecoveryAdapter for MemoryAdapter {
    fn revoke_serving(&self) {
        self.revoked.fetch_add(1, Ordering::SeqCst);
    }

    fn invalidate(
        &self,
        _op: RecoveryOperation,
        _distrust_bodies: bool,
        _permit: PublicationPermit,
    ) -> impl Future<Output = Result<(), AdapterError>> + Send {
        self.invalidations.fetch_add(1, Ordering::SeqCst);
        ready(Ok(()))
    }

    async fn rebuild_origin(
        &self,
        _op: RecoveryOperation,
        permit: PublicationPermit,
    ) -> Result<(), AdapterError> {
        self.rebuilds.fetch_add(1, Ordering::SeqCst);
        let pages = self.pages.load(Ordering::SeqCst);
        if pages > 0 {
            let page = Duration::from_millis(self.page_ms.load(Ordering::SeqCst));
            for _ in 0..pages {
                tokio::time::sleep(page).await;
                permit
                    .publish(|| self.published.fetch_add(1, Ordering::SeqCst))
                    .ok_or(AdapterErrorKind::Fenced)?;
                if self.report_progress.load(Ordering::SeqCst) {
                    permit.progress();
                }
            }
            return Ok(());
        }
        if self.fail_first.swap(false, Ordering::SeqCst) {
            *self.first_permit.lock().unwrap() = Some(permit);
            return Err(AdapterError::with_source(
                AdapterErrorKind::Unavailable,
                std::io::Error::other("origin LIST refused"),
            ));
        }
        if self.fail_all.load(Ordering::SeqCst) {
            return Err(AdapterErrorKind::Unavailable.into());
        }
        let applied = permit.publish(|| self.published.fetch_add(1, Ordering::SeqCst));
        applied.map_or(Err(AdapterErrorKind::Fenced.into()), |_| Ok(()))
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
        self.failures.lock().unwrap().push((
            stage,
            error.kind(),
            error.source().map(ToString::to_string),
        ));
    }

    fn fell_back(&self, _from: RecoveryStage, _reason: RecoveryFallback) {}
}

fn config() -> RecoveryConfig {
    RecoveryConfig {
        max_members: std::num::NonZeroUsize::new(4).unwrap(),
        max_member_bytes: std::num::NonZeroUsize::new(32).unwrap(),
        max_barrier_rounds: std::num::NonZeroU32::new(3).unwrap(),
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
    let handle = RecoveryHandle::builder(Arc::clone(&adapter), NodeId::from("me"), 1)
        .config(config())
        .mode(RecoveryMode::Unleased)
        .open()
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
    // The failed scan's cause reached the operator hook intact.
    assert_eq!(
        *adapter.failures.lock().unwrap(),
        [(
            RecoveryStage::Rebuilding,
            AdapterErrorKind::Unavailable,
            Some("origin LIST refused".to_owned()),
        )]
    );
    handle.cancel().unwrap();
}

/// Forty 50 ms pages take two seconds, four times the 500 ms episode budget
/// and twenty times the 100 ms attempt bound, yet the one scan completes: each
/// committed page renews both bounds, so nothing restarts it from zero.
#[tokio::test]
async fn progressing_scan_outlives_attempt_and_episode_bounds_in_one_pass() {
    let adapter = Arc::new(MemoryAdapter {
        pages: AtomicUsize::new(40),
        page_ms: AtomicU64::new(50),
        report_progress: AtomicBool::new(true),
        ..MemoryAdapter::default()
    });
    let started = Instant::now();
    let handle = RecoveryHandle::builder(Arc::clone(&adapter), NodeId::from("me"), 1)
        .config(config())
        .mode(RecoveryMode::Unleased)
        .open()
        .unwrap();
    eventually_within("slow progressing scan affirmed", SETTLE * 3, || {
        handle.status().may_serve
    })
    .await;
    assert!(started.elapsed() > Duration::from_millis(config().total_ms));
    assert_eq!(adapter.rebuilds.load(Ordering::SeqCst), 1, "one pass");
    assert_eq!(adapter.published.load(Ordering::SeqCst), 40);
    handle.cancel().unwrap();
}

/// The same scan without progress reports is a stall: its attempt expires,
/// its permit stops publishing, and the core retries instead of waiting.
#[tokio::test]
async fn scan_without_progress_reports_is_failed_at_its_attempt_bound() {
    let adapter = Arc::new(MemoryAdapter {
        pages: AtomicUsize::new(40),
        page_ms: AtomicU64::new(50),
        ..MemoryAdapter::default()
    });
    let handle = RecoveryHandle::builder(Arc::clone(&adapter), NodeId::from("me"), 1)
        .config(config())
        .mode(RecoveryMode::Unleased)
        .open()
        .unwrap();
    eventually_within("stalled scan retried", SETTLE, || {
        adapter.rebuilds.load(Ordering::SeqCst) >= 2
    })
    .await;
    assert!(!handle.status().may_serve);
    assert!(
        adapter.published.load(Ordering::SeqCst) < 40,
        "no attempt outlived its bound to finish the scan"
    );
    handle.cancel().unwrap();
}

#[tokio::test]
async fn public_gap_closes_read_gate_before_worker_handles_coalesced_signals() {
    let adapter = Arc::new(MemoryAdapter::default());
    let handle = RecoveryHandle::builder(Arc::clone(&adapter), NodeId::from("me"), 2)
        .config(config())
        .mode(RecoveryMode::Unleased)
        .open()
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
    let handle = RecoveryHandle::builder(Arc::clone(&adapter), NodeId::from("me"), 3)
        .config(config())
        .mode(RecoveryMode::Unleased)
        .open()
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
    let handle = RecoveryHandle::builder(Arc::clone(&adapter), NodeId::from("me"), 4)
        .config(config())
        .mode(RecoveryMode::Leased)
        .open()
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
    let handle = RecoveryHandle::builder(Arc::clone(&adapter), NodeId::from("me"), 5)
        .config(config())
        .mode(RecoveryMode::Unleased)
        .open()
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
    let handle = RecoveryHandle::builder(Arc::clone(&adapter), NodeId::from("me"), 6)
        .config(config())
        .mode(RecoveryMode::Unleased)
        .open()
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
    let handle = RecoveryHandle::builder(Arc::clone(&adapter), NodeId::from("me"), 7)
        .config(config())
        .mode(RecoveryMode::Unleased)
        .open()
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
    let handle = RecoveryHandle::builder(Arc::clone(&adapter), NodeId::from("me"), 8)
        .config(limits)
        .mode(RecoveryMode::Unleased)
        .open()
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
