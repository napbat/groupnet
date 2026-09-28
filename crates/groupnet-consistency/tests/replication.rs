//! Source-backed replay over a real Group handle and in-memory adapters.

#![cfg(feature = "replication")]

use std::io;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use groupnet_consistency::replication::{
    AdapterFailure, ApplicationAdapter, CatchUp, Checkpoint, CheckpointLimit, InstallPermit,
    Limits, Materialized, OpenError, ReadVerdict, Replication, RevocationPermit, ScanLimit,
    SourceAdapter, SourceBatch, TailLimit,
};
use groupnet_core::replication::{
    BoundComparison, Comparison, Cursor, ProofId, Scope, SourceHistory, SourceProof, Stage, Stream,
};
use groupnet_testkit::cluster::{MemCluster, eventually_within};
use tokio::sync::Notify;

const SETTLE: Duration = Duration::from_secs(3);

fn scope(partition: &str) -> Scope {
    Scope {
        stream: Stream {
            group: "stores".into(),
            topic: "records".into(),
            kind: "v1".into(),
        },
        partition: partition.into(),
    }
}

fn cursor(scope: &Scope, position: u64) -> Cursor {
    Cursor {
        scope: scope.clone(),
        history: SourceHistory {
            source: "mem-cas".into(),
            generation: 1,
        },
        position: position.to_le_bytes().to_vec(),
    }
}

#[derive(Clone, Debug)]
struct MemSource {
    head: Arc<AtomicU64>,
    scans: Arc<AtomicU64>,
    tail_calls: Arc<AtomicU64>,
    tail_completed: Arc<AtomicU64>,
    hold_tails: Arc<AtomicBool>,
    held_partition: Arc<Mutex<Option<String>>>,
    tail_release: Arc<Notify>,
}

impl MemSource {
    fn new() -> Self {
        Self {
            head: Arc::new(AtomicU64::new(0)),
            scans: Arc::new(AtomicU64::new(0)),
            tail_calls: Arc::new(AtomicU64::new(0)),
            tail_completed: Arc::new(AtomicU64::new(0)),
            hold_tails: Arc::new(AtomicBool::new(false)),
            held_partition: Arc::new(Mutex::new(None)),
            tail_release: Arc::new(Notify::new()),
        }
    }

    fn commit(&self, through: u64) {
        self.head.store(through, Ordering::Release);
    }
}

#[expect(
    clippy::unused_async_trait_impl,
    reason = "the in-memory scan completes synchronously while exercising the production async adapter contract"
)]
impl SourceAdapter for MemSource {
    type Position = u64;
    type Batch = u64;
    type Error = io::Error;

    fn cursor(&self, scope: &Scope, position: &u64) -> Result<Cursor, AdapterFailure<io::Error>> {
        Ok(cursor(scope, *position))
    }

    fn position(&self, cursor: &Cursor) -> Result<u64, AdapterFailure<io::Error>> {
        let raw: [u8; 8] = cursor.position.as_slice().try_into().map_err(|_| {
            AdapterFailure::Terminal(io::Error::new(io::ErrorKind::InvalidData, "cursor length"))
        })?;
        Ok(u64::from_le_bytes(raw))
    }

    fn compare(
        &self,
        left: &Cursor,
        right: &Cursor,
        proof: &SourceProof,
    ) -> Result<BoundComparison, AdapterFailure<io::Error>> {
        let order = if left.scope != right.scope || left.history != right.history {
            Comparison::Incomparable
        } else {
            match self.position(left)?.cmp(&self.position(right)?) {
                std::cmp::Ordering::Less => Comparison::Before,
                std::cmp::Ordering::Equal => Comparison::Equal,
                std::cmp::Ordering::Greater => Comparison::After,
            }
        };
        Ok(BoundComparison {
            left: left.clone(),
            right: right.clone(),
            proof: proof.id.clone(),
            order,
        })
    }

    async fn tail(
        &self,
        scope: Scope,
        _from: Option<u64>,
        _limit: TailLimit,
    ) -> Result<SourceProof, AdapterFailure<io::Error>> {
        self.tail_calls.fetch_add(1, Ordering::AcqRel);
        let held_here = self
            .held_partition
            .lock()
            .expect("partition lock")
            .as_deref()
            == Some(scope.partition.as_str());
        if self.hold_tails.load(Ordering::Acquire) || held_here {
            self.tail_release.notified().await;
        }
        self.tail_completed.fetch_add(1, Ordering::AcqRel);
        let head = self.head.load(Ordering::Acquire);
        Ok(SourceProof {
            id: ProofId(head.to_le_bytes().to_vec()),
            head: cursor(&scope, head),
            retained_from: cursor(&scope, 0),
            read_authority: true,
        })
    }

    async fn scan_after(
        &self,
        _scope: Scope,
        from: u64,
        proof: SourceProof,
        limit: ScanLimit,
    ) -> Result<SourceBatch<u64, u64>, AdapterFailure<io::Error>> {
        self.scans.fetch_add(1, Ordering::Relaxed);
        let head = self.position(&proof.head)?;
        let through = head.min(from.saturating_add(limit.events as u64));
        let events = usize::try_from(through - from).expect("bounded by limit");
        Ok(SourceBatch {
            from,
            through,
            proof: proof.id,
            certificate: vec![1],
            events,
            bytes: events.min(limit.bytes),
            native: through,
        })
    }
}

#[derive(Clone, Debug)]
struct MemApp {
    state: Arc<Mutex<(u64, u64)>>,
    durable: bool,
    serving: Arc<AtomicBool>,
    checkpoint_available: Arc<AtomicBool>,
}

impl MemApp {
    fn new(durable: bool) -> Self {
        Self {
            state: Arc::new(Mutex::new((0, 0))),
            durable,
            serving: Arc::new(AtomicBool::new(true)),
            checkpoint_available: Arc::new(AtomicBool::new(true)),
        }
    }

    fn live(&self) -> u64 {
        self.state.lock().expect("state lock").0
    }
}

#[expect(
    clippy::unused_async_trait_impl,
    reason = "the fixture uses synchronous atomic installs behind the async application adapter surface"
)]
impl ApplicationAdapter<u64, u64> for MemApp {
    type Error = io::Error;
    type Recovery = u64;

    async fn load_checkpoint(
        &self,
        _scope: Scope,
        limit: CheckpointLimit,
    ) -> Result<Option<Checkpoint<u64, u64>>, AdapterFailure<io::Error>> {
        assert!(limit.bytes >= 8);
        if !self.checkpoint_available.load(Ordering::Acquire) {
            return Ok(None);
        }
        let durable = self.state.lock().expect("state lock").1;
        Ok(Some(Checkpoint {
            position: durable,
            native: durable,
            bytes: 8,
        }))
    }

    async fn install_checkpoint(
        &self,
        _scope: Scope,
        checkpoint: Checkpoint<u64, u64>,
        permit: InstallPermit,
    ) -> Result<Materialized<u64>, AdapterFailure<io::Error>> {
        permit
            .commit_sync(checkpoint.position, true, || {
                let mut state = self.state.lock().expect("state lock");
                state.0 = checkpoint.native;
                state.1 = checkpoint.position;
                Ok::<(), io::Error>(())
            })
            .map_err(AdapterFailure::Terminal)?
            .ok_or_else(|| AdapterFailure::Retryable(io::Error::other("stale checkpoint")))
    }

    async fn revoke_serving(
        &self,
        _scope: Scope,
        permit: RevocationPermit,
    ) -> Result<(), AdapterFailure<io::Error>> {
        permit
            .revoke_sync(|| Ok::<(), io::Error>(()))
            .map_err(AdapterFailure::Terminal)?
            .ok_or_else(|| AdapterFailure::Retryable(io::Error::other("stale revoke")))
    }

    async fn apply(
        &self,
        _scope: Scope,
        _from: u64,
        through: u64,
        native: u64,
        permit: InstallPermit,
    ) -> Result<Materialized<u64>, AdapterFailure<io::Error>> {
        assert_eq!(through, native);
        let receipt = permit.commit_sync(through, self.durable, || {
            let mut state = self.state.lock().expect("state lock");
            state.0 = through;
            if self.durable {
                state.1 = through;
            }
            Ok::<(), io::Error>(())
        });
        receipt
            .map_err(AdapterFailure::Terminal)?
            .ok_or_else(|| AdapterFailure::Retryable(io::Error::other("stale install")))
    }

    fn may_serve(&self, _scope: &Scope, through: &u64) -> bool {
        self.serving.load(Ordering::Acquire) && self.live() >= *through
    }
}

#[derive(Clone, Debug)]
struct SlowApp {
    inner: MemApp,
    entered: Arc<AtomicBool>,
    release: Arc<Notify>,
    captured: Arc<Mutex<Option<InstallPermit>>>,
}

#[derive(Clone, Debug)]
struct DetachedRevokeApp {
    inner: MemApp,
    delayed: Arc<Mutex<Option<RevocationPermit>>>,
}

impl ApplicationAdapter<u64, u64> for DetachedRevokeApp {
    type Error = io::Error;
    type Recovery = u64;

    async fn load_checkpoint(
        &self,
        scope: Scope,
        limit: CheckpointLimit,
    ) -> Result<Option<Checkpoint<u64, u64>>, AdapterFailure<io::Error>> {
        self.inner.load_checkpoint(scope, limit).await
    }

    async fn install_checkpoint(
        &self,
        scope: Scope,
        checkpoint: Checkpoint<u64, u64>,
        permit: InstallPermit,
    ) -> Result<Materialized<u64>, AdapterFailure<io::Error>> {
        self.inner
            .install_checkpoint(scope, checkpoint, permit)
            .await
    }

    async fn revoke_serving(
        &self,
        _scope: Scope,
        permit: RevocationPermit,
    ) -> Result<(), AdapterFailure<io::Error>> {
        *self.delayed.lock().expect("delayed revoke lock") = Some(permit);
        std::future::pending::<()>().await;
        Ok(())
    }

    async fn apply(
        &self,
        scope: Scope,
        from: u64,
        through: u64,
        native: u64,
        permit: InstallPermit,
    ) -> Result<Materialized<u64>, AdapterFailure<io::Error>> {
        self.inner.apply(scope, from, through, native, permit).await
    }

    fn may_serve(&self, scope: &Scope, through: &u64) -> bool {
        self.inner.may_serve(scope, through)
    }
}

impl ApplicationAdapter<u64, u64> for SlowApp {
    type Error = io::Error;
    type Recovery = u64;

    async fn load_checkpoint(
        &self,
        scope: Scope,
        limit: CheckpointLimit,
    ) -> Result<Option<Checkpoint<u64, u64>>, AdapterFailure<io::Error>> {
        self.inner.load_checkpoint(scope, limit).await
    }

    async fn install_checkpoint(
        &self,
        scope: Scope,
        checkpoint: Checkpoint<u64, u64>,
        permit: InstallPermit,
    ) -> Result<Materialized<u64>, AdapterFailure<io::Error>> {
        self.inner
            .install_checkpoint(scope, checkpoint, permit)
            .await
    }

    async fn revoke_serving(
        &self,
        scope: Scope,
        permit: RevocationPermit,
    ) -> Result<(), AdapterFailure<io::Error>> {
        self.inner.revoke_serving(scope, permit).await
    }

    async fn apply(
        &self,
        scope: Scope,
        from: u64,
        through: u64,
        native: u64,
        permit: InstallPermit,
    ) -> Result<Materialized<u64>, AdapterFailure<io::Error>> {
        *self.captured.lock().expect("captured permit lock") = Some(permit.clone());
        self.entered.store(true, Ordering::Release);
        self.release.notified().await;
        self.inner.apply(scope, from, through, native, permit).await
    }

    fn may_serve(&self, scope: &Scope, through: &u64) -> bool {
        self.inner.may_serve(scope, through)
    }
}

fn limits() -> Limits {
    let mut limits = Limits::default();
    limits.core.tail_check_ms = 30;
    limits.core.attempt_timeout_ms = 500;
    limits.core.retry_ms = 20;
    limits.core.max_batch_events = 2;
    limits.core.max_batch_bytes = 8;
    limits.max_inflight_bytes = 8;
    limits
}

#[tokio::test]
async fn missed_notification_replays_from_source_timer_and_native_floor_waits_for_head() {
    let cluster = MemCluster::builder(&["reader-a"]).group("stores").spawn();
    let source = MemSource::new();
    let app = MemApp::new(true);
    let manager = Replication::new(
        cluster.groups[0].clone(),
        source.clone(),
        app.clone(),
        limits(),
    )
    .expect("valid limits");
    let handle = manager
        .open(scope("one"), NonZeroU64::new(1).expect("nonzero"))
        .expect("open scope");
    handle.set_authority(true).expect("authority command");
    eventually_within("initial replica ready", SETTLE, || {
        matches!(handle.read_decision(None, true), ReadVerdict::Serve(0))
    })
    .await;

    let future = handle.catch_up(3, Instant::now() + SETTLE);
    tokio::pin!(future);
    assert!(
        tokio::time::timeout(Duration::from_millis(40), &mut future)
            .await
            .is_err()
    );
    source.commit(3);
    assert!(matches!(future.await, CatchUp::Ready(3)));
    assert_eq!(app.live(), 3);
    assert!(source.scans.load(Ordering::Acquire) >= 2);
}

#[tokio::test]
async fn volatile_materialization_is_not_a_restart_checkpoint() {
    let cluster = MemCluster::builder(&["reader-b"]).group("stores").spawn();
    let source = MemSource::new();
    source.commit(1);
    let app = MemApp::new(false);
    let manager = Replication::new(
        cluster.groups[0].clone(),
        source.clone(),
        app.clone(),
        limits(),
    )
    .expect("valid limits");
    let handle = manager
        .open(scope("one"), NonZeroU64::new(2).expect("nonzero"))
        .expect("open scope");
    handle.set_authority(true).expect("authority command");
    assert!(matches!(
        handle.catch_up(1, Instant::now() + SETTLE).await,
        CatchUp::Ready(1)
    ));
    let status = handle.status().expect("status");
    assert_eq!(status.materialized, Some(1));
    assert_eq!(status.checkpoint, Some(0));
    assert_eq!(app.live(), 1);

    drop(manager);
    source.hold_tails.store(true, Ordering::Release);
    let prior_tail_calls = source.tail_calls.load(Ordering::Acquire);
    let restarted = Replication::new(cluster.groups[0].clone(), source.clone(), app, limits())
        .expect("valid restart limits");
    let resumed = restarted
        .open(scope("one"), NonZeroU64::new(22).expect("new incarnation"))
        .expect("restart scope");
    eventually_within("restart loaded durable cursor", SETTLE, || {
        resumed
            .status()
            .is_ok_and(|state| state.materialized == Some(0))
    })
    .await;
    eventually_within("restart tail blocked", SETTLE, || {
        source.tail_calls.load(Ordering::Acquire) > prior_tail_calls
    })
    .await;
    source.hold_tails.store(false, Ordering::Release);
    source.tail_release.notify_waiters();
    resumed.set_authority(true).expect("restart authority");
    assert!(matches!(
        resumed.catch_up(1, Instant::now() + SETTLE).await,
        CatchUp::Ready(1)
    ));
}

#[tokio::test]
async fn recovered_checkpoint_at_head_installs_live_state_before_serving() {
    let cluster = MemCluster::builder(&["reader-recovered"])
        .group("stores")
        .spawn();
    let source = MemSource::new();
    source.commit(2);
    let app = MemApp::new(true);
    *app.state.lock().expect("state lock") = (0, 2);
    let manager = Replication::new(
        cluster.groups[0].clone(),
        source.clone(),
        app.clone(),
        limits(),
    )
    .expect("valid limits");
    let handle = manager
        .open(scope("one"), NonZeroU64::new(16).expect("nonzero"))
        .expect("open scope");
    handle.set_authority(true).expect("authority command");
    eventually_within("recovered state ready", SETTLE, || {
        matches!(handle.read_decision(None, true), ReadVerdict::Serve(2))
    })
    .await;
    assert_eq!(app.live(), 2);
    assert_eq!(source.scans.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn absent_checkpoint_reports_snapshot_needed_after_source_proof() {
    let cluster = MemCluster::builder(&["reader-missing"])
        .group("stores")
        .spawn();
    let app = MemApp::new(true);
    app.checkpoint_available.store(false, Ordering::Release);
    let manager = Replication::new(cluster.groups[0].clone(), MemSource::new(), app, limits())
        .expect("valid limits");
    let handle = manager
        .open(scope("one"), NonZeroU64::new(15).expect("nonzero"))
        .expect("open scope");
    handle.set_authority(true).expect("authority command");
    assert!(matches!(
        handle.catch_up(0, Instant::now() + SETTLE).await,
        CatchUp::NeedsSnapshot
    ));
}

#[tokio::test]
async fn authority_revocation_closes_ready_read_gate_synchronously() {
    let cluster = MemCluster::builder(&["reader-c"]).group("stores").spawn();
    let manager = Replication::new(
        cluster.groups[0].clone(),
        MemSource::new(),
        MemApp::new(true),
        limits(),
    )
    .expect("valid limits");
    let handle = manager
        .open(scope("one"), NonZeroU64::new(3).expect("nonzero"))
        .expect("open scope");
    handle.set_authority(true).expect("authority command");
    eventually_within("ready before revocation", SETTLE, || {
        matches!(handle.read_decision(None, true), ReadVerdict::Serve(0))
    })
    .await;
    handle.set_authority(false).expect("revoke command");
    assert!(matches!(
        handle.read_decision(None, true),
        ReadVerdict::Fallback(_)
    ));
}

#[tokio::test]
async fn manager_drop_closes_a_ready_handle() {
    let cluster = MemCluster::builder(&["reader-drop"])
        .group("stores")
        .spawn();
    let manager = Replication::new(
        cluster.groups[0].clone(),
        MemSource::new(),
        MemApp::new(true),
        limits(),
    )
    .expect("valid limits");
    let handle = manager
        .open(scope("one"), NonZeroU64::new(18).expect("nonzero"))
        .expect("open scope");
    handle.set_authority(true).expect("authority command");
    eventually_within("ready before drop", SETTLE, || {
        matches!(handle.read_decision(None, true), ReadVerdict::Serve(0))
    })
    .await;
    drop(manager);
    assert!(matches!(
        handle.read_decision(None, true),
        ReadVerdict::Fallback(_)
    ));
}

#[tokio::test]
async fn materialized_floor_respects_application_read_policy() {
    let cluster = MemCluster::builder(&["reader-policy"])
        .group("stores")
        .spawn();
    let source = MemSource::new();
    let app = MemApp::new(true);
    let manager = Replication::new(cluster.groups[0].clone(), source, app.clone(), limits())
        .expect("valid limits");
    let handle = manager
        .open(scope("one"), NonZeroU64::new(6).expect("nonzero"))
        .expect("open scope");
    handle.set_authority(true).expect("authority command");
    eventually_within("ready before policy change", SETTLE, || {
        matches!(handle.read_decision(None, true), ReadVerdict::Serve(0))
    })
    .await;
    app.serving.store(false, Ordering::Release);
    assert!(matches!(
        handle.read_decision(None, true),
        ReadVerdict::Fallback(_)
    ));
    assert!(matches!(
        handle.catch_up(0, Instant::now() + SETTLE).await,
        CatchUp::ReadPolicyBlocked
    ));
}

#[tokio::test]
async fn read_gate_expires_while_tail_driver_is_stalled() {
    let cluster = MemCluster::builder(&["reader-e"]).group("stores").spawn();
    let source = MemSource::new();
    let manager = Replication::new(
        cluster.groups[0].clone(),
        source.clone(),
        MemApp::new(true),
        limits(),
    )
    .expect("valid limits");
    let handle = manager
        .open(scope("one"), NonZeroU64::new(5).expect("nonzero"))
        .expect("open scope");
    handle.set_authority(true).expect("authority command");
    eventually_within("initially ready", SETTLE, || {
        matches!(handle.read_decision(None, true), ReadVerdict::Serve(0))
    })
    .await;
    let prior = source.tail_calls.load(Ordering::Acquire);
    let completed_before = source.tail_completed.load(Ordering::Acquire);
    source.hold_tails.store(true, Ordering::Release);
    handle.hint();
    eventually_within("next tail entered", SETTLE, || {
        source.tail_calls.load(Ordering::Acquire) > prior
    })
    .await;
    eventually_within("read freshness expired", SETTLE, || {
        matches!(handle.read_decision(None, true), ReadVerdict::Fallback(_))
    })
    .await;
    source.tail_release.notify_waiters();
    eventually_within("aged tail handled", SETTLE, || {
        source.tail_completed.load(Ordering::Acquire) > completed_before
            && handle
                .status()
                .is_ok_and(|status| status.stage == Stage::Ready)
    })
    .await;
    // The delayed proof is aged from request start even after core settles.
    assert!(matches!(
        handle.read_decision(None, true),
        ReadVerdict::Fallback(_)
    ));
}

#[tokio::test]
async fn cancellation_fences_a_blocked_native_install() {
    let cluster = MemCluster::builder(&["reader-d"]).group("stores").spawn();
    let source = MemSource::new();
    source.commit(1);
    let inner = MemApp::new(true);
    let app = SlowApp {
        inner: inner.clone(),
        entered: Arc::new(AtomicBool::new(false)),
        release: Arc::new(Notify::new()),
        captured: Arc::new(Mutex::new(None)),
    };
    let manager = Replication::new(cluster.groups[0].clone(), source, app.clone(), limits())
        .expect("valid limits");
    let handle = manager
        .open(scope("one"), NonZeroU64::new(4).expect("nonzero"))
        .expect("open scope");
    handle.set_authority(true).expect("authority command");
    eventually_within("apply entered", SETTLE, || {
        app.entered.load(Ordering::Acquire)
    })
    .await;

    let cancellation = handle.cancel(Instant::now() + SETTLE);
    tokio::pin!(cancellation);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut cancellation)
            .await
            .is_err()
    );
    assert!(matches!(
        handle.read_decision(None, true),
        ReadVerdict::Fallback(_)
    ));
    app.release.notify_one();
    assert!(matches!(cancellation.await, CatchUp::Cancelled));
    assert_eq!(inner.live(), 0, "old install must be rejected");
    assert_eq!(handle.status().expect("status").checkpoint, Some(0));
}

#[tokio::test]
async fn expired_operation_permit_cannot_publish_a_detached_late_install() {
    let cluster = MemCluster::builder(&["reader-deadline"])
        .group("stores")
        .spawn();
    let source = MemSource::new();
    source.commit(1);
    let inner = MemApp::new(true);
    let app = SlowApp {
        inner: inner.clone(),
        entered: Arc::new(AtomicBool::new(false)),
        release: Arc::new(Notify::new()),
        captured: Arc::new(Mutex::new(None)),
    };
    let mut bounded = limits();
    bounded.core.attempt_timeout_ms = 80;
    let manager = Replication::new(cluster.groups[0].clone(), source, app.clone(), bounded)
        .expect("valid limits");
    let handle = manager
        .open(scope("one"), NonZeroU64::new(17).expect("nonzero"))
        .expect("open scope");
    handle.set_authority(true).expect("authority command");
    eventually_within("slow apply captured permit", SETTLE, || {
        app.captured.lock().expect("captured permit lock").is_some()
    })
    .await;
    let stale = app
        .captured
        .lock()
        .expect("captured permit lock")
        .take()
        .expect("permit");
    eventually_within("operation deadline elapsed", SETTLE, || {
        Instant::now() >= stale.deadline()
    })
    .await;
    assert!(
        stale
            .commit_sync(1, true, || {
                *inner.state.lock().expect("state lock") = (1, 1);
                Ok::<(), io::Error>(())
            })
            .expect("guarded install")
            .is_none()
    );
    assert_eq!(inner.live(), 0);
    assert!(matches!(
        handle.read_decision(Some(&1), true),
        ReadVerdict::Fallback(_)
    ));
}

#[tokio::test]
async fn slow_scope_cannot_hold_global_admission_forever() {
    let cluster = MemCluster::builder(&["reader-f"]).group("stores").spawn();
    let source = MemSource::new();
    *source.held_partition.lock().expect("partition lock") = Some("slow".into());
    let mut bounded = limits();
    bounded.core.attempt_timeout_ms = 80;
    bounded.core.max_retries = 10;
    bounded.max_parallel_ops = 2;
    let manager = Replication::new(
        cluster.groups[0].clone(),
        source.clone(),
        MemApp::new(true),
        bounded,
    )
    .expect("valid limits");
    let slow = manager
        .open(scope("slow"), NonZeroU64::new(7).expect("nonzero"))
        .expect("slow scope");
    slow.set_authority(true).expect("slow authority");
    eventually_within("slow source entered", SETTLE, || {
        source.tail_calls.load(Ordering::Acquire) > 0
    })
    .await;
    let fast = manager
        .open(scope("fast"), NonZeroU64::new(8).expect("nonzero"))
        .expect("fast scope");
    fast.set_authority(true).expect("fast authority");
    eventually_within("fast scope progresses", SETTLE, || {
        matches!(fast.read_decision(None, true), ReadVerdict::Serve(0))
    })
    .await;
}

#[tokio::test]
async fn concurrent_native_floors_keep_the_highest_target_across_replay_windows() {
    let cluster = MemCluster::builder(&["reader-floors"])
        .group("stores")
        .spawn();
    let source = MemSource::new();
    source.hold_tails.store(true, Ordering::Release);
    let manager = Replication::new(
        cluster.groups[0].clone(),
        source.clone(),
        MemApp::new(true),
        limits(),
    )
    .expect("valid limits");
    let handle = manager
        .open(scope("one"), NonZeroU64::new(9).expect("nonzero"))
        .expect("open scope");
    handle.set_authority(true).expect("authority command");
    eventually_within("initial tail blocked", SETTLE, || {
        source.tail_calls.load(Ordering::Acquire) > 0
    })
    .await;
    let higher = handle.catch_up(4, Instant::now() + SETTLE);
    let lower = handle.catch_up(2, Instant::now() + SETTLE);
    tokio::pin!(higher, lower);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), async {
            tokio::join!(&mut higher, &mut lower)
        })
        .await
        .is_err()
    );
    source.commit(4);
    source.hold_tails.store(false, Ordering::Release);
    source.tail_release.notify_waiters();
    let (high, low) = tokio::join!(higher, lower);
    assert!(matches!(high, CatchUp::Ready(4)));
    assert!(matches!(low, CatchUp::Ready(position) if position >= 2));
}

#[tokio::test]
async fn bounded_scope_registry_reports_backpressure_and_reclaims_capacity() {
    let cluster = MemCluster::builder(&["reader-capacity"])
        .group("stores")
        .spawn();
    let mut bounded = limits();
    bounded.max_scopes = 1;
    let manager = Replication::new(
        cluster.groups[0].clone(),
        MemSource::new(),
        MemApp::new(true),
        bounded,
    )
    .expect("valid limits");
    let first_scope = scope("one");
    let first = manager
        .open(first_scope.clone(), NonZeroU64::new(10).expect("nonzero"))
        .expect("first scope");
    assert!(matches!(
        manager.open(scope("two"), NonZeroU64::new(11).expect("nonzero")),
        Err(OpenError::Backpressured)
    ));
    manager.close(&first_scope);
    assert!(matches!(
        first.catch_up(0, Instant::now() + SETTLE).await,
        CatchUp::Cancelled
    ));
    manager
        .open(scope("two"), NonZeroU64::new(12).expect("nonzero"))
        .expect("capacity reclaimed");
}

#[tokio::test]
async fn closed_sessions_delayed_revoke_cannot_disable_a_reopened_scope() {
    let cluster = MemCluster::builder(&["reader-reopen"])
        .group("stores")
        .spawn();
    let app = DetachedRevokeApp {
        inner: MemApp::new(true),
        delayed: Arc::new(Mutex::new(None)),
    };
    let mut spaced = limits();
    spaced.core.tail_check_ms = 500;
    let manager = Replication::new(
        cluster.groups[0].clone(),
        MemSource::new(),
        app.clone(),
        spaced,
    )
    .expect("valid limits");
    let key = scope("one");
    let old = manager
        .open(key.clone(), NonZeroU64::new(13).expect("nonzero"))
        .expect("old session");
    old.set_authority(true).expect("old authority");
    eventually_within("old session ready", SETTLE, || {
        matches!(old.read_decision(None, true), ReadVerdict::Serve(0))
    })
    .await;
    old.set_authority(false).expect("revoke old authority");
    eventually_within("old revoke detached", SETTLE, || {
        app.delayed.lock().expect("delayed revoke lock").is_some()
    })
    .await;
    let stale = app
        .delayed
        .lock()
        .expect("delayed revoke lock")
        .take()
        .expect("old permit");
    manager.close(&key);
    let current = manager
        .open(key, NonZeroU64::new(14).expect("new incarnation"))
        .expect("reopened session");
    current.set_authority(true).expect("new authority");
    eventually_within("new session ready", SETTLE, || {
        matches!(current.read_decision(None, true), ReadVerdict::Serve(0))
    })
    .await;
    assert_eq!(
        stale
            .revoke_sync(|| {
                app.inner.serving.store(false, Ordering::Release);
                Ok::<(), io::Error>(())
            })
            .expect("revoke callback"),
        None
    );
    assert!(matches!(
        current.read_decision(None, true),
        ReadVerdict::Serve(0)
    ));
}

#[tokio::test]
async fn delayed_old_close_cannot_remove_reopened_scope() {
    let cluster = MemCluster::builder(&["reader-conditional-close"])
        .group("stores")
        .spawn();
    let manager = Replication::new(
        cluster.groups[0].clone(),
        MemSource::new(),
        MemApp::new(true),
        limits(),
    )
    .expect("valid manager");
    let key = scope("one");
    let old_id = NonZeroU64::new(30).expect("nonzero");
    let new_id = NonZeroU64::new(31).expect("nonzero");
    let old = manager.open(key.clone(), old_id).expect("old session");
    assert!(manager.close_if(&key, old_id));
    assert!(matches!(
        old.catch_up(0, Instant::now() + SETTLE).await,
        CatchUp::Cancelled
    ));
    let current = manager.open(key.clone(), new_id).expect("replacement");
    assert!(!manager.close_if(&key, old_id));
    current.set_authority(true).expect("current authority");
    eventually_within("replacement survives old close", SETTLE, || {
        matches!(current.read_decision(None, true), ReadVerdict::Serve(0))
    })
    .await;
    assert!(manager.close_if(&key, new_id));
    assert!(!manager.close_if(&key, new_id));
}
