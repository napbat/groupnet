//! Optional idle polling over the real async shell and an in-memory source.

#![cfg(feature = "replication")]

use std::io;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use groupnet_consistency::replication::{
    AdapterFailure, ApplicationAdapter, CatchUp, Checkpoint, CheckpointLimit, InstallPermit,
    Limits, Materialized, ReadVerdict, Replication, RevocationPermit, ScanLimit, SourceAdapter,
    SourceBatch, TailLimit,
};
use groupnet_core::replication::{
    BoundComparison, Comparison, Cursor, IdlePolicy, ProofId, Scope, SourceHistory, SourceProof,
    Stage, Stream,
};
use groupnet_testkit::cluster::{MemCluster, eventually_within};
use tokio::sync::Notify;

const SETTLE: Duration = Duration::from_secs(3);

fn scope() -> Scope {
    Scope {
        stream: Stream {
            group: "stores".into(),
            topic: "idle".into(),
            kind: "v1".into(),
        },
        partition: "p".into(),
    }
}

fn cursor(scope: &Scope, position: u64) -> Cursor {
    Cursor {
        scope: scope.clone(),
        history: SourceHistory {
            source: "memory".into(),
            generation: 1,
        },
        position: position.to_le_bytes().to_vec(),
    }
}

fn position(cursor: &Cursor) -> Result<u64, AdapterFailure<io::Error>> {
    let bytes: [u8; 8] = cursor.position.as_slice().try_into().map_err(|_| {
        AdapterFailure::Terminal(io::Error::new(io::ErrorKind::InvalidData, "cursor size"))
    })?;
    Ok(u64::from_le_bytes(bytes))
}

#[derive(Clone, Debug, Default)]
struct Source {
    head: Arc<AtomicU64>,
    calls: Arc<Mutex<Vec<Instant>>>,
    hold: Arc<AtomicBool>,
    release: Arc<Notify>,
}

impl Source {
    fn call_times(&self) -> Vec<Instant> {
        self.calls.lock().expect("call times").clone()
    }
}

#[expect(
    clippy::unused_async_trait_impl,
    reason = "the memory adapter completes synchronously while exercising the async source contract"
)]
impl SourceAdapter for Source {
    type Position = u64;
    type Batch = u64;
    type Error = io::Error;

    fn cursor(&self, scope: &Scope, at: &u64) -> Result<Cursor, AdapterFailure<io::Error>> {
        Ok(cursor(scope, *at))
    }

    fn position(&self, at: &Cursor) -> Result<u64, AdapterFailure<io::Error>> {
        position(at)
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
            match position(left)?.cmp(&position(right)?) {
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
        self.calls.lock().expect("call times").push(Instant::now());
        if self.hold.load(Ordering::Acquire) {
            self.release.notified().await;
        }
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
        let through = position(&proof.head)?.min(from.saturating_add(limit.events as u64));
        let events = usize::try_from(through - from).expect("bounded by scan limit");
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

#[derive(Clone, Debug, Default)]
struct App {
    state: Arc<AtomicU64>,
}

#[expect(
    clippy::unused_async_trait_impl,
    reason = "the memory sink installs synchronously behind the async adapter surface"
)]
impl ApplicationAdapter<u64, u64> for App {
    type Error = io::Error;
    type Recovery = u64;

    async fn load_checkpoint(
        &self,
        _scope: Scope,
        _limit: CheckpointLimit,
    ) -> Result<Option<Checkpoint<u64, u64>>, AdapterFailure<io::Error>> {
        let at = self.state.load(Ordering::Acquire);
        Ok(Some(Checkpoint {
            position: at,
            native: at,
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
                self.state.store(checkpoint.native, Ordering::Release);
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
        permit
            .commit_sync(through, true, || {
                self.state.store(through, Ordering::Release);
                Ok::<(), io::Error>(())
            })
            .map_err(AdapterFailure::Terminal)?
            .ok_or_else(|| AdapterFailure::Retryable(io::Error::other("stale apply")))
    }

    fn may_serve(&self, _scope: &Scope, through: &u64) -> bool {
        self.state.load(Ordering::Acquire) >= *through
    }
}

fn limits() -> Limits {
    let mut limits = Limits::default();
    limits.core.tail_check_ms = 40;
    limits.core.attempt_timeout_ms = 500;
    limits.core.retry_ms = 20;
    limits.core.max_batch_events = 2;
    limits.core.max_batch_bytes = 8;
    limits.max_inflight_bytes = 8;
    limits.core.idle = Some(IdlePolicy {
        unchanged_checks: 1,
        max_interval_ms: 160,
        jitter_ms: 0,
    });
    limits
}

#[tokio::test]
async fn quiet_scope_backs_off_but_discovers_a_commit_without_a_hint() {
    let cluster = MemCluster::builder(&["idle-reader"])
        .group("stores")
        .spawn()
        .await;
    let source = Source::default();
    let app = App::default();
    let manager = Replication::new(
        cluster.groups[0].clone(),
        source.clone(),
        app.clone(),
        limits(),
    )
    .expect("valid limits");
    let handle = manager
        .open(scope(), NonZeroU64::new(1).expect("nonzero"))
        .expect("open");
    handle.set_authority(true).expect("authority");
    eventually_within("idle source checks", SETTLE, || {
        source.call_times().len() >= 6
    })
    .await;
    let times = source.call_times();
    assert!(times[4].duration_since(times[3]) >= Duration::from_millis(130));
    assert!(times[5].duration_since(times[4]) >= Duration::from_millis(130));
    source.head.store(1, Ordering::Release);
    eventually_within("missed-notification replay", SETTLE, || {
        app.state.load(Ordering::Acquire) == 1
    })
    .await;
    eventually_within("serve after materialization", SETTLE, || {
        matches!(handle.read_decision(None, true), ReadVerdict::Serve(1))
    })
    .await;
    let before_burst = source.call_times().len();
    for _ in 0..40 {
        let _ = handle.read_decision(None, true);
    }
    eventually_within("coalesced read activity", SETTLE, || {
        source.call_times().len() > before_burst
    })
    .await;
    assert!(
        source.call_times().len() <= before_burst + 2,
        "a read burst cannot issue one source query per read"
    );
    manager.close(&scope());
    let calls = source.call_times().len();
    for _ in 0..20 {
        assert!(matches!(
            handle.read_decision(None, true),
            ReadVerdict::Fallback(_)
        ));
    }
    assert_eq!(source.call_times().len(), calls);
}

#[tokio::test]
async fn blocked_idle_check_expires_read_gate_and_activity_revalidates() {
    let cluster = MemCluster::builder(&["blocked-reader"])
        .group("stores")
        .spawn()
        .await;
    let source = Source::default();
    let manager = Replication::new(
        cluster.groups[0].clone(),
        source.clone(),
        App::default(),
        limits(),
    )
    .expect("valid limits");
    let handle = manager
        .open(scope(), NonZeroU64::new(2).expect("nonzero"))
        .expect("open");
    handle.set_authority(true).expect("authority");
    eventually_within("initial ready", SETTLE, || {
        handle
            .status()
            .is_ok_and(|state| state.stage == Stage::Ready)
    })
    .await;
    source.hold.store(true, Ordering::Release);
    eventually_within("blocked idle tail", SETTLE, || {
        source.call_times().len() >= 2
    })
    .await;
    eventually_within("proof expires while tail is blocked", SETTLE, || {
        matches!(handle.read_decision(None, true), ReadVerdict::Fallback(_))
    })
    .await;
    let blocked_call_count = source.call_times().len();
    for _ in 0..20 {
        let _ = handle.read_decision(None, true);
    }
    assert_eq!(
        source.call_times().len(),
        blocked_call_count,
        "read burst must coalesce"
    );
    source.hold.store(false, Ordering::Release);
    source.release.notify_one();
    eventually_within("revalidated after blocked source", SETTLE, || {
        matches!(handle.read_decision(None, true), ReadVerdict::Serve(0))
    })
    .await;
}

#[tokio::test]
async fn read_burst_shortens_an_idle_poll_without_a_query_per_read() {
    let cluster = MemCluster::builder(&["active-reader"])
        .group("stores")
        .spawn()
        .await;
    let source = Source::default();
    let manager = Replication::new(
        cluster.groups[0].clone(),
        source.clone(),
        App::default(),
        limits(),
    )
    .expect("valid limits");
    let handle = manager
        .open(scope(), NonZeroU64::new(3).expect("nonzero"))
        .expect("open");
    handle.set_authority(true).expect("authority");
    eventually_within("backed-off poll", SETTLE, || source.call_times().len() >= 4).await;
    eventually_within("backed-off proof installed", SETTLE, || {
        handle
            .status()
            .is_ok_and(|state| state.stage == Stage::Ready)
    })
    .await;
    let before = source.call_times().len();
    for _ in 0..40 {
        let _ = handle.read_decision(None, true);
    }
    eventually_within("activity poll", SETTLE, || {
        source.call_times().len() > before
    })
    .await;
    let calls = source.call_times();
    assert!(
        calls[before].duration_since(calls[before - 1]) < Duration::from_millis(120),
        "read activity should use the hot cadence, not the idle maximum"
    );
    assert!(calls.len() <= before + 2);
}

#[tokio::test]
async fn already_covered_floor_waits_use_the_fresh_local_proof() {
    let cluster = MemCluster::builder(&["floor-reader"])
        .group("stores")
        .spawn()
        .await;
    let source = Source::default();
    let manager = Replication::new(
        cluster.groups[0].clone(),
        source.clone(),
        App::default(),
        limits(),
    )
    .expect("valid limits");
    let handle = manager
        .open(scope(), NonZeroU64::new(4).expect("nonzero"))
        .expect("open");
    handle.set_authority(true).expect("authority");
    eventually_within("fresh local proof", SETTLE, || {
        matches!(handle.read_decision(None, true), ReadVerdict::Serve(0))
    })
    .await;
    let before = source.call_times().len();
    for _ in 0..40 {
        assert!(matches!(
            handle.catch_up(0, Instant::now() + SETTLE).await,
            CatchUp::Ready(0)
        ));
    }
    assert!(
        source.call_times().len() <= before + 2,
        "covered floors must not issue one source query per waiter"
    );
}

#[tokio::test]
async fn restored_authority_rechecks_even_when_false_command_was_backpressured() {
    let cluster = MemCluster::builder(&["restore-reader"])
        .group("stores")
        .spawn()
        .await;
    let source = Source::default();
    let mut config = limits();
    config.queue_depth = 1;
    let manager = Replication::new(
        cluster.groups[0].clone(),
        source.clone(),
        App::default(),
        config,
    )
    .expect("valid limits");
    let handle = manager
        .open(scope(), NonZeroU64::new(5).expect("nonzero"))
        .expect("open");
    handle.set_authority(true).expect("authority");
    eventually_within("initial serve", SETTLE, || {
        matches!(handle.read_decision(None, true), ReadVerdict::Serve(0))
    })
    .await;
    source.hold.store(true, Ordering::Release);
    let before = source.call_times().len();
    handle.hint();
    eventually_within("blocked tail", SETTLE, || {
        source.call_times().len() > before
    })
    .await;
    handle.set_authority(true).expect("fill command queue");
    assert!(matches!(
        handle.set_authority(false),
        Err(CatchUp::Backpressured)
    ));
    assert!(matches!(
        handle.read_decision(None, true),
        ReadVerdict::Fallback(_)
    ));
    assert!(matches!(
        handle.set_authority(true),
        Err(CatchUp::Backpressured)
    ));
    source.hold.store(false, Ordering::Release);
    source.release.notify_one();
    eventually_within("source rechecked after dropped false", SETTLE, || {
        source.call_times().len() >= before + 2
            && matches!(handle.read_decision(None, true), ReadVerdict::Serve(0))
    })
    .await;
}
