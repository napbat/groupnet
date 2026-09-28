//! Native snapshot transfer, private replay, guarded cutover, and cleanup.

#![cfg(feature = "replication")]

use std::collections::BTreeMap;
use std::io;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use groupnet_consistency::replication::{
    AdapterFailure, ApplicationAdapter, CatchUp, Checkpoint, CheckpointLimit, InstallPermit,
    Limits, Materialized, NativeSnapshot, ReadVerdict, Replication, RevocationPermit, ScanLimit,
    SnapshotApplicationAdapter, SnapshotAttachment, SnapshotHold, SnapshotImage,
    SnapshotSourceAdapter, SnapshotStage, SourceAdapter, SourceBatch, TailLimit,
};
use groupnet_core::replication::{
    BoundComparison, Comparison, Cursor, HoldReceipt, Operation, ProofId, Scope, SnapshotConfig,
    SnapshotOffer, SourceHistory, SourceProof, Stream,
};
use groupnet_testkit::cluster::{MemCluster, eventually_within};
use tokio::sync::Notify;

const SETTLE: Duration = Duration::from_secs(4);

fn scope() -> Scope {
    Scope {
        stream: Stream {
            group: "snapshots".into(),
            topic: "objects".into(),
            kind: "v1".into(),
        },
        partition: "bucket".into(),
    }
}

fn live_scope() -> Scope {
    let mut live = scope();
    live.partition = "live".into();
    live
}

fn cursor(scope: &Scope, position: u64) -> Cursor {
    Cursor {
        scope: scope.clone(),
        history: SourceHistory {
            source: "origin-log".into(),
            generation: 7,
        },
        position: position.to_le_bytes().to_vec(),
    }
}

#[derive(Clone, Debug)]
enum Mutation {
    Put(String, String),
    Delete(String),
}

fn fold(rows: &mut BTreeMap<String, String>, mutation: &Mutation) {
    match mutation {
        Mutation::Put(key, value) => {
            rows.insert(key.clone(), value.clone());
        }
        Mutation::Delete(key) => {
            rows.remove(key);
        }
    }
}

#[derive(Debug, Default)]
struct Log {
    head: u64,
    retained_from: u64,
    rows: BTreeMap<String, String>,
    events: Vec<(u64, Mutation)>,
}

#[derive(Debug, Default)]
struct Pause {
    enabled: AtomicBool,
    entered: AtomicBool,
    released: AtomicBool,
    wake: Notify,
}

impl Pause {
    fn arm(&self) {
        self.enabled.store(true, Ordering::Release);
        self.released.store(false, Ordering::Release);
    }
    fn release(&self) {
        self.enabled.store(false, Ordering::Release);
        self.released.store(true, Ordering::Release);
        self.wake.notify_one();
    }
    async fn wait(&self) {
        if self.enabled.load(Ordering::Acquire) {
            self.entered.store(true, Ordering::Release);
            while !self.released.load(Ordering::Acquire) {
                self.wake.notified().await;
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
struct Source {
    log: Arc<Mutex<Log>>,
    offer_pause: Arc<Pause>,
    scan_pause: Arc<Pause>,
    release_pause: Arc<Pause>,
    max_read: Arc<AtomicUsize>,
    releases: Arc<AtomicUsize>,
    release_inflight: Arc<AtomicUsize>,
    attachments: Arc<AtomicUsize>,
}

impl Source {
    fn commit(&self, mutation: Mutation) -> u64 {
        let mut log = self.log.lock().expect("log lock");
        log.head += 1;
        let head = log.head;
        fold(&mut log.rows, &mutation);
        log.events.push((head, mutation));
        head
    }

    fn proof(&self, scope: &Scope) -> SourceProof {
        let log = self.log.lock().expect("log lock");
        SourceProof {
            id: ProofId(log.head.to_le_bytes().to_vec()),
            head: cursor(scope, log.head),
            retained_from: cursor(scope, log.retained_from),
            read_authority: true,
        }
    }
}

#[derive(Debug)]
struct Hold {
    scope: Scope,
    deadline: Instant,
}

#[derive(Debug)]
struct Attachment(Arc<AtomicUsize>);

impl Drop for Attachment {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Debug)]
struct ReleaseFlight(Arc<AtomicUsize>);

impl Drop for ReleaseFlight {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

#[expect(
    clippy::unused_async_trait_impl,
    reason = "the in-memory fixture completes some asynchronous adapter methods synchronously"
)]
impl SourceAdapter for Source {
    type Position = u64;
    type Batch = Vec<Mutation>;
    type Error = io::Error;

    fn cursor(&self, scope: &Scope, position: &u64) -> Result<Cursor, AdapterFailure<io::Error>> {
        Ok(cursor(scope, *position))
    }
    fn position(&self, cursor: &Cursor) -> Result<u64, AdapterFailure<io::Error>> {
        let raw: [u8; 8] = cursor
            .position
            .as_slice()
            .try_into()
            .map_err(|_| AdapterFailure::Terminal(io::Error::other("cursor")))?;
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
        Ok(self.proof(&scope))
    }
    async fn scan_after(
        &self,
        scope: Scope,
        from: u64,
        proof: SourceProof,
        _limit: ScanLimit,
    ) -> Result<SourceBatch<u64, Vec<Mutation>>, AdapterFailure<io::Error>> {
        if scope.partition == "bucket" {
            self.scan_pause.wait().await;
        }
        let log = self.log.lock().expect("log lock");
        if from < log.retained_from {
            return Err(AdapterFailure::Terminal(io::Error::other("retention gap")));
        }
        let (through, mutation) = log
            .events
            .iter()
            .find(|(position, _)| {
                *position > from && *position <= self.position(&proof.head).expect("head cursor")
            })
            .cloned()
            .ok_or_else(|| AdapterFailure::Terminal(io::Error::other("no next record")))?;
        Ok(SourceBatch {
            from,
            through,
            proof: proof.id,
            certificate: vec![1],
            events: 1,
            bytes: 1,
            native: vec![mutation],
        })
    }
}

fn encode(rows: &BTreeMap<String, String>) -> Vec<u8> {
    rows.iter()
        .flat_map(|(key, value)| format!("{key}={value}\n").into_bytes())
        .collect()
}

fn digest(bytes: &[u8]) -> Vec<u8> {
    vec![bytes.iter().fold(0_u8, |sum, byte| sum.wrapping_add(*byte))]
}

#[expect(
    clippy::unused_async_trait_impl,
    reason = "the in-memory fixture completes some asynchronous adapter methods synchronously"
)]
impl SnapshotSourceAdapter for Source {
    type Hold = Hold;
    type ReadHandle = Vec<u8>;
    type Attachment = Attachment;

    async fn acquire_hold(
        &self,
        scope: Scope,
        request: Operation,
        total_due: groupnet_core::Time,
        wall_deadline: Instant,
        _limit: SnapshotConfig,
    ) -> Result<SnapshotHold<Hold>, AdapterFailure<io::Error>> {
        if Instant::now() >= wall_deadline {
            return Err(AdapterFailure::Terminal(io::Error::other("expired hold")));
        }
        Ok(SnapshotHold {
            receipt: HoldReceipt {
                request,
                history: cursor(&scope, 0).history,
                retained_until: total_due,
                certificate: vec![1],
            },
            handle: Hold {
                scope,
                deadline: wall_deadline,
            },
        })
    }
    async fn offer(
        &self,
        hold: &mut Hold,
        scope: Scope,
        limit: SnapshotConfig,
    ) -> Result<SnapshotImage<Vec<u8>>, AdapterFailure<io::Error>> {
        assert_eq!(hold.scope, scope);
        let (cut, rows) = {
            let log = self.log.lock().expect("log lock");
            (log.head, log.rows.clone())
        };
        let bytes = encode(&rows);
        let proof = self.proof(&scope);
        let cut_cursor = cursor(&scope, cut);
        let offer = SnapshotOffer {
            scope,
            schema: vec![1],
            cut: cut_cursor.clone(),
            cut_to_head: self.compare(&cut_cursor, &proof.head, &proof)?,
            retained_to_cut: self.compare(&proof.retained_from, &cut_cursor, &proof)?,
            proof,
            total_bytes: bytes.len() as u64,
            chunks: u32::try_from(bytes.len().div_ceil(limit.max_chunk_bytes))
                .expect("small image"),
            digest: digest(&bytes),
            certificate: vec![1],
        };
        self.offer_pause.wait().await;
        Ok(SnapshotImage { offer, read: bytes })
    }
    async fn read_chunk(
        &self,
        read: &mut Vec<u8>,
        offset: u64,
        max_bytes: usize,
    ) -> Result<Vec<u8>, AdapterFailure<io::Error>> {
        self.max_read.fetch_max(max_bytes, Ordering::AcqRel);
        let start = usize::try_from(offset).expect("small offset");
        Ok(read[start..read.len().min(start + max_bytes)].to_vec())
    }
    async fn barrier(
        &self,
        hold: &mut Hold,
        _from: u64,
        _limit: SnapshotConfig,
    ) -> Result<SourceProof, AdapterFailure<io::Error>> {
        if Instant::now() >= hold.deadline {
            return Err(AdapterFailure::Terminal(io::Error::other("hold expired")));
        }
        Ok(self.proof(&hold.scope))
    }
    async fn attach(
        &self,
        hold: &mut Hold,
        _after: u64,
    ) -> Result<SnapshotAttachment<Attachment>, AdapterFailure<io::Error>> {
        if Instant::now() >= hold.deadline {
            return Err(AdapterFailure::Terminal(io::Error::other("hold expired")));
        }
        self.attachments.fetch_add(1, Ordering::AcqRel);
        Ok(SnapshotAttachment {
            proof: self.proof(&hold.scope),
            handle: Attachment(Arc::clone(&self.attachments)),
        })
    }
    async fn release_hold(&self, _hold: Hold) -> Result<(), AdapterFailure<io::Error>> {
        self.release_inflight.fetch_add(1, Ordering::AcqRel);
        let _flight = ReleaseFlight(Arc::clone(&self.release_inflight));
        self.release_pause.wait().await;
        self.releases.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}

#[derive(Debug)]
struct StageData {
    raw: Vec<u8>,
    rows: BTreeMap<String, String>,
    limit: usize,
    charged: usize,
    active: Arc<AtomicUsize>,
}

impl Drop for StageData {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Debug, Default)]
struct Live {
    position: u64,
    rows: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default)]
struct App {
    live: Arc<Mutex<Live>>,
    stages: Arc<AtomicUsize>,
    install_pause: Arc<Pause>,
    old_permit: Arc<Mutex<Option<InstallPermit>>>,
}

#[expect(
    clippy::unused_async_trait_impl,
    reason = "the in-memory fixture completes some asynchronous adapter methods synchronously"
)]
impl ApplicationAdapter<u64, Vec<Mutation>> for App {
    type Error = io::Error;
    type Recovery = BTreeMap<String, String>;

    async fn load_checkpoint(
        &self,
        scope: Scope,
        _limit: CheckpointLimit,
    ) -> Result<Option<Checkpoint<u64, Self::Recovery>>, AdapterFailure<io::Error>> {
        Ok((scope.partition == "live").then(|| Checkpoint {
            position: 0,
            native: BTreeMap::new(),
            bytes: 1,
        }))
    }
    async fn install_checkpoint(
        &self,
        _scope: Scope,
        checkpoint: Checkpoint<u64, Self::Recovery>,
        permit: InstallPermit,
    ) -> Result<Materialized<u64>, AdapterFailure<io::Error>> {
        *self.old_permit.lock().expect("permit lock") = Some(permit.clone());
        self.install_pause.wait().await;
        permit
            .commit_sync(checkpoint.position, true, || {
                let mut live = self.live.lock().expect("live lock");
                live.position = checkpoint.position;
                live.rows = checkpoint.native;
                Ok::<(), io::Error>(())
            })
            .map_err(AdapterFailure::Terminal)?
            .ok_or_else(|| AdapterFailure::Retryable(io::Error::other("stale install")))
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
        native: Vec<Mutation>,
        permit: InstallPermit,
    ) -> Result<Materialized<u64>, AdapterFailure<io::Error>> {
        permit
            .commit_sync(through, true, || {
                let mut live = self.live.lock().expect("live lock");
                for mutation in &native {
                    fold(&mut live.rows, mutation);
                }
                live.position = through;
                Ok::<(), io::Error>(())
            })
            .map_err(AdapterFailure::Terminal)?
            .ok_or_else(|| AdapterFailure::Retryable(io::Error::other("stale apply")))
    }
    fn may_serve(&self, _scope: &Scope, through: &u64) -> bool {
        self.live.lock().expect("live lock").position >= *through
    }
}

#[expect(
    clippy::unused_async_trait_impl,
    reason = "the in-memory fixture completes some asynchronous adapter methods synchronously"
)]
impl SnapshotApplicationAdapter<u64, Vec<Mutation>> for App {
    type Stage = StageData;

    async fn begin_stage(
        &self,
        _scope: Scope,
        _offer: &SnapshotOffer,
        limit: CheckpointLimit,
    ) -> Result<SnapshotStage<StageData>, AdapterFailure<io::Error>> {
        self.stages.fetch_add(1, Ordering::AcqRel);
        Ok(SnapshotStage {
            handle: StageData {
                raw: Vec::new(),
                rows: BTreeMap::new(),
                limit: limit.bytes,
                charged: 0,
                active: Arc::clone(&self.stages),
            },
            charged_bytes: 0,
        })
    }
    async fn write_chunk(
        &self,
        stage: &mut StageData,
        offset: u64,
        bytes: Vec<u8>,
    ) -> Result<usize, AdapterFailure<io::Error>> {
        if stage.raw.len() as u64 != offset || stage.raw.len() + bytes.len() > stage.limit {
            return Err(AdapterFailure::Terminal(io::Error::other("stage bound")));
        }
        stage.raw.extend(bytes);
        stage.charged = stage.charged.max(stage.raw.len());
        Ok(stage.charged)
    }
    async fn verify_image(
        &self,
        stage: &mut StageData,
        expected: &[u8],
    ) -> Result<usize, AdapterFailure<io::Error>> {
        if digest(&stage.raw) != expected {
            return Err(AdapterFailure::Terminal(io::Error::other("bad digest")));
        }
        let text = std::str::from_utf8(&stage.raw)
            .map_err(|_| AdapterFailure::Terminal(io::Error::other("invalid image")))?;
        let extra = text.lines().map(str::len).sum::<usize>();
        let needed = stage
            .raw
            .len()
            .checked_add(extra)
            .ok_or_else(|| AdapterFailure::Terminal(io::Error::other("stage overflow")))?;
        if needed > stage.limit {
            return Err(AdapterFailure::Terminal(io::Error::other("stage bound")));
        }
        stage.rows = text
            .lines()
            .map(|line| {
                let (key, value) = line.split_once('=').expect("fixture encoding");
                (key.to_owned(), value.to_owned())
            })
            .collect();
        stage.charged = stage.charged.max(needed);
        Ok(stage.charged)
    }
    async fn apply_private(
        &self,
        stage: &mut StageData,
        _from: u64,
        _through: u64,
        batch: Vec<Mutation>,
    ) -> Result<usize, AdapterFailure<io::Error>> {
        for mutation in &batch {
            if let Mutation::Put(key, value) = mutation {
                let added = key
                    .len()
                    .checked_add(value.len())
                    .ok_or_else(|| AdapterFailure::Terminal(io::Error::other("stage overflow")))?;
                let needed = stage
                    .charged
                    .checked_add(added)
                    .ok_or_else(|| AdapterFailure::Terminal(io::Error::other("stage overflow")))?;
                if needed > stage.limit {
                    return Err(AdapterFailure::Terminal(io::Error::other("stage bound")));
                }
                stage.charged = needed;
            }
            fold(&mut stage.rows, mutation);
        }
        Ok(stage.charged)
    }
    async fn seal_stage(
        &self,
        mut stage: StageData,
        through: u64,
    ) -> Result<Checkpoint<u64, Self::Recovery>, AdapterFailure<io::Error>> {
        Ok(Checkpoint {
            position: through,
            native: std::mem::take(&mut stage.rows),
            bytes: stage.charged,
        })
    }
}

fn limits() -> Limits {
    let mut limits = Limits::default();
    limits.core.snapshot = Some(SnapshotConfig {
        max_metadata_bytes: 512,
        max_snapshot_bytes: 64,
        max_chunks: 16,
        max_chunk_bytes: 4,
        max_candidate_bytes: 64,
        max_total_ms: 3000,
    });
    limits.core.max_batch_events = 1;
    limits.core.max_batch_bytes = 8;
    limits.core.attempt_timeout_ms = 500;
    limits.core.tail_check_ms = 100;
    limits.max_inflight_bytes = 32;
    limits.max_checkpoint_bytes = 64;
    limits.max_checkpoint_inflight_bytes = 128;
    limits.max_parallel_ops = 2;
    limits
}

#[tokio::test]
async fn concurrent_delete_and_put_replay_after_the_snapshot_cut_before_local_reads() {
    let cluster = MemCluster::builder(&["reader"]).group("snapshots").spawn();
    let source = Source::default();
    let app = App::default();
    source.commit(Mutation::Put("old".into(), "v1".into()));
    source.offer_pause.arm();
    let manager = Replication::<_, _, NativeSnapshot>::new_native(
        cluster.groups[0].clone(),
        source.clone(),
        app.clone(),
        limits(),
    )
    .expect("finite snapshot limits");
    let handle = manager
        .open(scope(), NonZeroU64::new(1).expect("nonzero"))
        .expect("scope");
    handle.set_authority(true).expect("authority");
    eventually_within("snapshot cut offered", SETTLE, || {
        source.offer_pause.entered.load(Ordering::Acquire)
    })
    .await;
    assert!(matches!(
        handle.read_decision(None, true),
        ReadVerdict::Fallback(_)
    ));
    source.commit(Mutation::Delete("old".into()));
    source.commit(Mutation::Put("new".into(), "v2".into()));
    source.offer_pause.release();
    assert!(matches!(
        handle.catch_up(3, Instant::now() + SETTLE).await,
        CatchUp::Ready(3)
    ));
    {
        let live = app.live.lock().expect("live lock");
        assert_eq!(live.position, 3);
        assert_eq!(live.rows.get("old"), None);
        assert_eq!(live.rows.get("new").map(String::as_str), Some("v2"));
    }
    assert!(matches!(
        handle.read_decision(Some(&3), true),
        ReadVerdict::Serve(3)
    ));
    eventually_within("hold release", SETTLE, || {
        source.releases.load(Ordering::Acquire) == 1
    })
    .await;
    assert_eq!(source.max_read.load(Ordering::Acquire), 4);
    assert_eq!(app.stages.load(Ordering::Acquire), 0);
    assert_eq!(
        source.attachments.load(Ordering::Acquire),
        1,
        "live continuation survives hold cleanup"
    );
    manager.close(&scope());
    eventually_within("closed worker drops live attachment", SETTLE, || {
        source.attachments.load(Ordering::Acquire) == 0
    })
    .await;
}

#[tokio::test]
async fn cancelled_snapshot_install_cannot_publish_or_hold_capacity_after_reopen() {
    let cluster = MemCluster::builder(&["reader"]).group("snapshots").spawn();
    let source = Source::default();
    let app = App::default();
    source.commit(Mutation::Put("one".into(), "v1".into()));
    app.install_pause.arm();
    let manager = Replication::<_, _, NativeSnapshot>::new_native(
        cluster.groups[0].clone(),
        source.clone(),
        app.clone(),
        limits(),
    )
    .expect("finite snapshot limits");
    let old = manager
        .open(scope(), NonZeroU64::new(2).expect("nonzero"))
        .expect("old scope");
    old.set_authority(true).expect("authority");
    eventually_within("snapshot install entered", SETTLE, || {
        app.install_pause.entered.load(Ordering::Acquire)
    })
    .await;
    let old_permit = app
        .old_permit
        .lock()
        .expect("permit lock")
        .clone()
        .expect("captured install permit");
    assert!(matches!(
        old.read_decision(None, true),
        ReadVerdict::Fallback(_)
    ));
    let _ = old.cancel(Instant::now() + Duration::from_millis(30)).await;
    manager.close(&scope());
    assert!(
        old_permit
            .commit_sync(1, true, || Ok::<(), io::Error>(()))
            .expect("infallible")
            .is_none(),
        "an old snapshot install permit must be retired before a new session"
    );
    eventually_within("private stage dropped", SETTLE, || {
        app.stages.load(Ordering::Acquire) == 0
    })
    .await;
    app.install_pause.release();
    let fresh = manager
        .open(scope(), NonZeroU64::new(3).expect("nonzero"))
        .expect("new scope");
    fresh.set_authority(true).expect("authority");
    assert!(matches!(
        fresh.catch_up(1, Instant::now() + SETTLE).await,
        CatchUp::Ready(1)
    ));
    assert_eq!(
        app.live
            .lock()
            .expect("live lock")
            .rows
            .get("one")
            .map(String::as_str),
        Some("v1")
    );
    assert_eq!(source.attachments.load(Ordering::Acquire), 1);
    let _ = fresh.cancel(Instant::now() + SETTLE).await;
    eventually_within("ready cancellation drops live attachment", SETTLE, || {
        source.attachments.load(Ordering::Acquire) == 0
    })
    .await;
}

#[tokio::test]
async fn retention_gap_after_cut_discards_stage_without_installing_or_serving() {
    let cluster = MemCluster::builder(&["reader"]).group("snapshots").spawn();
    let source = Source::default();
    let app = App::default();
    source.commit(Mutation::Put("old".into(), "v1".into()));
    source.offer_pause.arm();
    source.release_pause.arm();
    let manager = Replication::<_, _, NativeSnapshot>::new_native(
        cluster.groups[0].clone(),
        source.clone(),
        app.clone(),
        limits(),
    )
    .expect("finite snapshot limits");
    let handle = manager
        .open(scope(), NonZeroU64::new(4).expect("nonzero"))
        .expect("scope");
    handle.set_authority(true).expect("authority");
    eventually_within("snapshot cut offered", SETTLE, || {
        source.offer_pause.entered.load(Ordering::Acquire)
    })
    .await;
    source.commit(Mutation::Delete("old".into()));
    source.log.lock().expect("log lock").retained_from = 2;
    source.offer_pause.release();
    assert!(matches!(
        handle.catch_up(2, Instant::now() + SETTLE).await,
        CatchUp::Failed(_) | CatchUp::NeedsSnapshot
    ));
    assert!(matches!(
        handle.read_decision(None, true),
        ReadVerdict::Fallback(_)
    ));
    assert_eq!(app.live.lock().expect("live lock").position, 0);
    eventually_within("gap stage dropped", SETTLE, || {
        app.stages.load(Ordering::Acquire) == 0
    })
    .await;
    assert_eq!(source.attachments.load(Ordering::Acquire), 0);
    eventually_within("blocked hold release entered", SETTLE, || {
        source.release_pause.entered.load(Ordering::Acquire)
    })
    .await;
    assert_eq!(source.release_inflight.load(Ordering::Acquire), 1);
    eventually_within("expired hold release cancelled", SETTLE, || {
        source.release_inflight.load(Ordering::Acquire) == 0
    })
    .await;
    assert_eq!(source.releases.load(Ordering::Acquire), 0);

    source.log.lock().expect("log lock").retained_from = 0;
    let live = manager
        .open(live_scope(), NonZeroU64::new(7).expect("nonzero"))
        .expect("live scope");
    live.set_authority(true).expect("authority");
    assert!(matches!(
        live.catch_up(2, Instant::now() + SETTLE).await,
        CatchUp::Ready(2)
    ));
}

#[tokio::test]
async fn stalled_snapshot_stage_and_scan_leave_operation_bytes_and_checkpoint_room_for_replay() {
    let cluster = MemCluster::builder(&["reader"]).group("snapshots").spawn();
    let source = Source::default();
    let app = App::default();
    source.commit(Mutation::Put("old".into(), "v1".into()));
    source.offer_pause.arm();
    source.scan_pause.arm();
    let manager = Replication::<_, _, NativeSnapshot>::new_native(
        cluster.groups[0].clone(),
        source.clone(),
        app.clone(),
        limits(),
    )
    .expect("finite snapshot limits");
    let snapshot = manager
        .open(scope(), NonZeroU64::new(5).expect("nonzero"))
        .expect("snapshot scope");
    snapshot.set_authority(true).expect("authority");
    eventually_within("cut captured", SETTLE, || {
        source.offer_pause.entered.load(Ordering::Acquire)
    })
    .await;
    source.commit(Mutation::Put("new".into(), "v2".into()));
    source.offer_pause.release();
    eventually_within("private scan stalled with stage resident", SETTLE, || {
        source.scan_pause.entered.load(Ordering::Acquire) && app.stages.load(Ordering::Acquire) == 1
    })
    .await;
    assert!(matches!(
        snapshot.read_decision(None, true),
        ReadVerdict::Fallback(_)
    ));

    let live = manager
        .open(live_scope(), NonZeroU64::new(6).expect("nonzero"))
        .expect("live scope");
    live.set_authority(true).expect("authority");
    assert!(
        matches!(
            live.catch_up(2, Instant::now() + SETTLE).await,
            CatchUp::Ready(2)
        ),
        "a snapshot-held candidate, scan bytes, and operation cannot consume replay's reserved capacity"
    );
    source.scan_pause.release();
    assert!(matches!(
        snapshot.catch_up(2, Instant::now() + SETTLE).await,
        CatchUp::Ready(2)
    ));
    manager.close(&scope());
    manager.close(&live_scope());
}
