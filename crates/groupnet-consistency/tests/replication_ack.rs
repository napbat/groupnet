//! Source-certified named waits beside ordinary state-sync replay.

#![cfg(feature = "replication")]

use std::collections::BTreeSet;
use std::io;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use groupnet_consistency::replication::{
    AckEvidenceSource, AckObservation, AckSourceFailure, AckSourceFuture, AckWaitStartError,
    AdapterFailure, ApplicationAdapter, CatchUp, Checkpoint, CheckpointLimit, InstallPermit,
    Limits, Materialized, NamedAckRequest, NamedAckResult, Replication, RevocationPermit,
    ScanLimit, SourceAdapter, SourceBatch, TailLimit,
};
use groupnet_core::replication::{
    AckEvidence, AckKind, AckTarget, AckWaitLimits, AckWaitOutcome, AckWaitRequest,
    BoundComparison, CertifiedRoster, Comparison, Cursor, Operation, ProofId, RequiredSubscriber,
    Scope, SourceHistory, SourceProof, Stream,
};
use groupnet_testkit::cluster::{MemCluster, eventually_within};
use tokio::sync::Notify;

const SETTLE: Duration = Duration::from_secs(3);

fn scope(partition: &str) -> Scope {
    Scope {
        stream: Stream {
            group: "acks".into(),
            topic: "objects".into(),
            kind: "v1".into(),
        },
        partition: partition.into(),
    }
}

fn cursor(scope: &Scope, position: u64) -> Cursor {
    Cursor {
        scope: scope.clone(),
        history: SourceHistory {
            source: "native-log".into(),
            generation: 9,
        },
        position: position.to_le_bytes().to_vec(),
    }
}

#[derive(Debug, Default)]
struct Pause {
    enabled: AtomicBool,
    entered: AtomicBool,
    released: AtomicBool,
    wake: Notify,
}

#[derive(Debug)]
struct Flight(Arc<AtomicUsize>);

impl Drop for Flight {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Debug)]
struct TailFlight {
    completed: bool,
    cancelled: Arc<AtomicUsize>,
}

impl Drop for TailFlight {
    fn drop(&mut self) {
        if !self.completed {
            self.cancelled.fetch_add(1, Ordering::AcqRel);
        }
    }
}

impl Pause {
    fn arm(&self) {
        self.enabled.store(true, Ordering::Release);
        self.released.store(false, Ordering::Release);
    }
    fn release(&self) {
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
    head: Arc<AtomicU64>,
    tail_pause: Arc<Pause>,
    tail_calls: Arc<AtomicUsize>,
    tail_cancelled: Arc<AtomicUsize>,
}

impl Source {
    fn commit(&self) -> u64 {
        self.head.fetch_add(1, Ordering::AcqRel) + 1
    }
    fn proof(&self, scope: &Scope) -> SourceProof {
        let head = self.head.load(Ordering::Acquire);
        SourceProof {
            id: ProofId(head.to_le_bytes().to_vec()),
            head: cursor(scope, head),
            retained_from: cursor(scope, 0),
            read_authority: true,
        }
    }
}

#[expect(
    clippy::unused_async_trait_impl,
    reason = "the in-memory source completes bounded async adapter calls synchronously"
)]
impl SourceAdapter for Source {
    type Position = u64;
    type Batch = ();
    type Error = io::Error;

    fn cursor(&self, scope: &Scope, position: &u64) -> Result<Cursor, AdapterFailure<io::Error>> {
        Ok(cursor(scope, *position))
    }
    fn position(&self, cursor: &Cursor) -> Result<u64, AdapterFailure<io::Error>> {
        let bytes: [u8; 8] = cursor
            .position
            .as_slice()
            .try_into()
            .map_err(|_| AdapterFailure::Terminal(io::Error::other("cursor")))?;
        Ok(u64::from_le_bytes(bytes))
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
        let mut flight = TailFlight {
            completed: false,
            cancelled: Arc::clone(&self.tail_cancelled),
        };
        self.tail_pause.wait().await;
        flight.completed = true;
        Ok(self.proof(&scope))
    }
    async fn scan_after(
        &self,
        _scope: Scope,
        from: u64,
        proof: SourceProof,
        _limit: ScanLimit,
    ) -> Result<SourceBatch<u64, ()>, AdapterFailure<io::Error>> {
        let head = self.position(&proof.head)?;
        let through = from + 1;
        if through > head {
            return Err(AdapterFailure::Terminal(io::Error::other("no record")));
        }
        Ok(SourceBatch {
            from,
            through,
            proof: proof.id,
            certificate: vec![1],
            events: 1,
            bytes: 1,
            native: (),
        })
    }
}

#[derive(Clone, Debug, Default)]
struct App {
    applied: Arc<AtomicU64>,
}

#[expect(
    clippy::unused_async_trait_impl,
    reason = "the in-memory sink uses immediate guarded transactions"
)]
impl ApplicationAdapter<u64, ()> for App {
    type Error = io::Error;
    type Recovery = u64;

    async fn load_checkpoint(
        &self,
        _scope: Scope,
        _limit: CheckpointLimit,
    ) -> Result<Option<Checkpoint<u64, u64>>, AdapterFailure<io::Error>> {
        Ok(Some(Checkpoint {
            position: 0,
            native: 0,
            bytes: 1,
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
                self.applied.store(checkpoint.native, Ordering::Release);
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
        _native: (),
        permit: InstallPermit,
    ) -> Result<Materialized<u64>, AdapterFailure<io::Error>> {
        permit
            .commit_sync(through, true, || {
                self.applied.store(through, Ordering::Release);
                Ok::<(), io::Error>(())
            })
            .map_err(AdapterFailure::Terminal)?
            .ok_or_else(|| AdapterFailure::Retryable(io::Error::other("stale apply")))
    }
    fn may_serve(&self, _scope: &Scope, through: &u64) -> bool {
        self.applied.load(Ordering::Acquire) >= *through
    }
}

#[derive(Clone, Debug, Default)]
struct EvidenceSource {
    available: Arc<Mutex<BTreeSet<String>>>,
    cert_pause: Arc<Pause>,
    observe_pause: Arc<Pause>,
    bad_kind: Arc<AtomicBool>,
    bad_epoch: Arc<AtomicBool>,
    certifications: Arc<AtomicUsize>,
    cert_inflight: Arc<AtomicUsize>,
    observations: Arc<AtomicUsize>,
}

impl EvidenceSource {
    fn ack(&self, name: &str) {
        self.available
            .lock()
            .expect("evidence lock")
            .insert(name.into());
    }
}

impl AckEvidenceSource for EvidenceSource {
    fn certify<'a>(
        &'a self,
        request: &'a NamedAckRequest,
        _limits: AckWaitLimits,
    ) -> AckSourceFuture<'a, ()> {
        Box::pin(async move {
            self.certifications.fetch_add(1, Ordering::AcqRel);
            self.cert_inflight.fetch_add(1, Ordering::AcqRel);
            let _flight = Flight(Arc::clone(&self.cert_inflight));
            self.cert_pause.wait().await;
            if request.roster.certificate != b"cert" {
                return Err(AckSourceFailure::AuthorityLost);
            }
            Ok(())
        })
    }
    fn observe<'a>(
        &'a self,
        request: &'a AckWaitRequest,
        poll: Operation,
        waiting: &'a [RequiredSubscriber],
        _limits: AckWaitLimits,
    ) -> AckSourceFuture<'a, AckObservation> {
        Box::pin(async move {
            self.observations.fetch_add(1, Ordering::AcqRel);
            self.observe_pause.wait().await;
            let available = self.available.lock().expect("evidence lock");
            let Some(subscriber) = waiting
                .iter()
                .find(|member| available.contains(&member.name))
            else {
                return Ok(AckObservation::Pending);
            };
            let kind = if self.bad_kind.load(Ordering::Acquire) {
                AckKind::Materialized
            } else {
                request.kind
            };
            let mut subscriber = (*subscriber).clone();
            if self.bad_epoch.load(Ordering::Acquire) {
                subscriber.epoch = vec![99];
            }
            Ok(AckObservation::Evidence(Box::new(AckEvidence {
                op: poll,
                request_id: request.request_id.clone(),
                target: request.target.clone(),
                kind,
                roster_certificate: request.roster.certificate.clone(),
                subscriber,
            })))
        })
    }
}

fn request(scope: &Scope, id: u8) -> NamedAckRequest {
    let target = AckTarget::Intent {
        scope: scope.clone(),
        history: cursor(scope, 0).history,
        id: vec![id],
    };
    let roster = CertifiedRoster {
        target: target.clone(),
        kind: AckKind::Invalidated,
        policy_version: 1,
        certificate: b"cert".to_vec(),
        required: ["alpha", "beta"]
            .into_iter()
            .map(|name| RequiredSubscriber {
                name: name.into(),
                incarnation: 1,
                epoch: vec![1],
            })
            .collect(),
    };
    NamedAckRequest {
        request_id: vec![id],
        target,
        kind: AckKind::Invalidated,
        roster,
    }
}

fn limits() -> (Limits, AckWaitLimits) {
    let mut replay = Limits::default();
    replay.core.attempt_timeout_ms = 80;
    replay.core.tail_check_ms = 50;
    replay.max_parallel_ops = 2;
    let ack = AckWaitLimits {
        max_required: 2,
        max_identity_bytes: 64,
        max_certificate_bytes: 64,
        max_metadata_bytes: 512,
        max_wait_ms: 4000,
        poll_ms: 20,
    };
    (replay, ack)
}

fn outcome(
    result: Result<NamedAckResult, AckWaitStartError>,
) -> Result<AckWaitOutcome, AckWaitStartError> {
    result.map(|receipt| receipt.outcome)
}

#[tokio::test]
async fn two_named_acks_arrive_without_hint_while_replay_stays_ready() {
    let cluster = MemCluster::builder(&["reader"]).group("acks").spawn();
    let source = Source::default();
    let app = App::default();
    let evidence = EvidenceSource::default();
    let (limits, ack_limits) = limits();
    let manager = Replication::new(cluster.groups[0].clone(), source.clone(), app, limits)
        .expect("manager")
        .with_ack_evidence(evidence.clone(), ack_limits)
        .expect("ack capability");
    let target_scope = scope("one");
    let handle = manager
        .open(target_scope.clone(), NonZeroU64::new(1).expect("nonzero"))
        .expect("session");
    handle.set_authority(true).expect("authority");
    source.commit();
    assert_eq!(
        handle.catch_up(1, Instant::now() + SETTLE).await,
        CatchUp::Ready(1)
    );
    let waiting = {
        let handle = handle.clone();
        let request = request(&target_scope, 7);
        tokio::spawn(async move { handle.wait_named(request, Instant::now() + SETTLE).await })
    };
    eventually_within("first empty source check", SETTLE, || {
        evidence.observations.load(Ordering::Acquire) > 0
    })
    .await;
    evidence.ack("alpha");
    evidence.ack("beta");
    let receipt = waiting.await.expect("wait task").expect("certified wait");
    assert_eq!(receipt.request_id, vec![7]);
    assert_eq!(receipt.target, request(&target_scope, 7).target);
    assert_eq!(receipt.kind, AckKind::Invalidated);
    assert_eq!(receipt.outcome, AckWaitOutcome::Satisfied);
    assert!(evidence.observations.load(Ordering::Acquire) >= 3);
    assert_eq!(
        handle.catch_up(1, Instant::now() + SETTLE).await,
        CatchUp::Ready(1)
    );
    let certified_before = evidence.certifications.load(Ordering::Acquire);
    let mut empty = request(&target_scope, 14);
    empty.roster.required.clear();
    assert_eq!(
        outcome(handle.wait_named(empty, Instant::now() + SETTLE).await),
        Ok(AckWaitOutcome::Satisfied)
    );
    assert_eq!(
        evidence.certifications.load(Ordering::Acquire),
        certified_before + 1,
        "an empty roster still needs source certification"
    );
}

#[tokio::test]
async fn bad_ack_evidence_fails_only_the_wait_and_later_replay_progresses() {
    let cluster = MemCluster::builder(&["reader"]).group("acks").spawn();
    let source = Source::default();
    let app = App::default();
    let evidence = EvidenceSource::default();
    evidence.bad_kind.store(true, Ordering::Release);
    evidence.ack("alpha");
    let (limits, ack_limits) = limits();
    let manager = Replication::new(cluster.groups[0].clone(), source.clone(), app, limits)
        .expect("manager")
        .with_ack_evidence(evidence.clone(), ack_limits)
        .expect("ack capability");
    let target_scope = scope("one");
    let handle = manager
        .open(target_scope.clone(), NonZeroU64::new(2).expect("nonzero"))
        .expect("session");
    handle.set_authority(true).expect("authority");
    let mut oversized = request(&target_scope, 6);
    oversized.request_id = vec![0; 65];
    assert!(matches!(
        handle.wait_named(oversized, Instant::now() + SETTLE).await,
        Err(AckWaitStartError::Invalid(_))
    ));
    assert_eq!(evidence.certifications.load(Ordering::Acquire), 0);
    assert_eq!(
        outcome(
            handle
                .wait_named(request(&target_scope, 8), Instant::now() + SETTLE)
                .await
        ),
        Ok(AckWaitOutcome::AuthorityLost)
    );
    source.commit();
    assert_eq!(
        handle.catch_up(1, Instant::now() + SETTLE).await,
        CatchUp::Ready(1)
    );
    evidence.bad_kind.store(false, Ordering::Release);
    evidence.ack("beta");
    evidence.bad_epoch.store(true, Ordering::Release);
    assert_eq!(
        outcome(
            handle
                .wait_named(request(&target_scope, 9), Instant::now() + SETTLE)
                .await
        ),
        Ok(AckWaitOutcome::AuthorityLost)
    );
    evidence.bad_epoch.store(false, Ordering::Release);
    assert_eq!(
        outcome(
            handle
                .wait_named(request(&target_scope, 10), Instant::now() + SETTLE)
                .await
        ),
        Ok(AckWaitOutcome::Satisfied)
    );
}

#[tokio::test]
async fn stalled_certification_is_bounded_and_old_cancellation_cannot_end_new_wait() {
    let cluster = MemCluster::builder(&["reader"]).group("acks").spawn();
    let source = Source::default();
    let app = App::default();
    let evidence = EvidenceSource::default();
    evidence.cert_pause.arm();
    let (limits, ack_limits) = limits();
    let manager = Replication::new(cluster.groups[0].clone(), source.clone(), app, limits)
        .expect("manager")
        .with_ack_evidence(evidence.clone(), ack_limits)
        .expect("ack capability");
    let target_scope = scope("one");
    let handle = manager
        .open(target_scope.clone(), NonZeroU64::new(3).expect("nonzero"))
        .expect("session");
    handle.set_authority(true).expect("authority");
    let first = {
        let handle = handle.clone();
        let request = request(&target_scope, 10);
        tokio::spawn(async move { handle.wait_named(request, Instant::now() + SETTLE).await })
    };
    eventually_within("certification entered", SETTLE, || {
        evidence.cert_pause.entered.load(Ordering::Acquire)
    })
    .await;
    assert_eq!(
        handle
            .wait_named(request(&target_scope, 11), Instant::now() + SETTLE)
            .await,
        Err(AckWaitStartError::Backpressured)
    );
    first.abort();
    evidence.cert_pause.release();
    eventually_within("old certification released", SETTLE, || {
        evidence.certifications.load(Ordering::Acquire) >= 1
    })
    .await;
    evidence.ack("alpha");
    evidence.ack("beta");
    let later = tokio::time::timeout(SETTLE, async {
        loop {
            let result = outcome(
                handle
                    .wait_named(request(&target_scope, 12), Instant::now() + SETTLE)
                    .await,
            );
            if result != Err(AckWaitStartError::Backpressured) {
                break result;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("old certification must release its slot");
    assert_eq!(later, Ok(AckWaitOutcome::Satisfied));
    source.commit();
    assert_eq!(
        handle.catch_up(1, Instant::now() + SETTLE).await,
        CatchUp::Ready(1)
    );
    evidence.ack("alpha");
    evidence.ack("beta");
    assert_eq!(
        outcome(
            handle
                .wait_named(request(&target_scope, 16), Instant::now() + SETTLE)
                .await
        ),
        Ok(AckWaitOutcome::Satisfied)
    );
}

#[tokio::test]
async fn certification_deadline_expires_without_starting_a_wait_or_stopping_replay() {
    let cluster = MemCluster::builder(&["reader"]).group("acks").spawn();
    let source = Source::default();
    let app = App::default();
    let evidence = EvidenceSource::default();
    evidence.cert_pause.arm();
    let (limits, ack_limits) = limits();
    let manager = Replication::new(cluster.groups[0].clone(), source.clone(), app, limits)
        .expect("manager")
        .with_ack_evidence(evidence.clone(), ack_limits)
        .expect("ack capability");
    let target_scope = scope("one");
    let handle = manager
        .open(target_scope.clone(), NonZeroU64::new(6).expect("nonzero"))
        .expect("session");
    handle.set_authority(true).expect("authority");
    let expired = handle
        .wait_named(
            request(&target_scope, 14),
            Instant::now() + Duration::from_millis(40),
        )
        .await
        .expect("bounded timeout receipt");
    assert_eq!(expired.request_id, vec![14]);
    assert_eq!(expired.target, request(&target_scope, 14).target);
    assert_eq!(expired.kind, AckKind::Invalidated);
    assert!(matches!(expired.outcome, AckWaitOutcome::TimedOut(_)));
    assert!(evidence.cert_pause.entered.load(Ordering::Acquire));
    eventually_within("timed-out certification future dropped", SETTLE, || {
        evidence.cert_inflight.load(Ordering::Acquire) == 0
    })
    .await;
    evidence.cert_pause.release();
    source.commit();
    assert_eq!(
        handle.catch_up(1, Instant::now() + SETTLE).await,
        CatchUp::Ready(1)
    );
}

#[tokio::test]
async fn timeout_receipt_lists_only_the_name_still_unmet() {
    let cluster = MemCluster::builder(&["reader"]).group("acks").spawn();
    let source = Source::default();
    let app = App::default();
    let evidence = EvidenceSource::default();
    evidence.ack("alpha");
    let (limits, ack_limits) = limits();
    let manager = Replication::new(cluster.groups[0].clone(), source.clone(), app, limits)
        .expect("manager")
        .with_ack_evidence(evidence.clone(), ack_limits)
        .expect("ack capability");
    let target_scope = scope("one");
    let handle = manager
        .open(target_scope.clone(), NonZeroU64::new(9).expect("nonzero"))
        .expect("session");
    let receipt = handle
        .wait_named(
            request(&target_scope, 18),
            Instant::now() + Duration::from_millis(180),
        )
        .await
        .expect("timeout receipt");
    assert_eq!(receipt.request_id, vec![18]);
    assert_eq!(receipt.target, request(&target_scope, 18).target);
    assert_eq!(receipt.kind, AckKind::Invalidated);
    let AckWaitOutcome::TimedOut(waiting) = receipt.outcome else {
        panic!("one required name must remain pending");
    };
    assert_eq!(waiting.len(), 1);
    assert_eq!(waiting[0].name, "beta");
    source.commit();
    handle.set_authority(true).expect("authority");
    assert_eq!(
        handle.catch_up(1, Instant::now() + SETTLE).await,
        CatchUp::Ready(1)
    );
}

#[tokio::test]
async fn stalled_ack_source_keeps_global_operation_room_for_another_scope() {
    let cluster = MemCluster::builder(&["reader"]).group("acks").spawn();
    let source = Source::default();
    let app = App::default();
    let evidence = EvidenceSource::default();
    evidence.observe_pause.arm();
    let (limits, ack_limits) = limits();
    let manager = Replication::new(cluster.groups[0].clone(), source.clone(), app, limits)
        .expect("manager")
        .with_ack_evidence(evidence.clone(), ack_limits)
        .expect("ack capability");
    let waiting_scope = scope("waiting");
    let waiting_handle = manager
        .open(waiting_scope.clone(), NonZeroU64::new(4).expect("nonzero"))
        .expect("wait session");
    let wait = tokio::spawn(async move {
        waiting_handle
            .wait_named(request(&waiting_scope, 13), Instant::now() + SETTLE)
            .await
    });
    eventually_within("evidence poll stalled", SETTLE, || {
        evidence.observe_pause.entered.load(Ordering::Acquire)
    })
    .await;

    source.commit();
    let live = manager
        .open(scope("live"), NonZeroU64::new(5).expect("nonzero"))
        .expect("live session");
    live.set_authority(true).expect("authority");
    assert_eq!(
        live.catch_up(1, Instant::now() + SETTLE).await,
        CatchUp::Ready(1),
        "one stalled evidence check must leave operation capacity for replay"
    );
    evidence.ack("alpha");
    evidence.ack("beta");
    evidence.observe_pause.release();
    assert_eq!(
        outcome(wait.await.expect("wait task")),
        Ok(AckWaitOutcome::Satisfied)
    );
}

#[tokio::test]
async fn cancelling_session_resolves_stalled_named_wait_without_leaving_a_slot() {
    let cluster = MemCluster::builder(&["reader"]).group("acks").spawn();
    let source = Source::default();
    let app = App::default();
    let evidence = EvidenceSource::default();
    evidence.observe_pause.arm();
    let (limits, ack_limits) = limits();
    let manager = Replication::new(cluster.groups[0].clone(), source, app, limits)
        .expect("manager")
        .with_ack_evidence(evidence.clone(), ack_limits)
        .expect("ack capability");
    let target_scope = scope("one");
    let handle = manager
        .open(target_scope.clone(), NonZeroU64::new(7).expect("nonzero"))
        .expect("session");
    let waiting = {
        let handle = handle.clone();
        tokio::spawn(async move {
            handle
                .wait_named(request(&target_scope, 15), Instant::now() + SETTLE)
                .await
        })
    };
    eventually_within("named poll stalled", SETTLE, || {
        evidence.observe_pause.entered.load(Ordering::Acquire)
    })
    .await;
    let _ = handle.cancel(Instant::now() + SETTLE).await;
    let cancelled = waiting.await.expect("wait task").expect("cancel receipt");
    assert_eq!(cancelled.request_id, vec![15]);
    assert_eq!(cancelled.kind, AckKind::Invalidated);
    assert_eq!(cancelled.outcome, AckWaitOutcome::Cancelled);
}

#[tokio::test]
async fn replay_operation_keeps_its_own_deadline_while_ack_poll_timer_is_earlier() {
    let cluster = MemCluster::builder(&["reader"]).group("acks").spawn();
    let source = Source::default();
    let app = App::default();
    let evidence = EvidenceSource::default();
    let (mut limits, ack_limits) = limits();
    limits.core.attempt_timeout_ms = 300;
    limits.core.tail_check_ms = 1000;
    let manager = Replication::new(cluster.groups[0].clone(), source.clone(), app, limits)
        .expect("manager")
        .with_ack_evidence(evidence.clone(), ack_limits)
        .expect("ack capability");
    let target_scope = scope("one");
    let handle = manager
        .open(target_scope.clone(), NonZeroU64::new(8).expect("nonzero"))
        .expect("session");
    handle.set_authority(true).expect("authority");
    source.commit();
    assert_eq!(
        handle.catch_up(1, Instant::now() + SETTLE).await,
        CatchUp::Ready(1)
    );
    let wait = {
        let handle = handle.clone();
        let target_scope = target_scope.clone();
        tokio::spawn(async move {
            handle
                .wait_named(request(&target_scope, 17), Instant::now() + SETTLE)
                .await
        })
    };
    eventually_within("first empty ack check", SETTLE, || {
        evidence.observations.load(Ordering::Acquire) > 0
    })
    .await;
    let calls_before = source.tail_calls.load(Ordering::Acquire);
    source.tail_pause.arm();
    source.commit();
    let catchup = {
        let handle = handle.clone();
        tokio::spawn(async move { handle.catch_up(2, Instant::now() + SETTLE).await })
    };
    eventually_within("ordinary source tail entered", SETTLE, || {
        source.tail_pause.entered.load(Ordering::Acquire)
    })
    .await;
    let delayed_at = Instant::now();
    eventually_within("earlier ack timer elapsed", SETTLE, || {
        delayed_at.elapsed() >= Duration::from_millis(100)
    })
    .await;
    source.tail_pause.release();
    assert_eq!(catchup.await.expect("catch-up task"), CatchUp::Ready(2));
    assert!(source.tail_calls.load(Ordering::Acquire) > calls_before);
    assert_eq!(
        source.tail_cancelled.load(Ordering::Acquire),
        0,
        "the ordinary tail future must not inherit the earlier ack poll deadline"
    );
    wait.abort();
}
