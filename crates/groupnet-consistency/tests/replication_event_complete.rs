//! Durable named delivery over a source-backed in-memory log and sink.

#![cfg(feature = "replication")]

use std::collections::{HashMap, HashSet};
use std::io;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use groupnet_consistency::replication::{
    AdapterFailure, ApplicationAdapter, Checkpoint, CheckpointLimit, DurableEventSink,
    DurableSubscriptionSource, InstallPermit, Limits, Materialized, OpenError, Replication,
    RevocationPermit, ScanLimit, SourceAdapter, SourceBatch, SubscriptionSourceResult,
    SubscriptionStart, TailLimit,
};
use groupnet_core::replication::{
    BoundComparison, CommitSubscriberAck, Comparison, Cursor, DurableDeliveryReceipt,
    FencedCheckpoint, ProofId, RegisterReceipt, RegisterSubscriber, RetentionPolicy, Scope,
    SourceHistory, SourceProof, SourceSubscriberState, Stream, SubscriberAckReceipt, SubscriberId,
    SubscriberKey, SubscriptionEpoch, SubscriptionError, SubscriptionLimits,
};
use groupnet_testkit::cluster::{MemCluster, eventually_within};

const SETTLE: Duration = Duration::from_secs(3);

#[path = "replication_event_complete/snapshot_composition.rs"]
mod snapshot_composition;
#[path = "replication_event_complete/source_fixture.rs"]
mod source_fixture;
#[path = "replication_event_complete/terminal_lifecycle.rs"]
mod terminal_lifecycle;

fn scope() -> Scope {
    Scope {
        stream: Stream {
            group: "stores".into(),
            topic: "events".into(),
            kind: "v1".into(),
        },
        partition: "one".into(),
    }
}

fn cursor(scope: &Scope, position: u64) -> Cursor {
    Cursor {
        scope: scope.clone(),
        history: SourceHistory {
            source: "native-cas".into(),
            generation: 1,
        },
        position: position.to_le_bytes().to_vec(),
    }
}

fn position(cursor: &Cursor) -> u64 {
    let bytes: [u8; 8] = cursor
        .position
        .as_slice()
        .try_into()
        .expect("native u64 cursor");
    u64::from_le_bytes(bytes)
}

fn comparison(left: &Cursor, right: &Cursor, proof: &SourceProof) -> BoundComparison {
    BoundComparison {
        left: left.clone(),
        right: right.clone(),
        proof: proof.id.clone(),
        order: if left.scope != right.scope || left.history != right.history {
            Comparison::Incomparable
        } else {
            match position(left).cmp(&position(right)) {
                std::cmp::Ordering::Less => Comparison::Before,
                std::cmp::Ordering::Equal => Comparison::Equal,
                std::cmp::Ordering::Greater => Comparison::After,
            }
        },
    }
}

fn proof(scope: &Scope, head: u64, retained: u64) -> SourceProof {
    SourceProof {
        id: ProofId(vec![9]),
        head: cursor(scope, head),
        retained_from: cursor(scope, retained),
        read_authority: false,
    }
}

fn policy() -> RetentionPolicy {
    RetentionPolicy {
        fingerprint: vec![7],
        max_bytes: 4096,
        max_events: 128,
        max_age_ms: 60_000,
        max_lag_events: 128,
    }
}

#[derive(Debug, Default)]
struct Log {
    head: u64,
    retained: u64,
    ordinal: HashMap<SubscriberKey, u64>,
    registrations: HashMap<SubscriberKey, RegisterReceipt>,
    source_ack: HashMap<SubscriberKey, u64>,
    registration_requests: HashMap<Vec<u8>, RegisterReceipt>,
    ack_requests: HashMap<(SubscriberKey, Vec<u8>), SubscriberAckReceipt>,
    terminal_requests:
        HashMap<(SubscriberKey, Vec<u8>), groupnet_core::replication::TerminalReceipt>,
}

#[derive(Clone, Debug, Default)]
struct MemSource {
    state: Arc<Mutex<Log>>,
    tails: Arc<AtomicU64>,
    unknown_ack_once: Arc<AtomicBool>,
    unknown_terminal_once: Arc<AtomicBool>,
    terminal_failure_once: Arc<Mutex<Option<groupnet_consistency::replication::FailureClass>>>,
    terminal_gate: Arc<Mutex<Option<Arc<TerminalGate>>>>,
    terminal_calls: Arc<AtomicU64>,
    register_calls: Arc<AtomicU64>,
    cursor_gate: Arc<Mutex<Option<Arc<CursorGate>>>>,
}

#[derive(Debug, Default)]
struct TerminalGate {
    entered: AtomicBool,
    release: tokio::sync::Notify,
}

#[derive(Debug, Default)]
struct CursorGate {
    entered: AtomicBool,
    released: Mutex<bool>,
    wake: Condvar,
}

impl CursorGate {
    fn release(&self) {
        *self.released.lock().expect("gate lock") = true;
        self.wake.notify_all();
    }
}

impl MemSource {
    fn append(&self, through: u64) {
        self.state.lock().expect("source lock").head = through;
    }

    fn acknowledged(&self, key: &SubscriberKey) -> Option<u64> {
        self.state
            .lock()
            .expect("source lock")
            .source_ack
            .get(key)
            .copied()
    }
}

#[derive(Clone, Debug, Default)]
struct StubApp;

#[expect(
    clippy::unused_async_trait_impl,
    reason = "named delivery tests do not use StateSync application methods"
)]
impl ApplicationAdapter<u64, Vec<u64>> for StubApp {
    type Error = io::Error;
    type Recovery = ();

    async fn load_checkpoint(
        &self,
        _scope: Scope,
        _limit: CheckpointLimit,
    ) -> Result<Option<Checkpoint<u64, ()>>, AdapterFailure<io::Error>> {
        Ok(None)
    }

    async fn install_checkpoint(
        &self,
        _scope: Scope,
        _checkpoint: Checkpoint<u64, ()>,
        _permit: InstallPermit,
    ) -> Result<Materialized<u64>, AdapterFailure<io::Error>> {
        Err(AdapterFailure::Terminal(io::Error::other("unused")))
    }

    async fn revoke_serving(
        &self,
        _scope: Scope,
        _permit: RevocationPermit,
    ) -> Result<(), AdapterFailure<io::Error>> {
        Ok(())
    }

    async fn apply(
        &self,
        _scope: Scope,
        _from: u64,
        _through: u64,
        _native: Vec<u64>,
        _permit: InstallPermit,
    ) -> Result<Materialized<u64>, AdapterFailure<io::Error>> {
        Err(AdapterFailure::Terminal(io::Error::other("unused")))
    }

    fn may_serve(&self, _scope: &Scope, _through: &u64) -> bool {
        false
    }
}

#[derive(Debug, Default)]
struct SinkState {
    ordinal: u64,
    epoch: Option<SubscriptionEpoch>,
    cursor: u64,
    applied: HashSet<u64>,
}

#[derive(Clone, Debug, Default)]
struct MemSink {
    state: Arc<Mutex<HashMap<SubscriberKey, SinkState>>>,
    bind_calls: Arc<AtomicU64>,
    fail_bind_once: Arc<AtomicBool>,
    saved_failed_permit: Arc<Mutex<Option<InstallPermit>>>,
}

impl MemSink {
    fn applied(&self, key: &SubscriberKey) -> HashSet<u64> {
        self.state
            .lock()
            .expect("sink lock")
            .get(key)
            .map_or_else(HashSet::new, |s| s.applied.clone())
    }
}

#[expect(
    clippy::unused_async_trait_impl,
    reason = "in-memory durable sink fixture matches the async sink interface"
)]
impl DurableEventSink<u64, Vec<u64>> for MemSink {
    type Error = io::Error;

    async fn bind_subscriber_epoch(
        &self,
        registration: RegisterReceipt,
        permit: InstallPermit,
    ) -> Result<FencedCheckpoint, AdapterFailure<io::Error>> {
        self.bind_calls.fetch_add(1, Ordering::AcqRel);
        if self.fail_bind_once.swap(false, Ordering::AcqRel) {
            *self.saved_failed_permit.lock().expect("permit lock") = Some(permit);
            return Err(AdapterFailure::Retryable(io::Error::other(
                "ambiguous bind",
            )));
        }
        let key = registration.key.clone();
        let new_epoch = registration.epoch.clone();
        let registration_cursor = position(&registration.protected);
        permit
            .commit_sync(registration_cursor, true, || {
                let mut all = self.state.lock().expect("sink lock");
                let state = all.entry(key.clone()).or_default();
                if new_epoch.ordinal.get() < state.ordinal {
                    return Err(io::Error::other("stale ordinal"));
                }
                if new_epoch.ordinal.get() > state.ordinal {
                    state.ordinal = new_epoch.ordinal.get();
                    state.epoch = Some(new_epoch.clone());
                    state.cursor = state.cursor.max(registration_cursor);
                } else if state.epoch.as_ref() != Some(&new_epoch) {
                    return Err(io::Error::other("ordinal reused"));
                }
                Ok(())
            })
            .map_err(AdapterFailure::Terminal)?
            .ok_or_else(|| AdapterFailure::AuthorityLost(io::Error::other("stale permit")))?;
        let current = self
            .state
            .lock()
            .expect("sink lock")
            .get(&key)
            .expect("bound")
            .cursor;
        Ok(FencedCheckpoint {
            key,
            request_id: registration.request_id,
            epoch: new_epoch,
            cursor: cursor(&registration.key.scope, current),
            durable: true,
        })
    }

    async fn apply_subscriber_batch(
        &self,
        registration: RegisterReceipt,
        _from: u64,
        through: u64,
        previous_sink: u64,
        native: Vec<u64>,
        permit: InstallPermit,
    ) -> Result<DurableDeliveryReceipt, AdapterFailure<io::Error>> {
        let key = registration.key.clone();
        let epoch = registration.epoch.clone();
        permit
            .commit_sync(through, true, || {
                let mut all = self.state.lock().expect("sink lock");
                let state = all
                    .get_mut(&key)
                    .ok_or_else(|| io::Error::other("unbound"))?;
                if state.epoch.as_ref() != Some(&epoch) || state.ordinal != epoch.ordinal.get() {
                    return Err(io::Error::other("stale epoch"));
                }
                if state.cursor == previous_sink {
                    state.applied.extend(native);
                    state.cursor = through.max(state.cursor);
                } else if state.cursor < through || !state.applied.contains(&through) {
                    return Err(io::Error::other("ambiguous divergent effect"));
                }
                Ok(())
            })
            .map_err(AdapterFailure::Terminal)?
            .ok_or_else(|| AdapterFailure::AuthorityLost(io::Error::other("stale permit")))?;
        let sink_cursor = self
            .state
            .lock()
            .expect("sink lock")
            .get(&key)
            .expect("bound")
            .cursor;
        let mut ack_request_id = epoch.ordinal.get().to_le_bytes().to_vec();
        ack_request_id.extend_from_slice(&through.to_le_bytes());
        Ok(DurableDeliveryReceipt {
            operation: permit.operation(),
            key: key.clone(),
            epoch,
            previous_sink: cursor(&key.scope, previous_sink),
            through: cursor(&key.scope, through),
            sink_cursor: cursor(&key.scope, sink_cursor),
            ack_request_id,
            durable: true,
        })
    }
}

#[tokio::test]
async fn failed_sink_operation_retires_its_cloned_permit_before_retry() {
    let cluster = MemCluster::builder(&["event-failed-permit"])
        .group("stores")
        .spawn()
        .await;
    let source = MemSource::default();
    let sink = MemSink::default();
    sink.fail_bind_once.store(true, Ordering::Release);
    let mut bounds = limits();
    bounds.core.retry_ms = 2_000;
    let manager = Replication::new(cluster.groups[0].clone(), source, StubApp, bounds)
        .expect("manager")
        .with_event_complete(sink.clone(), SubscriptionLimits::default())
        .expect("named capability");
    let handle = manager
        .open_named(
            &scope(),
            SubscriberId {
                name: "billing".into(),
            },
            NonZeroU64::new(7).expect("nonzero"),
            SubscriptionStart::StartAt {
                position: 0,
                policy: policy(),
                request_id: vec![7],
            },
        )
        .expect("subscriber");
    eventually_within("failed bind entered backoff", SETTLE, || {
        sink.saved_failed_permit
            .lock()
            .expect("permit lock")
            .is_some()
            && handle.status().stage == groupnet_core::replication::Stage::RetryWait
    })
    .await;
    let stale = sink
        .saved_failed_permit
        .lock()
        .expect("permit lock")
        .clone()
        .expect("captured failed permit");
    assert_eq!(handle.status().failure, None);
    assert!(
        stale
            .commit_sync(0, true, || Ok::<(), io::Error>(()))
            .expect("no sink error")
            .is_none()
    );
    handle.cancel();
    assert_eq!(
        handle.status().stage,
        groupnet_core::replication::Stage::Cancelled
    );
}

fn limits() -> Limits {
    let mut limits = Limits::default();
    limits.core.tail_check_ms = 20;
    limits.core.retry_ms = 10;
    limits.core.attempt_timeout_ms = 500;
    limits.core.max_batch_bytes = 1024;
    limits.max_inflight_bytes = 2048;
    limits
}

#[tokio::test]
async fn first_operation_budget_starts_before_blocked_cursor_encoding() {
    let cluster = MemCluster::builder(&["event-first-deadline"])
        .group("stores")
        .spawn()
        .await;
    let source = MemSource::default();
    let gate = Arc::new(CursorGate::default());
    *source.cursor_gate.lock().expect("cursor gate lock") = Some(Arc::clone(&gate));
    let mut bounds = limits();
    bounds.core.attempt_timeout_ms = 25;
    bounds.core.retry_ms = 1_000;
    let manager = Arc::new(
        Replication::new(cluster.groups[0].clone(), source.clone(), StubApp, bounds)
            .expect("manager")
            .with_event_complete(MemSink::default(), SubscriptionLimits::default())
            .expect("named capability"),
    );
    let started = Instant::now();
    let opening_manager = Arc::clone(&manager);
    let opening = tokio::task::spawn_blocking(move || {
        opening_manager.open_named(
            &scope(),
            SubscriberId {
                name: "billing".into(),
            },
            NonZeroU64::new(10).expect("nonzero"),
            SubscriptionStart::StartAt {
                position: 0,
                policy: policy(),
                request_id: vec![10],
            },
        )
    });
    eventually_within("cursor encoding entered", SETTLE, || {
        gate.entered.load(Ordering::Acquire)
    })
    .await;
    eventually_within("first operation deadline elapsed", SETTLE, || {
        started.elapsed() >= Duration::from_millis(75)
    })
    .await;
    gate.release();
    let handle = opening.await.expect("opening task").expect("named open");
    eventually_within("expired initial registration backed off", SETTLE, || {
        handle.status().stage == groupnet_core::replication::Stage::RetryWait
    })
    .await;
    assert_eq!(source.register_calls.load(Ordering::Acquire), 0);
    handle.cancel();
}

#[tokio::test]
async fn committed_events_arrive_without_hints_and_ack_only_after_sink_effects() {
    let cluster = MemCluster::builder(&["event-reader"])
        .group("stores")
        .spawn()
        .await;
    let source = MemSource::default();
    let sink = MemSink::default();
    let manager = Replication::new(cluster.groups[0].clone(), source.clone(), StubApp, limits())
        .expect("manager")
        .with_event_complete(sink.clone(), SubscriptionLimits::default())
        .expect("named capability");
    let handle = manager
        .open_named(
            &scope(),
            SubscriberId {
                name: "billing".into(),
            },
            NonZeroU64::new(1).expect("nonzero"),
            SubscriptionStart::StartAt {
                position: 0,
                policy: policy(),
                request_id: vec![1],
            },
        )
        .expect("subscriber");
    source.append(2);
    eventually_within("two durable source acks", SETTLE, || {
        source.acknowledged(handle.key()) == Some(2)
            && sink.applied(handle.key()) == HashSet::from([1, 2])
    })
    .await;
    assert!(source.tails.load(Ordering::Acquire) > 0);
    let status = handle.status();
    assert_eq!(status.source_ack, Some(2));
    assert_eq!(status.sink_cursor, Some(2));
    assert!(manager.close_named_if(handle.key(), NonZeroU64::new(1).expect("nonzero")));
}

#[tokio::test]
async fn unknown_ack_is_read_back_then_restart_resumes_exact_protected_cursor() {
    let cluster = MemCluster::builder(&["event-restart"])
        .group("stores")
        .spawn()
        .await;
    let source = MemSource::default();
    let sink = MemSink::default();
    source.unknown_ack_once.store(true, Ordering::Release);
    let manager = Replication::new(cluster.groups[0].clone(), source.clone(), StubApp, limits())
        .expect("manager")
        .with_event_complete(sink.clone(), SubscriptionLimits::default())
        .expect("named capability");
    let name = SubscriberId {
        name: "billing".into(),
    };
    let old = manager
        .open_named(
            &scope(),
            name.clone(),
            NonZeroU64::new(2).expect("nonzero"),
            SubscriptionStart::StartAt {
                position: 0,
                policy: policy(),
                request_id: vec![2],
            },
        )
        .expect("first incarnation");
    source.append(1);
    eventually_within("unknown ack resolved", SETTLE, || {
        source.acknowledged(old.key()) == Some(1) && old.status().source_ack == Some(1)
    })
    .await;
    let key = old.key().clone();
    assert!(manager.close_named_if(&key, NonZeroU64::new(2).expect("nonzero")));
    let fresh = manager
        .open_named(
            &scope(),
            name,
            NonZeroU64::new(3).expect("nonzero"),
            SubscriptionStart::ResumeExisting {
                policy: policy(),
                request_id: vec![3],
            },
        )
        .expect("resumed incarnation");
    source.append(2);
    eventually_within("resumed exact suffix", SETTLE, || {
        source.acknowledged(fresh.key()) == Some(2)
            && sink.applied(fresh.key()) == HashSet::from([1, 2])
    })
    .await;
    assert_eq!(fresh.status().source_ack, Some(2));
}

#[tokio::test]
async fn two_names_share_native_scope_and_global_registry_capacity() {
    let cluster = MemCluster::builder(&["event-capacity"])
        .group("stores")
        .spawn()
        .await;
    let mut bounds = limits();
    bounds.max_scopes = 2;
    let source = MemSource::default();
    let sink = MemSink::default();
    let manager = Replication::new(cluster.groups[0].clone(), source, StubApp, bounds)
        .expect("manager")
        .with_event_complete(sink, SubscriptionLimits::default())
        .expect("named capability");
    let first = manager
        .open_named(
            &scope(),
            SubscriberId {
                name: "first".into(),
            },
            NonZeroU64::new(4).expect("nonzero"),
            SubscriptionStart::StartAt {
                position: 0,
                policy: policy(),
                request_id: vec![4],
            },
        )
        .expect("first");
    let second = manager
        .open_named(
            &scope(),
            SubscriberId {
                name: "second".into(),
            },
            NonZeroU64::new(5).expect("nonzero"),
            SubscriptionStart::StartAt {
                position: 0,
                policy: policy(),
                request_id: vec![5],
            },
        )
        .expect("same native scope, separate name");
    assert_ne!(first.key(), second.key());
    assert!(matches!(
        manager.open_named(
            &scope(),
            SubscriberId {
                name: "third".into()
            },
            NonZeroU64::new(6).expect("nonzero"),
            SubscriptionStart::StartAt {
                position: 0,
                policy: policy(),
                request_id: vec![6]
            },
        ),
        Err(OpenError::Backpressured)
    ));
    assert!(manager.close_named_if(first.key(), NonZeroU64::new(4).expect("nonzero")));
    assert!(!manager.close_named_if(first.key(), NonZeroU64::new(4).expect("nonzero")));
    manager
        .open_named(
            &scope(),
            SubscriberId {
                name: "third".into(),
            },
            NonZeroU64::new(6).expect("nonzero"),
            SubscriptionStart::StartAt {
                position: 0,
                policy: policy(),
                request_id: vec![6],
            },
        )
        .expect("capacity reclaimed");
}

#[tokio::test]
async fn two_names_progress_with_one_operation_and_one_batch_of_bytes() {
    let cluster = MemCluster::builder(&["event-one-slot"])
        .group("stores")
        .spawn()
        .await;
    let mut bounds = limits();
    bounds.max_scopes = 2;
    bounds.max_parallel_ops = 1;
    bounds.max_inflight_bytes = bounds.core.max_batch_bytes;
    let source = MemSource::default();
    let sink = MemSink::default();
    let manager = Replication::new(cluster.groups[0].clone(), source.clone(), StubApp, bounds)
        .expect("manager")
        .with_event_complete(sink.clone(), SubscriptionLimits::default())
        .expect("named capability");
    let first = manager
        .open_named(
            &scope(),
            SubscriberId {
                name: "first".into(),
            },
            NonZeroU64::new(8).expect("nonzero"),
            SubscriptionStart::StartAt {
                position: 0,
                policy: policy(),
                request_id: vec![8],
            },
        )
        .expect("first");
    let second = manager
        .open_named(
            &scope(),
            SubscriberId {
                name: "second".into(),
            },
            NonZeroU64::new(9).expect("nonzero"),
            SubscriptionStart::StartAt {
                position: 0,
                policy: policy(),
                request_id: vec![9],
            },
        )
        .expect("second");
    source.append(2);
    eventually_within("both names deliver under one shared slot", SETTLE, || {
        source.acknowledged(first.key()) == Some(2)
            && source.acknowledged(second.key()) == Some(2)
            && sink.applied(first.key()) == HashSet::from([1, 2])
            && sink.applied(second.key()) == HashSet::from([1, 2])
    })
    .await;
    assert_ne!(
        first.status().stage,
        groupnet_core::replication::Stage::RetryWait
    );
    assert_ne!(
        second.status().stage,
        groupnet_core::replication::Stage::RetryWait
    );
}
