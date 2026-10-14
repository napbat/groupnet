//! Typed manager construction and registration failures.

use std::error::Error;

use super::*;
use groupnet_consistency::replication::{LimitsError, SnapshotConfig};
use tokio::sync::Semaphore;

fn snapshot() -> SnapshotConfig {
    SnapshotConfig {
        max_metadata_bytes: 512,
        max_snapshot_bytes: 64,
        max_chunks: 16,
        max_chunk_bytes: 4,
        max_candidate_bytes: 64,
        max_total_ms: 3000,
    }
}

fn start(request_id: Vec<u8>) -> SubscriptionStart<u64> {
    SubscriptionStart::StartAt {
        position: 0,
        policy: policy(),
        request_id,
    }
}

fn billing() -> SubscriberId {
    SubscriberId {
        name: "billing".into(),
    }
}

#[tokio::test]
async fn construction_reports_the_violated_limit_and_snapshot_mode() {
    let cluster = MemCluster::builder(&["event-open-limits"])
        .group("stores")
        .spawn()
        .await;
    let group = &cluster.groups[0];
    let depth = Semaphore::MAX_PERMITS + 1;
    let bounds = Limits {
        queue_depth: NonZeroUsize::new(depth).expect("nonzero"),
        ..limits()
    };
    let error = Replication::new(group.clone(), MemSource::default(), StubApp, bounds)
        .expect_err("queue depth above permits");
    let expected = LimitsError::QueueDepthAbovePermits { queue_depth: depth };
    assert_eq!(error, OpenError::InvalidLimits(expected));
    assert_eq!(
        error
            .source()
            .and_then(|source| source.downcast_ref::<LimitsError>()),
        Some(&expected)
    );

    let mut with_snapshot = limits();
    with_snapshot.core.snapshot = Some(snapshot());
    assert_eq!(
        Replication::new(group.clone(), MemSource::default(), StubApp, with_snapshot)
            .expect_err("replay-only rejects snapshot bounds"),
        OpenError::SnapshotLimitsUnsupported
    );
    assert_eq!(
        Replication::new_native(group.clone(), MemSource::default(), StubApp, limits())
            .expect_err("native snapshots need snapshot bounds"),
        OpenError::SnapshotLimitsRequired
    );
}

#[tokio::test]
async fn subscription_capability_and_registration_errors_are_typed() {
    let cluster = MemCluster::builder(&["event-open-registration"])
        .group("stores")
        .spawn()
        .await;
    let group = &cluster.groups[0];
    let zero = SubscriptionLimits {
        max_request_bytes: 0,
        ..SubscriptionLimits::default()
    };
    let error = Replication::new(group.clone(), MemSource::default(), StubApp, limits())
        .expect("manager")
        .with_event_complete(MemSink::default(), zero)
        .expect_err("zero request bound");
    assert_eq!(error, OpenError::InvalidSubscriptionLimits);

    let started =
        Replication::new(group.clone(), MemSource::default(), StubApp, limits()).expect("manager");
    let empty = Scope {
        partition: String::new(),
        ..scope()
    };
    assert!(matches!(
        started.open(empty.clone(), NonZeroU64::new(1).expect("nonzero")),
        Err(OpenError::InvalidScope(_))
    ));
    let _session = started
        .open(scope(), NonZeroU64::new(2).expect("nonzero"))
        .expect("state-sync session");
    let error = started
        .with_event_complete(MemSink::default(), SubscriptionLimits::default())
        .expect_err("sessions already started");
    assert_eq!(error, OpenError::SessionsStarted);

    let manager = Replication::new(group.clone(), MemSource::default(), StubApp, limits())
        .expect("manager")
        .with_event_complete(MemSink::default(), SubscriptionLimits::default())
        .expect("named capability");
    assert!(matches!(
        manager
            .open_named(
                &empty,
                billing(),
                NonZeroU64::new(3).expect("nonzero"),
                start(vec![3])
            )
            .err(),
        Some(OpenError::InvalidScope(_))
    ));
    assert!(matches!(
        manager
            .open_named(
                &scope(),
                billing(),
                NonZeroU64::new(4).expect("nonzero"),
                start(Vec::new())
            )
            .err(),
        Some(OpenError::StartRejected(_))
    ));
}
