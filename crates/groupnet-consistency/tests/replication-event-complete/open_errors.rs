//! Typed manager construction and registration failures.

use std::error::Error;

use super::*;
use groupnet_consistency::replication::{
    CoreConfigError, LimitsError, Reject, SnapshotConfig, SubscriptionLimitsError,
};
use tokio::sync::Semaphore;

fn snapshot() -> SnapshotConfig {
    SnapshotConfig {
        max_metadata_bytes: NonZeroUsize::new(512).expect("nonzero"),
        max_snapshot_bytes: NonZeroU64::new(64).expect("nonzero"),
        max_chunks: std::num::NonZeroU32::new(16).expect("nonzero"),
        max_chunk_bytes: NonZeroUsize::new(4).expect("nonzero"),
        max_candidate_bytes: NonZeroU64::new(64).expect("nonzero"),
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
        max_retained_age_ms: 0,
        ..SubscriptionLimits::default()
    };
    let error = Replication::new(group.clone(), MemSource::default(), StubApp, limits())
        .expect("manager")
        .with_event_complete(MemSink::default(), zero)
        .expect_err("zero retained-age bound");
    assert_eq!(
        error,
        OpenError::InvalidSubscriptionLimits(SubscriptionLimitsError::ZeroMaxRetainedAge)
    );
    assert_eq!(
        error
            .source()
            .and_then(|source| source.downcast_ref::<SubscriptionLimitsError>()),
        Some(&SubscriptionLimitsError::ZeroMaxRetainedAge)
    );

    let started =
        Replication::new(group.clone(), MemSource::default(), StubApp, limits()).expect("manager");
    let empty = Scope {
        partition: String::new(),
        ..scope()
    };
    let error = started
        .open(empty.clone(), NonZeroU64::new(1).expect("nonzero"))
        .expect_err("empty partition is rejected");
    assert!(matches!(error, OpenError::InvalidScope(_)));
    assert!(
        error
            .source()
            .and_then(|source| source.downcast_ref::<CoreConfigError>())
            .is_some(),
        "the core's config error is the source"
    );
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
    let error = manager
        .open_named(
            &scope(),
            billing(),
            NonZeroU64::new(4).expect("nonzero"),
            start(Vec::new()),
        )
        .expect_err("empty request id is rejected");
    assert!(matches!(error, OpenError::StartRejected(_)));
    assert!(
        error
            .source()
            .and_then(|source| source.downcast_ref::<Reject>())
            .is_some(),
        "the core's rejection is the source"
    );
}
