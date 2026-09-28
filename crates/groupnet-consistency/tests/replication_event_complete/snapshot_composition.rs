//! Composition check: native state snapshots and durable named events share a manager.

use std::future::Future;
use std::time::Instant;

use super::*;
use groupnet_consistency::replication::{
    SnapshotApplicationAdapter, SnapshotAttachment, SnapshotHold, SnapshotImage,
    SnapshotSourceAdapter, SnapshotStage,
};
use groupnet_core::replication::{Operation, SnapshotConfig, SnapshotOffer};

fn unused<T>() -> Result<T, AdapterFailure<io::Error>> {
    Err(AdapterFailure::Terminal(io::Error::other(
        "snapshot methods are unused by a named subscriber",
    )))
}

impl SnapshotSourceAdapter for MemSource {
    type Hold = ();
    type ReadHandle = ();
    type Attachment = ();

    fn acquire_hold(
        &self,
        _scope: Scope,
        _request: Operation,
        _total_due: groupnet_core::Time,
        _wall_deadline: Instant,
        _limit: SnapshotConfig,
    ) -> impl Future<Output = Result<SnapshotHold<()>, AdapterFailure<io::Error>>> + Send {
        std::future::ready(unused())
    }

    fn offer(
        &self,
        _hold: &mut (),
        _scope: Scope,
        _limit: SnapshotConfig,
    ) -> impl Future<Output = Result<SnapshotImage<()>, AdapterFailure<io::Error>>> + Send {
        std::future::ready(unused())
    }

    fn read_chunk(
        &self,
        _read: &mut (),
        _offset: u64,
        _max_bytes: usize,
    ) -> impl Future<Output = Result<Vec<u8>, AdapterFailure<io::Error>>> + Send {
        std::future::ready(unused())
    }

    fn barrier(
        &self,
        _hold: &mut (),
        _from: u64,
        _limit: SnapshotConfig,
    ) -> impl Future<Output = Result<SourceProof, AdapterFailure<io::Error>>> + Send {
        std::future::ready(unused())
    }

    fn attach(
        &self,
        _hold: &mut (),
        _after: u64,
    ) -> impl Future<Output = Result<SnapshotAttachment<()>, AdapterFailure<io::Error>>> + Send
    {
        std::future::ready(unused())
    }

    fn release_hold(
        &self,
        _hold: (),
    ) -> impl Future<Output = Result<(), AdapterFailure<io::Error>>> + Send {
        std::future::ready(unused())
    }
}

impl SnapshotApplicationAdapter<u64, Vec<u64>> for StubApp {
    type Stage = ();

    fn begin_stage(
        &self,
        _scope: Scope,
        _offer: &SnapshotOffer,
        _limit: CheckpointLimit,
    ) -> impl Future<Output = Result<SnapshotStage<()>, AdapterFailure<io::Error>>> + Send {
        std::future::ready(unused())
    }

    fn write_chunk(
        &self,
        _stage: &mut (),
        _offset: u64,
        _bytes: Vec<u8>,
    ) -> impl Future<Output = Result<usize, AdapterFailure<io::Error>>> + Send {
        std::future::ready(unused())
    }

    fn verify_image(
        &self,
        _stage: &mut (),
        _digest: &[u8],
    ) -> impl Future<Output = Result<usize, AdapterFailure<io::Error>>> + Send {
        std::future::ready(unused())
    }

    fn apply_private(
        &self,
        _stage: &mut (),
        _from: u64,
        _through: u64,
        _batch: Vec<u64>,
    ) -> impl Future<Output = Result<usize, AdapterFailure<io::Error>>> + Send {
        std::future::ready(unused())
    }

    fn seal_stage(
        &self,
        _stage: (),
        _through: u64,
    ) -> impl Future<Output = Result<Checkpoint<u64, ()>, AdapterFailure<io::Error>>> + Send {
        std::future::ready(unused())
    }
}

#[tokio::test]
async fn native_snapshot_state_sync_and_named_events_compose_without_snapshot_delivery() {
    let cluster = MemCluster::builder(&["event-snapshot-composition"])
        .group("stores")
        .spawn();
    let mut bounds = limits();
    bounds.core.snapshot = Some(SnapshotConfig {
        max_metadata_bytes: 512,
        max_snapshot_bytes: 64,
        max_chunks: 16,
        max_chunk_bytes: 4,
        max_candidate_bytes: 64,
        max_total_ms: 3000,
    });
    let manager = Replication::new_native(
        cluster.groups[0].clone(),
        MemSource::default(),
        StubApp,
        bounds,
    )
    .expect("native snapshot manager")
    .with_event_complete(MemSink::default(), SubscriptionLimits::default())
    .expect("named capability");
    let handle = manager
        .open_named(
            &scope(),
            SubscriberId {
                name: "billing".into(),
            },
            NonZeroU64::new(11).expect("nonzero"),
            SubscriptionStart::StartAt {
                position: 0,
                policy: policy(),
                request_id: vec![11],
            },
        )
        .expect("named open");
    eventually_within("named registration protected", SETTLE, || {
        handle.status().stage == groupnet_core::replication::Stage::Protected
    })
    .await;
}
