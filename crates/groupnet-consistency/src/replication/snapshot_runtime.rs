//! Static mode bridge for worker-owned native snapshot handles.

use std::future::Future;
use std::pin::Pin;
use std::time::Instant;

use groupnet_core::replication::{Operation, Scope, SnapshotConfig, SnapshotOffer, SourceProof};

use super::api::{ApplicationAdapter, Checkpoint, CheckpointLimit, FailureClass, SourceAdapter};
use super::snapshot_api::{
    NativeSnapshot, ReplayOnly, SnapshotApplicationAdapter, SnapshotAttachment, SnapshotHold,
    SnapshotImage, SnapshotSourceAdapter, SnapshotStage,
};

type Work<'a, T> = Pin<Box<dyn Future<Output = Result<T, FailureClass>> + Send + 'a>>;

fn unavailable<T>() -> Work<'static, T> {
    Box::pin(async { Err(FailureClass::Terminal) })
}

pub(crate) trait SnapshotMode<S, A>: Send + Sync + 'static
where
    S: SourceAdapter,
    A: ApplicationAdapter<S::Position, S::Batch>,
{
    type Hold: Send + 'static;
    type ReadHandle: Send + 'static;
    type Attachment: Send + 'static;
    type Stage: Send + 'static;
    const ENABLED: bool;

    fn acquire(
        _: &S,
        _: Scope,
        _: Operation,
        _: groupnet_core::Time,
        _: Instant,
        _: SnapshotConfig,
    ) -> Work<'_, SnapshotHold<Self::Hold>> {
        unavailable()
    }

    fn offer<'a>(
        _: &'a S,
        _: &'a mut Self::Hold,
        _: Scope,
        _: SnapshotConfig,
    ) -> Work<'a, SnapshotImage<Self::ReadHandle>> {
        unavailable()
    }

    fn read<'a>(_: &'a S, _: &'a mut Self::ReadHandle, _: u64, _: usize) -> Work<'a, Vec<u8>> {
        unavailable()
    }

    fn barrier<'a>(
        _: &'a S,
        _: &'a mut Self::Hold,
        _: S::Position,
        _: SnapshotConfig,
    ) -> Work<'a, SourceProof> {
        unavailable()
    }

    fn attach<'a>(
        _: &'a S,
        _: &'a mut Self::Hold,
        _: S::Position,
    ) -> Work<'a, SnapshotAttachment<Self::Attachment>> {
        unavailable()
    }

    fn release(_: &S, _: Self::Hold) -> Work<'_, ()> {
        unavailable()
    }

    fn open<'a>(
        _: &'a A,
        _: Scope,
        _: &'a SnapshotOffer,
        _: CheckpointLimit,
    ) -> Work<'a, SnapshotStage<Self::Stage>> {
        unavailable()
    }

    fn write<'a>(_: &'a A, _: &'a mut Self::Stage, _: u64, _: Vec<u8>) -> Work<'a, usize> {
        unavailable()
    }

    fn verify<'a>(_: &'a A, _: &'a mut Self::Stage, _: &'a [u8]) -> Work<'a, usize> {
        unavailable()
    }

    fn apply<'a>(
        _: &'a A,
        _: &'a mut Self::Stage,
        _: S::Position,
        _: S::Position,
        _: S::Batch,
    ) -> Work<'a, usize> {
        unavailable()
    }

    fn seal(
        _: &A,
        _: Self::Stage,
        _: S::Position,
    ) -> Work<'_, Checkpoint<S::Position, A::Recovery>> {
        unavailable()
    }
}

impl<S, A> SnapshotMode<S, A> for ReplayOnly
where
    S: SourceAdapter,
    A: ApplicationAdapter<S::Position, S::Batch>,
{
    type Hold = ();
    type ReadHandle = ();
    type Attachment = ();
    type Stage = ();
    const ENABLED: bool = false;
}

impl<S, A> SnapshotMode<S, A> for NativeSnapshot
where
    S: SnapshotSourceAdapter,
    A: SnapshotApplicationAdapter<S::Position, S::Batch>,
{
    type Hold = S::Hold;
    type ReadHandle = S::ReadHandle;
    type Attachment = S::Attachment;
    type Stage = A::Stage;
    const ENABLED: bool = true;

    fn acquire(
        source: &S,
        scope: Scope,
        request: Operation,
        due: groupnet_core::Time,
        wall_deadline: Instant,
        limit: SnapshotConfig,
    ) -> Work<'_, SnapshotHold<Self::Hold>> {
        Box::pin(async move {
            source
                .acquire_hold(scope, request, due, wall_deadline, limit)
                .await
                .map_err(|e| e.class())
        })
    }

    fn offer<'a>(
        source: &'a S,
        hold: &'a mut Self::Hold,
        scope: Scope,
        limit: SnapshotConfig,
    ) -> Work<'a, SnapshotImage<Self::ReadHandle>> {
        Box::pin(async move {
            source
                .offer(hold, scope, limit)
                .await
                .map_err(|e| e.class())
        })
    }

    fn read<'a>(
        source: &'a S,
        read: &'a mut Self::ReadHandle,
        offset: u64,
        max: usize,
    ) -> Work<'a, Vec<u8>> {
        Box::pin(async move {
            source
                .read_chunk(read, offset, max)
                .await
                .map_err(|e| e.class())
        })
    }

    fn barrier<'a>(
        source: &'a S,
        hold: &'a mut Self::Hold,
        from: S::Position,
        limit: SnapshotConfig,
    ) -> Work<'a, SourceProof> {
        Box::pin(async move {
            source
                .barrier(hold, from, limit)
                .await
                .map_err(|e| e.class())
        })
    }

    fn attach<'a>(
        source: &'a S,
        hold: &'a mut Self::Hold,
        after: S::Position,
    ) -> Work<'a, SnapshotAttachment<Self::Attachment>> {
        Box::pin(async move { source.attach(hold, after).await.map_err(|e| e.class()) })
    }

    fn release(source: &S, hold: Self::Hold) -> Work<'_, ()> {
        Box::pin(async move { source.release_hold(hold).await.map_err(|e| e.class()) })
    }

    fn open<'a>(
        app: &'a A,
        scope: Scope,
        offer: &'a SnapshotOffer,
        limit: CheckpointLimit,
    ) -> Work<'a, SnapshotStage<Self::Stage>> {
        Box::pin(async move {
            app.begin_stage(scope, offer, limit)
                .await
                .map_err(|e| e.class())
        })
    }

    fn write<'a>(
        app: &'a A,
        stage: &'a mut Self::Stage,
        offset: u64,
        bytes: Vec<u8>,
    ) -> Work<'a, usize> {
        Box::pin(async move {
            app.write_chunk(stage, offset, bytes)
                .await
                .map_err(|e| e.class())
        })
    }

    fn verify<'a>(app: &'a A, stage: &'a mut Self::Stage, digest: &'a [u8]) -> Work<'a, usize> {
        Box::pin(async move { app.verify_image(stage, digest).await.map_err(|e| e.class()) })
    }

    fn apply<'a>(
        app: &'a A,
        stage: &'a mut Self::Stage,
        from: S::Position,
        through: S::Position,
        batch: S::Batch,
    ) -> Work<'a, usize> {
        Box::pin(async move {
            app.apply_private(stage, from, through, batch)
                .await
                .map_err(|e| e.class())
        })
    }

    fn seal(
        app: &A,
        stage: Self::Stage,
        through: S::Position,
    ) -> Work<'_, Checkpoint<S::Position, A::Recovery>> {
        Box::pin(async move { app.seal_stage(stage, through).await.map_err(|e| e.class()) })
    }
}
