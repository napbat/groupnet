//! Static mode bridge for worker-owned native snapshot handles.

use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use groupnet_core::replication::{Operation, Scope, SnapshotConfig, SnapshotOffer, SourceProof};

use super::api::{ApplicationAdapter, Checkpoint, CheckpointLimit, FailureClass, SourceAdapter};
use super::snapshot_api::{
    NativeSnapshot, ReplayOnly, SnapshotApplicationAdapter, SnapshotAttachment, SnapshotHold,
    SnapshotImage, SnapshotSourceAdapter, SnapshotStage,
};

/// A replay-only mode has no snapshot capability: every operation resolves at
/// once to a terminal failure without touching an adapter.
struct Unavailable<T>(PhantomData<fn() -> T>);

impl<T> Future for Unavailable<T> {
    type Output = Result<T, FailureClass>;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Err(FailureClass::Terminal))
    }
}

fn unavailable<T>() -> Unavailable<T> {
    Unavailable(PhantomData)
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
    ) -> impl Future<Output = Result<SnapshotHold<Self::Hold>, FailureClass>> + Send + '_ {
        unavailable()
    }

    fn offer<'a>(
        _: &'a S,
        _: &'a mut Self::Hold,
        _: Scope,
        _: SnapshotConfig,
    ) -> impl Future<Output = Result<SnapshotImage<Self::ReadHandle>, FailureClass>> + Send + 'a
    {
        unavailable()
    }

    fn read<'a>(
        _: &'a S,
        _: &'a mut Self::ReadHandle,
        _: u64,
        _: usize,
    ) -> impl Future<Output = Result<Vec<u8>, FailureClass>> + Send + 'a {
        unavailable()
    }

    fn barrier<'a>(
        _: &'a S,
        _: &'a mut Self::Hold,
        _: S::Position,
        _: SnapshotConfig,
    ) -> impl Future<Output = Result<SourceProof, FailureClass>> + Send + 'a {
        unavailable()
    }

    fn attach<'a>(
        _: &'a S,
        _: &'a mut Self::Hold,
        _: S::Position,
    ) -> impl Future<Output = Result<SnapshotAttachment<Self::Attachment>, FailureClass>> + Send + 'a
    {
        unavailable()
    }

    fn release(_: &S, _: Self::Hold) -> impl Future<Output = Result<(), FailureClass>> + Send + '_ {
        unavailable()
    }

    fn open<'a>(
        _: &'a A,
        _: Scope,
        _: &'a SnapshotOffer,
        _: CheckpointLimit,
    ) -> impl Future<Output = Result<SnapshotStage<Self::Stage>, FailureClass>> + Send + 'a {
        unavailable()
    }

    fn write<'a>(
        _: &'a A,
        _: &'a mut Self::Stage,
        _: u64,
        _: Vec<u8>,
    ) -> impl Future<Output = Result<usize, FailureClass>> + Send + 'a {
        unavailable()
    }

    fn verify<'a>(
        _: &'a A,
        _: &'a mut Self::Stage,
        _: &'a [u8],
    ) -> impl Future<Output = Result<usize, FailureClass>> + Send + 'a {
        unavailable()
    }

    fn apply<'a>(
        _: &'a A,
        _: &'a mut Self::Stage,
        _: S::Position,
        _: S::Position,
        _: S::Batch,
    ) -> impl Future<Output = Result<usize, FailureClass>> + Send + 'a {
        unavailable()
    }

    fn seal(
        _: &A,
        _: Self::Stage,
        _: S::Position,
    ) -> impl Future<Output = Result<Checkpoint<S::Position, A::Recovery>, FailureClass>> + Send + '_
    {
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

    async fn acquire(
        source: &S,
        scope: Scope,
        request: Operation,
        due: groupnet_core::Time,
        wall_deadline: Instant,
        limit: SnapshotConfig,
    ) -> Result<SnapshotHold<Self::Hold>, FailureClass> {
        source
            .acquire_hold(scope, request, due, wall_deadline, limit)
            .await
            .map_err(|e| e.class())
    }

    async fn offer<'a>(
        source: &'a S,
        hold: &'a mut Self::Hold,
        scope: Scope,
        limit: SnapshotConfig,
    ) -> Result<SnapshotImage<Self::ReadHandle>, FailureClass> {
        source
            .offer(hold, scope, limit)
            .await
            .map_err(|e| e.class())
    }

    async fn read<'a>(
        source: &'a S,
        read: &'a mut Self::ReadHandle,
        offset: u64,
        max: usize,
    ) -> Result<Vec<u8>, FailureClass> {
        source
            .read_chunk(read, offset, max)
            .await
            .map_err(|e| e.class())
    }

    async fn barrier<'a>(
        source: &'a S,
        hold: &'a mut Self::Hold,
        from: S::Position,
        limit: SnapshotConfig,
    ) -> Result<SourceProof, FailureClass> {
        source
            .barrier(hold, from, limit)
            .await
            .map_err(|e| e.class())
    }

    async fn attach<'a>(
        source: &'a S,
        hold: &'a mut Self::Hold,
        after: S::Position,
    ) -> Result<SnapshotAttachment<Self::Attachment>, FailureClass> {
        source.attach(hold, after).await.map_err(|e| e.class())
    }

    async fn release(source: &S, hold: Self::Hold) -> Result<(), FailureClass> {
        source.release_hold(hold).await.map_err(|e| e.class())
    }

    async fn open<'a>(
        app: &'a A,
        scope: Scope,
        offer: &'a SnapshotOffer,
        limit: CheckpointLimit,
    ) -> Result<SnapshotStage<Self::Stage>, FailureClass> {
        app.begin_stage(scope, offer, limit)
            .await
            .map_err(|e| e.class())
    }

    async fn write<'a>(
        app: &'a A,
        stage: &'a mut Self::Stage,
        offset: u64,
        bytes: Vec<u8>,
    ) -> Result<usize, FailureClass> {
        app.write_chunk(stage, offset, bytes)
            .await
            .map_err(|e| e.class())
    }

    async fn verify<'a>(
        app: &'a A,
        stage: &'a mut Self::Stage,
        digest: &'a [u8],
    ) -> Result<usize, FailureClass> {
        app.verify_image(stage, digest).await.map_err(|e| e.class())
    }

    async fn apply<'a>(
        app: &'a A,
        stage: &'a mut Self::Stage,
        from: S::Position,
        through: S::Position,
        batch: S::Batch,
    ) -> Result<usize, FailureClass> {
        app.apply_private(stage, from, through, batch)
            .await
            .map_err(|e| e.class())
    }

    async fn seal(
        app: &A,
        stage: Self::Stage,
        through: S::Position,
    ) -> Result<Checkpoint<S::Position, A::Recovery>, FailureClass> {
        app.seal_stage(stage, through).await.map_err(|e| e.class())
    }
}
