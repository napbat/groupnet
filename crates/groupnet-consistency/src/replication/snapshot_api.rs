//! Optional native snapshot capabilities for state-sync sessions.

use std::future::Future;
use std::time::Instant;

use groupnet_core::replication::{
    HoldReceipt, Operation, Scope, SnapshotConfig, SnapshotOffer, SourceProof,
};

use super::api::{AdapterFailure, ApplicationAdapter, Checkpoint, CheckpointLimit, SourceAdapter};

/// A finite source retention hold obtained before selecting the snapshot cut.
#[derive(Debug)]
pub struct SnapshotHold<H> {
    /// Locally validated source retention evidence bound to the acquire operation.
    pub receipt: HoldReceipt,
    /// Worker-owned hold; its source-side reservation must also expire after process loss.
    pub handle: H,
}

/// A consistent source image offered under a previously acquired hold.
///
/// `R` should be a streaming range-read handle with small, bounded local
/// metadata. If it retains a native fork or the complete image, the source
/// adapter must hold separate domain memory admission for that allocation
/// until the handle and any background work are gone. The shell's snapshot
/// stage and chunk quotas do not charge memory owned by `R`.
#[derive(Debug)]
pub struct SnapshotImage<R> {
    /// Source-authenticated cut, coverage, size, schema, and digest.
    pub offer: SnapshotOffer,
    /// Worker-owned handle for ordered range reads.
    pub read: R,
}

/// A continuation attached to the retained source suffix without a gap.
#[derive(Debug)]
pub struct SnapshotAttachment<T> {
    /// Source-authenticated attach barrier and retained lower bound.
    pub proof: SourceProof,
    /// Worker-owned continuation; `()` is valid for authoritative log polling.
    pub handle: T,
}

/// Private application stage and its currently charged native-state bytes.
#[derive(Debug)]
pub struct SnapshotStage<T> {
    /// Stage that cannot serve reads until guarded installation.
    pub handle: T,
    /// Decoded/private-state bytes charged against the candidate limit.
    pub charged_bytes: usize,
}

/// Source-native consistent cuts and retained suffixes, opt-in for snapshots.
///
/// Every returned proof must be verified against the authoritative source by
/// this trusted local adapter. A peer's metadata or gossip head is insufficient.
/// Handles must release local resources on drop; holds must expire at source
/// even if this process dies before explicit release. Futures must be
/// cancellation-safe for their bounded allocations: no detached background
/// work may keep an uncharged native image after its future or handle drops.
pub trait SnapshotSourceAdapter: SourceAdapter {
    /// Finite retention reservation.
    type Hold: Send + 'static;
    /// Streaming image reader; retained native state needs separate source admission.
    type ReadHandle: Send + 'static;
    /// Attached native continuation.
    type Attachment: Send + 'static;

    /// Acquires a source hold before the snapshot cut is chosen. The receipt
    /// must conservatively cover `total_due` from before this request began.
    fn acquire_hold(
        &self,
        scope: Scope,
        request: Operation,
        total_due: groupnet_core::Time,
        wall_deadline: Instant,
        limit: SnapshotConfig,
    ) -> impl Future<Output = Result<SnapshotHold<Self::Hold>, AdapterFailure<Self::Error>>> + Send;

    /// Offers a source-certified consistent cut and a bounded image reader.
    fn offer(
        &self,
        hold: &mut Self::Hold,
        scope: Scope,
        limit: SnapshotConfig,
    ) -> impl Future<Output = Result<SnapshotImage<Self::ReadHandle>, AdapterFailure<Self::Error>>> + Send;

    /// Reads at most `max_bytes` at the exact image offset. An empty response
    /// before completion is invalid, and the shell checks returned size again.
    fn read_chunk(
        &self,
        read: &mut Self::ReadHandle,
        offset: u64,
        max_bytes: usize,
    ) -> impl Future<Output = Result<Vec<u8>, AdapterFailure<Self::Error>>> + Send;

    /// Returns a verified committed barrier while the same hold remains live.
    fn barrier(
        &self,
        hold: &mut Self::Hold,
        from: Self::Position,
        limit: SnapshotConfig,
    ) -> impl Future<Output = Result<SourceProof, AdapterFailure<Self::Error>>> + Send;

    /// Attaches a no-gap continuation after the installed barrier.
    fn attach(
        &self,
        hold: &mut Self::Hold,
        after: Self::Position,
    ) -> impl Future<
        Output = Result<SnapshotAttachment<Self::Attachment>, AdapterFailure<Self::Error>>,
    > + Send;

    /// Best-effort explicit release, bounded by the shell's cleanup timeout.
    fn release_hold(
        &self,
        hold: Self::Hold,
    ) -> impl Future<Output = Result<(), AdapterFailure<Self::Error>>> + Send;
}

/// Application-native private image staging and guarded state/cursor install.
/// The stage retains its [`CheckpointLimit`] while decoding or replaying; a
/// reported charge after allocation is not itself a memory bound. Dropping
/// a cancelled future or stage must release local resources, or any detached
/// native work must retain separate domain admission until it actually ends.
pub trait SnapshotApplicationAdapter<P, B>: ApplicationAdapter<P, B> {
    /// Private, bounded image stage.
    type Stage: Send + 'static;

    /// Creates a private stage under a reserved candidate byte budget.
    fn begin_stage(
        &self,
        scope: Scope,
        offer: &SnapshotOffer,
        limit: CheckpointLimit,
    ) -> impl Future<Output = Result<SnapshotStage<Self::Stage>, AdapterFailure<Self::Error>>> + Send;

    /// Writes one exact ordered chunk and reports total decoded-state charge.
    fn write_chunk(
        &self,
        stage: &mut Self::Stage,
        offset: u64,
        bytes: Vec<u8>,
    ) -> impl Future<Output = Result<usize, AdapterFailure<Self::Error>>> + Send;

    /// Verifies schema and complete image digest, reporting current charge.
    fn verify_image(
        &self,
        stage: &mut Self::Stage,
        digest: &[u8],
    ) -> impl Future<Output = Result<usize, AdapterFailure<Self::Error>>> + Send;

    /// Applies a bounded native committed prefix into private state only.
    fn apply_private(
        &self,
        stage: &mut Self::Stage,
        from: P,
        through: P,
        batch: B,
    ) -> impl Future<Output = Result<usize, AdapterFailure<Self::Error>>> + Send;

    /// Seals an atomic state/cursor candidate; normal guarded install follows.
    fn seal_stage(
        &self,
        stage: Self::Stage,
        through: P,
    ) -> impl Future<Output = Result<Checkpoint<P, Self::Recovery>, AdapterFailure<Self::Error>>> + Send;
}

/// Existing replay-only sessions, with no snapshot capability or storage writes.
#[derive(Clone, Copy, Debug, Default)]
pub struct ReplayOnly;

/// Opt-in native snapshot capability for a source and application that implement
/// the snapshot adapter traits.
#[derive(Clone, Copy, Debug, Default)]
pub struct NativeSnapshot;
