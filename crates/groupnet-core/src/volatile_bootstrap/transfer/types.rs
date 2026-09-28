//! Typed, finite transfer inputs and effects. No type grants read authority.

use crate::Time;
use crate::volatile_recovery::RecoveryOperation;

use super::super::journal::{
    AttachToken, BarrierReceipt, CaptureId, JournalBatch, JournalCursor, NativeCut, ReservationId,
};
use super::super::{BootstrapMemberIdentity, BootstrapOperation, BootstrapScope, ClaimIdentity};

/// Exact, sorted per-writer coverage; zero sequence denotes a quiet feed.
pub(crate) fn native_cuts_cover(actual: &[NativeCut], expected: &[NativeCut]) -> bool {
    actual.len() == expected.len()
        && actual
            .windows(2)
            .all(|pair| pair[0].writer < pair[1].writer)
        && expected
            .windows(2)
            .all(|pair| pair[0].writer < pair[1].writer)
        && actual.iter().zip(expected).all(|(found, cut)| {
            !found.writer.is_empty()
                && found.epoch != 0
                && !cut.writer.is_empty()
                && cut.epoch != 0
                && found.writer == cut.writer
                && found.epoch == cut.epoch
                && found.sequence >= cut.sequence
        })
}

/// Finite per-transfer metadata, image, native buffer, and batch limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TransferConfig {
    /// Application schema this follower can decode and install.
    pub expected_schema: u32,
    /// Maximum combined variable metadata bytes in one offer.
    pub max_metadata_bytes: usize,
    /// Maximum encoded private image bytes.
    pub max_encoded_bytes: usize,
    /// Maximum decoded private stage bytes.
    pub max_decoded_bytes: usize,
    /// Maximum encoded bytes in one verified chunk.
    pub max_chunk_bytes: usize,
    /// Maximum number of image chunks.
    pub max_chunks: usize,
    /// Maximum bytes in one donor journal batch.
    pub max_batch_bytes: usize,
    /// Maximum events in one donor journal batch.
    pub max_batch_events: usize,
    /// Maximum total donor journal events replayed in one transfer.
    pub max_replay_events: usize,
    /// Maximum buffered native effects before source coverage.
    pub max_native_buffer_bytes: usize,
    /// Maximum complete member identities in offer and barrier.
    pub max_members: usize,
    /// Maximum complete native writer cuts in offer and barrier.
    pub max_cuts: usize,
    /// Minimum logical cadence between unchanged native-coverage checks.
    pub coverage_poll_ms: u64,
}

impl TransferConfig {
    /// Rejects zero, inverted, or unrepresentable finite budgets.
    ///
    /// # Errors
    /// Returns [`TransferError::InvalidConfig`] for invalid budgets.
    pub fn validate(self) -> Result<Self, TransferError> {
        if self.expected_schema == 0
            || self.max_metadata_bytes == 0
            || self.max_encoded_bytes == 0
            || self.max_decoded_bytes == 0
            || self.max_chunk_bytes == 0
            || self.max_chunks == 0
            || self.max_batch_bytes == 0
            || self.max_batch_events == 0
            || self.max_replay_events == 0
            || self.max_native_buffer_bytes == 0
            || self.max_members == 0
            || self.max_cuts == 0
            || self.coverage_poll_ms == 0
            || self.max_chunk_bytes > self.max_encoded_bytes
            || self.max_chunks > self.max_encoded_bytes
        {
            return Err(TransferError::InvalidConfig);
        }
        Ok(self)
    }
}

/// Exact donor offer for a complete private image captured at C.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransferOffer {
    /// Fresh capture identity, including donor and guarded recovery generation.
    pub capture: CaptureId,
    /// C cursor, which must be position zero for this first transfer format.
    pub image_cut: JournalCursor,
    /// Application index schema version, equal to the follower's declared schema.
    pub schema: u32,
    /// Total encoded image bytes.
    pub encoded_bytes: usize,
    /// Maximum decoded private stage bytes charged before transfer.
    pub decoded_bytes: usize,
    /// Number of nonempty sequential chunks.
    pub chunks: usize,
    /// Opaque full-image commitment verified by the adapter.
    pub commitment: [u8; 32],
    /// Exact complete member identities observed at C, sorted by node.
    pub members: Vec<BootstrapMemberIdentity>,
    /// Exact native per-writer cuts at C, sorted by writer.
    pub cuts: Vec<NativeCut>,
}

/// Adapter-certified native feed coverage for one exact donor barrier. The
/// adapter proves contiguous feed application, not merely an advertised head;
/// current lease/read authority is checked separately after guarded handoff.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeCoverageReceipt {
    /// Exact claim-selected transfer session whose private stage was checked.
    pub parent: BootstrapOperation,
    /// Exact donor B whose native writer cuts are covered.
    pub barrier: BarrierReceipt,
    /// Last donor-local position applied in that same private stage.
    pub staged_through: JournalCursor,
    /// Actual contiguous native positions after buffered overlap handling.
    pub proven_cuts: Vec<NativeCut>,
    /// Complete currently observed member identities.
    pub members: Vec<BootstrapMemberIdentity>,
    /// Current bounded native-effect buffer memory charge.
    pub buffered_bytes: usize,
}

/// Adapter-certified atomic handoff from a private image to normal native
/// delivery. This is a continuity receipt, never local serving permission.
/// The application issues it only after a guarded candidate swap and feed
/// applier switch occur in one publication critical section.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeHandoffReceipt {
    /// Exact outer recovery acquisition whose guarded permit installed it.
    /// Claim and recovery sessions remain independent identities.
    pub recovery: RecoveryOperation,
    /// Exact guarded install operation whose callback performed the switch.
    pub install: BootstrapOperation,
    /// Previously accepted B, stage, membership, and native coverage proof.
    pub coverage: NativeCoverageReceipt,
    /// Donor stream held continuously until the normal applier took over.
    pub attachment: AttachToken,
    /// Installed private image schema, equal to the accepted donor offer.
    pub schema: u32,
    /// Fresh nonzero incarnation of the installed normal feed applier.
    pub applier_generation: u64,
    /// Contiguous native cuts owned by the normal applier after the switch.
    /// A quiet feed may legitimately have sequence zero.
    pub continued_cuts: Vec<NativeCut>,
    /// Remaining charged native overlap bytes, bounded by transfer policy.
    pub buffered_bytes: usize,
}

/// Finite private transfer phase.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransferStage {
    /// No transfer request has been issued.
    Unready,
    /// Waiting for an exact bounded offer.
    RequestingOffer,
    /// Reserving real private stage memory.
    ReservingStage,
    /// Reserving the donor's bounded suffix at C.
    ReservingDonor,
    /// Receiving and verifying one sequential chunk at a time.
    Receiving,
    /// Awaiting full-image commitment verification.
    VerifyingImage,
    /// Establishing the live donor suffix stream before B.
    Attaching,
    /// Waiting for an exact B and native cuts.
    RequestingBarrier,
    /// Applying donor-local suffix batches privately through B.
    Replaying,
    /// Waiting for exact donor ack/readback of one applied private batch.
    AcknowledgingBatch,
    /// Checking native feed coverage and bounded overlap after B.
    AwaitingNativeCoverage,
    /// Coverage is not yet complete; wait before sampling a later B.
    WaitingCoverage,
    /// Waiting for an application-guarded private stage install.
    Installing,
    /// Candidate handoff finished; separate application read gates still apply.
    Completed,
    /// Transfer failed closed; the claim parent chooses takeover or origin fallback.
    Aborted,
}

/// Explicit callback, failure, cancellation, or logical-time input.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransferEvent {
    /// Start from one exact claim-selected donor operation.
    Start,
    /// Bounded donor offer read back for the exact request.
    Offered {
        /// Exact offer fetch operation.
        op: BootstrapOperation,
        /// Bounded image metadata.
        offer: TransferOffer,
    },
    /// Runtime acquired private image and global memory permits.
    StageReserved {
        /// Exact stage reservation operation.
        op: BootstrapOperation,
    },
    /// Donor reserved the exact C suffix for this follower.
    DonorReserved {
        /// Exact donor reservation operation.
        op: BootstrapOperation,
        /// Fresh donor-side follower reservation.
        reservation: ReservationId,
    },
    /// One chunk passed length/integrity verification and was privately staged.
    ChunkStored {
        /// Exact chunk fetch operation.
        op: BootstrapOperation,
        /// Zero-based sequential chunk index.
        sequence: usize,
        /// Verified encoded chunk bytes.
        bytes: usize,
        /// Current bounded decoded stage charge.
        decoded_charge: usize,
    },
    /// Runtime verified the exact full image commitment.
    ImageVerified {
        /// Exact full-image verification operation.
        op: BootstrapOperation,
        /// Commitment verified against the offered image.
        commitment: [u8; 32],
    },
    /// Live donor stream is attached for this exact reservation.
    StreamAttached {
        /// Exact stream attachment operation.
        op: BootstrapOperation,
        /// Donor-issued attachment token.
        token: AttachToken,
    },
    /// Donor sampled B and native cuts after attachment.
    BarrierReceived {
        /// Exact barrier read/advance operation.
        op: BootstrapOperation,
        /// Atomic B, cuts, and member receipt.
        receipt: BarrierReceipt,
    },
    /// Bounded contiguous batch was applied to the volatile private stage.
    BatchStaged {
        /// Exact batch fetch operation.
        op: BootstrapOperation,
        /// Bounded applied donor batch.
        batch: JournalBatch,
    },
    /// Donor confirmed exact batch ack or its exact readback.
    BatchAcknowledged {
        /// Exact donor ack operation.
        op: BootstrapOperation,
        /// Last applied donor-local position.
        through: JournalCursor,
    },
    /// Adapter verified native stream continuity through B and safe overlap.
    NativeCovered {
        /// Exact coverage check operation.
        op: BootstrapOperation,
        /// Exact source- and stage-correlated coverage receipt, never a boolean.
        coverage: NativeCoverageReceipt,
    },
    /// Native feeds are still behind B; retain the attached donor bridge.
    NativePending {
        /// Exact native coverage check operation.
        op: BootstrapOperation,
    },
    /// Application atomically installed the private stage and switched ongoing
    /// native delivery under its publication guard.
    Installed {
        /// Exact guarded install operation.
        op: BootstrapOperation,
        /// Source- and stage-correlated atomic native continuation proof.
        handoff: Box<NativeHandoffReceipt>,
    },
    /// One current source/runtime operation failed or became unavailable.
    Failed {
        /// Exact failed operation.
        op: BootstrapOperation,
    },
    /// Monotone caller-supplied logical time.
    Tick(Time),
    /// Exact claim session was cancelled or superseded.
    Cancel,
}

/// Bounded work for a typed runtime/source adapter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransferEffect {
    /// Fetch a bounded source-certified offer.
    FetchOffer {
        /// Exact offer fetch operation.
        op: BootstrapOperation,
        /// Pre-allocation cap for offer variable metadata.
        max_metadata_bytes: usize,
    },
    /// Acquire real global private stage and image memory admission.
    ReserveStage {
        /// Exact stage reservation operation.
        op: BootstrapOperation,
        /// Exact accepted image cut, binding the private stage to C.
        image_cut: JournalCursor,
        /// Accepted finite image chunk count.
        chunks: usize,
        /// Encoded image memory to admit.
        encoded_bytes: usize,
        /// Decoded stage memory to admit.
        decoded_bytes: usize,
    },
    /// Reserve the donor's suffix from exact C before transfer.
    ReserveDonor {
        /// Exact donor reservation operation.
        op: BootstrapOperation,
        /// Exact source capture identity.
        capture: CaptureId,
        /// Image cut C.
        cut: JournalCursor,
    },
    /// Fetch and verify one bounded sequential chunk.
    FetchChunk {
        /// Exact chunk fetch operation.
        op: BootstrapOperation,
        /// Zero-based chunk sequence.
        sequence: usize,
        /// Maximum allowed encoded chunk bytes.
        max_bytes: usize,
    },
    /// Verify the assembled image's exact full commitment.
    VerifyImage {
        /// Exact full-image verification operation.
        op: BootstrapOperation,
        /// Offered full-image commitment.
        commitment: [u8; 32],
    },
    /// Attach the donor's live journal stream before sampling B.
    AttachStream {
        /// Exact attachment operation.
        op: BootstrapOperation,
        /// Exact donor reservation.
        reservation: ReservationId,
    },
    /// Request exact atomic B plus native cuts and members.
    FetchBarrier {
        /// Exact barrier read operation.
        op: BootstrapOperation,
        /// Attached donor reservation.
        reservation: ReservationId,
    },
    /// Sample B2 only after exact previous B was fully acknowledged.
    AdvanceBarrier {
        /// Exact barrier advancement operation.
        op: BootstrapOperation,
        /// Previously sampled, fully acknowledged B.
        expected: BarrierReceipt,
    },
    /// Fetch one bounded contiguous batch through the exact B.
    FetchBatch {
        /// Exact batch fetch operation.
        op: BootstrapOperation,
        /// Current atomic B and source metadata.
        receipt: BarrierReceipt,
    },
    /// Confirm the donor's exact batch ack or readback before proceeding.
    AckBatch {
        /// Exact ack/readback operation.
        op: BootstrapOperation,
        /// Attached donor reservation.
        reservation: ReservationId,
        /// Donor-local batch operation being acknowledged.
        batch_operation: u64,
        /// Last applied donor-local position.
        through: JournalCursor,
    },
    /// Check native feed coverage and safe buffered overlap through exact B.
    CheckNativeCoverage {
        /// Exact native coverage operation.
        op: BootstrapOperation,
        /// Current atomic B receipt.
        receipt: BarrierReceipt,
        /// Pre-allocation cap for buffered native effects.
        max_buffer_bytes: usize,
    },
    /// Ask the application to atomically install under current recovery/lease gates.
    InstallCandidate {
        /// Exact guarded install operation.
        op: BootstrapOperation,
        /// Exact previously accepted B, private-stage and native coverage
        /// receipt for the worker's guarded handoff callback.
        coverage: Box<NativeCoverageReceipt>,
    },
    /// Cancel and discard exact private stage/buffers; no serving grant.
    DiscardStage {
        /// Original claim-selected donor operation.
        parent: BootstrapOperation,
    },
    /// Drop the exact donor stream and reservation after resource retirement.
    ReleaseReservation(ReservationId),
    /// Immutable parent-constrained total transfer deadline.
    ArmTimer(Time),
}

/// Rejection reason; terminal failures emit cleanup effects as well.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransferError {
    /// Invalid finite limits or identity at construction.
    InvalidConfig,
    /// Input does not belong to the current phase.
    Stage,
    /// Reply belongs to a stale or forged operation, capture, or reservation.
    Stale,
    /// Trusted allocator returned a reused, wrong-session, or exhausted token.
    Allocator,
    /// Metadata, chunk, batch, or native buffer capacity exceeded.
    Capacity,
    /// Donor application schema is incompatible with the follower.
    Schema,
    /// Source returned conflicting, incomplete, corrupt, or unprovable continuity.
    Continuity,
    /// Caller-supplied time moved backward.
    BackwardTime,
    /// Immutable total transfer deadline elapsed.
    Expired,
}

/// One deterministic transition with effects and an optional rejection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransferStep {
    /// Bounded effects, including exact cleanup on terminal failure.
    pub effects: Vec<TransferEffect>,
    /// Rejection when an input could not be accepted.
    pub rejection: Option<TransferError>,
}

/// Exact scope and identities supplied by the composite claim engine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransferBinding {
    /// Original donor selection operation retained through transfer.
    pub parent: BootstrapOperation,
    /// Scope of one complete application index image.
    pub scope: BootstrapScope,
    /// Exact selected donor identity.
    pub donor: ClaimIdentity,
    /// Exact local follower identity; its boot/session/attempt match `parent`.
    pub follower: ClaimIdentity,
    /// Original min(donor wait, selection total) absolute deadline.
    pub due: Time,
}
