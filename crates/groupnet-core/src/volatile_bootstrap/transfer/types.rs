//! Typed, finite transfer inputs and effects. No type grants read authority.

use std::fmt;
use std::num::{NonZeroU32, NonZeroUsize};

use crate::Time;
use crate::volatile_recovery::RecoveryOperation;

use super::super::journal::{
    AttachToken, BarrierReceipt, CaptureId, JournalBatch, JournalConfig, JournalCursor, NativeCut,
    ReservationId,
};
use super::super::{BootstrapMemberIdentity, BootstrapOperation, BootstrapScope, ClaimIdentity};

/// Exact, sorted per-writer coverage; zero sequence denotes a quiet feed.
/// Positions compare epoch-major: a writer renewed into a newer epoch after a
/// sealed life covers every position of that life.
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
                && (found.epoch, found.sequence) >= (cut.epoch, cut.sequence)
        })
}

/// Finite per-transfer metadata, image, native buffer, and batch limits.
///
/// Budgets and the schema are nonzero by type; [`TransferConfig::validate`]
/// checks the coverage cadence and the chunk budgets against the image cap.
///
/// The expected schema identifies the application's image format, so there
/// is no default config: [`TransferConfig::new`] takes exactly the schema.
/// Its image, batch, replay, membership and cut budgets are those of
/// [`JournalConfig::default`] — a 64 MiB encoded and decoded image, 256-event
/// 1 MiB batches, the journal's 4096 retained events replayed, 1024 members
/// and 256 writer cuts — so a default follower fits a default donor; the rest
/// are its own: up to 1024 image chunks of 1 MiB, 1 MiB of offer metadata,
/// 16 MiB of buffered native effects and a 100 ms coverage poll.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TransferConfig {
    /// Application schema this follower can decode and install.
    pub expected_schema: NonZeroU32,
    /// Maximum combined variable metadata bytes in one offer.
    pub max_metadata_bytes: NonZeroUsize,
    /// Maximum encoded private image bytes.
    pub max_encoded_bytes: NonZeroUsize,
    /// Maximum decoded private stage bytes.
    pub max_decoded_bytes: NonZeroUsize,
    /// Maximum encoded bytes in one verified chunk; at most `max_encoded_bytes`.
    pub max_chunk_bytes: NonZeroUsize,
    /// Maximum number of image chunks; at most `max_encoded_bytes`.
    pub max_chunks: NonZeroUsize,
    /// Maximum bytes in one donor journal batch.
    pub max_batch_bytes: NonZeroUsize,
    /// Maximum events in one donor journal batch.
    pub max_batch_events: NonZeroUsize,
    /// Maximum total donor journal events replayed in one transfer.
    pub max_replay_events: NonZeroUsize,
    /// Maximum buffered native effects before source coverage.
    pub max_native_buffer_bytes: NonZeroUsize,
    /// Maximum complete member identities in offer and barrier.
    pub max_members: NonZeroUsize,
    /// Maximum complete native writer cuts in offer and barrier.
    pub max_cuts: NonZeroUsize,
    /// Minimum logical cadence between unchanged native-coverage checks;
    /// nonzero.
    pub coverage_poll_ms: u64,
}

/// Invalid [`TransferConfig`], naming the violated rule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransferConfigError {
    /// [`TransferConfig::coverage_poll_ms`] is zero.
    ZeroCoveragePoll,
    /// [`TransferConfig::max_chunk_bytes`] exceeds
    /// [`TransferConfig::max_encoded_bytes`].
    ChunkBytesExceedEncoded,
    /// [`TransferConfig::max_chunks`] exceeds [`TransferConfig::max_encoded_bytes`],
    /// so some chunk would be empty.
    ChunksExceedEncoded,
}

impl fmt::Display for TransferConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ZeroCoveragePoll => "transfer coverage_poll_ms must be nonzero",
            Self::ChunkBytesExceedEncoded => {
                "transfer max_chunk_bytes must not exceed max_encoded_bytes"
            }
            Self::ChunksExceedEncoded => "transfer max_chunks must not exceed max_encoded_bytes",
        })
    }
}

impl std::error::Error for TransferConfigError {}

impl TransferConfig {
    /// A follower config installing images of `expected_schema`, with the
    /// default budgets; refine them with struct-update syntax.
    #[must_use]
    pub const fn new(expected_schema: NonZeroU32) -> Self {
        let journal = JournalConfig::DEFAULT;
        Self {
            expected_schema,
            max_metadata_bytes: const { NonZeroUsize::new(1024 * 1024).unwrap() },
            max_encoded_bytes: journal.max_encoded_bytes,
            max_decoded_bytes: journal.max_decoded_bytes,
            max_chunk_bytes: const { NonZeroUsize::new(1024 * 1024).unwrap() },
            max_chunks: const { NonZeroUsize::new(1024).unwrap() },
            max_batch_bytes: journal.max_batch_bytes,
            max_batch_events: journal.max_batch_events,
            max_replay_events: journal.max_events,
            max_native_buffer_bytes: const { NonZeroUsize::new(16 * 1024 * 1024).unwrap() },
            max_members: journal.max_members,
            max_cuts: journal.max_cuts,
            coverage_poll_ms: 100,
        }
    }

    /// Rejects inverted or unprogressing finite budgets.
    ///
    /// # Errors
    /// Returns the [`TransferConfigError`] variant naming the first violated rule.
    pub fn validate(&self) -> Result<(), TransferConfigError> {
        if self.coverage_poll_ms == 0 {
            return Err(TransferConfigError::ZeroCoveragePoll);
        }
        if self.max_chunk_bytes > self.max_encoded_bytes {
            return Err(TransferConfigError::ChunkBytesExceedEncoded);
        }
        if self.max_chunks > self.max_encoded_bytes {
            return Err(TransferConfigError::ChunksExceedEncoded);
        }
        Ok(())
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
    pub encoded_bytes: NonZeroUsize,
    /// Maximum decoded private stage bytes charged before transfer.
    pub decoded_bytes: NonZeroUsize,
    /// Number of nonempty sequential chunks.
    pub chunks: NonZeroUsize,
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
        /// Verified encoded chunk bytes; a chunk is never empty.
        bytes: NonZeroUsize,
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
    /// Live native positions differ from B in either direction
    /// ([`CutAlignment::Pending`](crate::volatile_bootstrap::journal::CutAlignment));
    /// retain the attached bridge and sample a later barrier. Answers either
    /// a coverage check or an install that found its cuts moved.
    NativePending {
        /// Exact coverage check or install operation.
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
        max_metadata_bytes: NonZeroUsize,
    },
    /// Acquire real global private stage and image memory admission.
    ReserveStage {
        /// Exact stage reservation operation.
        op: BootstrapOperation,
        /// Exact accepted image cut, binding the private stage to C.
        image_cut: JournalCursor,
        /// Accepted finite image chunk count.
        chunks: NonZeroUsize,
        /// Encoded image memory to admit.
        encoded_bytes: NonZeroUsize,
        /// Decoded stage memory to admit.
        decoded_bytes: NonZeroUsize,
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
        max_bytes: NonZeroUsize,
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
    /// The transfer's current stall deadline, renewed as the transfer advances.
    ArmTimer(Time),
}

/// Rejection reason; terminal failures emit cleanup effects as well.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransferError {
    /// The [`TransferConfig`] is invalid.
    Config(TransferConfigError),
    /// The [`TransferBinding`] has an empty, zero, mismatched, or oversized
    /// identity, or its deadline already elapsed.
    InvalidBinding,
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
    /// The transfer made no progress for its whole stall bound.
    Expired,
    /// One runtime or source operation failed or outlived its deadline.
    Unavailable,
}

impl fmt::Display for TransferError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Config(_) => "invalid transfer config",
            Self::InvalidBinding => "invalid transfer binding",
            Self::Stage => "transfer input belongs to another phase",
            Self::Stale => "transfer reply is stale or forged",
            Self::Allocator => "transfer allocator returned an invalid token",
            Self::Capacity => "transfer capacity exceeded",
            Self::Schema => "donor schema is incompatible",
            Self::Continuity => "transfer source continuity is unprovable",
            Self::BackwardTime => "transfer time moved backward",
            Self::Expired => "transfer made no progress within its stall bound",
            Self::Unavailable => "transfer operation failed or timed out",
        })
    }
}

impl std::error::Error for TransferError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Config(error) => Some(error),
            _ => None,
        }
    }
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
    /// Initial stall bound: min(donor wait, selection total) from the start.
    /// The claim parent moves it later on each advance of the transfer.
    pub due: Time,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> TransferConfig {
        let nz = |n| NonZeroUsize::new(n).unwrap();
        TransferConfig {
            expected_schema: NonZeroU32::MIN,
            max_metadata_bytes: nz(32),
            max_encoded_bytes: nz(16),
            max_decoded_bytes: nz(16),
            max_chunk_bytes: nz(8),
            max_chunks: nz(4),
            max_batch_bytes: nz(16),
            max_batch_events: nz(3),
            max_replay_events: nz(5),
            max_native_buffer_bytes: nz(8),
            max_members: nz(2),
            max_cuts: nz(1),
            coverage_poll_ms: 2,
        }
    }

    #[test]
    fn new_config_is_valid_and_matches_the_default_journal() {
        let schema = NonZeroU32::new(7).unwrap();
        let config = TransferConfig::new(schema);
        assert_eq!(config.expected_schema, schema);
        assert_eq!(config.validate(), Ok(()));
        let journal = super::super::super::journal::JournalConfig::default();
        assert_eq!(config.max_encoded_bytes, journal.max_encoded_bytes);
        assert_eq!(config.max_decoded_bytes, journal.max_decoded_bytes);
        assert_eq!(config.max_batch_bytes, journal.max_batch_bytes);
        assert_eq!(config.max_batch_events, journal.max_batch_events);
        assert_eq!(config.max_replay_events, journal.max_events);
        assert_eq!(config.max_members, journal.max_members);
        assert_eq!(config.max_cuts, journal.max_cuts);
    }

    #[test]
    fn config_validate_names_each_rule() {
        assert_eq!(config().validate(), Ok(()));
        assert_eq!(
            TransferConfig {
                coverage_poll_ms: 0,
                ..config()
            }
            .validate(),
            Err(TransferConfigError::ZeroCoveragePoll)
        );
        assert_eq!(
            TransferConfig {
                max_chunk_bytes: NonZeroUsize::new(17).unwrap(),
                ..config()
            }
            .validate(),
            Err(TransferConfigError::ChunkBytesExceedEncoded)
        );
        assert_eq!(
            TransferConfig {
                max_chunks: NonZeroUsize::new(17).unwrap(),
                ..config()
            }
            .validate(),
            Err(TransferConfigError::ChunksExceedEncoded)
        );
        let error = TransferError::Config(TransferConfigError::ChunksExceedEncoded);
        assert_eq!(error.to_string(), "invalid transfer config");
        assert_eq!(
            std::error::Error::source(&error).map(ToString::to_string),
            Some("transfer max_chunks must not exceed max_encoded_bytes".to_owned())
        );
        assert!(std::error::Error::source(&TransferError::Stage).is_none());
    }
}
