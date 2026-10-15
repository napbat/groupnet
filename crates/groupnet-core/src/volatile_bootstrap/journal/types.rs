//! Bounded donor-local index transfer journal contract.

use std::fmt;
use std::num::NonZeroUsize;

use crate::Time;

use super::super::{BootstrapMemberIdentity, BootstrapScope, ClaimIdentity};
use super::DonorJournal;

/// A capture identity that cannot be reused by the same donor session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CaptureId {
    /// Application partition whose index was captured.
    pub scope: BootstrapScope,
    /// Fresh donor boot, session, and claim attempt.
    pub donor: ClaimIdentity,
    /// Guarded recovery generation that owns the index image.
    pub recovery_generation: u64,
    /// Fresh nonzero capture serial within the donor session.
    pub serial: u64,
}

/// Finite donor storage and per-follower transfer limits.
///
/// Every count and byte cap is nonzero by type; [`JournalConfig::validate`]
/// checks the millisecond lifetimes, the caps' relations to each other, and
/// that [`DonorJournal::storage_bound`] is representable.
///
/// [`Default`] reserves a 64 MiB encoded and 64 MiB decoded image, retains
/// 4096 delta events of at most 4 KiB (16 MiB in total, 256-byte identities),
/// serves eight followers in 256-event, 1 MiB batches with 4 MiB in flight,
/// captures up to 256 writer cuts (8 KiB of writer IDs) and 1024 members
/// (256 KiB), and keeps a candidate for five minutes and a follower
/// reservation for two.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JournalConfig {
    /// Maximum encoded image bytes reserved before capture.
    pub max_encoded_bytes: NonZeroUsize,
    /// Maximum decoded/private image bytes reserved before capture.
    pub max_decoded_bytes: NonZeroUsize,
    /// Maximum retained delta events for the candidate.
    pub max_events: NonZeroUsize,
    /// Maximum retained delta identity plus effect bytes.
    pub max_suffix_bytes: NonZeroUsize,
    /// Maximum bytes of one identity plus effect; at most `max_suffix_bytes`
    /// and `max_batch_bytes`.
    pub max_event_bytes: NonZeroUsize,
    /// Maximum bytes of one stable mutation identity; at most `max_event_bytes`.
    pub max_identity_bytes: NonZeroUsize,
    /// Maximum simultaneous follower reservations.
    pub max_followers: NonZeroUsize,
    /// Maximum bytes in one follower node identity.
    pub max_follower_id_bytes: NonZeroUsize,
    /// Maximum native writer cut entries.
    pub max_cuts: NonZeroUsize,
    /// Maximum total bytes of native writer IDs.
    pub max_cut_bytes: NonZeroUsize,
    /// Maximum exact member identities in the captured roster.
    pub max_members: NonZeroUsize,
    /// Maximum total bytes of the exact source-observed membership roster.
    pub max_membership_bytes: NonZeroUsize,
    /// Maximum combined bytes of the scope names.
    pub max_scope_bytes: NonZeroUsize,
    /// Maximum events in one returned batch.
    pub max_batch_events: NonZeroUsize,
    /// Maximum bytes in one returned batch; at most `max_inflight_bytes`.
    pub max_batch_bytes: NonZeroUsize,
    /// Maximum total bytes held by returned, unacked batches.
    pub max_inflight_bytes: NonZeroUsize,
    /// Maximum lifetime of a capture candidate, in logical milliseconds; nonzero.
    pub max_total_ms: u64,
    /// Maximum lifetime of one follower reservation, in logical milliseconds;
    /// nonzero and at most `max_total_ms`.
    pub max_follower_ms: u64,
}

impl Default for JournalConfig {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl JournalConfig {
    /// The [`Default`] budgets, in a `const` so the follower's
    /// [`TransferConfig::new`](super::super::transfer::TransferConfig::new)
    /// composes its matching budgets from this one definition.
    pub(crate) const DEFAULT: Self = Self {
        max_encoded_bytes: const { NonZeroUsize::new(64 * 1024 * 1024).unwrap() },
        max_decoded_bytes: const { NonZeroUsize::new(64 * 1024 * 1024).unwrap() },
        max_events: const { NonZeroUsize::new(4096).unwrap() },
        max_suffix_bytes: const { NonZeroUsize::new(16 * 1024 * 1024).unwrap() },
        max_event_bytes: const { NonZeroUsize::new(4096).unwrap() },
        max_identity_bytes: const { NonZeroUsize::new(256).unwrap() },
        max_followers: const { NonZeroUsize::new(8).unwrap() },
        max_follower_id_bytes: const { NonZeroUsize::new(256).unwrap() },
        max_cuts: const { NonZeroUsize::new(256).unwrap() },
        max_cut_bytes: const { NonZeroUsize::new(8 * 1024).unwrap() },
        max_members: const { NonZeroUsize::new(1024).unwrap() },
        max_membership_bytes: const { NonZeroUsize::new(256 * 1024).unwrap() },
        max_scope_bytes: const { NonZeroUsize::new(512).unwrap() },
        max_batch_events: const { NonZeroUsize::new(256).unwrap() },
        max_batch_bytes: const { NonZeroUsize::new(1024 * 1024).unwrap() },
        max_inflight_bytes: const { NonZeroUsize::new(4 * 1024 * 1024).unwrap() },
        max_total_ms: 300_000,
        max_follower_ms: 120_000,
    };
}

/// Invalid [`JournalConfig`], naming the violated rule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JournalConfigError {
    /// [`JournalConfig::max_total_ms`] is zero.
    ZeroMaxTotal,
    /// [`JournalConfig::max_follower_ms`] is zero.
    ZeroMaxFollower,
    /// [`JournalConfig::max_follower_ms`] exceeds [`JournalConfig::max_total_ms`].
    FollowerExceedsTotal,
    /// [`JournalConfig::max_event_bytes`] exceeds [`JournalConfig::max_suffix_bytes`].
    EventExceedsSuffix,
    /// [`JournalConfig::max_event_bytes`] exceeds [`JournalConfig::max_batch_bytes`].
    EventExceedsBatch,
    /// [`JournalConfig::max_batch_bytes`] exceeds [`JournalConfig::max_inflight_bytes`].
    BatchExceedsInflight,
    /// [`JournalConfig::max_identity_bytes`] exceeds [`JournalConfig::max_event_bytes`].
    IdentityExceedsEvent,
    /// The caps' product, [`DonorJournal::storage_bound`], overflows `usize`.
    StorageBoundOverflow,
}

impl fmt::Display for JournalConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ZeroMaxTotal => "journal max_total_ms must be nonzero",
            Self::ZeroMaxFollower => "journal max_follower_ms must be nonzero",
            Self::FollowerExceedsTotal => "journal max_follower_ms must not exceed max_total_ms",
            Self::EventExceedsSuffix => "journal max_event_bytes must not exceed max_suffix_bytes",
            Self::EventExceedsBatch => "journal max_event_bytes must not exceed max_batch_bytes",
            Self::BatchExceedsInflight => {
                "journal max_batch_bytes must not exceed max_inflight_bytes"
            }
            Self::IdentityExceedsEvent => {
                "journal max_identity_bytes must not exceed max_event_bytes"
            }
            Self::StorageBoundOverflow => "journal storage bound overflows usize",
        })
    }
}

impl std::error::Error for JournalConfigError {}

impl JournalConfig {
    /// Validate finite, internally compatible budgets.
    ///
    /// # Errors
    /// Returns the [`JournalConfigError`] variant naming the first violated rule.
    pub fn validate(&self) -> Result<(), JournalConfigError> {
        if self.max_total_ms == 0 {
            return Err(JournalConfigError::ZeroMaxTotal);
        }
        if self.max_follower_ms == 0 {
            return Err(JournalConfigError::ZeroMaxFollower);
        }
        if self.max_follower_ms > self.max_total_ms {
            return Err(JournalConfigError::FollowerExceedsTotal);
        }
        if self.max_event_bytes > self.max_suffix_bytes {
            return Err(JournalConfigError::EventExceedsSuffix);
        }
        if self.max_event_bytes > self.max_batch_bytes {
            return Err(JournalConfigError::EventExceedsBatch);
        }
        if self.max_batch_bytes > self.max_inflight_bytes {
            return Err(JournalConfigError::BatchExceedsInflight);
        }
        if self.max_identity_bytes > self.max_event_bytes {
            return Err(JournalConfigError::IdentityExceedsEvent);
        }
        if DonorJournal::checked_storage_bound(self).is_none() {
            return Err(JournalConfigError::StorageBoundOverflow);
        }
        Ok(())
    }
}

/// A source-native writer cut, scoped to one immutable writer incarnation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeCut {
    /// Stable native writer identity.
    pub writer: Vec<u8>,
    /// Source writer incarnation or epoch.
    pub epoch: u64,
    /// Last covered source sequence; zero may denote an empty writer feed.
    pub sequence: u64,
}

/// A native writer's proven seal and the newer epoch it continues in, for
/// [`DonorJournal::renew`]: the writer sealed its life exactly at `sealed`,
/// the application applied that life through the seal, and the writer now
/// continues in `epoch` from sequence zero.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeRenewal {
    /// The sealed position; it must be exactly the writer's covered cut.
    pub sealed: NativeCut,
    /// The newer epoch the writer continues in; must exceed `sealed.epoch`.
    pub epoch: u64,
}

/// Encoded and decoded byte sizes of one private index image; the journal
/// never admits an empty image.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ImageSize {
    /// Encoded image bytes.
    pub encoded_bytes: NonZeroUsize,
    /// Decoded private image bytes.
    pub decoded_bytes: NonZeroUsize,
}

/// What [`DonorJournal::begin_capture`] admits before the adapter clones the
/// index at C.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapturePlan {
    /// Planned image budgets, each at most its [`JournalConfig`] maximum;
    /// [`DonorJournal::finish_capture`] may report less, never more.
    pub image: ImageSize,
    /// Exact complete member identities at C, sorted by node.
    pub members: Vec<BootstrapMemberIdentity>,
    /// Exact native per-writer cuts at C, sorted by writer.
    pub cuts: Vec<NativeCut>,
}

/// Stable mutation identity; every index effect must enter the journal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeltaIdentity {
    /// Native feed effect with a comparable per-writer position.
    Native(NativeCut),
    /// Origin-validated repair or other local effect without a native ID.
    Local(Vec<u8>),
}

/// One contiguous, donor-local journal position; not a source-commit cursor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalCursor {
    /// Exact image capture to which this position belongs.
    pub capture: CaptureId,
    /// Number of captured deltas through this position; C begins at zero.
    pub position: u64,
}

/// Exact reservation incarnation for one follower transfer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReservationId {
    /// Exact donor capture.
    pub capture: CaptureId,
    /// Fresh follower boot/session/attempt.
    pub follower: ClaimIdentity,
    /// Increasing reservation serial, never reused during the candidate.
    pub serial: u64,
}

/// Correlation for one follower's stream attachment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttachToken {
    /// Exact follower reservation.
    pub reservation: ReservationId,
    /// Unique journal operation token.
    pub operation: u64,
}

/// One exact donor-local barrier sampled after stream attachment. Its cuts
/// cannot be substituted with later `covered_cuts()` observations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BarrierReceipt {
    /// Exact attached follower reservation.
    pub reservation: ReservationId,
    /// Exact confirmed stream attachment operation.
    pub attach_operation: u64,
    /// Unique operation for this exact sampled barrier and covered cuts.
    pub barrier_operation: u64,
    /// Donor-local B position.
    pub cursor: JournalCursor,
    /// Native writer cuts sampled atomically with B.
    pub covered_cuts: Vec<NativeCut>,
    /// Exact complete membership identities at B.
    pub members: Vec<BootstrapMemberIdentity>,
}

/// One final index effect in donor publication order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalDelta {
    /// Contiguous local position, beginning at one after image cut C.
    pub position: u64,
    /// Stable effect identity for exact duplicate detection.
    pub identity: DeltaIdentity,
    /// Bounded adapter-defined final index mutation, including tombstones.
    pub effect: Vec<u8>,
}

/// Bounded private transfer copy, charged until its exact ack or release.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalBatch {
    /// Exact follower reservation.
    pub reservation: ReservationId,
    /// Unique operation token for this returned batch.
    pub operation: u64,
    /// Last previously acknowledged position.
    pub from: JournalCursor,
    /// Last included position; a batch always advances past `from`.
    pub through: JournalCursor,
    /// Contiguous deltas after `from`.
    pub deltas: Vec<JournalDelta>,
    /// Reserved cloned identity plus effect bytes; a batch always charges
    /// at least one byte.
    pub bytes: NonZeroUsize,
}

/// Candidate lifecycle; none of these states grants local read authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JournalState {
    /// No image capture has started.
    Uncaptured,
    /// Private image memory is reserved while the adapter clones under lock.
    Capturing,
    /// Complete private image and bounded suffix are available for transfer.
    Active,
    /// Candidate failed closed; reservations and suffix were discarded.
    Invalidated,
}

/// Reason a candidate can no longer be transferred.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Invalidation {
    /// Donor feed gap or missing mutation.
    Gap,
    /// Donor lease lapse or serving revocation.
    Lapse,
    /// New guarded origin rebuild superseded this image.
    Rebuild,
    /// Complete membership continuity changed or could not be verified.
    Membership,
    /// Donor stopped or its recovery permission ended.
    DonorLost,
    /// Bounded journal, image, or in-flight capacity was exceeded.
    Capacity,
    /// Native identity or effect conflicted with retained history.
    Conflict,
    /// Candidate total lifetime elapsed.
    Expired,
}

/// Fail-closed journal error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JournalError {
    /// The [`JournalConfig`] is invalid.
    Config(JournalConfigError),
    /// The [`CaptureId`] has an empty, zero, or oversized field.
    InvalidIdentity,
    /// Wrong candidate phase.
    Stage,
    /// Wrong capture, follower reservation, operation, or cursor.
    Stale,
    /// Explicit resource limit was reached.
    Capacity,
    /// Conflicting duplicate or native writer progression.
    Conflict,
    /// Logical time moved backward.
    BackwardTime,
    /// Checked integer or logical-time arithmetic exhausted.
    Exhausted,
    /// Candidate total lifetime elapsed.
    Expired,
}

impl fmt::Display for JournalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Config(_) => "invalid journal config",
            Self::InvalidIdentity => "invalid journal capture identity",
            Self::Stage => "wrong journal candidate phase",
            Self::Stale => "stale journal capture, reservation, operation, or cursor",
            Self::Capacity => "journal resource limit reached",
            Self::Conflict => "conflicting journal duplicate or writer progression",
            Self::BackwardTime => "journal time moved backward",
            Self::Exhausted => "journal checked arithmetic exhausted",
            Self::Expired => "journal candidate lifetime elapsed",
        })
    }
}

impl std::error::Error for JournalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Config(error) => Some(error),
            _ => None,
        }
    }
}

/// One bounded reservation's current attachment and batch state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReservationStage {
    /// Reserved but stream is not attached.
    Reserved,
    /// Awaiting exact attach confirmation.
    Attaching,
    /// Stream is attached; barrier and batches may be requested.
    Attached,
}

/// Logical image charge from a successful capture admission. The runtime must
/// also hold real global memory permits for the private image and the
/// whole-journal [`super::DonorJournal::storage_bound`]. The latter exceeds the
/// logical suffix limit. This copyable value does not own heap capacity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CaptureCharge {
    /// Current private image bytes.
    pub image: ImageSize,
    /// Logical identity-plus-effect suffix limit, excluding retained journal
    /// headers, barrier copies, and vector capacity — the journal's
    /// [`JournalConfig::max_suffix_bytes`], positive by construction.
    pub suffix_bytes: NonZeroUsize,
    /// Admission started at this caller-supplied logical time.
    pub started: Time,
}
