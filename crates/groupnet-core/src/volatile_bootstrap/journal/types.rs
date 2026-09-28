//! Bounded donor-local index transfer journal contract.

use crate::Time;

use super::super::{BootstrapScope, ClaimIdentity};

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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JournalConfig {
    /// Maximum encoded image bytes reserved before capture.
    pub max_encoded_bytes: usize,
    /// Maximum decoded/private image bytes reserved before capture.
    pub max_decoded_bytes: usize,
    /// Maximum retained delta events for the candidate.
    pub max_events: usize,
    /// Maximum retained delta identity plus effect bytes.
    pub max_suffix_bytes: usize,
    /// Maximum bytes of one identity plus effect.
    pub max_event_bytes: usize,
    /// Maximum bytes of one stable mutation identity.
    pub max_identity_bytes: usize,
    /// Maximum simultaneous follower reservations.
    pub max_followers: usize,
    /// Maximum bytes in one follower node identity.
    pub max_follower_id_bytes: usize,
    /// Maximum native writer cut entries.
    pub max_cuts: usize,
    /// Maximum total bytes of native writer IDs.
    pub max_cut_bytes: usize,
    /// Maximum exact member identities in the captured roster.
    pub max_members: usize,
    /// Maximum total bytes of the exact source-observed membership roster.
    pub max_membership_bytes: usize,
    /// Maximum combined bytes of the scope names.
    pub max_scope_bytes: usize,
    /// Maximum events in one returned batch.
    pub max_batch_events: usize,
    /// Maximum bytes in one returned batch.
    pub max_batch_bytes: usize,
    /// Maximum total bytes held by returned, unacked batches.
    pub max_inflight_bytes: usize,
    /// Maximum lifetime of a capture candidate.
    pub max_total_ms: u64,
    /// Maximum lifetime of one follower reservation.
    pub max_follower_ms: u64,
}

impl JournalConfig {
    /// Validate finite, internally compatible budgets.
    ///
    /// # Errors
    /// Returns [`JournalError::InvalidConfig`] for zero or inverted bounds.
    pub fn validate(self) -> Result<Self, JournalError> {
        if self.max_encoded_bytes == 0
            || self.max_decoded_bytes == 0
            || self.max_events == 0
            || self.max_suffix_bytes == 0
            || self.max_event_bytes == 0
            || self.max_identity_bytes == 0
            || self.max_followers == 0
            || self.max_follower_id_bytes == 0
            || self.max_cuts == 0
            || self.max_cut_bytes == 0
            || self.max_members == 0
            || self.max_membership_bytes == 0
            || self.max_scope_bytes == 0
            || self.max_batch_events == 0
            || self.max_batch_bytes == 0
            || self.max_inflight_bytes == 0
            || self.max_total_ms == 0
            || self.max_follower_ms == 0
            || self.max_event_bytes > self.max_suffix_bytes
            || self.max_event_bytes > self.max_batch_bytes
            || self.max_batch_bytes > self.max_inflight_bytes
            || self.max_identity_bytes > self.max_event_bytes
            || self.max_follower_ms > self.max_total_ms
        {
            return Err(JournalError::InvalidConfig);
        }
        Ok(self)
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
    pub members: Vec<ClaimIdentity>,
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
    /// Last included position; equal to `from` only for an empty result.
    pub through: JournalCursor,
    /// Contiguous deltas after `from`.
    pub deltas: Vec<JournalDelta>,
    /// Reserved cloned identity plus effect bytes.
    pub bytes: usize,
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
    /// Invalid construction or budget configuration.
    InvalidConfig,
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
/// also hold a real global memory permit for the private image and suffix;
/// this copyable value alone does not allocate or own heap capacity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CaptureCharge {
    /// Current encoded private image bytes.
    pub encoded_bytes: usize,
    /// Current decoded private image bytes.
    pub decoded_bytes: usize,
    /// Full bounded live suffix capacity reserved for the candidate.
    pub suffix_bytes: usize,
    /// Admission started at this caller-supplied logical time.
    pub started: Time,
}
