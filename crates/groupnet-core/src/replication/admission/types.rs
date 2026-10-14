//! Shared identities and source-bound proof checks.

use std::fmt;
use std::num::NonZeroUsize;

use crate::replication::{BoundComparison, Comparison, Cursor, Scope, SourceHistory, SourceProof};

use super::ExpiryTiming;

const MAX_POLICY_FINGERPRINT_BYTES: usize = 256;
const MAX_POLICY_WAITERS: usize = 4096;
const MAX_POLICY_CURSOR_BYTES: usize = 4096;

/// Fleet-wide admission parameters, persisted by the source adapter.
///
/// Counts and byte caps are nonzero by type; [`AdmissionPolicy::validate`]
/// checks identities, upper caps, and the [`ExpiryTiming`] arithmetic.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmissionPolicy {
    /// Source history that orders admissions and intents.
    pub history: SourceHistory,
    /// Opaque stable fingerprint of this exact fleet policy; 1 to 256 bytes.
    pub fingerprint: Vec<u8>,
    /// Maximum local admission duration in milliseconds; nonzero.
    pub max_duration_ms: u64,
    /// Upper clock-rate ratio numerator, at least the denominator.
    pub rate_numerator: u64,
    /// Upper clock-rate ratio denominator; nonzero.
    pub rate_denominator: u64,
    /// Conservative writer-clock quantization and scheduling margin, nonzero.
    pub clock_margin_ms: u64,
    /// Maximum simultaneous earlier admissions in one writer waitset; at
    /// most 4096.
    pub max_waiters: NonZeroUsize,
    /// Maximum encoded native cursor or source proof size; at most 4096.
    pub max_cursor_bytes: NonZeroUsize,
}

/// A rejected or unrepresentable fleet policy, naming the violated rule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PolicyError {
    /// [`AdmissionPolicy::history`] has an empty source.
    EmptyHistorySource,
    /// [`AdmissionPolicy::history`]'s source exceeds
    /// [`AdmissionPolicy::max_cursor_bytes`].
    HistorySourceTooLarge,
    /// [`AdmissionPolicy::fingerprint`] is empty.
    EmptyFingerprint,
    /// [`AdmissionPolicy::fingerprint`] exceeds 256 bytes.
    FingerprintTooLarge,
    /// [`AdmissionPolicy::max_waiters`] exceeds 4096.
    TooManyWaiters,
    /// [`AdmissionPolicy::max_cursor_bytes`] exceeds 4096.
    CursorBytesTooLarge,
    /// [`ExpiryTiming::max_duration_ms`] is zero.
    ZeroMaxDuration,
    /// [`ExpiryTiming::rate_denominator`] is zero.
    ZeroRateDenominator,
    /// [`ExpiryTiming::rate_numerator`] is below its denominator.
    RateBelowOne,
    /// [`ExpiryTiming::clock_margin_ms`] is zero.
    ZeroClockMargin,
    /// Conservative expiry cannot be represented in the logical clock.
    Overflow,
}

impl fmt::Display for PolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::EmptyHistorySource => "admission policy history source must be nonempty",
            Self::HistorySourceTooLarge => {
                "admission policy history source exceeds max_cursor_bytes"
            }
            Self::EmptyFingerprint => "admission policy fingerprint must be nonempty",
            Self::FingerprintTooLarge => "admission policy fingerprint exceeds 256 bytes",
            Self::TooManyWaiters => "admission policy max_waiters exceeds 4096",
            Self::CursorBytesTooLarge => "admission policy max_cursor_bytes exceeds 4096",
            Self::ZeroMaxDuration => "admission max_duration_ms must be nonzero",
            Self::ZeroRateDenominator => "admission rate_denominator must be nonzero",
            Self::RateBelowOne => "admission rate_numerator must be at least rate_denominator",
            Self::ZeroClockMargin => "admission clock_margin_ms must be nonzero",
            Self::Overflow => "admission expiry wait overflows the logical clock",
        })
    }
}

impl std::error::Error for PolicyError {}

impl AdmissionPolicy {
    /// Validate all finite bounds and integer clock-rate arithmetic.
    ///
    /// # Errors
    /// Returns the [`PolicyError`] variant naming the first violated rule.
    pub fn validate(&self) -> Result<(), PolicyError> {
        if self.history.source.is_empty() {
            return Err(PolicyError::EmptyHistorySource);
        }
        if self.fingerprint.is_empty() {
            return Err(PolicyError::EmptyFingerprint);
        }
        if self.fingerprint.len() > MAX_POLICY_FINGERPRINT_BYTES {
            return Err(PolicyError::FingerprintTooLarge);
        }
        if self.max_waiters.get() > MAX_POLICY_WAITERS {
            return Err(PolicyError::TooManyWaiters);
        }
        if self.max_cursor_bytes.get() > MAX_POLICY_CURSOR_BYTES {
            return Err(PolicyError::CursorBytesTooLarge);
        }
        if self.history.source.len() > self.max_cursor_bytes.get() {
            return Err(PolicyError::HistorySourceTooLarge);
        }
        self.timing().validate()
    }

    /// The reusable finite-window timing bound for this fleet policy.
    #[must_use]
    pub const fn timing(&self) -> ExpiryTiming {
        ExpiryTiming {
            max_duration_ms: self.max_duration_ms,
            rate_numerator: self.rate_numerator,
            rate_denominator: self.rate_denominator,
            clock_margin_ms: self.clock_margin_ms,
        }
    }

    /// Conservative writer-clock wait for every earlier admission to expire.
    ///
    /// # Errors
    /// Returns the [`PolicyError`] timing variant for an invalid timing bound
    /// or [`PolicyError::Overflow`] if checked arithmetic fails.
    pub fn expiry_wait_ms(&self) -> Result<u64, PolicyError> {
        self.timing().expiry_wait_ms()
    }
}

/// Stable identity of one source admission, distinct across process boots.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AdmissionId {
    /// Stable reader name, which may be reused by a later process.
    pub reader: String,
    /// Unique incarnation for this admission, including renewals.
    pub incarnation: u64,
}

/// Exact native source record for an earlier reader admission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmissionRef {
    /// Reader and admission incarnation.
    pub id: AdmissionId,
    /// Immutable position of the admission record.
    pub cursor: Cursor,
    /// Persisted fleet policy that constrained its deadline.
    pub policy_fingerprint: Vec<u8>,
}

/// Source adapter's confirmation of an exact appended record.
/// The adapter must certify both the native position and the immutable
/// record payload/policy binding; a cursor-at-head comparison alone is not
/// evidence that the intended record occupies the slot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceReceipt {
    /// Exact payload identity certified at the durable position.
    pub binding: RecordBinding,
    /// Persisted fleet policy of that record.
    pub policy_fingerprint: Vec<u8>,
    /// Native cursor of the exact durable record.
    pub cursor: Cursor,
    /// Source head and history statement that includes the record.
    pub proof: SourceProof,
    /// Exact-bound comparison of record position to that head.
    pub record_to_head: BoundComparison,
}

/// Native record identity certified by a source append readback.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecordBinding {
    /// Exact reader admission payload identity.
    Admission(AdmissionId),
    /// Exact writer intent operation identity.
    Intent(String),
}

pub(super) fn valid_receipt(
    receipt: &SourceReceipt,
    scope: &Scope,
    policy: &AdmissionPolicy,
) -> bool {
    receipt.cursor.history == policy.history
        && receipt.policy_fingerprint == policy.fingerprint
        && receipt
            .cursor
            .validate(scope, policy.max_cursor_bytes.get())
            .is_ok()
        && receipt
            .proof
            .validate(scope, policy.max_cursor_bytes.get())
            .is_ok()
        && receipt.proof.head.history == policy.history
        && matches!(
            receipt.record_to_head.for_operands(
                &receipt.cursor,
                &receipt.proof.head,
                &receipt.proof.id
            ),
            Some(Comparison::Before | Comparison::Equal)
        )
}

pub(super) fn valid_scope(scope: &Scope, max_bytes: usize) -> bool {
    [
        &scope.stream.group,
        &scope.stream.topic,
        &scope.stream.kind,
        &scope.partition,
    ]
    .iter()
    .all(|name| !name.is_empty() && name.len() <= max_bytes)
}
