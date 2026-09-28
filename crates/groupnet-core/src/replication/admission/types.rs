//! Shared identities and source-bound proof checks.

use crate::replication::{BoundComparison, Comparison, Cursor, Scope, SourceHistory, SourceProof};

use super::ExpiryTiming;

/// Fleet-wide admission parameters, persisted by the source adapter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmissionPolicy {
    /// Source history that orders admissions and intents.
    pub history: SourceHistory,
    /// Opaque stable fingerprint of this exact fleet policy.
    pub fingerprint: Vec<u8>,
    /// Maximum local admission duration in milliseconds.
    pub max_duration_ms: u64,
    /// Upper clock-rate ratio numerator, at least the denominator.
    pub rate_numerator: u64,
    /// Upper clock-rate ratio denominator.
    pub rate_denominator: u64,
    /// Conservative writer-clock quantization and scheduling margin, nonzero.
    pub clock_margin_ms: u64,
    /// Maximum simultaneous earlier admissions in one writer waitset.
    pub max_waiters: usize,
    /// Maximum encoded native cursor or source proof size.
    pub max_cursor_bytes: usize,
}

/// A rejected or unrepresentable fleet policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PolicyError {
    /// An identity, duration, or bound is absent or unsupported.
    Invalid,
    /// Conservative expiry cannot be represented in the logical clock.
    Overflow,
}

impl AdmissionPolicy {
    /// Validate all finite bounds and integer clock-rate arithmetic.
    ///
    /// # Errors
    /// Returns [`PolicyError`] for invalid or overflowing policy values.
    pub fn validate(&self) -> Result<(), PolicyError> {
        if self.history.source.is_empty()
            || self.fingerprint.is_empty()
            || self.fingerprint.len() > 256
            || self.max_waiters == 0
            || self.max_waiters > 4096
            || self.max_cursor_bytes == 0
            || self.max_cursor_bytes > 4096
            || self.history.source.len() > self.max_cursor_bytes
        {
            return Err(PolicyError::Invalid);
        }
        self.timing().validate()?;
        Ok(())
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
    /// Returns [`PolicyError::Invalid`] for invalid timing bounds or
    /// [`PolicyError::Overflow`] if checked arithmetic fails.
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
            .validate(scope, policy.max_cursor_bytes)
            .is_ok()
        && receipt
            .proof
            .validate(scope, policy.max_cursor_bytes)
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
