//! Bounded metadata for opt-in source-backed state snapshots.

use super::{BoundComparison, Cursor, Scope, SourceProof};
use crate::Time;

/// Finite resource and time limits for one state-sync snapshot attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotConfig {
    /// Maximum combined variable-length offer metadata and certificate bytes.
    /// One offer's fixed struct overhead is bounded separately.
    pub max_metadata_bytes: usize,
    /// Maximum encoded image bytes.
    pub max_snapshot_bytes: u64,
    /// Maximum number of sequential chunks.
    pub max_chunks: u32,
    /// Maximum encoded bytes in one chunk.
    pub max_chunk_bytes: usize,
    /// Maximum charged decoded/private candidate bytes.
    pub max_candidate_bytes: u64,
    /// Total logical time from before hold acquisition through ready admission.
    pub max_total_ms: u64,
}

impl SnapshotConfig {
    pub(super) fn valid(self) -> bool {
        self.max_metadata_bytes > 0
            && self.max_snapshot_bytes > 0
            && self.max_chunks > 0
            && self.max_chunk_bytes > 0
            && self.max_candidate_bytes > 0
            && self.max_total_ms > 0
    }
}

/// Source-certified finite hold bound to the exact acquire request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HoldReceipt {
    /// Exact acquire operation, including session and generation.
    pub request: super::Operation,
    /// Source history whose suffix is retained for this attempt.
    pub history: super::SourceHistory,
    /// Trusted adapter certifies retention at least until this local deadline.
    pub retained_until: Time,
    /// Bounded source-specific certificate, validated by the adapter.
    pub certificate: Vec<u8>,
}

/// Consistent source cut and bounded image description under one hold.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotOffer {
    /// Snapshot scope, exactly matching the session.
    pub scope: Scope,
    /// Protocol or application snapshot schema identifier.
    pub schema: Vec<u8>,
    /// Consistent source-native cut represented by the image.
    pub cut: Cursor,
    /// Proof of the cut and retained suffix.
    pub proof: SourceProof,
    /// Exact comparison of cut with the held source head.
    pub cut_to_head: BoundComparison,
    /// Exact comparison of retention boundary with cut.
    pub retained_to_cut: BoundComparison,
    /// Exact encoded image size.
    pub total_bytes: u64,
    /// Number of sequential chunks.
    pub chunks: u32,
    /// Source-verified digest of the complete image.
    pub digest: Vec<u8>,
    /// Source-specific consistent-cut certificate.
    pub certificate: Vec<u8>,
}

impl SnapshotOffer {
    pub(super) fn charged_metadata_bytes(&self) -> Option<usize> {
        let cursors = [
            &self.cut,
            &self.proof.head,
            &self.proof.retained_from,
            &self.cut_to_head.left,
            &self.cut_to_head.right,
            &self.retained_to_cut.left,
            &self.retained_to_cut.right,
        ];
        let fields = [
            self.schema.len(),
            self.digest.len(),
            self.certificate.len(),
            self.proof.id.0.len(),
            self.cut_to_head.proof.0.len(),
            self.retained_to_cut.proof.0.len(),
            self.scope.stream.group.len(),
            self.scope.stream.topic.len(),
            self.scope.stream.kind.len(),
            self.scope.partition.len(),
        ];
        let mut total = fields.into_iter().try_fold(0usize, usize::checked_add)?;
        for cursor in cursors {
            for len in [
                cursor.scope.stream.group.len(),
                cursor.scope.stream.topic.len(),
                cursor.scope.stream.kind.len(),
                cursor.scope.partition.len(),
                cursor.history.source.len(),
                cursor.position.len(),
            ] {
                total = total.checked_add(len)?;
            }
        }
        Some(total)
    }
}

/// Exact chunk boundaries; payload bytes remain in the worker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChunkReceipt {
    /// Zero-based chunk index.
    pub index: u32,
    /// Offset in the encoded image.
    pub offset: u64,
    /// Bounded nonzero encoded chunk length.
    pub bytes: usize,
    /// Worker-held payload identifier, equal to its read operation token.
    pub payload_id: u64,
}
