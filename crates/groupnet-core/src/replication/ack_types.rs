//! Exact, bounded evidence for named acknowledgement waits.
//!
//! A source adapter certifies the fixed roster and each acknowledgement. The
//! core checks their binding; it cannot authenticate arbitrary application
//! bytes or infer a source order from encoded native positions.

use crate::Time;

use super::{Cursor, Operation, Scope, SourceHistory};

/// Milestone a named subscriber must prove for one committed target.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AckKind {
    /// Stale local serving was revoked for this exact mutation intent.
    Invalidated,
    /// Query-visible application effects reached this exact native cursor.
    Materialized,
}

/// Fixed source target; intent IDs are opaque and native cursors are not byte ordered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AckTarget {
    /// A mutation intent in one authoritative source history.
    Intent {
        /// Native stream and partition.
        scope: Scope,
        /// Authoritative source history.
        history: SourceHistory,
        /// Stable source-native intent identifier.
        id: Vec<u8>,
    },
    /// The exact source-native materialization position.
    Cursor(Cursor),
}

impl AckTarget {
    /// Target scope.
    #[must_use]
    pub fn scope(&self) -> &Scope {
        match self {
            Self::Intent { scope, .. } => scope,
            Self::Cursor(cursor) => &cursor.scope,
        }
    }

    /// Target authoritative history.
    #[must_use]
    pub fn history(&self) -> &SourceHistory {
        match self {
            Self::Intent { history, .. } => history,
            Self::Cursor(cursor) => &cursor.history,
        }
    }
}

/// Named subscriber pinned to a source-certified registration epoch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequiredSubscriber {
    /// Stable subscriber name within the target scope.
    pub name: String,
    /// Fresh process incarnation, distinct from the stable name.
    pub incarnation: u64,
    /// Exact source-registered subscription epoch.
    pub epoch: Vec<u8>,
}

/// Trusted source adapter's fixed required set for one exact wait.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CertifiedRoster {
    /// Exact target the source certified.
    pub target: AckTarget,
    /// Required proof kind, fixed for the life of the wait.
    pub kind: AckKind,
    /// Version of the configured/static roster policy.
    pub policy_version: u64,
    /// Opaque source certificate identity, verified by the adapter.
    pub certificate: Vec<u8>,
    /// Fixed named subscriber registrations. Lease holders need a separate
    /// source-ordered admission proof and are not accepted by this first slice.
    pub required: Vec<RequiredSubscriber>,
}

/// Request for one fixed-set acknowledgement wait.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AckWaitRequest {
    /// Stable caller request ID for retry/readback across process restarts.
    pub request_id: Vec<u8>,
    /// Exact externally committed target; this does not commit it.
    pub target: AckTarget,
    /// Milestone required from every pinned subscriber.
    pub kind: AckKind,
    /// Source-certified fixed named roster for this target and kind.
    pub roster: CertifiedRoster,
    /// Absolute logical deadline sampled by the caller.
    pub due: Time,
}

/// Source-confirmed acknowledgement stamped with this wait's local operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AckEvidence {
    /// Fresh local operation allocated by the owning `SessionEngine`.
    pub op: Operation,
    /// Stable external request ID recorded or read back by the source.
    pub request_id: Vec<u8>,
    /// Exact fixed target, including source scope and history.
    pub target: AckTarget,
    /// Exact proof kind; later progress of another kind does not count.
    pub kind: AckKind,
    /// Source certificate identity of the fixed roster.
    pub roster_certificate: Vec<u8>,
    /// Named registration that supplied this acknowledgement.
    pub subscriber: RequiredSubscriber,
}

/// Hard memory and time caps for one named wait.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AckWaitLimits {
    /// Maximum number of pinned named subscribers.
    pub max_required: usize,
    /// Maximum bytes in any identity, epoch, intent, or cursor field.
    pub max_identity_bytes: usize,
    /// Maximum source certificate bytes.
    pub max_certificate_bytes: usize,
    /// Maximum combined variable-length wait metadata bytes.
    pub max_metadata_bytes: usize,
    /// Maximum wait duration in logical milliseconds.
    pub max_wait_ms: u64,
    /// Minimum delay between empty source checks, including missed hints.
    pub poll_ms: u64,
}

/// Terminal or live state of a named wait; no outcome rolls back a commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AckWaitOutcome {
    /// All members of the fixed certified set supplied matching evidence.
    Satisfied,
    /// Still waiting for these exact pinned registrations.
    Pending(Vec<RequiredSubscriber>),
    /// Deadline passed with these registrations unmet.
    TimedOut(Vec<RequiredSubscriber>),
    /// Caller cancelled only the wait.
    Cancelled,
    /// The source can no longer certify the required authority/history.
    AuthorityLost,
}

/// Invalid request or contradictory evidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AckWaitError {
    /// Name, cursor, source history, or source certificate was malformed.
    Identity,
    /// Roster or metadata exceeded a configured bound.
    Backpressure,
    /// Wait deadline is invalid or would exceed its configured cap.
    Deadline,
    /// Roster certificate does not bind the requested target and proof kind.
    Roster,
    /// Acknowledgement belongs to another wait, target, kind, or registration.
    Evidence,
    /// This exact subscriber already acknowledged the wait.
    Duplicate,
    /// The wait already reached a terminal outcome.
    Closed,
}
