//! Exact, bounded evidence for named acknowledgement waits.
//!
//! A source adapter certifies the fixed roster and each acknowledgement. The
//! core checks their binding; it cannot authenticate arbitrary application
//! bytes or infer a source order from encoded native positions.

use std::fmt;
use std::num::NonZeroUsize;

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
///
/// Counts and byte caps are nonzero by type; [`AckWaitLimits::validate`]
/// checks the timers and the caps' relation to `max_metadata_bytes`.
/// [`Default`] admits 64 subscribers with identities and cursors up to the
/// replication [`Config`](super::Config) default cursor size (4 KiB), 4 KiB
/// certificates and 64 KiB of metadata, waiting up to 30 s and polling every
/// 100 ms.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AckWaitLimits {
    /// Maximum number of pinned named subscribers.
    pub max_required: NonZeroUsize,
    /// Maximum bytes in any identity, epoch, intent, or cursor field; at most
    /// `max_metadata_bytes`.
    pub max_identity_bytes: NonZeroUsize,
    /// Maximum source certificate bytes; at most `max_metadata_bytes`.
    pub max_certificate_bytes: NonZeroUsize,
    /// Maximum combined variable-length wait metadata bytes.
    pub max_metadata_bytes: NonZeroUsize,
    /// Maximum wait duration in logical milliseconds; nonzero.
    pub max_wait_ms: u64,
    /// Minimum delay between empty source checks, including missed hints;
    /// nonzero and at most `max_wait_ms`.
    pub poll_ms: u64,
}

impl Default for AckWaitLimits {
    fn default() -> Self {
        Self {
            max_required: const { NonZeroUsize::new(64).unwrap() },
            max_identity_bytes: const { NonZeroUsize::new(4096).unwrap() },
            max_certificate_bytes: const { NonZeroUsize::new(4096).unwrap() },
            max_metadata_bytes: const { NonZeroUsize::new(64 * 1024).unwrap() },
            max_wait_ms: 30_000,
            poll_ms: 100,
        }
    }
}

/// Invalid [`AckWaitLimits`], naming the violated rule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AckWaitLimitsError {
    /// [`AckWaitLimits::max_wait_ms`] is zero.
    ZeroMaxWait,
    /// [`AckWaitLimits::poll_ms`] is zero.
    ZeroPoll,
    /// [`AckWaitLimits::poll_ms`] exceeds [`AckWaitLimits::max_wait_ms`].
    PollExceedsMaxWait,
    /// [`AckWaitLimits::max_identity_bytes`] exceeds
    /// [`AckWaitLimits::max_metadata_bytes`].
    IdentityExceedsMetadata,
    /// [`AckWaitLimits::max_certificate_bytes`] exceeds
    /// [`AckWaitLimits::max_metadata_bytes`].
    CertificateExceedsMetadata,
}

impl fmt::Display for AckWaitLimitsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ZeroMaxWait => "ack wait max_wait_ms must be nonzero",
            Self::ZeroPoll => "ack wait poll_ms must be nonzero",
            Self::PollExceedsMaxWait => "ack wait poll_ms must not exceed max_wait_ms",
            Self::IdentityExceedsMetadata => {
                "ack wait max_identity_bytes must not exceed max_metadata_bytes"
            }
            Self::CertificateExceedsMetadata => {
                "ack wait max_certificate_bytes must not exceed max_metadata_bytes"
            }
        })
    }
}

impl std::error::Error for AckWaitLimitsError {}

impl AckWaitLimits {
    /// Checks the wait timers and the per-field caps against the combined
    /// metadata cap.
    ///
    /// # Errors
    /// Returns the [`AckWaitLimitsError`] variant naming the first violated rule.
    pub fn validate(&self) -> Result<(), AckWaitLimitsError> {
        if self.max_wait_ms == 0 {
            return Err(AckWaitLimitsError::ZeroMaxWait);
        }
        if self.poll_ms == 0 {
            return Err(AckWaitLimitsError::ZeroPoll);
        }
        if self.poll_ms > self.max_wait_ms {
            return Err(AckWaitLimitsError::PollExceedsMaxWait);
        }
        if self.max_identity_bytes > self.max_metadata_bytes {
            return Err(AckWaitLimitsError::IdentityExceedsMetadata);
        }
        if self.max_certificate_bytes > self.max_metadata_bytes {
            return Err(AckWaitLimitsError::CertificateExceedsMetadata);
        }
        Ok(())
    }
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
    /// The wait's [`AckWaitLimits`] are invalid.
    Limits(AckWaitLimitsError),
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

impl fmt::Display for AckWaitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Limits(_) => "invalid ack wait limits",
            Self::Identity => "malformed ack wait identity or certificate",
            Self::Backpressure => "ack wait roster or metadata exceeds its bound",
            Self::Deadline => "ack wait deadline is invalid or exceeds its cap",
            Self::Roster => "roster certificate does not bind the target and kind",
            Self::Evidence => "acknowledgement does not match this wait",
            Self::Duplicate => "subscriber already acknowledged this wait",
            Self::Closed => "ack wait already reached a terminal outcome",
        })
    }
}

impl std::error::Error for AckWaitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Limits(error) => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> AckWaitLimits {
        AckWaitLimits {
            max_required: NonZeroUsize::new(2).unwrap(),
            max_identity_bytes: NonZeroUsize::new(64).unwrap(),
            max_certificate_bytes: NonZeroUsize::new(64).unwrap(),
            max_metadata_bytes: NonZeroUsize::new(128).unwrap(),
            max_wait_ms: 20,
            poll_ms: 3,
        }
    }

    #[test]
    fn default_limits_are_valid() {
        assert_eq!(AckWaitLimits::default().validate(), Ok(()));
    }

    #[test]
    fn limits_validate_names_each_rule() {
        assert_eq!(limits().validate(), Ok(()));
        let cases = [
            (
                AckWaitLimits {
                    max_wait_ms: 0,
                    poll_ms: 0,
                    ..limits()
                },
                AckWaitLimitsError::ZeroMaxWait,
            ),
            (
                AckWaitLimits {
                    poll_ms: 0,
                    ..limits()
                },
                AckWaitLimitsError::ZeroPoll,
            ),
            (
                AckWaitLimits {
                    poll_ms: 21,
                    ..limits()
                },
                AckWaitLimitsError::PollExceedsMaxWait,
            ),
            (
                AckWaitLimits {
                    max_identity_bytes: NonZeroUsize::new(129).unwrap(),
                    ..limits()
                },
                AckWaitLimitsError::IdentityExceedsMetadata,
            ),
            (
                AckWaitLimits {
                    max_certificate_bytes: NonZeroUsize::new(129).unwrap(),
                    ..limits()
                },
                AckWaitLimitsError::CertificateExceedsMetadata,
            ),
        ];
        for (limits, expected) in cases {
            assert_eq!(limits.validate(), Err(expected));
        }
        assert_eq!(
            AckWaitLimitsError::IdentityExceedsMetadata.to_string(),
            "ack wait max_identity_bytes must not exceed max_metadata_bytes"
        );
        let error = AckWaitError::Limits(AckWaitLimitsError::ZeroPoll);
        assert_eq!(error.to_string(), "invalid ack wait limits");
        assert_eq!(
            std::error::Error::source(&error).map(ToString::to_string),
            Some("ack wait poll_ms must be nonzero".to_owned())
        );
        assert!(std::error::Error::source(&AckWaitError::Closed).is_none());
    }
}
