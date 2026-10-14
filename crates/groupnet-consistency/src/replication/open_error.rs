//! Typed construction and registration failures for replication managers.

use std::error::Error;
use std::fmt;

use groupnet_core::replication::{ConfigError as CoreConfigError, Reject};

use super::ack_api::AckWaitLimitsError;
use super::api::FailureClass;
use super::limits::LimitsError;

/// Failure to build a manager, attach an optional capability, or register a
/// local scope or named subscriber. No variant starts source work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpenError {
    /// [`Limits`](super::Limits) violate a rule; the [`LimitsError`] is the
    /// [`source`](Error::source).
    InvalidLimits(LimitsError),
    /// [`Replication::new_native`](super::Replication::new_native) needs
    /// finite `Limits.core.snapshot` bounds.
    SnapshotLimitsRequired,
    /// [`Replication::new`](super::Replication::new) is replay-only and
    /// rejects `Limits.core.snapshot`; use `new_native` for snapshots.
    SnapshotLimitsUnsupported,
    /// Named acknowledgement wait limits violate a rule; the
    /// [`AckWaitLimitsError`] is the [`source`](Error::source).
    InvalidAckLimits(AckWaitLimitsError),
    /// An acknowledgement evidence source is already attached.
    AckEvidenceAlreadyAttached,
    /// Named subscription limits contain a zero bound.
    InvalidSubscriptionLimits,
    /// An optional capability was attached after a session opened; attach
    /// capabilities before the first `open`.
    SessionsStarted,
    /// The scope identity is empty or exceeds `Limits.core.max_cursor_bytes`;
    /// carries the core's rejection unchanged.
    InvalidScope(CoreConfigError),
    /// The source adapter could not encode the requested start position.
    InvalidStartPosition(FailureClass),
    /// The core rejected the named subscriber's start request; carries the
    /// core's rejection unchanged.
    StartRejected(Reject),
    /// Scope belongs to another Groupnet group.
    WrongGroup,
    /// A session for the scope already exists.
    AlreadyOpen,
    /// A session incarnation is already active in this manager.
    DuplicateSession,
    /// The configured local scope registry is full.
    Backpressured,
    /// No Tokio executor is running.
    NoRuntime,
}

impl fmt::Display for OpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLimits(_) => f.write_str("invalid replication limits"),
            Self::SnapshotLimitsRequired => {
                f.write_str("native snapshot replication needs Limits.core.snapshot")
            }
            Self::SnapshotLimitsUnsupported => {
                f.write_str("replay-only replication rejects Limits.core.snapshot; use new_native")
            }
            Self::InvalidAckLimits(_) => f.write_str("invalid ack wait limits"),
            Self::AckEvidenceAlreadyAttached => {
                f.write_str("an ack evidence source is already attached")
            }
            Self::InvalidSubscriptionLimits => {
                f.write_str("subscription limits contain a zero bound")
            }
            Self::SessionsStarted => {
                f.write_str("capabilities must be attached before the first session opens")
            }
            Self::InvalidScope(error) => write!(f, "invalid replication scope: {error:?}"),
            Self::InvalidStartPosition(class) => {
                write!(f, "source cannot encode the start position ({class:?})")
            }
            Self::StartRejected(reason) => write!(f, "subscriber start rejected: {reason:?}"),
            Self::WrongGroup => f.write_str("scope belongs to another group"),
            Self::AlreadyOpen => f.write_str("a session for the scope is already open"),
            Self::DuplicateSession => f.write_str("session incarnation is already active"),
            Self::Backpressured => f.write_str("local scope registry is full"),
            Self::NoRuntime => f.write_str("no Tokio runtime is running"),
        }
    }
}

impl Error for OpenError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidLimits(error) => Some(error),
            Self::InvalidAckLimits(error) => Some(error),
            _ => None,
        }
    }
}

impl From<LimitsError> for OpenError {
    fn from(error: LimitsError) -> Self {
        Self::InvalidLimits(error)
    }
}

impl From<AckWaitLimitsError> for OpenError {
    fn from(error: AckWaitLimitsError) -> Self {
        Self::InvalidAckLimits(error)
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::{AckWaitLimitsError, LimitsError, OpenError};

    #[test]
    fn limit_failures_chain_as_source() {
        let limits = LimitsError::QueueDepthAbovePermits { queue_depth: 9 };
        let error = OpenError::from(limits);
        assert_eq!(error, OpenError::InvalidLimits(limits));
        let source = error.source().expect("limits error is the source");
        assert_eq!(source.downcast_ref::<LimitsError>(), Some(&limits));

        let error = OpenError::from(AckWaitLimitsError::ZeroPoll);
        let source = error.source().expect("ack limits error is the source");
        assert_eq!(
            source.downcast_ref::<AckWaitLimitsError>(),
            Some(&AckWaitLimitsError::ZeroPoll)
        );
        assert!(OpenError::NoRuntime.source().is_none());
    }
}
