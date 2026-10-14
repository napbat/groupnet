//! The one failure type shared by every volatile-recovery adapter trait.

use std::error::Error;
use std::fmt;

/// Diagnostic class of a failed adapter or bootstrap-port operation.
///
/// The recovery and bootstrap cores treat every failure alike: they choose
/// their own retry or fallback, which the shell reports separately (for
/// example as a [`RecoveryFallback`](crate::volatile_recovery::RecoveryFallback)).
/// The class only tells an operator what kind of thing went wrong.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AdapterErrorKind {
    /// The origin, native source, transport, or application could not
    /// complete the operation.
    Unavailable,
    /// A record or reply read from a native source or peer was malformed,
    /// contradictory, or bound to another identity.
    Malformed,
    /// A request or argument lies outside the configured bounds, policy, or
    /// local identity of this adapter.
    InvalidInput,
    /// A memory admission charge or an arithmetic bound refused the work.
    Capacity,
    /// The operation's permit, capture, journal, or claim is no longer current.
    Fenced,
    /// The operation's deadline passed before it completed.
    Expired,
    /// The adapter does not implement this optional capability.
    Unsupported,
}

impl fmt::Display for AdapterErrorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Unavailable => "source or application unavailable",
            Self::Malformed => "malformed or contradictory source data",
            Self::InvalidInput => "request outside configured bounds",
            Self::Capacity => "admission or arithmetic bound refused",
            Self::Fenced => "operation no longer current",
            Self::Expired => "operation deadline passed",
            Self::Unsupported => "capability not supported",
        })
    }
}

/// A failed source, application, or bootstrap-port operation.
///
/// It carries a diagnostic [`AdapterErrorKind`] and, optionally, the
/// underlying cause, reachable through [`Error::source`]. A recovery adapter's
/// failure reaches
/// [`RecoveryAdapter::operation_failed`](crate::volatile_recovery::RecoveryAdapter::operation_failed)
/// intact once the core has consumed it.
#[derive(Debug)]
pub struct AdapterError {
    kind: AdapterErrorKind,
    source: Option<Box<dyn Error + Send + Sync + 'static>>,
}

impl AdapterError {
    /// A failure with no further underlying cause.
    #[must_use]
    pub const fn new(kind: AdapterErrorKind) -> Self {
        Self { kind, source: None }
    }

    /// A failure caused by `source`, preserved for [`Error::source`].
    #[must_use]
    pub fn with_source(
        kind: AdapterErrorKind,
        source: impl Into<Box<dyn Error + Send + Sync + 'static>>,
    ) -> Self {
        Self {
            kind,
            source: Some(source.into()),
        }
    }

    /// Diagnostic class of this failure.
    #[must_use]
    pub const fn kind(&self) -> AdapterErrorKind {
        self.kind
    }
}

impl From<AdapterErrorKind> for AdapterError {
    fn from(kind: AdapterErrorKind) -> Self {
        Self::new(kind)
    }
}

impl fmt::Display for AdapterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "volatile recovery adapter failed: {}", self.kind)
    }
}

impl Error for AdapterError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::{AdapterError, AdapterErrorKind};

    #[derive(Debug)]
    struct Cause;

    impl std::fmt::Display for Cause {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("origin LIST refused")
        }
    }

    impl Error for Cause {}

    #[test]
    fn kind_and_source_survive() {
        let error = AdapterError::with_source(AdapterErrorKind::Unavailable, Cause);
        assert_eq!(error.kind(), AdapterErrorKind::Unavailable);
        assert_eq!(
            error.source().map(ToString::to_string).as_deref(),
            Some("origin LIST refused")
        );
        assert!(
            error
                .to_string()
                .contains("source or application unavailable")
        );
    }

    #[test]
    fn kind_converts_without_source() {
        let error = AdapterError::from(AdapterErrorKind::Unsupported);
        assert_eq!(error.kind(), AdapterErrorKind::Unsupported);
        assert!(error.source().is_none());
    }
}
