//! Typed validation failures of [`FrameLimit`](super::FrameLimit),
//! [`RpcConfig`](super::RpcConfig) and
//! [`RpcServerConfig`](super::RpcServerConfig).

use std::error::Error;
use std::fmt;
use std::io;
use std::time::Duration;

use super::{FrameLimit, MAX_TIMEOUT};

/// Why a byte count is not a [`FrameLimit`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameLimitError {
    /// Below [`FrameLimit::MIN`]: not even a request head with an empty
    /// payload would fit. Carries the rejected byte count.
    BelowMin(usize),
    /// Above [`FrameLimit::MAX`], the data plane's frame ceiling. Carries the
    /// rejected byte count.
    AboveMax(usize),
}

impl fmt::Display for FrameLimitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (bytes, bound, side) = match *self {
            Self::BelowMin(bytes) => (bytes, FrameLimit::MIN.get(), "at least"),
            Self::AboveMax(bytes) => (bytes, FrameLimit::MAX.get(), "at most"),
        };
        write!(
            f,
            "rpc frame limit must be {side} {bound} bytes, got {bytes}"
        )
    }
}

impl Error for FrameLimitError {}

impl From<FrameLimitError> for io::Error {
    fn from(error: FrameLimitError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}

/// Why an [`RpcConfig`](super::RpcConfig) was rejected; each variant names
/// the offending field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RpcConfigError {
    /// [`connect_timeout`](super::RpcConfig::connect_timeout) is zero, so no
    /// connection could ever open.
    ZeroConnectTimeout,
    /// [`connect_timeout`](super::RpcConfig::connect_timeout) exceeds
    /// [`MAX_TIMEOUT`]. Carries the rejected value.
    ConnectTimeoutAboveMax(Duration),
    /// [`idle_timeout`](super::RpcConfig::idle_timeout) is zero, so every
    /// connection would close as soon as it opened.
    ZeroIdleTimeout,
    /// [`idle_timeout`](super::RpcConfig::idle_timeout) exceeds
    /// [`MAX_TIMEOUT`]. Carries the rejected value.
    IdleTimeoutAboveMax(Duration),
}

impl fmt::Display for RpcConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::ZeroConnectTimeout => zero(f, "connect_timeout"),
            Self::ConnectTimeoutAboveMax(value) => above_max(f, "connect_timeout", value),
            Self::ZeroIdleTimeout => zero(f, "idle_timeout"),
            Self::IdleTimeoutAboveMax(value) => above_max(f, "idle_timeout", value),
        }
    }
}

impl Error for RpcConfigError {}

impl From<RpcConfigError> for io::Error {
    fn from(error: RpcConfigError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}

/// Why an [`RpcServerConfig`](super::RpcServerConfig) was rejected; each
/// variant names the offending field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RpcServerConfigError {
    /// [`idle_timeout`](super::RpcServerConfig::idle_timeout) is zero, so
    /// every connection would close as soon as it was admitted.
    ZeroIdleTimeout,
    /// [`idle_timeout`](super::RpcServerConfig::idle_timeout) exceeds
    /// [`MAX_TIMEOUT`]. Carries the rejected value.
    IdleTimeoutAboveMax(Duration),
}

impl fmt::Display for RpcServerConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::ZeroIdleTimeout => zero(f, "idle_timeout"),
            Self::IdleTimeoutAboveMax(value) => above_max(f, "idle_timeout", value),
        }
    }
}

impl Error for RpcServerConfigError {}

impl From<RpcServerConfigError> for io::Error {
    fn from(error: RpcServerConfigError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}

fn zero(f: &mut fmt::Formatter<'_>, field: &str) -> fmt::Result {
    write!(f, "rpc {field} must be nonzero")
}

fn above_max(f: &mut fmt::Formatter<'_>, field: &str, value: Duration) -> fmt::Result {
    write!(
        f,
        "rpc {field} must be at most {MAX_TIMEOUT:?} (the wire deadline), got {value:?}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_message_names_its_field_or_bound() {
        let over = MAX_TIMEOUT + Duration::from_millis(1);
        for (error, needle) in [
            (
                RpcConfigError::ZeroConnectTimeout.to_string(),
                "connect_timeout",
            ),
            (
                RpcConfigError::ConnectTimeoutAboveMax(over).to_string(),
                "connect_timeout",
            ),
            (RpcConfigError::ZeroIdleTimeout.to_string(), "idle_timeout"),
            (
                RpcConfigError::IdleTimeoutAboveMax(over).to_string(),
                "idle_timeout",
            ),
            (
                RpcServerConfigError::ZeroIdleTimeout.to_string(),
                "idle_timeout",
            ),
            (
                RpcServerConfigError::IdleTimeoutAboveMax(over).to_string(),
                "idle_timeout",
            ),
            (FrameLimitError::BelowMin(1).to_string(), "got 1"),
            (FrameLimitError::AboveMax(usize::MAX).to_string(), "at most"),
        ] {
            assert!(error.contains(needle), "{error:?} lacks {needle:?}");
        }
    }

    #[test]
    fn io_conversions_are_invalid_input_and_keep_the_typed_cause() {
        let error = io::Error::from(RpcConfigError::ZeroConnectTimeout);
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            error
                .get_ref()
                .and_then(|e| e.downcast_ref::<RpcConfigError>()),
            Some(&RpcConfigError::ZeroConnectTimeout)
        );
        let error = io::Error::from(RpcServerConfigError::ZeroIdleTimeout);
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            error
                .get_ref()
                .and_then(|e| e.downcast_ref::<RpcServerConfigError>()),
            Some(&RpcServerConfigError::ZeroIdleTimeout)
        );
        let error = io::Error::from(FrameLimitError::BelowMin(0));
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            error
                .get_ref()
                .and_then(|e| e.downcast_ref::<FrameLimitError>()),
            Some(&FrameLimitError::BelowMin(0))
        );
    }
}
