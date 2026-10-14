//! Why a [`Config`](super::Config) was rejected.

use std::{fmt, io};

/// One rule a [`Config`](super::Config) violates, as reported by
/// [`Config::validate`](super::Config::validate). Each variant names the field
/// at fault.
///
/// Converts into an [`io::Error`] of kind [`io::ErrorKind::InvalidInput`]
/// wrapping this value (`io::Error::get_ref` and `io::Error::into_inner`
/// downcast back to it), for the `io::Result` APIs that validate a config
/// (the runtime's `NodeBuilder::start`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigError {
    /// [`gossip_interval_ms`](super::Config::gossip_interval_ms) is zero, so
    /// the gossip round would have no cadence.
    ZeroGossipInterval,
    /// [`probe_interval_ms`](super::Config::probe_interval_ms) is zero, so the
    /// failure detector would have no cadence.
    ZeroProbeInterval,
    /// [`probe_timeout_ms`](super::Config::probe_timeout_ms) is zero, so every
    /// probe would miss before an ack could arrive.
    ZeroProbeTimeout,
    /// [`suspect_timeout_ms`](super::Config::suspect_timeout_ms) is zero, so a
    /// suspected member could never refute before being declared dead.
    ZeroSuspectTimeout,
    /// [`dead_timeout_ms`](super::Config::dead_timeout_ms) is zero, so a
    /// tombstone would be reaped before it could be gossiped.
    ZeroDeadTimeout,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let field = match self {
            Self::ZeroGossipInterval => "gossip_interval_ms",
            Self::ZeroProbeInterval => "probe_interval_ms",
            Self::ZeroProbeTimeout => "probe_timeout_ms",
            Self::ZeroSuspectTimeout => "suspect_timeout_ms",
            Self::ZeroDeadTimeout => "dead_timeout_ms",
        };
        write!(f, "group config {field} must be nonzero")
    }
}

impl std::error::Error for ConfigError {}

impl From<ConfigError> for io::Error {
    fn from(error: ConfigError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::ConfigError;
    use crate::Config;

    /// One case: how to zero a timer, the variant it must produce, and the
    /// field name the message must carry.
    type ZeroCase = (fn(&mut Config), ConfigError, &'static str);

    /// Each zeroed timer is rejected with the variant naming it, and the
    /// message names the field.
    #[test]
    fn each_zero_timer_is_rejected_by_name() {
        let cases: [ZeroCase; 5] = [
            (
                |c| c.gossip_interval_ms = 0,
                ConfigError::ZeroGossipInterval,
                "gossip_interval_ms",
            ),
            (
                |c| c.probe_interval_ms = 0,
                ConfigError::ZeroProbeInterval,
                "probe_interval_ms",
            ),
            (
                |c| c.probe_timeout_ms = 0,
                ConfigError::ZeroProbeTimeout,
                "probe_timeout_ms",
            ),
            (
                |c| c.suspect_timeout_ms = 0,
                ConfigError::ZeroSuspectTimeout,
                "suspect_timeout_ms",
            ),
            (
                |c| c.dead_timeout_ms = 0,
                ConfigError::ZeroDeadTimeout,
                "dead_timeout_ms",
            ),
        ];
        for (zero, expected, field) in cases {
            let mut config = Config::default();
            zero(&mut config);
            assert_eq!(config.validate(), Err(expected));
            assert!(
                expected.to_string().contains(field),
                "{expected} must name {field}"
            );
        }
    }

    /// The default config is valid, and a nonzero timer of 1 ms is accepted
    /// as given.
    #[test]
    fn defaults_and_minimal_timers_are_valid() {
        assert_eq!(Config::default().validate(), Ok(()));
        let minimal = Config {
            gossip_interval_ms: 1,
            probe_interval_ms: 1,
            probe_timeout_ms: 1,
            suspect_timeout_ms: 1,
            dead_timeout_ms: 1,
            ..Config::default()
        };
        assert_eq!(minimal.validate(), Ok(()));
    }

    /// The `io::Error` conversion is `InvalidInput` and wraps the typed error
    /// itself, so a caller downcasts it back out.
    #[test]
    fn converts_to_invalid_input_io_error_wrapping_the_cause() {
        let error = io::Error::from(ConfigError::ZeroProbeTimeout);
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            error
                .get_ref()
                .and_then(|inner| inner.downcast_ref::<ConfigError>()),
            Some(&ConfigError::ZeroProbeTimeout)
        );
        let inner = error.into_inner().expect("custom io error");
        assert_eq!(
            inner.downcast::<ConfigError>().ok().as_deref(),
            Some(&ConfigError::ZeroProbeTimeout)
        );
    }
}
