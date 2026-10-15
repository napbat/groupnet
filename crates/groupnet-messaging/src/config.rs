//! Node-wide application queue, retry and duplicate-retention bounds.

use std::{fmt, io, num::NonZeroUsize, ops::Deref, time::Duration};

use groupnet_transport::QueueCapacity;

use crate::{
    DEFAULT_DEDUP_RETENTION_MS, DEFAULT_MAX_MESSAGE_BYTES, DEFAULT_MAX_TIMEOUT_MS, SendOptions,
};

/// Finite operational bounds shared by all handles of a messaging endpoint.
/// Duplicate records are never evicted before their safe retry horizon.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessagingConfig {
    /// Maximum application payload, also limited by the destination's router
    /// envelope; at most `u32::MAX` bytes.
    pub max_payload: NonZeroUsize,
    /// Maximum frames queued in the dispatcher and each managed node/group inbox.
    pub inbox_capacity: QueueCapacity,
    /// Maximum concurrently acknowledged sends.
    pub pending_sends: QueueCapacity,
    /// Maximum retained acknowledged identities, including pending receipts.
    pub received_records: QueueCapacity,
    /// Delay between retries of an acknowledged send; nonzero.
    pub retry_interval: Duration,
    /// Maximum simultaneous recipient sends during managed group fanout.
    pub fanout_concurrency: QueueCapacity,
    /// Maximum local send deadline and admitted peer retry horizon; at least
    /// `retry_interval`. Frames exceeding the receiver's horizon fail closed
    /// without execution or ACK.
    pub max_timeout: Duration,
    /// Inactive receipt retention; exceeds `max_timeout` after rounding up to milliseconds.
    pub dedup_retention: Duration,
}

impl Default for MessagingConfig {
    fn default() -> Self {
        Self {
            max_payload: const { NonZeroUsize::new(DEFAULT_MAX_MESSAGE_BYTES).unwrap() },
            inbox_capacity: QueueCapacity::of(64),
            pending_sends: QueueCapacity::of(256),
            received_records: QueueCapacity::of(1024),
            retry_interval: Duration::from_millis(200),
            fanout_concurrency: QueueCapacity::of(32),
            max_timeout: Duration::from_millis(DEFAULT_MAX_TIMEOUT_MS),
            dedup_retention: Duration::from_millis(DEFAULT_DEDUP_RETENTION_MS),
        }
    }
}

impl MessagingConfig {
    /// Checks payload and wire representability and the retry-retention invariant.
    /// Counts and queue capacities carry their own nonzero range.
    ///
    /// # Errors
    /// Returns the first violated [`MessagingConfigError`] rule.
    pub fn validate(&self) -> Result<(), MessagingConfigError> {
        if u32::try_from(self.max_payload.get()).is_err() {
            return Err(MessagingConfigError::MaxPayloadAboveWire);
        }
        if self.retry_interval.is_zero() {
            return Err(MessagingConfigError::ZeroRetryInterval);
        }
        if self.max_timeout < self.retry_interval {
            return Err(MessagingConfigError::MaxTimeoutBelowRetryInterval);
        }
        let now = std::time::Instant::now();
        for (field, timer) in [
            ("retry_interval", self.retry_interval),
            ("max_timeout", self.max_timeout),
            ("dedup_retention", self.dedup_retention),
        ] {
            if timeout_ms(timer).is_none() || now.checked_add(timer).is_none() {
                return Err(MessagingConfigError::UnrepresentableTimer { field });
            }
        }
        if self.dedup_retention.as_nanos().div_ceil(1_000_000)
            <= self.max_timeout.as_nanos().div_ceil(1_000_000)
        {
            return Err(MessagingConfigError::RetentionNotAboveMaxTimeout);
        }
        Ok(())
    }

    /// Validates an operation against these bounds. Total on any configuration,
    /// validated or not.
    ///
    /// # Errors
    /// Returns the [`SendValidationError`] naming an oversized payload, an
    /// acknowledgement deadline outside `(0, max_timeout]`, or a deadline the
    /// wire's `u64` milliseconds cannot carry.
    pub fn validate_send(
        &self,
        options: SendOptions,
        payload_len: usize,
    ) -> Result<(), SendValidationError> {
        self.retry_horizon_ms(options, payload_len).map(drop)
    }

    /// Validates a send once against these bounds and returns its wire retry
    /// horizon: zero for best effort, else the deadline rounded up to
    /// milliseconds.
    pub(crate) fn retry_horizon_ms(
        &self,
        options: SendOptions,
        payload_len: usize,
    ) -> Result<u64, SendValidationError> {
        if payload_len > self.max_payload.get() {
            return Err(SendValidationError::PayloadTooLarge {
                len: payload_len,
                max: self.max_payload,
            });
        }
        let Some(timeout) = options.timeout() else {
            return Ok(0);
        };
        if timeout.is_zero() {
            return Err(SendValidationError::ZeroTimeout);
        }
        if timeout > self.max_timeout {
            return Err(SendValidationError::TimeoutAboveMax {
                timeout,
                max: self.max_timeout,
            });
        }
        timeout_ms(timeout).ok_or(SendValidationError::UnrepresentableTimeout { timeout })
    }

    pub(crate) fn retention_ms(&self) -> u64 {
        u64::try_from(self.dedup_retention.as_nanos().div_ceil(1_000_000))
            .expect("validated duplicate retention fits milliseconds")
    }

    pub(crate) fn max_timeout_ms(&self) -> u64 {
        timeout_ms(self.max_timeout).expect("validated timeout fits milliseconds")
    }
}

/// Whole milliseconds covering `timeout`, or `None` beyond `u64` milliseconds.
pub(crate) fn timeout_ms(timeout: Duration) -> Option<u64> {
    u64::try_from(timeout.as_nanos().div_ceil(1_000_000)).ok()
}

/// A [`MessagingConfig`] that passed [`MessagingConfig::validate`].
///
/// Constructible only through `TryFrom<MessagingConfig>`, it lets a caller that
/// must reject invalid bounds before other side effects (a node builder)
/// validate once and hand the proof to
/// [`Messaging::with_validated`](crate::Messaging::with_validated), which does
/// not check again. Dereferences to the validated configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidMessagingConfig(MessagingConfig);

impl TryFrom<MessagingConfig> for ValidMessagingConfig {
    type Error = MessagingConfigError;

    /// # Errors
    /// Returns the first violated [`MessagingConfigError`] rule.
    fn try_from(config: MessagingConfig) -> Result<Self, Self::Error> {
        config.validate()?;
        Ok(Self(config))
    }
}

impl Deref for ValidMessagingConfig {
    type Target = MessagingConfig;

    fn deref(&self) -> &MessagingConfig {
        &self.0
    }
}

impl ValidMessagingConfig {
    /// The validated configuration.
    pub(crate) fn into_inner(self) -> MessagingConfig {
        self.0
    }
}

/// A send that violates its endpoint's [`MessagingConfig`] bounds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SendValidationError {
    /// The payload exceeds `max_payload`.
    PayloadTooLarge {
        /// The rejected payload length.
        len: usize,
        /// The configured `max_payload`.
        max: NonZeroUsize,
    },
    /// An acknowledged send's deadline is zero.
    ZeroTimeout,
    /// An acknowledged send's deadline exceeds `max_timeout`.
    TimeoutAboveMax {
        /// The rejected deadline.
        timeout: Duration,
        /// The configured `max_timeout`.
        max: Duration,
    },
    /// An acknowledged send's deadline exceeds the wire's `u64` milliseconds
    /// or the monotonic clock. A deadline within `max_timeout` of a
    /// configuration that passed [`MessagingConfig::validate`] fits both.
    UnrepresentableTimeout {
        /// The rejected deadline.
        timeout: Duration,
    },
}

impl fmt::Display for SendValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PayloadTooLarge { len, max } => write!(
                f,
                "application payload of {len} bytes exceeds max_payload {max}"
            ),
            Self::ZeroTimeout => f.write_str("acknowledged send timeout must be nonzero"),
            Self::TimeoutAboveMax { timeout, max } => write!(
                f,
                "acknowledged send timeout {timeout:?} exceeds max_timeout {max:?}"
            ),
            Self::UnrepresentableTimeout { timeout } => write!(
                f,
                "acknowledged send timeout {timeout:?} exceeds the wire or monotonic clock"
            ),
        }
    }
}

impl std::error::Error for SendValidationError {}

impl From<SendValidationError> for io::Error {
    fn from(error: SendValidationError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}

/// A violated [`MessagingConfig`] rule, naming the offending field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessagingConfigError {
    /// `max_payload` exceeds the wire's `u32` payload length.
    MaxPayloadAboveWire,
    /// `retry_interval` is zero.
    ZeroRetryInterval,
    /// `max_timeout` is shorter than `retry_interval`.
    MaxTimeoutBelowRetryInterval,
    /// `dedup_retention` does not exceed `max_timeout` once both round up to
    /// whole milliseconds, so a retry could outlive its duplicate record.
    RetentionNotAboveMaxTimeout,
    /// A timer cannot extend the monotonic clock or exceeds `u64` milliseconds.
    UnrepresentableTimer {
        /// The offending field.
        field: &'static str,
    },
}

impl fmt::Display for MessagingConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MaxPayloadAboveWire => {
                f.write_str("messaging max_payload exceeds the u32 wire length")
            }
            Self::ZeroRetryInterval => f.write_str("messaging retry_interval must be nonzero"),
            Self::MaxTimeoutBelowRetryInterval => {
                f.write_str("messaging max_timeout must be at least retry_interval")
            }
            Self::RetentionNotAboveMaxTimeout => f.write_str(
                "messaging dedup_retention must exceed max_timeout in whole milliseconds",
            ),
            Self::UnrepresentableTimer { field } => {
                write!(f, "messaging {field} exceeds the monotonic clock")
            }
        }
    }
}

impl std::error::Error for MessagingConfigError {}

impl From<MessagingConfigError> for io::Error {
    fn from(error: MessagingConfigError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operational_bounds_are_configurable_but_retention_covers_retries() {
        let mut config = MessagingConfig {
            max_payload: NonZeroUsize::new(DEFAULT_MAX_MESSAGE_BYTES + 1).unwrap(),
            inbox_capacity: QueueCapacity::of(2048),
            pending_sends: QueueCapacity::of(2048),
            received_records: QueueCapacity::of(8192),
            max_timeout: Duration::from_secs(90),
            dedup_retention: Duration::from_secs(91),
            ..MessagingConfig::default()
        };
        config.validate().unwrap();
        config
            .validate_send(
                SendOptions::Applied {
                    timeout: Duration::from_secs(90),
                },
                DEFAULT_MAX_MESSAGE_BYTES + 1,
            )
            .unwrap();
        config.dedup_retention = config.max_timeout;
        assert_eq!(
            config.validate(),
            Err(MessagingConfigError::RetentionNotAboveMaxTimeout)
        );
    }

    #[test]
    fn each_invalid_field_is_named_by_its_own_error() {
        let cases = [
            (
                MessagingConfig {
                    max_payload: NonZeroUsize::new(usize::try_from(u32::MAX).unwrap() + 1).unwrap(),
                    ..MessagingConfig::default()
                },
                MessagingConfigError::MaxPayloadAboveWire,
            ),
            (
                MessagingConfig {
                    retry_interval: Duration::ZERO,
                    ..MessagingConfig::default()
                },
                MessagingConfigError::ZeroRetryInterval,
            ),
            (
                MessagingConfig {
                    retry_interval: Duration::from_secs(2),
                    max_timeout: Duration::from_secs(1),
                    ..MessagingConfig::default()
                },
                MessagingConfigError::MaxTimeoutBelowRetryInterval,
            ),
            (
                MessagingConfig {
                    dedup_retention: Duration::MAX,
                    ..MessagingConfig::default()
                },
                MessagingConfigError::UnrepresentableTimer {
                    field: "dedup_retention",
                },
            ),
        ];
        for (config, expected) in cases {
            assert_eq!(config.validate(), Err(expected));
            let error = io::Error::from(expected);
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
            assert_eq!(
                error
                    .get_ref()
                    .and_then(|inner| inner.downcast_ref::<MessagingConfigError>()),
                Some(&expected)
            );
        }
    }

    #[test]
    fn timer_rounding_keeps_retention_strictly_past_the_wire_retry_horizon() {
        assert_eq!(timeout_ms(Duration::from_nanos(1)), Some(1));
        assert_eq!(timeout_ms(Duration::from_millis(50)), Some(50));
        assert_eq!(
            timeout_ms(Duration::from_millis(50) + Duration::from_nanos(1)),
            Some(51)
        );
        assert_eq!(timeout_ms(Duration::MAX), None);
        let mut config = MessagingConfig {
            retry_interval: Duration::from_nanos(1),
            max_timeout: Duration::from_nanos(1),
            dedup_retention: Duration::from_nanos(2),
            ..MessagingConfig::default()
        };
        assert_eq!(
            config.validate(),
            Err(MessagingConfigError::RetentionNotAboveMaxTimeout)
        );
        config.dedup_retention = Duration::from_millis(2);
        config.validate().unwrap();
    }

    #[test]
    fn one_send_check_yields_the_wire_horizon_or_a_typed_rejection() {
        let config = MessagingConfig::default();
        assert_eq!(config.retry_horizon_ms(SendOptions::BestEffort, 0), Ok(0));
        assert_eq!(
            config.retry_horizon_ms(
                SendOptions::Applied {
                    timeout: Duration::from_micros(1_500)
                },
                config.max_payload.get()
            ),
            Ok(2)
        );
        let above = config.max_timeout + Duration::from_nanos(1);
        for (options, length, expected) in [
            (
                SendOptions::Delivered {
                    timeout: Duration::ZERO,
                },
                0,
                SendValidationError::ZeroTimeout,
            ),
            (
                SendOptions::Applied { timeout: above },
                0,
                SendValidationError::TimeoutAboveMax {
                    timeout: above,
                    max: config.max_timeout,
                },
            ),
            (
                SendOptions::BestEffort,
                config.max_payload.get() + 1,
                SendValidationError::PayloadTooLarge {
                    len: config.max_payload.get() + 1,
                    max: config.max_payload,
                },
            ),
        ] {
            assert_eq!(config.validate_send(options, length), Err(expected));
            let error = io::Error::from(expected);
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
            assert_eq!(
                error
                    .get_ref()
                    .and_then(|inner| inner.downcast_ref::<SendValidationError>()),
                Some(&expected)
            );
        }
    }

    #[test]
    fn standalone_send_check_is_total_on_unrepresentable_timeouts() {
        let unvalidated = MessagingConfig {
            max_timeout: Duration::MAX,
            ..MessagingConfig::default()
        };
        assert!(unvalidated.validate().is_err());
        let beyond_wire = Duration::from_millis(u64::MAX) + Duration::from_nanos(1);
        for timeout in [Duration::MAX, beyond_wire] {
            for options in [
                SendOptions::Delivered { timeout },
                SendOptions::Applied { timeout },
            ] {
                let expected = SendValidationError::UnrepresentableTimeout { timeout };
                assert_eq!(unvalidated.validate_send(options, 0), Err(expected));
                assert_eq!(unvalidated.retry_horizon_ms(options, 0), Err(expected));
                let error = io::Error::from(expected);
                assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
                assert_eq!(
                    error
                        .get_ref()
                        .and_then(|inner| inner.downcast_ref::<SendValidationError>()),
                    Some(&expected)
                );
            }
        }
        let wire_max = Duration::from_millis(u64::MAX);
        assert_eq!(
            unvalidated.retry_horizon_ms(SendOptions::Applied { timeout: wire_max }, 0),
            Ok(u64::MAX)
        );
        assert_eq!(
            unvalidated.validate_send(SendOptions::BestEffort, 0),
            Ok(())
        );
    }

    #[test]
    fn only_a_validated_configuration_becomes_a_construction_proof() {
        let valid = ValidMessagingConfig::try_from(MessagingConfig::default()).unwrap();
        assert_eq!(*valid, MessagingConfig::default());
        let invalid = MessagingConfig {
            retry_interval: Duration::ZERO,
            ..MessagingConfig::default()
        };
        assert_eq!(
            ValidMessagingConfig::try_from(invalid),
            Err(MessagingConfigError::ZeroRetryInterval)
        );
    }
}
