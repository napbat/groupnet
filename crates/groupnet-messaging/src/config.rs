//! Node-wide application queue, retry and duplicate-retention bounds.

use std::{fmt, io, num::NonZeroUsize, time::Duration};

use groupnet_transport::QueueCapacity;

use crate::{
    DEFAULT_DEDUP_RETENTION_MS, DEFAULT_MAX_MESSAGE_BYTES, DEFAULT_MAX_TIMEOUT_MS, Delivery,
    SendOptions,
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
            if timeout_ms(timer).is_err() || now.checked_add(timer).is_none() {
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

    /// Validates an operation against this endpoint's configured bounds.
    ///
    /// # Errors
    /// Returns `InvalidInput` for oversized payloads or acknowledgement deadlines.
    pub fn validate_send(&self, options: SendOptions, payload_len: usize) -> io::Result<()> {
        self.retry_horizon_ms(options, payload_len).map(drop)
    }

    /// Validates a send once against these (validated) bounds and returns its
    /// wire retry horizon: zero for best effort, else the deadline rounded up
    /// to milliseconds. Bounded by `max_payload`/`max_timeout`, a passing send
    /// is wire-representable without re-checking.
    pub(crate) fn retry_horizon_ms(
        &self,
        options: SendOptions,
        payload_len: usize,
    ) -> io::Result<u64> {
        let invalid = || {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "application send exceeds messaging bounds",
            )
        };
        if payload_len > self.max_payload.get() {
            return Err(invalid());
        }
        if options.delivery == Delivery::BestEffort {
            return Ok(0);
        }
        if options.timeout.is_zero() || options.timeout > self.max_timeout {
            return Err(invalid());
        }
        timeout_ms(options.timeout)
    }

    pub(crate) fn retention_ms(&self) -> u64 {
        u64::try_from(self.dedup_retention.as_nanos().div_ceil(1_000_000))
            .expect("validated duplicate retention fits milliseconds")
    }

    pub(crate) fn max_timeout_ms(&self) -> u64 {
        timeout_ms(self.max_timeout).expect("validated timeout fits milliseconds")
    }
}

pub(crate) fn timeout_ms(timeout: Duration) -> io::Result<u64> {
    u64::try_from(timeout.as_nanos().div_ceil(1_000_000)).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "application retry horizon overflow",
        )
    })
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
                SendOptions {
                    delivery: Delivery::Applied,
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
        assert_eq!(timeout_ms(Duration::from_nanos(1)).unwrap(), 1);
        assert_eq!(timeout_ms(Duration::from_millis(50)).unwrap(), 50);
        assert_eq!(
            timeout_ms(Duration::from_millis(50) + Duration::from_nanos(1)).unwrap(),
            51
        );
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
    fn one_send_check_yields_the_wire_horizon_or_rejects() {
        let config = MessagingConfig::default();
        let send = |delivery, timeout| SendOptions { delivery, timeout };
        assert_eq!(
            config
                .retry_horizon_ms(send(Delivery::BestEffort, Duration::ZERO), 0)
                .unwrap(),
            0
        );
        assert_eq!(
            config
                .retry_horizon_ms(
                    send(Delivery::Applied, Duration::from_micros(1_500)),
                    config.max_payload.get()
                )
                .unwrap(),
            2
        );
        for (options, length) in [
            (send(Delivery::Delivered, Duration::ZERO), 0),
            (
                send(
                    Delivery::Applied,
                    config.max_timeout + Duration::from_nanos(1),
                ),
                0,
            ),
            (
                send(Delivery::BestEffort, Duration::ZERO),
                config.max_payload.get() + 1,
            ),
        ] {
            assert_eq!(
                config.retry_horizon_ms(options, length).unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
            assert!(config.validate_send(options, length).is_err());
        }
    }
}
