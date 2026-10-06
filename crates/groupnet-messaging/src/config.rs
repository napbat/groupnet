//! Node-wide application queue, retry and duplicate-retention bounds.

use std::{io, time::Duration};

use crate::{
    DEFAULT_DEDUP_RETENTION_MS, DEFAULT_MAX_MESSAGE_BYTES, DEFAULT_MAX_TIMEOUT_MS, SendOptions,
};

/// Finite operational bounds shared by all handles of a messaging endpoint.
/// Duplicate records are never evicted before their safe retry horizon.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessagingConfig {
    /// Maximum application payload, also limited by the destination's router envelope.
    pub max_payload: usize,
    /// Maximum frames queued in the dispatcher and each managed node/group inbox.
    pub inbox_capacity: usize,
    /// Maximum concurrently acknowledged sends.
    pub pending_sends: usize,
    /// Maximum retained acknowledged identities, including pending receipts.
    pub received_records: usize,
    /// Delay between retries of an acknowledged send.
    pub retry_interval: Duration,
    /// Maximum simultaneous recipient sends during managed group fanout.
    pub fanout_concurrency: usize,
    /// Maximum local send deadline and admitted peer retry horizon.
    /// Frames exceeding the receiver's horizon fail closed without execution or ACK.
    pub max_timeout: Duration,
    /// Inactive receipt retention; exceeds `max_timeout` after rounding up to milliseconds.
    pub dedup_retention: Duration,
}

impl Default for MessagingConfig {
    fn default() -> Self {
        Self {
            max_payload: DEFAULT_MAX_MESSAGE_BYTES,
            inbox_capacity: 64,
            pending_sends: 256,
            received_records: 1024,
            retry_interval: Duration::from_millis(200),
            fanout_concurrency: 32,
            max_timeout: Duration::from_millis(DEFAULT_MAX_TIMEOUT_MS),
            dedup_retention: Duration::from_millis(DEFAULT_DEDUP_RETENTION_MS),
        }
    }
}

impl MessagingConfig {
    /// Checks capacities, wire representability and the retry-retention invariant.
    ///
    /// # Errors
    /// Returns `InvalidInput` for invalid bounds or unrepresentable timers.
    pub fn validate(&self) -> io::Result<()> {
        if self.max_payload == 0
            || u32::try_from(self.max_payload).is_err()
            || !(1..=tokio::sync::Semaphore::MAX_PERMITS).contains(&self.inbox_capacity)
            || !(1..=isize::MAX as usize).contains(&self.fanout_concurrency)
            || !(1..=isize::MAX as usize).contains(&self.pending_sends)
            || !(1..=isize::MAX as usize).contains(&self.received_records)
            || self.retry_interval.is_zero()
            || self.max_timeout < self.retry_interval
            || self.dedup_retention <= self.max_timeout
            || self.dedup_retention.as_nanos().div_ceil(1_000_000)
                <= self.max_timeout.as_nanos().div_ceil(1_000_000)
            || u64::try_from(self.dedup_retention.as_nanos().div_ceil(1_000_000)).is_err()
            || [self.retry_interval, self.max_timeout, self.dedup_retention]
                .iter()
                .any(|timer| std::time::Instant::now().checked_add(*timer).is_none())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid messaging bounds",
            ));
        }
        Ok(())
    }

    /// Validates an operation against this endpoint's configured bounds.
    ///
    /// # Errors
    /// Returns `InvalidInput` for oversized payloads or acknowledgement deadlines.
    pub fn validate_send(&self, options: SendOptions, payload_len: usize) -> io::Result<()> {
        options.validate(payload_len)?;
        if payload_len > self.max_payload
            || (options.delivery != crate::Delivery::BestEffort
                && options.timeout > self.max_timeout)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "application send exceeds messaging bounds",
            ));
        }
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operational_bounds_are_configurable_but_retention_covers_retries() {
        let mut config = MessagingConfig {
            max_payload: DEFAULT_MAX_MESSAGE_BYTES + 1,
            inbox_capacity: 2048,
            pending_sends: 2048,
            received_records: 8192,
            max_timeout: Duration::from_secs(90),
            dedup_retention: Duration::from_secs(91),
            ..MessagingConfig::default()
        };
        config.validate().unwrap();
        config
            .validate_send(
                SendOptions {
                    delivery: crate::Delivery::Applied,
                    timeout: Duration::from_secs(90),
                },
                DEFAULT_MAX_MESSAGE_BYTES + 1,
            )
            .unwrap();
        config.dedup_retention = config.max_timeout;
        assert!(config.validate().is_err());
        config.dedup_retention = Duration::from_secs(91);
        config.inbox_capacity = 0;
        assert!(config.validate().is_err());
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
        assert!(config.validate().is_err());
        config.dedup_retention = Duration::from_millis(2);
        config.validate().unwrap();
    }
}
