//! Admitted relay-data policy shared by the TCP and UDP rendezvous services.
//!
//! Control-rate protection never imposes a packet-rate ceiling on admitted relay
//! data; relay data is bounded only by backpressure or optional byte pacing.

use std::{num::NonZeroU64, time::Duration};
use tokio::time::Instant;

const NANOS_PER_SECOND: u128 = 1_000_000_000;

/// Optional per-source pacing of admitted relay payload bytes, never packets.
///
/// A TCP rendezvous delays reading a source that exceeds its byte budget. A UDP
/// rendezvous cannot push back on a datagram source without buffering, so it
/// drops admitted relay datagrams that exceed the budget.
#[derive(Clone, Copy, Debug, Default)]
pub enum RelayPacing {
    /// Rely only on bounded queues and transport backpressure.
    #[default]
    Backpressure,
    /// Pace application payload bytes, with a finite initial/idle burst.
    Bytes {
        /// Sustained payload bytes per second.
        bytes_per_second: NonZeroU64,
        /// Immediately available payload bytes after idle.
        burst_bytes: NonZeroU64,
    },
}

impl RelayPacing {
    /// Starts the per-source budget this policy requires, if any.
    pub(crate) fn budget(self, now: Instant) -> Option<Budget> {
        match self {
            Self::Backpressure => None,
            Self::Bytes {
                bytes_per_second,
                burst_bytes,
            } => Some(Budget::new(bytes_per_second.get(), burst_bytes.get(), now)),
        }
    }
}

/// Integer token bucket. [`Budget::ready_at`] reserves into debt for a single
/// waiting reader (TCP); [`Budget::try_take`] refuses instead (UDP datagrams).
#[derive(Debug)]
pub(crate) struct Budget {
    rate: u64,
    capacity: u128,
    credit: u128,
    updated: Instant,
}

impl Budget {
    pub(crate) fn new(rate: u64, burst: u64, now: Instant) -> Self {
        let capacity = u128::from(burst) * NANOS_PER_SECOND;
        Self {
            rate,
            capacity,
            credit: capacity,
            updated: now,
        }
    }

    fn refill(&mut self, now: Instant) {
        let refill = now
            .saturating_duration_since(self.updated)
            .as_nanos()
            .saturating_mul(u128::from(self.rate));
        self.credit = self.credit.saturating_add(refill).min(self.capacity);
    }

    /// Reserves `amount`, returning when the reservation is fully paid.
    pub(crate) fn ready_at(&mut self, now: Instant, amount: u64) -> Instant {
        self.refill(now);
        let cost = u128::from(amount) * NANOS_PER_SECOND;
        let deficit = cost.saturating_sub(self.credit);
        self.credit = self.credit.saturating_sub(cost);
        let nanos = deficit.div_ceil(u128::from(self.rate));
        // Reservations are one control message or at most one relay message,
        // so this duration fits u64 seconds even at one byte/second.
        let delay = Duration::new(
            u64::try_from(nanos / NANOS_PER_SECOND).expect("bounded reservation seconds"),
            u32::try_from(nanos % NANOS_PER_SECOND).expect("fractional second"),
        );
        self.updated = self.updated.max(now) + delay;
        self.updated
    }

    /// Spends `amount` only when it is already available; never goes into debt.
    pub(crate) fn try_take(&mut self, now: Instant, amount: u64) -> bool {
        self.refill(now);
        self.updated = self.updated.max(now);
        let cost = u128::from(amount) * NANOS_PER_SECOND;
        if cost > self.credit {
            return false;
        }
        self.credit -= cost;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_pacing_charges_bytes_not_packet_count() {
        let start = Instant::now();
        let mut small = Budget::new(1000, 1000, start);
        for _ in 0..1000 {
            assert_eq!(small.ready_at(start, 1), start);
        }
        assert_eq!(small.ready_at(start, 1), start + Duration::from_millis(1));
        let mut large = Budget::new(1000, 1000, start);
        assert_eq!(large.ready_at(start, 2000), start + Duration::from_secs(1));
        assert_eq!(
            large.ready_at(start + Duration::from_secs(1), 1000),
            start + Duration::from_secs(2)
        );
        assert_eq!(
            large.ready_at(start + Duration::from_secs(10), 1000),
            start + Duration::from_secs(10)
        );
    }

    #[test]
    fn dropping_budget_spends_available_bytes_and_refills_without_debt() {
        let start = Instant::now();
        let mut budget = Budget::new(1000, 1000, start);
        for _ in 0..1000 {
            assert!(budget.try_take(start, 1));
        }
        assert!(!budget.try_take(start, 1));
        // A refused request incurs no debt: the refill is immediately usable.
        assert!(budget.try_take(start + Duration::from_millis(1), 1));
        assert!(!budget.try_take(start + Duration::from_millis(1), 1));
        // Idle refill is capped at the burst.
        assert!(budget.try_take(start + Duration::from_secs(10), 1000));
        assert!(!budget.try_take(start + Duration::from_secs(10), 1));
        assert!(RelayPacing::Backpressure.budget(start).is_none());
    }
}
