//! Operational rendezvous limits and independent control/data pacing.

use std::{
    io,
    num::{NonZeroU32, NonZeroU64},
    time::Duration,
};
use tokio::time::Instant;

/// Rate and finite burst allowance for one admitted control connection.
#[derive(Clone, Copy, Debug)]
pub struct ControlRateLimit {
    /// Sustained control messages per second; relay data does not consume this budget.
    pub frames_per_second: NonZeroU32,
    /// Immediately available control messages after idle.
    pub burst_frames: NonZeroU32,
}

impl Default for ControlRateLimit {
    fn default() -> Self {
        Self {
            frames_per_second: NonZeroU32::new(32).expect("nonzero default"),
            burst_frames: NonZeroU32::new(32).expect("nonzero default"),
        }
    }
}

/// Optional per-source pacing of admitted relay payload bytes, never packets.
#[derive(Clone, Copy, Debug, Default)]
pub enum RelayPacing {
    /// Rely only on bounded queues and TCP backpressure.
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

/// Validated operational limits for a TCP rendezvous.
/// Identity, candidate, credential and frame-length wire bounds are independent.
#[derive(Clone, Copy, Debug)]
pub struct TcpRendezvousConfig {
    /// Maximum concurrently admitted sessions.
    pub max_sessions: usize,
    /// Maximum concurrent unauthenticated admission attempts.
    pub max_pending: usize,
    /// Bounded shared admission/lifecycle event capacity.
    pub event_queue: usize,
    /// Bounded per-session relay-data output capacity.
    pub session_queue: usize,
    /// Independent bounded control output capacity; accommodates admission introductions.
    pub control_queue: usize,
    /// Independent abuse bound for control and rejected relay requests.
    pub control: ControlRateLimit,
    /// Optional byte-based policy for valid, admitted relay data.
    pub relay_pacing: RelayPacing,
}

impl Default for TcpRendezvousConfig {
    fn default() -> Self {
        Self {
            max_sessions: 128,
            max_pending: 32,
            event_queue: 128,
            session_queue: 128,
            control: ControlRateLimit::default(),
            control_queue: 128,
            relay_pacing: RelayPacing::Backpressure,
        }
    }
}

impl TcpRendezvousConfig {
    pub(super) fn validate(self) -> io::Result<()> {
        for capacity in [
            self.max_sessions,
            self.max_pending,
            self.event_queue,
            self.session_queue,
            self.control_queue,
        ] {
            super::validate_capacity(capacity)?;
        }
        if self.control_queue < self.max_sessions {
            return Err(super::invalid(
                "TCP control queue cannot hold admission introductions",
            ));
        }
        self.max_sessions
            .checked_add(self.max_pending)
            .ok_or_else(|| super::invalid("TCP task capacity overflows"))?;
        Ok(())
    }
}

/// Integer token bucket. Only one decoded message waits outside each reader's
/// bounded queue; future reservations do not create additional queued work.
pub(super) struct Budget {
    rate: u64,
    capacity: u128,
    credit: u128,
    updated: Instant,
}

impl Budget {
    pub(super) fn new(rate: u64, burst: u64, now: Instant) -> Self {
        let capacity = u128::from(burst) * 1_000_000_000;
        Self {
            rate,
            capacity,
            credit: capacity,
            updated: now,
        }
    }

    pub(super) fn ready_at(&mut self, now: Instant, amount: u64) -> Instant {
        let refill = now
            .saturating_duration_since(self.updated)
            .as_nanos()
            .saturating_mul(u128::from(self.rate));
        self.credit = self.credit.saturating_add(refill).min(self.capacity);
        let cost = u128::from(amount) * 1_000_000_000;
        let deficit = cost.saturating_sub(self.credit);
        self.credit = self.credit.saturating_sub(cost);
        let nanos = deficit.div_ceil(u128::from(self.rate));
        // Reservations are one control message or at most MAX_TCP_MESSAGE bytes,
        // so this duration fits u64 seconds even at one byte/second.
        let delay = Duration::new(
            u64::try_from(nanos / 1_000_000_000).expect("bounded reservation seconds"),
            u32::try_from(nanos % 1_000_000_000).expect("fractional second"),
        );
        self.updated = self.updated.max(now) + delay;
        self.updated
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
    fn independent_control_budget_does_not_throttle_relay_packets() {
        let start = Instant::now();
        let mut control = Budget::new(2, 2, start);
        assert_eq!(control.ready_at(start, 1), start);
        assert_eq!(control.ready_at(start, 1), start);
        assert_eq!(
            control.ready_at(start, 1),
            start + Duration::from_millis(500)
        );
        assert!(matches!(
            TcpRendezvousConfig::default().relay_pacing,
            RelayPacing::Backpressure
        ));
    }

    #[test]
    fn capacities_are_operational_not_fixed_peer_limits() {
        let config = TcpRendezvousConfig {
            max_sessions: 512,
            control_queue: 512,
            ..Default::default()
        };
        assert!(config.validate().is_ok());
        assert!(
            TcpRendezvousConfig {
                max_pending: 0,
                ..Default::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            TcpRendezvousConfig {
                control_queue: 1,
                ..Default::default()
            }
            .validate()
            .is_err()
        );
    }
}
