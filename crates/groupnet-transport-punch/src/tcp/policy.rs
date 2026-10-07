//! Operational rendezvous limits and the independent control budget.

use crate::RelayPacing;
use groupnet_transport::QueueCapacity;
use std::{io, num::NonZeroU32};

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

/// Validated operational limits for a TCP rendezvous.
/// Identity, candidate, credential and frame-length wire bounds are independent.
#[derive(Clone, Copy, Debug)]
pub struct TcpRendezvousConfig {
    /// Maximum concurrently admitted sessions.
    pub max_sessions: QueueCapacity,
    /// Maximum concurrent unauthenticated admission attempts.
    pub max_pending: QueueCapacity,
    /// Bounded shared admission/lifecycle event capacity.
    pub event_queue: QueueCapacity,
    /// Bounded per-session relay-data output capacity.
    pub session_queue: QueueCapacity,
    /// Independent bounded control output capacity; must hold one admission
    /// introduction per admitted session (at least `max_sessions`).
    pub control_queue: QueueCapacity,
    /// Independent abuse bound for control and rejected relay requests.
    pub control: ControlRateLimit,
    /// Optional byte-based policy for valid, admitted relay data.
    pub relay_pacing: RelayPacing,
}

impl Default for TcpRendezvousConfig {
    fn default() -> Self {
        Self {
            max_sessions: QueueCapacity::of(128),
            max_pending: QueueCapacity::of(32),
            event_queue: QueueCapacity::of(128),
            session_queue: QueueCapacity::of(128),
            control: ControlRateLimit::default(),
            control_queue: QueueCapacity::of(128),
            relay_pacing: RelayPacing::Backpressure,
        }
    }
}

impl TcpRendezvousConfig {
    pub(super) fn validate(self) -> io::Result<()> {
        if self.control_queue < self.max_sessions {
            return Err(super::invalid(
                "TCP control queue cannot hold admission introductions",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pacing::Budget;
    use std::time::Duration;
    use tokio::time::Instant;

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
            max_sessions: QueueCapacity::of(512),
            control_queue: QueueCapacity::of(512),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
        assert!(
            TcpRendezvousConfig {
                control_queue: QueueCapacity::of(1),
                ..Default::default()
            }
            .validate()
            .is_err()
        );
    }
}
