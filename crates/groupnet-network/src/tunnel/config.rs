//! Node-wide bounded tunnel resource and reliability policy.
use std::{io, time::Duration};

/// Resource bounds for pinned TLS tunnel sessions on one node.
#[derive(Clone, Debug)]
pub struct TunnelLimits {
    /// Maximum admitted certificate pins.
    pub max_peers: usize,
    /// Maximum active or establishing sessions.
    pub max_sessions: usize,
    /// Maximum sessions sharing one admitted peer.
    pub sessions_per_peer: usize,
    /// Capacity of each isolated authenticated accept queue.
    pub accept_queue: usize,
    /// Capacity of each session's packet inbox.
    pub packet_queue: usize,
    /// Bytes of TLS ciphertext buffered in each direction of a session.
    pub stream_buffer: usize,
    /// Ciphertext bytes per reliability segment, bounded by the routing envelope.
    pub payload: usize,
    /// Receive/retransmission window; fits the wire u16 credit.
    pub window: usize,
    /// Initial congestion window, no larger than the receive window.
    pub initial_congestion: usize,
    /// TLS setup and authenticated preamble deadline.
    pub setup_timeout: Duration,
    /// First retransmission timeout.
    pub initial_rto: Duration,
    /// Minimum measured retransmission timeout and timer cadence.
    pub min_rto: Duration,
    /// Maximum retransmission backoff.
    pub max_rto: Duration,
    /// Expiration after the last valid peer packet.
    pub peer_timeout: Duration,
    /// Interval between idle acknowledgement heartbeats.
    pub heartbeat_interval: Duration,
}

impl Default for TunnelLimits {
    fn default() -> Self {
        Self {
            max_peers: 128,
            max_sessions: 64,
            sessions_per_peer: 8,
            accept_queue: 32,
            packet_queue: 64,
            stream_buffer: 32 * 1024,
            payload: 512,
            window: 32,
            initial_congestion: 4,
            setup_timeout: Duration::from_secs(10),
            initial_rto: Duration::from_millis(150),
            min_rto: Duration::from_millis(25),
            max_rto: Duration::from_secs(2),
            peer_timeout: Duration::from_secs(20),
            heartbeat_interval: Duration::from_secs(1),
        }
    }
}

impl TunnelLimits {
    /// Validates bounded channels, sequence credit, and timer relationships.
    /// # Errors
    /// Rejects zero capacities, unrepresentable windows, or inconsistent timers.
    pub fn validate(&self) -> io::Result<()> {
        let now = tokio::time::Instant::now();
        let timers = [
            self.setup_timeout,
            self.min_rto,
            self.initial_rto,
            self.max_rto,
            self.heartbeat_interval,
            self.peer_timeout,
        ];
        if self.max_peers == 0
            || self.max_sessions == 0
            || self.sessions_per_peer == 0
            || self.sessions_per_peer > self.max_sessions
            || self.stream_buffer == 0
            || self.stream_buffer > isize::MAX as usize
            || self.payload == 0
            || u32::try_from(self.payload).is_err()
            || self.payload.checked_add(super::wire::HEADER).is_none()
            || self.window == 0
            || self.window > usize::from(u16::MAX)
            || self.initial_congestion == 0
            || self.initial_congestion > self.window
            || [self.accept_queue, self.packet_queue]
                .iter()
                .any(|capacity| *capacity == 0 || *capacity > tokio::sync::Semaphore::MAX_PERMITS)
            || self.setup_timeout.is_zero()
            || self.min_rto.is_zero()
            || self.initial_rto < self.min_rto
            || self.max_rto < self.initial_rto
            || self.heartbeat_interval.is_zero()
            || self.peer_timeout <= self.heartbeat_interval
            || self.peer_timeout <= self.max_rto
            || timers
                .iter()
                .any(|duration| now.checked_add(*duration).is_none())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid tunnel limits",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configurable_capacities_keep_credit_and_queue_representability() {
        let larger = TunnelLimits {
            max_peers: 8192,
            max_sessions: 2048,
            sessions_per_peer: 16,
            payload: 4096,
            window: 1024,
            ..TunnelLimits::default()
        };
        assert!(larger.validate().is_ok());
        assert!(
            TunnelLimits {
                window: 65_536,
                ..larger.clone()
            }
            .validate()
            .is_err()
        );
        assert!(
            TunnelLimits {
                initial_congestion: 1025,
                ..larger.clone()
            }
            .validate()
            .is_err()
        );
        assert!(
            TunnelLimits {
                packet_queue: 0,
                ..larger.clone()
            }
            .validate()
            .is_err()
        );
        assert!(
            TunnelLimits {
                setup_timeout: Duration::ZERO,
                ..larger.clone()
            }
            .validate()
            .is_err()
        );
        assert!(
            TunnelLimits {
                setup_timeout: Duration::MAX,
                ..larger.clone()
            }
            .validate()
            .is_err()
        );
        assert!(
            TunnelLimits {
                peer_timeout: larger.heartbeat_interval,
                ..larger
            }
            .validate()
            .is_err()
        );
    }
}
