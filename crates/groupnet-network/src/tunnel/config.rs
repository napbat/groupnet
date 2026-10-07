//! Node-wide bounded tunnel resource and reliability policy.

use std::{
    io,
    num::{NonZeroU16, NonZeroUsize},
    time::Duration,
};

use groupnet_transport::QueueCapacity;

use super::wire::MAX_SEGMENT;

/// Control packets (open, heartbeat, close) queued per session beyond the
/// window-derived data and acknowledgement allowance.
const PACKET_QUEUE_SLACK: usize = 16;

/// Ciphertext bytes carried by one reliability segment.
///
/// Values lie in `1..=`[`SegmentSize::MAX`]. `MAX` is the protocol-wide receive
/// bound — the largest segment a default routing envelope carries between
/// one-byte identities — so every endpoint accepts any peer's segment size and
/// peers may configure different send segments.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SegmentSize(NonZeroUsize);

impl SegmentSize {
    /// The protocol-wide receive bound shared by every endpoint.
    pub const MAX: Self = Self::of(MAX_SEGMENT);

    /// Validates a segment size, returning `None` for zero or a value above
    /// [`Self::MAX`].
    #[must_use]
    pub const fn new(bytes: usize) -> Option<Self> {
        if bytes > MAX_SEGMENT {
            return None;
        }
        match NonZeroUsize::new(bytes) {
            Some(bytes) => Some(Self(bytes)),
            None => None,
        }
    }

    /// A segment size known valid at compile time, for constants and defaults.
    ///
    /// # Panics
    /// If `bytes` is zero or above [`Self::MAX`]; in a `const` context this is
    /// a compile error.
    #[must_use]
    pub const fn of(bytes: usize) -> Self {
        match Self::new(bytes) {
            Some(size) => size,
            None => panic!("segment size must be within 1..=SegmentSize::MAX"),
        }
    }

    /// The number of ciphertext bytes.
    #[must_use]
    pub const fn get(self) -> usize {
        self.0.get()
    }
}

impl TryFrom<usize> for SegmentSize {
    type Error = io::Error;

    /// # Errors
    /// Returns `InvalidInput` for zero or a value above [`SegmentSize::MAX`].
    fn try_from(bytes: usize) -> io::Result<Self> {
        Self::new(bytes).ok_or_else(|| invalid("segment size must be within 1..=SegmentSize::MAX"))
    }
}

/// Retransmission timeout policy, ordered once at construction:
/// `0 < min <= initial <= max`.
///
/// `min` floors the RTT-derived timeout and is the retransmission timer
/// cadence; `initial` applies before the first RTT sample and paces session
/// opening; `max` caps exponential backoff.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetransmitTimeouts {
    min: Duration,
    initial: Duration,
    max: Duration,
}

impl RetransmitTimeouts {
    /// Validates ordered retransmission timeouts.
    ///
    /// # Errors
    /// Returns `InvalidInput` for a zero minimum, unordered bounds, or a
    /// maximum that cannot extend the current Tokio instant.
    pub fn new(min: Duration, initial: Duration, max: Duration) -> io::Result<Self> {
        if min.is_zero()
            || initial < min
            || max < initial
            || tokio::time::Instant::now().checked_add(max).is_none()
        {
            return Err(invalid(
                "retransmission timeouts must satisfy 0 < min <= initial <= max",
            ));
        }
        Ok(Self { min, initial, max })
    }

    /// Floor of the RTT-derived timeout and the retransmission timer cadence.
    #[must_use]
    pub const fn min(self) -> Duration {
        self.min
    }

    /// Timeout used before the first RTT sample.
    #[must_use]
    pub const fn initial(self) -> Duration {
        self.initial
    }

    /// Cap of exponential retransmission backoff.
    #[must_use]
    pub const fn max(self) -> Duration {
        self.max
    }

    pub(super) fn clamp(self, estimate: Duration) -> Duration {
        estimate.clamp(self.min, self.max)
    }

    pub(super) fn back_off(self, timeout: Duration) -> Duration {
        timeout.saturating_mul(2).min(self.max)
    }
}

impl Default for RetransmitTimeouts {
    /// A 200 ms floor (TCP practice: spurious retransmits over reliable links
    /// duplicate data and halve the congestion window), the RFC 6298 one-second
    /// initial timeout, and a two-second backoff cap.
    fn default() -> Self {
        Self {
            min: Duration::from_millis(200),
            initial: Duration::from_secs(1),
            max: Duration::from_secs(2),
        }
    }
}

/// Resource bounds and reliability policy for pinned TLS tunnel sessions on one node.
///
/// Defaults are bulk-capable: 16 KiB send segments and a 64-segment window keep
/// up to 1 MiB in flight per stream direction (about 100 Mbit/s at 80 ms RTT).
///
/// Per session, retained ciphertext is bounded by `window × payload` awaiting
/// acknowledgement, `window × SegmentSize::MAX` awaiting in-order delivery
/// (`window × payload` of the sending peer when it uses these defaults), and
/// `2 × stream_buffer` of TLS buffering. The inbound packet queue
/// ([`packet_queue`](Self::packet_queue)) holds only traffic not yet assigned to
/// those buffers. With defaults and default peers a saturated session retains
/// about 2 MiB; `max_sessions` multiplies the node-wide bound.
#[derive(Clone, Debug)]
pub struct TunnelLimits {
    /// Maximum admitted certificate pins.
    pub max_peers: NonZeroUsize,
    /// Maximum active or establishing sessions.
    pub max_sessions: NonZeroUsize,
    /// Maximum sessions sharing one admitted peer; at most `max_sessions`.
    pub sessions_per_peer: NonZeroUsize,
    /// Capacity of each isolated authenticated accept queue.
    pub accept_queue: QueueCapacity,
    /// Bytes of TLS ciphertext buffered in each direction of a session.
    pub stream_buffer: NonZeroUsize,
    /// Local send segment: ciphertext bytes read into one reliability segment.
    /// It must also fit the router envelope to each admitted peer. Receiving
    /// accepts any peer segment up to [`SegmentSize::MAX`].
    pub payload: SegmentSize,
    /// Receive window and congestion-window ceiling, in segments; advertised as
    /// the wire's u16 credit.
    pub window: NonZeroU16,
    /// Initial congestion window in segments; at most `window`. Slow start
    /// doubles it per round trip until loss or the window.
    pub initial_congestion: NonZeroU16,
    /// TLS setup and authenticated preamble deadline.
    pub setup_timeout: Duration,
    /// Retransmission timeout floor, initial value and backoff cap.
    pub retransmit: RetransmitTimeouts,
    /// Expiration after the last valid peer packet; longer than the heartbeat
    /// interval and the retransmission backoff cap.
    pub peer_timeout: Duration,
    /// Interval between idle acknowledgement heartbeats.
    pub heartbeat_interval: Duration,
}

impl Default for TunnelLimits {
    fn default() -> Self {
        Self {
            max_peers: const { NonZeroUsize::new(128).unwrap() },
            max_sessions: const { NonZeroUsize::new(64).unwrap() },
            sessions_per_peer: const { NonZeroUsize::new(8).unwrap() },
            accept_queue: QueueCapacity::of(32),
            stream_buffer: const { NonZeroUsize::new(32 * 1024).unwrap() },
            payload: SegmentSize::of(16 * 1024),
            window: const { NonZeroU16::new(64).unwrap() },
            initial_congestion: const { NonZeroU16::new(4).unwrap() },
            setup_timeout: Duration::from_secs(10),
            retransmit: RetransmitTimeouts::default(),
            peer_timeout: Duration::from_secs(20),
            heartbeat_interval: Duration::from_secs(1),
        }
    }
}

impl TunnelLimits {
    /// Validates relationships between fields; each field's own range is
    /// carried by its type.
    ///
    /// # Errors
    /// Rejects per-peer sessions above `max_sessions`, an initial congestion
    /// window above `window`, an unaddressable stream buffer, or inconsistent
    /// or unrepresentable deadlines.
    pub fn validate(&self) -> io::Result<()> {
        let now = tokio::time::Instant::now();
        if self.sessions_per_peer > self.max_sessions
            || self.stream_buffer.get() > isize::MAX.unsigned_abs()
            || self.initial_congestion > self.window
            || self.setup_timeout.is_zero()
            || self.heartbeat_interval.is_zero()
            || self.peer_timeout <= self.heartbeat_interval
            || self.peer_timeout <= self.retransmit.max()
            || [self.setup_timeout, self.peer_timeout]
                .iter()
                .any(|duration| now.checked_add(*duration).is_none())
        {
            return Err(invalid("invalid tunnel limits"));
        }
        Ok(())
    }

    /// Per-session inbound packet queue, derived from the window: a window of
    /// peer data plus acknowledgements for a window of local data, with slack
    /// for control packets. Arrivals beyond it are dropped as packet loss.
    #[must_use]
    pub fn packet_queue(&self) -> QueueCapacity {
        QueueCapacity::of(usize::from(self.window.get()))
            .saturating_mul(2)
            .saturating_add(PACKET_QUEUE_SLACK)
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sessions(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).unwrap()
    }

    fn segments(value: u16) -> NonZeroU16 {
        NonZeroU16::new(value).unwrap()
    }

    #[test]
    fn defaults_are_bulk_capable_and_valid() {
        let limits = TunnelLimits::default();
        assert!(limits.validate().is_ok());
        assert_eq!(limits.payload.get(), 16 * 1024);
        assert_eq!(limits.window.get(), 64);
        assert_eq!(limits.retransmit.min(), Duration::from_millis(200));
        assert_eq!(limits.packet_queue().get(), 2 * 64 + PACKET_QUEUE_SLACK);
    }

    #[test]
    fn segment_size_is_bounded_by_the_protocol_receive_bound() {
        assert_eq!(SegmentSize::new(0), None);
        assert_eq!(SegmentSize::new(MAX_SEGMENT), Some(SegmentSize::MAX));
        assert_eq!(SegmentSize::new(MAX_SEGMENT + 1), None);
        assert_eq!(
            SegmentSize::try_from(MAX_SEGMENT + 1).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(SegmentSize::try_from(512).unwrap().get(), 512);
    }

    #[test]
    fn retransmit_timeouts_are_ordered_and_representable() {
        let ms = Duration::from_millis;
        assert!(RetransmitTimeouts::new(ms(200), ms(200), ms(200)).is_ok());
        assert!(RetransmitTimeouts::new(Duration::ZERO, ms(200), ms(400)).is_err());
        assert!(RetransmitTimeouts::new(ms(300), ms(200), ms(400)).is_err());
        assert!(RetransmitTimeouts::new(ms(200), ms(500), ms(400)).is_err());
        assert!(RetransmitTimeouts::new(ms(200), ms(500), Duration::MAX).is_err());
        let timeouts = RetransmitTimeouts::new(ms(200), ms(500), ms(800)).unwrap();
        assert_eq!(timeouts.clamp(ms(10)), ms(200));
        assert_eq!(timeouts.clamp(ms(5000)), ms(800));
        assert_eq!(timeouts.back_off(ms(300)), ms(600));
        assert_eq!(timeouts.back_off(ms(600)), ms(800));
        assert_eq!(timeouts.back_off(Duration::MAX), ms(800));
    }

    #[test]
    fn packet_queue_is_derived_from_the_window() {
        let largest = TunnelLimits {
            window: NonZeroU16::MAX,
            ..TunnelLimits::default()
        };
        assert_eq!(
            largest.packet_queue().get(),
            2 * usize::from(u16::MAX) + PACKET_QUEUE_SLACK
        );
        let smallest = TunnelLimits {
            window: segments(1),
            initial_congestion: segments(1),
            ..TunnelLimits::default()
        };
        assert_eq!(smallest.packet_queue().get(), 2 + PACKET_QUEUE_SLACK);
    }

    #[test]
    fn cross_field_relations_are_validated() {
        let larger = TunnelLimits {
            max_peers: sessions(8192),
            max_sessions: sessions(2048),
            sessions_per_peer: sessions(16),
            payload: SegmentSize::of(4096),
            window: segments(1024),
            ..TunnelLimits::default()
        };
        assert!(larger.validate().is_ok());
        let rejected = [
            TunnelLimits {
                sessions_per_peer: sessions(2049),
                ..larger.clone()
            },
            TunnelLimits {
                initial_congestion: segments(1025),
                ..larger.clone()
            },
            TunnelLimits {
                setup_timeout: Duration::ZERO,
                ..larger.clone()
            },
            TunnelLimits {
                setup_timeout: Duration::MAX,
                ..larger.clone()
            },
            TunnelLimits {
                heartbeat_interval: Duration::ZERO,
                ..larger.clone()
            },
            TunnelLimits {
                peer_timeout: larger.heartbeat_interval,
                ..larger.clone()
            },
            TunnelLimits {
                peer_timeout: larger.retransmit.max(),
                ..larger.clone()
            },
        ];
        for limits in rejected {
            assert_eq!(
                limits.validate().unwrap_err().kind(),
                io::ErrorKind::InvalidInput,
                "{limits:?}"
            );
        }
    }
}
