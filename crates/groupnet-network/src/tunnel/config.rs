//! Node-wide bounded tunnel resource and reliability policy.

use std::{
    fmt, io,
    num::{NonZeroU16, NonZeroU32, NonZeroUsize},
    time::Duration,
};

use groupnet_transport::QueueCapacity;

use super::wire::{MAX_SEGMENT, cost};

/// Control packets (open, heartbeat, close) queued per session beyond the
/// window-derived data and acknowledgement allowance.
const PACKET_QUEUE_SLACK: usize = 16;

const DEFAULT_PAYLOAD: SegmentSize = SegmentSize::of(16 * 1024);

const DEFAULT_MAX_WINDOW: NonZeroU32 = NonZeroU32::new(16 << 20).unwrap();

/// Frames one saturated default stream keeps queued; router queue defaults are
/// multiples of it.
pub(crate) const DEFAULT_STREAM_FRAMES: usize = stream_frames(DEFAULT_MAX_WINDOW, DEFAULT_PAYLOAD);

/// Segments of `payload` that fill `window`.
const fn max_segments(window: NonZeroU32, payload: SegmentSize) -> usize {
    (window.get() as usize).div_ceil(payload.get())
}

/// A window of data segments plus one acknowledgement per segment.
const fn stream_frames(window: NonZeroU32, payload: SegmentSize) -> usize {
    2 * max_segments(window, payload)
}

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
    type Error = TunnelLimitsError;

    /// # Errors
    /// Returns [`TunnelLimitsError::PayloadOutOfRange`] for zero or a value
    /// above [`SegmentSize::MAX`].
    fn try_from(bytes: usize) -> Result<Self, TunnelLimitsError> {
        Self::new(bytes).ok_or(TunnelLimitsError::PayloadOutOfRange)
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
    /// Returns [`TunnelLimitsError::RetransmitUnordered`] unless
    /// `0 < min <= initial <= max`, or
    /// [`TunnelLimitsError::UnrepresentableTimer`] for a maximum that cannot
    /// extend the current Tokio instant.
    pub fn new(min: Duration, initial: Duration, max: Duration) -> Result<Self, TunnelLimitsError> {
        if min.is_zero() || initial < min || max < initial {
            return Err(TunnelLimitsError::RetransmitUnordered);
        }
        if tokio::time::Instant::now().checked_add(max).is_none() {
            return Err(TunnelLimitsError::UnrepresentableTimer {
                field: "retransmit.max",
            });
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
/// Defaults are sized for one stream over a 1 Gbit/s × 100 ms path: a 16 MiB
/// window per direction (about 1.6 Gbit/s at 80 ms RTT). All retained
/// ciphertext of a transport — unacknowledged sent segments and received
/// segments awaiting reordering or delivery — is bounded by `memory_budget`.
/// Each session direction holds a guaranteed `min_window` floor and grows
/// opportunistically from the remaining budget up to `max_window`.
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
    /// Guaranteed per-direction reservation of every session, in bytes of
    /// segment cost (ciphertext plus 38-byte header); holds at least one
    /// `payload` segment and at most `max_window`.
    pub min_window: NonZeroU32,
    /// Per-direction ceiling on retained ciphertext: unacknowledged sent bytes
    /// and advertised receive credit, in bytes of segment cost.
    pub max_window: NonZeroU32,
    /// Node-wide bound on retained tunnel ciphertext of this transport; at
    /// least `max_sessions × 2 × min_window`.
    pub memory_budget: NonZeroUsize,
    /// Initial congestion window in segments; at most `max_window / payload`.
    /// Slow start doubles it per round trip until loss or that ceiling.
    pub initial_congestion: NonZeroU16,
    /// TLS setup and authenticated preamble deadline.
    pub setup_timeout: Duration,
    /// Retransmission timeout floor, initial value and backoff cap.
    pub retransmit: RetransmitTimeouts,
    /// Expiration after the last valid peer packet; longer than the heartbeat
    /// interval and the retransmission backoff cap.
    pub peer_timeout: Duration,
    /// Interval between idle acknowledgement heartbeats; also how long a sender
    /// stays without new data before relinquishing its receive credit.
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
            payload: DEFAULT_PAYLOAD,
            min_window: const { NonZeroU32::new(128 * 1024).unwrap() },
            max_window: DEFAULT_MAX_WINDOW,
            memory_budget: const { NonZeroUsize::new(128 << 20).unwrap() },
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
    /// Returns the first violated [`TunnelLimitsError`] rule.
    pub fn validate(&self) -> Result<(), TunnelLimitsError> {
        if self.sessions_per_peer > self.max_sessions {
            return Err(TunnelLimitsError::SessionsPerPeerAboveMaxSessions);
        }
        if self.stream_buffer.get() > isize::MAX.unsigned_abs() {
            return Err(TunnelLimitsError::StreamBufferUnaddressable);
        }
        if (self.min_window.get() as usize) < cost(self.payload.get()) {
            return Err(TunnelLimitsError::MinWindowBelowPayloadSegment);
        }
        if self.min_window > self.max_window {
            return Err(TunnelLimitsError::MinWindowAboveMaxWindow);
        }
        if self
            .max_sessions
            .get()
            .checked_mul(2 * self.min_window.get() as usize)
            .is_none_or(|floors| floors > self.memory_budget.get())
        {
            return Err(TunnelLimitsError::MemoryBudgetBelowSessionFloors);
        }
        if usize::from(self.initial_congestion.get()) > self.max_segments() {
            return Err(TunnelLimitsError::InitialCongestionAboveMaxSegments);
        }
        if self.setup_timeout.is_zero() {
            return Err(TunnelLimitsError::ZeroSetupTimeout);
        }
        if self.heartbeat_interval.is_zero() {
            return Err(TunnelLimitsError::ZeroHeartbeatInterval);
        }
        if self.peer_timeout <= self.heartbeat_interval {
            return Err(TunnelLimitsError::PeerTimeoutNotAboveHeartbeat);
        }
        if self.peer_timeout <= self.retransmit.max() {
            return Err(TunnelLimitsError::PeerTimeoutNotAboveRetransmitMax);
        }
        let now = tokio::time::Instant::now();
        for (field, timer) in [
            ("setup_timeout", self.setup_timeout),
            ("peer_timeout", self.peer_timeout),
        ] {
            if now.checked_add(timer).is_none() {
                return Err(TunnelLimitsError::UnrepresentableTimer { field });
            }
        }
        Ok(())
    }

    /// Segments of `payload` that fill `max_window`: the congestion-window cap.
    #[must_use]
    pub const fn max_segments(&self) -> usize {
        max_segments(self.max_window, self.payload)
    }

    /// Frames one saturated stream keeps queued: a window of data segments plus
    /// their acknowledgements. Size router link and tunnel queues in multiples
    /// of it.
    #[must_use]
    pub const fn stream_frames(&self) -> usize {
        stream_frames(self.max_window, self.payload)
    }

    /// Per-session inbound packet queue, derived from the window: a window of
    /// peer data plus acknowledgements for a window of local data, with slack
    /// for control packets. Arrivals beyond it are dropped as packet loss.
    #[must_use]
    pub fn packet_queue(&self) -> QueueCapacity {
        QueueCapacity::of(self.stream_frames()).saturating_add(PACKET_QUEUE_SLACK)
    }

    /// Bytes set aside so every possible session holds both direction floors.
    pub(super) fn floor_budget(&self) -> usize {
        self.max_sessions.get() * 2 * self.min_window.get() as usize
    }
}

/// A violated [`TunnelLimits`], [`RetransmitTimeouts`] or [`SegmentSize`]
/// rule, naming the offending field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TunnelLimitsError {
    /// `payload` is zero or above [`SegmentSize::MAX`].
    PayloadOutOfRange,
    /// `retransmit` violates `0 < min <= initial <= max`.
    RetransmitUnordered,
    /// `sessions_per_peer` exceeds `max_sessions`.
    SessionsPerPeerAboveMaxSessions,
    /// `stream_buffer` exceeds the addressable `isize::MAX` bytes.
    StreamBufferUnaddressable,
    /// `min_window` cannot hold one `payload` segment and its header.
    MinWindowBelowPayloadSegment,
    /// `min_window` exceeds `max_window`.
    MinWindowAboveMaxWindow,
    /// `memory_budget` is below `max_sessions × 2 × min_window`.
    MemoryBudgetBelowSessionFloors,
    /// `initial_congestion` exceeds `max_window / payload` segments.
    InitialCongestionAboveMaxSegments,
    /// `setup_timeout` is zero.
    ZeroSetupTimeout,
    /// `heartbeat_interval` is zero.
    ZeroHeartbeatInterval,
    /// `peer_timeout` does not exceed `heartbeat_interval`.
    PeerTimeoutNotAboveHeartbeat,
    /// `peer_timeout` does not exceed the `retransmit` backoff cap.
    PeerTimeoutNotAboveRetransmitMax,
    /// A timer cannot extend the current monotonic instant.
    UnrepresentableTimer {
        /// The offending field.
        field: &'static str,
    },
}

impl fmt::Display for TunnelLimitsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::PayloadOutOfRange => "tunnel payload must be within 1..=SegmentSize::MAX",
            Self::RetransmitUnordered => {
                "tunnel retransmit timeouts must satisfy 0 < min <= initial <= max"
            }
            Self::SessionsPerPeerAboveMaxSessions => {
                "tunnel sessions_per_peer must not exceed max_sessions"
            }
            Self::StreamBufferUnaddressable => "tunnel stream_buffer exceeds isize::MAX bytes",
            Self::MinWindowBelowPayloadSegment => {
                "tunnel min_window must hold one payload segment and its header"
            }
            Self::MinWindowAboveMaxWindow => "tunnel min_window must not exceed max_window",
            Self::MemoryBudgetBelowSessionFloors => {
                "tunnel memory_budget must cover max_sessions x 2 x min_window"
            }
            Self::InitialCongestionAboveMaxSegments => {
                "tunnel initial_congestion must not exceed max_window / payload"
            }
            Self::ZeroSetupTimeout => "tunnel setup_timeout must be nonzero",
            Self::ZeroHeartbeatInterval => "tunnel heartbeat_interval must be nonzero",
            Self::PeerTimeoutNotAboveHeartbeat => {
                "tunnel peer_timeout must exceed heartbeat_interval"
            }
            Self::PeerTimeoutNotAboveRetransmitMax => {
                "tunnel peer_timeout must exceed the retransmit backoff cap"
            }
            Self::UnrepresentableTimer { field } => {
                return write!(f, "tunnel {field} exceeds the monotonic clock");
            }
        };
        f.write_str(message)
    }
}

impl std::error::Error for TunnelLimitsError {}

impl From<TunnelLimitsError> for io::Error {
    fn from(error: TunnelLimitsError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn count(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).unwrap()
    }

    fn bytes(value: u32) -> NonZeroU32 {
        NonZeroU32::new(value).unwrap()
    }

    #[test]
    fn defaults_cover_a_gigabit_long_path_within_the_budget() {
        let limits = TunnelLimits::default();
        assert!(limits.validate().is_ok());
        assert_eq!(limits.payload.get(), 16 * 1024);
        assert_eq!(limits.max_window.get(), 16 << 20);
        // 1 Gbit/s × 100 ms = 12.5 MB fits one window.
        assert!(limits.max_window.get() as usize > 1_000_000_000 / 8 / 10);
        assert_eq!(limits.floor_budget(), 16 << 20);
        assert_eq!(limits.memory_budget.get(), 128 << 20);
        assert_eq!(limits.retransmit.min(), Duration::from_millis(200));
        assert_eq!(limits.max_segments(), 1024);
        assert_eq!(limits.stream_frames(), DEFAULT_STREAM_FRAMES);
        assert_eq!(DEFAULT_STREAM_FRAMES, 2048);
        assert_eq!(limits.packet_queue().get(), 2048 + PACKET_QUEUE_SLACK);
    }

    #[test]
    fn segment_size_is_bounded_by_the_protocol_receive_bound() {
        assert_eq!(SegmentSize::new(0), None);
        assert_eq!(SegmentSize::new(MAX_SEGMENT), Some(SegmentSize::MAX));
        assert_eq!(SegmentSize::new(MAX_SEGMENT + 1), None);
        assert_eq!(
            SegmentSize::try_from(MAX_SEGMENT + 1),
            Err(TunnelLimitsError::PayloadOutOfRange)
        );
        assert_eq!(SegmentSize::try_from(512).unwrap().get(), 512);
    }

    #[test]
    fn retransmit_timeouts_are_ordered_and_representable() {
        let ms = Duration::from_millis;
        assert!(RetransmitTimeouts::new(ms(200), ms(200), ms(200)).is_ok());
        for (min, initial, max) in [
            (Duration::ZERO, ms(200), ms(400)),
            (ms(300), ms(200), ms(400)),
            (ms(200), ms(500), ms(400)),
        ] {
            assert_eq!(
                RetransmitTimeouts::new(min, initial, max),
                Err(TunnelLimitsError::RetransmitUnordered)
            );
        }
        assert_eq!(
            RetransmitTimeouts::new(ms(200), ms(500), Duration::MAX),
            Err(TunnelLimitsError::UnrepresentableTimer {
                field: "retransmit.max"
            })
        );
        let timeouts = RetransmitTimeouts::new(ms(200), ms(500), ms(800)).unwrap();
        assert_eq!(timeouts.clamp(ms(10)), ms(200));
        assert_eq!(timeouts.clamp(ms(5000)), ms(800));
        assert_eq!(timeouts.back_off(ms(300)), ms(600));
        assert_eq!(timeouts.back_off(ms(600)), ms(800));
        assert_eq!(timeouts.back_off(Duration::MAX), ms(800));
    }

    #[test]
    fn packet_queue_and_stream_frames_derive_from_the_window() {
        let largest = TunnelLimits {
            payload: SegmentSize::of(1),
            min_window: bytes(64 * 1024),
            max_window: NonZeroU32::MAX,
            ..TunnelLimits::default()
        };
        assert_eq!(largest.stream_frames(), 2 * u32::MAX as usize);
        let smallest = TunnelLimits {
            min_window: bytes(16 * 1024 + 38),
            max_window: bytes(16 * 1024 + 38),
            initial_congestion: NonZeroU16::new(2).unwrap(),
            ..TunnelLimits::default()
        };
        assert!(smallest.validate().is_ok());
        assert_eq!(smallest.max_segments(), 2);
        assert_eq!(smallest.packet_queue().get(), 4 + PACKET_QUEUE_SLACK);
    }

    #[test]
    fn cross_field_relations_are_validated_by_field() {
        let larger = TunnelLimits {
            max_peers: count(8192),
            max_sessions: count(2048),
            sessions_per_peer: count(16),
            payload: SegmentSize::of(4096),
            min_window: bytes(64 * 1024),
            max_window: bytes(4 << 20),
            memory_budget: count(2048 * 2 * 64 * 1024),
            ..TunnelLimits::default()
        };
        assert!(larger.validate().is_ok());
        let rejected = [
            (
                TunnelLimits {
                    sessions_per_peer: count(2049),
                    ..larger.clone()
                },
                TunnelLimitsError::SessionsPerPeerAboveMaxSessions,
            ),
            (
                TunnelLimits {
                    min_window: bytes(4096 + 37),
                    ..larger.clone()
                },
                TunnelLimitsError::MinWindowBelowPayloadSegment,
            ),
            (
                TunnelLimits {
                    min_window: bytes((4 << 20) + 1),
                    ..larger.clone()
                },
                TunnelLimitsError::MinWindowAboveMaxWindow,
            ),
            (
                TunnelLimits {
                    memory_budget: count(2048 * 2 * 64 * 1024 - 1),
                    ..larger.clone()
                },
                TunnelLimitsError::MemoryBudgetBelowSessionFloors,
            ),
            (
                TunnelLimits {
                    initial_congestion: NonZeroU16::new(1025).unwrap(),
                    ..larger.clone()
                },
                TunnelLimitsError::InitialCongestionAboveMaxSegments,
            ),
            (
                TunnelLimits {
                    setup_timeout: Duration::ZERO,
                    ..larger.clone()
                },
                TunnelLimitsError::ZeroSetupTimeout,
            ),
            (
                TunnelLimits {
                    setup_timeout: Duration::MAX,
                    ..larger.clone()
                },
                TunnelLimitsError::UnrepresentableTimer {
                    field: "setup_timeout",
                },
            ),
            (
                TunnelLimits {
                    heartbeat_interval: Duration::ZERO,
                    ..larger.clone()
                },
                TunnelLimitsError::ZeroHeartbeatInterval,
            ),
            (
                TunnelLimits {
                    peer_timeout: larger.heartbeat_interval,
                    ..larger.clone()
                },
                TunnelLimitsError::PeerTimeoutNotAboveHeartbeat,
            ),
            (
                TunnelLimits {
                    peer_timeout: larger.retransmit.max(),
                    ..larger.clone()
                },
                TunnelLimitsError::PeerTimeoutNotAboveRetransmitMax,
            ),
        ];
        for (limits, expected) in rejected {
            assert_eq!(limits.validate(), Err(expected), "{limits:?}");
            let error = io::Error::from(expected);
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
            assert_eq!(
                error
                    .get_ref()
                    .and_then(|inner| inner.downcast_ref::<TunnelLimitsError>()),
                Some(&expected)
            );
        }
    }
}
