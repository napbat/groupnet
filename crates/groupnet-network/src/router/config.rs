//! Node routing and resource policy with field-specific validation.

use std::fmt;
use std::io;
use std::num::{NonZeroU16, NonZeroUsize};
use std::time::Duration;

use groupnet_transport::QueueCapacity;

use crate::wire;

/// Bounds for retained, generation-scoped fragment assemblies.
#[derive(Clone, Debug)]
pub struct ReassemblyConfig {
    /// Maximum simultaneous incomplete frames across all admitted links.
    pub max_pending: NonZeroUsize,
    /// Maximum fragments retained for one frame; the wire carries a `u16` count.
    pub max_fragments: NonZeroU16,
    /// Expiration of an incomplete frame; nonzero.
    pub timeout: Duration,
}

impl Default for ReassemblyConfig {
    fn default() -> Self {
        Self {
            max_pending: const { NonZeroUsize::new(64).unwrap() },
            max_fragments: const { NonZeroU16::new(1024).unwrap() },
            timeout: Duration::from_secs(3),
        }
    }
}

/// Routing and resource policy for one node.
///
/// Queue sizing: one saturated tunnel stream keeps about
/// [`TunnelLimits::stream_frames`](crate::tunnel::TunnelLimits::stream_frames)
/// frames queued (a window of data plus its acknowledgements; 2,048 with
/// default limits). Size `link_queue` for the bulk streams sharing a link; the
/// default holds two default-window streams. Arriving tunnel packets go straight
/// to their session's own bounded queue. A full queue drops frames, which
/// streams repair as loss.
#[derive(Clone, Debug)]
pub struct RouterConfig {
    /// Whether this node forwards transit traffic between admitted peers.
    /// Enabled by default; set to `false` for an endpoint-only node.
    pub forwarding: bool,
    /// Maximum learned destinations and configured neighbors per transport.
    pub max_routes: NonZeroUsize,
    /// Maximum simultaneous registered transport adapters.
    pub max_transports: NonZeroUsize,
    /// Expiration time for an unrefreshed route; longer than `announce_interval`.
    pub route_ttl: Duration,
    /// Interval between bounded path-vector announcements; nonzero.
    pub announce_interval: Duration,
    /// Largest complete routing envelope: at least the largest data header and
    /// at most the fragment `u32` total.
    pub max_frame: usize,
    /// Maximum path length and data hop budget, within `2..=255`.
    pub max_hops: usize,
    /// Retained incomplete-fragment bounds.
    pub reassembly: ReassemblyConfig,
    /// Maximum exclusive application namespaces; at most the 65,536 `u16` IDs.
    pub max_protocols: NonZeroUsize,
    /// Bounded inbox capacity for each application namespace.
    pub protocol_queue: QueueCapacity,
    /// Bounded router event queue capacity.
    pub event_queue: QueueCapacity,
    /// Bounded coordination inbox capacity.
    pub message_queue: QueueCapacity,
    /// Bounded outbound queue capacity for each link.
    pub link_queue: QueueCapacity,
    /// Recent routed identities retained for loop/replay suppression: each
    /// origin stream remembers its last 64 packet sequences and rejects older
    /// ones, and at most `replay_capacity / 64` (rounded up) streams are kept,
    /// least recently active evicted first.
    pub replay_capacity: NonZeroUsize,
    /// Physical-send deadline shared by all fragments of a frame; nonzero.
    pub send_timeout: Duration,
    /// Bytes of idle packet storage kept for reuse by later packets of the
    /// same size class; zero allocates every packet from the heap.
    pub packet_pool: usize,
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            forwarding: true,
            max_routes: const { NonZeroUsize::new(128).unwrap() },
            max_transports: const { NonZeroUsize::new(16).unwrap() },
            route_ttl: Duration::from_secs(6),
            announce_interval: Duration::from_secs(1),
            max_frame: wire::MAX_FRAME,
            max_hops: wire::MAX_HOPS,
            reassembly: ReassemblyConfig::default(),
            max_protocols: const { NonZeroUsize::new(32).unwrap() },
            protocol_queue: QueueCapacity::of(128),
            event_queue: QueueCapacity::of(256),
            message_queue: QueueCapacity::of(64),
            link_queue: QueueCapacity::of(2 * crate::tunnel::DEFAULT_STREAM_FRAMES),
            replay_capacity: const { NonZeroUsize::new(4096).unwrap() },
            send_timeout: Duration::from_secs(5),
            packet_pool: 2 << 20,
        }
    }
}

impl RouterConfig {
    /// Validates wire representability and timer relationships; counts and
    /// queue capacities carry their own nonzero range.
    ///
    /// # Errors
    /// Returns the first violated [`RouterConfigError`] rule.
    pub fn validate(&self) -> Result<(), RouterConfigError> {
        if self.max_frame < wire::MAX_DATA_HEADER
            || u32::try_from(self.max_frame).is_err()
            || self.max_frame > isize::MAX.unsigned_abs()
        {
            return Err(RouterConfigError::MaxFrameOutOfRange);
        }
        if !(2..=usize::from(u8::MAX)).contains(&self.max_hops) {
            return Err(RouterConfigError::MaxHopsOutOfRange);
        }
        if self.max_protocols.get() > usize::from(u16::MAX) + 1 {
            return Err(RouterConfigError::MaxProtocolsAboveIdSpace);
        }
        if self.reassembly.timeout.is_zero() {
            return Err(RouterConfigError::ZeroReassemblyTimeout);
        }
        if self.send_timeout.is_zero() {
            return Err(RouterConfigError::ZeroSendTimeout);
        }
        if self.announce_interval.is_zero() {
            return Err(RouterConfigError::ZeroAnnounceInterval);
        }
        if self.route_ttl <= self.announce_interval {
            return Err(RouterConfigError::RouteTtlNotAboveAnnounceInterval);
        }
        let now = tokio::time::Instant::now();
        for (field, timer) in [
            ("reassembly.timeout", self.reassembly.timeout),
            ("send_timeout", self.send_timeout),
            ("announce_interval", self.announce_interval),
            ("route_ttl", self.route_ttl),
        ] {
            if now.checked_add(timer).is_none() {
                return Err(RouterConfigError::UnrepresentableTimer { field });
            }
        }
        Ok(())
    }
}

/// A violated [`RouterConfig`] rule, naming the offending field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RouterConfigError {
    /// `max_frame` is below the largest data header or above the `u32`
    /// fragment total.
    MaxFrameOutOfRange,
    /// `max_hops` is outside `2..=255`.
    MaxHopsOutOfRange,
    /// `max_protocols` exceeds the 65,536 IDs of the `u16` namespace.
    MaxProtocolsAboveIdSpace,
    /// `reassembly.timeout` is zero.
    ZeroReassemblyTimeout,
    /// `send_timeout` is zero.
    ZeroSendTimeout,
    /// `announce_interval` is zero.
    ZeroAnnounceInterval,
    /// `route_ttl` does not exceed `announce_interval`.
    RouteTtlNotAboveAnnounceInterval,
    /// A timer cannot extend the current monotonic instant.
    UnrepresentableTimer {
        /// The offending field.
        field: &'static str,
    },
}

impl fmt::Display for RouterConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MaxFrameOutOfRange => f.write_str(
                "router max_frame must hold a data header and fit the u32 fragment total",
            ),
            Self::MaxHopsOutOfRange => f.write_str("router max_hops must be within 2..=255"),
            Self::MaxProtocolsAboveIdSpace => {
                f.write_str("router max_protocols exceeds the 65,536 u16 protocol IDs")
            }
            Self::ZeroReassemblyTimeout => f.write_str("router reassembly.timeout must be nonzero"),
            Self::ZeroSendTimeout => f.write_str("router send_timeout must be nonzero"),
            Self::ZeroAnnounceInterval => f.write_str("router announce_interval must be nonzero"),
            Self::RouteTtlNotAboveAnnounceInterval => {
                f.write_str("router route_ttl must exceed announce_interval")
            }
            Self::UnrepresentableTimer { field } => {
                write!(f, "router {field} exceeds the monotonic clock")
            }
        }
    }
}

impl std::error::Error for RouterConfigError {}

impl From<RouterConfigError> for io::Error {
    fn from(error: RouterConfigError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}
