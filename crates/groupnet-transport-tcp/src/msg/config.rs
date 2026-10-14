//! Validated tuning for the TCP message transport.

use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::time::Duration;
use std::{error, fmt, io};

use groupnet_transport::QueueCapacity;
use groupnet_transport::framing::MAX_FRAME_BYTES;

/// Default allocation guard for an inbound payload; deployments may tune it
/// up to the shared stream frame ceiling.
const DEFAULT_MAX_FRAME: usize = 16 * 1024 * 1024;

/// Tuning for [`TcpMsgTransport`](crate::TcpMsgTransport) and the managed
/// `TcpLink` (feature `link`). The default values suit a gossip control plane.
/// Capacities are valid by type; the remaining bounds are checked by
/// [`validate`](Self::validate), which every binding calls before opening a
/// socket.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TcpMsgConfig {
    /// Close an outbound connection after this long without a frame to send.
    /// The read side of an inbound connection allows twice this before
    /// presuming the peer gone, so a clean close normally comes from the
    /// sender and the reader timeout only reaps half-open sockets left by a
    /// peer that died without a FIN. Must be nonzero. Default: 30s.
    pub idle_timeout: Duration,
    /// Most outbound connections pooled at once; dialing past the cap closes
    /// the oldest. Size it to cover the gossip/anti-entropy fanout — the pool
    /// follows who this node is currently talking to, not the cluster.
    /// Default: 64.
    pub max_outbound: NonZeroUsize,
    /// Frames buffered per outbound connection while it dials or drains; a
    /// full queue drops the frame (best-effort). Default: 256.
    pub outbound_queue: QueueCapacity,
    /// Frames buffered between socket readers and the consumer. Readers wait
    /// when full, propagating TCP backpressure. Default: 1024.
    pub inbound_queue: QueueCapacity,
    /// Maximum payload bytes per frame, checked before inbound allocation and
    /// outbound queueing. Must be nonzero and at most the shared stream ceiling
    /// [`MAX_FRAME_BYTES`]. Default: 16 MiB.
    pub max_frame_bytes: usize,
    /// The listener address to introduce ourselves with when dialing, so the
    /// accepting side can dial back without prior registration. `None`
    /// introduces the bound address unless it is unspecified (`0.0.0.0`);
    /// set it when peers must reach this node somewhere else (NAT, container
    /// networking). Default: `None`.
    pub advertise: Option<SocketAddr>,
}

impl Default for TcpMsgConfig {
    fn default() -> Self {
        Self {
            idle_timeout: Duration::from_secs(30),
            max_outbound: NonZeroUsize::new(64).expect("nonzero literal"),
            outbound_queue: QueueCapacity::of(256),
            inbound_queue: QueueCapacity::of(1024),
            max_frame_bytes: DEFAULT_MAX_FRAME,
            advertise: None,
        }
    }
}

impl TcpMsgConfig {
    /// Checks the bounds the field types cannot express.
    ///
    /// # Errors
    /// Returns the [`TcpMsgConfigError`] naming the first violated field.
    pub const fn validate(&self) -> Result<(), TcpMsgConfigError> {
        if self.idle_timeout.is_zero() {
            return Err(TcpMsgConfigError::ZeroIdleTimeout);
        }
        if self.max_frame_bytes == 0 {
            return Err(TcpMsgConfigError::ZeroMaxFrameBytes);
        }
        if self.max_frame_bytes > MAX_FRAME_BYTES {
            return Err(TcpMsgConfigError::MaxFrameBytesAboveCeiling {
                max_frame_bytes: self.max_frame_bytes,
            });
        }
        Ok(())
    }
}

/// A [`TcpMsgConfig`] bound violated, from [`TcpMsgConfig::validate`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TcpMsgConfigError {
    /// `idle_timeout` is zero.
    ZeroIdleTimeout,
    /// `max_frame_bytes` is zero.
    ZeroMaxFrameBytes,
    /// `max_frame_bytes` exceeds the shared stream frame ceiling.
    MaxFrameBytesAboveCeiling {
        /// The configured limit.
        max_frame_bytes: usize,
    },
}

impl fmt::Display for TcpMsgConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroIdleTimeout => f.write_str("idle_timeout must be nonzero"),
            Self::ZeroMaxFrameBytes => f.write_str("max_frame_bytes must be nonzero"),
            Self::MaxFrameBytesAboveCeiling { max_frame_bytes } => write!(
                f,
                "max_frame_bytes {max_frame_bytes} exceeds the {MAX_FRAME_BYTES}-byte stream ceiling"
            ),
        }
    }
}

impl error::Error for TcpMsgConfigError {}

impl From<TcpMsgConfigError> for io::Error {
    fn from(error: TcpMsgConfigError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}
