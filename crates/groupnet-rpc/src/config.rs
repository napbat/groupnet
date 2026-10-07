//! Validated client and server limits: frame bounds, queue capacities,
//! destination/connection admission and idle eviction timers.

use std::io;
use std::time::Duration;

use groupnet_transport::QueueCapacity;
use groupnet_transport::framing::MAX_FRAME_BYTES;

use crate::codec::REQUEST_HEAD;

/// The longest timer the RPC layer represents. A request's deadline travels
/// as `u32` milliseconds (~49.7 days): a call's timeout is clamped to it, and
/// every configured timer must lie within it, so deadlines computed from
/// them never overflow the monotonic clock.
pub const MAX_TIMEOUT: Duration = Duration::from_millis(u32::MAX as u64);

/// A validated bound on one whole RPC frame, head included: at least the
/// longest RPC head, at most the data plane's
/// [`MAX_FRAME_BYTES`](groupnet_transport::framing::MAX_FRAME_BYTES).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FrameLimit(usize);

impl FrameLimit {
    /// The smallest limit: a request head with an empty payload.
    pub const MIN: Self = Self(REQUEST_HEAD);
    /// The data plane's frame ceiling.
    pub const MAX: Self = Self(MAX_FRAME_BYTES);
    /// The default on both sides: 16 MiB.
    pub const DEFAULT: Self = Self(16 << 20);

    /// Validates a limit, returning `None` outside
    /// [`MIN`](Self::MIN)`..=`[`MAX`](Self::MAX).
    #[must_use]
    pub const fn new(bytes: usize) -> Option<Self> {
        if bytes < Self::MIN.0 || bytes > Self::MAX.0 {
            None
        } else {
            Some(Self(bytes))
        }
    }

    /// The limit in bytes.
    #[must_use]
    pub const fn get(self) -> usize {
        self.0
    }

    /// Body bytes left under this limit after an RPC head of `head` bytes;
    /// every RPC head fits [`MIN`](Self::MIN).
    pub(crate) const fn body(self, head: usize) -> usize {
        self.0 - head
    }
}

impl Default for FrameLimit {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl TryFrom<usize> for FrameLimit {
    type Error = io::Error;

    /// # Errors
    /// Returns `InvalidInput` outside [`FrameLimit::MIN`]`..=`[`FrameLimit::MAX`].
    fn try_from(bytes: usize) -> io::Result<Self> {
        Self::new(bytes).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "rpc frame limit must be within {}..={}, got {bytes}",
                    Self::MIN.0,
                    Self::MAX.0
                ),
            )
        })
    }
}

/// Client limits, validated once by [`RpcClient::new`](crate::RpcClient::new).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RpcConfig {
    /// How long opening a connection to a peer may take before the call
    /// fails [`Unreachable`](crate::RpcError::Unreachable). A call's own
    /// timeout still bounds it (and then fails
    /// [`Timeout`](crate::RpcError::Timeout)). Nonzero, at most
    /// [`MAX_TIMEOUT`].
    pub connect_timeout: Duration,
    /// How long a connection with no call waiting on it stays open before
    /// the client closes it; the next call reconnects. Nonzero, at most
    /// [`MAX_TIMEOUT`]. Keep it below the servers'
    /// [`RpcServerConfig::idle_timeout`] so the client, which knows no call
    /// is in flight, closes first.
    pub idle_timeout: Duration,
    /// The largest RPC frame (head included) this client sends or accepts.
    /// Match the server's [`RpcServerConfig::max_frame_bytes`].
    pub max_frame_bytes: FrameLimit,
    /// Concurrent calls allowed to one peer; more wait for a slot within
    /// their own timeout. Also the connection's request queue capacity.
    pub max_in_flight_per_peer: QueueCapacity,
    /// Destinations the client tracks at once. A call to a new destination
    /// at the limit evicts one no call is using (its connection closes); if
    /// every destination has a call in progress it fails
    /// [`Saturated`](crate::RpcError::Saturated) without being sent.
    pub max_peers: QueueCapacity,
}

impl Default for RpcConfig {
    /// 3 s to connect, 60 s idle, 16 MiB frames, 1024 calls in flight per
    /// peer, 1024 destinations.
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(3),
            idle_timeout: Duration::from_secs(60),
            max_frame_bytes: FrameLimit::DEFAULT,
            max_in_flight_per_peer: QueueCapacity::of(1024),
            max_peers: QueueCapacity::of(1024),
        }
    }
}

impl RpcConfig {
    /// Checks the timers; frame and queue bounds are valid by construction.
    ///
    /// # Errors
    /// `InvalidInput` for a zero timer or one above [`MAX_TIMEOUT`].
    pub fn validate(&self) -> io::Result<()> {
        timer("connect_timeout", self.connect_timeout)?;
        timer("idle_timeout", self.idle_timeout)
    }
}

/// Server limits, validated once by
/// [`RpcServer::spawn_with`](crate::RpcServer::spawn_with).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RpcServerConfig {
    /// The largest RPC frame (head included) this server accepts or sends.
    /// Match the clients' [`RpcConfig::max_frame_bytes`].
    pub max_frame_bytes: FrameLimit,
    /// Handlers run concurrently for one connection; at the limit the server
    /// stops reading that connection until one finishes.
    pub max_concurrent_per_connection: QueueCapacity,
    /// Connections served at once across all peers; at the limit the server
    /// stops accepting until one ends (idle eviction ends unused ones).
    pub max_connections: QueueCapacity,
    /// How long a connection with no request read and no handler running
    /// stays open; answers already produced are written before it closes.
    /// Nonzero, at most [`MAX_TIMEOUT`]. Keep it above the clients'
    /// [`RpcConfig::idle_timeout`].
    pub idle_timeout: Duration,
}

impl Default for RpcServerConfig {
    /// 16 MiB frames, 64 concurrent handlers per connection, 1024
    /// connections, 120 s idle.
    fn default() -> Self {
        Self {
            max_frame_bytes: FrameLimit::DEFAULT,
            max_concurrent_per_connection: QueueCapacity::of(64),
            max_connections: QueueCapacity::of(1024),
            idle_timeout: Duration::from_secs(120),
        }
    }
}

impl RpcServerConfig {
    /// Checks the idle timer; frame and queue bounds are valid by
    /// construction.
    ///
    /// # Errors
    /// `InvalidInput` for a zero idle timeout or one above [`MAX_TIMEOUT`].
    pub fn validate(&self) -> io::Result<()> {
        timer("idle_timeout", self.idle_timeout)
    }
}

/// A configured timer must be nonzero and representable as a wire deadline.
fn timer(name: &str, value: Duration) -> io::Result<()> {
    if value.is_zero() || value > MAX_TIMEOUT {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("rpc {name} must be within (0, {MAX_TIMEOUT:?}], got {value:?}"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_limits_span_the_longest_head_to_the_data_plane_ceiling() {
        assert_eq!(FrameLimit::new(REQUEST_HEAD - 1), None);
        assert_eq!(FrameLimit::new(MAX_FRAME_BYTES + 1), None);
        assert_eq!(FrameLimit::new(REQUEST_HEAD), Some(FrameLimit::MIN));
        assert_eq!(FrameLimit::new(MAX_FRAME_BYTES), Some(FrameLimit::MAX));
        assert_eq!(FrameLimit::default().get(), 16 << 20);
        assert_eq!(FrameLimit::MIN.body(REQUEST_HEAD), 0);
        assert_eq!(
            FrameLimit::try_from(0).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn timers_must_be_nonzero_and_wire_representable() {
        RpcConfig::default().validate().unwrap();
        RpcServerConfig::default().validate().unwrap();
        for bad in [Duration::ZERO, MAX_TIMEOUT + Duration::from_nanos(1)] {
            for config in [
                RpcConfig {
                    connect_timeout: bad,
                    ..RpcConfig::default()
                },
                RpcConfig {
                    idle_timeout: bad,
                    ..RpcConfig::default()
                },
            ] {
                assert_eq!(
                    config.validate().unwrap_err().kind(),
                    io::ErrorKind::InvalidInput
                );
            }
            let server = RpcServerConfig {
                idle_timeout: bad,
                ..RpcServerConfig::default()
            };
            assert!(server.validate().is_err());
        }
        RpcConfig {
            connect_timeout: MAX_TIMEOUT,
            idle_timeout: MAX_TIMEOUT,
            ..RpcConfig::default()
        }
        .validate()
        .unwrap();
    }
}
