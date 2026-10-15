//! Validated client and server limits: frame bounds, queue capacities,
//! destination/connection admission and idle eviction timers.

mod error;

use std::num::NonZeroUsize;
use std::time::Duration;

use groupnet_transport::QueueCapacity;
use groupnet_transport::framing::MAX_FRAME_BYTES;

use crate::codec::REQUEST_HEAD;

pub use error::{FrameLimitError, RpcConfigError, RpcServerConfigError};

/// The longest timer the RPC layer represents. A request's deadline travels
/// as `u32` milliseconds (~49.7 days): a call's timeout saturates to it (the
/// one permitted correction — the difference is unobservable), and every
/// configured timer must lie within it, so deadlines computed from them never
/// overflow the monotonic clock.
pub const MAX_TIMEOUT: Duration = Duration::from_millis(u32::MAX as u64);

/// A validated bound on one whole RPC frame, head included: at least the
/// longest RPC head, at most the data plane's
/// [`MAX_FRAME_BYTES`](groupnet_transport::framing::MAX_FRAME_BYTES).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FrameLimit(NonZeroUsize);

impl FrameLimit {
    /// The smallest limit: a request head with an empty payload.
    pub const MIN: Self = Self::of(REQUEST_HEAD);

    /// The data plane's frame ceiling.
    pub const MAX: Self = Self::of(MAX_FRAME_BYTES);

    /// The default on both sides: 16 MiB.
    pub const DEFAULT: Self = Self::of(16 << 20);

    /// One of the nonzero constants above.
    const fn of(bytes: usize) -> Self {
        Self(NonZeroUsize::new(bytes).expect("RPC frame limit constants are nonzero"))
    }

    /// Validates a limit of `bytes`.
    ///
    /// # Errors
    /// [`FrameLimitError::BelowMin`] under [`MIN`](Self::MIN),
    /// [`FrameLimitError::AboveMax`] over [`MAX`](Self::MAX).
    pub const fn new(bytes: usize) -> Result<Self, FrameLimitError> {
        match NonZeroUsize::new(bytes) {
            Some(limit) if bytes >= Self::MIN.get() && bytes <= Self::MAX.get() => Ok(Self(limit)),
            _ if bytes > Self::MAX.get() => Err(FrameLimitError::AboveMax(bytes)),
            _ => Err(FrameLimitError::BelowMin(bytes)),
        }
    }

    /// The limit in bytes.
    #[must_use]
    pub const fn get(self) -> usize {
        self.0.get()
    }

    /// The limit as the data plane's bounded send/receive argument.
    pub(crate) const fn frame_bytes(self) -> NonZeroUsize {
        self.0
    }

    /// Body bytes left under this limit after an RPC head of `head` bytes;
    /// every RPC head fits [`MIN`](Self::MIN).
    pub(crate) const fn body(self, head: usize) -> usize {
        self.get() - head
    }
}

impl Default for FrameLimit {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl TryFrom<usize> for FrameLimit {
    type Error = FrameLimitError;

    /// [`FrameLimit::new`] as a conversion.
    ///
    /// # Errors
    /// As [`FrameLimit::new`].
    fn try_from(bytes: usize) -> Result<Self, FrameLimitError> {
        Self::new(bytes)
    }
}

/// Client limits, validated once by
/// [`RpcClient::with_config`](crate::RpcClient::with_config).
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
    /// The first violated rule, naming its field: a zero timer or one above
    /// [`MAX_TIMEOUT`].
    pub fn validate(&self) -> Result<(), RpcConfigError> {
        if self.connect_timeout.is_zero() {
            return Err(RpcConfigError::ZeroConnectTimeout);
        }
        if self.connect_timeout > MAX_TIMEOUT {
            return Err(RpcConfigError::ConnectTimeoutAboveMax(self.connect_timeout));
        }
        if self.idle_timeout.is_zero() {
            return Err(RpcConfigError::ZeroIdleTimeout);
        }
        if self.idle_timeout > MAX_TIMEOUT {
            return Err(RpcConfigError::IdleTimeoutAboveMax(self.idle_timeout));
        }
        Ok(())
    }
}

/// Server limits, validated once by
/// [`RpcServer::with_config`](crate::RpcServer::with_config) (or
/// [`RpcServer::bind_with`](crate::RpcServer::bind_with), which builds on it).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RpcServerConfig {
    /// The largest RPC frame (head included) this server accepts or sends.
    /// Match the clients' [`RpcConfig::max_frame_bytes`].
    pub max_frame_bytes: FrameLimit,
    /// Handlers run concurrently for one connection; at the limit the server
    /// stops reading that connection until one finishes.
    pub max_concurrent_per_connection: QueueCapacity,
    /// Connections served at once across all peers. At the limit
    /// [`RpcServer::serve`](crate::RpcServer::serve) refuses a connection
    /// ([`RpcServeError::AtCapacity`](crate::RpcServeError::AtCapacity)) and
    /// [`RpcServer::reserve`](crate::RpcServer::reserve) waits until one
    /// ends (idle eviction ends unused ones).
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
    /// [`RpcServerConfigError::ZeroIdleTimeout`] or
    /// [`RpcServerConfigError::IdleTimeoutAboveMax`].
    pub fn validate(&self) -> Result<(), RpcServerConfigError> {
        if self.idle_timeout.is_zero() {
            return Err(RpcServerConfigError::ZeroIdleTimeout);
        }
        if self.idle_timeout > MAX_TIMEOUT {
            return Err(RpcServerConfigError::IdleTimeoutAboveMax(self.idle_timeout));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_limits_span_the_longest_head_to_the_data_plane_ceiling() {
        assert_eq!(
            FrameLimit::new(REQUEST_HEAD - 1),
            Err(FrameLimitError::BelowMin(REQUEST_HEAD - 1))
        );
        assert_eq!(
            FrameLimit::new(MAX_FRAME_BYTES + 1),
            Err(FrameLimitError::AboveMax(MAX_FRAME_BYTES + 1))
        );
        assert_eq!(FrameLimit::new(REQUEST_HEAD), Ok(FrameLimit::MIN));
        assert_eq!(FrameLimit::new(MAX_FRAME_BYTES), Ok(FrameLimit::MAX));
        assert_eq!(FrameLimit::default().get(), 16 << 20);
        assert_eq!(FrameLimit::MIN.body(REQUEST_HEAD), 0);
        assert_eq!(FrameLimit::try_from(0), Err(FrameLimitError::BelowMin(0)));
    }

    #[test]
    fn each_client_timer_rule_is_rejected_by_name() {
        RpcConfig::default().validate().unwrap();
        let over = MAX_TIMEOUT + Duration::from_nanos(1);
        let cases = [
            (
                RpcConfig {
                    connect_timeout: Duration::ZERO,
                    ..RpcConfig::default()
                },
                RpcConfigError::ZeroConnectTimeout,
            ),
            (
                RpcConfig {
                    connect_timeout: over,
                    ..RpcConfig::default()
                },
                RpcConfigError::ConnectTimeoutAboveMax(over),
            ),
            (
                RpcConfig {
                    idle_timeout: Duration::ZERO,
                    ..RpcConfig::default()
                },
                RpcConfigError::ZeroIdleTimeout,
            ),
            (
                RpcConfig {
                    idle_timeout: over,
                    ..RpcConfig::default()
                },
                RpcConfigError::IdleTimeoutAboveMax(over),
            ),
        ];
        for (config, expected) in cases {
            assert_eq!(config.validate(), Err(expected));
        }
        RpcConfig {
            connect_timeout: MAX_TIMEOUT,
            idle_timeout: MAX_TIMEOUT,
            ..RpcConfig::default()
        }
        .validate()
        .unwrap();
    }

    #[test]
    fn each_server_timer_rule_is_rejected_by_name() {
        RpcServerConfig::default().validate().unwrap();
        let over = MAX_TIMEOUT + Duration::from_nanos(1);
        for (idle_timeout, expected) in [
            (Duration::ZERO, RpcServerConfigError::ZeroIdleTimeout),
            (over, RpcServerConfigError::IdleTimeoutAboveMax(over)),
        ] {
            let config = RpcServerConfig {
                idle_timeout,
                ..RpcServerConfig::default()
            };
            assert_eq!(config.validate(), Err(expected));
        }
        RpcServerConfig {
            idle_timeout: MAX_TIMEOUT,
            ..RpcServerConfig::default()
        }
        .validate()
        .unwrap();
    }
}
