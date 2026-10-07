//! Local resource budgets and deadlines, independent of immutable framing bounds.

use std::io;
use std::num::NonZeroUsize;
use std::time::Duration;

use groupnet_transport::QueueCapacity;

/// Finite operational limits for a native IPC listener.
///
/// These limits never change the protocol's frame or node-ID bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IpcConfig {
    /// Registered peer addresses allowed in the local address book.
    pub max_peers: NonZeroUsize,
    /// Concurrent connections, including inbound and outbound setup.
    /// Windows allows at most 252, keeping its native instance count finite.
    pub max_sessions: QueueCapacity,
    /// Pending outbound messages per connection.
    pub session_queue: QueueCapacity,
    /// Pending inbound messages shared by all connections.
    pub inbound_queue: QueueCapacity,
    /// Deadline for connecting and exchanging node-ID introductions; a
    /// session not established in time is dropped.
    pub setup_timeout: Duration,
    /// Deadline for writing one frame; a peer that stops reading for longer
    /// ends its session.
    pub write_timeout: Duration,
    /// Longest silence tolerated between inbound frames before the session
    /// is treated as dead.
    pub read_timeout: Duration,
}

impl Default for IpcConfig {
    fn default() -> Self {
        Self {
            max_peers: NonZeroUsize::new(128).expect("nonzero default"),
            max_sessions: QueueCapacity::of(64),
            session_queue: QueueCapacity::of(16),
            inbound_queue: QueueCapacity::of(64),
            setup_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(60),
        }
    }
}

/// Windows reserves 255 as its "unlimited instances" sentinel; with two
/// listener instances held back, 252 sessions is the finite maximum.
#[cfg(windows)]
const MAX_WINDOWS_SESSIONS: usize = 252;

impl IpcConfig {
    /// Checks the platform session limit and nonzero deadlines before any
    /// listener is bound; capacities are valid by construction.
    ///
    /// # Errors
    /// Returns `InvalidInput` for a zero deadline or, on Windows, more than
    /// 252 sessions.
    pub fn validate(&self) -> io::Result<()> {
        if [self.setup_timeout, self.write_timeout, self.read_timeout]
            .iter()
            .any(Duration::is_zero)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "IPC deadlines must be nonzero",
            ));
        }
        #[cfg(windows)]
        if self.max_sessions.get() > MAX_WINDOWS_SESSIONS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Windows IPC supports at most 252 concurrent sessions",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_each_zero_deadline() {
        assert!(IpcConfig::default().validate().is_ok());
        for field in 0..3 {
            let mut config = IpcConfig::default();
            match field {
                0 => config.setup_timeout = Duration::ZERO,
                1 => config.write_timeout = Duration::ZERO,
                _ => config.read_timeout = Duration::ZERO,
            }
            assert_eq!(
                config.validate().unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
        }
        let longest = IpcConfig {
            setup_timeout: Duration::MAX,
            write_timeout: Duration::MAX,
            read_timeout: Duration::MAX,
            ..IpcConfig::default()
        };
        assert!(longest.validate().is_ok());
    }

    #[cfg(windows)]
    #[test]
    fn windows_session_limit_never_uses_unlimited_instance_sentinel() {
        assert!(
            IpcConfig {
                max_sessions: QueueCapacity::of(252),
                ..IpcConfig::default()
            }
            .validate()
            .is_ok()
        );
        assert!(
            IpcConfig {
                max_sessions: QueueCapacity::of(253),
                ..IpcConfig::default()
            }
            .validate()
            .is_err()
        );
    }
}
