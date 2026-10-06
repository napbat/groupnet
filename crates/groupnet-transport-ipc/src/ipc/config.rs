//! Local resource budgets, independent of immutable framing bounds.

use std::io;

/// Finite operational limits for a native IPC listener.
///
/// These limits never change the protocol's frame or node-ID bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IpcConfig {
    /// Registered peer addresses allowed in the local address book.
    pub max_peers: usize,
    /// Concurrent connections, including inbound and outbound setup.
    /// Windows allows at most 252, keeping its native instance count finite.
    pub max_sessions: usize,
    /// Pending outbound messages per connection.
    pub session_queue: usize,
    /// Pending inbound messages shared by all connections.
    pub inbound_queue: usize,
}

impl Default for IpcConfig {
    fn default() -> Self {
        Self {
            max_peers: 128,
            max_sessions: 64,
            session_queue: 16,
            inbound_queue: 64,
        }
    }
}

impl IpcConfig {
    /// Checks every operational budget before any listener is bound.
    ///
    /// # Errors
    /// Returns `InvalidInput` for zero or capacities unsupported by Tokio.
    pub fn validate(&self) -> io::Result<()> {
        for limit in [
            self.max_peers,
            self.max_sessions,
            self.session_queue,
            self.inbound_queue,
        ] {
            if limit == 0 || limit > tokio::sync::Semaphore::MAX_PERMITS {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "IPC limits must be nonzero and fit Tokio's semaphore limit",
                ));
            }
        }
        #[cfg(windows)]
        if self.max_sessions > 252 {
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
    fn rejects_each_zero_or_unsupported_budget() {
        assert!(IpcConfig::default().validate().is_ok());
        for value in [0, usize::MAX] {
            for field in 0..4 {
                let mut config = IpcConfig::default();
                match field {
                    0 => config.max_peers = value,
                    1 => config.max_sessions = value,
                    2 => config.session_queue = value,
                    _ => config.inbound_queue = value,
                }
                assert_eq!(
                    config.validate().unwrap_err().kind(),
                    io::ErrorKind::InvalidInput
                );
            }
        }
    }

    #[cfg(windows)]
    #[test]
    fn windows_session_limit_never_uses_unlimited_instance_sentinel() {
        assert!(
            IpcConfig {
                max_sessions: 252,
                ..IpcConfig::default()
            }
            .validate()
            .is_ok()
        );
        assert!(
            IpcConfig {
                max_sessions: 253,
                ..IpcConfig::default()
            }
            .validate()
            .is_err()
        );
    }
}
