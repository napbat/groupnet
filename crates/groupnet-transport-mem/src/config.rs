//! Finite queue budgets for the in-process fabrics.

use groupnet_transport::QueueCapacity;
#[cfg(feature = "bulk")]
use std::{io, num::NonZeroUsize};

/// Operational limits for the in-process message network.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NetworkConfig {
    /// Messages buffered per endpoint before senders wait for receive capacity.
    pub inbound_queue: QueueCapacity,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            inbound_queue: QueueCapacity::of(1024),
        }
    }
}

/// Operational limits for the in-process bulk network.
#[cfg(feature = "bulk")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemBulkConfig {
    /// Connections buffered per endpoint before connectors wait for acceptance.
    pub accept_queue: QueueCapacity,
    /// Bytes buffered per direction of each pipe before writers wait for
    /// readers; at most `isize::MAX` (addressable storage).
    pub pipe_buffer: NonZeroUsize,
}

#[cfg(feature = "bulk")]
impl Default for MemBulkConfig {
    fn default() -> Self {
        Self {
            accept_queue: QueueCapacity::of(64),
            pipe_buffer: NonZeroUsize::new(64 * 1024).expect("nonzero default"),
        }
    }
}

#[cfg(feature = "bulk")]
impl MemBulkConfig {
    /// Checks that the pipe capacity fits addressable storage; the accept
    /// queue is valid by construction.
    ///
    /// # Errors
    /// Returns `InvalidInput` for a pipe buffer above `isize::MAX`.
    pub fn validate(&self) -> io::Result<()> {
        if self.pipe_buffer.get() > isize::MAX.unsigned_abs() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "pipe buffer must fit addressable storage",
            ));
        }
        Ok(())
    }
}
