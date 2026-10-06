//! Finite queue budgets for the in-process fabrics.

use std::io;

/// Operational limits for the in-process message network.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NetworkConfig {
    /// Messages buffered per endpoint before senders wait for receive capacity.
    pub inbound_queue: usize,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            inbound_queue: 1024,
        }
    }
}

impl NetworkConfig {
    /// Checks that the queue capacity is nonzero and supported by Tokio.
    ///
    /// # Errors
    /// Returns `InvalidInput` for zero or unsupported capacities.
    pub fn validate(&self) -> io::Result<()> {
        validate_queue(self.inbound_queue)
    }
}

pub(crate) fn validate_queue(capacity: usize) -> io::Result<()> {
    if capacity == 0 || capacity > tokio::sync::Semaphore::MAX_PERMITS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "queue capacity must be nonzero and fit Tokio's semaphore limit",
        ));
    }
    Ok(())
}

/// Operational limits for the in-process bulk network.
#[cfg(feature = "bulk")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemBulkConfig {
    /// Connections buffered per endpoint before connectors wait for acceptance.
    pub accept_queue: usize,
    /// Bytes buffered per direction of each pipe before writers wait for readers.
    pub pipe_buffer: usize,
}

#[cfg(feature = "bulk")]
impl Default for MemBulkConfig {
    fn default() -> Self {
        Self {
            accept_queue: 64,
            pipe_buffer: 64 * 1024,
        }
    }
}

#[cfg(feature = "bulk")]
impl MemBulkConfig {
    /// Checks the accept queue and finite, addressable pipe capacity.
    ///
    /// # Errors
    /// Returns `InvalidInput` for zero or unsupported capacities.
    pub fn validate(&self) -> io::Result<()> {
        validate_queue(self.accept_queue)?;
        if self.pipe_buffer == 0 || self.pipe_buffer > isize::MAX.unsigned_abs() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "pipe buffer must be nonzero and fit addressable storage",
            ));
        }
        Ok(())
    }
}
