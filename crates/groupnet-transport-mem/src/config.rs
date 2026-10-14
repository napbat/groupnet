//! Finite queue budgets for the in-process fabrics.

use groupnet_transport::QueueCapacity;
#[cfg(feature = "bulk")]
use std::{fmt, io, num::NonZeroUsize};

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
    /// Returns [`MemBulkConfigError::PipeBufferTooLarge`].
    pub const fn validate(&self) -> Result<(), MemBulkConfigError> {
        if self.pipe_buffer.get() > isize::MAX.unsigned_abs() {
            return Err(MemBulkConfigError::PipeBufferTooLarge {
                pipe_buffer: self.pipe_buffer,
            });
        }
        Ok(())
    }
}

/// A violated [`MemBulkConfig`] bound, from [`MemBulkConfig::validate`].
#[cfg(feature = "bulk")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemBulkConfigError {
    /// `pipe_buffer` exceeds `isize::MAX` bytes of addressable storage.
    PipeBufferTooLarge {
        /// The configured pipe capacity.
        pipe_buffer: NonZeroUsize,
    },
}

#[cfg(feature = "bulk")]
impl fmt::Display for MemBulkConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PipeBufferTooLarge { pipe_buffer } => write!(
                f,
                "pipe_buffer {pipe_buffer} exceeds addressable storage (isize::MAX bytes)"
            ),
        }
    }
}

#[cfg(feature = "bulk")]
impl std::error::Error for MemBulkConfigError {}

#[cfg(feature = "bulk")]
impl From<MemBulkConfigError> for io::Error {
    fn from(error: MemBulkConfigError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}
