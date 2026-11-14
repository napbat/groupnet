//! The hub key, admission policy, credential and accept-side bounds of a TLS
//! [`TcpBulkTransport`](crate::TcpBulkTransport).

use std::sync::Arc;
use std::time::Duration;
use std::{error, fmt, io};

use groupnet_transport::QueueCapacity;
use groupnet_transport::admission::{Admission, MAX_CREDENTIAL_BYTES};

use super::TcpTlsKey;
use crate::bulk::HANDSHAKE_TIMEOUT;

/// The settings of a TLS hub, from `TcpBulkTransport::bind_tls`: its
/// long-lived key, the policy that admits its clients, the credential it
/// presents when it dials, and its bounds.
///
/// Build it with [`new`](Self::new), which takes the two security inputs,
/// and refine the bounds with struct-update syntax. There is no default
/// policy: use `OpenAdmission` deliberately to admit every client that
/// completes TLS.
#[derive(Clone)]
pub struct TcpTlsConfig {
    /// The hub's long-lived key. Its public key is what clients pin.
    pub key: TcpTlsKey,
    /// Judges every inbound identity claim, its credential and its remote
    /// socket address; a refusal closes the connection. Never printed by
    /// `Debug`.
    pub admission: Arc<dyn Admission>,
    /// Opaque application bytes this endpoint presents to the policies of the
    /// endpoints it dials, at most [`MAX_CREDENTIAL_BYTES`]. Never printed by
    /// `Debug`. Default: empty.
    pub credential: Vec<u8>,
    /// Deadline for one handshake to complete (TLS and admission, the policy
    /// included), both on accepted connections and on connections this
    /// endpoint dials (a dialed handshake that misses it fails with
    /// `TimedOut`). Must be nonzero. Default: 5s.
    pub handshake_timeout: Duration,
    /// Accepted connections held at once between `accept(2)` and
    /// `accept`: handshaking or waiting in the queue. At the bound, new
    /// connections wait in the kernel backlog. Default: 64.
    pub max_handshakes: QueueCapacity,
    /// Admitted connections buffered until `accept` takes them. Default: 64.
    pub accept_queue: QueueCapacity,
    /// Accepted connections alive at once, from `accept(2)` until the stream
    /// is dropped. At the bound the listener accepts nothing until a
    /// connection ends. Default: 1024.
    pub max_connections: QueueCapacity,
}

impl TcpTlsConfig {
    /// A hub identified by `key` that admits through `admission`, presents no
    /// credential, and keeps the default bounds.
    #[must_use]
    pub fn new(key: TcpTlsKey, admission: Arc<dyn Admission>) -> Self {
        Self {
            key,
            admission,
            credential: Vec::new(),
            handshake_timeout: HANDSHAKE_TIMEOUT,
            max_handshakes: QueueCapacity::of(64),
            accept_queue: QueueCapacity::of(64),
            max_connections: QueueCapacity::of(1024),
        }
    }

    /// Checks the rules the types cannot express, before any socket exists.
    ///
    /// # Errors
    /// Returns the [`TcpTlsConfigError`] naming the first violated field.
    pub const fn validate(&self) -> Result<(), TcpTlsConfigError> {
        check_dial(self.handshake_timeout, self.credential.len())
    }
}

impl fmt::Debug for TcpTlsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Policies can hold secrets too: neither policy nor credential is printed.
        f.debug_struct("TcpTlsConfig")
            .field("key", &self.key)
            .field("handshake_timeout", &self.handshake_timeout)
            .field("max_handshakes", &self.max_handshakes)
            .field("accept_queue", &self.accept_queue)
            .field("max_connections", &self.max_connections)
            .finish_non_exhaustive()
    }
}

/// The settings of a TLS client, from `TcpBulkTransport::dial_only_tls`.
/// It has no key: the client makes a fresh one for every connection.
#[derive(Clone)]
pub struct TcpTlsDialConfig {
    /// Opaque application bytes presented to the policies of the hubs this
    /// endpoint dials, at most [`MAX_CREDENTIAL_BYTES`]. Never printed by
    /// `Debug`. Default: empty.
    pub credential: Vec<u8>,
    /// Deadline for one dialed handshake (TLS and admission); a dial that
    /// misses it fails with `TimedOut`. Must be nonzero. Default: 5s.
    pub handshake_timeout: Duration,
}

impl Default for TcpTlsDialConfig {
    fn default() -> Self {
        Self {
            credential: Vec::new(),
            handshake_timeout: HANDSHAKE_TIMEOUT,
        }
    }
}

impl TcpTlsDialConfig {
    /// Checks the deadline and the credential size.
    ///
    /// # Errors
    /// Returns [`TcpTlsConfigError::ZeroHandshakeTimeout`] or
    /// [`TcpTlsConfigError::CredentialTooLong`].
    pub const fn validate(&self) -> Result<(), TcpTlsConfigError> {
        check_dial(self.handshake_timeout, self.credential.len())
    }
}

impl fmt::Debug for TcpTlsDialConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TcpTlsDialConfig")
            .field("handshake_timeout", &self.handshake_timeout)
            .finish_non_exhaustive()
    }
}

/// The rules both configurations share.
const fn check_dial(
    handshake_timeout: Duration,
    credential_len: usize,
) -> Result<(), TcpTlsConfigError> {
    if handshake_timeout.is_zero() {
        return Err(TcpTlsConfigError::ZeroHandshakeTimeout);
    }
    if credential_len > MAX_CREDENTIAL_BYTES {
        return Err(TcpTlsConfigError::CredentialTooLong {
            len: credential_len,
        });
    }
    Ok(())
}

/// A violated [`TcpTlsConfig`] or [`TcpTlsDialConfig`] rule, from their
/// `validate`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TcpTlsConfigError {
    /// `handshake_timeout` is zero.
    ZeroHandshakeTimeout,
    /// `credential` exceeds [`MAX_CREDENTIAL_BYTES`].
    CredentialTooLong {
        /// The credential's length in bytes.
        len: usize,
    },
}

impl fmt::Display for TcpTlsConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroHandshakeTimeout => f.write_str("handshake_timeout must be nonzero"),
            Self::CredentialTooLong { len } => write!(
                f,
                "credential is {len} bytes, above the {MAX_CREDENTIAL_BYTES}-byte limit"
            ),
        }
    }
}

impl error::Error for TcpTlsConfigError {}

impl From<TcpTlsConfigError> for io::Error {
    fn from(error: TcpTlsConfigError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}
