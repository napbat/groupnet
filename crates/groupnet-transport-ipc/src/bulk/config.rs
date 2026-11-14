//! Accept-side bounds, the admission policy and the dial credential of an
//! [`IpcBulkTransport`](super::IpcBulkTransport).

use std::sync::Arc;
use std::time::Duration;
use std::{error, fmt, io};

use groupnet_transport::QueueCapacity;
use groupnet_transport::admission::{Admission, MAX_CREDENTIAL_BYTES};

#[cfg(windows)]
use crate::ipc::platform::MAX_SESSIONS;

/// Default deadline for one identity handshake.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// Bounds, admission policy and credential of a listening
/// [`IpcBulkTransport`](super::IpcBulkTransport), from
/// [`bind_with`](super::IpcBulkTransport::bind_with).
#[derive(Clone)]
pub struct IpcBulkConfig {
    /// Deadline for one identity handshake to complete, both on accepted
    /// connections (the admission policy runs inside it) and on connections
    /// this endpoint dials (connecting included; a dial that misses it fails
    /// with `TimedOut`). Must be nonzero. Default: 5s.
    pub handshake_timeout: Duration,
    /// Accepted connections held at once between the native accept and
    /// [`accept`](groupnet_transport::bulk::BulkTransport::accept):
    /// handshaking or waiting in the queue. At the bound, new connections wait
    /// for the listener. Default: 64.
    pub max_handshakes: QueueCapacity,
    /// Handshaken connections buffered until
    /// [`accept`](groupnet_transport::bulk::BulkTransport::accept) takes them.
    /// Default: 64.
    pub accept_queue: QueueCapacity,
    /// Accepted connections alive at once, from the native accept until the
    /// stream is dropped. At the bound the listener accepts nothing until a
    /// connection ends. Windows allows at most 252, keeping its named-pipe
    /// instance count finite. Default: 64.
    pub max_connections: QueueCapacity,
    /// Judges every inbound identity claim and its credential; a refusal
    /// closes the connection. `None` (the default) accepts every local peer:
    /// the operating system boundary is the trust boundary, as for
    /// [`IpcTransport`](crate::IpcTransport). Never printed by `Debug`.
    pub admission: Option<Arc<dyn Admission>>,
    /// Opaque application bytes this endpoint presents to the policies of the
    /// endpoints it dials, at most [`MAX_CREDENTIAL_BYTES`]. Never printed by
    /// `Debug`. Default: empty.
    pub credential: Vec<u8>,
}

impl Default for IpcBulkConfig {
    fn default() -> Self {
        Self {
            handshake_timeout: HANDSHAKE_TIMEOUT,
            max_handshakes: QueueCapacity::of(64),
            accept_queue: QueueCapacity::of(64),
            max_connections: QueueCapacity::of(64),
            admission: None,
            credential: Vec::new(),
        }
    }
}

impl IpcBulkConfig {
    /// Checks the bounds the types cannot express, before any listener exists.
    ///
    /// # Errors
    /// Returns the [`IpcBulkConfigError`] naming the first violated field.
    pub const fn validate(&self) -> Result<(), IpcBulkConfigError> {
        if let Err(error) = check_dial(self.handshake_timeout, self.credential.len()) {
            return Err(error);
        }
        #[cfg(windows)]
        if self.max_connections.get() > MAX_SESSIONS {
            return Err(IpcBulkConfigError::TooManyWindowsConnections {
                max_connections: self.max_connections,
            });
        }
        Ok(())
    }
}

impl fmt::Debug for IpcBulkConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Policies can hold secrets too: neither policy nor credential is printed.
        f.debug_struct("IpcBulkConfig")
            .field("handshake_timeout", &self.handshake_timeout)
            .field("max_handshakes", &self.max_handshakes)
            .field("accept_queue", &self.accept_queue)
            .field("max_connections", &self.max_connections)
            .finish_non_exhaustive()
    }
}

/// The handshake deadline and credential of a dial-only
/// [`IpcBulkTransport`](super::IpcBulkTransport), from
/// [`dial_only`](super::IpcBulkTransport::dial_only).
#[derive(Clone)]
pub struct IpcBulkDialConfig {
    /// Deadline for connecting and completing one identity handshake; a dial
    /// that misses it fails with `TimedOut`. Must be nonzero. Default: 5s.
    pub handshake_timeout: Duration,
    /// Opaque application bytes presented to the policies of the endpoints
    /// this one dials, at most [`MAX_CREDENTIAL_BYTES`]. Never printed by
    /// `Debug`. Default: empty.
    pub credential: Vec<u8>,
}

impl Default for IpcBulkDialConfig {
    fn default() -> Self {
        Self {
            handshake_timeout: HANDSHAKE_TIMEOUT,
            credential: Vec::new(),
        }
    }
}

impl IpcBulkDialConfig {
    /// Checks the deadline and the credential size.
    ///
    /// # Errors
    /// Returns [`IpcBulkConfigError::ZeroHandshakeTimeout`] or
    /// [`IpcBulkConfigError::CredentialTooLong`].
    pub const fn validate(&self) -> Result<(), IpcBulkConfigError> {
        check_dial(self.handshake_timeout, self.credential.len())
    }
}

impl fmt::Debug for IpcBulkDialConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IpcBulkDialConfig")
            .field("handshake_timeout", &self.handshake_timeout)
            .finish_non_exhaustive()
    }
}

/// The rules both configurations share.
const fn check_dial(
    handshake_timeout: Duration,
    credential_len: usize,
) -> Result<(), IpcBulkConfigError> {
    if handshake_timeout.is_zero() {
        return Err(IpcBulkConfigError::ZeroHandshakeTimeout);
    }
    if credential_len > MAX_CREDENTIAL_BYTES {
        return Err(IpcBulkConfigError::CredentialTooLong {
            len: credential_len,
        });
    }
    Ok(())
}

/// A violated [`IpcBulkConfig`] or [`IpcBulkDialConfig`] rule, from their
/// `validate`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IpcBulkConfigError {
    /// `handshake_timeout` is zero.
    ZeroHandshakeTimeout,
    /// `credential` exceeds [`MAX_CREDENTIAL_BYTES`].
    CredentialTooLong {
        /// The credential's length in bytes.
        len: usize,
    },
    /// `max_connections` exceeds the 252 named-pipe instances Windows allows.
    TooManyWindowsConnections {
        /// The configured connection capacity.
        max_connections: QueueCapacity,
    },
}

impl fmt::Display for IpcBulkConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroHandshakeTimeout => f.write_str("handshake_timeout must be nonzero"),
            Self::CredentialTooLong { len } => write!(
                f,
                "credential is {len} bytes, above the {MAX_CREDENTIAL_BYTES}-byte limit"
            ),
            Self::TooManyWindowsConnections { max_connections } => write!(
                f,
                "max_connections {max_connections} exceeds the 252 concurrent connections Windows IPC supports"
            ),
        }
    }
}

impl error::Error for IpcBulkConfigError {}

impl From<IpcBulkConfigError> for io::Error {
    fn from(error: IpcBulkConfigError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid_and_each_rule_names_its_field() {
        assert_eq!(IpcBulkConfig::default().validate(), Ok(()));
        assert_eq!(IpcBulkDialConfig::default().validate(), Ok(()));
        assert_eq!(
            IpcBulkConfig {
                handshake_timeout: Duration::ZERO,
                ..IpcBulkConfig::default()
            }
            .validate(),
            Err(IpcBulkConfigError::ZeroHandshakeTimeout)
        );
        assert_eq!(
            IpcBulkDialConfig {
                handshake_timeout: Duration::ZERO,
                ..IpcBulkDialConfig::default()
            }
            .validate(),
            Err(IpcBulkConfigError::ZeroHandshakeTimeout)
        );
        let longest = vec![7; MAX_CREDENTIAL_BYTES];
        assert_eq!(
            IpcBulkDialConfig {
                credential: longest.clone(),
                ..IpcBulkDialConfig::default()
            }
            .validate(),
            Ok(())
        );
        let mut overlong = longest;
        overlong.push(7);
        let error = IpcBulkConfig {
            credential: overlong,
            ..IpcBulkConfig::default()
        }
        .validate()
        .expect_err("credential above the bound");
        assert_eq!(
            error,
            IpcBulkConfigError::CredentialTooLong {
                len: MAX_CREDENTIAL_BYTES + 1
            }
        );
        let io = io::Error::from(error);
        assert_eq!(io.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            io.get_ref()
                .and_then(|inner| inner.downcast_ref::<IpcBulkConfigError>()),
            Some(&error)
        );
    }

    #[test]
    fn debug_never_prints_the_credential() {
        let config = IpcBulkConfig {
            credential: b"secret-token".to_vec(),
            ..IpcBulkConfig::default()
        };
        assert!(!format!("{config:?}").contains("secret"));
        let dial = IpcBulkDialConfig {
            credential: b"secret-token".to_vec(),
            ..IpcBulkDialConfig::default()
        };
        assert!(!format!("{dial:?}").contains("secret"));
    }

    #[cfg(windows)]
    #[test]
    fn windows_connection_limit_never_uses_unlimited_instance_sentinel() {
        assert_eq!(
            IpcBulkConfig {
                max_connections: QueueCapacity::of(252),
                ..IpcBulkConfig::default()
            }
            .validate(),
            Ok(())
        );
        assert_eq!(
            IpcBulkConfig {
                max_connections: QueueCapacity::of(253),
                ..IpcBulkConfig::default()
            }
            .validate(),
            Err(IpcBulkConfigError::TooManyWindowsConnections {
                max_connections: QueueCapacity::of(253)
            })
        );
    }
}
