//! Application admission options and bounds for managed TCP endpoints.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use std::{error, fmt, io};

use groupnet_transport::QueueCapacity;
use groupnet_transport::admission::{Admission, MAX_CREDENTIAL_BYTES};

/// Bounds for policy work and established managed TCP sessions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TcpAdmissionConfig {
    /// Maximum handshakes executing concurrently per direction, including custom
    /// policy work. Inbound and outbound each have this capacity. Default: 64.
    pub max_pending: QueueCapacity,
    /// Maximum live admitted identities and dynamically learned dial addresses.
    /// Duplicate live identities are rejected. Registered bootstrap addresses
    /// are retained separately and cannot be overwritten by learning.
    /// Default: 1024.
    pub max_peers: NonZeroUsize,
    /// Deadline covering connection establishment, wire exchange, and policy
    /// work. Must be nonzero. Default: 5s.
    pub handshake_timeout: Duration,
}

impl Default for TcpAdmissionConfig {
    fn default() -> Self {
        Self {
            max_pending: QueueCapacity::of(64),
            max_peers: NonZeroUsize::new(1024).expect("nonzero literal"),
            handshake_timeout: Duration::from_secs(5),
        }
    }
}

impl TcpAdmissionConfig {
    /// Checks the bound the field types cannot express.
    ///
    /// # Errors
    /// Returns [`TcpAdmissionError::ZeroHandshakeTimeout`].
    pub const fn validate(&self) -> Result<(), TcpAdmissionError> {
        if self.handshake_timeout.is_zero() {
            return Err(TcpAdmissionError::ZeroHandshakeTimeout);
        }
        Ok(())
    }
}

/// Application admission for a managed TCP endpoint: the policy judging
/// inbound claims, the credential this endpoint presents to its peers'
/// policies, and the bounds on that work. Passed to
/// [`TcpMsgTransport::bind_admitted`](crate::TcpMsgTransport::bind_admitted).
///
/// There is no default policy: membership must be an explicit choice, so use
/// `OpenAdmission` deliberately for unauthenticated membership.
#[derive(Clone)]
pub struct TcpAdmission {
    /// Judges every inbound claim; adapters fail closed on its errors.
    pub policy: Arc<dyn Admission>,
    /// Opaque application bytes presented to remote policies, at most
    /// [`MAX_CREDENTIAL_BYTES`]. Never printed by `Debug`. Default: empty.
    pub credential: Vec<u8>,
    /// Handshake concurrency, live-peer capacity and handshake deadline.
    pub config: TcpAdmissionConfig,
}

impl TcpAdmission {
    /// Admits through `policy`, presenting no credential, with default bounds.
    #[must_use]
    pub fn new(policy: Arc<dyn Admission>) -> Self {
        Self {
            policy,
            credential: Vec::new(),
            config: TcpAdmissionConfig::default(),
        }
    }

    /// Checks the credential size and the [`TcpAdmissionConfig`] bounds.
    ///
    /// # Errors
    /// Returns the [`TcpAdmissionError`] naming the offending field.
    pub fn validate(&self) -> Result<(), TcpAdmissionError> {
        if self.credential.len() > MAX_CREDENTIAL_BYTES {
            return Err(TcpAdmissionError::CredentialTooLong {
                len: self.credential.len(),
            });
        }
        self.config.validate()
    }
}

impl fmt::Debug for TcpAdmission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Policies can hold secrets too: neither policy nor credential is printed.
        f.debug_struct("TcpAdmission")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// A violated [`TcpAdmission`] rule, from [`TcpAdmission::validate`] or
/// [`TcpAdmissionConfig::validate`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TcpAdmissionError {
    /// `credential` exceeds [`MAX_CREDENTIAL_BYTES`].
    CredentialTooLong {
        /// The credential's length in bytes.
        len: usize,
    },
    /// `handshake_timeout` is zero.
    ZeroHandshakeTimeout,
}

impl fmt::Display for TcpAdmissionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CredentialTooLong { len } => write!(
                f,
                "credential is {len} bytes, above the {MAX_CREDENTIAL_BYTES}-byte limit"
            ),
            Self::ZeroHandshakeTimeout => f.write_str("handshake_timeout must be nonzero"),
        }
    }
}

impl error::Error for TcpAdmissionError {}

impl From<TcpAdmissionError> for io::Error {
    fn from(error: TcpAdmissionError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}

/// Why [`TcpMsgTransport::connect_peer`](crate::TcpMsgTransport::connect_peer)
/// started nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TcpConnectPeerError {
    /// The endpoint is direct but was bound without a [`TcpAdmission`]
    /// ([`bind`](crate::TcpMsgTransport::bind) rather than
    /// [`bind_admitted`](crate::TcpMsgTransport::bind_admitted)): it has no
    /// admission exchange to start.
    AdmissionNotConfigured,
}

impl fmt::Display for TcpConnectPeerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AdmissionNotConfigured => f.write_str(
                "connect_peer needs an admission-enabled endpoint (bind_admitted), \
                 and this endpoint was bound without TCP admission",
            ),
        }
    }
}

impl error::Error for TcpConnectPeerError {}

impl From<TcpConnectPeerError> for io::Error {
    fn from(error: TcpConnectPeerError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}
