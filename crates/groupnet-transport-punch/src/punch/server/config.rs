//! Rendezvous admission policy, operational resource limits and their validation.

use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::{error, fmt, io};

use groupnet_core::NodeId;
use groupnet_transport::NodeIdError;
use groupnet_transport::admission::{Admission, OpenAdmission};

use super::super::{DEFAULT_MAX_PEERS, MAX_UDP_MESSAGE, NetworkKey};
use crate::RelayPacing;
use crate::common::{PeerListFault, check_peers};

const DEFAULT_PENDING_ADMISSIONS: NonZeroUsize = NonZeroUsize::new(32).unwrap();

/// Operational rendezvous capacities, independent of wire and authentication bounds.
/// Every combination is valid; cross-field rules live in [`UdpRendezvousConfig::validate`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UdpRendezvousLimits {
    /// Maximum established or admission-pending peer identities.
    pub max_peers: NonZeroUsize,
    /// Maximum evictable, unproven address challenges.
    pub max_challenges: NonZeroUsize,
    /// Maximum concurrent application policy evaluations.
    pub max_pending_admissions: NonZeroUsize,
}

impl Default for UdpRendezvousLimits {
    fn default() -> Self {
        Self {
            max_peers: DEFAULT_MAX_PEERS,
            max_challenges: DEFAULT_MAX_PEERS,
            max_pending_admissions: DEFAULT_PENDING_ADMISSIONS,
        }
    }
}

/// Socket, fabric authentication, application admission, and rendezvous capacities.
#[derive(Clone)]
pub struct UdpRendezvousConfig {
    /// UDP socket bind address (multicast is rejected).
    pub bind: SocketAddr,
    /// Optional trusted-fabric key; keyed traffic never downgrades.
    pub key: Option<NetworkKey>,
    /// Fixed identity allowlist, or `None` for dynamic application admission.
    pub peers: Option<Vec<NodeId>>,
    /// Application admission policy, evaluated only after return-routability.
    pub admission: Arc<dyn Admission>,
    /// Operational capacities; control-rate limits and challenge/session deadlines stay fixed.
    pub limits: UdpRendezvousLimits,
    /// Optional per-identity byte budget for admitted relay data, which never
    /// spends the control-rate budget. Datagrams over budget are dropped, so a
    /// byte burst must hold at least one [`MAX_UDP_MESSAGE`] payload.
    pub relay_pacing: RelayPacing,
}

impl fmt::Debug for UdpRendezvousConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UdpRendezvousConfig")
            .field("bind", &self.bind)
            .field("key", &self.key)
            .field("peers", &self.peers)
            .field("admission", &"application policy")
            .field("limits", &self.limits)
            .field("relay_pacing", &self.relay_pacing)
            .finish()
    }
}

impl UdpRendezvousConfig {
    /// Creates keyed rendezvous configuration with a fixed identity allowlist
    /// and open application admission.
    #[must_use]
    pub fn new(bind: SocketAddr, key: NetworkKey, peers: Vec<NodeId>) -> Self {
        Self {
            peers: Some(peers),
            ..Self::with_admission(bind, Some(key), Arc::new(OpenAdmission))
        }
    }

    /// Creates explicitly keyless dynamic discovery with open application admission.
    /// Open admission provides no cryptographic identity assurance.
    #[must_use]
    pub fn open(bind: SocketAddr) -> Self {
        Self::with_admission(bind, None, Arc::new(OpenAdmission))
    }

    /// Creates dynamic discovery with application admission and optional fabric HMAC.
    /// Keyed datagrams never fall back to keyless parsing. Policies run only
    /// after address return-routability, with bounded time and concurrency.
    #[must_use]
    pub fn with_admission(
        bind: SocketAddr,
        key: Option<NetworkKey>,
        admission: Arc<dyn Admission>,
    ) -> Self {
        Self {
            bind,
            key,
            peers: None,
            admission,
            limits: UdpRendezvousLimits::default(),
            relay_pacing: RelayPacing::Backpressure,
        }
    }

    /// Checks every rule [`UdpRendezvous::bind`](super::UdpRendezvous::bind)
    /// enforces before it binds a socket.
    ///
    /// # Errors
    /// Returns the first violated rule, naming its field.
    pub fn validate(&self) -> Result<(), UdpRendezvousConfigError> {
        if let RelayPacing::Bytes { burst_bytes, .. } = self.relay_pacing
            && burst_bytes.get() < MAX_UDP_MESSAGE as u64
        {
            return Err(UdpRendezvousConfigError::PacingBurstBelowMessage);
        }
        if self.bind.ip().is_multicast() {
            return Err(UdpRendezvousConfigError::MulticastBind);
        }
        if let Some(peers) = &self.peers {
            if peers.len() > self.limits.max_peers.get() {
                return Err(UdpRendezvousConfigError::PeersExceedMaxPeers {
                    peers: peers.len(),
                    max_peers: self.limits.max_peers,
                });
            }
            check_peers(peers).map_err(|fault| match fault {
                PeerListFault::Invalid { index, error } => {
                    UdpRendezvousConfigError::Peer { index, error }
                }
                PeerListFault::Duplicate { index } => {
                    UdpRendezvousConfigError::DuplicatePeer { index }
                }
            })?;
        }
        Ok(())
    }
}

/// A [`UdpRendezvousConfig`] rule violation, naming the offending field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UdpRendezvousConfigError {
    /// `relay_pacing` byte burst cannot hold one [`MAX_UDP_MESSAGE`] payload.
    PacingBurstBelowMessage,
    /// `bind` is a multicast address.
    MulticastBind,
    /// `peers` lists more identities than `limits.max_peers` retains.
    PeersExceedMaxPeers {
        /// Number of allowlisted identities.
        peers: usize,
        /// The configured peer capacity.
        max_peers: NonZeroUsize,
    },
    /// `peers[index]` is not a valid native identity.
    Peer {
        /// Position of the rejected entry.
        index: usize,
        /// Why the identity was rejected.
        error: NodeIdError,
    },
    /// `peers[index]` repeats an earlier entry.
    DuplicatePeer {
        /// Position of the repeated entry.
        index: usize,
    },
}

impl fmt::Display for UdpRendezvousConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PacingBurstBelowMessage => write!(
                f,
                "relay_pacing burst_bytes must hold one {MAX_UDP_MESSAGE}-byte relay message"
            ),
            Self::MulticastBind => f.write_str("bind must not be a multicast address"),
            Self::PeersExceedMaxPeers { peers, max_peers } => write!(
                f,
                "peers lists {peers} identities, above limits.max_peers {max_peers}"
            ),
            Self::Peer { index, error } => write!(f, "peers[{index}]: {error}"),
            Self::DuplicatePeer { index } => {
                write!(f, "peers[{index}] repeats an earlier identity")
            }
        }
    }
}

impl error::Error for UdpRendezvousConfigError {}

impl From<UdpRendezvousConfigError> for io::Error {
    fn from(error: UdpRendezvousConfigError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}
