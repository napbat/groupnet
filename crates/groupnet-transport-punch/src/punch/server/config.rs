//! Rendezvous admission policy, operational resource limits and their validation.

use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::{error, fmt, io};

use groupnet_transport::NodeIdError;
use groupnet_transport::admission::Admission;

use super::super::{DEFAULT_MAX_PEERS, MAX_UDP_MESSAGE, NetworkKey};
use crate::common::{PeerListFault, check_peers};
use crate::{PeerPolicy, RelayPacing};

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
///
/// [`new`](Self::new) takes exactly the security inputs — the optional fabric
/// key, the peer policy and the application admission policy — with the bind
/// address; capacities and relay pacing are defaultable policy.
#[derive(Clone)]
pub struct UdpRendezvousConfig {
    /// UDP socket bind address (multicast is rejected).
    pub bind: SocketAddr,
    /// Optional trusted-fabric key; keyed traffic never downgrades.
    pub key: Option<NetworkKey>,
    /// A fixed identity allowlist (packets from other identities are dropped
    /// unanswered), or dynamic discovery.
    pub peers: PeerPolicy,
    /// Application admission policy, evaluated only after the allowlist and
    /// return-routability checks, with the client's bounded credential, in
    /// either peer mode.
    pub admission: Arc<dyn Admission>,
    /// Operational capacities; control-rate limits and challenge/session
    /// deadlines stay fixed. Default: [`UdpRendezvousLimits::default`].
    pub limits: UdpRendezvousLimits,
    /// Optional per-identity byte budget for admitted relay data, which never
    /// spends the control-rate budget. Datagrams over budget are dropped, so a
    /// byte burst must hold at least one [`MAX_UDP_MESSAGE`] payload.
    /// Default: [`RelayPacing::Backpressure`].
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
    /// Configures a rendezvous from its security inputs with default limits
    /// and relay pacing. Keyed datagrams never fall back to keyless parsing.
    /// `admission` runs only after address return-routability, with bounded
    /// time and concurrency; `OpenAdmission` is the explicit choice to accept
    /// every claimed identity, which proves no identity ownership.
    #[must_use]
    pub fn new(
        bind: SocketAddr,
        key: Option<NetworkKey>,
        peers: PeerPolicy,
        admission: Arc<dyn Admission>,
    ) -> Self {
        Self {
            bind,
            key,
            peers,
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
        let peers = self.peers.static_peers();
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
            PeerListFault::Duplicate { index } => UdpRendezvousConfigError::DuplicatePeer { index },
        })?;
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
    /// The static `peers` list holds more identities than `limits.max_peers`
    /// retains.
    PeersExceedMaxPeers {
        /// Number of allowlisted identities.
        peers: usize,
        /// The configured peer capacity.
        max_peers: NonZeroUsize,
    },
    /// Static `peers[index]` is not a valid native identity.
    Peer {
        /// Position of the rejected entry.
        index: usize,
        /// Why the identity was rejected.
        error: NodeIdError,
    },
    /// Static `peers[index]` repeats an earlier entry.
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

impl error::Error for UdpRendezvousConfigError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Self::Peer { error, .. } => Some(error),
            _ => None,
        }
    }
}

impl From<UdpRendezvousConfigError> for io::Error {
    fn from(error: UdpRendezvousConfigError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}
