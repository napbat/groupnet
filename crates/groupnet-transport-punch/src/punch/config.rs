//! Native UDP endpoint configuration and its validation rules.

use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroUsize;
use std::{error, fmt, io};

use groupnet_core::NodeId;
use groupnet_transport::admission::MAX_CREDENTIAL_BYTES;
use groupnet_transport::{NodeIdError, QueueCapacity, validate_node_id};

use super::candidates::{self, MAX_CANDIDATES, MAX_SOCKETS};
use super::{DEFAULT_MAX_PEERS, DEFAULT_QUEUE_CAPACITY, NetworkKey, PathPolicy};
use crate::PeerPolicy;
use crate::common::{MAX_IDENTITY_BYTES, PeerListFault, check_peers, wildcard_for};

/// Socket, trusted identities and rendezvous policy for one UDP endpoint.
///
/// [`new`](Self::new) takes exactly the identity and security inputs — the
/// local identity, the rendezvous, the optional fabric key and the peer
/// policy; every other field is operational policy with a documented default
/// that callers refine with struct-update syntax.
#[derive(Clone)]
pub struct UdpPunchConfig {
    /// Stable routing identity (1–64 UTF-8 bytes).
    pub local: NodeId,
    /// Primary UDP bind address; its family must match the rendezvous.
    /// This socket owns the observed mapping and remains the relay/control path.
    /// Default: the ephemeral wildcard of the rendezvous address family.
    pub bind: SocketAddr,
    /// Explicit rendezvous address; no third-party discovery service is used.
    pub rendezvous: SocketAddr,
    /// Optional explicit trusted-fabric key; keyed peers never downgrade.
    /// `None` requires no provisioned transport key or identity keypair, but does
    /// not disable internal random challenges or application admission credentials.
    pub key: Option<NetworkKey>,
    /// A static allowlist, or dynamic discovery of rendezvous-admitted peers.
    pub peers: PeerPolicy,
    /// Opaque application admission credential (at most
    /// [`MAX_CREDENTIAL_BYTES`] bytes). Default: empty.
    pub credential: Vec<u8>,
    /// Direct-path preference or relay-only operation. Default:
    /// [`PathPolicy::DirectPreferred`] when keyed, conservatively
    /// [`PathPolicy::RelayOnly`] when keyless; a keyless endpoint may enable
    /// direct paths explicitly.
    pub policy: PathPolicy,
    /// Additional direct-path sockets (at most three), including another IP family.
    /// Their lifetimes are owned by this endpoint; the primary `bind` owns relay registration.
    /// Default: none.
    pub candidate_binds: Vec<SocketAddr>,
    /// Operator-supplied direct addresses (at most eight), checked independently.
    /// Addresses are untrusted hints, never proof of a usable path. Default: none.
    pub advertised_candidates: Vec<SocketAddr>,
    /// Gather usable local interface addresses for wildcard-bound sockets.
    /// Link-local addresses are excluded because remote scope identifiers are not portable.
    /// Default: `true`.
    pub gather_interfaces: bool,
    /// Maximum live admitted peers retained by this endpoint.
    /// This operational limit does not change candidate or wire-format bounds.
    /// Default: 128.
    pub max_peers: NonZeroUsize,
    /// Packet capacity of each incoming and outgoing queue.
    /// Full queues drop best-effort packets without blocking. Default: 128.
    pub queue_capacity: QueueCapacity,
}

impl UdpPunchConfig {
    /// Configures an endpoint from its identity and security inputs, with an
    /// ephemeral wildcard bind, no candidates beyond gathered interfaces, an
    /// empty credential and the key-dependent default [`PathPolicy`].
    #[must_use]
    pub fn new(
        local: NodeId,
        rendezvous: SocketAddr,
        key: Option<NetworkKey>,
        peers: PeerPolicy,
    ) -> Self {
        Self {
            local,
            bind: wildcard_for(rendezvous),
            rendezvous,
            policy: if key.is_some() {
                PathPolicy::DirectPreferred
            } else {
                PathPolicy::RelayOnly
            },
            key,
            peers,
            credential: Vec::new(),
            candidate_binds: Vec::new(),
            advertised_candidates: Vec::new(),
            gather_interfaces: true,
            max_peers: DEFAULT_MAX_PEERS,
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
        }
    }

    /// Checks every rule [`UdpConnection::bind`](super::UdpConnection::bind)
    /// enforces before it binds a socket.
    ///
    /// # Errors
    /// Returns the first violated rule, naming its field.
    pub fn validate(&self) -> Result<(), UdpPunchConfigError> {
        validate_node_id(&self.local, MAX_IDENTITY_BYTES).map_err(UdpPunchConfigError::Local)?;
        let peers = self.peers.static_peers();
        if peers.len() > self.max_peers.get() {
            return Err(UdpPunchConfigError::PeersExceedMaxPeers {
                peers: peers.len(),
                max_peers: self.max_peers,
            });
        }
        check_peers(peers).map_err(|fault| match fault {
            PeerListFault::Invalid { index, error } => UdpPunchConfigError::Peer { index, error },
            PeerListFault::Duplicate { index } => UdpPunchConfigError::DuplicatePeer { index },
        })?;
        if peers.contains(&self.local) {
            return Err(UdpPunchConfigError::LocalListedAsPeer);
        }
        if self.credential.len() > MAX_CREDENTIAL_BYTES {
            return Err(UdpPunchConfigError::CredentialTooLarge {
                len: self.credential.len(),
            });
        }
        if !candidates::valid(self.rendezvous) {
            return Err(UdpPunchConfigError::InvalidRendezvous);
        }
        if self.bind.is_ipv4() != self.rendezvous.is_ipv4() {
            return Err(UdpPunchConfigError::BindFamilyMismatch);
        }
        if !bindable(self.bind) {
            return Err(UdpPunchConfigError::UnsupportedBind);
        }
        if self.candidate_binds.len() >= MAX_SOCKETS {
            return Err(UdpPunchConfigError::TooManyCandidateBinds {
                len: self.candidate_binds.len(),
            });
        }
        if let Some(index) = self
            .candidate_binds
            .iter()
            .position(|bind| !bindable(*bind))
        {
            return Err(UdpPunchConfigError::UnsupportedCandidateBind { index });
        }
        if self.advertised_candidates.len() > MAX_CANDIDATES {
            return Err(UdpPunchConfigError::TooManyAdvertisedCandidates {
                len: self.advertised_candidates.len(),
            });
        }
        if let Some(index) = self
            .advertised_candidates
            .iter()
            .position(|address| !candidates::valid(*address))
        {
            return Err(UdpPunchConfigError::InvalidAdvertisedCandidate { index });
        }
        Ok(())
    }
}

impl fmt::Debug for UdpPunchConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UdpPunchConfig")
            .field("local", &self.local)
            .field("bind", &self.bind)
            .field("rendezvous", &self.rendezvous)
            .field("key", &self.key)
            .field("peers", &self.peers)
            .field("credential", &"[REDACTED]")
            .field("policy", &self.policy)
            .field("candidate_binds", &self.candidate_binds)
            .field("advertised_candidates", &self.advertised_candidates)
            .field("gather_interfaces", &self.gather_interfaces)
            .field("max_peers", &self.max_peers)
            .field("queue_capacity", &self.queue_capacity)
            .finish()
    }
}

/// Multicast, broadcast and link-local UDP binds are unsupported.
fn bindable(address: SocketAddr) -> bool {
    match address.ip() {
        IpAddr::V4(ip) => !ip.is_multicast() && !ip.is_link_local() && !ip.is_broadcast(),
        IpAddr::V6(ip) => !ip.is_multicast() && !ip.is_unicast_link_local(),
    }
}

/// A [`UdpPunchConfig`] rule violation, naming the offending field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UdpPunchConfigError {
    /// `local` is not a valid native identity.
    Local(NodeIdError),
    /// The static `peers` list holds more identities than `max_peers` admits.
    PeersExceedMaxPeers {
        /// Number of configured peers.
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
    /// The static `peers` list holds the endpoint's own `local` identity.
    LocalListedAsPeer,
    /// `credential` exceeds [`MAX_CREDENTIAL_BYTES`].
    CredentialTooLarge {
        /// The credential's length in bytes.
        len: usize,
    },
    /// `rendezvous` is not a routable unicast address with a nonzero port.
    InvalidRendezvous,
    /// `bind` and `rendezvous` use different address families.
    BindFamilyMismatch,
    /// `bind` is a multicast, broadcast or link-local address.
    UnsupportedBind,
    /// `candidate_binds` lists more than three additional sockets.
    TooManyCandidateBinds {
        /// Number of configured candidate binds.
        len: usize,
    },
    /// `candidate_binds[index]` is a multicast, broadcast or link-local address.
    UnsupportedCandidateBind {
        /// Position of the rejected bind.
        index: usize,
    },
    /// `advertised_candidates` lists more than eight addresses.
    TooManyAdvertisedCandidates {
        /// Number of advertised candidates.
        len: usize,
    },
    /// `advertised_candidates[index]` is not a routable unicast address with a nonzero port.
    InvalidAdvertisedCandidate {
        /// Position of the rejected candidate.
        index: usize,
    },
}

impl fmt::Display for UdpPunchConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Local(error) => write!(f, "local: {error}"),
            Self::PeersExceedMaxPeers { peers, max_peers } => {
                write!(
                    f,
                    "peers lists {peers} identities, above max_peers {max_peers}"
                )
            }
            Self::Peer { index, error } => write!(f, "peers[{index}]: {error}"),
            Self::DuplicatePeer { index } => {
                write!(f, "peers[{index}] repeats an earlier identity")
            }
            Self::LocalListedAsPeer => f.write_str("peers must not list the local identity"),
            Self::CredentialTooLarge { len } => write!(
                f,
                "credential is {len} bytes, above the {MAX_CREDENTIAL_BYTES}-byte bound"
            ),
            Self::InvalidRendezvous => {
                f.write_str("rendezvous must be a routable unicast address with a nonzero port")
            }
            Self::BindFamilyMismatch => f.write_str("bind must use the rendezvous address family"),
            Self::UnsupportedBind => {
                f.write_str("bind must not be a multicast, broadcast or link-local address")
            }
            Self::TooManyCandidateBinds { len } => write!(
                f,
                "candidate_binds lists {len} sockets, above the {}-socket bound",
                MAX_SOCKETS - 1
            ),
            Self::UnsupportedCandidateBind { index } => write!(
                f,
                "candidate_binds[{index}] must not be a multicast, broadcast or link-local address"
            ),
            Self::TooManyAdvertisedCandidates { len } => write!(
                f,
                "advertised_candidates lists {len} addresses, above the {MAX_CANDIDATES}-address bound"
            ),
            Self::InvalidAdvertisedCandidate { index } => write!(
                f,
                "advertised_candidates[{index}] must be a routable unicast address with a nonzero port"
            ),
        }
    }
}

impl error::Error for UdpPunchConfigError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Self::Local(error) | Self::Peer { error, .. } => Some(error),
            _ => None,
        }
    }
}

impl From<UdpPunchConfigError> for io::Error {
    fn from(error: UdpPunchConfigError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}

#[cfg(test)]
mod tests;
