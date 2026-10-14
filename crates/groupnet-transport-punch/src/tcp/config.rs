//! Native TCP endpoint configuration and its validation rules.

use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::{error, fmt, io};

use groupnet_core::NodeId;
use groupnet_transport::admission::MAX_CREDENTIAL_BYTES;
use groupnet_transport::{NodeIdError, QueueCapacity, validate_node_id};

use super::{MAX_CANDIDATE_BINDS, MAX_CANDIDATES, MAX_PEERS, sockets};
use crate::common::{MAX_IDENTITY_BYTES, PeerListFault, check_peers, wildcard_for};
use crate::{NetworkKey, PathPolicy, PeerPolicy};

const DEFAULT_MAX_PEERS: NonZeroUsize = NonZeroUsize::new(128).unwrap();

/// Configuration for one TCP-only traversal endpoint.
///
/// [`new`](Self::new) takes exactly the identity and security inputs — the
/// local identity, the rendezvous, the optional fabric key and the peer
/// policy; every other field is operational policy with a documented default
/// that callers refine with struct-update syntax.
#[derive(Clone)]
pub struct TcpPunchConfig {
    /// Application-chosen routing identity (1–64 UTF-8 bytes).
    pub local: NodeId,
    /// Registration and primary reusable listening/source socket bind.
    /// Default: the ephemeral wildcard of the rendezvous address family.
    pub bind: SocketAddr,
    /// Explicit TCP rendezvous address.
    pub rendezvous: SocketAddr,
    /// Optional trusted-fabric key. Keyed endpoints never downgrade.
    pub key: Option<NetworkKey>,
    /// A static allowlist (at most 255 entries), or dynamic discovery of
    /// rendezvous-admitted identities.
    pub peers: PeerPolicy,
    /// Opaque application admission credential, limited to
    /// [`MAX_CREDENTIAL_BYTES`] bytes. Default: empty.
    pub credential: Vec<u8>,
    /// Direct preference or privacy-preserving relay-only operation. Default:
    /// [`PathPolicy::DirectPreferred`] when keyed, [`PathPolicy::RelayOnly`]
    /// when keyless.
    pub policy: PathPolicy,
    /// Additional listener/source binds, at most three, including other families.
    /// Each socket is retained for the lifetime of the endpoint. Default: none.
    pub candidate_binds: Vec<SocketAddr>,
    /// Explicit externally mapped TCP candidates, at most eight total candidates.
    /// These are hints, never authorization. Invalid addresses are rejected.
    /// Each local/remote pair permits one outstanding check, with three-attempt
    /// bursts separated by a ten-second cooldown. A rotating peer scheduler caps
    /// the endpoint at sixteen outstanding dials and sixteen new checks/second.
    /// Default: none.
    pub advertised_candidates: Vec<SocketAddr>,
    /// Expand wildcard listeners to local interface addresses when gathering.
    /// IPv6 link-local addresses without a scope are not advertised.
    /// Default: `true`.
    pub gather_interfaces: bool,
    /// Maximum concurrently admitted neighbors (an operational memory bound).
    /// Default: 128.
    pub max_peers: NonZeroUsize,
    /// Capacity of the endpoint's bounded packet, event and writer queues.
    /// Default: 128.
    pub queue_capacity: QueueCapacity,
}

impl TcpPunchConfig {
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
            queue_capacity: QueueCapacity::of(128),
        }
    }

    /// Checks every rule [`TcpConnection::bind`](super::TcpConnection::bind)
    /// enforces before it opens a socket.
    ///
    /// # Errors
    /// Returns the first violated rule, naming its field.
    pub fn validate(&self) -> Result<(), TcpPunchConfigError> {
        validate_node_id(&self.local, MAX_IDENTITY_BYTES).map_err(TcpPunchConfigError::Local)?;
        let peers = self.peers.static_peers();
        if peers.len() > MAX_PEERS {
            return Err(TcpPunchConfigError::PeersExceedWireLimit { peers: peers.len() });
        }
        if peers.len() > self.max_peers.get() {
            return Err(TcpPunchConfigError::PeersExceedMaxPeers {
                peers: peers.len(),
                max_peers: self.max_peers,
            });
        }
        check_peers(peers).map_err(|fault| match fault {
            PeerListFault::Invalid { index, error } => TcpPunchConfigError::Peer { index, error },
            PeerListFault::Duplicate { index } => TcpPunchConfigError::DuplicatePeer { index },
        })?;
        if peers.contains(&self.local) {
            return Err(TcpPunchConfigError::LocalListedAsPeer);
        }
        if self.credential.len() > MAX_CREDENTIAL_BYTES {
            return Err(TcpPunchConfigError::CredentialTooLarge {
                len: self.credential.len(),
            });
        }
        if !sockets::valid(self.rendezvous) {
            return Err(TcpPunchConfigError::InvalidRendezvous);
        }
        if self.bind.is_ipv4() != self.rendezvous.is_ipv4() {
            return Err(TcpPunchConfigError::BindFamilyMismatch);
        }
        if self.bind.ip().is_multicast() {
            return Err(TcpPunchConfigError::MulticastBind);
        }
        if self.candidate_binds.len() > MAX_CANDIDATE_BINDS {
            return Err(TcpPunchConfigError::TooManyCandidateBinds {
                len: self.candidate_binds.len(),
            });
        }
        if let Some(index) = self
            .candidate_binds
            .iter()
            .position(|bind| bind.ip().is_multicast())
        {
            return Err(TcpPunchConfigError::MulticastCandidateBind { index });
        }
        if self.advertised_candidates.len() > MAX_CANDIDATES {
            return Err(TcpPunchConfigError::TooManyAdvertisedCandidates {
                len: self.advertised_candidates.len(),
            });
        }
        if let Some(index) = self
            .advertised_candidates
            .iter()
            .position(|address| !sockets::valid(*address))
        {
            return Err(TcpPunchConfigError::InvalidAdvertisedCandidate { index });
        }
        Ok(())
    }
}

impl fmt::Debug for TcpPunchConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TcpPunchConfig")
            .field("local", &self.local)
            .field("bind", &self.bind)
            .field("rendezvous", &self.rendezvous)
            .field("key", &self.key)
            .field("peers", &self.peers)
            .field("policy", &self.policy)
            .field("candidate_binds", &self.candidate_binds)
            .field("advertised_candidates", &self.advertised_candidates)
            .field("gather_interfaces", &self.gather_interfaces)
            .field("max_peers", &self.max_peers)
            .field("queue_capacity", &self.queue_capacity)
            .finish_non_exhaustive()
    }
}

/// A [`TcpPunchConfig`] rule violation, naming the offending field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TcpPunchConfigError {
    /// `local` is not a valid native identity.
    Local(NodeIdError),
    /// The static `peers` list holds more than the 255 identities a
    /// registration can carry.
    PeersExceedWireLimit {
        /// Number of configured peers.
        peers: usize,
    },
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
    /// `bind` is a multicast address.
    MulticastBind,
    /// `candidate_binds` lists more than three additional sockets.
    TooManyCandidateBinds {
        /// Number of configured candidate binds.
        len: usize,
    },
    /// `candidate_binds[index]` is a multicast address.
    MulticastCandidateBind {
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

impl fmt::Display for TcpPunchConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Local(error) => write!(f, "local: {error}"),
            Self::PeersExceedWireLimit { peers } => write!(
                f,
                "peers lists {peers} identities, above the {MAX_PEERS}-identity wire bound"
            ),
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
            Self::MulticastBind => f.write_str("bind must not be a multicast address"),
            Self::TooManyCandidateBinds { len } => write!(
                f,
                "candidate_binds lists {len} sockets, above the {MAX_CANDIDATE_BINDS}-socket bound"
            ),
            Self::MulticastCandidateBind { index } => {
                write!(
                    f,
                    "candidate_binds[{index}] must not be a multicast address"
                )
            }
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

impl error::Error for TcpPunchConfigError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Self::Local(error) | Self::Peer { error, .. } => Some(error),
            _ => None,
        }
    }
}

impl From<TcpPunchConfigError> for io::Error {
    fn from(error: TcpPunchConfigError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}

#[cfg(test)]
mod tests;
