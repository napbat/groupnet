//! Adjacent-peer admission, cost and frame bound of one link, and their
//! intrinsic validation.

use std::collections::HashSet;
use std::num::{NonZeroU32, NonZeroUsize};
use std::{error, fmt, io};

use groupnet_core::NodeId;

use crate::{MAX_NODE_ID_BYTES, NodeIdError, validate_node_id};

/// Admission, cost, and frame bounds for one adjacent-peer link.
///
/// Cost and MTU are nonzero by type. [`validate`](Self::validate) checks the
/// configured peer list; [`validate_for`](Self::validate_for) adds the rule
/// that ties it to the binding node. Every link provider runs
/// `validate_for` before opening any socket or listener. A router applies its
/// own bounds (route capacity, the MTU its fragment header and frame ceiling
/// need) when the link registers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LinkConfig {
    /// Admitted adjacent peers; routing rejects other sources. Empty for a
    /// link whose peers are admitted dynamically through a session registry.
    /// Default: none.
    pub peers: Vec<NodeId>,
    /// Path cost. Default: 1.
    pub cost: NonZeroU32,
    /// Maximum physical frame size supported by the link. Default: 65,000.
    pub mtu: NonZeroUsize,
}

impl Default for LinkConfig {
    fn default() -> Self {
        Self {
            peers: Vec::new(),
            cost: NonZeroU32::MIN,
            mtu: NonZeroUsize::new(65_000).expect("nonzero literal"),
        }
    }
}

impl LinkConfig {
    /// An equal-cost link to `peers`, supporting frames up to 65,000 bytes.
    #[must_use]
    pub fn new(peers: Vec<NodeId>) -> Self {
        Self {
            peers,
            ..Self::default()
        }
    }

    /// Checks the peer list: every identity valid and listed once.
    ///
    /// # Errors
    /// Returns the [`LinkConfigError`] naming the first offending entry.
    pub fn validate(&self) -> Result<(), LinkConfigError> {
        let mut seen = HashSet::with_capacity(self.peers.len());
        for (index, peer) in self.peers.iter().enumerate() {
            validate_node_id(peer, MAX_NODE_ID_BYTES)
                .map_err(|error| LinkConfigError::Peer { index, error })?;
            if !seen.insert(peer) {
                return Err(LinkConfigError::DuplicatePeer { index });
            }
        }
        Ok(())
    }

    /// [`validate`](Self::validate), plus the rule that a link never lists
    /// the node it binds for as its own adjacent peer.
    ///
    /// # Errors
    /// Returns the [`LinkConfigError`] naming the first offending entry.
    pub fn validate_for(&self, local: &NodeId) -> Result<(), LinkConfigError> {
        self.validate()?;
        match self.peers.iter().position(|peer| peer == local) {
            Some(index) => Err(LinkConfigError::LocalPeer { index }),
            None => Ok(()),
        }
    }
}

/// A [`LinkConfig`] rule violation, naming the offending `peers` entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LinkConfigError {
    /// `peers[index]` is not a valid routing identity.
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
    /// `peers[index]` is the binding node's own identity.
    LocalPeer {
        /// Position of the local identity.
        index: usize,
    },
}

impl fmt::Display for LinkConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Peer { index, error } => write!(f, "peers[{index}]: {error}"),
            Self::DuplicatePeer { index } => {
                write!(f, "peers[{index}] repeats an earlier identity")
            }
            Self::LocalPeer { index } => {
                write!(f, "peers[{index}] is the binding node's own identity")
            }
        }
    }
}

impl error::Error for LinkConfigError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Self::Peer { error, .. } => Some(error),
            Self::DuplicatePeer { .. } | Self::LocalPeer { .. } => None,
        }
    }
}

impl From<LinkConfigError> for io::Error {
    fn from(error: LinkConfigError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}

/// Why a link provider refuses the node that binds it, beyond its
/// [`LinkConfig`]: the provider was built for another identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LinkBindError {
    /// The provider's endpoint or configuration names `configured` as its
    /// local identity, but `binding` is binding it.
    IdentityMismatch {
        /// The local identity the provider was built with.
        configured: NodeId,
        /// The identity of the node binding the provider.
        binding: NodeId,
    },
}

impl fmt::Display for LinkBindError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IdentityMismatch {
                configured,
                binding,
            } => write!(
                f,
                "link built for {configured} cannot bind for node {binding}"
            ),
        }
    }
}

impl error::Error for LinkBindError {}

impl From<LinkBindError> for io::Error {
    fn from(error: LinkBindError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}
