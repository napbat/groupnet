//! Rendezvous admission policy and operational resource limits.

use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;

use groupnet_core::NodeId;
use groupnet_transport::admission::{Admission, OpenAdmission};

use super::super::{DEFAULT_MAX_PEERS, NetworkKey, invalid, validate_names_with_limit};

/// Operational rendezvous capacities, independent of wire and authentication bounds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RendezvousLimits {
    /// Maximum established or admission-pending peer identities (must be nonzero).
    pub max_peers: usize,
    /// Maximum evictable, unproven address challenges (must be nonzero).
    pub max_challenges: usize,
    /// Maximum concurrent application policy evaluations (must be nonzero).
    pub max_pending_admissions: usize,
}

impl Default for RendezvousLimits {
    fn default() -> Self {
        Self {
            max_peers: DEFAULT_MAX_PEERS,
            max_challenges: DEFAULT_MAX_PEERS,
            max_pending_admissions: 32,
        }
    }
}

impl RendezvousLimits {
    pub(super) fn validate(self) -> std::io::Result<()> {
        if self.max_peers == 0 || self.max_challenges == 0 || self.max_pending_admissions == 0 {
            return Err(invalid("UDP rendezvous capacities must be nonzero"));
        }
        Ok(())
    }
}

/// Socket, fabric authentication, application admission, and rendezvous capacities.
#[derive(Clone)]
pub struct RendezvousConfig {
    /// UDP socket bind address (multicast is rejected).
    pub bind: SocketAddr,
    /// Optional trusted-fabric key; keyed traffic never downgrades.
    pub key: Option<NetworkKey>,
    /// Fixed identity allowlist, or `None` for dynamic application admission.
    pub peers: Option<Vec<NodeId>>,
    /// Application admission policy, evaluated only after return-routability.
    pub admission: Arc<dyn Admission>,
    /// Operational capacities; rate limits and challenge/session deadlines stay fixed.
    pub limits: RendezvousLimits,
}

impl fmt::Debug for RendezvousConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RendezvousConfig")
            .field("bind", &self.bind)
            .field("key", &self.key)
            .field("peers", &self.peers)
            .field("admission", &"application policy")
            .field("limits", &self.limits)
            .finish()
    }
}

impl RendezvousConfig {
    /// Creates keyed rendezvous configuration with a fixed identity allowlist.
    #[must_use]
    pub fn new(bind: SocketAddr, key: NetworkKey, peers: Vec<NodeId>) -> Self {
        Self {
            bind,
            key: Some(key),
            peers: Some(peers),
            admission: Arc::new(OpenAdmission),
            limits: RendezvousLimits::default(),
        }
    }

    /// Creates explicitly keyless dynamic discovery with open application admission.
    #[must_use]
    pub fn open(bind: SocketAddr) -> Self {
        Self::with_admission(bind, None, Arc::new(OpenAdmission))
    }

    /// Creates dynamic discovery with application admission and optional fabric HMAC.
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
            limits: RendezvousLimits::default(),
        }
    }

    pub(super) fn validate(&self) -> std::io::Result<()> {
        self.limits.validate()?;
        if self.bind.ip().is_multicast() {
            return Err(invalid("multicast rendezvous bind is not supported"));
        }
        if let Some(peers) = &self.peers {
            validate_names_with_limit(peers, self.limits.max_peers)?;
        }
        Ok(())
    }
}
