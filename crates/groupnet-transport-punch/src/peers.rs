//! Static versus dynamic peer admission, shared by every native configuration.

use groupnet_core::NodeId;

/// Which identities a native endpoint or rendezvous admits.
///
/// Each mode carries only its own data: a static policy owns its allowlist,
/// while a dynamic one has none to ignore.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PeerPolicy {
    /// Exactly these identities. An endpoint registers and connects to them
    /// alone; a rendezvous ignores every other identity and admits a listed
    /// one only after its application admission policy also accepts it,
    /// judging the same credential a dynamic rendezvous would. An empty list
    /// admits no one.
    Static(Vec<NodeId>),
    /// Identities discovered at run time: an endpoint connects to every peer
    /// the rendezvous admits, and a rendezvous admits whoever its application
    /// admission policy accepts.
    Dynamic,
}

impl PeerPolicy {
    /// The static allowlist, or no identities for a dynamic policy.
    #[must_use]
    pub fn static_peers(&self) -> &[NodeId] {
        match self {
            Self::Static(peers) => peers,
            Self::Dynamic => &[],
        }
    }

    /// Whether peers are discovered dynamically.
    #[must_use]
    pub const fn is_dynamic(&self) -> bool {
        matches!(self, Self::Dynamic)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_mode_exposes_only_its_own_peers() {
        let peers = vec![NodeId::new("a"), NodeId::new("b")];
        let fixed = PeerPolicy::Static(peers.clone());
        assert!(!fixed.is_dynamic());
        assert_eq!(fixed.static_peers(), peers.as_slice());
        assert!(PeerPolicy::Dynamic.is_dynamic());
        assert_eq!(PeerPolicy::Dynamic.static_peers(), &[] as &[NodeId]);
        assert!(!PeerPolicy::Static(Vec::new()).is_dynamic());
    }
}
