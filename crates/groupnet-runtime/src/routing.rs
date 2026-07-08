//! The inter-group routing table.
//!
//! Groupnet maintains a cluster-wide, eventually-consistent map of *which group
//! owns which resource* and *which node coordinates each group*, so any node can
//! forward a request to the right owner without global consensus.
//!
//! It is built by reusing the group machinery: every node joins a reserved
//! internal system group whose LWW metadata *is* the routing table. Ownership
//! claims and coordinator identities are just metadata keys, disseminated and
//! merged by the same gossip + last-writer-wins path as everything else. Reads
//! are lock-free snapshots.

use groupnet_core::{GroupId, NodeId};

use crate::driver::coordinator_key;
use crate::group::Group;

fn owner_key(resource: &str) -> String {
    format!("owner:{resource}")
}

/// A handle to the cluster-wide routing table.
///
/// Obtain one with [`Node::routing`](crate::Node::routing). Lookups reflect the
/// routing group's converged view; a freshly published claim appears once it has
/// gossiped to this node.
#[derive(Debug, Clone)]
pub struct Routing {
    group: Group,
}

impl Routing {
    pub(crate) fn new(group: Group) -> Self {
        Self { group }
    }

    /// Records that `group` owns `resource`. Intended to be called by that
    /// group's coordinator; it is disseminated and resolved by last-writer-wins.
    pub fn claim(&self, resource: &str, group: &GroupId) {
        let owner = group.to_string();
        let key = owner_key(resource);
        self.group.sync(move |ctx| ctx.update_metadata(key, owner));
    }

    /// Which group currently owns `resource`, if any node has claimed it.
    #[must_use]
    pub fn owner(&self, resource: &str) -> Option<GroupId> {
        self.group.metadata(&owner_key(resource)).map(GroupId::new)
    }

    /// The coordinator node of `group`, as last announced into the routing
    /// table by that group's coordinator.
    #[must_use]
    pub fn coordinator_of(&self, group: &GroupId) -> Option<NodeId> {
        self.group
            .metadata(&coordinator_key(group))
            .map(NodeId::new)
    }

    /// The node to send a request for `resource` to: the coordinator of the
    /// group that owns it. `None` if ownership or that group's coordinator isn't
    /// known here yet.
    #[must_use]
    pub fn route(&self, resource: &str) -> Option<NodeId> {
        self.owner(resource)
            .and_then(|group| self.coordinator_of(&group))
    }
}
