use groupnet_core::{Command, GroupId, NodeId, Status};
use tokio::sync::{mpsc, watch};

use crate::driver::{
    Event, GroupViews, MembersSnapshot, MetaSnapshot, NodeStatesSnapshot, StatusesSnapshot,
};

/// A transactional batch of shard-local operations, built inside
/// [`Group::sync`]. Operations are collected and handed to the group actor to
/// apply in order.
#[derive(Debug, Default)]
pub struct SyncCtx {
    cmds: Vec<Command>,
}

impl SyncCtx {
    /// Stages a metadata write. Applied when the enclosing `sync` returns.
    pub fn update_metadata(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.cmds.push(Command::UpdateMetadata {
            key: key.into(),
            value: value.into(),
        });
    }
}

/// A handle to this node's participation in one group.
///
/// Cheap to clone and hold; all state lives in the group's actor task. Reads
/// ([`coordinator`](Self::coordinator), [`is_coordinator`](Self::is_coordinator))
/// are lock-free snapshots via a `watch` channel.
#[derive(Debug, Clone)]
pub struct Group {
    id: GroupId,
    local: NodeId,
    tx: mpsc::UnboundedSender<Event>,
    coord_rx: watch::Receiver<Option<NodeId>>,
    meta_rx: watch::Receiver<MetaSnapshot>,
    members_rx: watch::Receiver<MembersSnapshot>,
    statuses_rx: watch::Receiver<StatusesSnapshot>,
    node_states_rx: watch::Receiver<NodeStatesSnapshot>,
}

impl Group {
    pub(crate) fn new(
        id: GroupId,
        local: NodeId,
        tx: mpsc::UnboundedSender<Event>,
        views: GroupViews,
    ) -> Self {
        Self {
            id,
            local,
            tx,
            coord_rx: views.coordinator,
            meta_rx: views.metadata,
            members_rx: views.members,
            statuses_rx: views.statuses,
            node_states_rx: views.node_states,
        }
    }

    /// This group's id.
    #[must_use]
    pub fn id(&self) -> &GroupId {
        &self.id
    }

    /// The coordinator this node currently believes in, or `None` before the
    /// first convergence.
    #[must_use]
    pub fn coordinator(&self) -> Option<NodeId> {
        self.coord_rx.borrow().clone()
    }

    /// Whether the local node is currently the coordinator.
    #[must_use]
    pub fn is_coordinator(&self) -> bool {
        self.coord_rx.borrow().as_ref() == Some(&self.local)
    }

    /// The current live members (anything not `Dead`), in id order. Failed and
    /// departed nodes drop out once failure detection converges.
    #[must_use]
    pub fn members(&self) -> Vec<NodeId> {
        self.members_rx.borrow().as_ref().clone()
    }

    /// The status ([`Status::Alive`]/`Suspect`/`Dead`) this node currently perceives
    /// for `node`, or `None` if it is unknown. Unlike [`members`](Self::members)
    /// (the not-`Dead` set), this exposes the Alive/Suspect distinction a router
    /// needs to route *around* a suspected peer before it is declared dead.
    #[must_use]
    pub fn member_status(&self, node: &NodeId) -> Option<Status> {
        self.statuses_rx.borrow().get(node).copied()
    }

    /// A snapshot of every known member and its status, in id order (includes
    /// `Suspect` members and not-yet-reaped `Dead` tombstones).
    #[must_use]
    pub fn statuses(&self) -> Vec<(NodeId, Status)> {
        self.statuses_rx
            .borrow()
            .iter()
            .map(|(n, s)| (n.clone(), *s))
            .collect()
    }

    /// Reads a metadata value as this node currently sees it. Values propagate
    /// via gossip and are merged by last-writer-wins, so a freshly-written value
    /// on another node appears here after it converges.
    #[must_use]
    pub fn metadata(&self, key: &str) -> Option<String> {
        self.meta_rx.borrow().get(key).cloned()
    }

    /// Replaces this node's app-defined per-node state (capacity weight,
    /// readiness, replication progress — whatever the application encodes). It
    /// is gossiped to every peer and readable there via [`node_state`](Self::node_state).
    pub fn set_state(&self, state: impl Into<Vec<u8>>) {
        let _ = self
            .tx
            .send(Event::Local(Command::SetLocalState(state.into())));
    }

    /// Reads the app-defined state `node` last advertised, as this node sees it.
    #[must_use]
    pub fn node_state(&self, node: &NodeId) -> Option<Vec<u8>> {
        self.node_states_rx.borrow().get(node).cloned()
    }

    /// Runs a batch of shard-local operations against the group.
    ///
    /// The closure stages operations on the [`SyncCtx`]; they are enqueued to
    /// the group actor when it returns. This is fire-and-forget: it does not
    /// block on the operations being applied cluster-wide.
    pub fn sync<F: FnOnce(&mut SyncCtx)>(&self, f: F) {
        let mut ctx = SyncCtx::default();
        f(&mut ctx);
        for cmd in ctx.cmds {
            let _ = self.tx.send(Event::Local(cmd));
        }
    }

    /// Leaves the group (best-effort).
    pub fn leave(&self) {
        let _ = self.tx.send(Event::Local(Command::Leave));
    }

    /// Introduce a peer learned out-of-band (e.g. from an external roster / service
    /// discovery) so this node starts gossiping to it without waiting to be contacted
    /// first. Idempotent; complements build-time [`seed`](crate::NodeBuilder::seed).
    pub fn add_peer(&self, node: NodeId) {
        let _ = self.tx.send(Event::Local(Command::AddPeer(node)));
    }

    /// The command channel into this group's actor (for internal wiring, e.g.
    /// publishing coordinator identity into the routing group).
    pub(crate) fn command_sender(&self) -> mpsc::UnboundedSender<Event> {
        self.tx.clone()
    }
}
