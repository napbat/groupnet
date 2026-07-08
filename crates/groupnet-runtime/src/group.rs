use groupnet_core::{Command, GroupId, NodeId};
use tokio::sync::{mpsc, watch};

use crate::driver::{Event, MembersSnapshot, MetaSnapshot};

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
/// Cheap to hold; all state lives in the group's actor task. Reads
/// ([`coordinator`](Self::coordinator), [`is_coordinator`](Self::is_coordinator))
/// are lock-free snapshots via a `watch` channel.
#[derive(Debug)]
pub struct Group {
    id: GroupId,
    local: NodeId,
    tx: mpsc::UnboundedSender<Event>,
    coord_rx: watch::Receiver<Option<NodeId>>,
    meta_rx: watch::Receiver<MetaSnapshot>,
    members_rx: watch::Receiver<MembersSnapshot>,
}

impl Group {
    pub(crate) fn new(
        id: GroupId,
        local: NodeId,
        tx: mpsc::UnboundedSender<Event>,
        coord_rx: watch::Receiver<Option<NodeId>>,
        meta_rx: watch::Receiver<MetaSnapshot>,
        members_rx: watch::Receiver<MembersSnapshot>,
    ) -> Self {
        Self {
            id,
            local,
            tx,
            coord_rx,
            meta_rx,
            members_rx,
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

    /// Reads a metadata value as this node currently sees it. Values propagate
    /// via gossip and are merged by last-writer-wins, so a freshly-written value
    /// on another node appears here after it converges.
    #[must_use]
    pub fn metadata(&self, key: &str) -> Option<String> {
        self.meta_rx.borrow().get(key).cloned()
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
}
