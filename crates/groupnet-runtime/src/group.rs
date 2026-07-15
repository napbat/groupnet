use groupnet_core::{Command, GroupId, NodeId, Status};
use tokio::sync::{broadcast, mpsc, watch};

use crate::driver::{
    Event, GroupEvent, GroupViews, MembersSnapshot, MetaSnapshot, NodeEntriesSnapshot,
    StatusesSnapshot,
};

/// A local command could not be enqueued: the group actor's bounded inbox is
/// full (sustained overload) or the actor has shut down. Callers retry after a
/// beat or treat it as the group being gone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommandRejected;

impl std::fmt::Display for CommandRejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("group actor inbox full or closed")
    }
}

impl std::error::Error for CommandRejected {}

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
    tx: mpsc::Sender<Event>,
    coord_rx: watch::Receiver<Option<NodeId>>,
    meta_rx: watch::Receiver<MetaSnapshot>,
    members_rx: watch::Receiver<MembersSnapshot>,
    statuses_rx: watch::Receiver<StatusesSnapshot>,
    entries_rx: watch::Receiver<NodeEntriesSnapshot>,
    events_tx: broadcast::Sender<GroupEvent>,
}

impl Group {
    pub(crate) fn new(
        id: GroupId,
        local: NodeId,
        tx: mpsc::Sender<Event>,
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
            entries_rx: views.entries,
            events_tx: views.events,
        }
    }

    /// Subscribe to this group's change events. Bounded: a slow subscriber
    /// observes `Lagged` and must resync from the snapshot reads (which are
    /// always current) — the stream is an edge trigger, not a reliable log.
    #[must_use]
    pub fn events(&self) -> broadcast::Receiver<GroupEvent> {
        self.events_tx.subscribe()
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

    /// Replaces this node's single-blob state (a shim over the keyed model's
    /// reserved `~blob` key). Best-effort under backpressure, like
    /// [`sync`](Self::sync); prefer [`set_entry`](Self::set_entry).
    pub fn set_state(&self, state: impl Into<Vec<u8>>) {
        let _ = self
            .tx
            .try_send(Event::Local(Command::SetLocalState(state.into())));
    }

    /// Reads the single-blob (`~blob`) state `node` last advertised.
    #[must_use]
    pub fn node_state(&self, node: &NodeId) -> Option<Vec<u8>> {
        self.node_entry(node, groupnet_core::GroupEngine::BLOB_KEY)
    }

    /// Set one key of this node's app-defined state. Independently versioned
    /// per key and gossiped; `ttl_ms` (if `Some`) makes every receiver expire
    /// the entry that long after last adopting it — refresh by re-setting.
    pub fn set_entry(
        &self,
        key: impl Into<String>,
        value: impl Into<Vec<u8>>,
        ttl_ms: Option<u64>,
    ) -> Result<(), CommandRejected> {
        self.tx
            .try_send(Event::Local(Command::SetLocalEntry {
                key: key.into(),
                value: value.into(),
                ttl_ms,
            }))
            .map_err(|_| CommandRejected)
    }

    /// Delete one key of this node's state (a versioned tombstone disseminates
    /// so every peer drops it).
    pub fn delete_entry(&self, key: impl Into<String>) -> Result<(), CommandRejected> {
        self.tx
            .try_send(Event::Local(Command::DeleteLocalEntry { key: key.into() }))
            .map_err(|_| CommandRejected)
    }

    /// One key of `node`'s state, as this node currently sees it.
    #[must_use]
    pub fn node_entry(&self, node: &NodeId, key: &str) -> Option<Vec<u8>> {
        self.entries_rx.borrow().get(node)?.get(key).cloned()
    }

    /// A snapshot of `node`'s live state entries.
    #[must_use]
    pub fn node_entries(&self, node: &NodeId) -> Vec<(String, Vec<u8>)> {
        self.entries_rx
            .borrow()
            .get(node)
            .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default()
    }

    /// A snapshot of every node's live state entries (the full map, one
    /// `Arc` clone — cheap).
    #[must_use]
    pub fn all_entries(&self) -> NodeEntriesSnapshot {
        self.entries_rx.borrow().clone()
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
            // Best-effort under backpressure (bounded inbox): metadata syncs
            // are periodic/idempotent at every call site, so a rare drop under
            // overload re-converges on the next round.
            let _ = self.tx.try_send(Event::Local(cmd));
        }
    }

    /// Leaves the group (best-effort).
    pub fn leave(&self) {
        let _ = self.tx.try_send(Event::Local(Command::Leave));
    }

    /// Introduce a peer learned out-of-band (e.g. from an external roster / service
    /// discovery) so this node starts gossiping to it without waiting to be contacted
    /// first. Idempotent; complements build-time [`seed`](crate::NodeBuilder::seed).
    pub fn add_peer(&self, node: NodeId) {
        let _ = self.tx.try_send(Event::Local(Command::AddPeer(node)));
    }

    /// The command channel into this group's actor (for internal wiring, e.g.
    /// publishing coordinator identity into the routing group).
    pub(crate) fn command_sender(&self) -> mpsc::Sender<Event> {
        self.tx.clone()
    }
}
