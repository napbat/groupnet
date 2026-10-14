use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use groupnet_core::{Command, Config, GroupId, NetStats, NodeId, Role};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{broadcast, mpsc, watch};

use crate::clock::core_millis;
use crate::driver::{
    Event, GroupEvent, GroupViews, MembersSnapshot, MetaSnapshot, NodeEntriesSnapshot,
    StatusesSnapshot,
};

mod entries;
mod messaging;
mod roster;

pub use entries::{
    EntryBudget, EntryInspectionError, EntryInspectionLimits, EntryInspectionLimitsError,
    EntryMutationError, EntryMutationLimits, EntryMutationLimitsError, EntryRemoval, EntryRevision,
    EntryWrite, InspectedEntries, InspectedEntry, InspectedPair, InspectedPairEntry,
};

/// Why a local command was not enqueued into the group actor. Nothing was
/// applied either way.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommandRejected {
    /// The actor's bounded inbox is full (sustained overload). Retry after a
    /// beat; the write is state, so a later one carries the same intent.
    InboxFull,
    /// The group actor has shut down (its node closed or the group was left).
    /// Retrying cannot succeed; treat the group as gone.
    GroupClosed,
}

impl CommandRejected {
    /// Classifies a refused send into the actor's bounded inbox.
    pub(crate) fn from_send<T>(error: &TrySendError<T>) -> Self {
        match error {
            TrySendError::Full(_) => Self::InboxFull,
            TrySendError::Closed(_) => Self::GroupClosed,
        }
    }
}

impl fmt::Display for CommandRejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InboxFull => "group actor inbox full",
            Self::GroupClosed => "group actor closed",
        })
    }
}

impl std::error::Error for CommandRejected {}

/// A membership snapshot exceeded a caller's allocation bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BoundedRosterError {
    /// The observed roster has more identities than the caller can retain.
    TooManyMembers {
        /// Observed members, including the local node.
        members: usize,
        /// The caller's bound.
        max: usize,
    },
    /// At least one observed identity exceeds the caller's byte limit.
    IdentityTooLong {
        /// Bytes in the first identity over the bound.
        bytes: usize,
        /// The caller's bound.
        max: usize,
    },
}

impl fmt::Display for BoundedRosterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyMembers { members, max } => {
                write!(f, "roster has {members} members, above the bound of {max}")
            }
            Self::IdentityTooLong { bytes, max } => {
                write!(
                    f,
                    "member identity has {bytes} bytes, above the bound of {max}"
                )
            }
        }
    }
}

impl std::error::Error for BoundedRosterError {}

/// What this node believes its group's leadership to be: the adopted
/// `(epoch, host)` pair, and the part this node plays in it.
///
/// Read it with [`Group::leadership`], which is a lock-free `watch` snapshot
/// exactly like [`Group::coordinator`] — and republished by the driver every
/// time the engine changes its belief. `Leadership { epoch: 0, host: None,
/// role: Role::Follower }` before anything has been elected, and *forever* in
/// an [`Eventual`](groupnet_core::GroupMode::Eventual) group, which runs no
/// election at all.
///
/// **Observer-local, like every other read here.** During a partition two
/// nodes legitimately report different pairs; the engine's epoch-major fencing
/// order decides which one survives the heal. A `None` host at a non-zero
/// epoch means the group is believed hostless *at that epoch* (a lease lapsed,
/// or the incumbent stepped down) — the epoch is kept so a later pair still
/// fences against it.
///
/// This is not the [`coordinator`](Group::coordinator). The coordinator is
/// derived from the membership view and is never authoritative; the host is an
/// epoch-fenced authority that only a [`Hosted`](groupnet_core::GroupMode::Hosted)
/// group elects.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Leadership {
    /// The epoch of the adopted pair. Monotone per observer: it never
    /// regresses, and a `None` host does not reset it.
    pub epoch: u64,
    /// The host of that epoch, or `None` if the group is believed hostless.
    pub host: Option<NodeId>,
    /// The part the local node plays — derived here, observer-locally, from
    /// the pair itself: [`Role::Host`] exactly when [`host`](Self::host) names
    /// this node, [`Role::Follower`] otherwise.
    ///
    /// **[`Role::Claimant`] never surfaces at the runtime layer in M1**, by
    /// design: a claim is a bid, not authority. It changes nobody's belief —
    /// not even the claimant's — until its settle window closes and it
    /// activates, and a consumer that could observe `Claimant` here would be
    /// tempted to read a standing claim as licence to serve, which is exactly
    /// what the settle window exists to withhold. The engine still reports it
    /// through [`GroupEngine::role`](groupnet_core::GroupEngine::role), where
    /// the simulator asserts on it.
    pub role: Role,
}

impl Leadership {
    /// Derives `local`'s view of an adopted `(epoch, host)` pair.
    ///
    /// The single place the role is computed, so the boot seed in `node.rs`
    /// and the driver's republish can never drift apart about what counts as
    /// hosting.
    pub(crate) fn observed(epoch: u64, host: Option<NodeId>, local: &NodeId) -> Self {
        let role = if host.as_ref() == Some(local) {
            Role::Host
        } else {
            Role::Follower
        };
        Self { epoch, host, role }
    }
}

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
/// ([`coordinator`](Self::coordinator), [`is_coordinator`](Self::is_coordinator),
/// [`leadership`](Self::leadership)) are lock-free snapshots via a `watch`
/// channel.
#[derive(Debug, Clone)]
pub struct Group {
    id: GroupId,
    local: NodeId,
    /// The effective protocol config the owning node was built with, shared
    /// (never copied per handle) so [`config`](Self::config) can hand out a
    /// borrow.
    config: Arc<Config>,
    /// The node's logical-time origin — the same instant the driver measures
    /// the engine's [`Time`](groupnet_core::Time) from, so a published
    /// `status_since` stamp and a read taken here share one clock.
    start: Instant,
    tx: mpsc::Sender<Event>,
    messaging: crate::messaging::GroupMessaging,
    coord_rx: watch::Receiver<Option<NodeId>>,
    leadership_rx: watch::Receiver<Leadership>,
    meta_rx: watch::Receiver<MetaSnapshot>,
    members_rx: watch::Receiver<MembersSnapshot>,
    statuses_rx: watch::Receiver<StatusesSnapshot>,
    entries_rx: watch::Receiver<NodeEntriesSnapshot>,
    net_stats_rx: watch::Receiver<NetStats>,
    events_tx: broadcast::Sender<GroupEvent>,
}

impl Group {
    pub(crate) fn new(
        id: GroupId,
        local: NodeId,
        config: Arc<Config>,
        start: Instant,
        tx: mpsc::Sender<Event>,
        views: GroupViews,
        messaging: crate::messaging::GroupMessaging,
    ) -> Self {
        Self {
            id,
            local,
            config,
            start,
            tx,
            messaging,
            coord_rx: views.coordinator,
            leadership_rx: views.leadership,
            meta_rx: views.metadata,
            members_rx: views.members,
            statuses_rx: views.statuses,
            entries_rx: views.entries,
            net_stats_rx: views.net_stats,
            events_tx: views.events,
        }
    }

    /// The entries watch, for the node-level peer-address sync task.
    pub(crate) fn entries_watch(&self) -> watch::Receiver<NodeEntriesSnapshot> {
        self.entries_rx.clone()
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

    /// Stable local member identity used for actor-owned entries.
    #[must_use]
    pub fn local_node(&self) -> &NodeId {
        &self.local
    }

    /// The **effective** protocol config this node is running — the
    /// builder-applied values, not [`Config::default`].
    ///
    /// Read failure-detector timings from here, never from the defaults: a
    /// consumer that sizes a trust window off
    /// [`Config::detection_window_ms`] against the defaults silently breaks
    /// the moment a deployment retunes its probe/suspect timings, which is
    /// exactly the drift this accessor exists to close.
    ///
    /// ```no_run
    /// # fn demo(group: &groupnet_runtime::Group) {
    /// let window = group.config().detection_window_ms(group.members().len());
    /// # let _ = window;
    /// # }
    /// ```
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
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

    /// What this node believes the group's epoch-fenced leadership to be — see
    /// [`Leadership`] for the (deliberately narrow) guarantees.
    ///
    /// A lock-free snapshot of the same `watch` shape as
    /// [`coordinator`](Self::coordinator), republished whenever the engine
    /// changes its adopted pair. An
    /// [`Eventual`](groupnet_core::GroupMode::Eventual) group never elects, so
    /// this reads `Leadership { epoch: 0, host: None, role: Role::Follower }`
    /// for its whole life — opt a group in with
    /// [`Node::join_group_with`](crate::Node::join_group_with).
    ///
    /// ```no_run
    /// # fn demo(group: &groupnet_runtime::Group) {
    /// use groupnet_core::Role;
    /// let lead = group.leadership();
    /// if lead.role == Role::Host {
    ///     // We hold the group for `lead.epoch` — until our lease lapses or a
    ///     // higher pair fences us. Stamp writes with the epoch; never assume
    ///     // it is still current by the time they land.
    /// }
    /// # }
    /// ```
    #[must_use]
    pub fn leadership(&self) -> Leadership {
        self.leadership_rx.borrow().clone()
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
    /// per key and gossiped; `ttl` (if `Some`) makes every receiver expire
    /// the entry that long after last adopting it — refresh by re-setting.
    ///
    /// The core's clock is whole milliseconds: a sub-millisecond remainder
    /// rounds up to the next millisecond and a TTL beyond `u64::MAX`
    /// milliseconds saturates there.
    ///
    /// # Errors
    /// [`CommandRejected`] if the group actor's bounded inbox is full or the
    /// actor has shut down; the write was not enqueued.
    pub fn set_entry(
        &self,
        key: impl Into<String>,
        value: impl Into<Vec<u8>>,
        ttl: Option<Duration>,
    ) -> Result<(), CommandRejected> {
        self.tx
            .try_send(Event::Local(Command::SetLocalEntry {
                key: key.into(),
                value: value.into(),
                ttl_ms: ttl.map(core_millis),
            }))
            .map_err(|error| CommandRejected::from_send(&error))
    }

    /// Delete one key of this node's state (a versioned tombstone disseminates
    /// so every peer drops it).
    ///
    /// # Errors
    /// [`CommandRejected`] if the group actor's bounded inbox is full or the
    /// actor has shut down; the delete was not enqueued.
    pub fn delete_entry(&self, key: impl Into<String>) -> Result<(), CommandRejected> {
        self.tx
            .try_send(Event::Local(Command::DeleteLocalEntry { key: key.into() }))
            .map_err(|error| CommandRejected::from_send(&error))
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

    /// Cumulative anti-entropy traffic counters for this group on this node.
    /// The ratio worth watching at scale is
    /// `digest_summaries_listed / digests_built` — with delta digests it
    /// tracks recent churn, not membership size (see [`NetStats`]).
    #[must_use]
    pub fn net_stats(&self) -> NetStats {
        *self.net_stats_rx.borrow()
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

#[cfg(test)]
mod tests {
    use std::error::Error;

    use tokio::sync::mpsc;

    use super::CommandRejected;

    #[test]
    fn a_refused_send_names_why_it_was_refused() {
        let (tx, rx) = mpsc::channel(1);
        tx.try_send(()).expect("one slot");
        let full = CommandRejected::from_send(&tx.try_send(()).unwrap_err());
        assert_eq!(full, CommandRejected::InboxFull);
        drop(rx);
        let closed = CommandRejected::from_send(&tx.try_send(()).unwrap_err());
        assert_eq!(closed, CommandRejected::GroupClosed);
        assert_eq!(closed.to_string(), "group actor closed");
        assert!(closed.source().is_none());
    }
}
