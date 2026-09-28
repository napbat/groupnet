use std::any::Any;
use std::sync::Arc;
use std::time::{Duration, Instant};

use groupnet_core::{Command, Config, GroupId, NetStats, NodeId, Role, Status};
use tokio::sync::{broadcast, mpsc, oneshot, watch};

use crate::driver::{
    Event, GroupEvent, GroupViews, MembersSnapshot, MetaSnapshot, NodeEntriesSnapshot,
    StatusesSnapshot, now_since,
};

/// A local command could not be enqueued: the group actor's bounded inbox is
/// full (sustained overload) or the actor has shut down. Callers retry after a
/// beat or treat it as the group being gone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommandRejected;

/// A membership snapshot exceeded a caller's allocation bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BoundedRosterError {
    /// A zero count or identity-byte limit cannot describe a roster.
    InvalidLimit,
    /// The observed roster has more identities than the caller can retain.
    TooManyMembers,
    /// At least one observed identity exceeds the caller's byte limit.
    IdentityTooLong,
}

/// Finite actor-side observation limits, checked before cloning any entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EntryInspectionLimits {
    /// Maximum bytes in the requested key.
    pub max_key_bytes: usize,
    /// Maximum returned members, including the local node.
    pub max_members: usize,
    /// Maximum bytes in one member identity.
    pub max_member_bytes: usize,
    /// Maximum bytes in one entry value.
    pub max_value_bytes: usize,
    /// Maximum combined response charge, including the fixed result and
    /// per-entry record headers, identities, and value bytes.
    pub max_response_bytes: usize,
}

/// Exact actor-cut revision precondition for one local entry mutation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EntryRevision {
    /// Retained key revision, including a tombstone. An absent revision also
    /// binds `member` to prevent an old create after a publish and removal.
    pub key: Option<u64>,
    /// Native member state high-water at the observation cut.
    pub member: u64,
}

/// Actor-side byte limits for one conditional entry publication.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EntryMutationLimits {
    /// Maximum owned bytes in the scoped key.
    pub max_key_bytes: usize,
    /// Maximum owned bytes in its value.
    pub max_value_bytes: usize,
}

/// An owned admission guard carried through the actor queue and response.
///
/// A cancelled requester does not release its budget while the actor still
/// owns or allocates the reply. The runtime is independent of any particular
/// admission implementation.
pub trait EntryBudget: Any + Send {
    /// Maximum queued or response bytes already reserved by this owner.
    fn bytes(&self) -> usize;
}

/// One member and exactly one scoped entry from an actor-state cut.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InspectedEntry {
    /// Member identity.
    pub node: NodeId,
    /// Membership status at the same actor cut.
    pub status: Status,
    /// Entry bytes, if present and unexpired.
    pub value: Option<Vec<u8>>,
    /// Observer-local remaining TTL in milliseconds, if the entry has one.
    pub remaining_ttl_ms: Option<u64>,
}

/// One complete bounded actor-state cut with its monotonic sample instant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InspectedEntries {
    /// Local monotonic instant at or before the actor's expiry calculation.
    pub sampled_at: Instant,
    /// Complete known roster in node-id order, including absent entries.
    pub entries: Vec<InspectedEntry>,
}

/// One member and two scoped entries from the same actor-state cut.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InspectedPairEntry {
    /// Native member identity.
    pub node: NodeId,
    /// Native membership status.
    pub status: Status,
    /// SWIM refutation incarnation; the entry's boot token fences restarts.
    pub member_incarnation: u64,
    /// Native high-water version including deleted entries. Conditional local
    /// publication binds this value so an old absent-value create cannot
    /// revive after a newer publication and withdrawal.
    pub member_state_version: u64,
    /// First entry bytes, if present and unexpired.
    pub first: Option<Vec<u8>>,
    /// Retained first-entry revision, including an absent tombstone.
    pub first_version: Option<u64>,
    /// Native remaining TTL for the first entry at the sample.
    pub first_remaining_ttl_ms: Option<u64>,
    /// Second entry bytes, if present and unexpired.
    pub second: Option<Vec<u8>>,
    /// Retained second-entry revision, including an absent tombstone.
    pub second_version: Option<u64>,
    /// Native remaining TTL for the second entry at the sample.
    pub second_remaining_ttl_ms: Option<u64>,
}

/// Complete bounded two-entry roster at one observer-local actor instant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InspectedPair {
    /// Instant at or before the actor's native TTL calculation.
    pub sampled_at: Instant,
    /// Complete known roster in `NodeId` order, including absent entries.
    pub entries: Vec<InspectedPairEntry>,
}

/// A bounded actor-side inspection failed without returning a partial roster.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EntryInspectionError {
    /// Zero, inverted, or unrepresentable bound.
    InvalidLimit,
    /// The complete roster exceeded its count cap.
    TooManyMembers,
    /// One member identity exceeded its byte cap.
    IdentityTooLong,
    /// One present value exceeded its byte cap.
    ValueTooLong,
    /// The complete response exceeded the caller's owned budget.
    Capacity,
    /// The group actor is full or closed, or its reply was lost.
    Unavailable,
    /// The runtime could not recover its exact type-erased budget owner.
    Internal,
}

/// A confirmed local entry mutation failed or its outcome is unknown.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EntryMutationError {
    /// The key or value violates the caller's finite bounds.
    InvalidLimit,
    /// The actor inbox is full or closed, or its reply was lost. The caller
    /// must read back the exact value before deciding whether it applied.
    Unavailable,
    /// The actor did not apply a local set operation.
    NotApplied,
}

impl std::fmt::Display for CommandRejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("group actor inbox full or closed")
    }
}

impl std::error::Error for CommandRejected {}

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
    ) -> Self {
        Self {
            id,
            local,
            config,
            start,
            tx,
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
    /// use groupnet_runtime::Role;
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
        self.statuses_rx
            .borrow()
            .get(node)
            .map(|(status, _)| *status)
    }

    /// A snapshot of every known member and its status, in id order (includes
    /// `Suspect` members and not-yet-reaped `Dead` tombstones).
    #[must_use]
    pub fn statuses(&self) -> Vec<(NodeId, Status)> {
        self.statuses_rx
            .borrow()
            .iter()
            .map(|(n, (s, _))| (n.clone(), *s))
            .collect()
    }

    /// The status this node perceives for `node`, **and how long it has held
    /// that status uninterrupted** — the fencing-verdict primitive: "dead for
    /// long enough ⇒ safe to fence", "healthy for long enough ⇒ safe to
    /// unfence".
    ///
    /// The duration only restarts when the status *value* changes. Repeated
    /// gossip re-asserting the same status leaves it running, so
    /// `Some((Status::Dead, d))` with `d` past
    /// [`Config::detection_window_ms`] is a real "this observer has been sure
    /// for `d`" — not "the last message about it arrived `d` ago".
    ///
    /// **Observer-local, by construction.** This is *this* node's verdict from
    /// *its* probes and the gossip it received. Two nodes can honestly report
    /// different durations for the same peer, and a consumer that needs a
    /// cluster-wide verdict must combine observers itself (or anchor the
    /// durable act in a CAS-capable store, which is the pattern this feeds).
    ///
    /// **For the local node the duration is process lifetime, not group
    /// tenure**: this node has been `Alive` since its own logical origin, so a
    /// group joined an hour after boot still reports self as Alive for an
    /// hour. Nothing observes the local node into a status, so there is no
    /// observation to date it from. Fencing consumers read *peers*, where the
    /// stamp is observation-based and means what it says.
    ///
    /// **Reap horizon — the one sharp edge.** A `Dead` tombstone is removed
    /// `2 × dead_timeout_ms` after death, and from then on this returns
    /// `None`: the duration is only readable *inside* that horizon, so a
    /// long-departed node is indistinguishable from one never heard of. Two
    /// remedies, both fine:
    ///
    /// * raise [`Config::dead_timeout_ms`] above the longest verdict window
    ///   you need (it is already required to exceed the longest survivable
    ///   partition), or
    /// * treat `None` for a node you *know* was registered as "dead for at
    ///   least `2 × dead_timeout_ms`" — which is what the reap actually
    ///   proves, and is monotone in the safe direction for fencing.
    ///
    /// The duration is measured on the same logical clock the driver feeds the
    /// engine, so it is directly comparable to
    /// [`Config::detection_window_ms`]. Clamped at zero: a stamp cannot be in
    /// the future.
    #[must_use]
    pub fn status_held_for(&self, node: &NodeId) -> Option<(Status, Duration)> {
        let (status, since) = *self.statuses_rx.borrow().get(node)?;
        let now = now_since(self.start);
        Some((status, Duration::from_millis(now.0.saturating_sub(since.0))))
    }

    /// Every known member with its status **and how long this node has held
    /// that status for it**, in id order — the roster-shaped
    /// [`status_held_for`](Self::status_held_for), for a consumer that sweeps
    /// the whole membership rather than asking about one peer.
    ///
    /// One borrow of the published snapshot and one clock read for the whole
    /// roster, so every duration is measured against the same instant and the
    /// membership cannot shift between entries — unlike `status_held_for` in a
    /// loop, which takes a fresh borrow per node and can straddle a
    /// republish, listing a member at one moment and a peer at another. That
    /// consistency is the point for a fence maintainer (shardstore's, for
    /// instance) deciding a whole shard map in one pass.
    ///
    /// Includes `Suspect` members and not-yet-reaped `Dead` tombstones, and
    /// the local node — whose duration is process lifetime, with the same
    /// caveats (and the same reap horizon) that
    /// [`status_held_for`](Self::status_held_for) documents.
    ///
    /// **Policy stays with the caller.** This reports what is observed and for
    /// how long; which threshold counts as "dead long enough to fence", and
    /// what to do when it is met, is the consumer's decision and deliberately
    /// not encoded here.
    #[must_use]
    pub fn statuses_held(&self) -> Vec<(NodeId, Status, Duration)> {
        let now = now_since(self.start);
        self.statuses_rx
            .borrow()
            .iter()
            .map(|(node, (status, since))| {
                (
                    node.clone(),
                    *status,
                    Duration::from_millis(now.0.saturating_sub(since.0)),
                )
            })
            .collect()
    }

    /// A single coherent status-and-held-age snapshot with allocation bounds.
    ///
    /// This checks the count and every identity against the borrowed published
    /// view *before* cloning any identities. The local node counts toward
    /// `max_members`; callers can remove it after receiving the snapshot.
    ///
    /// # Errors
    /// Returns [`BoundedRosterError`] if a limit is zero or the observed view
    /// exceeds either limit. An error is not a truncated roster.
    pub fn statuses_held_bounded(
        &self,
        max_members: usize,
        max_member_bytes: usize,
    ) -> Result<Vec<(NodeId, Status, Duration)>, BoundedRosterError> {
        if max_members == 0 || max_member_bytes == 0 {
            return Err(BoundedRosterError::InvalidLimit);
        }
        let view = self.statuses_rx.borrow();
        if view.len() > max_members {
            return Err(BoundedRosterError::TooManyMembers);
        }
        if view
            .keys()
            .any(|node| node.as_str().len() > max_member_bytes)
        {
            return Err(BoundedRosterError::IdentityTooLong);
        }
        let now = now_since(self.start);
        Ok(view
            .iter()
            .map(|(node, (status, since))| {
                (
                    node.clone(),
                    *status,
                    Duration::from_millis(now.0.saturating_sub(since.0)),
                )
            })
            .collect())
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
    ///
    /// # Errors
    /// [`CommandRejected`] if the group actor's bounded inbox is full or the
    /// actor has shut down; the write was not enqueued.
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

    /// Applies a bounded local entry and waits until the actor has published
    /// the resulting local view. This confirms only local publication; gossip
    /// delivery and remote adoption remain advisory.
    ///
    /// A lost reply has an unknown outcome. Read back the exact scoped entry
    /// rather than issuing an unrelated claim with the same identity.
    ///
    /// # Errors
    /// Returns [`EntryMutationError`] for invalid bounds, actor overload or
    /// closure, a lost reply, or a rejected local application.
    pub async fn set_entry_confirmed<B: EntryBudget>(
        &self,
        key: impl Into<String>,
        value: impl Into<Vec<u8>>,
        ttl_ms: Option<u64>,
        max_key_bytes: usize,
        max_value_bytes: usize,
        budget: B,
    ) -> Result<B, EntryMutationError> {
        let key = key.into();
        let value = value.into();
        if key.is_empty()
            || max_key_bytes == 0
            || max_value_bytes == 0
            || key.len() > max_key_bytes
            || value.len() > max_value_bytes
            || key.capacity() > max_key_bytes
            || value.capacity() > max_value_bytes
            || key
                .capacity()
                .checked_add(value.capacity())
                .is_none_or(|bytes| bytes > budget.bytes())
        {
            return Err(EntryMutationError::InvalidLimit);
        }
        let (reply, response) = oneshot::channel();
        self.tx
            .try_send(Event::SetEntryConfirmed {
                key,
                value,
                ttl_ms,
                budget: Box::new(budget),
                reply,
            })
            .map_err(|_| EntryMutationError::Unavailable)?;
        let (applied, erased) = response
            .await
            .map_err(|_| EntryMutationError::Unavailable)?;
        let budget = erased
            .downcast::<B>()
            .map_err(|_| EntryMutationError::Unavailable)?;
        if applied {
            Ok(*budget)
        } else {
            Err(EntryMutationError::NotApplied)
        }
    }

    /// Publishes one bounded local entry only while its retained per-key
    /// revision still matches the actor cut. An absent key also binds the
    /// member's state-version high-water mark, so a delayed old create cannot
    /// revive after a newer create and withdrawal have both disappeared.
    /// Unrelated local writes do not reject a renewal with a retained key
    /// revision. The actor checks the condition after processing expiries.
    ///
    /// A `false` result confirms rejection without mutation. A lost reply is
    /// unknown and requires exact readback. The owned budget follows queued
    /// work and the response even if the requester is cancelled.
    ///
    /// # Errors
    /// Rejects invalid limits, an unavailable actor, or a lost reply.
    pub async fn set_entry_if_revision<B: EntryBudget>(
        &self,
        key: impl Into<String>,
        value: impl Into<Vec<u8>>,
        ttl_ms: Option<u64>,
        expected: EntryRevision,
        limits: EntryMutationLimits,
        budget: B,
    ) -> Result<(bool, B), EntryMutationError> {
        let key = key.into();
        let value = value.into();
        if key.is_empty()
            || limits.max_key_bytes == 0
            || limits.max_value_bytes == 0
            || key.len() > limits.max_key_bytes
            || value.len() > limits.max_value_bytes
            || key.capacity() > limits.max_key_bytes
            || value.capacity() > limits.max_value_bytes
            || expected.member == u64::MAX
            || expected.key == Some(u64::MAX)
            || key
                .capacity()
                .checked_add(value.capacity())
                .is_none_or(|bytes| bytes > budget.bytes())
        {
            return Err(EntryMutationError::InvalidLimit);
        }
        let (reply, response) = oneshot::channel();
        self.tx
            .try_send(Event::SetEntryIfRevision {
                key,
                value,
                ttl_ms,
                expected,
                budget: Box::new(budget),
                reply,
            })
            .map_err(|_| EntryMutationError::Unavailable)?;
        let (applied, erased) = response
            .await
            .map_err(|_| EntryMutationError::Unavailable)?;
        match erased.downcast::<B>() {
            Ok(budget) => Ok((applied, *budget)),
            Err(erased) => {
                drop(erased);
                Err(EntryMutationError::Unavailable)
            }
        }
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
            .map_err(|_| CommandRejected)
    }

    /// Deletes this node's entry only if its actor-visible bytes exactly match
    /// `expected`. A distinct renewal/incarnation must change those bytes for
    /// this to fence delayed withdrawals. A `false` result does not delete a
    /// newer claim. Publication precedes the successful reply.
    ///
    /// # Errors
    /// Returns [`EntryMutationError`] for invalid bounds or an unavailable
    /// actor/reply. A lost reply has an unknown outcome and needs readback.
    pub async fn delete_entry_if_value<B: EntryBudget>(
        &self,
        key: impl Into<String>,
        expected: impl Into<Vec<u8>>,
        max_key_bytes: usize,
        max_value_bytes: usize,
        budget: B,
    ) -> Result<(bool, B), EntryMutationError> {
        let key = key.into();
        let expected = expected.into();
        if key.is_empty()
            || max_key_bytes == 0
            || max_value_bytes == 0
            || key.len() > max_key_bytes
            || expected.len() > max_value_bytes
            || key.capacity() > max_key_bytes
            || expected.capacity() > max_value_bytes
            || key
                .capacity()
                .checked_add(expected.capacity())
                .is_none_or(|bytes| bytes > budget.bytes())
        {
            return Err(EntryMutationError::InvalidLimit);
        }
        let (reply, response) = oneshot::channel();
        self.tx
            .try_send(Event::DeleteEntryIfValue {
                key,
                expected,
                budget: Box::new(budget),
                reply,
            })
            .map_err(|_| EntryMutationError::Unavailable)?;
        let (deleted, erased) = response
            .await
            .map_err(|_| EntryMutationError::Unavailable)?;
        let budget = erased
            .downcast::<B>()
            .map_err(|_| EntryMutationError::Unavailable)?;
        Ok((deleted, *budget))
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

    /// Samples one key across the complete group roster inside the group
    /// actor, including each entry's observer-local remaining TTL.
    ///
    /// The budget moves into the bounded actor command before any response
    /// allocation. If this future is cancelled, that command or its reply
    /// keeps the budget until its cloned bytes are dropped.
    ///
    /// # Errors
    /// Rejects invalid limits, a full/closed actor, or any response overflow
    /// without returning a truncated roster.
    pub async fn inspect_scoped_entry<B: EntryBudget>(
        &self,
        key: impl Into<String>,
        limits: EntryInspectionLimits,
        budget: B,
    ) -> Result<(InspectedEntries, B), EntryInspectionError> {
        let key = key.into();
        if key.is_empty()
            || limits.max_key_bytes == 0
            || key.len() > limits.max_key_bytes
            || key.capacity() > limits.max_key_bytes
            || limits.max_members == 0
            || limits.max_member_bytes == 0
            || limits.max_value_bytes == 0
            || limits.max_response_bytes == 0
            || limits
                .max_response_bytes
                .checked_add(key.capacity())
                .is_none_or(|bytes| bytes > budget.bytes())
        {
            return Err(EntryInspectionError::InvalidLimit);
        }
        let (reply, response) = oneshot::channel();
        self.tx
            .try_send(Event::InspectScopedEntry {
                key,
                limits,
                budget: Box::new(budget),
                reply,
            })
            .map_err(|_| EntryInspectionError::Unavailable)?;
        let (entries, erased) = response
            .await
            .map_err(|_| EntryInspectionError::Unavailable)??;
        match erased.downcast::<B>() {
            Ok(budget) => Ok((entries, *budget)),
            Err(erased) => {
                drop(entries);
                drop(erased);
                Err(EntryInspectionError::Internal)
            }
        }
    }

    /// Samples two scoped keys and native membership incarnations from one
    /// complete actor cut, with the owned budget held through cancellation.
    ///
    /// # Errors
    /// Rejects invalid limits, a full/closed actor, or a response too large
    /// to retain without returning a truncated roster.
    pub async fn inspect_scoped_pair<B: EntryBudget>(
        &self,
        first: &str,
        second: &str,
        limits: EntryInspectionLimits,
        budget: B,
    ) -> Result<(InspectedPair, B), EntryInspectionError> {
        let key_bytes = first
            .len()
            .checked_add(second.len())
            .ok_or(EntryInspectionError::InvalidLimit)?;
        if first.is_empty()
            || second.is_empty()
            || first == second
            || first.len() > limits.max_key_bytes
            || second.len() > limits.max_key_bytes
            || limits.max_members == 0
            || limits.max_member_bytes == 0
            || limits.max_value_bytes == 0
            || limits.max_response_bytes == 0
            || limits
                .max_response_bytes
                .checked_add(key_bytes)
                .is_none_or(|bytes| bytes > budget.bytes())
        {
            return Err(EntryInspectionError::InvalidLimit);
        }
        let first = first.to_owned();
        let second = second.to_owned();
        if first.capacity() > limits.max_key_bytes
            || second.capacity() > limits.max_key_bytes
            || limits
                .max_response_bytes
                .checked_add(first.capacity())
                .and_then(|bytes| bytes.checked_add(second.capacity()))
                .is_none_or(|bytes| bytes > budget.bytes())
        {
            return Err(EntryInspectionError::InvalidLimit);
        }
        let (reply, response) = oneshot::channel();
        self.tx
            .try_send(Event::InspectScopedPair {
                first,
                second,
                limits,
                budget: Box::new(budget),
                reply,
            })
            .map_err(|_| EntryInspectionError::Unavailable)?;
        let (entries, erased) = response
            .await
            .map_err(|_| EntryInspectionError::Unavailable)??;
        match erased.downcast::<B>() {
            Ok(budget) => Ok((entries, *budget)),
            Err(erased) => {
                drop(entries);
                drop(erased);
                Err(EntryInspectionError::Internal)
            }
        }
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
