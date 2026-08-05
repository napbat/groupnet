//! The per-group actor loop: the glue that pumps events between one
//! [`GroupEngine`] and a [`Transport`], and executes the effects the engine
//! returns.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use groupnet_core::{Command, Effect, GroupEngine, GroupId, NetStats, NodeId, Status, Time};
use groupnet_transport::Transport;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::time::MissedTickBehavior;

use crate::group::Leadership;

/// A change notification a consumer can subscribe to via
/// [`Group::events`](crate::Group::events). The stream is bounded: a slow
/// subscriber observes `RecvError::Lagged` and must resync from the watch
/// snapshots (which are always current) — events are edge triggers, not a
/// reliable log.
///
/// An edge is published *after* the state it announces: a consumer woken by
/// one and then reading the matching snapshot
/// ([`Group::leadership`](crate::Group::leadership),
/// [`coordinator`](crate::Group::coordinator),
/// [`members`](crate::Group::members), …) sees at least the value that edge
/// carried, never the one from before it.
#[derive(Clone, Debug)]
pub enum GroupEvent {
    /// The observed coordinator changed.
    CoordinatorChanged(Option<NodeId>),
    /// The membership set or some member's status changed.
    MembershipChanged,
    /// The group's epoch-fenced host changed (a new host activated, the
    /// incumbent was deposed, or the lease lapsed leaving none). Only a
    /// [`GroupMode::Hosted`](groupnet_core::GroupMode) group emits this;
    /// it is unrelated to [`CoordinatorChanged`](Self::CoordinatorChanged),
    /// whose coordinator is derived and never authoritative.
    LeadershipChanged {
        /// The epoch this observation belongs to (monotone per group).
        epoch: u64,
        /// The host of that epoch, or `None` if the group has none.
        host: Option<NodeId>,
    },
    /// One key of a node's app-defined state changed.
    NodeStateChanged {
        /// The node whose entry changed.
        node: NodeId,
        /// The key that changed.
        key: String,
    },
    /// A metadata key took a new value.
    MetadataChanged {
        /// The key that changed.
        key: String,
    },
}

/// Capacity of the per-group event stream. Slow subscribers lag (and resync
/// from snapshots) rather than growing memory.
pub(crate) const EVENTS_CAPACITY: usize = 256;

/// Capacity of a group actor's inbox. Network events beyond it are DROPPED
/// (gossip is loss-tolerant by design); local commands use `try_send` and
/// surface the miss to the caller.
pub(crate) const INBOX_CAPACITY: usize = 1024;

/// Formats the routing-table key under which a group's coordinator is published.
pub(crate) fn coordinator_key(group: &GroupId) -> String {
    format!("coord:{group}")
}

/// A published, read-only snapshot of a group's metadata.
pub(crate) type MetaSnapshot = Arc<BTreeMap<String, String>>;

/// A published, read-only snapshot of a group's live members (id order).
pub(crate) type MembersSnapshot = Arc<Vec<NodeId>>;

/// A published, read-only snapshot of every known member's status (Alive/Suspect,
/// plus not-yet-reaped Dead tombstones), so readers can route *around* a suspected
/// peer that [`MembersSnapshot`] (the not-`Dead` set) still lists.
///
/// Each status carries the engine-logical [`Time`] this observer has held it
/// continuously since, so a reader can turn a status into a *duration* —
/// see [`Group::status_held_for`](crate::Group::status_held_for).
pub(crate) type StatusesSnapshot = Arc<BTreeMap<NodeId, (Status, Time)>>;

/// Collects the engine's whole status roster, stamps and all, into the shape
/// [`StatusesSnapshot`] publishes. One place, so the boot snapshot in
/// `node.rs` and the per-change republish below can never drift apart.
pub(crate) fn statuses_snapshot(engine: &GroupEngine) -> StatusesSnapshot {
    Arc::new(
        engine
            .member_statuses_since()
            .map(|(n, s, since)| (n.clone(), (s, since)))
            .collect(),
    )
}

/// A published, read-only snapshot of every node's keyed app-defined state
/// (live entries only — tombstoned/expired keys are absent). Two-level
/// `Arc` sharing keeps updates incremental: a state change rebuilds only the
/// changed node's inner map; the outer map is pointer-cloned per publish.
pub(crate) type NodeEntriesSnapshot = Arc<BTreeMap<NodeId, Arc<BTreeMap<String, Vec<u8>>>>>;

/// An event delivered to a group actor: either a decoded network frame or a
/// local command from the [`Group`](crate::Group) handle.
pub(crate) enum Event {
    Message { from: NodeId, wire: Vec<u8> },
    Local(Command),
}

/// The `watch` senders a group actor publishes its readable state through.
pub(crate) struct Publishers {
    pub coordinator: watch::Sender<Option<NodeId>>,
    pub leadership: watch::Sender<Leadership>,
    pub metadata: watch::Sender<MetaSnapshot>,
    pub members: watch::Sender<MembersSnapshot>,
    pub statuses: watch::Sender<StatusesSnapshot>,
    pub entries: watch::Sender<NodeEntriesSnapshot>,
    pub net_stats: watch::Sender<NetStats>,
    pub events: broadcast::Sender<GroupEvent>,
}

/// The `watch` receivers a [`Group`](crate::Group) reads its published state
/// through — the read-side mirror of [`Publishers`].
pub(crate) struct GroupViews {
    pub coordinator: watch::Receiver<Option<NodeId>>,
    pub leadership: watch::Receiver<Leadership>,
    pub metadata: watch::Receiver<MetaSnapshot>,
    pub members: watch::Receiver<MembersSnapshot>,
    pub statuses: watch::Receiver<StatusesSnapshot>,
    pub entries: watch::Receiver<NodeEntriesSnapshot>,
    pub net_stats: watch::Receiver<NetStats>,
    pub events: broadcast::Sender<GroupEvent>,
}

/// Maps wall-clock elapsed time onto the engine's logical [`Time`]. This is the
/// one place the runtime reads the real clock; the core never does.
#[expect(
    clippy::cast_possible_truncation,
    reason = "a process would have to stay up ~584 million years for its elapsed \
              milliseconds to overflow a u64"
)]
pub(crate) fn now_since(start: Instant) -> Time {
    Time(start.elapsed().as_millis() as u64)
}

/// Runs a single group's engine as an actor until its inbox closes.
pub(crate) async fn group_task<T: Transport>(
    mut engine: GroupEngine,
    mut inbox: mpsc::Receiver<Event>,
    transport: Arc<T>,
    publishers: Publishers,
    // When this group's coordinator becomes us, announce it into the routing
    // group through this channel. `None` for the routing group itself.
    routing: Option<mpsc::Sender<Event>>,
    start: Instant,
    tick_period: Duration,
) {
    // The observer whose view every published `Leadership` is derived from.
    // Cloned once so `dispatch` never has to borrow the engine.
    let local = engine.local().clone();

    let boot = engine.start(now_since(start));
    dispatch(&transport, &publishers, &local, &boot).await;

    let mut ticker = tokio::time::interval(tick_period);
    // We approximate the engine's precise ArmTimer deadlines with a fixed
    // interval; the engine is idempotent under early/extra ticks. If we fall
    // behind, skip missed ticks rather than firing a burst.
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    // The coordinator we last published to the routing table.
    let mut announced_coordinator: Option<NodeId> = None;
    announce_coordinator(&engine, routing.as_ref(), &mut announced_coordinator);

    // The entries snapshot, maintained incrementally: only nodes named in a
    // NodeStateChanged effect rebuild their (Arc-shared) inner map.
    let mut entries_master: BTreeMap<NodeId, Arc<BTreeMap<String, Vec<u8>>>> = BTreeMap::new();

    loop {
        let effects = tokio::select! {
            maybe = inbox.recv() => match maybe {
                Some(Event::Message { from, wire }) => {
                    engine.on_message(from, &wire, now_since(start))
                }
                Some(Event::Local(cmd)) => engine.apply(cmd),
                None => break, // handle and all route senders dropped
            },
            _ = ticker.tick() => engine.on_tick(now_since(start)),
        };
        let meta_dirty = effects
            .iter()
            .any(|e| matches!(e, Effect::MetadataChanged { .. }));
        let members_dirty = effects
            .iter()
            .any(|e| matches!(e, Effect::MembershipChanged));
        let touched: BTreeSet<NodeId> = effects
            .iter()
            .filter_map(|e| match e {
                Effect::NodeStateChanged { node, .. } => Some(node.clone()),
                _ => None,
            })
            .collect();
        // Publish first, wake second. Every `watch` and snapshot this batch
        // touches is republished *before* the matching `GroupEvent` goes out at
        // the bottom of the loop, so a consumer woken by an edge always reads
        // the state that edge announced — never the snapshot from before it.
        dispatch(&transport, &publishers, &local, &effects).await;
        // Republish snapshots; readers borrow them lock-free.
        if meta_dirty {
            let snapshot: BTreeMap<String, String> = engine
                .metadata_iter()
                .map(|(k, v)| (k.to_owned(), v.to_owned()))
                .collect();
            let _ = publishers.metadata.send(Arc::new(snapshot));
        }
        if members_dirty {
            let snapshot: Vec<NodeId> = engine.members().cloned().collect();
            let _ = publishers.members.send(Arc::new(snapshot));
            let _ = publishers.statuses.send(statuses_snapshot(&engine));
            announce_coordinator(&engine, routing.as_ref(), &mut announced_coordinator);
        }
        if !touched.is_empty() {
            for node in touched {
                let entries: BTreeMap<String, Vec<u8>> = engine
                    .node_entries(&node)
                    .map(|(k, v)| (k.to_owned(), v.to_vec()))
                    .collect();
                if entries.is_empty() {
                    entries_master.remove(&node);
                } else {
                    entries_master.insert(node, Arc::new(entries));
                }
            }
            let _ = publishers.entries.send(Arc::new(entries_master.clone()));
        }
        if members_dirty {
            // Drop reaped members' state from the snapshot.
            let live: BTreeSet<NodeId> = engine.member_statuses().map(|(n, _)| n.clone()).collect();
            let before = entries_master.len();
            entries_master.retain(|node, _| live.contains(node));
            if entries_master.len() != before {
                let _ = publishers.entries.send(Arc::new(entries_master.clone()));
            }
        }
        let stats = engine.net_stats();
        publishers.net_stats.send_if_modified(|current| {
            let changed = *current != stats;
            if changed {
                *current = stats;
            }
            changed
        });
        // Last: the edges. Everything they announce is already readable.
        emit_events(&publishers.events, &effects);
    }
}

/// Forwards the engine's change effects onto the bounded event stream (a full
/// stream lags slow subscribers instead of buffering unboundedly).
///
/// Called *after* every publish this batch produces (see the loop in
/// [`group_task`]): an event is a wake-up whose whole purpose is to send the
/// consumer to a snapshot read, so emitting it before that snapshot exists
/// would hand it the pre-edge value.
fn emit_events(events: &broadcast::Sender<GroupEvent>, effects: &[Effect]) {
    for effect in effects {
        let event = match effect {
            Effect::CoordinatorChanged { coordinator } => {
                GroupEvent::CoordinatorChanged(coordinator.clone())
            }
            Effect::MembershipChanged => GroupEvent::MembershipChanged,
            Effect::LeadershipChanged { epoch, host } => GroupEvent::LeadershipChanged {
                epoch: *epoch,
                host: host.clone(),
            },
            Effect::NodeStateChanged { node, key } => GroupEvent::NodeStateChanged {
                node: node.clone(),
                key: key.clone(),
            },
            Effect::MetadataChanged { key, .. } => GroupEvent::MetadataChanged { key: key.clone() },
            Effect::Send { .. } | Effect::ArmTimer { .. } => continue,
        };
        let _ = events.send(event); // no subscribers / lagged is fine
    }
}

/// Publishes the coordinator this node currently *observes* for its group into
/// the routing table, whenever that observation changes.
///
/// Every member publishes — not just the coordinator itself. Because
/// coordinator selection is deterministic, all members eventually write the
/// same value, so last-writer-wins converges on the correct coordinator with no
/// risk of a stale self-announcement lingering.
fn announce_coordinator(
    engine: &GroupEngine,
    routing: Option<&mpsc::Sender<Event>>,
    announced: &mut Option<NodeId>,
) {
    let Some(routing) = routing else { return };
    let current = engine.coordinator().cloned();
    if current == *announced {
        return;
    }
    if let Some(coordinator) = &current {
        // Best-effort under backpressure: a dropped announcement re-fires on
        // the next coordinator change, and gossip converges the table anyway.
        let _ = routing.try_send(Event::Local(Command::UpdateMetadata {
            key: coordinator_key(engine.group()),
            value: coordinator.to_string(),
        }));
    }
    *announced = current;
}

/// Executes one batch of engine effects: sends frames, and republishes the
/// `watch` views whose value an effect carries outright. `local` is the
/// observer the [`Leadership`] role is derived against.
///
/// Runs before [`emit_events`] for the same batch, so the snapshots below are
/// in place before any consumer is woken to read them.
async fn dispatch<T: Transport>(
    transport: &Arc<T>,
    publishers: &Publishers,
    local: &NodeId,
    effects: &[Effect],
) {
    for effect in effects {
        match effect {
            Effect::Send { to, wire } => {
                // Best-effort: a send error is just a drop, which the protocol
                // tolerates. Fanout is sequential here for clarity; a hot
                // deployment can `join_all` these futures instead.
                let _ = transport.send(to, wire).await;
            }
            Effect::CoordinatorChanged { coordinator } => {
                // Publish to readers; ignore error if no receivers remain.
                let _ = publishers.coordinator.send(coordinator.clone());
            }
            Effect::LeadershipChanged { epoch, host } => {
                // The role is observer-local: this node is the host exactly
                // when the adopted pair names it. `Role::Claimant` cannot
                // reach here — a standing claim emits no effect at all, so
                // only an activation or a demotion ever republishes.
                // The matching `GroupEvent` follows in `emit_events`, once
                // this — the always-current snapshot behind it — is readable.
                let _ =
                    publishers
                        .leadership
                        .send(Leadership::observed(*epoch, host.clone(), local));
            }
            // ArmTimer is advisory — this driver uses a fixed-interval ticker.
            // Membership/metadata/state change signals are surfaced by
            // republishing snapshots in `group_task`, not here.
            Effect::ArmTimer { .. }
            | Effect::MembershipChanged
            | Effect::NodeStateChanged { .. }
            | Effect::MetadataChanged { .. } => {}
        }
    }
}
