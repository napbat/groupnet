//! The per-group actor loop: the glue that pumps events between one
//! [`GroupEngine`] and a [`Transport`], and executes the effects the engine
//! returns.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use groupnet_core::{Command, Effect, GroupEngine, GroupId, NodeId, Status, Time};
use groupnet_transport::Transport;
use tokio::sync::{mpsc, watch};
use tokio::time::MissedTickBehavior;

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
pub(crate) type StatusesSnapshot = Arc<BTreeMap<NodeId, Status>>;

/// A published, read-only snapshot of every node's app-defined state.
pub(crate) type NodeStatesSnapshot = Arc<BTreeMap<NodeId, Vec<u8>>>;

/// An event delivered to a group actor: either a decoded network frame or a
/// local command from the [`Group`](crate::Group) handle.
pub(crate) enum Event {
    Message { from: NodeId, wire: Vec<u8> },
    Local(Command),
}

/// The `watch` senders a group actor publishes its readable state through.
pub(crate) struct Publishers {
    pub coordinator: watch::Sender<Option<NodeId>>,
    pub metadata: watch::Sender<MetaSnapshot>,
    pub members: watch::Sender<MembersSnapshot>,
    pub statuses: watch::Sender<StatusesSnapshot>,
    pub node_states: watch::Sender<NodeStatesSnapshot>,
}

/// The `watch` receivers a [`Group`](crate::Group) reads its published state
/// through — the read-side mirror of [`Publishers`].
pub(crate) struct GroupViews {
    pub coordinator: watch::Receiver<Option<NodeId>>,
    pub metadata: watch::Receiver<MetaSnapshot>,
    pub members: watch::Receiver<MembersSnapshot>,
    pub statuses: watch::Receiver<StatusesSnapshot>,
    pub node_states: watch::Receiver<NodeStatesSnapshot>,
}

/// Maps wall-clock elapsed time onto the engine's logical [`Time`]. This is the
/// one place the runtime reads the real clock; the core never does.
pub(crate) fn now_since(start: Instant) -> Time {
    Time(start.elapsed().as_millis() as u64)
}

/// Runs a single group's engine as an actor until its inbox closes.
pub(crate) async fn group_task<T: Transport>(
    mut engine: GroupEngine,
    mut inbox: mpsc::UnboundedReceiver<Event>,
    transport: Arc<T>,
    publishers: Publishers,
    // When this group's coordinator becomes us, announce it into the routing
    // group through this channel. `None` for the routing group itself.
    routing: Option<mpsc::UnboundedSender<Event>>,
    start: Instant,
    tick_period: Duration,
) {
    let boot = engine.start(now_since(start));
    dispatch(&transport, &publishers.coordinator, boot).await;

    let mut ticker = tokio::time::interval(tick_period);
    // We approximate the engine's precise ArmTimer deadlines with a fixed
    // interval; the engine is idempotent under early/extra ticks. If we fall
    // behind, skip missed ticks rather than firing a burst.
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    // The coordinator we last published to the routing table.
    let mut announced_coordinator: Option<NodeId> = None;
    announce_coordinator(&engine, routing.as_ref(), &mut announced_coordinator);

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
        let state_dirty = effects
            .iter()
            .any(|e| matches!(e, Effect::NodeStateChanged { .. }));
        dispatch(&transport, &publishers.coordinator, effects).await;
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
            let statuses: BTreeMap<NodeId, Status> = engine
                .member_statuses()
                .map(|(n, s)| (n.clone(), s))
                .collect();
            let _ = publishers.statuses.send(Arc::new(statuses));
            announce_coordinator(&engine, routing.as_ref(), &mut announced_coordinator);
        }
        if state_dirty {
            let snapshot: BTreeMap<NodeId, Vec<u8>> = engine
                .node_states_iter()
                .map(|(n, s)| (n.clone(), s.to_vec()))
                .collect();
            let _ = publishers.node_states.send(Arc::new(snapshot));
        }
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
    routing: Option<&mpsc::UnboundedSender<Event>>,
    announced: &mut Option<NodeId>,
) {
    let Some(routing) = routing else { return };
    let current = engine.coordinator().cloned();
    if current == *announced {
        return;
    }
    if let Some(coordinator) = &current {
        let _ = routing.send(Event::Local(Command::UpdateMetadata {
            key: coordinator_key(engine.group()),
            value: coordinator.to_string(),
        }));
    }
    *announced = current;
}

async fn dispatch<T: Transport>(
    transport: &Arc<T>,
    coord_tx: &watch::Sender<Option<NodeId>>,
    effects: Vec<Effect>,
) {
    for effect in effects {
        match effect {
            Effect::Send { to, wire } => {
                // Best-effort: a send error is just a drop, which the protocol
                // tolerates. Fanout is sequential here for clarity; a hot
                // deployment can `join_all` these futures instead.
                let _ = transport.send(&to, &wire).await;
            }
            Effect::CoordinatorChanged { coordinator } => {
                // Publish to readers; ignore error if no receivers remain.
                let _ = coord_tx.send(coordinator);
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
