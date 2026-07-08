//! The per-group actor loop: the glue that pumps events between one
//! [`GroupEngine`] and a [`Transport`], and executes the effects the engine
//! returns.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use groupnet_core::{Command, Effect, GroupEngine, NodeId, Time};
use groupnet_transport::Transport;
use tokio::sync::{mpsc, watch};
use tokio::time::MissedTickBehavior;

/// A published, read-only snapshot of a group's metadata.
pub(crate) type MetaSnapshot = Arc<BTreeMap<String, String>>;

/// A published, read-only snapshot of a group's live members (id order).
pub(crate) type MembersSnapshot = Arc<Vec<NodeId>>;

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
        }
    }
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
            // Membership/metadata change signals aren't surfaced yet.
            Effect::ArmTimer { .. }
            | Effect::MembershipChanged
            | Effect::MetadataChanged { .. } => {}
        }
    }
}
