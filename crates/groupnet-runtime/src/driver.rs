//! The per-group actor loop: the glue that pumps events between one
//! [`GroupEngine`] and a [`Transport`], and executes the effects the engine
//! returns.

use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use groupnet_core::{Command, Effect, GroupEngine, GroupId, NetStats, NodeId, Time};
use groupnet_transport::Transport;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::group::{
    EntryInspectionError, EntryInspectionLimits, InspectedEntries, InspectedPair, Leadership,
};

mod durability;
mod entry_inspection;
mod views;
use durability::Durability;
pub(crate) use durability::Grants;
pub use durability::{CloseReport, UnfinishedGrant};
use entry_inspection::{inspect_scoped_entry, inspect_scoped_pair};

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

pub(crate) use views::{AllEntries, MembersSnapshot, NodeEntriesSnapshot, StatusesSnapshot, Views};

/// An event delivered to a group actor: either a decoded network frame or a
/// local command from the [`Group`](crate::Group) handle.
type ErasedBudget = Box<dyn Any + Send>;
type InspectionReply =
    oneshot::Sender<Result<(InspectedEntries, ErasedBudget), EntryInspectionError>>;
type PairInspectionReply =
    oneshot::Sender<Result<(InspectedPair, ErasedBudget), EntryInspectionError>>;
type MutationReply = oneshot::Sender<(bool, ErasedBudget)>;

pub(crate) enum Event {
    Message {
        from: NodeId,
        wire: Vec<u8>,
    },
    Local(Command),
    InspectScopedEntry {
        key: String,
        limits: EntryInspectionLimits,
        budget: ErasedBudget,
        reply: InspectionReply,
    },
    InspectScopedPair {
        first: String,
        second: String,
        limits: EntryInspectionLimits,
        budget: ErasedBudget,
        reply: PairInspectionReply,
    },
    SetEntryConfirmed {
        key: String,
        value: Vec<u8>,
        ttl_ms: Option<u64>,
        budget: ErasedBudget,
        reply: MutationReply,
    },
    SetEntryIfRevision {
        key: String,
        value: Vec<u8>,
        ttl_ms: Option<u64>,
        expected: crate::group::EntryRevision,
        budget: ErasedBudget,
        reply: MutationReply,
    },
    DeleteEntryIfValue {
        key: String,
        expected: Vec<u8>,
        budget: ErasedBudget,
        reply: MutationReply,
    },
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
    /// The voters that did not answer the resign of this node yet.
    pub resign_pending: watch::Sender<usize>,
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
    pub resign_pending: watch::Receiver<usize>,
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

struct MutationCompletion {
    reply: MutationReply,
    applied: bool,
    budget: ErasedBudget,
}

struct ActorTurn {
    effects: Vec<Effect>,
    completion: Option<MutationCompletion>,
}

/// Handles one actor command without crossing an await. Inspection sends its
/// read-only reply directly; a mutation's completion waits for publication.
fn apply_actor_event(engine: &mut GroupEngine, event: Event, start: Instant) -> Option<ActorTurn> {
    match event {
        Event::Message { from, wire } => Some(ActorTurn {
            effects: engine.on_message(from, &wire, now_since(start)),
            completion: None,
        }),
        Event::Local(cmd) => Some(ActorTurn {
            effects: engine.apply(cmd),
            completion: None,
        }),
        Event::InspectScopedEntry {
            key,
            limits,
            budget,
            reply,
        } => {
            let sampled_at = std::time::Instant::now();
            let result = inspect_scoped_entry(engine, &key, limits, now_since(start), sampled_at)
                .map(|entries| (entries, budget));
            let _ = reply.send(result);
            None
        }
        Event::InspectScopedPair {
            first,
            second,
            limits,
            budget,
            reply,
        } => {
            let sampled_at = std::time::Instant::now();
            let result = inspect_scoped_pair(
                engine,
                &first,
                &second,
                limits,
                now_since(start),
                sampled_at,
            )
            .map(|entries| (entries, budget));
            let _ = reply.send(result);
            None
        }
        mutation => Some(apply_mutation_event(engine, mutation, start)),
    }
}

/// Executes one actor-owned entry mutation; completion follows publication.
fn apply_mutation_event(engine: &mut GroupEngine, event: Event, start: Instant) -> ActorTurn {
    let (effects, completion) = match event {
        Event::SetEntryConfirmed {
            key,
            value,
            ttl_ms,
            budget,
            reply,
        } => {
            let mut effects = engine.on_tick(now_since(start));
            let applied = engine.apply(Command::SetLocalEntry { key, value, ttl_ms });
            let accepted = !applied.is_empty();
            effects.extend(applied);
            (
                effects,
                Some(MutationCompletion {
                    reply,
                    applied: accepted,
                    budget,
                }),
            )
        }
        Event::SetEntryIfRevision {
            key,
            value,
            ttl_ms,
            expected,
            budget,
            reply,
        } => {
            let mut effects = engine.on_tick(now_since(start));
            let local = engine.local();
            let current_key = engine.node_entry_version(local, &key);
            let accepted = engine.member_state_version(local).is_some_and(|version| {
                version < u64::MAX
                    && if let Some(expected_key) = expected.key {
                        current_key == Some(expected_key)
                    } else {
                        current_key.is_none() && version == expected.member
                    }
            });
            if accepted {
                effects.extend(engine.apply(Command::SetLocalEntry { key, value, ttl_ms }));
            }
            (
                effects,
                Some(MutationCompletion {
                    reply,
                    applied: accepted,
                    budget,
                }),
            )
        }
        Event::DeleteEntryIfValue {
            key,
            expected,
            budget,
            reply,
        } => {
            let now = now_since(start);
            let mut effects = engine.on_tick(now);
            let matching = engine
                .node_entry(engine.local(), &key)
                .filter(|_| {
                    engine
                        .node_entry_expires_at(engine.local(), &key)
                        .is_none_or(|expiry| expiry > now)
                })
                .is_some_and(|value| value == expected.as_slice());
            let applied = if matching {
                let applied = engine.apply(Command::DeleteLocalEntry { key });
                let accepted = !applied.is_empty();
                effects.extend(applied);
                accepted
            } else {
                false
            };
            (
                effects,
                Some(MutationCompletion {
                    reply,
                    applied,
                    budget,
                }),
            )
        }
        Event::Message { .. }
        | Event::Local(_)
        | Event::InspectScopedEntry { .. }
        | Event::InspectScopedPair { .. } => unreachable!("inspection is handled before mutation"),
    };
    ActorTurn {
        effects,
        completion,
    }
}

/// Everything one group actor is spawned with: its engine, its inbox, and the
/// driver-side wiring it executes effects through.
///
/// One struct rather than a parameter list because the list had reached the
/// point where a caller could silently transpose two of them — and because the
/// members are exactly what `spawn_group` in `node.rs` decides per group.
pub(crate) struct GroupTask<T: Transport> {
    /// The engine this actor pumps — already built, and already carrying any
    /// recovered voter grant.
    pub engine: GroupEngine,
    /// Decoded frames and local commands, bounded.
    pub inbox: mpsc::Receiver<Event>,
    /// Where [`Effect::Send`] goes.
    pub transport: Arc<T>,
    /// The `watch`/`broadcast` senders this group publishes through.
    pub publishers: Publishers,
    /// The member views the `publishers` were seeded from, kept by the actor
    /// so each later publish writes only what changed.
    pub views: Views,
    /// When this group's coordinator becomes us, announce it into the routing
    /// group through this channel. `None` for the routing group itself.
    pub routing: Option<mpsc::Sender<Event>>,
    /// Voter durability for a [`Quorum`](groupnet_core::Activation::Quorum)
    /// group: the store, and how long a write that runs when the node closes
    /// is waited for. `None` is the **blackout posture**:
    /// [`Effect::PersistGrant`] is ignored and the engine's post-restart grant
    /// blackout stands in for durability.
    pub store: Option<Grants>,
    /// Where [`Effect::AnchorClaimDue`]'s epoch hints go: the capacity-one
    /// inbox of this group's [`anchor_task`](crate::anchor::anchor_task).
    /// `None` for every group that is not both
    /// [`External`](groupnet_core::Activation::External) and configured with an
    /// [`Anchor`](crate::Anchor) — the **fail-safe posture**, where the prompt
    /// is dropped and the group simply never activates a host.
    pub anchor_prompts: Option<mpsc::Sender<u64>>,
    /// The node's logical-time origin — a Tokio instant, so the engine's
    /// [`Time`] and the timer the actor sleeps on read one clock.
    pub start: Instant,
    /// Cancelled when the node's network shuts down — [`Node::close`] or a
    /// router shutdown. The actor stops then, whatever `Group` handles remain
    /// (napbat/groupnet#9): a closed node's engine must not go on claiming
    /// and writing grants into the store its next life loads.
    ///
    /// [`Node::close`]: crate::Node::close
    pub closed: CancellationToken,
}

/// Runs a single group's engine as an actor until its node closes or its inbox
/// closes (every handle and route sender dropped).
///
/// A turn that is running when the node closes completes; no turn starts after
/// that. A grant write in that turn is waited for one close wait at most (see
/// [`UnfinishedGrant`]), which is what the actor returns when the wait ran
/// out. The `watch` and `broadcast` senders drop with the actor, so a held
/// handle keeps its last readable views, and each later command on it is
/// refused as `CommandRejected::GroupClosed`.
pub(crate) async fn group_task<T: Transport>(task: GroupTask<T>) -> Option<UnfinishedGrant> {
    let GroupTask {
        mut engine,
        mut inbox,
        transport,
        publishers,
        mut views,
        routing,
        store,
        anchor_prompts,
        start,
        closed,
    } = task;
    if closed.is_cancelled() {
        return None; // joined after the close: nothing to start
    }
    // The observer whose view every published `Leadership` is derived from.
    // Cloned once so `dispatch` never has to borrow the engine.
    let local = engine.local().clone();
    let mut durability = Durability::new(store, closed.clone());

    let boot = engine.start(now_since(start));
    dispatch(
        &transport,
        &publishers,
        &local,
        &boot,
        &mut durability,
        anchor_prompts.as_ref(),
    )
    .await;

    // The actor sleeps until the engine's own next deadline, re-read after
    // every turn: a frame or a command can bring it forward (a nudged round,
    // a short-TTL entry) without emitting `ArmTimer`. An idle group so wakes
    // only when something is due.
    let mut last_tick: Option<Time> = None;
    let wake = tokio::time::sleep_until(tick_at(start, engine.next_deadline(), last_tick));
    tokio::pin!(wake);

    // The coordinator we last published to the routing table.
    let mut announced_coordinator: Option<NodeId> = None;
    announce_coordinator(&engine, routing.as_ref(), &mut announced_coordinator);

    loop {
        // Checked before every turn, and not only raced in the `select!`
        // below: a busy inbox or a due tick must not win a turn after the
        // close.
        if closed.is_cancelled() {
            break;
        }
        let due = tick_at(start, engine.next_deadline(), last_tick);
        if wake.deadline() != due {
            wake.as_mut().reset(due);
        }
        let turn = tokio::select! {
            () = closed.cancelled() => break,
            maybe = inbox.recv() => match maybe {
                Some(event) => apply_actor_event(&mut engine, event, start),
                None => break, // handle and all route senders dropped
            },
            () = wake.as_mut() => {
                let now = now_since(start);
                last_tick = Some(now);
                Some(ActorTurn {
                    effects: engine.on_tick(now),
                    completion: None,
                })
            }
        };
        let Some(ActorTurn {
            effects,
            completion,
        }) = turn
        else {
            continue;
        };
        // Publish first, wake second. Every `watch` and snapshot this batch
        // touches is republished *before* the matching `GroupEvent` goes out at
        // the bottom of the loop, so a consumer woken by an edge always reads
        // the state that edge announced — never the snapshot from before it.
        dispatch(
            &transport,
            &publishers,
            &local,
            &effects,
            &mut durability,
            anchor_prompts.as_ref(),
        )
        .await;
        publish_actor_views(
            &engine,
            &publishers,
            routing.as_ref(),
            &mut announced_coordinator,
            &mut views,
            &effects,
        );
        if let Some(MutationCompletion {
            reply,
            applied,
            budget,
        }) = completion
        {
            let _ = reply.send((applied, budget));
        }
    }
    durability.into_unfinished(engine.group())
}

/// When the actor next ticks its engine: at `deadline`
/// ([`GroupEngine::next_deadline`]), but never at or before the previous
/// tick's instant. A tick at `t` handles everything due by `t`, so waking at
/// `t` again could only spin; one millisecond later is the clock's next
/// distinct instant. A deadline past what the clock represents waits as long
/// as Tokio's own far future.
fn tick_at(start: Instant, deadline: Time, last_tick: Option<Time>) -> Instant {
    let at = last_tick.map_or(deadline, |tick| deadline.max(tick.saturating_add(1)));
    start
        .checked_add(Duration::from_millis(at.0))
        .unwrap_or_else(|| Instant::now() + FAR_FUTURE)
}

/// How far ahead an unrepresentable deadline is parked — the horizon Tokio
/// itself uses for a timer that never fires (about 30 years).
const FAR_FUTURE: Duration = Duration::from_hours(24 * 365 * 30);

/// Republishes all readable views before the matching edge notifications.
///
/// The member views are incremental: [`Views::update`] writes only the
/// members the engine's change clock names and the nodes this batch's
/// `NodeStateChanged` effects name, each into a persistent map, and a view
/// is sent only when it changed — `O(k log N)` for `k` changed members,
/// nothing when none did.
fn publish_actor_views(
    engine: &GroupEngine,
    publishers: &Publishers,
    routing: Option<&mpsc::Sender<Event>>,
    announced_coordinator: &mut Option<NodeId>,
    views: &mut Views,
    effects: &[Effect],
) {
    let meta_dirty = effects
        .iter()
        .any(|e| matches!(e, Effect::MetadataChanged { .. }));
    let members_dirty = effects
        .iter()
        .any(|e| matches!(e, Effect::MembershipChanged));
    let touched: BTreeSet<&NodeId> = effects
        .iter()
        .filter_map(|e| match e {
            Effect::NodeStateChanged { node, .. } => Some(node),
            _ => None,
        })
        .collect();
    if meta_dirty {
        let snapshot: BTreeMap<String, String> = engine
            .metadata_iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect();
        let _ = publishers.metadata.send(Arc::new(snapshot));
    }
    // Before the statuses: a reader that sees this node dead after its leave
    // also sees the voters its resign waits for.
    let pending = engine.resign_pending();
    publishers.resign_pending.send_if_modified(|current| {
        let changed = *current != pending;
        *current = pending;
        changed
    });
    let changed = views.update(engine, touched.into_iter());
    if changed.members {
        let _ = publishers.members.send(views.members());
    }
    if changed.statuses {
        let _ = publishers.statuses.send(views.statuses());
    }
    if members_dirty {
        announce_coordinator(engine, routing, announced_coordinator);
    }
    if changed.entries {
        let _ = publishers.entries.send(views.entries());
    }
    let stats = engine.net_stats();
    publishers.net_stats.send_if_modified(|current| {
        let changed = *current != stats;
        if changed {
            *current = stats;
        }
        changed
    });
    emit_events(&publishers.events, effects);
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
            Effect::Send { .. }
            | Effect::ArmTimer { .. }
            | Effect::PersistGrant { .. }
            | Effect::AnchorClaimDue { .. } => continue,
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

/// Executes one batch of engine effects: sends frames, honours the write-ahead
/// grant persist, and republishes the `watch` views whose value an effect
/// carries outright. `local` is the observer the [`Leadership`] role is derived
/// against.
///
/// Runs before [`emit_events`] for the same batch, so the snapshots below are
/// in place before any consumer is woken to read them.
async fn dispatch<T: Transport>(
    transport: &Arc<T>,
    publishers: &Publishers,
    local: &NodeId,
    effects: &[Effect],
    durability: &mut Durability,
    anchor_prompts: Option<&mpsc::Sender<u64>>,
) {
    for effect in effects {
        match effect {
            Effect::Send { to, wire } => {
                if durability.withholds(wire, local) {
                    // Fail closed. The store said no, so this node grants
                    // nothing — see [`GrantStore`] for why silently.
                    continue;
                }
                // Best-effort: a send error is just a drop, which the protocol
                // tolerates. Fanout is sequential here for clarity; a hot
                // deployment can `join_all` these futures instead.
                let _ = transport.send(to, wire).await;
            }
            // The write-ahead half of the Quorum voter contract: complete the
            // persist *here*, before the loop reaches the frame it belongs to.
            // Blocking the actor on it is the contract, not an oversight — a
            // grant that outruns its own durability is exactly the double-grant
            // a crash-restart turns into two hosts for one epoch. Meanwhile the
            // bounded inbox drops inbound frames, which gossip re-teaches.
            //
            // With no store this is the **blackout posture**: the engine's
            // post-restart grant blackout stands in for durability, and there
            // is nothing to do here.
            Effect::PersistGrant { epoch, claimant } => {
                durability.persist(*epoch, claimant).await;
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
            // The External tier's prompt, and the exact opposite of
            // `PersistGrant` above: that one blocks the actor by contract,
            // this one must **never** block it. A `try_send` into a
            // capacity-one channel *is* the debounce the engine's repeated
            // level signal requires — a prompt arriving while the anchor task
            // is mid-round finds the slot full and is dropped, so a slow store
            // cannot stack claims and burn an epoch per prompt. Nothing here
            // waits on a network round trip a group's gossip has no business
            // being behind.
            //
            // With no anchor prompts channel this is the documented fail-safe
            // posture: the prompt is dropped and the group never activates a
            // host, exactly as an empty voter roster behaves under Quorum.
            Effect::AnchorClaimDue { epoch_hint } => {
                if let Some(prompts) = anchor_prompts {
                    let _ = prompts.try_send(*epoch_hint);
                }
            }
            // ArmTimer needs no handling: the actor re-reads the engine's
            // `next_deadline` after every turn, which covers it and the
            // deadlines a frame or command moves without emitting one.
            // Membership/metadata/state change signals are surfaced by
            // republishing snapshots in `group_task`, not here.
            Effect::ArmTimer { .. }
            | Effect::MembershipChanged
            | Effect::NodeStateChanged { .. }
            | Effect::MetadataChanged { .. } => {}
        }
    }
}
