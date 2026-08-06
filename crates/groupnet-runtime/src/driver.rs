//! The per-group actor loop: the glue that pumps events between one
//! [`GroupEngine`] and a [`Transport`], and executes the effects the engine
//! returns.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use groupnet_core::{Command, Effect, GroupEngine, GroupId, NetStats, NodeId, Status, Time, wire};
use groupnet_transport::Transport;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::time::MissedTickBehavior;

use crate::group::Leadership;
use crate::store::GrantStore;

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
    /// When this group's coordinator becomes us, announce it into the routing
    /// group through this channel. `None` for the routing group itself.
    pub routing: Option<mpsc::Sender<Event>>,
    /// Voter durability for a [`Quorum`](groupnet_core::Activation::Quorum)
    /// group. `None` is the **blackout posture**: [`Effect::PersistGrant`] is
    /// ignored and the engine's post-restart grant blackout stands in for
    /// durability.
    pub store: Option<Arc<dyn GrantStore>>,
    /// The node's logical-time origin.
    pub start: Instant,
    /// How often the engine is ticked.
    pub tick_period: Duration,
}

/// Runs a single group's engine as an actor until its inbox closes.
pub(crate) async fn group_task<T: Transport>(task: GroupTask<T>) {
    let GroupTask {
        mut engine,
        mut inbox,
        transport,
        publishers,
        routing,
        store,
        start,
        tick_period,
    } = task;
    // The observer whose view every published `Leadership` is derived from.
    // Cloned once so `dispatch` never has to borrow the engine.
    let local = engine.local().clone();

    // The grant this engine believes it made but whose durability the store
    // refused. It outlives the batch it was armed in, deliberately: the engine
    // re-answers an already-recorded pair without re-persisting it (a re-grant
    // decides nothing new), so a guard scoped to one batch would swallow the
    // first frame and let the very next round leak the same undurable grant.
    // Disarmed by the next persist that *succeeds* — which supersedes the pair
    // on disk, making everything at or below it safe to answer again.
    let mut undurable: Undurable = None;

    let boot = engine.start(now_since(start));
    dispatch(
        &transport,
        &publishers,
        &local,
        &boot,
        store.as_ref(),
        &mut undurable,
    )
    .await;

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
        dispatch(
            &transport,
            &publishers,
            &local,
            &effects,
            store.as_ref(),
            &mut undurable,
        )
        .await;
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
        republish_entries(
            &engine,
            &publishers,
            touched,
            members_dirty,
            &mut entries_master,
        );
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

/// Maintains and republishes the per-node entries snapshot for one batch.
///
/// Incremental on purpose: only the nodes an effect actually named rebuild
/// their (`Arc`-shared) inner map, so a publish costs one pointer clone per
/// untouched node. `members_dirty` additionally sweeps out anything membership
/// has reaped, which is the one way an entry leaves the master map without an
/// effect naming it.
fn republish_entries(
    engine: &GroupEngine,
    publishers: &Publishers,
    touched: BTreeSet<NodeId>,
    members_dirty: bool,
    master: &mut BTreeMap<NodeId, Arc<BTreeMap<String, Vec<u8>>>>,
) {
    if !touched.is_empty() {
        for node in touched {
            let entries: BTreeMap<String, Vec<u8>> = engine
                .node_entries(&node)
                .map(|(k, v)| (k.to_owned(), v.to_vec()))
                .collect();
            if entries.is_empty() {
                master.remove(&node);
            } else {
                master.insert(node, Arc::new(entries));
            }
        }
        let _ = publishers.entries.send(Arc::new(master.clone()));
    }
    if members_dirty {
        // Drop reaped members' state from the snapshot.
        let live: BTreeSet<NodeId> = engine.member_statuses().map(|(n, _)| n.clone()).collect();
        let before = master.len();
        master.retain(|node, _| live.contains(node));
        if master.len() != before {
            let _ = publishers.entries.send(Arc::new(master.clone()));
        }
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
            Effect::Send { .. } | Effect::ArmTimer { .. } | Effect::PersistGrant { .. } => continue,
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
    store: Option<&Arc<dyn GrantStore>>,
    undurable: &mut Undurable,
) {
    for effect in effects {
        match effect {
            Effect::Send { to, wire } => {
                if undurable
                    .as_ref()
                    .is_some_and(|grant| undurable_frame(wire, grant, local))
                {
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
                if let Some(store) = store {
                    *undurable = if persist_grant(store, *epoch, claimant).await {
                        // On disk, and it supersedes whatever was there — so
                        // any older pair the guard was withholding is covered
                        // by this one and the node may answer again.
                        None
                    } else {
                        Some((*epoch, claimant.clone()))
                    };
                }
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

/// The `(epoch, claimant)` a group actor's engine has recorded as granted but
/// its store has not accepted — `None` while everything the engine believes is
/// also on disk. See the guard's declaration in [`group_task`].
type Undurable = Option<(u64, NodeId)>;

/// Completes one grant's write-ahead persist and reports whether the pair is
/// now **durable**.
///
/// Runs on Tokio's blocking pool: a store is real I/O (a write and an `fsync`,
/// if it means what it says), and a group actor holds no lock but would
/// otherwise stall a runtime worker for the length of a disk round trip. The
/// actor still waits for the answer — that wait is the write-ahead contract.
///
/// A store error and a *panicking* store answer the same `false`, because
/// neither proves the pair reached disk.
async fn persist_grant(store: &Arc<dyn GrantStore>, epoch: u64, claimant: &NodeId) -> bool {
    let store = store.clone();
    let claimant = claimant.clone();
    matches!(
        tokio::task::spawn_blocking(move || store.persist(epoch, &claimant)).await,
        Ok(Ok(()))
    )
}

/// Whether `wire` is a frame that would publish the undurable `grant` — the one
/// the driver must therefore swallow.
///
/// The engine emits [`Effect::PersistGrant`] before any frame the grant it
/// records licenses — and only two of that effect's four emission shapes (see
/// [`Effect::PersistGrant`] for all of them) put such a frame in the batch at
/// all:
///
/// * **A peer's claim answered** — the `LeadGrant` naming this node as granter.
///   Dropping it means the claimant never counts us toward its majority.
/// * **Our own claim opened** — the voter's own grant is counted straight into
///   the round rather than sent, so what the persist precedes is the `LeadClaim`
///   broadcast itself. Dropping it means nobody answers a round whose first
///   grant is not durable, so it cannot reach a majority either. The same arm
///   swallows a *host's* renewal claim once the guard is armed at the pair it is
///   renewing, which is what bounds the case below.
///
/// # What this guard bounds rather than prevents
///
/// Row Q4b re-attempts a claimant's self-grant on every tick its round is open,
/// and that retry's persist precedes no frame at all — the claim went out at
/// round open, before the ledger had anything to write down. If the retry both
/// fails to persist *and* completes the majority, the activation's `LeadState`
/// is not withheld and this node hosts on a grant its disk refused. The same
/// goes for a **roster of one**, whose majority the self-grant alone satisfies:
/// row Q4's round is closed before the claim it opened is ever broadcast.
///
/// How long that hostship lasts depends on whether renewing costs a frame:
///
/// * **A roster of two or more.** One lease and no further. The guard stays
///   armed, so the renewal round's `LeadClaim` is swallowed by the arm above,
///   no voter re-grants, and the host demotes when the lease lapses.
/// * **A roster of one.** Indefinitely. A solo voter's renewal round closes
///   in-engine on its own re-grant — which persists nothing (row Q2 writes
///   nothing new) and sends nothing (there is no peer to claim from) — so the
///   guard never sees a frame to swallow and the lease is extended forever on a
///   grant the disk refused.
///
/// Both are the same risk class, because neither touches the clock: S4c-global
/// is untouched — the lease is a real one, anchored to the send instant like any
/// other, and nothing outside this node ever believed the grant. What is at risk
/// is S1-strict across an amnesiac restart, which is the property a failing
/// store has already forfeited.
///
/// Matched by **content, not by position**, for two reasons. A batch whose
/// claim fan-out is empty leaves the `PersistGrant` adjacent to unrelated
/// gossip traffic, so "swallow the next `Send`" would eat a digest. And the
/// engine re-offers both shapes on the anti-entropy cadence *without* a fresh
/// `PersistGrant` — a re-grant of a pair already in the ledger writes nothing
/// new — so positional matching would withhold the first frame and ship the
/// same undurable grant a round later, which is the whole hole this closes.
/// The decode is paid only while the guard is armed, i.e. only after a store
/// has actually failed.
fn undurable_frame(wire: &[u8], (epoch, claimant): &(u64, NodeId), local: &NodeId) -> bool {
    let Some(frame) = wire::decode(wire) else {
        return false;
    };
    match frame.lead {
        Some(wire::LeadBody::Grant {
            epoch: sent,
            claimant: to,
            granter,
        }) => sent == *epoch && to == *claimant && granter == *local,
        // A self-grant's claim: the round it opens already counts the grant
        // this node could not write down, so the claim is what must not fly.
        Some(wire::LeadBody::Claim {
            epoch: sent,
            claimant: bidder,
        }) => sent == *epoch && bidder == *claimant && bidder == *local,
        Some(wire::LeadBody::State { .. }) | None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::undurable_frame;
    use groupnet_core::{GroupId, NodeId, wire};

    fn n(id: &str) -> NodeId {
        NodeId::new(id)
    }

    /// One encoded election frame, kind derived from the body exactly as the
    /// engine derives it.
    fn lead(body: wire::LeadBody) -> Vec<u8> {
        let kind = match body {
            wire::LeadBody::Claim { .. } => wire::Kind::LeadClaim,
            wire::LeadBody::Grant { .. } => wire::Kind::LeadGrant,
            wire::LeadBody::State { .. } => wire::Kind::LeadState,
        };
        frame(kind, Some(body))
    }

    fn frame(kind: wire::Kind, lead: Option<wire::LeadBody>) -> Vec<u8> {
        wire::encode(&wire::Frame {
            kind,
            group: GroupId::new("g"),
            target: None,
            digest: Vec::new(),
            wants: Vec::new(),
            members: Vec::new(),
            metadata: Vec::new(),
            lead,
        })
    }

    fn grant(epoch: u64, claimant: &str, granter: &str) -> Vec<u8> {
        lead(wire::LeadBody::Grant {
            epoch,
            claimant: n(claimant),
            granter: n(granter),
        })
    }

    fn claim(epoch: u64, claimant: &str) -> Vec<u8> {
        lead(wire::LeadBody::Claim {
            epoch,
            claimant: n(claimant),
        })
    }

    /// The armed guard: this node ("me") could not write down its grant of
    /// epoch 7 to "them".
    fn withheld(wire: &[u8]) -> bool {
        undurable_frame(wire, &(7, n("them")), &n("me"))
    }

    /// The first shape: a peer's claim answered. The frame that carries the
    /// undurable grant is exactly the one this node must not send.
    #[test]
    fn our_own_grant_of_the_undurable_pair_is_withheld() {
        assert!(withheld(&grant(7, "them", "me")));
    }

    /// A grant is withheld only if it is *ours* and *this* pair. Anything else
    /// is somebody else's business, or a pair the store already accepted.
    #[test]
    fn a_grant_that_is_not_this_pair_from_this_node_flies() {
        assert!(!withheld(&grant(7, "them", "other")), "another granter");
        assert!(!withheld(&grant(8, "them", "me")), "another epoch");
        assert!(!withheld(&grant(7, "someone", "me")), "another claimant");
        assert!(!withheld(&grant(6, "them", "me")), "an earlier epoch");
    }

    /// The second shape: our own claim. A claimant's self-grant is counted
    /// into the round rather than sent, so the claim is what publishes it —
    /// and this is also the arm that swallows a host's *renewal* claim once
    /// the guard is armed at the pair it is renewing.
    #[test]
    fn our_own_claim_for_the_undurable_pair_is_withheld() {
        assert!(undurable_frame(&claim(7, "me"), &(7, n("me")), &n("me")));
    }

    /// A claim is only ever withheld when this node is bidding for itself: a
    /// peer's claim is not ours to drop, and a claim whose claimant is not the
    /// pair's claimant publishes nothing we failed to write down.
    #[test]
    fn somebody_elses_claim_flies() {
        assert!(!undurable_frame(
            &claim(7, "them"),
            &(7, n("them")),
            &n("me")
        ));
        assert!(!withheld(&claim(7, "me")), "our claim, another pair");
        assert!(!undurable_frame(&claim(8, "me"), &(7, n("me")), &n("me")));
    }

    /// The documented hole, pinned so it cannot be closed by accident: an
    /// activation's `LeadState` is **not** withheld. A row Q4b retry (or a
    /// roster of one) can close a round on the very grant the store refused,
    /// and by then this node is already host — see the guard's doc for how far
    /// that reaches.
    #[test]
    fn the_activation_state_is_not_withheld() {
        assert!(!withheld(&lead(wire::LeadBody::State {
            epoch: 7,
            host: Some(n("me")),
        })));
        assert!(!withheld(&lead(wire::LeadBody::State {
            epoch: 7,
            host: None,
        })));
    }

    /// Matched by content, not by position: an armed guard must not eat the
    /// gossip traffic that happens to share the batch, and must not choke on
    /// bytes it cannot decode.
    #[test]
    fn non_election_traffic_and_garbage_fly() {
        assert!(!withheld(&frame(wire::Kind::Digest, None)));
        assert!(!withheld(&frame(wire::Kind::Ping, None)));
        assert!(!withheld(&[]), "an empty frame decodes to nothing");
        assert!(!withheld(&[0xff, 0xff, 0xff, 0xff]), "garbage");
    }
}
