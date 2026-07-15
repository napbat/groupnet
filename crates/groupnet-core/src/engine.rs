use std::collections::{BTreeMap, BTreeSet};

use crate::config::Config;
use crate::membership::{Member, StateEntry, Status};
use crate::{GroupId, NodeId, Time, placement, wire};

/// A metadata value tagged with a version and its writer, resolved by
/// last-writer-wins with a deterministic `(version, writer)` tiebreak.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionedValue {
    /// The value.
    pub value: String,
    /// Monotonic version at the writing node.
    pub version: u64,
    /// The node that produced this version (tiebreaker).
    pub writer: NodeId,
}

/// A local instruction to the engine, applied via [`GroupEngine::apply`].
#[derive(Clone, Debug)]
pub enum Command {
    /// Set a shard-local metadata key.
    UpdateMetadata {
        /// Metadata key.
        key: String,
        /// New value.
        value: String,
    },
    /// Replace this node's single-blob state — a compatibility shim over the
    /// keyed model: exactly `SetLocalEntry` on the reserved key `~blob`.
    SetLocalState(Vec<u8>),
    /// Set one key of this node's app-defined state (weight, readiness, an
    /// address, one page of a progress map). Independently versioned per key,
    /// disseminated to peers; `ttl_ms` (if `Some`) lets every receiver expire
    /// the entry that long after last adopting it — the author refreshes by
    /// re-setting.
    SetLocalEntry {
        /// State key. Keys starting with `~` are reserved for groupnet.
        key: String,
        /// The value.
        value: Vec<u8>,
        /// Optional time-to-live in ms.
        ttl_ms: Option<u64>,
    },
    /// Delete one key of this node's state: a versioned tombstone is
    /// disseminated so every peer drops the key, then reaped.
    DeleteLocalEntry {
        /// State key.
        key: String,
    },
    /// This node voluntarily leaves the group.
    Leave,
    /// Introduce a peer learned out-of-band (e.g. from an external roster / service
    /// discovery) so the failure detector starts probing it without waiting to be
    /// contacted first. Idempotent; complements build-time [`seed`]ing.
    ///
    /// [`seed`]: crate::GroupEngine
    AddPeer(NodeId),
}

/// An intent the engine emits in response to an event. The driver carries it
/// out — the engine itself performs no I/O.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    /// Ship `wire` to node `to` (best-effort; the engine tolerates loss).
    Send {
        /// Destination node.
        to: NodeId,
        /// Opaque encoded frame (see [`crate::wire`]).
        wire: Vec<u8>,
    },
    /// Ask the driver to deliver a [`GroupEngine::on_tick`] no later than `at`.
    /// A driver with a coarser timer may tick earlier; the engine is idempotent
    /// under early ticks.
    ArmTimer {
        /// Absolute logical time by which a tick is wanted.
        at: Time,
    },
    /// The coordinator changed (including to/from `None`).
    CoordinatorChanged {
        /// The new coordinator, or `None` if the group is now empty.
        coordinator: Option<NodeId>,
    },
    /// The membership set or a member's status changed.
    MembershipChanged,
    /// One key of a node's app-defined state changed (local write, merged
    /// delta, delete, or a restart-recovery adoption of our own echoed entry).
    NodeStateChanged {
        /// The node whose state changed.
        node: NodeId,
        /// The key that changed.
        key: String,
    },
    /// A metadata key took a new value (from a local write or a merged delta).
    MetadataChanged {
        /// The key that changed.
        key: String,
        /// Its new value.
        value: String,
    },
}

/// Which phase of failure detection an outstanding probe is in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProbePhase {
    /// A direct `Ping` we sent ourselves.
    Direct,
    /// We asked indirect probers to reach the target after a direct miss.
    Indirect,
}

/// The probe currently awaiting a response.
#[derive(Clone, Debug)]
struct Pending {
    target: NodeId,
    deadline: Time,
    phase: ProbePhase,
}

/// The per-group coordination state machine.
///
/// One instance owns the state of exactly one group on one node. It is a plain
/// value: `Send`, cheap to move between threads, and free of any I/O or clock
/// access. Drive it by feeding events ([`on_message`](Self::on_message),
/// [`on_tick`](Self::on_tick), [`apply`](Self::apply)) and executing the
/// [`Effect`]s it returns.
///
/// Membership uses a SWIM-style protocol: direct liveness probes with indirect
/// (`ping-req`) fallback, a `Suspect` state with a refutation window,
/// last-writer-wins merge keyed by per-node incarnation numbers, and reaping of
/// stale `Dead` tombstones.
#[derive(Debug)]
pub struct GroupEngine {
    group: GroupId,
    local: NodeId,
    /// The local node's incarnation, bumped to refute suspicion about itself.
    incarnation: u64,
    /// Set once the local node has voluntarily left (so it won't refute its own
    /// death).
    leaving: bool,
    /// All known members, including self.
    members: BTreeMap<NodeId, Member>,
    /// Bootstrap contacts to disseminate toward before membership is learned.
    seeds: BTreeSet<NodeId>,
    metadata: BTreeMap<String, VersionedValue>,
    coordinator: Option<NodeId>,
    config: Config,
    next_gossip: Time,
    next_probe: Time,
    /// Round-robin cursor over probe candidates.
    probe_cursor: usize,
    /// The outstanding probe awaiting a response, if any.
    pending: Option<Pending>,
    /// As an indirect prober: target -> the origins waiting for us to relay an
    /// ack about it.
    relaying: BTreeMap<NodeId, BTreeSet<NodeId>>,
    /// The most recent logical time observed via `start`/`on_message`/`on_tick`.
    /// Used only where a `Command` (which carries no clock) needs a timestamp —
    /// entry TTLs and tombstone ages; command-path precision is one event-loop
    /// turn, which is far finer than any TTL.
    now_hint: Time,
    /// State keys this process has authored since boot. Echoes of our own
    /// entries are ADOPTED only for keys not in this set (restart recovery);
    /// for authored keys we keep our value and out-version the echo (the
    /// sole-author rule — a peer can never replace what we wrote this boot).
    authored: BTreeSet<String>,
}

impl GroupEngine {
    /// Creates an engine for `local`'s participation in `group`.
    ///
    /// `seeds` are contact nodes to bootstrap against; membership starts as just
    /// `{local}` (alive) and grows as gossip converges.
    pub fn new(
        group: GroupId,
        local: NodeId,
        seeds: impl IntoIterator<Item = NodeId>,
        config: Config,
    ) -> Self {
        let mut members = BTreeMap::new();
        members.insert(local.clone(), Member::new(0, Status::Alive));
        let seeds = seeds
            .into_iter()
            .filter(|p| *p != local)
            .collect::<BTreeSet<_>>();
        let mut engine = Self {
            group,
            local,
            incarnation: 0,
            leaving: false,
            members,
            seeds,
            metadata: BTreeMap::new(),
            coordinator: None,
            config,
            next_gossip: Time::ZERO,
            next_probe: Time::ZERO,
            probe_cursor: 0,
            pending: None,
            relaying: BTreeMap::new(),
            now_hint: Time::ZERO,
            authored: BTreeSet::new(),
        };
        engine.coordinator = engine.compute_coordinator();
        engine
    }

    /// The group this engine belongs to.
    #[must_use]
    pub fn group(&self) -> &GroupId {
        &self.group
    }

    /// This engine's local node id.
    #[must_use]
    pub fn local(&self) -> &NodeId {
        &self.local
    }

    /// The current coordinator, or `None` before any member is known.
    #[must_use]
    pub fn coordinator(&self) -> Option<&NodeId> {
        self.coordinator.as_ref()
    }

    /// Whether the local node is currently the coordinator.
    #[must_use]
    pub fn is_coordinator(&self) -> bool {
        self.coordinator.as_ref() == Some(&self.local)
    }

    /// Iterates the current *live* members (anything not `Dead`) in id order.
    pub fn members(&self) -> impl Iterator<Item = &NodeId> {
        self.members
            .iter()
            .filter(|(_, m)| m.status != Status::Dead)
            .map(|(n, _)| n)
    }

    /// The status of a specific node, if known (including `Dead` tombstones,
    /// until they are reaped).
    #[must_use]
    pub fn member_status(&self, node: &NodeId) -> Option<Status> {
        self.members.get(node).map(|m| m.status)
    }

    /// Iterates every known member with its status, in id order — including
    /// `Suspect` members and `Dead` tombstones that have not yet been reaped. A
    /// consumer that needs the Alive/Suspect/Dead distinction (not just the
    /// not-`Dead` set [`members`](Self::members) yields) reads this.
    pub fn member_statuses(&self) -> impl Iterator<Item = (&NodeId, Status)> {
        self.members.iter().map(|(node, m)| (node, m.status))
    }

    /// The reserved key backing the single-blob state shim.
    pub const BLOB_KEY: &str = "~blob";

    /// One key of a node's state, if known and live (not tombstoned).
    #[must_use]
    pub fn node_entry(&self, node: &NodeId, key: &str) -> Option<&[u8]> {
        self.members
            .get(node)
            .and_then(|m| m.entries.get(key))
            .filter(|e| !e.tombstone)
            .map(|e| e.value.as_slice())
    }

    /// Iterates a node's live state entries in key order.
    pub fn node_entries(&self, node: &NodeId) -> impl Iterator<Item = (&str, &[u8])> {
        self.members
            .get(node)
            .into_iter()
            .flat_map(|m| m.entries.iter())
            .filter(|(_, e)| !e.tombstone)
            .map(|(k, e)| (k.as_str(), e.value.as_slice()))
    }

    /// The single-blob state a node last advertised (the `~blob` shim key).
    #[must_use]
    pub fn node_state(&self, node: &NodeId) -> Option<&[u8]> {
        self.node_entry(node, Self::BLOB_KEY)
    }

    /// This node's own single-blob state (the `~blob` shim key).
    #[must_use]
    pub fn local_state(&self) -> &[u8] {
        self.node_entry(&self.local, Self::BLOB_KEY).unwrap_or(&[])
    }

    /// Iterates every node advertising a non-empty `~blob` state, in id order.
    pub fn node_states_iter(&self) -> impl Iterator<Item = (&NodeId, &[u8])> {
        self.members.keys().filter_map(|node| {
            self.node_state(node).filter(|s| !s.is_empty()).map(|s| (node, s))
        })
    }

    /// Reads a shard-local metadata value.
    #[must_use]
    pub fn metadata(&self, key: &str) -> Option<&str> {
        self.metadata.get(key).map(|v| v.value.as_str())
    }

    /// Iterates all metadata key/value pairs in key order.
    pub fn metadata_iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.metadata
            .iter()
            .map(|(k, v)| (k.as_str(), v.value.as_str()))
    }

    /// Primes the engine: announces this node and arms the first timers. A
    /// driver calls this once, right after construction, passing current time.
    pub fn start(&mut self, now: Time) -> Vec<Effect> {
        self.now_hint = self.now_hint.max(now);
        self.next_gossip = now.saturating_add(self.config.gossip_interval_ms);
        self.next_probe = now.saturating_add(self.config.probe_interval_ms);
        let mut effects = self.gossip(now);
        effects.push(self.arm_timer());
        effects
    }

    /// Applies a local command.
    pub fn apply(&mut self, cmd: Command) -> Vec<Effect> {
        match cmd {
            Command::UpdateMetadata { key, value } => {
                // Bump above the highest version we hold for the key (already
                // the merged max) so a fresh local write supersedes anything we
                // have seen. Peers converge on it via gossip + LWW.
                let version = self.metadata.get(&key).map_or(1, |v| v.version + 1);
                self.metadata.insert(
                    key.clone(),
                    VersionedValue {
                        value: value.clone(),
                        version,
                        writer: self.local.clone(),
                    },
                );
                vec![Effect::MetadataChanged { key, value }]
            }
            Command::SetLocalState(state) => self.set_local_entry(Self::BLOB_KEY, state, None),
            Command::SetLocalEntry { key, value, ttl_ms } => {
                self.set_local_entry(&key, value, ttl_ms)
            }
            Command::DeleteLocalEntry { key } => {
                let Some(m) = self.members.get_mut(&self.local) else {
                    return Vec::new();
                };
                // A tombstone at version+1 supersedes the live entry everywhere;
                // deleting an unknown key still plants a tombstone (idempotent,
                // and it wins over any in-flight older write).
                let version = m.entries.get(&key).map_or(1, |e| e.version + 1);
                m.entries.insert(
                    key.clone(),
                    StateEntry {
                        version,
                        value: Vec::new(),
                        ttl_ms: 0,
                        expires_at: Time::MAX,
                        tombstone: true,
                        tombstone_since: self.now_hint,
                    },
                );
                self.authored.insert(key.clone());
                vec![Effect::NodeStateChanged { node: self.local.clone(), key }]
            }
            Command::Leave => {
                // Declare ourselves Dead at our current incarnation and stop
                // refuting. Dead supersedes Alive at equal incarnation, so the
                // leave sticks as it disseminates on the next gossip round.
                self.leaving = true;
                if let Some(m) = self.members.get_mut(&self.local) {
                    m.status = Status::Dead;
                }
                let mut effects = vec![Effect::MembershipChanged];
                effects.extend(self.recompute_coordinator());
                effects
            }
            Command::AddPeer(node) => {
                // Learn a peer out-of-band so we start probing it even before any
                // gossip has been exchanged. Idempotent: a known node (or self) is
                // left untouched; a new one is inserted Alive at incarnation 0, which
                // any real advertised state supersedes.
                if node == self.local || self.members.contains_key(&node) {
                    return Vec::new();
                }
                self.members.insert(node, Member::new(0, Status::Alive));
                let mut effects = vec![Effect::MembershipChanged];
                effects.extend(self.recompute_coordinator());
                effects
            }
        }
    }

    /// Handles an inbound wire frame from node `from`, observed at `now`.
    pub fn on_message(&mut self, from: NodeId, wire: &[u8], now: Time) -> Vec<Effect> {
        self.now_hint = self.now_hint.max(now);
        let Some(frame) = wire::decode(wire) else {
            return Vec::new(); // undecodable == dropped; transport is best-effort
        };
        if frame.group != self.group {
            return Vec::new(); // not ours
        }

        let mut effects = self.merge_members(frame.members, now);
        effects.extend(self.merge_metadata(frame.metadata));

        match frame.kind {
            wire::Kind::Ping => {
                effects.push(self.frame_to(from, wire::Kind::Ack, None, now));
            }
            wire::Kind::Ack => {
                self.clear_pending_if(&from);
                // As an indirect prober: relay this proof of life to any origins
                // waiting on `from`.
                if let Some(origins) = self.relaying.remove(&from) {
                    for origin in origins {
                        effects.push(self.frame_to(
                            origin,
                            wire::Kind::IndirectAck,
                            Some(from.clone()),
                            now,
                        ));
                    }
                }
            }
            wire::Kind::PingReq => {
                // We were asked to probe `frame.target` on `from`'s behalf.
                if let Some(target) = frame.target {
                    if target != self.local {
                        self.relaying
                            .entry(target.clone())
                            .or_default()
                            .insert(from);
                        effects.push(self.frame_to(target, wire::Kind::Ping, None, now));
                    }
                }
            }
            wire::Kind::IndirectAck => {
                // A prober reports our probe target is alive.
                if let Some(target) = frame.target {
                    self.clear_pending_if(&target);
                }
            }
            wire::Kind::Gossip => {}
        }
        effects
    }

    /// Advances logical time: escalates or expires probes, ages out suspicions
    /// and tombstones, and emits probe / gossip rounds when due.
    pub fn on_tick(&mut self, now: Time) -> Vec<Effect> {
        self.now_hint = self.now_hint.max(now);
        let mut effects = Vec::new();

        // 1. A probe whose window elapsed: escalate a direct miss to indirect,
        //    or declare an indirect miss suspect.
        let expired = self
            .pending
            .as_ref()
            .filter(|p| now >= p.deadline)
            .map(|p| (p.target.clone(), p.phase));
        if let Some((target, phase)) = expired {
            match phase {
                ProbePhase::Direct => effects.extend(self.escalate_indirect(&target, now)),
                ProbePhase::Indirect => {
                    self.pending = None;
                    effects.extend(self.suspect(&target, now));
                }
            }
        }

        // 2. Suspects past their suspicion window -> dead.
        effects.extend(self.reap_suspects(now));

        // 3. Dead tombstones past their reap window -> removed entirely.
        self.reap_dead(now);

        // 3b. Expired state entries and stale entry tombstones.
        self.reap_entries(now);

        // 4. Send the next liveness probe.
        if now >= self.next_probe {
            self.next_probe = now.saturating_add(self.config.probe_interval_ms);
            effects.extend(self.probe(now));
        }

        // 5. Disseminate the full view.
        if now >= self.next_gossip {
            self.next_gossip = now.saturating_add(self.config.gossip_interval_ms);
            effects.extend(self.gossip(now));
        }

        effects.push(self.arm_timer());
        effects
    }

    /// Author one key of our own state: bump above whatever version we hold
    /// (ours or an adopted echo) so the write supersedes every prior copy.
    fn set_local_entry(&mut self, key: &str, value: Vec<u8>, ttl_ms: Option<u64>) -> Vec<Effect> {
        let now = self.now_hint;
        let Some(m) = self.members.get_mut(&self.local) else {
            return Vec::new();
        };
        let version = m.entries.get(key).map_or(1, |e| e.version + 1);
        self.authored.insert(key.to_owned());
        let ttl = ttl_ms.unwrap_or(0);
        m.entries.insert(
            key.to_owned(),
            StateEntry {
                version,
                value,
                ttl_ms: ttl,
                expires_at: if ttl == 0 { Time::MAX } else { now.saturating_add(ttl) },
                tombstone: false,
                tombstone_since: Time::ZERO,
            },
        );
        vec![Effect::NodeStateChanged { node: self.local.clone(), key: key.to_owned() }]
    }

    fn probe(&mut self, now: Time) -> Vec<Effect> {
        if self.pending.is_some() {
            return Vec::new(); // one probe outstanding at a time
        }
        let candidates: Vec<NodeId> = self.probe_candidates().cloned().collect();
        if candidates.is_empty() {
            return Vec::new();
        }
        let target = candidates[self.probe_cursor % candidates.len()].clone();
        self.probe_cursor = self.probe_cursor.wrapping_add(1);
        self.pending = Some(Pending {
            target: target.clone(),
            deadline: now.saturating_add(self.config.probe_timeout_ms),
            phase: ProbePhase::Direct,
        });
        vec![self.frame_to(target, wire::Kind::Ping, None, now)]
    }

    /// A direct probe missed: enlist indirect probers instead of suspecting
    /// outright. This is what prevents a single dropped packet or one-way link
    /// from falsely killing a healthy node.
    fn escalate_indirect(&mut self, target: &NodeId, now: Time) -> Vec<Effect> {
        let probers: Vec<NodeId> = self
            .probe_candidates()
            .filter(|n| **n != *target)
            .take(self.config.indirect_probes.max(1))
            .cloned()
            .collect();
        if probers.is_empty() {
            // No one to ask (tiny cluster) — fall back to direct suspicion.
            self.pending = None;
            return self.suspect(target, now);
        }
        self.pending = Some(Pending {
            target: target.clone(),
            deadline: now.saturating_add(self.config.probe_timeout_ms),
            phase: ProbePhase::Indirect,
        });
        probers
            .into_iter()
            .map(|p| self.frame_to(p, wire::Kind::PingReq, Some(target.clone()), now))
            .collect()
    }

    fn clear_pending_if(&mut self, target: &NodeId) {
        if self.pending.as_ref().is_some_and(|p| p.target == *target) {
            self.pending = None;
        }
    }

    fn suspect(&mut self, target: &NodeId, now: Time) -> Vec<Effect> {
        let became_suspect = match self.members.get_mut(target) {
            Some(m) if m.status == Status::Alive => {
                m.status = Status::Suspect;
                m.suspect_since = now;
                true
            }
            _ => false,
        };
        if !became_suspect {
            return Vec::new();
        }
        let mut effects = vec![Effect::MembershipChanged];
        effects.extend(self.recompute_coordinator());
        effects
    }

    fn reap_suspects(&mut self, now: Time) -> Vec<Effect> {
        let timeout = self.config.suspect_timeout_ms;
        let dead: Vec<NodeId> = self
            .members
            .iter()
            .filter(|(node, m)| {
                **node != self.local
                    && m.status == Status::Suspect
                    && now >= m.suspect_since.saturating_add(timeout)
            })
            .map(|(node, _)| node.clone())
            .collect();
        if dead.is_empty() {
            return Vec::new();
        }
        for node in &dead {
            if let Some(m) = self.members.get_mut(node) {
                m.status = Status::Dead;
                m.dead_since = now;
            }
        }
        let mut effects = vec![Effect::MembershipChanged];
        effects.extend(self.recompute_coordinator());
        effects
    }

    /// Removes `Dead` tombstones that have aged past `2×dead_timeout`. By then
    /// they have stopped being gossiped (see `should_gossip`), so no peer
    /// re-teaches them and the removal converges.
    fn reap_dead(&mut self, now: Time) {
        let reap_after = self.config.dead_timeout_ms.saturating_mul(2);
        let stale: Vec<NodeId> = self
            .members
            .iter()
            .filter(|(node, m)| {
                **node != self.local
                    && m.status == Status::Dead
                    && now >= m.dead_since.saturating_add(reap_after)
            })
            .map(|(node, _)| node.clone())
            .collect();
        for node in stale {
            self.members.remove(&node);
        }
    }

    /// Merges incoming member deltas. Liveness (`incarnation`/`status`, by SWIM
    /// precedence) and app state (`state_version`, by last-writer-wins) are
    /// merged *independently* — a status update never clobbers state, and vice
    /// versa. Also refutes any suspicion about ourselves.
    fn merge_members(&mut self, deltas: Vec<wire::MemberDelta>, now: Time) -> Vec<Effect> {
        let mut membership_changed = false;
        let mut state_changed: Vec<(NodeId, String)> = Vec::new();
        let mut refute_to: Option<u64> = None;

        for delta in deltas {
            let Some(status) = Status::from_wire(delta.status) else {
                continue; // unknown status code — ignore
            };

            if delta.node == self.local {
                // Refute a false suspicion, AND handle restart: if a peer
                // remembers us at a higher incarnation than our (possibly fresh)
                // one, out-incarnate it so our announcements aren't ignored as
                // stale.
                if !self.leaving {
                    let false_suspicion =
                        status != Status::Alive && delta.incarnation >= self.incarnation;
                    let peer_ahead = delta.incarnation > self.incarnation;
                    if false_suspicion || peer_ahead {
                        let target = delta.incarnation + 1;
                        refute_to = Some(refute_to.map_or(target, |t| t.max(target)));
                    }
                }
                // Restart recovery (the wipe fix): our own entries echoed back
                // at versions above what we hold are OUR data from before a
                // restart — ADOPT them verbatim instead of out-versioning them
                // with fresh emptiness (which is exactly how the old blob model
                // briefly wiped a restarted node's state cluster-wide). The app
                // sees a NodeStateChanged for each adopted key and re-authors
                // whatever should be fresher; a later local write bumps above
                // the adopted version and supersedes everywhere.
                for entry in delta.entries {
                    let ours = self.members[&self.local].entries.get(&entry.key);
                    if ours.is_some_and(|e| entry.version <= e.version) {
                        continue; // echo of something we already hold — ignore
                    }
                    let m = self.members.get_mut(&self.local).expect("self present");
                    if self.authored.contains(&entry.key) {
                        // Sole-author rule: never let an echo (or a forgery)
                        // replace a value we wrote this boot. Jump our version
                        // above it and keep re-advertising OUR value, which
                        // then supersedes everywhere.
                        if let Some(e) = m.entries.get_mut(&entry.key) {
                            e.version = entry.version.saturating_add(1);
                            state_changed.push((self.local.clone(), entry.key));
                        }
                    } else {
                        // Restart recovery (the wipe fix): a key we have NOT
                        // authored this boot, echoed at a higher version, is
                        // our own pre-restart data — adopt it verbatim instead
                        // of out-versioning it with fresh emptiness. The app
                        // sees NodeStateChanged and re-authors what should be
                        // fresher.
                        m.entries.insert(
                            entry.key.clone(),
                            StateEntry {
                                version: entry.version,
                                value: entry.value,
                                ttl_ms: entry.ttl_ms,
                                expires_at: if entry.ttl_ms == 0 {
                                    Time::MAX
                                } else {
                                    now.saturating_add(entry.ttl_ms)
                                },
                                tombstone: entry.tombstone,
                                tombstone_since: if entry.tombstone { now } else { Time::ZERO },
                            },
                        );
                        state_changed.push((self.local.clone(), entry.key));
                    }
                }
                continue;
            }

            match self.members.get(&delta.node) {
                None => {
                    // Unknown node: adopt its liveness and state wholesale.
                    let mut member = Member::new(delta.incarnation, status);
                    match status {
                        Status::Suspect => member.suspect_since = now,
                        Status::Dead => member.dead_since = now,
                        Status::Alive => {}
                    }
                    for entry in delta.entries {
                        state_changed.push((delta.node.clone(), entry.key.clone()));
                        member.entries.insert(
                            entry.key,
                            StateEntry {
                                version: entry.version,
                                value: entry.value,
                                ttl_ms: entry.ttl_ms,
                                expires_at: if entry.ttl_ms == 0 {
                                    Time::MAX
                                } else {
                                    now.saturating_add(entry.ttl_ms)
                                },
                                tombstone: entry.tombstone,
                                tombstone_since: if entry.tombstone { now } else { Time::ZERO },
                            },
                        );
                    }
                    self.members.insert(delta.node.clone(), member);
                    membership_changed = true;
                }
                Some(cur) => {
                    let status_wins = cur.superseded_by(delta.incarnation, status);
                    let member = self.members.get_mut(&delta.node).expect("present");
                    if status_wins {
                        member.incarnation = delta.incarnation;
                        member.status = status;
                        match status {
                            Status::Suspect => member.suspect_since = now,
                            Status::Dead => member.dead_since = now,
                            Status::Alive => {}
                        }
                        membership_changed = true;
                    }
                    // Per-key LWW, independent of liveness: each entry is
                    // single-writer (its owning node), so version order alone
                    // decides; a fresher version also re-arms the local TTL.
                    for entry in delta.entries {
                        let wins = member
                            .entries
                            .get(&entry.key)
                            .is_none_or(|e| entry.version > e.version);
                        if !wins {
                            continue;
                        }
                        member.entries.insert(
                            entry.key.clone(),
                            StateEntry {
                                version: entry.version,
                                value: entry.value,
                                ttl_ms: entry.ttl_ms,
                                expires_at: if entry.ttl_ms == 0 {
                                    Time::MAX
                                } else {
                                    now.saturating_add(entry.ttl_ms)
                                },
                                tombstone: entry.tombstone,
                                tombstone_since: if entry.tombstone { now } else { Time::ZERO },
                            },
                        );
                        state_changed.push((delta.node.clone(), entry.key));
                    }
                }
            }
        }

        if let Some(new_incarnation) = refute_to {
            self.incarnation = new_incarnation;
            if let Some(m) = self.members.get_mut(&self.local) {
                m.incarnation = new_incarnation;
                m.status = Status::Alive;
            }
            membership_changed = true;
        }

        let mut effects = Vec::new();
        if membership_changed {
            effects.push(Effect::MembershipChanged);
            effects.extend(self.recompute_coordinator());
        }
        for (node, key) in state_changed {
            effects.push(Effect::NodeStateChanged { node, key });
        }
        effects
    }

    /// Merges incoming metadata deltas by last-writer-wins: an entry is adopted
    /// iff its `(version, writer)` strictly exceeds what we hold. A per-key
    /// LWW-register (a CRDT), so all replicas converge on one value.
    fn merge_metadata(&mut self, incoming: Vec<wire::MetaDelta>) -> Vec<Effect> {
        let mut effects = Vec::new();
        for wire::MetaDelta {
            key,
            version,
            writer,
            value,
        } in incoming
        {
            let wins = match self.metadata.get(&key) {
                Some(local) => (version, &writer) > (local.version, &local.writer),
                None => true,
            };
            if wins {
                self.metadata.insert(
                    key.clone(),
                    VersionedValue {
                        value: value.clone(),
                        version,
                        writer,
                    },
                );
                effects.push(Effect::MetadataChanged { key, value });
            }
        }
        effects
    }

    /// Live members (excluding self) we may probe or gossip to.
    fn probe_candidates(&self) -> impl Iterator<Item = &NodeId> {
        self.members
            .iter()
            .filter(|(node, m)| **node != self.local && m.status != Status::Dead)
            .map(|(node, _)| node)
    }

    fn dissemination_targets(&self) -> Vec<NodeId> {
        let mut set: BTreeSet<NodeId> = self.probe_candidates().cloned().collect();
        set.extend(self.seeds.iter().cloned());
        set.into_iter().collect()
    }

    fn gossip(&self, now: Time) -> Vec<Effect> {
        let targets = self.dissemination_targets();
        if targets.is_empty() {
            return Vec::new();
        }
        let frame = self.encode_frame(wire::Kind::Gossip, None, now);
        targets
            .into_iter()
            .take(self.config.fanout.max(1))
            .map(|to| Effect::Send {
                to,
                wire: frame.clone(),
            })
            .collect()
    }

    /// Builds a `Send` effect of `kind` addressed to `to`.
    fn frame_to(&self, to: NodeId, kind: wire::Kind, target: Option<NodeId>, now: Time) -> Effect {
        Effect::Send {
            to,
            wire: self.encode_frame(kind, target, now),
        }
    }

    fn encode_frame(&self, kind: wire::Kind, target: Option<NodeId>, now: Time) -> Vec<u8> {
        wire::encode(&wire::Frame {
            kind,
            group: self.group.clone(),
            target,
            members: self
                .members
                .iter()
                .filter(|(_, m)| self.should_gossip(m, now))
                .map(|(node, m)| wire::MemberDelta {
                    node: node.clone(),
                    incarnation: m.incarnation,
                    status: m.status.to_wire(),
                    entries: m
                        .entries
                        .iter()
                        .filter(|(_, e)| self.should_gossip_entry(e, now))
                        .map(|(key, e)| wire::EntryDelta {
                            key: key.clone(),
                            version: e.version,
                            ttl_ms: e.ttl_ms,
                            tombstone: e.tombstone,
                            value: e.value.clone(),
                        })
                        .collect(),
                })
                .collect(),
            metadata: self
                .metadata
                .iter()
                .map(|(key, v)| wire::MetaDelta {
                    key: key.clone(),
                    version: v.version,
                    writer: v.writer.clone(),
                    value: v.value.clone(),
                })
                .collect(),
        })
    }

    /// A `Dead` member is advertised only until `dead_timeout` elapses; after
    /// that peers are assumed to know, and dropping it from gossip lets everyone
    /// reap the tombstone without re-teaching each other.
    fn should_gossip(&self, m: &Member, now: Time) -> bool {
        m.status != Status::Dead || now < m.dead_since.saturating_add(self.config.dead_timeout_ms)
    }

    /// An entry is advertised while live and unexpired; a tombstone only until
    /// `dead_timeout` (after that peers are assumed to know — same shape as a
    /// Dead member tombstone).
    fn should_gossip_entry(&self, e: &StateEntry, now: Time) -> bool {
        if e.tombstone {
            return now < e.tombstone_since.saturating_add(self.config.dead_timeout_ms);
        }
        !e.expired(now)
    }

    /// Drop expired TTL entries (they converge to absent everywhere once the
    /// author stops refreshing — no tombstone needed) and reap entry
    /// tombstones past `2×dead_timeout` (no longer gossiped after 1×, so no
    /// peer re-teaches them).
    fn reap_entries(&mut self, now: Time) {
        let reap_after = self.config.dead_timeout_ms.saturating_mul(2);
        for member in self.members.values_mut() {
            member.entries.retain(|_, e| {
                if e.tombstone {
                    now < e.tombstone_since.saturating_add(reap_after)
                } else {
                    !e.expired(now)
                }
            });
        }
    }

    fn compute_coordinator(&self) -> Option<NodeId> {
        // The coordinator is just the placement owner of the group id among live
        // members (Alive or Suspect); a Dead node is never a candidate. Same
        // HA-hash the public `placement` API exposes.
        let live: BTreeSet<NodeId> = self.members().cloned().collect();
        placement::owner(self.group.as_str(), &live)
    }

    fn recompute_coordinator(&mut self) -> Vec<Effect> {
        let next = self.compute_coordinator();
        if next == self.coordinator {
            return Vec::new();
        }
        self.coordinator = next.clone();
        vec![Effect::CoordinatorChanged { coordinator: next }]
    }

    fn arm_timer(&self) -> Effect {
        let mut at = self.next_gossip.min(self.next_probe);
        if let Some(p) = &self.pending {
            at = at.min(p.deadline);
        }
        for m in self.members.values() {
            if m.status == Status::Suspect {
                at = at.min(
                    m.suspect_since
                        .saturating_add(self.config.suspect_timeout_ms),
                );
            }
        }
        Effect::ArmTimer { at }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn engine(id: &str, seeds: &[&str]) -> GroupEngine {
        GroupEngine::new(
            GroupId::new("g"),
            NodeId::new(id),
            seeds.iter().map(|s| NodeId::new(*s)),
            Config::default(),
        )
    }

    fn gossip_frame(members: Vec<wire::MemberDelta>, metadata: Vec<wire::MetaDelta>) -> Vec<u8> {
        wire::encode(&wire::Frame {
            kind: wire::Kind::Gossip,
            group: GroupId::new("g"),
            target: None,
            members,
            metadata,
        })
    }

    fn member(node: &str, inc: u64, status: Status) -> wire::MemberDelta {
        wire::MemberDelta {
            node: NodeId::new(node),
            incarnation: inc,
            status: status.to_wire(),
            entries: Vec::new(),
        }
    }

    #[test]
    fn start_announces_to_seeds() {
        let mut e = engine("a", &["b", "c"]);
        let sends = e
            .start(Time::ZERO)
            .iter()
            .filter(|e| matches!(e, Effect::Send { .. }))
            .count();
        assert_eq!(sends, 2, "should announce to both seeds");
    }

    #[test]
    fn learns_members_and_recomputes_coordinator() {
        let mut a = engine("a", &["b"]);
        a.on_message(
            NodeId::new("b"),
            &gossip_frame(vec![member("b", 0, Status::Alive)], vec![]),
            Time(1),
        );
        assert_eq!(a.members().count(), 2);
        let set: BTreeSet<NodeId> = [NodeId::new("a"), NodeId::new("b")].into_iter().collect();
        assert_eq!(a.coordinator().cloned(), placement::owner("g", &set));
    }

    #[test]
    fn two_node_probe_leads_to_suspect_then_dead() {
        let cfg = Config {
            probe_interval_ms: 100,
            probe_timeout_ms: 50,
            suspect_timeout_ms: 200,
            gossip_interval_ms: 100,
            ..Config::default()
        };
        let mut a = GroupEngine::new(GroupId::new("g"), NodeId::new("a"), [NodeId::new("b")], cfg);
        a.on_message(
            NodeId::new("b"),
            &gossip_frame(vec![member("b", 0, Status::Alive)], vec![]),
            Time(1),
        );
        a.start(Time(1));

        // With no third node to relay, a direct miss falls straight through to
        // suspicion.
        a.on_tick(Time(101)); // sends the direct probe (deadline 151)
        a.on_tick(Time(160)); // window elapsed, no probers -> suspect
        assert_eq!(a.member_status(&NodeId::new("b")), Some(Status::Suspect));

        a.on_tick(Time(400)); // suspicion window elapsed -> dead
        assert_eq!(a.member_status(&NodeId::new("b")), Some(Status::Dead));
        assert!(!a.members().any(|n| *n == NodeId::new("b")));
    }

    #[test]
    fn direct_miss_escalates_to_indirect_before_suspecting() {
        let cfg = Config {
            probe_interval_ms: 1000,
            probe_timeout_ms: 50,
            ..Config::default()
        };
        let mut a = GroupEngine::new(GroupId::new("g"), NodeId::new("a"), [], cfg);
        // Learn b and c.
        a.on_message(
            NodeId::new("b"),
            &gossip_frame(
                vec![member("b", 0, Status::Alive), member("c", 0, Status::Alive)],
                vec![],
            ),
            Time(1),
        );
        a.start(Time(1));

        a.on_tick(Time(1001)); // direct probe to first candidate (b)
        let effects = a.on_tick(Time(1100)); // direct miss -> ping-req, NOT suspect
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::Send { to, .. } if *to == NodeId::new("c"))),
            "should ask c to probe b indirectly"
        );
        assert_eq!(a.member_status(&NodeId::new("b")), Some(Status::Alive));

        // An indirect ack keeps b alive.
        a.on_message(
            NodeId::new("c"),
            &wire::encode(&wire::Frame {
                kind: wire::Kind::IndirectAck,
                group: GroupId::new("g"),
                target: Some(NodeId::new("b")),
                members: vec![],
                metadata: vec![],
            }),
            Time(1120),
        );
        a.on_tick(Time(2000));
        assert_eq!(a.member_status(&NodeId::new("b")), Some(Status::Alive));
    }

    #[test]
    fn ping_req_makes_us_probe_and_relay() {
        let mut p = engine("p", &[]);
        // origin o asks p to probe t.
        let effects = p.on_message(
            NodeId::new("o"),
            &wire::encode(&wire::Frame {
                kind: wire::Kind::PingReq,
                group: GroupId::new("g"),
                target: Some(NodeId::new("t")),
                members: vec![],
                metadata: vec![],
            }),
            Time(1),
        );
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::Send { to, .. } if *to == NodeId::new("t"))),
            "prober should ping the target"
        );
        // When t acks, we relay an IndirectAck back to the origin o.
        let ack = p.on_message(
            NodeId::new("t"),
            &wire::encode(&wire::Frame {
                kind: wire::Kind::Ack,
                group: GroupId::new("g"),
                target: None,
                members: vec![],
                metadata: vec![],
            }),
            Time(3),
        );
        assert!(
            ack.iter()
                .any(|e| matches!(e, Effect::Send { to, .. } if *to == NodeId::new("o"))),
            "should relay an indirect ack to the origin"
        );
    }

    #[test]
    fn refutes_false_suspicion_about_self() {
        let mut a = engine("a", &["b"]);
        a.on_message(
            NodeId::new("b"),
            &gossip_frame(vec![member("a", 0, Status::Suspect)], vec![]),
            Time(1),
        );
        assert_eq!(a.member_status(&NodeId::new("a")), Some(Status::Alive));
        let frame =
            wire::decode(&a.encode_frame(wire::Kind::Gossip, None, Time(2))).expect("frame");
        let self_delta = frame
            .members
            .iter()
            .find(|m| m.node == NodeId::new("a"))
            .expect("self in frame");
        assert_eq!(self_delta.status, Status::Alive.to_wire());
        assert!(
            self_delta.incarnation >= 1,
            "refutation should bump incarnation"
        );
    }

    #[test]
    fn voluntary_leave_is_not_refuted() {
        let mut a = engine("a", &["b"]);
        a.apply(Command::Leave);
        assert_eq!(a.member_status(&NodeId::new("a")), Some(Status::Dead));
        a.on_message(
            NodeId::new("b"),
            &gossip_frame(vec![member("a", 0, Status::Dead)], vec![]),
            Time(1),
        );
        assert_eq!(a.member_status(&NodeId::new("a")), Some(Status::Dead));
    }

    #[test]
    fn metadata_merges_by_last_writer_wins() {
        let mut a = engine("a", &["b"]);
        let meta = |ver, writer: &str, val: &str| {
            gossip_frame(
                vec![],
                vec![wire::MetaDelta {
                    key: "k".into(),
                    version: ver,
                    writer: NodeId::new(writer),
                    value: val.into(),
                }],
            )
        };

        a.on_message(NodeId::new("z"), &meta(2, "z", "remote"), Time(1));
        assert_eq!(a.metadata("k"), Some("remote"));

        a.apply(Command::UpdateMetadata {
            key: "k".into(),
            value: "local".into(),
        });
        assert_eq!(a.metadata("k"), Some("local")); // 2 -> 3 beats remote

        a.on_message(NodeId::new("z"), &meta(1, "z", "stale"), Time(2));
        assert_eq!(a.metadata("k"), Some("local")); // stale ignored
    }

    /// A member delta carrying only app state (alive, no liveness change) on
    /// the single-blob shim key.
    fn state_delta(node: &str, version: u64, state: &[u8]) -> wire::MemberDelta {
        wire::MemberDelta {
            node: NodeId::new(node),
            incarnation: 0,
            status: Status::Alive.to_wire(),
            entries: vec![wire::EntryDelta {
                key: GroupEngine::BLOB_KEY.to_owned(),
                version,
                ttl_ms: 0,
                tombstone: false,
                value: state.to_vec(),
            }],
        }
    }

    #[test]
    fn per_node_state_merges_by_last_writer_wins() {
        let mut a = engine("a", &["b"]);

        // Learn b's state at version 2.
        a.on_message(
            NodeId::new("b"),
            &gossip_frame(vec![state_delta("b", 2, b"v2")], vec![]),
            Time(1),
        );
        assert_eq!(a.node_state(&NodeId::new("b")), Some(&b"v2"[..]));

        // A newer version wins; a stale one is ignored.
        a.on_message(
            NodeId::new("b"),
            &gossip_frame(vec![state_delta("b", 3, b"v3")], vec![]),
            Time(2),
        );
        assert_eq!(a.node_state(&NodeId::new("b")), Some(&b"v3"[..]));
        a.on_message(
            NodeId::new("b"),
            &gossip_frame(vec![state_delta("b", 1, b"old")], vec![]),
            Time(3),
        );
        assert_eq!(a.node_state(&NodeId::new("b")), Some(&b"v3"[..]));
    }

    #[test]
    fn a_node_authors_only_its_own_state() {
        let mut a = engine("a", &["b"]);
        a.apply(Command::SetLocalState(b"mine".to_vec()));
        assert_eq!(a.local_state(), b"mine");

        // A peer's claim about *our* state is ignored — we're the sole author.
        a.on_message(
            NodeId::new("b"),
            &gossip_frame(vec![state_delta("a", 999, b"forged")], vec![]),
            Time(1),
        );
        assert_eq!(a.local_state(), b"mine");
    }

    #[test]
    fn state_and_liveness_merge_independently() {
        let mut a = engine("a", &["b"]);
        // Learn b alive with state v1.
        a.on_message(
            NodeId::new("b"),
            &gossip_frame(vec![state_delta("b", 1, b"s1")], vec![]),
            Time(1),
        );
        // A pure liveness update (suspect, no newer state) must not wipe state.
        a.on_message(
            NodeId::new("c"),
            &gossip_frame(vec![member("b", 0, Status::Suspect)], vec![]),
            Time(2),
        );
        assert_eq!(a.member_status(&NodeId::new("b")), Some(Status::Suspect));
        assert_eq!(
            a.node_state(&NodeId::new("b")),
            Some(&b"s1"[..]),
            "state survived a status change"
        );
    }

    // ---- keyed per-node state (G1) + restart recovery (G2) ----

    fn entry(key: &str, version: u64, ttl_ms: u64, tombstone: bool, value: &[u8]) -> wire::EntryDelta {
        wire::EntryDelta {
            key: key.to_owned(),
            version,
            ttl_ms,
            tombstone,
            value: value.to_vec(),
        }
    }

    fn entries_delta(node: &str, entries: Vec<wire::EntryDelta>) -> wire::MemberDelta {
        wire::MemberDelta {
            node: NodeId::new(node),
            incarnation: 0,
            status: Status::Alive.to_wire(),
            entries,
        }
    }

    #[test]
    fn keys_version_independently() {
        let mut a = engine("a", &["b"]);
        a.on_message(
            NodeId::new("b"),
            &gossip_frame(
                vec![entries_delta("b", vec![entry("x", 5, 0, false, b"x5"), entry("y", 1, 0, false, b"y1")])],
                vec![],
            ),
            Time(1),
        );
        // A fresher y does not disturb x; a stale x is ignored.
        a.on_message(
            NodeId::new("b"),
            &gossip_frame(
                vec![entries_delta("b", vec![entry("x", 4, 0, false, b"stale"), entry("y", 2, 0, false, b"y2")])],
                vec![],
            ),
            Time(2),
        );
        assert_eq!(a.node_entry(&NodeId::new("b"), "x"), Some(&b"x5"[..]));
        assert_eq!(a.node_entry(&NodeId::new("b"), "y"), Some(&b"y2"[..]));
        let keys: Vec<&str> = a.node_entries(&NodeId::new("b")).map(|(k, _)| k).collect();
        assert_eq!(keys, ["x", "y"]);
    }

    #[test]
    fn ttl_entries_expire_and_a_refresh_rearms() {
        let mut a = engine("a", &["b"]);
        a.on_message(
            NodeId::new("b"),
            &gossip_frame(vec![entries_delta("b", vec![entry("hot", 1, 100, false, b"v1")])], vec![]),
            Time(0),
        );
        assert!(a.node_entry(&NodeId::new("b"), "hot").is_some());
        // A fresher version at t=60 re-arms the expiry to t=160.
        a.on_message(
            NodeId::new("b"),
            &gossip_frame(vec![entries_delta("b", vec![entry("hot", 2, 100, false, b"v2")])], vec![]),
            Time(60),
        );
        a.on_tick(Time(120)); // old deadline passed, refreshed one hasn't
        assert_eq!(a.node_entry(&NodeId::new("b"), "hot"), Some(&b"v2"[..]));
        a.on_tick(Time(161));
        assert_eq!(a.node_entry(&NodeId::new("b"), "hot"), None, "expired after ttl");
    }

    #[test]
    fn delete_disseminates_a_tombstone_then_reaps_it() {
        let mut a = engine("a", &["b"]);
        a.apply(Command::SetLocalEntry { key: "k".into(), value: b"v".to_vec(), ttl_ms: None });
        a.apply(Command::DeleteLocalEntry { key: "k".into() });
        assert_eq!(a.node_entry(&NodeId::new("a"), "k"), None, "deleted locally");

        // The tombstone is still gossiped (so peers drop the key)...
        let frame = wire::decode(&a.on_tick(Time(1_000)).iter().find_map(|e| match e {
            Effect::Send { wire, .. } => Some(wire.clone()),
            _ => None,
        }).expect("a gossip send")).expect("decodes");
        let m = frame.members.iter().find(|m| m.node.as_str() == "a").expect("self delta");
        assert!(m.entries.iter().any(|e| e.key == "k" && e.tombstone), "tombstone advertised");

        // ...and is reaped (and no longer advertised) after 2× dead_timeout.
        let far = Time(1_000 + Config::default().dead_timeout_ms * 2 + 1);
        a.on_tick(far);
        let frame = wire::decode(&a.on_tick(far.saturating_add(10_000)).iter().find_map(|e| match e {
            Effect::Send { wire, .. } => Some(wire.clone()),
            _ => None,
        }).expect("a gossip send")).expect("decodes");
        let m = frame.members.iter().find(|m| m.node.as_str() == "a").expect("self delta");
        assert!(!m.entries.iter().any(|e| e.key == "k"), "tombstone reaped");
    }

    #[test]
    fn restart_adopts_echoed_entries_for_unauthored_keys() {
        // Fresh engine (a restart): a peer echoes entries we authored last boot.
        let mut a = engine("a", &["b"]);
        let effects = a.on_message(
            NodeId::new("b"),
            &gossip_frame(vec![entries_delta("a", vec![entry("addr", 7, 0, false, b"10.0.0.1")])], vec![]),
            Time(1),
        );
        // Adopted verbatim (NOT wiped by out-versioning with emptiness)...
        assert_eq!(a.node_entry(&NodeId::new("a"), "addr"), Some(&b"10.0.0.1"[..]));
        // ...with a change event so the app can re-author.
        assert!(effects.iter().any(|e| matches!(
            e,
            Effect::NodeStateChanged { node, key } if node.as_str() == "a" && key == "addr"
        )));
        // A post-restart local write supersedes the adopted version everywhere.
        a.apply(Command::SetLocalEntry { key: "addr".into(), value: b"10.0.0.2".to_vec(), ttl_ms: None });
        assert_eq!(a.node_entry(&NodeId::new("a"), "addr"), Some(&b"10.0.0.2"[..]));

        // And once authored this boot, echoes can never replace it (sole-author
        // rule): the forged/echoed 999 only bumps our version past it.
        a.on_message(
            NodeId::new("b"),
            &gossip_frame(vec![entries_delta("a", vec![entry("addr", 999, 0, false, b"forged")])], vec![]),
            Time(2),
        );
        assert_eq!(a.node_entry(&NodeId::new("a"), "addr"), Some(&b"10.0.0.2"[..]));
    }
}
