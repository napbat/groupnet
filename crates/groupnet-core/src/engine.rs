use std::collections::{BTreeMap, BTreeSet};

use crate::config::Config;
use crate::membership::{Member, StateEntry, Status};
use crate::{GroupId, NodeId, Time, placement, wire};

/// One step of an FNV-1a fold (dep-free), used to hash a member's held entries
/// into the digest's content hash.
fn fnv1a(mut h: u64, bytes: &[u8]) -> u64 {
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

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
/// stale `Dead` tombstones. Probes carry no piggybacked view — they are tiny.
///
/// Dissemination is **digest/delta anti-entropy** (G3). Each node stamps every
/// one of its own keys from a single monotonic per-node version clock, so a
/// scalar high-water mark per member summarizes its whole keyed map. The
/// periodic round emits compact [`wire::Kind::Digest`] frames (per-node
/// `(incarnation, status, max_version)` plus the small metadata register set); a
/// receiver compares them against its own high-water marks and either requests
/// the gap ([`wire::Kind::DeltaRequest`]) or pushes it ([`wire::Kind::Delta`],
/// bounded per frame — successive rounds converge the rest).
///
/// **Reap-horizon invariant.** A member's high-water mark only ever rises;
/// reaping a tombstone or expiring a TTL entry drops the value but never lowers
/// the mark. Because a delta only ever carries entries *strictly newer* than the
/// peer's advertised mark, an entry reaped past the horizon — whose version is
/// below every connected peer's mark, since its tombstone was gossiped for a
/// full `dead_timeout` before anyone reaped it — can never be regenerated. A
/// node partitioned longer than the reap window (`2×dead_timeout`) is outside
/// the horizon and may retain a reaped entry, exactly as a `Dead`-member
/// tombstone already requires; set `dead_timeout` above the longest survivable
/// partition.
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
    /// When the next periodic anti-entropy (digest) round is due.
    next_anti_entropy: Time,
    next_probe: Time,
    /// Round-robin cursor over probe candidates.
    probe_cursor: usize,
    /// Round-robin cursor over dissemination targets, so digest fanout rotates
    /// across peers instead of forever favouring the lowest ids.
    gossip_cursor: usize,
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
    /// Monotonic change clock: bumped whenever any member's digest-visible
    /// summary changes; the member is stamped with it (`Member::changed_at`)
    /// so per-peer delta digests can list only "changed since I last
    /// digested to you".
    change_clock: u64,
    /// Per peer: the change-clock value as of the last digest built for it.
    digest_cursors: BTreeMap<NodeId, u64>,
    /// Per peer: digests built for it so far (drives the full-digest cadence).
    digest_visits: BTreeMap<NodeId, u64>,
    /// Cumulative anti-entropy traffic counters.
    stats: NetStats,
}

/// Cumulative anti-entropy traffic counters for one engine (one node's view
/// of one group), read via [`GroupEngine::net_stats`] (the runtime exposes
/// them per group). The ratio to watch at scale is
/// `digest_summaries_listed / digests_built`: with per-peer delta digests it
/// tracks recent churn, not membership size — if it tracks membership size,
/// the group has outgrown its cadence or `full_digest_every` (see the
/// README's scaling envelope).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct NetStats {
    /// Digests built (one per fanout target per round; delta + full).
    pub digests_built: u64,
    /// How many of those were full digests (every gossipable member listed).
    pub full_digests_built: u64,
    /// Member summaries listed across all digests.
    pub digest_summaries_listed: u64,
    /// Encoded digest frames handed to the transport (budget chunking means
    /// one digest can span several frames).
    pub digest_frames_sent: u64,
    /// Delta frames handed to the transport (anti-entropy backfill, offers,
    /// and eager push).
    pub delta_frames_sent: u64,
    /// Delta-request frames sent (including truncation continuations).
    pub delta_requests_sent: u64,
    /// Total encoded bytes of the frames counted above. Constant-size probe
    /// frames (ping/ack) are excluded — this measures the traffic that
    /// scales with state, not liveness.
    pub anti_entropy_bytes_sent: u64,
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
        let mut own = Member::new(0, Status::Alive);
        own.changed_at = 1; // stamped at clock 1, so a first digest lists us
        members.insert(local.clone(), own);
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
            next_anti_entropy: Time::ZERO,
            next_probe: Time::ZERO,
            probe_cursor: 0,
            gossip_cursor: 0,
            pending: None,
            relaying: BTreeMap::new(),
            now_hint: Time::ZERO,
            authored: BTreeSet::new(),
            change_clock: 1,
            digest_cursors: BTreeMap::new(),
            digest_visits: BTreeMap::new(),
            stats: NetStats::default(),
        };
        engine.coordinator = engine.compute_coordinator();
        engine
    }

    /// Cumulative anti-entropy traffic counters (see [`NetStats`]).
    #[must_use]
    pub const fn net_stats(&self) -> NetStats {
        self.stats
    }

    /// Bumps the change clock and stamps `node`'s member record, so the next
    /// delta digest to any peer re-advertises this member. Cheap enough to
    /// call once per mutation site; over-stamping only costs a digest line.
    fn stamp(&mut self, node: &NodeId) {
        self.change_clock += 1;
        let stamp = self.change_clock;
        if let Some(m) = self.members.get_mut(node) {
            m.changed_at = stamp;
        }
    }

    /// [`stamp`](Self::stamp) for the local member, avoiding a borrow clash.
    fn stamp_self(&mut self) {
        self.change_clock += 1;
        let stamp = self.change_clock;
        if let Some(m) = self.members.get_mut(&self.local) {
            m.changed_at = stamp;
        }
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
            self.node_state(node)
                .filter(|s| !s.is_empty())
                .map(|s| (node, s))
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

    /// Primes the engine: announces this node (a first digest) and arms the
    /// first timers. A driver calls this once, right after construction, passing
    /// current time.
    pub fn start(&mut self, now: Time) -> Vec<Effect> {
        self.now_hint = self.now_hint.max(now);
        self.next_anti_entropy = now.saturating_add(self.anti_entropy_interval());
        self.next_probe = now.saturating_add(self.config.probe_interval_ms);
        let mut effects = self.disseminate_digest(now);
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
                self.nudge_anti_entropy();
                vec![Effect::MetadataChanged { key, value }]
            }
            Command::SetLocalState(state) => self.set_local_entry(Self::BLOB_KEY, state, None),
            Command::SetLocalEntry { key, value, ttl_ms } => {
                self.set_local_entry(&key, value, ttl_ms)
            }
            Command::DeleteLocalEntry { key } => {
                let now = self.now_hint;
                let Some(m) = self.members.get_mut(&self.local) else {
                    return Vec::new();
                };
                // A tombstone at the next per-node version supersedes the live
                // entry everywhere; deleting an unknown key still plants a
                // tombstone (idempotent, and it wins over any in-flight write).
                let version = m.max_state_version.saturating_add(1);
                m.max_state_version = version;
                m.entries.insert(
                    key.clone(),
                    StateEntry {
                        version,
                        value: Vec::new(),
                        ttl_ms: 0,
                        expires_at: Time::MAX,
                        tombstone: true,
                        tombstone_since: now,
                    },
                );
                self.authored.insert(key.clone());
                self.stamp_self();
                self.nudge_anti_entropy();
                let mut effects = vec![Effect::NodeStateChanged {
                    node: self.local.clone(),
                    key,
                }];
                effects.extend(self.eager_push());
                effects
            }
            Command::Leave => {
                // Declare ourselves Dead at our current incarnation and stop
                // refuting. Dead supersedes Alive at equal incarnation, so the
                // leave sticks as it disseminates on the next digest round.
                self.leaving = true;
                if let Some(m) = self.members.get_mut(&self.local) {
                    m.status = Status::Dead;
                }
                self.stamp_self();
                let mut effects = vec![Effect::MembershipChanged];
                effects.extend(self.recompute_coordinator());
                self.nudge_anti_entropy();
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
                self.members
                    .insert(node.clone(), Member::new(0, Status::Alive));
                self.stamp(&node);
                let mut effects = vec![Effect::MembershipChanged];
                effects.extend(self.recompute_coordinator());
                self.nudge_anti_entropy();
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

        match frame.kind {
            wire::Kind::Digest => self.on_digest(&from, &frame, now),
            wire::Kind::DeltaRequest => self.on_delta_request(&from, &frame, now),
            wire::Kind::Delta => {
                // Remember what each member section *advertised* so a
                // truncated backfill can be continued: if, after merging, our
                // stored high-water for a member still exceeds the frame's
                // advertised max, the sender (or a third party) has entries we
                // did not receive — re-request from where this frame stopped.
                // The entries guard keeps max-only advancement frames (used to
                // jump past reaped tails) from triggering spurious requests,
                // and a peer with nothing above `have` simply builds no frame,
                // so continuation cannot loop.
                let carried: Vec<(NodeId, u64, bool)> = frame
                    .members
                    .iter()
                    .map(|md| (md.node.clone(), md.max_version, !md.entries.is_empty()))
                    .collect();
                let mut effects = self.merge_members(frame.members, now);
                let wants: Vec<wire::NodeWant> = carried
                    .into_iter()
                    .filter(|(node, advertised, had_entries)| {
                        *had_entries
                            && *node != self.local
                            && self
                                .members
                                .get(node)
                                .is_some_and(|m| m.max_state_version > *advertised)
                    })
                    .map(|(node, advertised, _)| wire::NodeWant {
                        node,
                        have_version: advertised,
                    })
                    .collect();
                if !wants.is_empty() {
                    effects.push(self.send_delta_request(from.clone(), wants));
                }
                effects
            }
            wire::Kind::Ping => {
                // Prove we're alive. The ack carries no view (anti-entropy owns
                // dissemination); it is a bare liveness token.
                vec![self.send_probe(from, wire::Kind::Ack, None)]
            }
            wire::Kind::Ack => {
                let mut effects = Vec::new();
                self.clear_pending_if(&from);
                // As an indirect prober: relay this proof of life to any origins
                // waiting on `from`.
                if let Some(origins) = self.relaying.remove(&from) {
                    for origin in origins {
                        effects.push(self.send_probe(
                            origin,
                            wire::Kind::IndirectAck,
                            Some(from.clone()),
                        ));
                    }
                }
                effects
            }
            wire::Kind::PingReq => {
                // We were asked to probe `frame.target` on `from`'s behalf.
                let mut effects = Vec::new();
                if let Some(target) = frame.target {
                    if target != self.local {
                        self.relaying
                            .entry(target.clone())
                            .or_default()
                            .insert(from);
                        effects.push(self.send_probe(target, wire::Kind::Ping, None));
                    }
                }
                effects
            }
            wire::Kind::IndirectAck => {
                // A prober reports our probe target is alive.
                if let Some(target) = frame.target {
                    self.clear_pending_if(&target);
                }
                Vec::new()
            }
        }
    }

    /// Advances logical time: escalates or expires probes, ages out suspicions
    /// and tombstones, and emits probe / anti-entropy rounds when due.
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
        effects.extend(self.reap_entries(now));

        // 4. Send the next liveness probe.
        if now >= self.next_probe {
            self.next_probe = now.saturating_add(self.config.probe_interval_ms);
            effects.extend(self.probe(now));
        }

        // 5. Run the anti-entropy digest round.
        if now >= self.next_anti_entropy {
            self.next_anti_entropy = now.saturating_add(self.anti_entropy_interval());
            effects.extend(self.disseminate_digest(now));
        }

        effects.push(self.arm_timer());
        effects
    }

    fn anti_entropy_interval(&self) -> u64 {
        self.config.anti_entropy_interval_ms.max(1)
    }

    /// Brings the next anti-entropy round forward to now, so a fresh membership
    /// change or local write disseminates on the next tick rather than waiting a
    /// whole interval — restoring the "state change rides the next frame"
    /// promptness the full-view piggyback used to give for free.
    fn nudge_anti_entropy(&mut self) {
        self.next_anti_entropy = self.next_anti_entropy.min(self.now_hint);
    }

    /// Author one key of our own state: bump above whatever version our per-node
    /// clock has reached (covering every key, and any adopted echo) so the write
    /// supersedes every prior copy and advances our digest high-water mark.
    fn set_local_entry(&mut self, key: &str, value: Vec<u8>, ttl_ms: Option<u64>) -> Vec<Effect> {
        let now = self.now_hint;
        let ttl = ttl_ms.unwrap_or(0);
        let Some(m) = self.members.get_mut(&self.local) else {
            return Vec::new();
        };
        let version = m.max_state_version.saturating_add(1);
        m.max_state_version = version;
        m.entries.insert(
            key.to_owned(),
            StateEntry {
                version,
                value,
                ttl_ms: ttl,
                expires_at: if ttl == 0 {
                    Time::MAX
                } else {
                    now.saturating_add(ttl)
                },
                tombstone: false,
                tombstone_since: Time::ZERO,
            },
        );
        self.authored.insert(key.to_owned());
        self.stamp_self();
        self.nudge_anti_entropy();
        let mut effects = vec![Effect::NodeStateChanged {
            node: self.local.clone(),
            key: key.to_owned(),
        }];
        effects.extend(self.eager_push());
        effects
    }

    /// Pushes the just-authored entry straight to the current fanout targets
    /// as an unsolicited `Delta` frame — one hop, no digest round-trip — so a
    /// local write reaches live peers at network latency rather than tick
    /// cadence. Receivers adopt it through the ordinary versioned merge, so
    /// duplication with the following anti-entropy round is harmless; that
    /// round remains the repair path for peers outside this fanout and for
    /// any frame the transport drops.
    fn eager_push(&mut self) -> Vec<Effect> {
        if !self.config.eager_push {
            return Vec::new();
        }
        let have = self
            .members
            .get(&self.local)
            .map_or(0, |m| m.max_state_version.saturating_sub(1));
        let now = self.now_hint;
        let Some(delta) = self.build_delta_frame(&[(self.local.clone(), have)], now) else {
            return Vec::new();
        };
        let targets = self.select_fanout_targets();
        self.stats.delta_frames_sent += targets.len() as u64;
        self.stats.anti_entropy_bytes_sent += (delta.len() * targets.len()) as u64;
        targets
            .into_iter()
            .map(|to| Effect::Send {
                to,
                wire: delta.clone(),
            })
            .collect()
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
        vec![self.send_probe(target, wire::Kind::Ping, None)]
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
            .map(|p| self.send_probe(p, wire::Kind::PingReq, Some(target.clone())))
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
        self.stamp(target);
        let mut effects = vec![Effect::MembershipChanged];
        effects.extend(self.recompute_coordinator());
        self.nudge_anti_entropy();
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
            self.stamp(node);
        }
        let mut effects = vec![Effect::MembershipChanged];
        effects.extend(self.recompute_coordinator());
        self.nudge_anti_entropy();
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
            self.digest_cursors.remove(&node);
            self.digest_visits.remove(&node);
        }
    }

    // ---- anti-entropy: digest generation -------------------------------------

    /// Runs one anti-entropy round: send a digest (chunked to the frame budget)
    /// to a rotating fanout of peers.
    fn disseminate_digest(&mut self, now: Time) -> Vec<Effect> {
        let targets = self.select_fanout_targets();
        if targets.is_empty() {
            return Vec::new();
        }
        let mut effects = Vec::new();
        for to in targets {
            // A peer's first digest — and every Nth after — is full; the rest
            // are per-peer delta digests listing only members whose summary
            // changed since the last digest built for this peer. The cursor
            // advances on build, not delivery: anything a dropped frame loses
            // stays divergent only until this peer's next full digest.
            let every = self.config.full_digest_every.max(1);
            let visit = self.digest_visits.entry(to.clone()).or_insert(0);
            let full = *visit % every == 0;
            *visit += 1;
            let since = if full {
                None
            } else {
                Some(self.digest_cursors.get(&to).copied().unwrap_or(0))
            };
            let (chunks, listed) = self.build_digest_chunks(now, since);
            self.digest_cursors.insert(to.clone(), self.change_clock);
            self.stats.digests_built += 1;
            if full {
                self.stats.full_digests_built += 1;
            }
            self.stats.digest_summaries_listed += listed as u64;
            for chunk in chunks {
                self.stats.digest_frames_sent += 1;
                self.stats.anti_entropy_bytes_sent += chunk.len() as u64;
                effects.push(Effect::Send {
                    to: to.clone(),
                    wire: chunk,
                });
            }
        }
        effects
    }

    /// Picks `anti_entropy_fanout` distinct peers, rotating a cursor so every
    /// peer is covered over successive rounds.
    fn select_fanout_targets(&mut self) -> Vec<NodeId> {
        let candidates = self.dissemination_targets();
        let n = candidates.len();
        if n == 0 {
            return Vec::new();
        }
        let k = self.config.anti_entropy_fanout.max(1).min(n);
        let mut out = Vec::with_capacity(k);
        for i in 0..k {
            out.push(candidates[(self.gossip_cursor + i) % n].clone());
        }
        self.gossip_cursor = self.gossip_cursor.wrapping_add(k);
        out
    }

    /// Builds a digest for one peer as encoded [`wire::Kind::Digest`] frames
    /// within the frame budget, returning the frames and how many member
    /// summaries they list. `since` of `None` builds a full digest; `Some(c)`
    /// lists only members stamped after change-clock `c` — a per-peer delta
    /// digest, safe because a digest only ever triggers per-listed-member
    /// reconciliation (absence is never interpreted). The metadata register
    /// set rides the first chunk either way.
    fn build_digest_chunks(&self, now: Time, since: Option<u64>) -> (Vec<Vec<u8>>, usize) {
        let budget = self.config.max_delta_frame_bytes;
        let summaries: Vec<wire::NodeDigest> = self
            .members
            .iter()
            .filter(|(_, m)| self.should_gossip(m, now))
            .filter(|(_, m)| since.is_none_or(|cursor| m.changed_at > cursor))
            .map(|(node, m)| wire::NodeDigest {
                node: node.clone(),
                incarnation: m.incarnation,
                status: m.status.to_wire(),
                max_version: m.max_state_version,
                content_hash: self.content_hash(m, now),
            })
            .collect();
        let metadata: Vec<wire::MetaDelta> = self
            .metadata
            .iter()
            .map(|(key, v)| wire::MetaDelta {
                key: key.clone(),
                version: v.version,
                writer: v.writer.clone(),
                value: v.value.clone(),
            })
            .collect();

        let base = 1 + 1 + (4 + self.group.as_str().len()) + 1 + 4; // ..+ digest count
        let mut chunks: Vec<Vec<u8>> = Vec::new();
        let mut i = 0usize;
        loop {
            let first = chunks.is_empty();
            let meta = if first { metadata.clone() } else { Vec::new() };
            let meta_len = 4 + meta
                .iter()
                .map(|d| {
                    (4 + d.key.len()) + 8 + (4 + d.writer.as_str().len()) + (4 + d.value.len())
                })
                .sum::<usize>();
            let mut size = base + meta_len;
            let mut slice: Vec<wire::NodeDigest> = Vec::new();
            while i < summaries.len() {
                let d = &summaries[i];
                let dlen = (4 + d.node.as_str().len()) + 8 + 1 + 8 + 8;
                if size + dlen > budget && !slice.is_empty() {
                    break;
                }
                size += dlen;
                slice.push(d.clone());
                i += 1;
            }
            if slice.is_empty() && meta.is_empty() {
                break; // nothing left to emit
            }
            chunks.push(wire::encode(&wire::Frame {
                kind: wire::Kind::Digest,
                group: self.group.clone(),
                target: None,
                digest: slice,
                wants: Vec::new(),
                members: Vec::new(),
                metadata: meta,
            }));
            if i >= summaries.len() {
                break;
            }
        }
        let listed = summaries.len();
        (chunks, listed)
    }

    // ---- anti-entropy: reconciliation ----------------------------------------

    /// Merges a peer's digest: reconcile liveness and metadata directly, then
    /// request whatever entries we're behind on and offer whatever we're ahead
    /// on.
    fn on_digest(&mut self, from: &NodeId, frame: &wire::Frame, now: Time) -> Vec<Effect> {
        let mut effects = self.merge_digest_liveness(&frame.digest, now);
        effects.extend(self.merge_metadata(frame.metadata.clone()));

        let mut wants: Vec<wire::NodeWant> = Vec::new();
        let mut offers: Vec<(NodeId, u64)> = Vec::new();
        for d in &frame.digest {
            let (ours_max, ours_hash) = match self.members.get(&d.node) {
                Some(m) => (m.max_state_version, self.content_hash(m, now)),
                None => (0, 0),
            };
            if d.max_version > ours_max {
                wants.push(wire::NodeWant {
                    node: d.node.clone(),
                    have_version: ours_max,
                });
            } else if d.max_version < ours_max {
                offers.push((d.node.clone(), d.max_version));
            } else if d.content_hash != ours_hash {
                // Equal high-water but divergent holdings — a restart reused a
                // version clock, so the same number now names different entries on
                // each side. Fall back to a full per-key exchange (request and
                // offer everything), which last-writer-wins reconciles where the
                // scalar comparison alone was blind.
                wants.push(wire::NodeWant {
                    node: d.node.clone(),
                    have_version: 0,
                });
                offers.push((d.node.clone(), 0));
            }
        }
        if !wants.is_empty() {
            effects.push(self.send_delta_request(from.clone(), wants));
        }
        if let Some(delta) = self.build_delta_frame(&offers, now) {
            self.stats.delta_frames_sent += 1;
            self.stats.anti_entropy_bytes_sent += delta.len() as u64;
            effects.push(Effect::Send {
                to: from.clone(),
                wire: delta,
            });
        }
        effects
    }

    /// Answers a peer's `DeltaRequest` with the entries it asked for, bounded to
    /// the frame budget.
    fn on_delta_request(&mut self, from: &NodeId, frame: &wire::Frame, now: Time) -> Vec<Effect> {
        let offers: Vec<(NodeId, u64)> = frame
            .wants
            .iter()
            .map(|w| (w.node.clone(), w.have_version))
            .collect();
        match self.build_delta_frame(&offers, now) {
            Some(delta) => {
                self.stats.delta_frames_sent += 1;
                self.stats.anti_entropy_bytes_sent += delta.len() as u64;
                vec![Effect::Send {
                    to: from.clone(),
                    wire: delta,
                }]
            }
            None => Vec::new(),
        }
    }

    fn send_delta_request(&mut self, to: NodeId, wants: Vec<wire::NodeWant>) -> Effect {
        let budget = self.config.max_delta_frame_bytes;
        let base = 1 + 1 + (4 + self.group.as_str().len()) + 1 + 4;
        let mut size = base;
        let mut kept: Vec<wire::NodeWant> = Vec::new();
        for w in wants {
            let wlen = (4 + w.node.as_str().len()) + 8;
            if size + wlen > budget && !kept.is_empty() {
                break; // remainder re-requested next round
            }
            size += wlen;
            kept.push(w);
        }
        Effect::Send {
            to,
            wire: wire::encode(&wire::Frame {
                kind: wire::Kind::DeltaRequest,
                group: self.group.clone(),
                target: None,
                digest: Vec::new(),
                wants: kept,
                members: Vec::new(),
                metadata: Vec::new(),
            }),
        }
    }

    /// Assembles the entries newer than each `(node, have_version)` into a single
    /// bounded `Delta` frame. Entries go out in ascending version order and are
    /// truncated at the budget (the recipient re-requests the tail next round);
    /// a member with no qualifying entries but a higher-water mark than the
    /// requester is still included so the recipient can advance past a reaped
    /// tail. Returns `None` when there is nothing to send.
    fn build_delta_frame(&self, wants: &[(NodeId, u64)], now: Time) -> Option<Vec<u8>> {
        let budget = self.config.max_delta_frame_bytes;
        let mut size = wire::delta_frame_overhead(&self.group);
        let mut members: Vec<wire::MemberDelta> = Vec::new();

        'wants: for (node, have) in wants {
            let Some(m) = self.members.get(node) else {
                continue;
            };
            let mut qualifying: Vec<(&String, &StateEntry)> = m
                .entries
                .iter()
                .filter(|(_, e)| e.version > *have && self.should_gossip_entry(e, now))
                .collect();
            qualifying.sort_by_key(|(_, e)| e.version);

            let header = wire::member_header_len(node);
            if size + header > budget && !members.is_empty() {
                break 'wants; // no room even for the header
            }

            let mut md = wire::MemberDelta {
                node: node.clone(),
                incarnation: m.incarnation,
                status: m.status.to_wire(),
                max_version: 0,
                entries: Vec::new(),
            };
            let mut member_size = header;
            let mut top = *have;
            let mut truncated = false;

            for (k, e) in qualifying {
                let ed = wire::EntryDelta {
                    key: k.clone(),
                    version: e.version,
                    ttl_ms: e.ttl_ms,
                    tombstone: e.tombstone,
                    value: e.value.clone(),
                };
                let elen = wire::entry_len(&ed);
                // The one exception to the budget: never starve the very first
                // entry, even if its value alone exceeds the cap.
                let first_ever = members.is_empty() && md.entries.is_empty();
                if size + member_size + elen > budget && !first_ever {
                    truncated = true;
                    break;
                }
                top = ed.version;
                member_size += elen;
                md.entries.push(ed);
            }

            // If we sent everything qualifying, the recipient can jump straight
            // to our true high-water (which may sit above the last entry when
            // the top was reaped); if we truncated, only to the last we included.
            md.max_version = if truncated {
                top
            } else {
                m.max_state_version.max(top)
            };

            if !md.entries.is_empty() || md.max_version > *have {
                size += member_size;
                members.push(md);
            }
            if truncated {
                break 'wants;
            }
        }

        if members.is_empty() {
            return None;
        }
        Some(wire::encode(&wire::Frame {
            kind: wire::Kind::Delta,
            group: self.group.clone(),
            target: None,
            digest: Vec::new(),
            wants: Vec::new(),
            members,
            metadata: Vec::new(),
        }))
    }

    // ---- liveness merges -----------------------------------------------------

    /// Would an incoming `(status, incarnation)` about *ourselves* require us to
    /// refute (bump our incarnation and reassert Alive)? Returns the incarnation
    /// to jump to, if so. A voluntary leave is never refuted.
    fn self_refute_target(&self, status: Status, incarnation: u64) -> Option<u64> {
        if self.leaving {
            return None;
        }
        let false_suspicion = status != Status::Alive && incarnation >= self.incarnation;
        let peer_ahead = incarnation > self.incarnation;
        (false_suspicion || peer_ahead).then_some(incarnation + 1)
    }

    fn apply_refutation(&mut self, refute_to: Option<u64>) -> bool {
        let Some(ni) = refute_to else {
            return false;
        };
        self.incarnation = ni;
        self.stamp_self();
        if let Some(m) = self.members.get_mut(&self.local) {
            m.incarnation = ni;
            m.status = Status::Alive;
        }
        true
    }

    /// Applies a peer's liveness claim about a *remote* node by SWIM precedence.
    /// Adopts an unknown node (liveness only — no entries) or updates a known
    /// one. Returns whether membership changed.
    fn merge_remote_liveness(
        &mut self,
        node: &NodeId,
        incarnation: u64,
        status: Status,
        now: Time,
    ) -> bool {
        match self.members.get(node) {
            None => {
                let mut member = Member::new(incarnation, status);
                match status {
                    Status::Suspect => member.suspect_since = now,
                    Status::Dead => member.dead_since = now,
                    Status::Alive => {}
                }
                self.members.insert(node.clone(), member);
                self.stamp(node);
                true
            }
            Some(cur) => {
                if !cur.superseded_by(incarnation, status) {
                    return false;
                }
                let member = self.members.get_mut(node).expect("present");
                member.incarnation = incarnation;
                member.status = status;
                match status {
                    Status::Suspect => member.suspect_since = now,
                    Status::Dead => member.dead_since = now,
                    Status::Alive => {}
                }
                self.stamp(node);
                true
            }
        }
    }

    /// Merges the liveness half of a digest (incarnation/status per node) and
    /// refutes any suspicion of ourselves it carries. State reconciliation is a
    /// separate delta round-trip.
    fn merge_digest_liveness(&mut self, digest: &[wire::NodeDigest], now: Time) -> Vec<Effect> {
        let mut membership_changed = false;
        let mut refute_to: Option<u64> = None;
        for d in digest {
            let Some(status) = Status::from_wire(d.status) else {
                continue;
            };
            if d.node == self.local {
                if let Some(t) = self.self_refute_target(status, d.incarnation) {
                    refute_to = Some(refute_to.map_or(t, |x| x.max(t)));
                }
                continue;
            }
            membership_changed |= self.merge_remote_liveness(&d.node, d.incarnation, status, now);
        }
        membership_changed |= self.apply_refutation(refute_to);

        let mut effects = Vec::new();
        if membership_changed {
            effects.push(Effect::MembershipChanged);
            effects.extend(self.recompute_coordinator());
            self.nudge_anti_entropy();
        }
        effects
    }

    /// Merges the member deltas of a `Delta` frame. Liveness (`incarnation` /
    /// `status`, by SWIM precedence) and app state (per-key last-writer-wins) are
    /// merged *independently*, high-water marks advance, and our own echoed
    /// entries are adopted for restart recovery.
    fn merge_members(&mut self, deltas: Vec<wire::MemberDelta>, now: Time) -> Vec<Effect> {
        let mut membership_changed = false;
        let mut state_changed: Vec<(NodeId, String)> = Vec::new();
        let mut refute_to: Option<u64> = None;

        for delta in deltas {
            let Some(status) = Status::from_wire(delta.status) else {
                continue; // unknown status code — ignore
            };

            if delta.node == self.local {
                // Refute a false suspicion / out-incarnate a peer ahead of us.
                if let Some(t) = self.self_refute_target(status, delta.incarnation) {
                    refute_to = Some(refute_to.map_or(t, |x| x.max(t)));
                }
                // Restart recovery (the wipe fix): our own entries echoed back at
                // versions above what we hold are OUR data from before a restart.
                // ADOPT them verbatim for keys we have NOT authored this boot; for
                // authored keys keep our value and out-version the echo.
                for entry in delta.entries {
                    let ours = self.members[&self.local].entries.get(&entry.key);
                    if ours.is_some_and(|e| entry.version <= e.version) {
                        continue; // echo of something we already hold — ignore
                    }
                    let m = self.members.get_mut(&self.local).expect("self present");
                    if self.authored.contains(&entry.key) {
                        // Sole-author rule: never let an echo (or forgery) replace
                        // a value we wrote this boot. Jump our version above it and
                        // keep re-advertising OUR value, which supersedes everywhere.
                        let bumped = m.entries.get_mut(&entry.key).map(|e| {
                            e.version = entry.version.saturating_add(1);
                            e.version
                        });
                        if let Some(v) = bumped {
                            m.observe_version(v);
                            state_changed.push((self.local.clone(), entry.key));
                            self.stamp_self();
                        }
                    } else {
                        // A key we have NOT authored this boot, echoed at a higher
                        // version, is our own pre-restart data — adopt it verbatim.
                        m.observe_version(entry.version);
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
                        self.stamp_self();
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
                        member.observe_version(entry.version);
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
                    member.observe_version(delta.max_version);
                    self.members.insert(delta.node.clone(), member);
                    self.stamp(&delta.node);
                    membership_changed = true;
                }
                Some(cur) => {
                    let status_wins = cur.superseded_by(delta.incarnation, status);
                    let member = self.members.get_mut(&delta.node).expect("present");
                    let high_water_before = member.max_state_version;
                    let mut adopted = false;
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
                    // single-writer, so version order alone decides; a fresher
                    // version also re-arms the local TTL. Every seen version
                    // advances the high-water mark.
                    for entry in delta.entries {
                        member.observe_version(entry.version);
                        // Per-key LWW by version, with a deterministic tiebreak
                        // (tombstone, then value) so a version reused across a
                        // restart can never deadlock two divergent values at the
                        // same number — one side always wins and both converge.
                        let wins = member.entries.get(&entry.key).is_none_or(|e| {
                            (entry.version, entry.tombstone, &entry.value)
                                > (e.version, e.tombstone, &e.value)
                        });
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
                        adopted = true;
                        state_changed.push((delta.node.clone(), entry.key));
                    }
                    // The sender's high-water (>= every version it holds) lets us
                    // advance our summary past a reaped tail without re-requesting.
                    member.observe_version(delta.max_version);
                    // Anything digest-visible moved (liveness, content, or
                    // high-water): re-advertise via future delta digests.
                    if status_wins || adopted || member.max_state_version > high_water_before {
                        self.stamp(&delta.node);
                    }
                }
            }
        }

        membership_changed |= self.apply_refutation(refute_to);

        let mut effects = Vec::new();
        if membership_changed {
            effects.push(Effect::MembershipChanged);
            effects.extend(self.recompute_coordinator());
            self.nudge_anti_entropy();
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

    /// Drop expired TTL entries (they converge to absent everywhere once the
    /// author stops refreshing — no tombstone needed) and reap entry tombstones
    /// past `2×dead_timeout` (no longer gossiped after 1×, so no peer re-teaches
    /// them). The member's high-water mark is left untouched by reaping, so a
    /// digest can never claim to be behind on — and so resurrect — a reaped
    /// version. A TTL expiry is an observable state change and emits
    /// [`Effect::NodeStateChanged`]; a tombstone reap is not (the key already
    /// read as absent).
    fn reap_entries(&mut self, now: Time) -> Vec<Effect> {
        let reap_after = self.config.dead_timeout_ms.saturating_mul(2);
        let mut expired: Vec<(NodeId, String)> = Vec::new();
        for (node, member) in &mut self.members {
            member.entries.retain(|key, e| {
                if e.tombstone {
                    now < e.tombstone_since.saturating_add(reap_after)
                } else if e.expired(now) {
                    expired.push((node.clone(), key.clone()));
                    false
                } else {
                    true
                }
            });
        }
        expired
            .into_iter()
            .map(|(node, key)| Effect::NodeStateChanged { node, key })
            .collect()
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

    /// Builds a `Send` effect carrying a bare probe frame (no piggybacked view).
    fn send_probe(&self, to: NodeId, kind: wire::Kind, target: Option<NodeId>) -> Effect {
        Effect::Send {
            to,
            wire: wire::encode(&wire::Frame {
                kind,
                group: self.group.clone(),
                target,
                digest: Vec::new(),
                wants: Vec::new(),
                members: Vec::new(),
                metadata: Vec::new(),
            }),
        }
    }

    /// A `Dead` member is summarized in digests only until `dead_timeout`
    /// elapses; after that peers are assumed to know, and dropping it lets
    /// everyone reap the tombstone without re-teaching each other.
    fn should_gossip(&self, m: &Member, now: Time) -> bool {
        m.status != Status::Dead || now < m.dead_since.saturating_add(self.config.dead_timeout_ms)
    }

    /// An entry is offered in a delta while live and unexpired; a tombstone only
    /// until `dead_timeout` (after that peers are assumed to know — the same
    /// shape as a Dead member tombstone, and what upholds the reap horizon).
    fn should_gossip_entry(&self, e: &StateEntry, now: Time) -> bool {
        if e.tombstone {
            return now
                < e.tombstone_since
                    .saturating_add(self.config.dead_timeout_ms);
        }
        !e.expired(now)
    }

    /// A hash of a member's currently-advertised entries (keys, versions,
    /// tombstones, values, in key order), carried in the digest so a receiver can
    /// tell two summaries apart when their high-water marks coincide but their
    /// holdings do not (a version reused across a restart). Empty holdings hash to
    /// zero. A tiny dependency-free FNV-1a fold — the core stays dep-free.
    fn content_hash(&self, m: &Member, now: Time) -> u64 {
        let mut h: u64 = 0;
        for (key, e) in &m.entries {
            if !self.should_gossip_entry(e, now) {
                continue;
            }
            h = fnv1a(h, key.as_bytes());
            h = fnv1a(h, &e.version.to_le_bytes());
            h = fnv1a(h, &[u8::from(e.tombstone)]);
            h = fnv1a(h, &e.value);
        }
        h
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
        let mut at = self.next_anti_entropy.min(self.next_probe);
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

    /// The member summaries listed across all digest frames in `effects`.
    fn digest_summaries(effects: &[Effect]) -> Vec<NodeId> {
        effects
            .iter()
            .filter_map(|e| match e {
                Effect::Send { wire, .. } => wire::decode(wire),
                _ => None,
            })
            .filter(|f| f.kind == wire::Kind::Digest)
            .flat_map(|f| f.digest.into_iter().map(|d| d.node))
            .collect()
    }

    /// Delta digests list only members changed since the last digest built
    /// for the peer; a quiet round emits nothing at all; every Nth digest is
    /// full again. The counters expose exactly that shape.
    #[test]
    fn delta_digests_list_only_changed_members_with_periodic_full() {
        let config = Config {
            anti_entropy_fanout: 1,
            full_digest_every: 3,
            // Keep probes out of the timeline: no suspicion stamps.
            probe_interval_ms: 1_000_000,
            ..Config::default()
        };
        let mut a = GroupEngine::new(
            GroupId::new("g"),
            NodeId::new("a"),
            [NodeId::new("b")],
            config,
        );

        // Visit 1 (full): only ourselves exist — one summary.
        let effects = a.start(Time(0));
        assert_eq!(digest_summaries(&effects), vec![NodeId::new("a")]);

        // b joins (stamped): the next digest is a delta listing exactly b.
        let _ = a.apply(Command::AddPeer(NodeId::new("b")));
        let effects = a.on_tick(Time(200));
        assert_eq!(digest_summaries(&effects), vec![NodeId::new("b")]);

        // Nothing changed since: a quiet delta round sends no digest at all.
        let effects = a.on_tick(Time(400));
        assert_eq!(digest_summaries(&effects), Vec::<NodeId>::new());

        // A local write stamps us; visit 4 is the periodic FULL digest, so it
        // lists everyone — the repair bound for anything a dropped frame lost.
        let _ = a.apply(Command::SetLocalEntry {
            key: "k".into(),
            value: b"v".to_vec(),
            ttl_ms: None,
        });
        let effects = a.on_tick(Time(600));
        assert_eq!(
            digest_summaries(&effects),
            vec![NodeId::new("a"), NodeId::new("b")],
            "every full_digest_every-th digest lists all members"
        );

        // Quiet again: back to zero-cost rounds.
        let effects = a.on_tick(Time(800));
        assert_eq!(digest_summaries(&effects), Vec::<NodeId>::new());

        let stats = a.net_stats();
        assert_eq!(stats.digests_built, 5);
        assert_eq!(stats.full_digests_built, 2, "visit 1 and visit 4");
        assert_eq!(
            stats.digest_summaries_listed, 4,
            "1 (boot) + 1 (b joined) + 0 + 2 (full) + 0"
        );
        assert!(stats.anti_entropy_bytes_sent > 0);
    }

    /// A digest frame (liveness summaries + metadata) — how liveness and
    /// metadata now disseminate.
    fn digest_frame(digest: Vec<wire::NodeDigest>, metadata: Vec<wire::MetaDelta>) -> Vec<u8> {
        wire::encode(&wire::Frame {
            kind: wire::Kind::Digest,
            group: GroupId::new("g"),
            target: None,
            digest,
            wants: Vec::new(),
            members: Vec::new(),
            metadata,
        })
    }

    /// A delta frame (member entries) — how per-node state now disseminates.
    fn delta_frame(members: Vec<wire::MemberDelta>) -> Vec<u8> {
        wire::encode(&wire::Frame {
            kind: wire::Kind::Delta,
            group: GroupId::new("g"),
            target: None,
            digest: Vec::new(),
            wants: Vec::new(),
            members,
            metadata: Vec::new(),
        })
    }

    fn probe_frame(kind: wire::Kind, target: Option<NodeId>) -> Vec<u8> {
        wire::encode(&wire::Frame {
            kind,
            group: GroupId::new("g"),
            target,
            digest: Vec::new(),
            wants: Vec::new(),
            members: Vec::new(),
            metadata: Vec::new(),
        })
    }

    fn ndigest(node: &str, inc: u64, status: Status, max_version: u64) -> wire::NodeDigest {
        wire::NodeDigest {
            node: NodeId::new(node),
            incarnation: inc,
            status: status.to_wire(),
            max_version,
            // Empty holdings hash to zero; these liveness-only digests advertise
            // no entries, so a zero here matches an empty receiver.
            content_hash: 0,
        }
    }

    fn entry(
        key: &str,
        version: u64,
        ttl_ms: u64,
        tombstone: bool,
        value: &[u8],
    ) -> wire::EntryDelta {
        wire::EntryDelta {
            key: key.to_owned(),
            version,
            ttl_ms,
            tombstone,
            value: value.to_vec(),
        }
    }

    /// A member delta carrying `entries` (a well-formed delta sets its
    /// high-water to the max entry version).
    fn member_delta(node: &str, entries: Vec<wire::EntryDelta>) -> wire::MemberDelta {
        let max_version = entries.iter().map(|e| e.version).max().unwrap_or(0);
        wire::MemberDelta {
            node: NodeId::new(node),
            incarnation: 0,
            status: Status::Alive.to_wire(),
            max_version,
            entries,
        }
    }

    /// Decodes the single digest frame a round emits (all chunks in one, at
    /// these small sizes), returning the sender's own summaries and metadata.
    fn decode_one_digest(effects: &[Effect]) -> wire::Frame {
        let bytes = effects
            .iter()
            .find_map(|e| match e {
                Effect::Send { wire, .. } => {
                    let f = wire::decode(wire)?;
                    (f.kind == wire::Kind::Digest).then_some(wire.clone())
                }
                _ => None,
            })
            .expect("a digest send");
        wire::decode(&bytes).expect("decodes")
    }

    #[test]
    fn start_announces_to_seeds() {
        // Fanout defaults to 2, so a round reaches up to two seeds.
        let mut e = engine("a", &["b", "c"]);
        let sends = e
            .start(Time::ZERO)
            .iter()
            .filter(|e| matches!(e, Effect::Send { .. }))
            .count();
        assert_eq!(sends, 2, "should announce to two seeds via digest");
    }

    #[test]
    fn learns_members_and_recomputes_coordinator() {
        let mut a = engine("a", &["b"]);
        a.on_message(
            NodeId::new("b"),
            &digest_frame(vec![ndigest("b", 0, Status::Alive, 0)], vec![]),
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
            anti_entropy_interval_ms: 100,
            ..Config::default()
        };
        let mut a = GroupEngine::new(GroupId::new("g"), NodeId::new("a"), [NodeId::new("b")], cfg);
        a.on_message(
            NodeId::new("b"),
            &digest_frame(vec![ndigest("b", 0, Status::Alive, 0)], vec![]),
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
            &digest_frame(
                vec![
                    ndigest("b", 0, Status::Alive, 0),
                    ndigest("c", 0, Status::Alive, 0),
                ],
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
            &probe_frame(wire::Kind::IndirectAck, Some(NodeId::new("b"))),
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
            &probe_frame(wire::Kind::PingReq, Some(NodeId::new("t"))),
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
            &probe_frame(wire::Kind::Ack, None),
            Time(3),
        );
        assert!(
            ack.iter()
                .any(|e| matches!(e, Effect::Send { to, .. } if *to == NodeId::new("o"))),
            "should relay an indirect ack to the origin"
        );
    }

    #[test]
    fn ping_is_answered_with_a_bare_ack() {
        let mut a = engine("a", &[]);
        let effects = a.on_message(
            NodeId::new("b"),
            &probe_frame(wire::Kind::Ping, None),
            Time(1),
        );
        let ack = effects
            .iter()
            .find_map(|e| match e {
                Effect::Send { to, wire } if *to == NodeId::new("b") => wire::decode(wire),
                _ => None,
            })
            .expect("an ack send");
        assert_eq!(ack.kind, wire::Kind::Ack);
        assert!(ack.digest.is_empty() && ack.members.is_empty() && ack.metadata.is_empty());
    }

    #[test]
    fn refutes_false_suspicion_about_self() {
        let mut a = engine("a", &["b"]);
        a.on_message(
            NodeId::new("b"),
            &digest_frame(vec![ndigest("a", 0, Status::Suspect, 0)], vec![]),
            Time(1),
        );
        assert_eq!(a.member_status(&NodeId::new("a")), Some(Status::Alive));
        // Our next digest advertises ourselves Alive at a bumped incarnation.
        let frame = decode_one_digest(&a.on_tick(Time(2)));
        let self_d = frame
            .digest
            .iter()
            .find(|d| d.node == NodeId::new("a"))
            .expect("self in digest");
        assert_eq!(self_d.status, Status::Alive.to_wire());
        assert!(
            self_d.incarnation >= 1,
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
            &digest_frame(vec![ndigest("a", 0, Status::Dead, 0)], vec![]),
            Time(1),
        );
        assert_eq!(a.member_status(&NodeId::new("a")), Some(Status::Dead));
    }

    #[test]
    fn metadata_merges_by_last_writer_wins() {
        let mut a = engine("a", &["b"]);
        let meta = |ver, writer: &str, val: &str| {
            digest_frame(
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

    #[test]
    fn per_node_state_merges_by_last_writer_wins() {
        let mut a = engine("a", &["b"]);

        // Learn b's blob state at version 2.
        a.on_message(
            NodeId::new("b"),
            &delta_frame(vec![member_delta(
                "b",
                vec![entry(GroupEngine::BLOB_KEY, 2, 0, false, b"v2")],
            )]),
            Time(1),
        );
        assert_eq!(a.node_state(&NodeId::new("b")), Some(&b"v2"[..]));

        // A newer version wins; a stale one is ignored.
        a.on_message(
            NodeId::new("b"),
            &delta_frame(vec![member_delta(
                "b",
                vec![entry(GroupEngine::BLOB_KEY, 3, 0, false, b"v3")],
            )]),
            Time(2),
        );
        assert_eq!(a.node_state(&NodeId::new("b")), Some(&b"v3"[..]));
        a.on_message(
            NodeId::new("b"),
            &delta_frame(vec![member_delta(
                "b",
                vec![entry(GroupEngine::BLOB_KEY, 1, 0, false, b"old")],
            )]),
            Time(3),
        );
        assert_eq!(a.node_state(&NodeId::new("b")), Some(&b"v3"[..]));
    }

    #[test]
    fn a_node_authors_only_its_own_state() {
        let mut a = engine("a", &["b"]);
        a.apply(Command::SetLocalState(b"mine".to_vec()));
        assert_eq!(a.local_state(), b"mine");

        // A peer's delta claiming *our* state is out-versioned — we're the sole
        // author.
        a.on_message(
            NodeId::new("b"),
            &delta_frame(vec![member_delta(
                "a",
                vec![entry(GroupEngine::BLOB_KEY, 999, 0, false, b"forged")],
            )]),
            Time(1),
        );
        assert_eq!(a.local_state(), b"mine");
    }

    #[test]
    fn state_and_liveness_merge_independently() {
        let mut a = engine("a", &["b"]);
        // Learn b alive with blob state at version 1.
        a.on_message(
            NodeId::new("b"),
            &delta_frame(vec![member_delta(
                "b",
                vec![entry(GroupEngine::BLOB_KEY, 1, 0, false, b"s1")],
            )]),
            Time(1),
        );
        // A pure liveness digest (suspect, same state version) must not wipe state.
        a.on_message(
            NodeId::new("c"),
            &digest_frame(vec![ndigest("b", 0, Status::Suspect, 1)], vec![]),
            Time(2),
        );
        assert_eq!(a.member_status(&NodeId::new("b")), Some(Status::Suspect));
        assert_eq!(
            a.node_state(&NodeId::new("b")),
            Some(&b"s1"[..]),
            "state survived a status change"
        );
    }

    #[test]
    fn keys_version_independently() {
        let mut a = engine("a", &["b"]);
        a.on_message(
            NodeId::new("b"),
            &delta_frame(vec![member_delta(
                "b",
                vec![
                    entry("x", 5, 0, false, b"x5"),
                    entry("y", 1, 0, false, b"y1"),
                ],
            )]),
            Time(1),
        );
        // A fresher y does not disturb x; a stale x is ignored.
        a.on_message(
            NodeId::new("b"),
            &delta_frame(vec![member_delta(
                "b",
                vec![
                    entry("x", 4, 0, false, b"stale"),
                    entry("y", 2, 0, false, b"y2"),
                ],
            )]),
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
            &delta_frame(vec![member_delta(
                "b",
                vec![entry("hot", 1, 100, false, b"v1")],
            )]),
            Time(0),
        );
        assert!(a.node_entry(&NodeId::new("b"), "hot").is_some());
        // A fresher version at t=60 re-arms the expiry to t=160.
        a.on_message(
            NodeId::new("b"),
            &delta_frame(vec![member_delta(
                "b",
                vec![entry("hot", 2, 100, false, b"v2")],
            )]),
            Time(60),
        );
        a.on_tick(Time(120)); // old deadline passed, refreshed one hasn't
        assert_eq!(a.node_entry(&NodeId::new("b"), "hot"), Some(&b"v2"[..]));
        a.on_tick(Time(161));
        assert_eq!(
            a.node_entry(&NodeId::new("b"), "hot"),
            None,
            "expired after ttl"
        );
    }

    #[test]
    fn a_truncated_delta_triggers_a_continuation_request() {
        let mut a = engine("a", &["b"]);
        // An eager frame teaches `a` that b's high-water is 3 while carrying
        // only the newest entry — holes below it.
        a.on_message(
            NodeId::new("b"),
            &delta_frame(vec![member_delta(
                "b",
                vec![entry("k3", 3, 0, false, b"v3")],
            )]),
            Time(0),
        );
        // A backfill arrives truncated: entries through v1, advertised max 1,
        // below our stored high-water of 3 — the merge must ask for the rest.
        let effects = a.on_message(
            NodeId::new("b"),
            &delta_frame(vec![member_delta(
                "b",
                vec![entry("k1", 1, 0, false, b"v1")],
            )]),
            Time(1),
        );
        let request = effects
            .iter()
            .find_map(|e| match e {
                Effect::Send { wire, .. } => wire::decode(wire),
                _ => None,
            })
            .expect("a continuation frame");
        assert!(matches!(request.kind, wire::Kind::DeltaRequest));
        assert_eq!(request.wants.len(), 1);
        assert_eq!(request.wants[0].node, NodeId::new("b"));
        assert_eq!(request.wants[0].have_version, 1);

        // A frame that matches our stored high-water requests nothing.
        let effects = a.on_message(
            NodeId::new("b"),
            &delta_frame(vec![member_delta(
                "b",
                vec![
                    entry("k2", 2, 0, false, b"v2"),
                    entry("k3", 3, 0, false, b"v3"),
                ],
            )]),
            Time(2),
        );
        assert!(
            !effects.iter().any(|e| matches!(e, Effect::Send { .. })),
            "no continuation once holdings match the advertised high-water"
        );
    }

    #[test]
    fn a_local_write_eagerly_pushes_a_delta_to_fanout_peers() {
        let mut a = engine("a", &["b"]);
        let effects = a.apply(Command::SetLocalEntry {
            key: "k".into(),
            value: b"v1".to_vec(),
            ttl_ms: None,
        });
        let wires: Vec<Vec<u8>> = effects
            .iter()
            .filter_map(|e| match e {
                Effect::Send { wire, .. } => Some(wire.clone()),
                _ => None,
            })
            .collect();
        assert!(!wires.is_empty(), "the write must emit eager delta frames");
        let frame = wire::decode(&wires[0]).expect("decodes");
        assert!(matches!(frame.kind, wire::Kind::Delta));
        let m = frame
            .members
            .iter()
            .find(|m| m.node.as_str() == "a")
            .expect("self delta");
        assert!(m.entries.iter().any(|e| e.key == "k" && e.value == b"v1"));

        // A peer adopts it with no tick and no digest exchange: the write
        // travels at network latency, not gossip cadence.
        let mut b = engine("b", &["a"]);
        b.on_message(NodeId::new("a"), &wires[0], Time(1));
        assert_eq!(b.node_entry(&NodeId::new("a"), "k"), Some(&b"v1"[..]));
    }

    #[test]
    fn eager_push_carries_only_the_newest_change_including_tombstones() {
        let mut a = engine("a", &["b"]);
        a.apply(Command::SetLocalEntry {
            key: "old".into(),
            value: b"x".to_vec(),
            ttl_ms: None,
        });
        let effects = a.apply(Command::DeleteLocalEntry { key: "old".into() });
        let bytes = effects
            .iter()
            .find_map(|e| match e {
                Effect::Send { wire, .. } => Some(wire.clone()),
                _ => None,
            })
            .expect("eager frame");
        let frame = wire::decode(&bytes).expect("decodes");
        let m = frame
            .members
            .iter()
            .find(|m| m.node.as_str() == "a")
            .expect("self delta");
        assert_eq!(
            m.entries.len(),
            1,
            "exactly the newest change rides the eager frame"
        );
        assert!(m.entries[0].tombstone && m.entries[0].key == "old");
    }

    #[test]
    fn eager_push_can_be_disabled() {
        let mut a = GroupEngine::new(
            GroupId::new("g"),
            NodeId::new("a"),
            [NodeId::new("b")],
            Config {
                eager_push: false,
                ..Config::default()
            },
        );
        let effects = a.apply(Command::SetLocalEntry {
            key: "k".into(),
            value: b"v".to_vec(),
            ttl_ms: None,
        });
        assert!(
            !effects.iter().any(|e| matches!(e, Effect::Send { .. })),
            "no unsolicited frames when disabled"
        );
    }

    #[test]
    fn delete_offers_a_tombstone_in_a_delta_then_reaps_it_without_resurrection() {
        let mut a = engine("a", &["b"]);
        a.apply(Command::SetLocalEntry {
            key: "k".into(),
            value: b"v".to_vec(),
            ttl_ms: None,
        });
        a.apply(Command::DeleteLocalEntry { key: "k".into() });
        assert_eq!(
            a.node_entry(&NodeId::new("a"), "k"),
            None,
            "deleted locally"
        );

        // A peer requesting our full state gets the tombstone in a delta (so it
        // drops the key too).
        let req = wire::encode(&wire::Frame {
            kind: wire::Kind::DeltaRequest,
            group: GroupId::new("g"),
            target: None,
            digest: vec![],
            wants: vec![wire::NodeWant {
                node: NodeId::new("a"),
                have_version: 0,
            }],
            members: vec![],
            metadata: vec![],
        });
        let delta = wire::decode(
            &a.on_message(NodeId::new("b"), &req, Time(1_000))
                .iter()
                .find_map(|e| match e {
                    Effect::Send { wire, .. } => Some(wire.clone()),
                    _ => None,
                })
                .expect("a delta response"),
        )
        .expect("decodes");
        let m = delta
            .members
            .iter()
            .find(|m| m.node.as_str() == "a")
            .expect("self member");
        assert!(
            m.entries.iter().any(|e| e.key == "k" && e.tombstone),
            "tombstone offered"
        );
        let hwm = m.max_version;

        // After 2× dead_timeout the tombstone is reaped and no longer offered,
        // but the high-water mark is preserved — so a request can never resurrect
        // it.
        let far = Time(1_000 + Config::default().dead_timeout_ms * 2 + 1);
        a.on_tick(far);
        let delta = wire::decode(
            &a.on_message(NodeId::new("b"), &req, far.saturating_add(1))
                .iter()
                .find_map(|e| match e {
                    Effect::Send { wire, .. } => Some(wire.clone()),
                    _ => None,
                })
                .expect("a delta response"),
        )
        .expect("decodes");
        let m = delta
            .members
            .iter()
            .find(|m| m.node.as_str() == "a")
            .expect("self member");
        assert!(!m.entries.iter().any(|e| e.key == "k"), "tombstone reaped");
        assert!(
            m.max_version >= hwm,
            "high-water preserved across reap (no resurrection)"
        );
    }

    #[test]
    fn restart_adopts_echoed_entries_for_unauthored_keys() {
        // Fresh engine (a restart): a peer echoes entries we authored last boot.
        let mut a = engine("a", &["b"]);
        let effects = a.on_message(
            NodeId::new("b"),
            &delta_frame(vec![member_delta(
                "a",
                vec![entry("addr", 7, 0, false, b"10.0.0.1")],
            )]),
            Time(1),
        );
        // Adopted verbatim (NOT wiped by out-versioning with emptiness)...
        assert_eq!(
            a.node_entry(&NodeId::new("a"), "addr"),
            Some(&b"10.0.0.1"[..])
        );
        // ...with a change event so the app can re-author.
        assert!(effects.iter().any(|e| matches!(
            e,
            Effect::NodeStateChanged { node, key } if node.as_str() == "a" && key == "addr"
        )));
        // A post-restart local write supersedes the adopted version everywhere.
        a.apply(Command::SetLocalEntry {
            key: "addr".into(),
            value: b"10.0.0.2".to_vec(),
            ttl_ms: None,
        });
        assert_eq!(
            a.node_entry(&NodeId::new("a"), "addr"),
            Some(&b"10.0.0.2"[..])
        );

        // And once authored this boot, echoes can never replace it (sole-author
        // rule): the forged/echoed 999 only bumps our version past it.
        a.on_message(
            NodeId::new("b"),
            &delta_frame(vec![member_delta(
                "a",
                vec![entry("addr", 999, 0, false, b"forged")],
            )]),
            Time(2),
        );
        assert_eq!(
            a.node_entry(&NodeId::new("a"), "addr"),
            Some(&b"10.0.0.2"[..])
        );
    }
}
