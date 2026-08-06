//! The engine's state, construction, read accessors, and event entrypoints
//! (`start` / `on_message` / `on_tick`).

use std::collections::{BTreeMap, BTreeSet};

use crate::config::{Config, GroupMode};
use crate::membership::{Member, Status};
use crate::{GroupId, NodeId, Time, placement, wire};

use super::effect::Effect;
use super::election::Election;
use super::stats::NetStats;

/// Which phase of failure detection an outstanding probe is in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ProbePhase {
    /// A direct `Ping` we sent ourselves.
    Direct,
    /// We asked indirect probers to reach the target after a direct miss.
    Indirect,
}

/// The probe currently awaiting a response.
#[derive(Clone, Debug)]
pub(super) struct Pending {
    pub(super) target: NodeId,
    pub(super) deadline: Time,
    pub(super) phase: ProbePhase,
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
    pub(super) group: GroupId,
    pub(super) local: NodeId,
    /// The local node's incarnation, bumped to refute suspicion about itself.
    pub(super) incarnation: u64,
    /// Set once the local node has voluntarily left (so it won't refute its own
    /// death).
    pub(super) leaving: bool,
    /// All known members, including self.
    pub(super) members: BTreeMap<NodeId, Member>,
    /// Bootstrap contacts to disseminate toward before membership is learned.
    pub(super) seeds: BTreeSet<NodeId>,
    pub(super) metadata: BTreeMap<String, VersionedValue>,
    pub(super) coordinator: Option<NodeId>,
    pub(super) config: Config,
    /// When the next periodic anti-entropy (digest) round is due.
    pub(super) next_anti_entropy: Time,
    pub(super) next_probe: Time,
    /// Round-robin cursor over probe candidates.
    pub(super) probe_cursor: usize,
    /// Round-robin cursor over dissemination targets, so digest fanout rotates
    /// across peers instead of forever favouring the lowest ids.
    pub(super) gossip_cursor: usize,
    /// The outstanding probe awaiting a response, if any.
    pub(super) pending: Option<Pending>,
    /// As an indirect prober: target -> the origins waiting for us to relay an
    /// ack about it.
    pub(super) relaying: BTreeMap<NodeId, BTreeSet<NodeId>>,
    /// The most recent logical time observed via `start`/`on_message`/`on_tick`.
    /// Used only where a `Command` (which carries no clock) needs a timestamp —
    /// entry TTLs and tombstone ages; command-path precision is one event-loop
    /// turn, which is far finer than any TTL.
    pub(super) now_hint: Time,
    /// State keys this process has authored since boot. Echoes of our own
    /// entries are ADOPTED only for keys not in this set (restart recovery);
    /// for authored keys we keep our value and out-version the echo (the
    /// sole-author rule — a peer can never replace what we wrote this boot).
    pub(super) authored: BTreeSet<String>,
    /// Monotonic change clock: bumped whenever any member's digest-visible
    /// summary changes; the member is stamped with it (`Member::changed_at`)
    /// so per-peer delta digests can list only "changed since I last
    /// digested to you".
    pub(super) change_clock: u64,
    /// Per peer: the change-clock value as of the last digest built for it.
    pub(super) digest_cursors: BTreeMap<NodeId, u64>,
    /// Per peer: digests built for it so far (drives the full-digest cadence).
    pub(super) digest_visits: BTreeMap<NodeId, u64>,
    /// The Hosted-mode election state — `Some` exactly when
    /// [`Config::mode`](crate::Config::mode) is
    /// [`Hosted`](crate::GroupMode::Hosted). An `Eventual` group does not
    /// allocate it, and every election path checks it first, so the mode's
    /// "runs no election" contract is structural rather than conventional.
    pub(super) election: Option<Election>,
    /// Cumulative anti-entropy traffic counters.
    pub(super) stats: NetStats,
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
        // Construction has no clock of its own; the local node has been Alive
        // since the origin of this engine's logical timeline.
        let mut own = Member::new(0, Status::Alive, Time::ZERO);
        own.changed_at = 1; // stamped at clock 1, so a first digest lists us
        members.insert(local.clone(), own);
        let seeds = seeds
            .into_iter()
            .filter(|p| *p != local)
            .collect::<BTreeSet<_>>();
        let election = match &config.mode {
            GroupMode::Hosted(hosted) => Some(Election::new(hosted.clone())),
            GroupMode::Eventual => None,
        };
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
            election,
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
    pub(super) fn stamp(&mut self, node: &NodeId) {
        self.change_clock += 1;
        let stamp = self.change_clock;
        if let Some(m) = self.members.get_mut(node) {
            m.changed_at = stamp;
        }
    }

    /// [`stamp`](Self::stamp) for the local member, avoiding a borrow clash.
    pub(super) fn stamp_self(&mut self) {
        let me = self.local.clone();
        self.stamp(&me);
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

    /// The status of a specific node **and the instant this observer has held
    /// it continuously since** — the fencing-verdict primitive: "gossip-dead
    /// for N ⇒ fence; gossip-healthy for N ⇒ unfence".
    ///
    /// The stamp moves only when the status *value* changes, so a member that
    /// keeps being re-gossiped as `Alive` keeps its original stamp and the
    /// duration `now - since` really is uninterrupted. Refutation back to
    /// `Alive` is a change, and resets it.
    ///
    /// **Observer-local.** This is what *this* node concluded, from *its*
    /// probes and the gossip it received; two observers legitimately hold
    /// different stamps for the same member.
    ///
    /// **Reap horizon.** `Dead` tombstones are removed `2×dead_timeout_ms`
    /// after death, and this returns `None` from then on — the duration is
    /// readable only inside that horizon. A consumer that needs a longer
    /// verdict either raises `dead_timeout_ms` past the longest window it
    /// cares about, or treats "known member, now absent" as *dead for at least
    /// the horizon*.
    #[must_use]
    pub fn member_status_since(&self, node: &NodeId) -> Option<(Status, Time)> {
        self.members.get(node).map(|m| (m.status, m.status_since))
    }

    /// Iterates every known member with its status and the instant this
    /// observer has held that status since, in id order — the whole
    /// fencing-verdict roster in one pass. Same semantics and same
    /// reap-horizon caveat as [`member_status_since`](Self::member_status_since).
    pub fn member_statuses_since(&self) -> impl Iterator<Item = (&NodeId, Status, Time)> {
        self.members
            .iter()
            .map(|(node, m)| (node, m.status, m.status_since))
    }

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

    /// The logical instant **this engine** will expire its adopted copy of one
    /// key of a node's state — the *lease-lapse instant* the coherence-lease
    /// tier is built on: the moment after which this observer provably stops
    /// serving the entry, whether or not anyone can reach it. A writer
    /// invalidating a key waits for either an ack from a reachable holder or
    /// this instant to pass on a silent one; the second branch is what turns a
    /// timeout-with-a-hope into a bound, because the exposure is ended by the
    /// *holder's own clock* (a bounded-clock-**rate** assumption) rather than
    /// by the holder learning anything.
    ///
    /// `None` when there is no such instant: the key is absent, it is held as
    /// a tombstone, or it was authored with no TTL (`ttl_ms == 0`, which arms
    /// [`Time::MAX`] — never expires). Also `None` once an expired entry has
    /// been reaped, since the copy it described is gone.
    ///
    /// **Observer-local, and armed at ADOPTION.** A TTL travels on the wire as
    /// a *duration*, never as an absolute stamp: each receiver arms
    /// `now + ttl_ms` against **its own** clock at the instant it adopts the
    /// entry, so two observers of the same write legitimately hold different
    /// expiries, and the author's own copy expires on the author's timeline.
    /// Re-adopting a strictly newer version re-arms it; re-gossiping a version
    /// already held does not, so a chatty peer can never extend a lapse
    /// instant it did not advance. A writer reasoning about a peer's lapse
    /// must therefore add the propagation delay and clock-rate skew it is
    /// willing to assume to what *it* reads here.
    #[must_use]
    pub fn node_entry_expires_at(&self, node: &NodeId, key: &str) -> Option<Time> {
        self.members
            .get(node)
            .and_then(|m| m.entries.get(key))
            .filter(|e| !e.tombstone)
            .map(|e| e.expires_at)
            .filter(|at| *at != Time::MAX)
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
        self.election_start(now);
        let mut effects = self.disseminate_digest(now);
        effects.push(self.arm_timer());
        effects
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
            // The Hosted-mode election (see `election.rs`). In an `Eventual`
            // group these decode and are dropped, exactly as they were before
            // the election existed.
            wire::Kind::LeadClaim | wire::Kind::LeadGrant | wire::Kind::LeadState => {
                self.on_lead(&from, &frame, now)
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
        effects.extend(self.reap_dead(now));

        // 3b. Expired state entries and stale entry tombstones.
        effects.extend(self.reap_entries(now));

        // 4. Send the next liveness probe.
        if now >= self.next_probe {
            self.next_probe = now.saturating_add(self.config.probe_interval_ms);
            effects.extend(self.probe(now));
        }

        // 5. Run the anti-entropy digest round.
        let anti_entropy_due = now >= self.next_anti_entropy;
        if anti_entropy_due {
            self.next_anti_entropy = now.saturating_add(self.anti_entropy_interval());
            effects.extend(self.disseminate_digest(now));
        }

        // 6. The Hosted-mode election: claim, settle, renew, or step down. A
        //    no-op in an `Eventual` group. Runs after the liveness steps above
        //    so it reads this tick's membership, and rides the same round
        //    cadence for its re-broadcasts.
        effects.extend(self.election_tick(now, anti_entropy_due));

        effects.push(self.arm_timer());
        effects
    }

    pub(super) fn arm_timer(&self) -> Effect {
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
        // A standing claim's settle window and a host's lease are deadlines
        // like any other: without them a driver whose timer is the gossip
        // cadence would activate — or step down — a whole round late.
        if let Some(deadline) = self.election.as_ref().and_then(Election::deadline) {
            at = at.min(deadline);
        }
        Effect::ArmTimer { at }
    }

    pub(super) fn compute_coordinator(&self) -> Option<NodeId> {
        // The coordinator is just the placement owner of the group id among live
        // members (Alive or Suspect); a Dead node is never a candidate. Same
        // HA-hash the public `placement` API exposes.
        let live: BTreeSet<NodeId> = self.members().cloned().collect();
        placement::owner(self.group.as_str(), &live)
    }

    pub(super) fn recompute_coordinator(&mut self) -> Vec<Effect> {
        let next = self.compute_coordinator();
        if next == self.coordinator {
            return Vec::new();
        }
        self.coordinator.clone_from(&next);
        vec![Effect::CoordinatorChanged { coordinator: next }]
    }
}
