use std::collections::{BTreeMap, BTreeSet};

use crate::{GroupId, NodeId};
use crate::{Time, coord, wire};

/// Tunables for a single group's gossip and failure detection.
#[derive(Clone, Debug)]
pub struct Config {
    /// How often (ms of logical time) to disseminate the full view.
    pub gossip_interval_ms: u64,
    /// How often to probe a member for liveness.
    pub probe_interval_ms: u64,
    /// How long to wait for a probe ack before suspecting the target.
    pub probe_timeout_ms: u64,
    /// How long a member may stay `Suspect` before it is declared `Dead`.
    pub suspect_timeout_ms: u64,
    /// Maximum peers to disseminate to per gossip round.
    pub fanout: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            gossip_interval_ms: 200,
            probe_interval_ms: 100,
            probe_timeout_ms: 50,
            suspect_timeout_ms: 500,
            fanout: 3,
        }
    }
}

/// A member's liveness status. Ordered by precedence: a higher-precedence
/// status wins ties at equal incarnation during merge.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    /// Believed healthy.
    Alive,
    /// A probe went unanswered; awaiting refutation or death.
    Suspect,
    /// Confirmed gone (failed or voluntarily left). Terminal.
    Dead,
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

/// A member's local record.
#[derive(Clone, Debug)]
struct Member {
    incarnation: u64,
    status: Status,
    /// When *this* node first observed the member as `Suspect` (for the
    /// suspicion timeout). Only meaningful while `status == Suspect`.
    suspect_since: Time,
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
    /// This node voluntarily leaves the group.
    Leave,
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
    /// A metadata key took a new value (from a local write or a merged delta).
    MetadataChanged {
        /// The key that changed.
        key: String,
        /// Its new value.
        value: String,
    },
}

/// The per-group coordination state machine.
///
/// One instance owns the state of exactly one group on one node. It is a plain
/// value: `Send`, cheap to move between threads, and free of any I/O or clock
/// access. Drive it by feeding events ([`on_message`](Self::on_message),
/// [`on_tick`](Self::on_tick), [`apply`](Self::apply)) and executing the
/// [`Effect`]s it returns.
///
/// Membership uses a SWIM-style protocol: direct liveness probes
/// (`Ping`/`Ack`), a `Suspect` state with a refutation window, and
/// last-writer-wins merge keyed by per-node incarnation numbers. Indirect
/// probes (`ping-req`) are not yet implemented.
#[derive(Debug)]
pub struct GroupEngine {
    group: GroupId,
    local: NodeId,
    /// The local node's incarnation, bumped to refute suspicion about itself.
    incarnation: u64,
    /// Set once the local node has voluntarily left (so it won't refute its own
    /// death).
    leaving: bool,
    /// All known members, including self. Dead entries are retained as
    /// tombstones (not reaped in this scaffold).
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
    /// The outstanding probe awaiting an ack, if any.
    pending: Option<Pending>,
}

#[derive(Clone, Debug)]
struct Pending {
    target: NodeId,
    deadline: Time,
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
        members.insert(
            local.clone(),
            Member {
                incarnation: 0,
                status: Status::Alive,
                suspect_since: Time::ZERO,
            },
        );
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
        };
        engine.coordinator = engine.compute_coordinator();
        engine
    }

    // ---- reads -----------------------------------------------------------

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

    /// The status of a specific node, if known (including `Dead` tombstones).
    #[must_use]
    pub fn member_status(&self, node: &NodeId) -> Option<Status> {
        self.members.get(node).map(|m| m.status)
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

    // ---- lifecycle -------------------------------------------------------

    /// Primes the engine: announces this node and arms the first timers. A
    /// driver calls this once, right after construction, passing current time.
    pub fn start(&mut self, now: Time) -> Vec<Effect> {
        self.next_gossip = now.saturating_add(self.config.gossip_interval_ms);
        self.next_probe = now.saturating_add(self.config.probe_interval_ms);
        let mut effects = self.gossip();
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
            Command::Leave => {
                // Declare ourselves Dead at our current incarnation and stop
                // refuting. Dead supersedes Alive at equal incarnation, so the
                // leave sticks as it disseminates.
                self.leaving = true;
                if let Some(m) = self.members.get_mut(&self.local) {
                    m.status = Status::Dead;
                }
                let mut effects = vec![Effect::MembershipChanged];
                effects.extend(self.recompute_coordinator());
                effects.extend(self.gossip()); // push the leave out immediately
                effects
            }
        }
    }

    /// Handles an inbound wire frame from node `from`, observed at `now`.
    pub fn on_message(&mut self, from: NodeId, wire: &[u8], now: Time) -> Vec<Effect> {
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
                // Prove we're alive by replying with our current view.
                let ack = self.make_frame(wire::Kind::Ack);
                effects.push(Effect::Send {
                    to: from,
                    wire: ack,
                });
            }
            wire::Kind::Ack => {
                if self.pending.as_ref().is_some_and(|p| p.target == from) {
                    self.pending = None; // liveness confirmed
                }
            }
            wire::Kind::Gossip => {}
        }
        effects
    }

    /// Advances logical time: expires probe acks and suspicions, and emits
    /// probe / gossip rounds when due. Safe to call more often than requested.
    pub fn on_tick(&mut self, now: Time) -> Vec<Effect> {
        let mut effects = Vec::new();

        // 1. An outstanding probe whose ack window elapsed -> suspect it.
        if self.pending.as_ref().is_some_and(|p| now >= p.deadline) {
            let target = self.pending.take().expect("checked").target;
            effects.extend(self.suspect(&target, now));
        }

        // 2. Suspects past their suspicion window -> dead.
        effects.extend(self.reap_suspects(now));

        // 3. Send the next liveness probe.
        if now >= self.next_probe {
            self.next_probe = now.saturating_add(self.config.probe_interval_ms);
            effects.extend(self.probe(now));
        }

        // 4. Disseminate the full view.
        if now >= self.next_gossip {
            self.next_gossip = now.saturating_add(self.config.gossip_interval_ms);
            effects.extend(self.gossip());
        }

        effects.push(self.arm_timer());
        effects
    }

    // ---- failure detection ----------------------------------------------

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
        });
        vec![Effect::Send {
            to: target,
            wire: self.make_frame(wire::Kind::Ping),
        }]
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
            }
        }
        let mut effects = vec![Effect::MembershipChanged];
        effects.extend(self.recompute_coordinator());
        effects
    }

    // ---- merge -----------------------------------------------------------

    /// Merges incoming member deltas by SWIM precedence, refuting any suspicion
    /// about ourselves.
    fn merge_members(&mut self, deltas: Vec<wire::MemberDelta>, now: Time) -> Vec<Effect> {
        let mut changed = false;
        let mut refute_to: Option<u64> = None;

        for delta in deltas {
            let Some(status) = status_from_wire(delta.status) else {
                continue; // unknown status code — ignore
            };

            if delta.node == self.local {
                // Someone thinks we're suspect/dead: refute by out-incarnating,
                // unless we're deliberately leaving.
                if !self.leaving && status != Status::Alive && delta.incarnation >= self.incarnation
                {
                    let target = delta.incarnation + 1;
                    refute_to = Some(refute_to.map_or(target, |t| t.max(target)));
                }
                continue;
            }

            match self.members.get(&delta.node) {
                Some(cur) if !supersedes(cur, delta.incarnation, status) => {}
                existing => {
                    let suspect_since = if status == Status::Suspect {
                        now // our suspicion clock for this member starts now
                    } else {
                        Time::ZERO
                    };
                    let _ = existing;
                    self.members.insert(
                        delta.node,
                        Member {
                            incarnation: delta.incarnation,
                            status,
                            suspect_since,
                        },
                    );
                    changed = true;
                }
            }
        }

        if let Some(new_incarnation) = refute_to {
            self.incarnation = new_incarnation;
            if let Some(m) = self.members.get_mut(&self.local) {
                m.incarnation = new_incarnation;
                m.status = Status::Alive;
            }
            changed = true;
        }

        if !changed {
            return Vec::new();
        }
        let mut effects = vec![Effect::MembershipChanged];
        effects.extend(self.recompute_coordinator());
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

    // ---- helpers ---------------------------------------------------------

    /// Nodes we may probe or gossip to: live members (excluding self) plus any
    /// seeds we haven't confirmed yet.
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

    fn gossip(&self) -> Vec<Effect> {
        let targets = self.dissemination_targets();
        if targets.is_empty() {
            return Vec::new();
        }
        let frame = self.make_frame(wire::Kind::Gossip);
        targets
            .into_iter()
            .take(self.config.fanout.max(1))
            .map(|to| Effect::Send {
                to,
                wire: frame.clone(),
            })
            .collect()
    }

    fn make_frame(&self, kind: wire::Kind) -> Vec<u8> {
        wire::encode(&wire::Frame {
            kind,
            group: self.group.clone(),
            members: self
                .members
                .iter()
                .map(|(node, m)| wire::MemberDelta {
                    node: node.clone(),
                    incarnation: m.incarnation,
                    status: status_to_wire(m.status),
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

    fn compute_coordinator(&self) -> Option<NodeId> {
        // Coordinator is chosen among live members (Alive or Suspect); a Dead
        // node is never a candidate.
        let live: BTreeSet<NodeId> = self.members().cloned().collect();
        coord::select(&self.group, &live)
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

/// SWIM merge precedence: does `(incarnation, status)` override `cur`?
///
/// * `Alive` overrides only a strictly newer incarnation (you must
///   out-incarnate to refute).
/// * `Suspect` overrides an alive member at equal-or-newer incarnation, or a
///   suspect at strictly newer; never a dead one.
/// * `Dead` overrides anything not already dead at equal-or-newer incarnation.
fn supersedes(cur: &Member, incarnation: u64, status: Status) -> bool {
    match status {
        Status::Alive => incarnation > cur.incarnation,
        Status::Suspect => match cur.status {
            Status::Alive => incarnation >= cur.incarnation,
            Status::Suspect => incarnation > cur.incarnation,
            Status::Dead => false,
        },
        Status::Dead => cur.status != Status::Dead && incarnation >= cur.incarnation,
    }
}

fn status_to_wire(s: Status) -> u8 {
    match s {
        Status::Alive => 0,
        Status::Suspect => 1,
        Status::Dead => 2,
    }
}

fn status_from_wire(b: u8) -> Option<Status> {
    match b {
        0 => Some(Status::Alive),
        1 => Some(Status::Suspect),
        2 => Some(Status::Dead),
        _ => None,
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
            members,
            metadata,
        })
    }

    fn alive(node: &str, inc: u64) -> wire::MemberDelta {
        wire::MemberDelta {
            node: NodeId::new(node),
            incarnation: inc,
            status: status_to_wire(Status::Alive),
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
            &gossip_frame(vec![alive("b", 0)], vec![]),
            Time(1),
        );
        assert_eq!(a.members().count(), 2);
        let set: BTreeSet<NodeId> = [NodeId::new("a"), NodeId::new("b")].into_iter().collect();
        assert_eq!(
            a.coordinator().cloned(),
            coord::select(&GroupId::new("g"), &set)
        );
    }

    #[test]
    fn unanswered_probe_leads_to_suspect_then_dead() {
        let cfg = Config {
            probe_interval_ms: 100,
            probe_timeout_ms: 50,
            suspect_timeout_ms: 200,
            gossip_interval_ms: 100,
            fanout: 3,
        };
        let mut a = GroupEngine::new(GroupId::new("g"), NodeId::new("a"), [NodeId::new("b")], cfg);
        // Learn b as alive.
        a.on_message(
            NodeId::new("b"),
            &gossip_frame(vec![alive("b", 0)], vec![]),
            Time(1),
        );
        a.start(Time(1));

        // Probe fires; b never acks. After probe_timeout, b is suspect.
        a.on_tick(Time(101)); // sends the probe (deadline 151)
        a.on_tick(Time(160)); // ack window elapsed -> suspect
        assert_eq!(a.member_status(&NodeId::new("b")), Some(Status::Suspect));

        // After the suspicion window, b is dead and drops out of membership.
        a.on_tick(Time(400));
        assert_eq!(a.member_status(&NodeId::new("b")), Some(Status::Dead));
        assert!(!a.members().any(|n| *n == NodeId::new("b")));
    }

    #[test]
    fn refutes_false_suspicion_about_self() {
        let mut a = engine("a", &["b"]);
        // b claims a is suspect at incarnation 0.
        a.on_message(
            NodeId::new("b"),
            &gossip_frame(
                vec![wire::MemberDelta {
                    node: NodeId::new("a"),
                    incarnation: 0,
                    status: status_to_wire(Status::Suspect),
                }],
                vec![],
            ),
            Time(1),
        );
        // a must still consider itself alive, at a higher incarnation.
        assert_eq!(a.member_status(&NodeId::new("a")), Some(Status::Alive));
        // The refutation must bump our incarnation above the suspect's, so the
        // re-asserted Alive supersedes it as it disseminates.
        let frame = wire::decode(&a.make_frame(wire::Kind::Gossip)).expect("valid frame");
        let self_delta = frame
            .members
            .iter()
            .find(|m| m.node == NodeId::new("a"))
            .expect("self in frame");
        assert_eq!(self_delta.status, status_to_wire(Status::Alive));
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
        // Even hearing itself alive again shouldn't resurrect a leaver here:
        // a Dead-about-self arrives and is simply ignored (we don't refute).
        a.on_message(
            NodeId::new("b"),
            &gossip_frame(
                vec![wire::MemberDelta {
                    node: NodeId::new("a"),
                    incarnation: 0,
                    status: status_to_wire(Status::Dead),
                }],
                vec![],
            ),
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
}
