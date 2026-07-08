use std::collections::{BTreeMap, BTreeSet};

use crate::{GroupId, NodeId};
use crate::{Time, coord, wire};

/// Tunables for a single group's gossip behaviour.
#[derive(Clone, Debug)]
pub struct Config {
    /// How often (ms of logical time) the engine wants to gossip its view.
    pub gossip_interval_ms: u64,
    /// Maximum peers to gossip to per round.
    pub fanout: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            gossip_interval_ms: 200,
            fanout: 3,
        }
    }
}

/// A metadata value tagged with a version and its writer, so replicas can
/// resolve concurrent writes by last-writer-wins with a deterministic
/// `(version, writer)` tiebreak.
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
    /// This node leaves the group.
    Leave,
}

/// An intent the engine emits in response to an event. The driver is
/// responsible for carrying it out — the engine itself performs no I/O.
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
    /// A driver with a coarser timer may tick earlier; the engine is
    /// idempotent under early ticks.
    ArmTimer {
        /// Absolute logical time by which a tick is wanted.
        at: Time,
    },
    /// The coordinator changed (including to/from `None`).
    CoordinatorChanged {
        /// The new coordinator, or `None` if the group is now empty.
        coordinator: Option<NodeId>,
    },
    /// The membership set changed.
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
#[derive(Debug)]
pub struct GroupEngine {
    group: GroupId,
    local: NodeId,
    /// Confirmed members (grow-set in this scaffold).
    members: BTreeSet<NodeId>,
    /// Nodes we gossip toward: initial seeds plus everyone we've learned of.
    peers: BTreeSet<NodeId>,
    metadata: BTreeMap<String, VersionedValue>,
    coordinator: Option<NodeId>,
    config: Config,
    next_gossip: Time,
}

impl GroupEngine {
    /// Creates an engine for `local`'s participation in `group`.
    ///
    /// `seeds` are contact nodes to bootstrap gossip against; membership itself
    /// starts as just `{local}` and grows as gossip converges.
    pub fn new(
        group: GroupId,
        local: NodeId,
        seeds: impl IntoIterator<Item = NodeId>,
        config: Config,
    ) -> Self {
        let mut members = BTreeSet::new();
        members.insert(local.clone());
        let peers = seeds
            .into_iter()
            .filter(|p| *p != local)
            .collect::<BTreeSet<_>>();
        let coordinator = coord::select(&group, &members);
        Self {
            group,
            local,
            members,
            peers,
            metadata: BTreeMap::new(),
            coordinator,
            config,
            next_gossip: Time::ZERO,
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

    /// Iterates the current membership set in id order.
    pub fn members(&self) -> impl Iterator<Item = &NodeId> {
        self.members.iter()
    }

    /// Reads a shard-local metadata value.
    #[must_use]
    pub fn metadata(&self, key: &str) -> Option<&str> {
        self.metadata.get(key).map(|v| v.value.as_str())
    }

    /// Iterates all metadata key/value pairs in key order (e.g. to publish a
    /// snapshot).
    pub fn metadata_iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.metadata
            .iter()
            .map(|(k, v)| (k.as_str(), v.value.as_str()))
    }

    /// Primes the engine: announces this node and arms the first gossip timer.
    /// A driver calls this once, right after construction, passing current time.
    pub fn start(&mut self, now: Time) -> Vec<Effect> {
        self.next_gossip = now.saturating_add(self.config.gossip_interval_ms);
        let mut effects = vec![Effect::ArmTimer {
            at: self.next_gossip,
        }];
        effects.extend(self.gossip());
        effects
    }

    /// Applies a local command.
    pub fn apply(&mut self, cmd: Command) -> Vec<Effect> {
        match cmd {
            Command::UpdateMetadata { key, value } => {
                // Bump above the highest version we currently hold for the key
                // (already the merged max), so a fresh local write supersedes
                // anything we've seen. Peers converge on it via gossip + LWW.
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
                self.members.remove(&self.local);
                // NOTE: a grow-set can't truly express a leave — peers will
                // re-add us on their next gossip. Real membership needs a
                // tombstone (SWIM's `dead` state) disseminated to peers.
                let mut effects = vec![Effect::MembershipChanged];
                effects.extend(self.recompute_coordinator());
                effects
            }
        }
    }

    /// Handles an inbound wire frame from node `from`.
    pub fn on_message(&mut self, from: NodeId, wire: &[u8]) -> Vec<Effect> {
        let Some(msg) = wire::decode(wire) else {
            return Vec::new(); // undecodable == dropped; transport is best-effort
        };
        match msg {
            wire::Msg::Gossip {
                group,
                members,
                metadata,
            } => {
                if group != self.group {
                    return Vec::new(); // not ours
                }
                if from != self.local {
                    self.peers.insert(from);
                }
                let mut membership_changed = false;
                for m in members {
                    if m != self.local {
                        self.peers.insert(m.clone());
                    }
                    membership_changed |= self.members.insert(m);
                }
                let mut effects = Vec::new();
                if membership_changed {
                    effects.push(Effect::MembershipChanged);
                    effects.extend(self.recompute_coordinator());
                }
                effects.extend(self.merge_metadata(metadata));
                effects
            }
        }
    }

    /// Merges incoming metadata deltas by last-writer-wins: an entry is adopted
    /// iff its `(version, writer)` strictly exceeds what we hold. This is a
    /// per-key LWW-register (a CRDT), so all replicas converge on one value.
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

    /// Advances logical time. Emits a gossip round when due and re-arms the
    /// timer. Safe to call more often than requested.
    pub fn on_tick(&mut self, now: Time) -> Vec<Effect> {
        if now < self.next_gossip {
            // Not due yet — just (re)arm so a coarse driver still converges.
            return vec![Effect::ArmTimer {
                at: self.next_gossip,
            }];
        }
        self.next_gossip = now.saturating_add(self.config.gossip_interval_ms);
        let mut effects = vec![Effect::ArmTimer {
            at: self.next_gossip,
        }];
        effects.extend(self.gossip());
        effects
    }

    fn gossip(&self) -> Vec<Effect> {
        if self.peers.is_empty() {
            return Vec::new();
        }
        let frame = wire::encode(&wire::Msg::Gossip {
            group: self.group.clone(),
            members: self.members.iter().cloned().collect(),
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
        });
        // Deterministic fanout: the first N peers in id order. Membership still
        // reaches everyone transitively as peers gossip onward.
        self.peers
            .iter()
            .take(self.config.fanout.max(1))
            .map(|to| Effect::Send {
                to: to.clone(),
                wire: frame.clone(),
            })
            .collect()
    }

    fn recompute_coordinator(&mut self) -> Vec<Effect> {
        let next = coord::select(&self.group, &self.members);
        if next == self.coordinator {
            return Vec::new();
        }
        self.coordinator = next.clone();
        vec![Effect::CoordinatorChanged { coordinator: next }]
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

    #[test]
    fn start_announces_to_seeds() {
        let mut e = engine("a", &["b", "c"]);
        let effects = e.start(Time::ZERO);
        let sends = effects
            .iter()
            .filter(|e| matches!(e, Effect::Send { .. }))
            .count();
        assert_eq!(sends, 2, "should gossip to both seeds");
    }

    #[test]
    fn merging_membership_updates_coordinator() {
        let mut a = engine("a", &["b"]);
        let frame = wire::encode(&wire::Msg::Gossip {
            group: GroupId::new("g"),
            members: vec![NodeId::new("b")],
            metadata: vec![],
        });
        a.on_message(NodeId::new("b"), &frame);
        assert_eq!(a.members().count(), 2);
        // Coordinator is now computed over {a, b}; must match an independent
        // computation over the same set.
        let set: BTreeSet<NodeId> = [NodeId::new("a"), NodeId::new("b")].into_iter().collect();
        assert_eq!(
            a.coordinator().cloned(),
            coord::select(&GroupId::new("g"), &set)
        );
    }

    fn gossip_metadata(deltas: Vec<wire::MetaDelta>) -> Vec<u8> {
        wire::encode(&wire::Msg::Gossip {
            group: GroupId::new("g"),
            members: vec![],
            metadata: deltas,
        })
    }

    #[test]
    fn metadata_merges_by_last_writer_wins() {
        let mut a = engine("a", &["b"]);

        // A remote delta beats having nothing.
        a.on_message(
            NodeId::new("z"),
            &gossip_metadata(vec![wire::MetaDelta {
                key: "k".into(),
                version: 2,
                writer: NodeId::new("z"),
                value: "remote".into(),
            }]),
        );
        assert_eq!(a.metadata("k"), Some("remote"));

        // A local write bumps above the merged version (2 -> 3) and wins.
        a.apply(Command::UpdateMetadata {
            key: "k".into(),
            value: "local".into(),
        });
        assert_eq!(a.metadata("k"), Some("local"));

        // A lower-version remote delta must NOT override the local write.
        a.on_message(
            NodeId::new("z"),
            &gossip_metadata(vec![wire::MetaDelta {
                key: "k".into(),
                version: 1,
                writer: NodeId::new("z"),
                value: "stale".into(),
            }]),
        );
        assert_eq!(a.metadata("k"), Some("local"));
    }

    #[test]
    fn concurrent_same_version_writes_break_ties_by_writer() {
        // Two deltas at the same version: the greater writer id wins,
        // deterministically, regardless of arrival order.
        let apply_order = |first: &str, second: &str| {
            let mut e = engine("a", &[]);
            for w in [first, second] {
                e.on_message(
                    NodeId::new(w),
                    &gossip_metadata(vec![wire::MetaDelta {
                        key: "k".into(),
                        version: 1,
                        writer: NodeId::new(w),
                        value: w.into(),
                    }]),
                );
            }
            e.metadata("k").map(str::to_owned)
        };
        assert_eq!(apply_order("x", "y"), Some("y".into()));
        assert_eq!(apply_order("y", "x"), Some("y".into()));
    }
}
