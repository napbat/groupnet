//! Local commands and their application.

use crate::NodeId;
use crate::membership::{Member, StateEntry, Status};

use super::effect::Effect;
use super::state::{GroupEngine, VersionedValue};

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

impl GroupEngine {
    /// The reserved key backing the single-blob state shim.
    pub const BLOB_KEY: &str = "~blob";

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
                    StateEntry::adopted(version, Vec::new(), 0, true, now),
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

    /// Author one key of our own state: bump above whatever version our per-node
    /// clock has reached (covering every key, and any adopted echo) so the write
    /// supersedes every prior copy and advances our digest high-water mark.
    pub(super) fn set_local_entry(
        &mut self,
        key: &str,
        value: Vec<u8>,
        ttl_ms: Option<u64>,
    ) -> Vec<Effect> {
        let now = self.now_hint;
        let ttl = ttl_ms.unwrap_or(0);
        let Some(m) = self.members.get_mut(&self.local) else {
            return Vec::new();
        };
        let version = m.max_state_version.saturating_add(1);
        m.max_state_version = version;
        m.entries.insert(
            key.to_owned(),
            StateEntry::adopted(version, value, ttl, false, now),
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
}
