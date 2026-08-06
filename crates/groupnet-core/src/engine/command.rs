//! Local commands and their application.

use crate::membership::{Member, StateEntry, Status};
use crate::{NodeId, Time};

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
    /// **The driver won an external-anchor round:** this node now holds
    /// `epoch` at the anchor, and its authority should lapse at `lease_until`.
    ///
    /// Row **X2** activates on it — the same
    /// [`activate`](crate::GroupEngine) row 4 runs, so a `LeadState` broadcast
    /// and one [`Effect::LeadershipChanged`](crate::Effect::LeadershipChanged)
    /// follow — and row **X3** treats it as a lease extension when this node
    /// is already `Host` at exactly `epoch`. Ignored (row **X6**) outside
    /// [`Activation::External`](crate::Activation::External) and below the
    /// monotone bar: an `epoch` under
    /// [`observed_epoch`](crate::GroupEngine::observed_epoch) is a stale or
    /// duplicated report and dies here, which is where driver input has to be
    /// filtered.
    ///
    /// # `lease_until` is the engine's logical clock, and is anchored *early*
    ///
    /// It is a [`Time`], not a wall-clock millisecond — the anchor record's
    /// `expires_at_wall_ms` never enters the engine, and this never leaves it.
    /// The driver passes it in rather than the engine deriving it, because
    /// deriving it would mean knowing how long the store round-trip took.
    ///
    /// **A driver must anchor it at the instant it *began* the round — the
    /// time captured before the load, not after the CAS returned.** The
    /// record's `expires_at_wall_ms` was computed from a `now_wall_ms` sampled
    /// at or after that instant, so a lease anchored at initiation always
    /// expires no later than the record it was earned from. Anchoring it after
    /// the round hands the host an overhang past its own anchor record, which
    /// is exactly the window a successor's steal is entitled to use. (Same
    /// argument as `Quorum`'s send-instant attribution, in a different dress.)
    AnchorActivated {
        /// The epoch the anchor awarded this node.
        epoch: u64,
        /// When this node's hostship lapses, in the engine's logical time,
        /// anchored at the instant the anchor round began.
        lease_until: Time,
    },
    /// **The driver read an anchor record it did not win:** the anchor
    /// currently names `host` at `epoch`.
    ///
    /// Row **X4** adopts it when the pair `(epoch, host)` outranks the adopted
    /// one in the ordinary epoch-major fencing order — deposing this node if
    /// it was hosting — and row **X5** is row 12b verbatim when it names *this*
    /// node at a strictly higher epoch: the epoch is learned, the hostship is
    /// not, and the node re-earns the group by winning above it. Hostship is
    /// only ever entered by this node's own activation, and a record naming us
    /// is evidence of an epoch rather than a grant of authority — which is why
    /// a restart re-wins through the anchor instead of resuming.
    ///
    /// Ignored (row **X6**) outside
    /// [`Activation::External`](crate::Activation::External) and whenever the
    /// observed pair does not outrank the adopted one; an equal or lower pair
    /// teaches nothing and announces nothing.
    ///
    /// This reports what the record *says*, not who is alive: an expired
    /// record still names its holder until somebody supersedes it, and
    /// adoption here is fence-ordered rather than liveness-ordered. The
    /// successor's own activation is what moves the cluster on, and it
    /// broadcasts `LeadState` to every live member when it does.
    AnchorObserved {
        /// The epoch the anchor record carries.
        epoch: u64,
        /// The node that record names as host.
        host: NodeId,
    },
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
                // Give up hostship (or a standing claim) first, so this node
                // never serves an epoch it has already announced it is gone
                // from. A no-op in an `Eventual` group.
                let mut effects = self.election_on_leave();
                // Declare ourselves Dead at our current incarnation and stop
                // refuting. Dead supersedes Alive at equal incarnation, so the
                // leave sticks as it disseminates on the next digest round.
                self.leaving = true;
                // A command carries no clock; `now_hint` is the freshest time
                // the engine has been told about (one event-loop turn stale at
                // worst — far finer than any status duration is read at).
                let now = self.now_hint;
                if let Some(m) = self.members.get_mut(&self.local) {
                    m.adopt_status(Status::Dead, now);
                }
                self.stamp_self();
                effects.push(Effect::MembershipChanged);
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
                    .insert(node.clone(), Member::new(0, Status::Alive, self.now_hint));
                self.stamp(&node);
                let mut effects = vec![Effect::MembershipChanged];
                effects.extend(self.recompute_coordinator());
                self.nudge_anti_entropy();
                effects
            }
            // Rows X2/X3 and X4/X5 — the external anchor's whole inbound
            // surface. Both are gated on `Activation::External` inside, so an
            // `Eventual`, `Settle` or `Quorum` group drops them silently
            // (row X6): a driver misconfiguration must not be able to make a
            // group host on nobody's authority.
            Command::AnchorActivated { epoch, lease_until } => {
                self.on_anchor_activated(epoch, lease_until)
            }
            Command::AnchorObserved { epoch, host } => self.on_anchor_observed(epoch, &host),
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
