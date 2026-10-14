//! The per-group consistency posture a group is joined under, one variant per
//! mode, each carrying only the options that apply to it.

use std::fmt;
use std::sync::Arc;

use groupnet_core::{Activation, GroupMode, HostedConfig, VoterRoster};

use crate::anchor::Anchor;
use crate::store::VoterStorage;

#[cfg(doc)]
use crate::Node;
#[cfg(doc)]
use groupnet_core::{Config, RecoveredGrant};

/// The consistency posture one group is joined under — what
/// [`Node::join_group_with`] takes.
///
/// The posture is **per group, not per node**: a node freely mixes hosted
/// shard groups with eventual fabric groups, and opting one group in cannot
/// make another run an election. Everything else about the group (gossip
/// cadence and fanout, detector timings) still comes from the node's builder
/// config — a profile only decides the mode.
///
/// Each variant carries exactly its mode's options, so a setting that cannot
/// apply does not exist: **voter storage** belongs to [`Quorum`](Self::Quorum)
/// alone (the only mode with a voter ledger) and the **anchor** to
/// [`External`](Self::External) alone (the only mode whose epochs an external
/// register allocates):
///
/// ```compile_fail,E0559
/// # fn demo(storage: groupnet_runtime::VoterStorage) {
/// let _ = groupnet_runtime::GroupProfile::Settle {
///     lease_ms: 2_000,
///     claim_settle_ms: 600,
///     storage: Some(storage),
/// };
/// # }
/// ```
///
/// ```compile_fail,E0559
/// # fn demo(anchor: std::sync::Arc<dyn groupnet_runtime::Anchor>, voters: groupnet_core::VoterRoster) {
/// let _ = groupnet_runtime::GroupProfile::Quorum {
///     lease_ms: 2_000,
///     voters,
///     storage: None,
///     anchor: Some(anchor),
/// };
/// # }
/// ```
///
/// The hosted variants' timings are the engine's logical milliseconds, the
/// fields of the [`HostedConfig`] they build; size them by the rules on
/// [`HostedConfig`].
///
/// ```no_run
/// use groupnet_runtime::GroupProfile;
///
/// # fn demo(node: &groupnet_runtime::Node) {
/// let shard = node.join_group_with(
///     "shard-7",
///     GroupProfile::Settle {
///         lease_ms: 2_000,
///         claim_settle_ms: 600,
///     },
/// );
/// # let _ = shard;
/// # }
/// ```
///
/// Neither the store nor the anchor is part of a profile's identity (two
/// profiles differing only in which file their store writes to, or which
/// bucket their anchor lives in, describe the same group), which is why this
/// type carries no `PartialEq`.
#[derive(Clone)]
pub enum GroupProfile {
    /// Metadata and membership only, converging eventually — no election, no
    /// host, and no election frames on the wire. What every group was before
    /// Hosted mode existed, and what [`Node::join_group`] uses unless the
    /// node's [`Config::mode`] says otherwise.
    Eventual,
    /// A hosted group whose claimant activates after an unchallenged settle
    /// window ([`Activation::Settle`]), surfaced through
    /// [`Group::leadership`](crate::Group::leadership) and
    /// [`GroupEvent::LeadershipChanged`](crate::GroupEvent::LeadershipChanged).
    Settle {
        /// [`HostedConfig::lease_ms`].
        lease_ms: u64,
        /// The settle window, `Activation::Settle::claim_settle_ms`.
        claim_settle_ms: u64,
    },
    /// A hosted group whose host a majority of a static voter roster grants
    /// ([`Activation::Quorum`]).
    ///
    /// # Voter storage: what it buys, and what it does not
    ///
    /// `storage` gives this group's **voter ledger** durability: what storage
    /// says was granted before this process started, and where to write what
    /// it grants next. Only a node named in the roster ever writes to it.
    ///
    /// Without it (`None`), a voter falls back on the engine's **boot
    /// blackout**: it refuses every new claimant for one `lease_ms` after
    /// start, on the argument that any grant it made before the crash has
    /// expired by then. That is a timing rule standing in for a durability
    /// one. With it, the granted pair is a floor that survives the restart
    /// outright — this voter can never grant that epoch to a second claimant
    /// however long it was down — and the claimant named in it may be
    /// re-granted **immediately**, so a restarted voter stops starving the
    /// sitting host for a lease.
    ///
    /// What it does not buy is a shorter blackout for anyone *else*: recovery
    /// restores the pair, never the instant it was granted at, so a recovered
    /// voter still applies the boot-anchored window to every new claimant. See
    /// [`RecoveredGrant`] for that argument in full, and in particular for why
    /// [`RecoveredGrant::none`] is the one statement that lifts the blackout —
    /// and may only be made by a driver that really did persist every grant.
    ///
    /// # Load recovery *before* joining
    ///
    /// The recovered grant is a value, not a callback, because the join path
    /// is synchronous and spawns the group actor under the node's group-table
    /// lock: it can await nothing and must touch no disk. Read the store, then
    /// join.
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use groupnet_core::{RecoveredGrant, VoterRoster};
    /// # use groupnet_runtime::{GrantStore, GroupProfile, VoterStorage};
    /// # fn demo(
    /// #     node: &groupnet_runtime::Node,
    /// #     store: Arc<dyn GrantStore>,
    /// #     recovered: RecoveredGrant,
    /// #     voters: VoterRoster,
    /// # ) {
    /// // `recovered` was read off `store` before this call — no I/O happens
    /// // inside the join.
    /// let shard = node.join_group_with(
    ///     "shard-7",
    ///     GroupProfile::Quorum {
    ///         lease_ms: 2_000,
    ///         voters,
    ///         storage: Some(VoterStorage::new(recovered, store)),
    ///     },
    /// );
    /// # let _ = shard;
    /// # }
    /// ```
    Quorum {
        /// [`HostedConfig::lease_ms`].
        lease_ms: u64,
        /// The voters whose majority closes an epoch.
        voters: VoterRoster,
        /// Voter durability; `None` for the boot-blackout posture.
        storage: Option<VoterStorage>,
    },
    /// A hosted group whose epochs a linearizable external CAS register
    /// allocates ([`Activation::External`]): a claimant must win a conditional
    /// write on the `anchor` to become host.
    ///
    /// # No anchor, no host
    ///
    /// An `External` group configured **without** one (`anchor: None`) is a
    /// supported, deliberately fail-safe posture, not a misconfiguration the
    /// runtime rescues: the engine's [`AnchorClaimDue`] prompts are dropped,
    /// this node never claims, and the group stays at `(0, None)` unless it
    /// *adopts* a pair some other node's driver won and beaconed. So a
    /// read-only member of an `External` group — one that must never host, but
    /// must follow whoever does — is configured by leaving the anchor off, and
    /// it costs nothing on the wire.
    ///
    /// # One anchor, one object, one group
    ///
    /// A process hosting several `External` groups gives each its own
    /// [`Anchor`] over its own object: the trait names no group, because the
    /// profile a group is joined under is what pairs them. Two groups sharing
    /// one object would share one epoch sequence and fight, and no rule in the
    /// tier can detect it.
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use groupnet_runtime::{Anchor, GroupProfile};
    /// # fn demo(
    /// #     node: &groupnet_runtime::Node,
    /// #     anchor: Arc<dyn Anchor>,
    /// # ) {
    /// let shard = node.join_group_with(
    ///     "shard-7",
    ///     GroupProfile::External {
    ///         lease_ms: 2_000,
    ///         steal_margin_ms: 500,
    ///         anchor: Some(anchor),
    ///     },
    /// );
    /// # let _ = shard;
    /// # }
    /// ```
    ///
    /// [`AnchorClaimDue`]: groupnet_core::Effect::AnchorClaimDue
    External {
        /// [`HostedConfig::lease_ms`]: the anchor record's TTL and the engine
        /// lease, one knob by design.
        lease_ms: u64,
        /// How far past a record's expiry a claimant waits before stealing,
        /// `Activation::External::steal_margin_ms`.
        steal_margin_ms: u64,
        /// The register epochs are allocated at; `None` for the never-claims
        /// posture.
        anchor: Option<Arc<dyn Anchor>>,
    },
}

impl fmt::Debug for GroupProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Neither the store nor the anchor carries a `Debug` bound —
        // deliberately, so an implementation is free to hold a client handle
        // and nothing else.
        match self {
            Self::Eventual => f.write_str("Eventual"),
            Self::Settle {
                lease_ms,
                claim_settle_ms,
            } => f
                .debug_struct("Settle")
                .field("lease_ms", lease_ms)
                .field("claim_settle_ms", claim_settle_ms)
                .finish(),
            Self::Quorum {
                lease_ms,
                voters,
                storage,
            } => f
                .debug_struct("Quorum")
                .field("lease_ms", lease_ms)
                .field("voters", voters)
                .field("storage", storage)
                .finish(),
            Self::External {
                lease_ms,
                steal_margin_ms,
                anchor,
            } => f
                .debug_struct("External")
                .field("lease_ms", lease_ms)
                .field("steal_margin_ms", steal_margin_ms)
                .field("anchor", &anchor.is_some())
                .finish(),
        }
    }
}

/// What one group's actor is actually spawned with: the engine mode plus the
/// mode's own resources. Built from a [`GroupProfile`], or from the node's
/// [`Config::mode`] (which carries no resources) for a bare join.
pub(super) struct Posture {
    pub(super) mode: GroupMode,
    pub(super) storage: Option<VoterStorage>,
    /// The anchor with the lease and steal margin its claims run under.
    pub(super) anchor: Option<(Arc<dyn Anchor>, u64, u64)>,
}

impl Posture {
    /// The node-wide mode a bare [`Node::join_group`] joins under.
    pub(super) const fn from_mode(mode: GroupMode) -> Self {
        Self {
            mode,
            storage: None,
            anchor: None,
        }
    }
}

impl From<GroupProfile> for Posture {
    fn from(profile: GroupProfile) -> Self {
        let hosted = |activation, lease_ms| {
            GroupMode::Hosted(HostedConfig {
                activation,
                lease_ms,
            })
        };
        match profile {
            GroupProfile::Eventual => Self::from_mode(GroupMode::Eventual),
            GroupProfile::Settle {
                lease_ms,
                claim_settle_ms,
            } => Self::from_mode(hosted(Activation::Settle { claim_settle_ms }, lease_ms)),
            GroupProfile::Quorum {
                lease_ms,
                voters,
                storage,
            } => Self {
                mode: hosted(Activation::Quorum { voters }, lease_ms),
                storage,
                anchor: None,
            },
            GroupProfile::External {
                lease_ms,
                steal_margin_ms,
                anchor,
            } => Self {
                mode: hosted(Activation::External { steal_margin_ms }, lease_ms),
                storage: None,
                anchor: anchor.map(|anchor| (anchor, lease_ms, steal_margin_ms)),
            },
        }
    }
}
