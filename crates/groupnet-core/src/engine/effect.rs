//! What the engine asks the driver to do.

use crate::{NodeId, Time};

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
    /// Ask the driver to deliver a [`GroupEngine::on_tick`](crate::GroupEngine::on_tick) no later than `at`.
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
    /// The group's epoch-fenced **host** changed — a new host activated, the
    /// incumbent was deposed by a higher epoch, or the lease lapsed and left
    /// the group hostless.
    ///
    /// Emitted only by a [`GroupMode::Hosted`](crate::GroupMode) group; an
    /// `Eventual` group never emits one. Distinct from
    /// [`Effect::CoordinatorChanged`]: the coordinator is *derived* and never
    /// authoritative, while the host is elected and epoch-fenced. Both exist
    /// side by side in a Hosted group.
    LeadershipChanged {
        /// The epoch this observation belongs to. Monotone per group: an
        /// observer's epoch never regresses, so a reader can discard a
        /// stale-arriving notification by comparing it.
        epoch: u64,
        /// The host of that epoch, or `None` if the group currently has none.
        host: Option<NodeId>,
    },
    /// **Write-ahead:** persist this voter's grant of `epoch` to `claimant`
    /// before any frame that grant licenses leaves the node.
    ///
    /// Emitted only by a group whose activation is
    /// [`Activation::Quorum`](crate::Activation::Quorum), and only by a node in
    /// its voter roster. The order within the batch is the contract: **a driver
    /// providing voter durability MUST complete the persist before executing
    /// the rest of the batch, and MUST withhold the frames this grant licenses
    /// if it fails.** A grant that reached the wire but not the store is exactly
    /// the double-grant a crash-restart can turn into two hosts for one epoch.
    ///
    /// # What follows it — decode, do not count positions
    ///
    /// There are four emission shapes, and only two of them put a frame this
    /// grant licenses into the batch at all. A driver must therefore identify
    /// what to withhold by **decoding** it, never by taking "the next `Send`":
    ///
    /// * **A peer's claim answered** (row Q1) — followed by the
    ///   [`Send`](Effect::Send) carrying the matching
    ///   [`LeadGrant`](crate::wire::Kind::LeadGrant) frame. Withholding it means
    ///   the claimant never counts this voter toward its majority.
    /// * **This node's own claim opened** (row Q4) — a claimant's self-grant is
    ///   counted straight into its round rather than sent, so what follows is
    ///   the [`LeadClaim`](crate::wire::Kind::LeadClaim) broadcast itself.
    ///   Withholding it means nobody answers a round whose first grant is not
    ///   durable.
    /// * **A round this very grant closes** — a roster of one, whose majority
    ///   the self-grant alone satisfies, and a row Q4b retry that completes the
    ///   majority. What follows is the activation's
    ///   [`LeadState`](crate::wire::Kind::LeadState) broadcast, which is *not* a
    ///   frame the grant licenses and is not withheld: the node is already host
    ///   by the time the effect batch is executed.
    /// * **A row Q4b retry that does not close the round** — nothing follows at
    ///   all. The claim went out when the round was opened, before there was
    ///   anything to write down.
    ///
    /// The third shape is the one a failed persist cannot close — the round is
    /// already won by the time the batch is executed — and the fourth is why
    /// "swallow the next `Send`" is not even well defined. See the runtime
    /// driver's guard for how far that residue reaches.
    ///
    /// Emitted only for a **new** `(epoch, claimant)` pair. An idempotent
    /// re-grant — the same pair asked for again, which is how a host renews —
    /// re-sends the frame without re-persisting, because there is nothing new
    /// to write down.
    ///
    /// A driver with no storage ignores it. That is a supported posture, not a
    /// bug: the engine's **boot blackout** (a freshly started voter refuses to
    /// grant for one `lease_ms`) converts the durability requirement into a
    /// timing one — see
    /// [`GroupEngine::with_recovered`](crate::GroupEngine::with_recovered) for
    /// the durable posture and what recovery does and does not buy.
    PersistGrant {
        /// The epoch granted.
        epoch: u64,
        /// The claimant this voter granted that epoch to.
        claimant: NodeId,
    },
    /// A metadata key took a new value (from a local write or a merged delta).
    MetadataChanged {
        /// The key that changed.
        key: String,
        /// Its new value.
        value: String,
    },
}
