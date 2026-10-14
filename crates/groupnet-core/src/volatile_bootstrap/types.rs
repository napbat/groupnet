//! Bounded, non-authoritative claim decisions for peer index bootstrap.

use std::fmt;
use std::num::NonZeroUsize;

use super::transfer::{TransferConfigError, TransferEffect, TransferError, TransferEvent};
use crate::{NodeId, Status, Time};

/// Stable application scope for one index image and origin builder.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BootstrapScope {
    /// Source/application domain, such as an origin endpoint identity.
    pub domain: String,
    /// Partition within that domain, such as an S3 bucket.
    pub partition: String,
}

impl BootstrapScope {
    /// Collision-free rendezvous key: the byte length delimits the domain.
    #[must_use]
    pub fn placement_key(&self) -> String {
        format!("{}:{}{}", self.domain.len(), self.domain, self.partition)
    }
}

/// Fixed resource and logical-time limits for one scope.
///
/// Roster and byte caps are nonzero by type; [`BootstrapConfig::validate`]
/// checks the millisecond timers and their relations to `total_ms`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BootstrapConfig {
    /// Maximum complete member/claim roster entries; below `usize::MAX`.
    pub max_members: NonZeroUsize,
    /// Maximum UTF-8 bytes in one member ID.
    pub max_member_bytes: NonZeroUsize,
    /// Maximum combined UTF-8 bytes in the scope names.
    pub max_scope_bytes: NonZeroUsize,
    /// Time allowed for claims to converge before selecting a builder;
    /// nonzero and below `total_ms`.
    pub settle_ms: u64,
    /// Local cadence for publishing a fresh, monotone renewal sequence; nonzero.
    pub renew_ms: u64,
    /// Native TTL for an advertised claim; above `renew_ms`.
    pub claim_ttl_ms: u64,
    /// Finite observation attempt timeout; nonzero and at most `total_ms`.
    pub observe_ms: u64,
    /// Stall bound for a builder: how long its local origin build may run,
    /// and a follower keeps waiting for it, without the build advancing.
    /// Nonzero and at most `total_ms`.
    pub donor_wait_ms: u64,
    /// Liveness budget for this bootstrap selection episode, restarted each
    /// time the selected build advances; nonzero.
    pub total_ms: u64,
}

/// Invalid [`BootstrapConfig`], naming the violated rule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BootstrapConfigError {
    /// [`BootstrapConfig::max_members`] is `usize::MAX`, leaving no room for
    /// checked roster arithmetic.
    UnboundedMaxMembers,
    /// [`BootstrapConfig::settle_ms`] is zero.
    ZeroSettle,
    /// [`BootstrapConfig::renew_ms`] is zero.
    ZeroRenew,
    /// [`BootstrapConfig::claim_ttl_ms`] is not above [`BootstrapConfig::renew_ms`].
    ClaimTtlNotAboveRenew,
    /// [`BootstrapConfig::observe_ms`] is zero.
    ZeroObserve,
    /// [`BootstrapConfig::donor_wait_ms`] is zero.
    ZeroDonorWait,
    /// [`BootstrapConfig::total_ms`] is zero.
    ZeroTotal,
    /// [`BootstrapConfig::settle_ms`] is not below [`BootstrapConfig::total_ms`].
    SettleNotBelowTotal,
    /// [`BootstrapConfig::observe_ms`] exceeds [`BootstrapConfig::total_ms`].
    ObserveExceedsTotal,
    /// [`BootstrapConfig::donor_wait_ms`] exceeds [`BootstrapConfig::total_ms`].
    DonorWaitExceedsTotal,
}

impl fmt::Display for BootstrapConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::UnboundedMaxMembers => "bootstrap max_members must be below usize::MAX",
            Self::ZeroSettle => "bootstrap settle_ms must be nonzero",
            Self::ZeroRenew => "bootstrap renew_ms must be nonzero",
            Self::ClaimTtlNotAboveRenew => "bootstrap claim_ttl_ms must exceed renew_ms",
            Self::ZeroObserve => "bootstrap observe_ms must be nonzero",
            Self::ZeroDonorWait => "bootstrap donor_wait_ms must be nonzero",
            Self::ZeroTotal => "bootstrap total_ms must be nonzero",
            Self::SettleNotBelowTotal => "bootstrap settle_ms must be below total_ms",
            Self::ObserveExceedsTotal => "bootstrap observe_ms must not exceed total_ms",
            Self::DonorWaitExceedsTotal => "bootstrap donor_wait_ms must not exceed total_ms",
        })
    }
}

impl std::error::Error for BootstrapConfigError {}

impl BootstrapConfig {
    /// Rejects absent, inverted, or unbounded limits.
    ///
    /// # Errors
    /// Returns the [`BootstrapConfigError`] variant naming the first violated rule.
    pub fn validate(&self) -> Result<(), BootstrapConfigError> {
        if self.max_members.get() == usize::MAX {
            return Err(BootstrapConfigError::UnboundedMaxMembers);
        }
        if self.settle_ms == 0 {
            return Err(BootstrapConfigError::ZeroSettle);
        }
        if self.renew_ms == 0 {
            return Err(BootstrapConfigError::ZeroRenew);
        }
        if self.claim_ttl_ms <= self.renew_ms {
            return Err(BootstrapConfigError::ClaimTtlNotAboveRenew);
        }
        if self.observe_ms == 0 {
            return Err(BootstrapConfigError::ZeroObserve);
        }
        if self.donor_wait_ms == 0 {
            return Err(BootstrapConfigError::ZeroDonorWait);
        }
        if self.total_ms == 0 {
            return Err(BootstrapConfigError::ZeroTotal);
        }
        if self.settle_ms >= self.total_ms {
            return Err(BootstrapConfigError::SettleNotBelowTotal);
        }
        if self.observe_ms > self.total_ms {
            return Err(BootstrapConfigError::ObserveExceedsTotal);
        }
        if self.donor_wait_ms > self.total_ms {
            return Err(BootstrapConfigError::DonorWaitExceedsTotal);
        }
        Ok(())
    }
}

/// Caller-supplied boot nonce, fresh across process restarts for one node.
///
/// The core does not generate entropy. A runtime must obtain 128 bits from an
/// operating-system CSPRNG or a documented operator-backed monotonic identity.
/// Tests may inject fixed nonzero values.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct BootId(pub u128);

/// One session/attempt identity; a reboot must supply a fresh incarnation.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ClaimIdentity {
    /// Member identity used by Groupnet placement.
    pub node: NodeId,
    /// Caller-supplied, nonzero boot identity, never generated by this core.
    pub incarnation: BootId,
    /// Fresh nonzero session identity within this process/boot.
    pub session: u64,
    /// Monotone local attempt within that boot.
    pub attempt: u64,
}

/// Long-lived opted-in participation identity, independent of one builder
/// attempt. It remains visible after a follower completes its transfer.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct PresenceIdentity {
    /// Native Groupnet member name.
    pub node: NodeId,
    /// Caller-supplied process-fresh boot token.
    pub boot: BootId,
    /// Fresh worker session under that boot.
    pub session: u64,
}

/// One native TTL-backed participation renewal, not donor or read authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BootstrapPresence {
    /// Exact long-lived participant.
    pub identity: PresenceIdentity,
    /// Strictly increasing on each publication of this identity.
    pub renewal: u64,
    /// Observer-local native TTL remaining at the source sample. It is never
    /// encoded into the entry body or treated as a wall-clock timestamp.
    pub remaining_ms: u64,
}

/// Exact native member from one complete actor cut retained by a donor image.
/// A noneligible member may have no participation entry, but its status and
/// native incarnation still belong to the roster. The participation boot
/// separately fences processes reusing the same `NodeId`. Two cuts bind the
/// same membership by [`same_membership`], not by equality: status and
/// incarnation are the observer's SWIM opinion of a member.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BootstrapMemberIdentity {
    /// Native member node, including a nonparticipant or noneligible member.
    pub node: NodeId,
    /// Fresh native TTL participation identity, when source-present.
    pub presence: Option<PresenceIdentity>,
    /// Native Groupnet membership refutation incarnation.
    pub member_incarnation: u64,
    /// Exact native status at this complete actor cut.
    pub status: Status,
}

impl BootstrapMemberIdentity {
    /// Whether this member may currently donate or receive a peer image.
    #[must_use]
    pub fn eligible(&self) -> bool {
        self.status == Status::Alive
    }

    /// Whether one exact eligible member still names this builder worker.
    #[must_use]
    pub fn matches_claim(&self, claim: &ClaimIdentity) -> bool {
        self.eligible()
            && self.node == claim.node
            && self.presence.as_ref().is_some_and(|presence| {
                presence.boot == claim.incarnation && presence.session == claim.session
            })
    }

    /// Validate one bounded native roster element before retaining or wiring.
    #[must_use]
    pub fn valid_bounded(&self, max_node_bytes: usize) -> bool {
        !self.node.as_str().is_empty()
            && self.node.as_str().len() <= max_node_bytes
            && (!self.eligible() || self.presence.is_some())
            && self.presence.as_ref().is_none_or(|presence| {
                presence.node == self.node && presence.boot.0 != 0 && presence.session != 0
            })
    }

    /// Whether `other` is this same member process: the same node with the
    /// same participation presence (boot and session), or with none in both.
    /// Native status and membership incarnation are not compared. They are
    /// the observing node's SWIM opinion of the member, which a suspicion and
    /// its refutation change without changing the process, its presence or
    /// the writes it can make; a restart, a lapsed presence, a join and a
    /// leave all change the presence or the node set instead.
    #[must_use]
    pub fn same_member(&self, other: &Self) -> bool {
        self.node == other.node && self.presence == other.presence
    }
}

/// Whether two sorted complete rosters bind the same membership: the same
/// members in order, each [`BootstrapMemberIdentity::same_member`]. This is
/// what a donor image's roster binds, from the cut at C through Ready, each
/// barrier and a follower's post-handoff check; a cut that differs only in
/// member status or incarnation is not a roster change.
#[must_use]
pub fn same_membership(a: &[BootstrapMemberIdentity], b: &[BootstrapMemberIdentity]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(a, b)| a.same_member(b))
}

/// One live, source-observed participation entry at a complete native cut.
/// Its remaining TTL is measured at that cut, never encoded as a timestamp.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BootstrapParticipant {
    /// Exact process/worker identity and native member incarnation.
    pub member: BootstrapMemberIdentity,
    /// Strictly increasing renewal for this exact participation identity.
    pub renewal: u64,
    /// Source-observed native TTL remaining at the same roster cut.
    pub remaining_ms: u64,
}

/// Claim state is a donor availability hint, never a read-authority proof.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ClaimPhase {
    /// Eligible for deterministic builder selection.
    Willing,
    /// Origin build selected and in progress.
    Building,
    /// Builder reports an image candidate available for guarded transfer.
    Ready,
}

/// One native TTL entry observed through a bounded source snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BootstrapClaim {
    /// Exact node, boot, session, and attempt.
    pub identity: ClaimIdentity,
    /// Strictly increasing on each renewal of this exact claim.
    pub renewal: u64,
    /// Candidate's advertised phase.
    pub phase: ClaimPhase,
    /// Builder progress, monotone within this exact claim. A Building claim
    /// advances it as its origin build commits work; followers treat an
    /// unchanged value as a stalled build once `donor_wait_ms` passes.
    pub progress: u64,
    /// Native source TTL remaining at the observation, not a fresh local TTL.
    pub remaining_ms: u64,
}

/// One complete-roster member eligibility fact.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BootstrapMember {
    /// Native Groupnet node identity.
    pub node: NodeId,
    /// Whether this member is currently eligible to build or donate.
    pub eligible: bool,
}

/// Correlation for one core operation, from a single checked allocator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BootstrapOperation {
    /// Local session identity.
    pub session: u64,
    /// Caller-supplied fresh boot identity; protects token reuse after restart.
    pub incarnation: BootId,
    /// Supersession generation.
    pub generation: u64,
    /// Unique token within the session.
    pub token: u64,
}

/// Non-authoritative selection phase.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BootstrapStage {
    /// No claim has been published.
    Unready,
    /// Publishing and allowing a connected claim set to converge.
    Settling,
    /// Reading a bounded current roster and claim set.
    Observing,
    /// This node is the provisional origin builder.
    Building,
    /// Following a selected builder or ready donor.
    Following,
    /// A candidate image may be requested, but no read permission is granted.
    DonorAvailable,
    /// A bounded donor transfer is active under the original selected claim.
    Transferring,
    /// Private candidate installed; external read and lease gates still apply.
    Transferred,
    /// Selection budget ended; the ordinary origin fallback remains available.
    Fallback,
    /// Candidate retired while this process continues scoped presence renewal.
    Participating,
    /// Terminal local cancellation.
    Cancelled,
}

/// Fail-closed input or state rejection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BootstrapError {
    /// The [`BootstrapConfig`] is invalid.
    Config(BootstrapConfigError),
    /// The transfer config passed to `ClaimEngine::enable_transfer` is invalid.
    TransferConfig(TransferConfigError),
    /// Scope, node identity, boot incarnation, or session is empty, zero, or
    /// exceeds its configured byte cap.
    InvalidIdentity,
    /// Input belongs to the wrong phase.
    Stage,
    /// Callback does not match the current operation or selected claim.
    StaleOperation,
    /// Source supplied incomplete, over-limit, or contradictory facts.
    InvalidObservation,
    /// Time moved backward.
    BackwardTime,
    /// Checked generation, token, renewal, or logical-time arithmetic ended.
    Exhausted,
}

impl fmt::Display for BootstrapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Config(_) => "invalid bootstrap config",
            Self::TransferConfig(_) => "invalid bootstrap transfer config",
            Self::InvalidIdentity => "invalid bootstrap scope or identity",
            Self::Stage => "bootstrap input belongs to the wrong phase",
            Self::StaleOperation => "bootstrap callback does not match the current operation",
            Self::InvalidObservation => "bootstrap source facts are incomplete or contradictory",
            Self::BackwardTime => "bootstrap time moved backward",
            Self::Exhausted => "bootstrap checked arithmetic exhausted",
        })
    }
}

impl std::error::Error for BootstrapError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Config(error) => Some(error),
            Self::TransferConfig(error) => Some(error),
            _ => None,
        }
    }
}

/// Explicit observation, completion, failure, and clock inputs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BootstrapEvent {
    /// Retire one recovery candidate without withdrawing process presence.
    RetireCandidate,
    /// The retired candidate's installed peer image now backs a Ready local
    /// recovery: hold it as a completed local image, a donor like an origin
    /// build's, whose Ready recapture is pending at once under a fresh
    /// Building claim.
    AdoptInstalled,
    /// Publish a fresh local claim and start one finite selection episode.
    Start,
    /// One source-certified complete bounded member and claim snapshot.
    ClaimsObserved {
        /// Exact observation operation.
        op: BootstrapOperation,
        /// All members in scope, bounded before adapter allocation.
        members: Vec<BootstrapMember>,
        /// All source TTL claims in scope, bounded before adapter allocation.
        claims: Vec<BootstrapClaim>,
    },
    /// Complete native member, participation, and transient claim cut.
    /// Required for an opted-in peer path. A cut that is incomplete, such as
    /// an eligible member without presence, is refused; the runtime then
    /// reports [`Self::ObservationFailed`] for the same operation.
    ParticipantsObserved {
        /// Exact observation operation.
        op: BootstrapOperation,
        /// Complete native membership statuses.
        members: Vec<BootstrapMember>,
        /// Exact same-cut identity of every native member, including those
        /// with no compatible participation entry.
        roster: Vec<BootstrapMemberIdentity>,
        /// Live scoped presence entries, including members without claims.
        participants: Vec<BootstrapParticipant>,
        /// Optional donor/builder claims at the same source cut.
        claims: Vec<BootstrapClaim>,
    },
    /// The observation for `op` produced no usable complete cut: the source
    /// read failed or timed out, or the cut was refused. A follower still
    /// inside its grace for the selected builder samples again; any other
    /// selection ends in the ordinary origin fallback.
    ObservationFailed {
        /// Exact observation operation.
        op: BootstrapOperation,
    },
    /// Local guarded origin build completed; image still needs transfer proof.
    Built {
        /// Exact build operation.
        op: BootstrapOperation,
        /// Exact selected local builder identity.
        selected: ClaimIdentity,
    },
    /// The local guarded origin build committed more work. Renews the build's
    /// stall bound and advertises the advance in a fresh claim renewal.
    BuildProgressed {
        /// Exact build operation.
        op: BootstrapOperation,
        /// Exact selected local builder identity.
        selected: ClaimIdentity,
    },
    /// A guarded local origin build completed, but this episode cannot
    /// advertise a donor candidate yet; ordinary local recovery may still
    /// affirm independently. With participation required a Ready recapture
    /// follows, so the builder's claim stays renewed until that recapture's
    /// claim supersedes it, for at most `donor_wait_ms` from now, however
    /// many attempts fail meanwhile; otherwise it is withdrawn.
    LocalOnlyBuilt {
        /// Exact build operation.
        op: BootstrapOperation,
        /// Exact selected local builder identity.
        selected: ClaimIdentity,
    },
    /// Local origin is serving, so attempt one fresh bounded donor recapture
    /// under the complete participation cut just verified. This is separate
    /// from the completed origin build and cannot rescan it. A cut naming
    /// only the participants a failed attempt was taken under starts nothing
    /// until its backoff has passed
    /// ([`ClaimEngine::ready_recapture_backing_off`](crate::volatile_bootstrap::ClaimEngine::ready_recapture_backing_off));
    /// one naming a new participant, a joiner or a restarted peer, starts at
    /// once. Once the claim window has closed, a cut binding the same
    /// membership as the one the last attempt failed under starts nothing.
    StartReadyRecapture,
    /// Peer transfer cannot represent the required roster format. Retire the
    /// transient local claim while leaving participation renewal active and
    /// allowing the outer recovery to use origin fallback.
    PeerTransferDeclined {
        /// Exact selected donor observation operation.
        op: BootstrapOperation,
        /// Exact selected peer that cannot be transferred in this mode.
        selected: ClaimIdentity,
    },
    /// Local guarded origin build failed or lost its recovery permit. For a
    /// Ready recapture this only makes the recapture pending again, to be
    /// retried after a backoff; it is no origin fallback.
    BuildFailed {
        /// Exact build operation.
        op: BootstrapOperation,
        /// Exact selected local builder identity.
        selected: ClaimIdentity,
    },
    /// Exact selected donor became unavailable, with callback correlation.
    DonorUnavailable {
        /// Exact follow operation.
        op: BootstrapOperation,
        /// Selected node, boot, and attempt the callback observed.
        selected: ClaimIdentity,
    },
    /// Fresh native TTL observation of the exact selected claim during transfer.
    SelectedClaimObserved {
        /// Exact refresh operation from the same session allocator.
        op: BootstrapOperation,
        /// Absent means the donor claim is no longer source-visible.
        claim: Option<BootstrapClaim>,
    },
    /// The refresh read of the selected donor's claim failed or timed out.
    /// Not evidence that the donor left: the transfer continues while the
    /// claim it last observed is unexpired, and samples again.
    SelectedClaimUnobserved {
        /// Exact refresh operation whose read failed.
        op: BootstrapOperation,
    },
    /// Begin the opt-in transfer for this exact selected ready donor.
    StartTransfer {
        /// Original follow operation retained as the transfer parent.
        op: BootstrapOperation,
        /// Exact selected donor identity.
        selected: ClaimIdentity,
    },
    /// One exact event for the bounded transfer child.
    Transfer(Box<TransferEvent>),
    /// Monotone caller-supplied logical time.
    Tick(Time),
    /// Source could not confirm or renew this worker's exact participation.
    /// Peer bootstrap ends; ordinary local recovery/serving gates are separate.
    PresenceFailed {
        /// Exact worker participation identity.
        identity: PresenceIdentity,
        /// Failed renewal sequence; older responses cannot end a newer one.
        renewal: u64,
    },
    /// Exact local donor capture retired while ordinary local recovery may
    /// remain healthy. With participation required a recapture follows under
    /// the next verified cut, and a renewed Building claim keeps joiners
    /// waiting for it; otherwise donor availability ends, retaining
    /// participation.
    CaptureRetired {
        /// Local builder claim that owned the retired capture.
        selected: ClaimIdentity,
    },
    /// A Ready claim publication failed; stop donating without restarting a
    /// healthy local recovery or withdrawing long-lived participation.
    DonorPublicationFailed {
        /// Exact local Ready claim whose publication failed.
        selected: ClaimIdentity,
    },
    /// Withdraw the local claim and end this session.
    Cancel,
}

/// Bounded work for an external source/runtime adapter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BootstrapEffect {
    /// Cancel/fence exactly this old work before any replacement image work.
    CancelWork {
        /// Correlated work being superseded.
        op: BootstrapOperation,
    },
    /// Publish/renew the exact claim with native TTL and new renewal sequence.
    PublishClaim(BootstrapClaim),
    /// Publish/renew long-lived scoped participation, including after follower
    /// transfer and after a local origin-builder episode reaches Ready.
    PublishPresence(BootstrapPresence),
    /// Best-effort withdrawal; stale entries must still expire by native TTL.
    WithdrawClaim(ClaimIdentity),
    /// Best-effort exact participation withdrawal; native TTL bounds loss.
    WithdrawPresence(PresenceIdentity),
    /// Read a complete roster and claims through a pre-allocation bounded API.
    ObserveClaims {
        /// Exact observation operation.
        op: BootstrapOperation,
        /// Pre-allocation cap for the complete member and claim roster.
        max_members: usize,
        /// Pre-allocation cap for each member identity.
        max_member_bytes: usize,
    },
    /// Begin the existing guarded origin recovery as provisional builder.
    BuildOrigin {
        /// Exact build operation.
        op: BootstrapOperation,
        /// Exact provisional builder identity.
        selected: ClaimIdentity,
    },
    /// Capture the already-Ready local index under a fresh finite operation;
    /// this must never issue another origin scan.
    RecaptureCurrent {
        /// Fresh operation from the same token allocator.
        op: BootstrapOperation,
        /// New donor claim attempt, superseding the previous claim.
        selected: ClaimIdentity,
    },
    /// Await a selected builder's candidate without opening local reads.
    FollowBuilder {
        /// Exact follow operation.
        op: BootstrapOperation,
        /// Exact candidate identity.
        selected: ClaimIdentity,
    },
    /// The followed builder's claim advertised new build progress, or the
    /// transfer of its image advanced. The runtime renews the parent
    /// recovery's liveness budget; this is no read authority.
    BuilderProgressed,
    /// This node stopped waiting for the builder or donor it had selected.
    /// Informational for the runtime's operator log: the core has already
    /// acted on it, by selecting again or ending in the origin fallback.
    Released {
        /// The builder or donor this node was waiting for.
        builder: ClaimIdentity,
        /// Why the wait ended.
        reason: ReleaseReason,
    },
    /// A ready donor can be tried for a separate bounded transfer session.
    DonorAvailable {
        /// Exact follow operation.
        op: BootstrapOperation,
        /// Exact donor identity.
        selected: ClaimIdentity,
    },
    /// Refresh exact selected donor TTL without replacing the transfer parent.
    ObserveSelectedClaim {
        /// Exact refresh operation, separate from the parent and child work.
        op: BootstrapOperation,
        /// Exact donor identity to read from native TTL storage.
        selected: ClaimIdentity,
    },
    /// Work owned by the transfer child and the same session token allocator.
    Transfer(Box<TransferEffect>),
    /// Use the ordinary guarded origin recovery path after selection fails.
    FallbackOrigin,
    /// Earliest logical deadline across claim renewal and current work.
    ArmTimer(Time),
}

/// Why a follower stopped waiting for its selected builder or donor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReleaseReason {
    /// The followed build advertised no progress for the follower's whole
    /// grace: the builder's own stall bound, one claim TTL and one sample.
    Stalled,
    /// The builder withdrew its claim while it was still a live member: its
    /// build, or its pending Ready recapture, ended without an image.
    Withdrawn,
    /// The selected Ready donor could not serve its image.
    DonorUnavailable,
    /// No complete participation cut confirmed the Ready donor before the
    /// transfer.
    Unverified,
    /// The selection episode ended: its budget ran out, no usable cut arrived
    /// outside the grace, or a bound's arithmetic was exhausted.
    Ended,
    /// The peer transfer from the selected Ready donor aborted, for the
    /// transfer's own reason, such as a stall past its bound or a failed
    /// operation.
    TransferAborted(TransferError),
}

/// One deterministic transition result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BootstrapStep {
    /// Bounded effects for the runtime to execute.
    pub effects: Vec<BootstrapEffect>,
    /// Rejection, when the event cannot be accepted.
    pub rejection: Option<BootstrapError>,
}
