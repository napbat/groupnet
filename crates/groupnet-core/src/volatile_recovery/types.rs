//! Bounded facts and effects for recovery from a volatile coherence feed.

use crate::volatile_bootstrap::BootstrapMemberIdentity;
use crate::volatile_bootstrap::transfer::NativeHandoffReceipt;
use crate::{NodeId, Time};

/// Which existing reader-side coherence policy a consumer uses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryMode {
    /// A self-expiring serve lease permits the cheaper lapse proof.
    Leased,
    /// A gap needs origin reconciliation; there is no lease-lapse proof.
    Unleased,
}

/// Finite recovery, roster, and operation limits supplied by the consumer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoveryConfig {
    /// Maximum peer identities in any observation or retained recovery roster.
    pub max_members: usize,
    /// Maximum UTF-8 bytes in one peer identity.
    pub max_member_bytes: usize,
    /// Maximum source-head resamples after a reached frontier moves.
    pub max_barrier_rounds: u32,
    /// Liveness budget for one recovery episode: time allowed since the
    /// episode began or since its long-running rebuild or baseline operation
    /// last reported progress. A progressing operation is never failed over.
    pub total_ms: u64,
    /// Duration one adapter operation may run without completing or, for an
    /// origin rebuild, without reporting progress. Bounded by `total_ms`.
    pub attempt_ms: u64,
    /// Delay after all per-granter renewals advance.
    pub settle_ms: u64,
    /// Delay between unsuccessful grant or affirmation observations.
    pub poll_ms: u64,
}

impl RecoveryConfig {
    /// Validates every finite limit before any effect is emitted.
    ///
    /// # Errors
    /// Returns [`RecoveryError::InvalidConfig`] for an absent bound or a
    /// duration longer than the total recovery budget.
    pub fn validate(self) -> Result<Self, RecoveryError> {
        if self.max_members == 0
            || self.max_member_bytes == 0
            || self.max_barrier_rounds == 0
            || self.total_ms == 0
            || self.attempt_ms == 0
            || self.settle_ms == 0
            || self.poll_ms == 0
            || self.attempt_ms > self.total_ms
            || self.settle_ms > self.total_ms
            || self.poll_ms > self.total_ms
            || self
                .max_members
                .checked_mul(self.max_member_bytes)
                .is_none()
        {
            return Err(RecoveryError::InvalidConfig);
        }
        Ok(self)
    }
}

/// Optional spacing between exhausted full recovery episodes.
/// Individual operations inside an episode still retry on `RecoveryConfig::poll_ms`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoveryRearm {
    /// Delay after the first exhausted episode.
    pub initial_ms: u64,
    /// Inclusive cap on later doubled delays.
    pub max_ms: u64,
}

impl RecoveryRearm {
    /// Rejects absent or inverted delay bounds before a recovery event starts.
    ///
    /// # Errors
    /// Returns [`RecoveryError::InvalidConfig`] for an invalid policy.
    pub fn validate(self) -> Result<Self, RecoveryError> {
        if self.initial_ms == 0 || self.max_ms < self.initial_ms {
            return Err(RecoveryError::InvalidConfig);
        }
        Ok(self)
    }
}

/// Epoch-major renewal or write-feed position native to the existing fabric.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Mark {
    /// Writer or reader incarnation; a restart must change it.
    pub epoch: u64,
    /// Sequence within the incarnation.
    pub sequence: u64,
}

/// One bounded membership, lease, and advertised-head observation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Peer {
    /// Stable Groupnet node identity.
    pub node: NodeId,
    /// This node is presently considered alive by the adapter. A non-live but
    /// still present member is not vanished and must still contribute its head.
    pub alive: bool,
    /// This node still advertises the serve-lease capability.
    pub grants_lease: bool,
    /// Adapter policy says this node was already non-live before the lapse.
    /// Only the initial lapse observation may use this exemption.
    pub old_nonlive: bool,
    /// Renewal of this reader that the granter has adopted, if any.
    pub grant: Option<Mark>,
    /// Advertised volatile feed head. `None` means no write has been
    /// advertised in this observed feed; an unreadable observation must fail
    /// the whole adapter operation, not be converted to `None`. This does not
    /// prove that no origin mutation committed before publication.
    pub head: Option<Mark>,
}

/// Exact operation correlation; no token is reused within an engine session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoveryOperation {
    /// Unique local engine incarnation; reconstruction must use a new value.
    pub session: u64,
    /// Recovery generation, changed on every gap, lapse, or restart.
    pub generation: u64,
    /// Monotonic engine operation token.
    pub token: u64,
}

/// Externally visible recovery stage; local serving also requires the lease
/// and application policy gates.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryStage {
    /// No verified index/recovery authority has been installed.
    Unready,
    /// Local serving closed, application invalidation pending.
    Invalidating,
    /// A bounded origin-index rebuild is pending.
    Rebuilding,
    /// An opt-in claim/transfer child is acquiring a guarded baseline.
    AcquiringBaseline,
    /// Recording the initial member/granter set for a lease-lapse proof.
    SamplingInitial,
    /// Waiting for each relevant granter to adopt a later renewal.
    WaitingRenewals,
    /// Waiting for gossip propagation after the renewals.
    Settling,
    /// Checking vanished peers and collecting advertised heads.
    SamplingHeads,
    /// Waiting until the application frontier reaches every sampled head.
    WaitingFrontiers,
    /// Confirming source heads and vanished peers after the frontier barrier.
    RecheckingHeads,
    /// Checking the complete peer/head roster after atomic native handoff.
    PeerSamplingHeads,
    /// Waiting for sampled native heads through the normal feed applier.
    PeerWaitingFrontiers,
    /// Rechecking peer/head continuity after the peer-specific barrier.
    PeerRecheckingHeads,
    /// A generation-bound lease affirmation is pending.
    Affirming,
    /// Recovery proof completed; read permission still intersects lease and app gates.
    Ready,
    /// Recovery could not be established in the configured budget.
    OriginOnly,
    /// Explicit stop; old responses cannot reopen serving.
    Cancelled,
}

/// Read-only state for the shell's synchronously published gate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoveryState {
    /// Current generation.
    pub generation: u64,
    /// Current stage.
    pub stage: RecoveryStage,
    /// Only the recovery facet's permission; lease and app checks remain separate.
    pub recovered: bool,
    /// Highest lapse counter covered by an invalidating recovery generation.
    pub covered_lapses: u64,
}

/// A recovery event accepted by the sans-IO engine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecoveryEvent {
    /// Explicitly start or reopen a cold origin-index build with serving closed.
    /// `Cancel` is terminal even for an explicit restart.
    Start,
    /// Explicit restart coalesced with a newly observed lapse or feed gap.
    /// The full rebuild covers at least this lapse counter and begins now,
    /// even when an automatic rearm cooldown is pending.
    StartWithLapses {
        /// Latest local serve-lease lapse counter to cover.
        lapses: u64,
    },
    /// A volatile feed gap requires immediate invalidation and origin rebuild.
    FeedGap {
        /// Latest local serve-lease lapse counter covered by this invalidation.
        lapses: u64,
    },
    /// A previously uncovered serve-lease lapse may take the cheaper proof.
    LeaseLapse {
        /// Monotone local lapse counter observed by the lease adapter.
        count: u64,
    },
    /// Application confirms prior index/body state is unservable.
    Invalidated {
        /// Exact invalidation operation.
        op: RecoveryOperation,
    },
    /// Application installed origin-reconciled index state under this generation.
    Materialized {
        /// Exact fenced origin rebuild operation.
        op: RecoveryOperation,
    },
    /// The opt-in claim/transfer child declined or lost its donor; rebuild
    /// from origin within the same original recovery episode.
    BootstrapDeclined {
        /// Exact baseline acquisition operation.
        op: RecoveryOperation,
    },
    /// The selected local provisional builder completed a guarded origin
    /// image. The claim may remain available for bounded donor service;
    /// serving still requires independent lease/domain affirmation.
    LocalBaselineBuilt {
        /// Exact baseline acquisition operation.
        op: RecoveryOperation,
    },
    /// The current origin rebuild or baseline acquisition committed work.
    /// Renews that operation's stall bound and the episode budget from now;
    /// any other stage or a stale operation is refused.
    Progressed {
        /// Exact rebuild or baseline acquisition operation.
        op: RecoveryOperation,
    },
    /// A source-correlated private candidate was installed with continuous
    /// normal native delivery, but still has no local serving permission.
    PeerBaselineInstalled {
        /// Exact baseline acquisition operation.
        op: RecoveryOperation,
        /// Exact transfer handoff accepted before donor release.
        handoff: Box<NativeHandoffReceipt>,
    },
    /// Bounded membership and per-granter renewal observation.
    PeersObserved {
        /// Exact observation operation.
        op: RecoveryOperation,
        /// Includes all currently known peer statuses, not only alive nodes.
        peers: Vec<Peer>,
        /// Roster-wide lease confirmation, used only alongside per-granter checks.
        confirmed: Option<Mark>,
    },
    /// Complete exact-incarnation roster and heads after peer handoff.
    PeerHeadsObserved {
        /// Exact peer-specific observation operation.
        op: RecoveryOperation,
        /// Bounded source-backed peer and head facts, excluding the local node.
        peers: Vec<Peer>,
        /// Complete current roster including incarnation and session identity.
        identities: Vec<BootstrapMemberIdentity>,
    },
    /// All sampled per-writer heads were applied to the local index.
    FrontiersReached {
        /// Exact sampled-head barrier operation.
        op: RecoveryOperation,
    },
    /// Lease adapter accepts or declines affirmation for the current generation.
    Affirmed {
        /// Exact affirmation operation.
        op: RecoveryOperation,
        /// False means renewal is not yet confirmed, not a durable failure.
        accepted: bool,
    },
    /// Adapter operation failed or timed out; the core chooses fallback.
    Failed {
        /// Exact operation that could not complete.
        op: RecoveryOperation,
    },
    /// Monotonic caller-supplied logical time.
    Tick(Time),
    /// Cancel recovery and fence all queued work.
    Cancel,
}

/// Work requested from a bounded consumer driver.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecoveryEffect {
    /// Synchronously publish a closed read gate and index-publication permit.
    CloseGate {
        /// Generation whose read and publication permissions are revoked.
        generation: u64,
    },
    /// Make retained hot/warm bodies prove themselves against a newer index.
    Invalidate {
        /// Exact invalidation operation.
        op: RecoveryOperation,
        /// A gap or fallback moves body trust generation; a cheap lapse does not.
        distrust_bodies: bool,
    },
    /// Build origin index state privately and publish only with generation fence.
    RebuildOrigin {
        /// Exact private origin rebuild operation.
        op: RecoveryOperation,
    },
    /// Run opt-in claim/takeover/transfer under this original recovery turn.
    AcquireBaseline {
        /// Exact bounded baseline acquisition operation.
        op: RecoveryOperation,
    },
    /// Cancel an exact provisional claim/transfer child before later work.
    /// An installed normal feed applier is independent of this child.
    CancelBaseline {
        /// Original baseline acquisition operation.
        op: RecoveryOperation,
    },
    /// Withdraw a local donor's old capture and claim while a lease-lapse
    /// episode proves the already-built baseline again. Presence may renew.
    SuspendLocalBaseline {
        /// Exact prior locally built baseline binding.
        op: RecoveryOperation,
    },
    /// A lease-lapse episode has re-affirmed its retained local baseline.
    /// The child may start one new bounded Ready recapture, with no origin IO.
    ResumeLocalBaseline {
        /// Exact suspended prior binding.
        previous: RecoveryOperation,
        /// Fresh unique binding in the affirmed recovery generation.
        current: RecoveryOperation,
    },
    /// Observe complete bounded peer/granter/head facts for this stage.
    ObservePeers {
        /// Exact bounded membership observation operation.
        op: RecoveryOperation,
    },
    /// Observe an exact bounded incarnation roster and its native feed heads.
    ObservePeerHeads {
        /// Exact peer-specific observation operation.
        op: RecoveryOperation,
    },
    /// Wait for each exact sampled head to be locally applied.
    WaitFrontiers {
        /// Exact barrier operation.
        op: RecoveryOperation,
        /// Bounded per-peer target feed heads.
        heads: Vec<(NodeId, Mark)>,
    },
    /// Ask the lease adapter to affirm this exact recovery generation.
    Affirm {
        /// Exact recovery affirmation operation.
        op: RecoveryOperation,
    },
    /// The episode abandoned the path it was on: `from` is the stage it
    /// left. The effects before this one in the same step start the
    /// fallback (an origin rebuild, a full rebuild after a lapse proof, or
    /// origin-only service). Informational, for the operator log.
    FellBack {
        /// Stage the episode was in when it gave up.
        from: RecoveryStage,
        /// Why it gave up.
        reason: RecoveryFallback,
    },
    /// Arm the earliest finite logical deadline.
    ArmTimer(Time),
}

/// Why a recovery episode abandoned its current path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryFallback {
    /// The episode made no progress for its whole `total_ms`.
    EpisodeExpired,
    /// One operation outlived its bound without completing or progressing.
    OperationExpired,
    /// The adapter or child reported the current operation failed.
    OperationFailed,
    /// The peer-bootstrap child ended without an image.
    BaselineDeclined,
    /// A peer handoff did not bind this exact acquisition and roster.
    HandoffRejected,
    /// Peer evidence contradicted itself: a head moved backward, or a peer
    /// roster did not match the peer baseline's members.
    EvidenceRejected,
    /// A member appeared, disappeared, or exceeded the configured roster.
    MembershipChanged,
    /// Frontier rechecks kept moving past `max_barrier_rounds`.
    BarrierExhausted,
    /// A counter, token, or deadline's arithmetic was exhausted.
    Exhausted,
}

/// Reason a recovery input or bounded proof was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryError {
    /// A required configured bound is invalid.
    InvalidConfig,
    /// Wrong stage for this event.
    Stage,
    /// A stale generation, session, or operation token.
    StaleOperation,
    /// A roster, identity, or source-head bound would overflow.
    Capacity,
    /// Logical time or operation token would wrap.
    Exhausted,
    /// Caller-supplied logical time moved backwards.
    BackwardTime,
    /// An observation was internally contradictory.
    InvalidEvidence,
}

/// Effects and a typed refusal from one deterministic transition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryStep {
    /// Ordered effects; `CloseGate` precedes queued work.
    pub effects: Vec<RecoveryEffect>,
    /// No state advancement on a rejected response, except fail-closed limits.
    pub rejection: Option<RecoveryError>,
}
