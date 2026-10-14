//! Bounded facts and effects for recovery from a volatile coherence feed.

use std::fmt;
use std::num::{NonZeroU32, NonZeroUsize};

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
///
/// Roster caps are nonzero by type; [`RecoveryConfig::validate`] checks the
/// millisecond timers, their relation to `total_ms`, and the roster byte
/// product. [`Default`] admits 1024 peers with 256-byte identities and eight
/// barrier rounds, and gives an episode 60 s, one operation 10 s, the settle
/// delay 1 s and the observation poll 200 ms.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoveryConfig {
    /// Maximum peer identities in any observation or retained recovery roster.
    pub max_members: NonZeroUsize,
    /// Maximum UTF-8 bytes in one peer identity.
    pub max_member_bytes: NonZeroUsize,
    /// Maximum source-head resamples after a reached frontier moves.
    pub max_barrier_rounds: NonZeroU32,
    /// Liveness budget for one recovery episode: time allowed since the
    /// episode began or since its long-running rebuild or baseline operation
    /// last reported progress. A progressing operation is never failed over.
    /// Nonzero.
    pub total_ms: u64,
    /// Duration one adapter operation may run without completing or, for an
    /// origin rebuild, without reporting progress. Nonzero and at most
    /// `total_ms`.
    pub attempt_ms: u64,
    /// Delay after all per-granter renewals advance; nonzero and at most
    /// `total_ms`.
    pub settle_ms: u64,
    /// Delay between unsuccessful grant or affirmation observations; nonzero
    /// and at most `total_ms`.
    pub poll_ms: u64,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            max_members: const { NonZeroUsize::new(1024).unwrap() },
            max_member_bytes: const { NonZeroUsize::new(256).unwrap() },
            max_barrier_rounds: const { NonZeroU32::new(8).unwrap() },
            total_ms: 60_000,
            attempt_ms: 10_000,
            settle_ms: 1_000,
            poll_ms: 200,
        }
    }
}

/// Invalid [`RecoveryConfig`], naming the violated rule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryConfigError {
    /// [`RecoveryConfig::total_ms`] is zero.
    ZeroTotal,
    /// [`RecoveryConfig::attempt_ms`] is zero.
    ZeroAttempt,
    /// [`RecoveryConfig::settle_ms`] is zero.
    ZeroSettle,
    /// [`RecoveryConfig::poll_ms`] is zero.
    ZeroPoll,
    /// [`RecoveryConfig::attempt_ms`] exceeds [`RecoveryConfig::total_ms`].
    AttemptExceedsTotal,
    /// [`RecoveryConfig::settle_ms`] exceeds [`RecoveryConfig::total_ms`].
    SettleExceedsTotal,
    /// [`RecoveryConfig::poll_ms`] exceeds [`RecoveryConfig::total_ms`].
    PollExceedsTotal,
    /// [`RecoveryConfig::max_members`] times
    /// [`RecoveryConfig::max_member_bytes`] overflows `usize`.
    RosterBytesOverflow,
}

impl fmt::Display for RecoveryConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ZeroTotal => "recovery total_ms must be nonzero",
            Self::ZeroAttempt => "recovery attempt_ms must be nonzero",
            Self::ZeroSettle => "recovery settle_ms must be nonzero",
            Self::ZeroPoll => "recovery poll_ms must be nonzero",
            Self::AttemptExceedsTotal => "recovery attempt_ms must not exceed total_ms",
            Self::SettleExceedsTotal => "recovery settle_ms must not exceed total_ms",
            Self::PollExceedsTotal => "recovery poll_ms must not exceed total_ms",
            Self::RosterBytesOverflow => "recovery max_members * max_member_bytes overflows usize",
        })
    }
}

impl std::error::Error for RecoveryConfigError {}

impl RecoveryConfig {
    /// Validates every finite limit before any effect is emitted.
    ///
    /// # Errors
    /// Returns the [`RecoveryConfigError`] variant naming the first violated rule.
    pub fn validate(&self) -> Result<(), RecoveryConfigError> {
        if self.total_ms == 0 {
            return Err(RecoveryConfigError::ZeroTotal);
        }
        if self.attempt_ms == 0 {
            return Err(RecoveryConfigError::ZeroAttempt);
        }
        if self.settle_ms == 0 {
            return Err(RecoveryConfigError::ZeroSettle);
        }
        if self.poll_ms == 0 {
            return Err(RecoveryConfigError::ZeroPoll);
        }
        if self.attempt_ms > self.total_ms {
            return Err(RecoveryConfigError::AttemptExceedsTotal);
        }
        if self.settle_ms > self.total_ms {
            return Err(RecoveryConfigError::SettleExceedsTotal);
        }
        if self.poll_ms > self.total_ms {
            return Err(RecoveryConfigError::PollExceedsTotal);
        }
        if self
            .max_members
            .checked_mul(self.max_member_bytes)
            .is_none()
        {
            return Err(RecoveryConfigError::RosterBytesOverflow);
        }
        Ok(())
    }
}

/// Optional spacing between exhausted full recovery episodes.
/// Individual operations inside an episode still retry on `RecoveryConfig::poll_ms`.
///
/// [`Default`] waits 1 s after the first exhausted episode and doubles up to
/// a minute.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoveryRearm {
    /// Delay after the first exhausted episode, in logical milliseconds; nonzero.
    pub initial_ms: u64,
    /// Inclusive cap on later doubled delays; at least `initial_ms`.
    pub max_ms: u64,
}

impl Default for RecoveryRearm {
    fn default() -> Self {
        Self {
            initial_ms: 1_000,
            max_ms: 60_000,
        }
    }
}

/// Invalid [`RecoveryRearm`], naming the violated rule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryRearmError {
    /// [`RecoveryRearm::initial_ms`] is zero.
    ZeroInitial,
    /// [`RecoveryRearm::max_ms`] is below [`RecoveryRearm::initial_ms`].
    MaxBelowInitial,
}

impl fmt::Display for RecoveryRearmError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ZeroInitial => "recovery rearm initial_ms must be nonzero",
            Self::MaxBelowInitial => "recovery rearm max_ms must be at least initial_ms",
        })
    }
}

impl std::error::Error for RecoveryRearmError {}

impl RecoveryRearm {
    /// Rejects absent or inverted delay bounds before a recovery event starts.
    ///
    /// # Errors
    /// Returns the [`RecoveryRearmError`] variant naming the violated rule.
    pub fn validate(&self) -> Result<(), RecoveryRearmError> {
        if self.initial_ms == 0 {
            return Err(RecoveryRearmError::ZeroInitial);
        }
        if self.max_ms < self.initial_ms {
            return Err(RecoveryRearmError::MaxBelowInitial);
        }
        Ok(())
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

/// A writer's restart that this observer crossed without a gap: its feed
/// delivered the previous life through the writer's seal and then renewed
/// the writer's cursor into `epoch`
/// (`groupnet_consistency::PeerWrite::Renewed`). The old life ended at its
/// seal, so an advertised head that moved from that life into `epoch` is a
/// progression, not lost evidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Renewal {
    /// The previous life's delivered seal: the position after its last write.
    pub sealed: Mark,
    /// The life the observer crossed into; must be newer than `sealed`'s.
    pub epoch: u64,
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
    /// The observer's latest crossing of this writer's restarts, when it was a
    /// sealed renewal into the writer's current life. Report `None` once any
    /// later crossing of the same writer was a gap.
    pub renewal: Option<Renewal>,
    /// The observer's delivered seal of this writer's life it is still in:
    /// the position after that life's last write. Report it from the
    /// delivered `groupnet_consistency::PeerWrite::Sealed` until the observer
    /// crosses into a later life of the writer (`renewal` then covers the
    /// crossing) or takes a gap. Every write of that life is applied and the
    /// writer promised none after it, so its advertised head leaving the
    /// roster — the member reaped, relearned with no state yet, or replaced
    /// by its next life — is not lost evidence.
    pub sealed: Option<Mark>,
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
    /// A recovery Ready on a peer's installed image binds that image to the
    /// child as its local baseline, which may then donate it like an origin
    /// build, with no origin IO.
    AdoptLocalBaseline {
        /// Fresh unique binding in the affirmed recovery generation.
        op: RecoveryOperation,
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
    /// The [`RecoveryConfig`] is invalid.
    Config(RecoveryConfigError),
    /// The [`RecoveryRearm`] policy is invalid.
    Rearm(RecoveryRearmError),
    /// The local node identity is empty or oversized, or the session is zero.
    InvalidIdentity,
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

impl fmt::Display for RecoveryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Config(_) => "invalid recovery config",
            Self::Rearm(_) => "invalid recovery rearm policy",
            Self::InvalidIdentity => "invalid recovery node identity or session",
            Self::Stage => "wrong recovery stage for this event",
            Self::StaleOperation => "stale recovery generation, session, or operation",
            Self::Capacity => "recovery roster, identity, or head bound would overflow",
            Self::Exhausted => "recovery time or operation token would wrap",
            Self::BackwardTime => "recovery time moved backwards",
            Self::InvalidEvidence => "recovery observation was contradictory",
        })
    }
}

impl std::error::Error for RecoveryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Config(error) => Some(error),
            Self::Rearm(error) => Some(error),
            _ => None,
        }
    }
}

/// Effects and a typed refusal from one deterministic transition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryStep {
    /// Ordered effects; `CloseGate` precedes queued work.
    pub effects: Vec<RecoveryEffect>,
    /// No state advancement on a rejected response, except fail-closed limits.
    pub rejection: Option<RecoveryError>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> RecoveryConfig {
        RecoveryConfig {
            max_members: NonZeroUsize::new(4).unwrap(),
            max_member_bytes: NonZeroUsize::new(32).unwrap(),
            max_barrier_rounds: NonZeroU32::new(3).unwrap(),
            total_ms: 100,
            attempt_ms: 20,
            settle_ms: 5,
            poll_ms: 2,
        }
    }

    #[test]
    fn defaults_are_valid() {
        assert_eq!(RecoveryConfig::default().validate(), Ok(()));
        assert_eq!(RecoveryRearm::default().validate(), Ok(()));
    }

    #[test]
    fn config_validate_names_each_rule() {
        assert_eq!(config().validate(), Ok(()));
        let cases = [
            (
                RecoveryConfig {
                    total_ms: 0,
                    ..config()
                },
                RecoveryConfigError::ZeroTotal,
            ),
            (
                RecoveryConfig {
                    attempt_ms: 0,
                    ..config()
                },
                RecoveryConfigError::ZeroAttempt,
            ),
            (
                RecoveryConfig {
                    settle_ms: 0,
                    ..config()
                },
                RecoveryConfigError::ZeroSettle,
            ),
            (
                RecoveryConfig {
                    poll_ms: 0,
                    ..config()
                },
                RecoveryConfigError::ZeroPoll,
            ),
            (
                RecoveryConfig {
                    attempt_ms: 101,
                    ..config()
                },
                RecoveryConfigError::AttemptExceedsTotal,
            ),
            (
                RecoveryConfig {
                    settle_ms: 101,
                    ..config()
                },
                RecoveryConfigError::SettleExceedsTotal,
            ),
            (
                RecoveryConfig {
                    poll_ms: 101,
                    ..config()
                },
                RecoveryConfigError::PollExceedsTotal,
            ),
            (
                RecoveryConfig {
                    max_members: NonZeroUsize::MAX,
                    max_member_bytes: NonZeroUsize::new(2).unwrap(),
                    ..config()
                },
                RecoveryConfigError::RosterBytesOverflow,
            ),
        ];
        for (config, expected) in cases {
            assert_eq!(config.validate(), Err(expected));
        }
        let error = RecoveryError::Config(RecoveryConfigError::ZeroPoll);
        assert_eq!(error.to_string(), "invalid recovery config");
        assert_eq!(
            std::error::Error::source(&error).map(ToString::to_string),
            Some("recovery poll_ms must be nonzero".to_owned())
        );
    }

    #[test]
    fn rearm_validate_names_each_rule() {
        let rearm = RecoveryRearm {
            initial_ms: 5,
            max_ms: 5,
        };
        assert_eq!(rearm.validate(), Ok(()));
        assert_eq!(
            RecoveryRearm {
                initial_ms: 0,
                ..rearm
            }
            .validate(),
            Err(RecoveryRearmError::ZeroInitial)
        );
        assert_eq!(
            RecoveryRearm { max_ms: 4, ..rearm }.validate(),
            Err(RecoveryRearmError::MaxBelowInitial)
        );
        let error = RecoveryError::Rearm(RecoveryRearmError::MaxBelowInitial);
        assert_eq!(
            std::error::Error::source(&error).map(ToString::to_string),
            Some("recovery rearm max_ms must be at least initial_ms".to_owned())
        );
        assert!(std::error::Error::source(&RecoveryError::Stage).is_none());
    }
}
