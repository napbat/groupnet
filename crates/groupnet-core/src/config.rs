//! Per-group failure-detection and gossip tunables.

mod error;

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;

use crate::NodeId;

pub use error::ConfigError;

/// The consistency posture of a single group: whether it is a pure
/// metadata/membership group, or one that elects an epoch-fenced **host**.
///
/// The mode is per group, so one node mixes them freely — shard groups that
/// serialize writes through a host alongside a fabric group that stays purely
/// eventual.
///
/// # The `Eventual` guarantee
///
/// `Eventual` is the default and its contract never changes: SWIM membership,
/// last-writer-wins metadata registers, single-writer per-node keyed state,
/// and the *derived* coordinator — which is a deterministic function of the
/// membership view, never an authority. **An `Eventual` group runs no
/// election**: it never emits a [`LeadClaim`], [`LeadGrant`], or
/// [`LeadState`] frame, never claims an epoch, and never emits an
/// [`Effect::LeadershipChanged`]. Opting a *different* group into `Hosted`
/// cannot change that — the mode is not a node-wide switch.
///
/// [`LeadClaim`]: crate::wire::Kind::LeadClaim
/// [`LeadGrant`]: crate::wire::Kind::LeadGrant
/// [`LeadState`]: crate::wire::Kind::LeadState
/// [`Effect::LeadershipChanged`]: crate::Effect::LeadershipChanged
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum GroupMode {
    /// Metadata and membership only, converging eventually — the default, and
    /// what every group was before Hosted mode existed. Costs nothing beyond
    /// the gossip the fabric already pays for.
    #[default]
    Eventual,
    /// The group elects one epoch-fenced host, per [`HostedConfig`]. Writes
    /// that need a single serializer go through it; base-fabric gossip
    /// continues underneath unchanged.
    Hosted(HostedConfig),
}

/// Tunables for a [`GroupMode::Hosted`] group's election and lease.
///
/// # Sizing
///
/// Both durations here are in the engine's *logical* milliseconds, and both
/// must be **much larger than the driver's tick period** — the engine only
/// observes time when it is ticked, so a deadline finer than the tick is
/// rounded up to it. The runtime driver ticks at half the tightest configured
/// detector deadline; a lease or settle window near that period would expire
/// a whole tick late (or, worse, be indistinguishable from zero). Prefer at
/// least an order of magnitude above the tick period.
///
/// Size them against [`Config::detection_window_ms`], which is how long it
/// takes this observer to call a silent host dead:
///
/// * **`lease_ms` bounds split-brain, so it must be the shorter one.** A host
///   demotes itself `lease_ms` after its last successful renewal, and the
///   cluster must not be able to elect a successor before that: the safety
///   rule is `lease_ms` **<** `detection_window_ms(members)` + the activation's
///   settle window — the deposed host has stepped down before anyone else can
///   step up. Larger than that and two nodes can believe they hold the same
///   group at the same instant.
/// * **Too small costs availability, not safety.** A lease shorter than a
///   couple of gossip rounds makes a host renew constantly and demote itself
///   over ordinary jitter, so leadership churns.
///
/// In production this rests on bounded clock-*rate* error between nodes — the
/// standard lease assumption, stated plainly rather than assumed away. The
/// simulator checks lease disjointness exactly, in virtual time.
///
/// # Default
///
/// [`Activation::Settle`] with a 600 ms settle window (three default gossip
/// rounds) and a 1 000 ms lease, sized against [`Config::default`]: the lease
/// stays below `detection_window_ms(members)` plus the settle window for every
/// group of two or more members, so the default is split-brain-bounded as
/// shipped. Retune both when the detector timings change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostedConfig {
    /// How an epoch is closed — which peers' agreement makes a claimant the
    /// host. See [`Activation`].
    pub activation: Activation,
    /// How long a host's authority survives its last successful renewal
    /// before it demotes itself. See the sizing guidance on
    /// [`HostedConfig`] — this is the number that bounds split-brain.
    ///
    /// Under [`Activation::External`] this one number is **both** the anchor
    /// record's TTL ([`AnchorTerms::ttl_ms`](crate::anchor::AnchorTerms::ttl_ms),
    /// read off this config by
    /// [`AnchorTerms::of`](crate::anchor::AnchorTerms::of), which sets the
    /// record's `expires_at_wall_ms`) **and** the engine lease that row 6
    /// steps down on. Deliberately one knob: the two describe the same
    /// authority seen from the two clocks, and letting them drift apart is how
    /// a node keeps hosting past the record it holds — or abandons a record it
    /// still owns. It doubles as the boot guard there too, exactly as it does
    /// under [`Activation::Quorum`].
    pub lease_ms: u64,
}

/// [`HostedConfig::default`]'s settle window.
const DEFAULT_CLAIM_SETTLE_MS: u64 = 600;

/// [`HostedConfig::default`]'s lease.
const DEFAULT_LEASE_MS: u64 = 1_000;

impl Default for HostedConfig {
    fn default() -> Self {
        Self {
            activation: Activation::Settle {
                claim_settle_ms: DEFAULT_CLAIM_SETTLE_MS,
            },
            lease_ms: DEFAULT_LEASE_MS,
        }
    }
}

/// How a [`GroupMode::Hosted`] group closes an epoch: what makes a claimant
/// the host.
///
/// Activation answers *who may serve as host*; it says nothing about when a
/// hosted write may be acknowledged (that is a separate, orthogonal knob).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Activation {
    /// Lobby-style: a claimant that is still the top-ranked live candidate
    /// after the settle window activates. Available on both sides of a
    /// partition — each side elects its own host — so split-brain is possible
    /// but **bounded and fenced**: at heal exactly one epoch survives, and the
    /// loser is demoted rather than left writing.
    Settle {
        /// How long a claim must stand unchallenged before its claimant
        /// activates. It must comfortably exceed one round-trip plus the
        /// driver's tick period, or claims settle before their peers have had
        /// a chance to answer; sizing it around a small multiple of
        /// [`Config::gossip_interval_ms`] is the usual choice. It also
        /// lengthens the safe [`lease_ms`](HostedConfig::lease_ms) — see the
        /// sizing guidance on [`HostedConfig`].
        claim_settle_ms: u64,
    },
    /// Majority-style: only a claimant endorsed by a majority of the **static
    /// voter roster** activates, so a partition side that cannot reach a
    /// majority never gets a host at all. The CP posture — no split-brain
    /// outside the lease window — bought at the price of availability on the
    /// minority side.
    ///
    /// The roster is explicit and static on purpose: a gossip-*derived*
    /// majority is unsafe, because two partition sides can each hold a
    /// majority of what they can see. See [`VoterRoster`].
    ///
    /// # What it costs
    ///
    /// One grant round per election *and per renewal*: a Quorum host re-claims
    /// its epoch from the roster every [`Config::gossip_interval_ms`] and extends its
    /// lease only when a majority answers, where a `Settle` host renews by
    /// re-reading its own membership view and asks nobody. That round trip is
    /// the price of the CP posture — a host that cannot reach a majority stops
    /// being one when its lease lapses, rather than serving a second copy of
    /// the group.
    ///
    /// A voter grants at most one claimant per epoch and refuses a *new*
    /// claimant for a lease after each grant. That promise is durable when the
    /// driver honours
    /// [`Effect::PersistGrant`](crate::Effect::PersistGrant) (see
    /// [`GroupEngine::recovering`](crate::GroupEngine::recovering));
    /// with no store it rests on the post-restart grant blackout, which is a
    /// timing rule rather than a durability one.
    Quorum {
        /// The nodes whose grants close an epoch. A majority of *these* — not
        /// of whoever gossip currently shows alive — makes a host.
        voters: VoterRoster,
    },
    /// Anchor-style: an epoch is closed by a **linearizable external CAS
    /// register** — an S3/R2 object touched with `If-None-Match`/`If-Match`,
    /// an etcd key, a CAS row — and by nothing in the fabric at all. Whoever
    /// wins the conditional write is host; partition sides are irrelevant.
    ///
    /// The engine never touches the anchor. It *prompts* the driver with
    /// [`Effect::AnchorClaimDue`](crate::Effect::AnchorClaimDue) on the
    /// gossip round ([`Config::gossip_interval_ms`]) while the ordinary claim guard is open, and
    /// consumes the outcome as
    /// [`Command::AnchorActivated`](crate::Command::AnchorActivated) or
    /// [`Command::AnchorObserved`](crate::Command::AnchorObserved). No
    /// `LeadClaim` or `LeadGrant` frame is ever built, and
    /// [`Role::Claimant`](crate::Role::Claimant) is never entered: an
    /// `External` group's only election frame is `LeadState`, which carries
    /// the adopted pair for repair. See [`crate::anchor`] for the record and
    /// the decision rules.
    ///
    /// # What it buys
    ///
    /// **Absolute epoch uniqueness, with no node-local storage.** The anchor
    /// *allocates* the epoch, so no two nodes ever hold the same one — at any
    /// instant, at disjoint times, across any partition. That is strictly
    /// stronger than [`Quorum`](Activation::Quorum), whose equivalent
    /// (`S1-strict`) is *storage-conditional*: an amnesiac voter can re-issue
    /// an epoch. There is no `GrantStore` analogue here, no recovery
    /// constructor and no boot blackout, because the anchor is the ledger.
    ///
    /// A host that keeps anchor access keeps hosting however the fabric is
    /// cut; a node that loses anchor access cannot renew and demotes on lease
    /// lapse however well-connected its peers are. Anchor connectivity is the
    /// availability axis.
    ///
    /// # What it assumes, stated plainly
    ///
    /// One thing, and only on the **steal** path:
    /// **claimant wall-clock skew ≤ `steal_margin_ms`**. That is an assumption
    /// about *absolute* clock offset between nodes, which is a stronger class
    /// than the coherence-lease tier's bounded clock-**rate** error — a node
    /// whose clock ticks correctly but is set an hour fast violates it while
    /// satisfying the rate bound. It is stated here rather than assumed away
    /// because it is the only clock this tier consults.
    ///
    /// **Nothing about epoch uniqueness or fencing depends on it.** When the
    /// assumption is violated, a deposed holder may keep believing itself live
    /// for the excess; that overlap is bounded by the excess, is *always*
    /// cross-epoch (a successor's epoch is strictly higher by construction),
    /// and is *always* fenced at the store. The record is succession; the
    /// fence is safety.
    External {
        /// How far past a record's `expires_at_wall_ms` a claimant must wait
        /// before it may steal — the margin that absorbs wall-clock
        /// disagreement between the holder that wrote the expiry and the
        /// claimant reading it. See
        /// [`AnchorRecord::stealable`](crate::anchor::AnchorRecord::stealable),
        /// which is the whole rule.
        ///
        /// **The condition is *pairwise*, and that distinction is
        /// load-bearing.** What enters the arithmetic is the disagreement
        /// between *these two* clocks — the holder stamped
        /// `expires_at_wall_ms` from its own, the claimant judges it against
        /// its own — so what must hold is
        /// `|skew(claimant) - skew(holder)| <= steal_margin_ms` (plus the
        /// anchor round trip). "Every node within `steal_margin_ms` of true
        /// time" is a strictly weaker premise and is **not** sufficient: two
        /// nodes at opposite ends of that band disagree by *two* margins. Size
        /// the margin against the worst **pair**, or equivalently hold each
        /// node to half of it — which is exactly why the simulator's
        /// within-margin family draws its per-node offsets inside
        /// ±`margin / 2`.
        ///
        /// Size it above the worst clock offset the deployment tolerates
        /// (NTP-disciplined hosts: a few hundred milliseconds; unsynchronized
        /// ones: seconds). Too small risks an overlapping handover — bounded,
        /// cross-epoch, and fenced, so it costs succession timing rather than
        /// safety. Too large costs failover latency and nothing else.
        steal_margin_ms: u64,
    },
}

/// The static set of nodes whose endorsements close an epoch under
/// [`Activation::Quorum`].
///
/// Canonicalized on construction — sorted and deduplicated — so two rosters
/// built from the same nodes in different orders (or with a node named twice)
/// are the *same* roster by [`PartialEq`]. That matters because the roster is
/// part of a group's configuration: every member must agree on it, and
/// agreement cannot depend on the order a caller happened to list ids in.
///
/// Cheap to clone (an `Arc<[NodeId]>` inside), and deliberately **not**
/// `Copy` — which is what costs [`Activation`], [`HostedConfig`] and
/// [`GroupMode`] their `Copy` too.
///
/// The roster is *not* the membership view: it names who votes, not who is
/// alive. A voter that is down simply does not grant, which is exactly how a
/// minority side is starved of a host rather than served a second one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VoterRoster(Arc<[NodeId]>);

impl VoterRoster {
    /// Builds a roster from `voters`, canonicalizing it: sorted, with
    /// duplicates collapsed. Any iterable of [`NodeId`] will do.
    #[must_use]
    pub fn new(voters: impl IntoIterator<Item = NodeId>) -> Self {
        let mut voters: Vec<NodeId> = voters.into_iter().collect();
        voters.sort_unstable();
        voters.dedup();
        Self(voters.into())
    }

    /// How many distinct nodes vote.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the roster names nobody — see [`majority`](Self::majority) for
    /// what that means for the group.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Whether `node` is one of the voters.
    #[must_use]
    pub fn contains(&self, node: &NodeId) -> bool {
        self.0.contains(node)
    }

    /// The voters, in canonical (sorted) order.
    pub fn iter(&self) -> std::slice::Iter<'_, NodeId> {
        self.0.iter()
    }

    /// How many grants close an epoch: `len / 2 + 1`, a strict majority — so
    /// two rosters' majorities always intersect in at least one voter, which
    /// is what makes at most one host per epoch possible at all.
    ///
    /// | voters | majority |
    /// |---|---|
    /// | 1 | 1 |
    /// | 2 | 2 |
    /// | 3 | 2 |
    /// | 4 | 3 |
    /// | 5 | 3 |
    ///
    /// # The empty roster
    ///
    /// An empty roster yields `1`, which is unattainable from zero voters: the
    /// group can never activate a host, and stays permanently hostless. That
    /// is the fail-safe answer, and it is a deliberate `1` rather than a panic
    /// or a `0` — a `0` would make the *first* claimant a host with nobody's
    /// agreement, turning a misconfiguration into a silent loss of the very
    /// property Quorum activation is chosen for.
    #[must_use]
    pub fn majority(&self) -> usize {
        self.0.len() / 2 + 1
    }
}

impl<'a> IntoIterator for &'a VoterRoster {
    type Item = &'a NodeId;
    type IntoIter = std::slice::Iter<'a, NodeId>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Tunables for a single group's gossip and failure detection.
///
/// All durations are in milliseconds of *logical* time (see
/// [`Time`](crate::Time)), so they mean the same thing whether the engine is
/// driven by a real clock or a simulator's virtual one. [`Default`] is tuned for
/// a small, low-latency cluster.
///
/// Construct one with struct-update syntax over [`Config::default`]. The
/// timers must be nonzero; [`Config::validate`] names the first that is not,
/// and [`GroupEngine::new`](crate::GroupEngine::new) refuses a config that
/// fails it rather than correcting it. Counts and byte sizes are
/// [`NonZeroUsize`]/[`NonZeroU64`], so they cannot be zero at all.
#[derive(Clone, Debug)]
pub struct Config {
    /// Cadence of the **gossip round** — the digest/delta anti-entropy
    /// exchange. Every `gossip_interval_ms` the engine sends a digest (a
    /// compact per-member version summary, full or per-peer delta; see
    /// [`full_digest_every`](Self::full_digest_every)) to
    /// [`gossip_fanout`](Self::gossip_fanout) peers, which answer with the
    /// deltas the sender lacks and request the ones they lack. It is the
    /// group's only gossip timer: a local write or membership change brings
    /// the next round forward (and [`eager_push`](Self::eager_push) pushes a
    /// write at once), but the steady-state cadence is this one. A Hosted
    /// group's claims, grants and renewals ride the same round. The runtime
    /// driver sizes its tick from it alongside the probe timers. Must be
    /// nonzero.
    pub gossip_interval_ms: u64,
    /// How often to probe a member for liveness. Must be nonzero.
    pub probe_interval_ms: u64,
    /// How long to wait for a probe ack before escalating / suspecting. Must
    /// be nonzero.
    pub probe_timeout_ms: u64,
    /// How long a member may stay `Suspect` before it is declared `Dead`.
    /// Must be nonzero.
    pub suspect_timeout_ms: u64,
    /// How long a `Dead` tombstone (member *or* per-key entry) is gossiped. After
    /// this it stops being re-advertised; after `2×` it is reaped. This window is
    /// the **reap horizon**: it must exceed the longest partition a node can
    /// survive and still be reconciled, because a digest never resurrects an
    /// entry reaped past it (see [`GroupEngine`](crate::GroupEngine)). Must be
    /// nonzero.
    pub dead_timeout_ms: u64,
    /// How many indirect probers to enlist when a direct probe goes unanswered.
    pub indirect_probes: NonZeroUsize,
    /// How many peers each gossip round sends a digest to. A targeted pull
    /// needs far less redundancy than a blind push, so 1–2 is plenty; the
    /// default is 2. Targets rotate round-robin so every peer is covered over
    /// successive rounds. [`eager_push`](Self::eager_push) and a Hosted
    /// group's election traffic use the same rotating targets.
    pub gossip_fanout: NonZeroUsize,
    /// Push a just-authored entry (or tombstone) straight to the current
    /// [`gossip_fanout`](Self::gossip_fanout) targets as an unsolicited
    /// `Delta` frame, skipping both the tick wait and the digest/request
    /// round-trip — a local write reaches live peers at network latency. The
    /// nudged gossip round remains the repair path for everyone else and for
    /// dropped frames. Costs one small frame per write per target; disable
    /// for write-storms where batching into the next digest round is
    /// preferable.
    pub eager_push: bool,
    /// Soft cap on the encoded byte size of any single emitted frame (digest,
    /// delta, or request). Deltas that would exceed it are split across
    /// successive rounds; the only frame permitted to exceed it is one carrying a
    /// single entry whose value alone is larger (it is sent whole rather than
    /// starved).
    pub max_delta_frame_bytes: NonZeroUsize,
    /// Every Nth digest built for a given peer is a **full** digest (every
    /// gossipable member listed); the digests in between are per-peer
    /// **delta digests**, listing only members whose summary changed since
    /// the last digest built for that peer. Delta digests make the
    /// steady-state gossip round scale with recent churn instead of
    /// membership size; the periodic full digest bounds how long anything a
    /// dropped frame (or TTL-expiry drift) left divergent can stay that way.
    /// A peer's first digest is always full. `1` disables delta digests
    /// (every digest full — the pre-delta behaviour). Default: 4.
    pub full_digest_every: NonZeroU64,
    /// This group's consistency posture. Defaults to
    /// [`GroupMode::Eventual`] — metadata and membership only, no election.
    pub mode: GroupMode,
}

impl Config {
    /// Checks every timer for a value the engine cannot run on, returning
    /// the first violated rule.
    ///
    /// [`mode`](Self::mode) is not inspected: a group's posture is chosen per
    /// group and validated by whatever builds it, so overriding the mode of a
    /// validated config leaves it valid.
    ///
    /// # Errors
    ///
    /// One [`ConfigError`] variant per rule, naming the field:
    /// [`ZeroGossipInterval`](ConfigError::ZeroGossipInterval),
    /// [`ZeroProbeInterval`](ConfigError::ZeroProbeInterval),
    /// [`ZeroProbeTimeout`](ConfigError::ZeroProbeTimeout),
    /// [`ZeroSuspectTimeout`](ConfigError::ZeroSuspectTimeout) and
    /// [`ZeroDeadTimeout`](ConfigError::ZeroDeadTimeout).
    pub const fn validate(&self) -> Result<(), ConfigError> {
        if self.gossip_interval_ms == 0 {
            return Err(ConfigError::ZeroGossipInterval);
        }
        if self.probe_interval_ms == 0 {
            return Err(ConfigError::ZeroProbeInterval);
        }
        if self.probe_timeout_ms == 0 {
            return Err(ConfigError::ZeroProbeTimeout);
        }
        if self.suspect_timeout_ms == 0 {
            return Err(ConfigError::ZeroSuspectTimeout);
        }
        if self.dead_timeout_ms == 0 {
            return Err(ConfigError::ZeroDeadTimeout);
        }
        Ok(())
    }

    /// The worst-case time, in milliseconds, from a member falling silent to
    /// **this** observer holding it [`Status::Dead`], in a group of `members`
    /// nodes (this observer included).
    ///
    /// This is the number a consumer sizes a trust window from — "gossip has
    /// been healthy for this long, so a missing peer really is gone" — and it
    /// is deliberately a *conservative upper* bound: overestimating costs a
    /// little latency, underestimating makes the claim false. Read it off the
    /// **effective** config of a built node (`Group::config()` in the runtime
    /// layer), never off [`Config::default`], so it tracks configuration
    /// drift.
    ///
    /// # The bound
    ///
    /// ```text
    /// (members - 1) · (probe_interval_ms + 2 · probe_timeout_ms)  // one pass of the ring
    ///               + suspect_timeout_ms                          // Suspect -> Dead
    /// ```
    ///
    /// Every step of the round-robin pass is charged the price of a step that
    /// stalls behind a **silent** peer, not just the target's own step — which
    /// is why the two miss deadlines are folded into the per-step term rather
    /// than added once at the end. That is what makes the bound survive a
    /// *concurrent* failure: a second member falling silent at the same
    /// instant blocks the ring on the way to the first, and the budget for
    /// that is already in every step. (Three or more at once is a different
    /// story — see the last assumption below.)
    ///
    /// Term by term, against what the detector actually does:
    ///
    /// * **One step of the pass.** The detector probes *one* peer per
    ///   `probe_interval`, round-robin, with at most one probe outstanding —
    ///   a slot whose predecessor is still in flight is skipped, and the
    ///   cursor only advances when a probe is issued. A step that lands on a
    ///   peer which is *up* costs at most `probe_interval_ms +
    ///   probe_timeout_ms`: the wait for the next slot, plus the window the
    ///   outstanding probe may occupy before its ack clears it. The **sum**,
    ///   not the larger of the two: a round trip that outlives its slot makes
    ///   the detector skip that slot and wait for the next, so a step
    ///   genuinely costs both terms (the simulator pins this — a `max(…)`
    ///   folding under-promises whenever `probe_timeout` exceeds
    ///   `probe_interval`).
    /// * **A step that lands on another silent member** costs
    ///   `probe_interval_ms + 2 · probe_timeout_ms` instead: that peer burns
    ///   its direct-miss deadline and then the indirect (`ping-req`) round
    ///   that follows, and *both* keep the single outstanding-probe slot
    ///   occupied — the whole ring waits behind it. Charging every step this
    ///   price is what pays for a concurrent failure instead of discovering
    ///   it as an overrun.
    /// * **Reaching the target.** A member that fell silent just after being
    ///   acked must wait for the cursor to come all the way round:
    ///   `members - 2` steps for the other peers, and one more for its own
    ///   slot — whose unanswered direct probe plus the indirect probes after
    ///   it cost exactly the same `probe_interval_ms + 2 · probe_timeout_ms`.
    ///   Folded here as `members - 1` steps at the silent price. (In a group
    ///   too small to enlist a prober the engine suspects immediately
    ///   instead, which is strictly faster.)
    /// * **The refutation window.** A `Suspect` member is declared `Dead`
    ///   `suspect_timeout_ms` later.
    ///
    /// A group of one has no peer to detect, and the bound degenerates to the
    /// refutation window: it is the `members` count that is meaningless
    /// there, not the arithmetic.
    ///
    /// # What it assumes, stated plainly
    ///
    /// * Peers that are *up* answer a probe within `probe_timeout_ms`.
    ///   Sustained probe loss stretches detection without bound — no failure
    ///   detector can bound that away, and this number does not pretend to. A
    ///   live peer whose acks keep getting dropped is indistinguishable from a
    ///   slow one, so it is *lossy live* peers this cannot cover; a peer that
    ///   is simply silent is budgeted for.
    /// * It bounds **this observer's own** detection. Gossip from a peer that
    ///   detected the failure sooner only ever shortens it.
    /// * It is in the engine's *logical* milliseconds. A driver that samples
    ///   time on a tick coarser than `probe_timeout_ms` adds up to one tick
    ///   period per phase; the runtime driver ticks at half the tightest
    ///   configured deadline, which the per-step slack above absorbs.
    /// * **At most two members are silent at once** — the failure and one
    ///   more. The per-step budget above assumes the ring reaches the target
    ///   in one pass, and with three or more simultaneous silences it may not:
    ///   a `Suspect` member keeps its slot in the probe ring until it is
    ///   declared `Dead` (that is how a member that comes back gets a chance
    ///   to refute), so one silent peer can stall the ring more than once, and
    ///   a member leaving the candidate set shifts the round-robin cursor's
    ///   modulus. The simulator pins one and two simultaneous crashes with no
    ///   slack; three overruns it in the same harness, so this is a measured
    ///   limit rather than a hedge. For correlated failure of a large fraction
    ///   of the group, take the verdict from
    ///   [`dead_timeout_ms`](Self::dead_timeout_ms)'s reap horizon instead.
    ///
    /// Saturating throughout, so an absurd config yields `u64::MAX` rather
    /// than a wrapped — and silently too small — window.
    ///
    /// [`Status::Dead`]: crate::Status
    #[must_use]
    pub fn detection_window_ms(&self, members: usize) -> u64 {
        let peers = u64::try_from(members.saturating_sub(1)).unwrap_or(u64::MAX);
        // Every step pays the silent-peer price (slot + direct miss + indirect
        // miss), so a step stalled behind a concurrent failure is budgeted for
        // rather than discovered as an overrun.
        let step = self
            .probe_interval_ms
            .saturating_add(self.probe_timeout_ms.saturating_mul(2));
        peers
            .saturating_mul(step)
            .saturating_add(self.suspect_timeout_ms)
    }
}

/// [`Config::indirect_probes`] by default.
const DEFAULT_INDIRECT_PROBES: NonZeroUsize = NonZeroUsize::new(2).unwrap();

/// [`Config::gossip_fanout`] by default.
const DEFAULT_GOSSIP_FANOUT: NonZeroUsize = NonZeroUsize::new(2).unwrap();

/// [`Config::max_delta_frame_bytes`] by default.
const DEFAULT_MAX_DELTA_FRAME_BYTES: NonZeroUsize = NonZeroUsize::new(60_000).unwrap();

/// [`Config::full_digest_every`] by default.
const DEFAULT_FULL_DIGEST_EVERY: NonZeroU64 = NonZeroU64::new(4).unwrap();

impl Default for Config {
    fn default() -> Self {
        Self {
            gossip_interval_ms: 200,
            probe_interval_ms: 100,
            probe_timeout_ms: 50,
            suspect_timeout_ms: 500,
            dead_timeout_ms: 10_000,
            indirect_probes: DEFAULT_INDIRECT_PROBES,
            gossip_fanout: DEFAULT_GOSSIP_FANOUT,
            eager_push: true,
            max_delta_frame_bytes: DEFAULT_MAX_DELTA_FRAME_BYTES,
            full_digest_every: DEFAULT_FULL_DIGEST_EVERY,
            mode: GroupMode::Eventual,
        }
    }
}

#[cfg(test)]
mod tests;
