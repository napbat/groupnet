//! Per-group failure-detection and gossip tunables.

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
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HostedConfig {
    /// How an epoch is closed — which peers' agreement makes a claimant the
    /// host. See [`Activation`].
    pub activation: Activation,
    /// How long a host's authority survives its last successful renewal
    /// before it demotes itself. See the sizing guidance on
    /// [`HostedConfig`] — this is the number that bounds split-brain.
    pub lease_ms: u64,
}

/// How a [`GroupMode::Hosted`] group closes an epoch: what makes a claimant
/// the host.
///
/// Activation answers *who may serve as host*; it says nothing about when a
/// hosted write may be acknowledged (that is a separate, orthogonal knob).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
}

/// Tunables for a single group's gossip and failure detection.
///
/// All durations are in milliseconds of *logical* time (see
/// [`Time`](crate::Time)), so they mean the same thing whether the engine is
/// driven by a real clock or a simulator's virtual one. [`Default`] is tuned for
/// a small, low-latency cluster.
#[derive(Clone, Debug)]
pub struct Config {
    /// How often the periodic anti-entropy round fires. Retained under its
    /// historic name because it *is* "the current gossip interval": since G3 the
    /// round disseminates compact **digests** (per-node version summaries), not
    /// the full view, and [`anti_entropy_interval_ms`](Self::anti_entropy_interval_ms)
    /// defaults to it.
    pub gossip_interval_ms: u64,
    /// How often to probe a member for liveness.
    pub probe_interval_ms: u64,
    /// How long to wait for a probe ack before escalating / suspecting.
    pub probe_timeout_ms: u64,
    /// How long a member may stay `Suspect` before it is declared `Dead`.
    pub suspect_timeout_ms: u64,
    /// How long a `Dead` tombstone (member *or* per-key entry) is gossiped. After
    /// this it stops being re-advertised; after `2×` it is reaped. This window is
    /// the **reap horizon**: it must exceed the longest partition a node can
    /// survive and still be reconciled, because a digest never resurrects an
    /// entry reaped past it (see [`GroupEngine`](crate::GroupEngine)).
    pub dead_timeout_ms: u64,
    /// How many indirect probers to enlist when a direct probe goes unanswered.
    pub indirect_probes: usize,
    /// Maximum peers a legacy full-view push would fan out to. Retained for
    /// source compatibility; the anti-entropy path uses
    /// [`anti_entropy_fanout`](Self::anti_entropy_fanout).
    pub fanout: usize,
    /// Cadence of the digest/delta **anti-entropy** exchange. Defaults to
    /// [`gossip_interval_ms`](Self::gossip_interval_ms); the `gossip_interval_ms`
    /// builder knob keeps the two in step, so tuning the gossip rate tunes
    /// anti-entropy with it.
    pub anti_entropy_interval_ms: u64,
    /// How many peers each anti-entropy round sends a digest to. A targeted pull
    /// needs far less redundancy than a blind push, so 1–2 is plenty; the default
    /// is 2. Fanout rotates round-robin so every peer is covered over successive
    /// rounds.
    pub anti_entropy_fanout: usize,
    /// Push a just-authored entry (or tombstone) straight to the current
    /// fanout targets as an unsolicited `Delta` frame, skipping both the tick
    /// wait and the digest/request round-trip — a local write reaches live
    /// peers at network latency. The nudged anti-entropy round remains the
    /// repair path for everyone else and for dropped frames. Costs one small
    /// frame per write per fanout target; disable for write-storms where
    /// batching into the next digest round is preferable.
    pub eager_push: bool,
    /// Soft cap on the encoded byte size of any single emitted frame (digest,
    /// delta, or request). Deltas that would exceed it are split across
    /// successive rounds; the only frame permitted to exceed it is one carrying a
    /// single entry whose value alone is larger (it is sent whole rather than
    /// starved).
    pub max_delta_frame_bytes: usize,
    /// Every Nth digest built for a given peer is a **full** digest (every
    /// gossipable member listed); the digests in between are per-peer
    /// **delta digests**, listing only members whose summary changed since
    /// the last digest built for that peer. Delta digests make the
    /// steady-state anti-entropy round scale with recent churn instead of
    /// membership size; the periodic full digest bounds how long anything a
    /// dropped frame (or TTL-expiry drift) left divergent can stay that way.
    /// A peer's first digest is always full. `1` disables delta digests
    /// (every digest full — the pre-delta behaviour). Default: 4.
    pub full_digest_every: u64,
    /// This group's consistency posture. Defaults to
    /// [`GroupMode::Eventual`] — metadata and membership only, no election.
    pub mode: GroupMode,
}

impl Config {
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

impl Default for Config {
    fn default() -> Self {
        Self {
            gossip_interval_ms: 200,
            probe_interval_ms: 100,
            probe_timeout_ms: 50,
            suspect_timeout_ms: 500,
            dead_timeout_ms: 10_000,
            indirect_probes: 2,
            fanout: 3,
            anti_entropy_interval_ms: 200,
            anti_entropy_fanout: 2,
            eager_push: true,
            max_delta_frame_bytes: 60_000,
            full_digest_every: 4,
            mode: GroupMode::Eventual,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Activation, Config, GroupMode, HostedConfig};

    /// A group is a plain metadata/membership group unless it is explicitly
    /// opted in — the Eventual contract is the default, in both spellings.
    #[test]
    fn eventual_is_the_default_mode() {
        assert_eq!(Config::default().mode, GroupMode::Eventual);
        assert_eq!(GroupMode::default(), GroupMode::Eventual);
    }

    /// The mode rides `Config`'s struct-update syntax like every other knob,
    /// and a Hosted config compares by value.
    #[test]
    fn hosted_mode_carries_its_activation_and_lease() {
        let hosted = HostedConfig {
            activation: Activation::Settle {
                claim_settle_ms: 600,
            },
            lease_ms: 2_000,
        };
        let cfg = Config {
            mode: GroupMode::Hosted(hosted),
            ..Config::default()
        };
        assert_eq!(cfg.mode, GroupMode::Hosted(hosted));
        assert_ne!(cfg.mode, GroupMode::Eventual);
        // Everything else is untouched by opting in.
        assert_eq!(
            cfg.detection_window_ms(3),
            Config::default().detection_window_ms(3)
        );
    }

    /// A bigger group means a longer round-robin pass before the detector
    /// reaches any one member, so the window may never shrink as members are
    /// added — and at the default timings it strictly grows.
    #[test]
    fn detection_window_is_monotone_in_members() {
        let cfg = Config::default();
        let mut prev = cfg.detection_window_ms(0);
        for members in 1..=64usize {
            let window = cfg.detection_window_ms(members);
            assert!(
                window >= prev,
                "window shrank from {prev} to {window} at {members} members"
            );
            if members >= 2 {
                assert!(
                    window > prev,
                    "window did not grow from {prev} to {window} at {members} members"
                );
            }
            prev = window;
        }
    }

    /// The reason every step is charged the silent price: over one pass of the
    /// ring, with `F` *other* members gone quiet at the same moment, the
    /// detector's worst path to a verdict is `(members - 2 - F)·(i + t)` live
    /// steps, `F·(i + 2t)` steps stalled behind a silent peer's direct miss
    /// *and* its indirect re-arm, the target's own `i + 2t` slot, and `s`. The
    /// window must cover every `F`, not only the `F = 0` single-failure case —
    /// the pre-fix formula covered it only up to `F = 1`.
    ///
    /// This is the *one-pass* algebra; whether the ring reaches the target in
    /// one pass is the separate assumption the rustdoc states (and the
    /// simulator probes), not something arithmetic can settle.
    #[test]
    fn window_covers_a_one_pass_ring_with_any_number_of_silent_members() {
        for cfg in [
            Config::default(),
            Config {
                probe_interval_ms: 40,
                probe_timeout_ms: 250, // timeout > interval: slots get skipped
                suspect_timeout_ms: 90,
                ..Config::default()
            },
        ] {
            let (i, t, s) = (
                cfg.probe_interval_ms,
                cfg.probe_timeout_ms,
                cfg.suspect_timeout_ms,
            );
            for members in 2..=32u64 {
                let window =
                    cfg.detection_window_ms(usize::try_from(members).expect("a small count"));
                // `F` ranges over every possibility, up to everyone else being
                // silent too (`members - 2`).
                for silent in 0..=members - 2 {
                    let worst =
                        (members - 2 - silent) * (i + t) + silent * (i + 2 * t) + (i + 2 * t) + s;
                    assert!(
                        window >= worst,
                        "{members}-node window {window} is under the {worst}ms worst case \
                         with {silent} other members silent"
                    );
                }
            }
        }
    }

    /// The smallest real detection: one peer, so the whole path is one probe
    /// slot, the unanswered probe, and the refutation window. The bound must
    /// cover at least that.
    #[test]
    fn two_node_window_covers_one_probe_plus_suspicion() {
        for cfg in [
            Config::default(),
            Config {
                probe_interval_ms: 40,
                probe_timeout_ms: 250, // timeout > interval: slots get skipped
                suspect_timeout_ms: 90,
                ..Config::default()
            },
        ] {
            let floor = cfg.probe_interval_ms + cfg.probe_timeout_ms + cfg.suspect_timeout_ms;
            assert!(
                cfg.detection_window_ms(2) >= floor,
                "2-node window {} is below the {floor}ms floor",
                cfg.detection_window_ms(2)
            );
        }
    }

    /// The defaults must land somewhere a consumer can actually use: under a
    /// second for a small cluster, and still seconds — not minutes — at fifty.
    #[test]
    fn default_window_is_sane_at_realistic_cluster_sizes() {
        let cfg = Config::default();
        // 2 · (100 + 2·50) + 500
        assert_eq!(cfg.detection_window_ms(3), 900);
        assert!(
            (500..2_000).contains(&cfg.detection_window_ms(2)),
            "a 2-node window should be sub-2s, got {}",
            cfg.detection_window_ms(2)
        );
        assert!(
            cfg.detection_window_ms(50) < 30_000,
            "a 50-node window should stay in seconds, not minutes, got {}",
            cfg.detection_window_ms(50)
        );
    }

    /// Degenerate inputs never panic and never wrap into a too-small window.
    #[test]
    fn absurd_inputs_saturate_instead_of_wrapping() {
        let cfg = Config {
            probe_interval_ms: u64::MAX,
            probe_timeout_ms: u64::MAX,
            suspect_timeout_ms: u64::MAX,
            ..Config::default()
        };
        assert_eq!(cfg.detection_window_ms(usize::MAX), u64::MAX);
        // A group with no peers has nothing to detect: no probe steps, so the
        // bound degenerates to the refutation window rather than to garbage.
        assert_eq!(
            Config::default().detection_window_ms(1),
            Config::default().suspect_timeout_ms
        );
    }
}
