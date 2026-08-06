//! The coherence-lease tier (T3): a reader's self-expiring **right to serve**.
//!
//! The strong-coherence tier below this one ([`acks`](crate::AckLedger)) is
//! unanimity over a rumour-derived set: every write blocks on every peer the
//! writer currently believes alive, one degraded-but-alive peer taxes every
//! write cluster-wide, and a timeout ends in a *degradation* rather than a
//! guarantee — correctness during the window depends on the stale peer
//! *learning* it should stand down, which is exactly what an
//! asymmetrically-partitioned peer cannot do. The root cause is structural:
//! the read side has no self-expiring right to serve, so the write side has no
//! choice but to chase acks from everyone, forever.
//!
//! This tier gives the read side that right (Gray–Cheriton freshness leases).
//! A node may serve locally-cached state — including authoritative negatives,
//! the 404 a cache wants to answer without asking — only while it holds an
//! unexpired **serve-lease**. A writer's invalidation blocks on responsive
//! lease-holders (the fast path, exactly the cost of a T2 ack round when
//! healthy) or on the *lapse* of a silent peer's lease (the slow path,
//! bounded, and ended by the stale node's own clock rather than by anyone's
//! patience).
//!
//! # The protocol, in one page
//!
//! * **Renew.** A reader publishes `~lease` — one [`RenewalId`] under a TTL of
//!   one lease duration — every [`LeaseConfig::renew_every`], recording the
//!   instant `s_i` *before* each write is enqueued.
//! * **Grant.** Every member folds the renewals it has adopted into one
//!   wholesale `~lease:g` entry: `(reader, epoch, seq)` per reader, replace
//!   semantics, no TTL. That is a granter saying "I have seen this reader's
//!   lease and I will wait for it".
//! * **Serve.** The reader may serve iff `now < s_i + duration - rate_margin`
//!   for the newest renewal `i` confirmed by *every* granter in its roster —
//!   and it is not in [`LeaseState::NeedsResync`]. Each granter's own copy of
//!   renewal `i` expires no earlier than `s_i + duration`, because the engine
//!   arms the TTL when it *adopts* the entry, which is after `s_i`.
//! * **Invalidate.** A writer's coherent write waits, per member holding a
//!   live `~lease` entry, for either an applied ack at or past the write's
//!   [`WriteToken`](crate::WriteToken) (the T2 fast path) or for its own
//!   engine to expire that member's `~lease` entry (the lapse).
//! * **Resync.** A reader that lapsed enters [`LeaseState::NeedsResync`] and
//!   stays there — a freshly confirmed lease is *not* enough — until its
//!   consumer has re-synchronized and affirmed it. This is the correctness
//!   rule of the whole tier: a lapsed reader missed exactly the invalidations
//!   whose writers proceeded *because* it had lapsed.
//!
//! The sans-IO halves are [`LeaseCore`] (reader) and [`CoherenceCore`]
//! (writer); the tokio shell around them is [`Leases`] / [`LeaseView`].
//!
//! # Honesty box: what this guarantees, and where it stops
//!
//! **The guarantee.** While a reader's [`LeaseView::valid`] answers `true`, no
//! completed write of a participating writer is invisible to it: the writer
//! either waited for this node to apply the invalidation, or it waited for
//! this node's serve-lease to lapse — and a lapsed node serves nothing cached
//! until its consumer re-synchronizes. Write-wait under failure is therefore
//! `min(acks, lease remainder)` with a real guarantee at the end, instead of a
//! timeout with a hope at the end — *for the two failure shapes those two terms
//! cover*. There is a third, and it belongs in the same breath: a holder that
//! keeps **renewing** while it stops **applying** offers neither an ack nor a
//! lapse, so the write ends at the caller's own deadline as
//! [`CoherenceOutcome::TimedOut`], with no guarantee at all. That is an
//! availability failure and never a silent stale serve (the writer knows, and
//! says so in `waiting_on`) — see the fail-slow reader below.
//!
//! It rests on four assumptions, each of which is a failure mode you should
//! know by name:
//!
//! * **Bounded clock *rate* skew — not bounded connectivity.** The reader's
//!   window is computed on its own clock and the granters' expiries on theirs.
//!   If a reader's clock runs slow relative to a granter's by more than
//!   [`LeaseConfig::rate_margin`] over one lease duration, the reader can
//!   still believe it holds a lease the granter has already expired. This is
//!   an assumption about *rates* (a few hundred ppm on any healthy host), not
//!   about steps: a wall-clock jump cannot affect it, because every instant
//!   here comes from a monotonic clock. Size `rate_margin` for the worst
//!   drift you accept, not for the typical one.
//! * **Membership divergence, bounded by the reap horizon.** A reader waits
//!   for confirmations from every not-reaped member advertising
//!   [`CAP_LEASE`]. If it *reaps* a granter that is in fact still writing —
//!   an asymmetric partition outliving the reap horizon — that granter leaves
//!   the reader's min-set and the reader keeps serving while a live writer no
//!   longer waits for it. Three guards narrow the residual and none closes it:
//!
//!   1. a `Suspect` or `Dead`-but-not-reaped granter stays in the min-set (only
//!      a full reap removes it) — the guard with a bill attached, priced below;
//!   2. a booting **writer** refuses to resolve on an empty wait set, or to
//!      excuse an unseen [`CAP_LEASE`] advertiser
//!      ([`Leases::invalidated_coherently`]);
//!   3. a booting **reader** cannot reach [`LeaseState::Serving`] at all —
//!      [`LeaseView::mark_caught_up`] declines to take and no serve deadline is
//!      published — which closes the vacuous-confirmation hole an unlearned
//!      roster would otherwise open ([`LeaseCore::set_roster`]).
//!
//!   Both boot guards are **enforced in the shell**, not asked of the
//!   deployment, and both run for the node's first `detection_window_ms +
//!   2 × anti_entropy_interval` of participation. That is a convergence bound,
//!   not a lease-duration one: a live reader republishes every
//!   [`LeaseConfig::renew_every`], so the question is only how long membership
//!   and anti-entropy need to deliver an entry that already exists. A booting
//!   reader starts in [`LeaseState::NeedsResync`] for the symmetric reason.
//! * **Ghost echoes over-wait.** The engine's restart recovery re-adopts
//!   un-authored entries from peer echoes, so a departed reader's `~lease`
//!   entry can outlive it in a writer's view, and writers wait for a lease
//!   nobody holds. That costs latency, never correctness — the entry carries a
//!   TTL, so the ghost expires, and the wait ends at the lapse. The grant map
//!   is immune to the mirror-image hazard by construction: it is one wholesale
//!   entry, so a granter's first republish after a restart authors over its
//!   whole previous life rather than leaving retired grants to haunt the
//!   group.
//! * **Every failure degrades to origin-serving, never to stale-serving.** A
//!   lost renewal, an undecodable entry, a granter that goes silent, a
//!   confirmation older than the reader tracks, a partition, a clock that
//!   stops: every one of them removes or freezes a confirmation, which
//!   shortens or closes the serve window, which sends the reader to the
//!   origin. There is no failure path in this tier whose effect is a longer
//!   window than the granters actually gave.
//!
//! ## Two availability failure modes, priced
//!
//! Both keep the guarantee above intact and take service away instead. Both are
//! worth knowing before an incident rather than during one.
//!
//! * **The fail-slow reader: renewing but behind.** A node whose renewal ticker
//!   runs while its apply loop does not — a stuck consumer, an
//!   [`AckLedger`](crate::AckLedger) that was never wired up, a partition that
//!   carries gossip but not writes — is the participant this tier cannot bound.
//!   Its lease never lapses (it is renewing it) and its watermark never reaches
//!   the write, so *every* coherent write that overlaps it waits out the
//!   caller's whole `timeout` and returns [`CoherenceOutcome::TimedOut`] naming
//!   it in `waiting_on`. Raising the deadline cannot help: there is no instant
//!   at which either excuse arrives. The remedy is operational — stop its
//!   renewals. Drop its [`Leases`] and every blocked write resolves on the lapse
//!   path one lease duration later, or call [`Leases::leave`] for the immediate
//!   retraction. `waiting_on` names the node to do it to.
//! * **One unreaped granter freezes every reader.** Confirmation is a min over
//!   the *whole* roster and only a **reap** removes a member from it, so a
//!   single `CAP_LEASE` member that stops publishing grants (crashed, hung,
//!   partitioned) freezes every other reader's confirmation cluster-wide. At the
//!   defaults that is: every reader's window closes within one `D` (2 s) of the
//!   freeze, and no reader can reopen one until membership reaps the silent
//!   member at the **reap horizon**, `2 × dead_timeout_ms` (20 s) past the
//!   instant it was declared `Dead` — itself up to `detection_window_ms`
//!   (0.9 s in a group of three) past the silence. One dead member therefore
//!   costs on the order of
//!   `detection_window_ms + 2 × dead_timeout_ms − D` ≈ **19 s of cluster-wide
//!   origin-serving**, and the reads are correct throughout. The default
//!   `dead_timeout_ms` is the *safe* number, not the available one: a
//!   lease deployment wants it on the order of its own `D` (with `D = 2 s`, a
//!   2 s `dead_timeout_ms` turns that 19 s into ≈ 3 s), bounded below by the
//!   longest partition the deployment must survive and still reconcile — the
//!   reap horizon is also what makes a returning node's entries recoverable.
//!
//! ## What it costs to run
//!
//! In a group of `N` participants, per lease set:
//!
//! * **Renewals** are the cheap half and the one the sketch advertises: one
//!   16-byte entry per reader per [`LeaseConfig::renew_every`], riding the
//!   gossip cadence that already exists.
//! * **Grants are not.** A granter re-folds and republishes its *whole*
//!   `~lease:g` map on every peer renewal it adopts, so each member authors up
//!   to `N − 1` rewrites of an `O(N)`-entry value per renewal interval —
//!   `O(N²)` bytes per member per interval, before dissemination charges its own
//!   fanout. The granter's byte-equality check suppresses only genuinely
//!   identical re-folds (membership churn, backstop ticks); it cannot suppress
//!   the renewal-driven ones, because a peer's sequence number has moved.
//! * **The view fold is charged to write traffic, not lease traffic.** It runs
//!   on *every* `NodeStateChanged` this node observes — deliberately unfiltered
//!   by key, because the roster derives from a capability entry this crate does
//!   not name — so an [`AckLedger`](crate::AckLedger) republishing a watermark
//!   per applied write wakes it too, and each turn re-decodes every granter's
//!   map (`O(N²)` decode work in the worst case) for one deduplicated `watch`
//!   publish. Cheap per turn; the turn *count* is what scales.
//!
//! Both of the last two are super-linear in `N`. This tier belongs on the same
//! size of cluster the ack tier does (see the README's scaling envelope), and
//! the knob that buys the most headroom is [`LeaseConfig::renew_every`].
//!
//! # How the pieces fit
//!
//! [`LeaseCore`], [`CoherenceCore`] and the codecs are the sans-IO rules;
//! [`Leases`] is the tokio shell that gives them a clock, group entries and
//! three background tasks (renew, grant, ingest), and [`LeaseView`] is the
//! cheap read handle a request path holds. A node participates by constructing
//! one [`Leases`] per lease set, advertising [`CAP_LEASE`], and calling
//! [`Leases::invalidated_coherently`] after each write it must not be stale
//! behind.

mod coherence;
mod core;
mod shell;
mod tasks;
mod wire;

use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use groupnet_core::NodeId;

pub use self::coherence::{CoherenceCore, CoherenceStep, WaitMember};
pub use self::core::{ClockMs, LeaseCore};
pub use self::shell::{LeaseView, Leases};
pub use self::wire::{
    GrantMap, RenewalId, decode_grants, decode_renewal, encode_grants, encode_renewal,
    grant_entry_key, renewal_entry_key,
};

/// The capability a node advertises (via
/// [`Group::advertise_capabilities`](groupnet_runtime::Group::advertise_capabilities))
/// to declare that it participates in the coherence-lease tier: that it grants
/// readers' leases, and that it blocks its own coherent writes on them.
///
/// Readers wait for a confirmation from **every** not-reaped member
/// advertising this, so the advertisement is load-bearing in both directions —
/// and it carries the same rolling-upgrade footgun the ack tier documents: a
/// node that participates but has not advertised yet is invisible to readers'
/// rosters and is not waited for. Advertise fleet-wide first, confirm the
/// advertisements have landed
/// ([`Group::members_with_capability`](groupnet_runtime::Group::members_with_capability)),
/// and only then let readers start serving under leases.
pub const CAP_LEASE: &str = "leases";

/// The tuning of one lease set: how long a lease lasts, how often it is
/// renewed, and how much of it the reader gives back for clock-rate skew.
///
/// `duration` is the knob that trades write-stall-under-failure against
/// renewal traffic: a writer whose peer goes silent stalls for at most one
/// lease remainder, and the reader republishes an entry every `renew_every`
/// to keep it. Renewals ride the existing gossip cadence, so the traffic is
/// one small entry per reader per `renew_every`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseConfig {
    /// How long a granted serve-lease lasts (`D`). The writer's worst-case
    /// stall on a silent peer, and the reader's window between confirmations.
    pub duration: Duration,
    /// How often the reader republishes its renewal. Must be well inside
    /// `duration` — the default is `duration / 3`, so two consecutive lost
    /// renewals still leave the lease standing.
    pub renew_every: Duration,
    /// Reader-side safety margin subtracted from every serve window, for
    /// clock-*rate* skew between the reader and its granters. Defaults to
    /// `max(duration / 100, 5ms)`; see the honesty box on what it does and
    /// does not buy.
    pub rate_margin: Duration,
}

/// Why a [`LeaseConfig`] cannot be honoured as written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseConfigError {
    /// `duration` rounds to zero milliseconds. Zero is the engine's
    /// "never expires" TTL — a lease that never expires is precisely the stale
    /// claim this tier exists to prevent — so it is clamped to 1 ms, which is
    /// certainly not what the caller meant.
    DurationTooShort,
    /// `renew_every` is zero (a spinning ticker) or past `duration / 2`, which
    /// leaves no room for a single lost renewal.
    RenewalCadence,
    /// `rate_margin` is at or past `duration`: no serve window can ever open,
    /// so the reader would never serve. Fail-closed rather than clamped — the
    /// margin is a safety number and this layer must not quietly shrink it.
    MarginTooLarge,
}

impl fmt::Display for LeaseConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            LeaseConfigError::DurationTooShort => "lease duration rounds to zero milliseconds",
            LeaseConfigError::RenewalCadence => {
                "renew_every must be non-zero and at most half the lease duration"
            }
            LeaseConfigError::MarginTooLarge => "rate_margin leaves no serve window",
        })
    }
}

impl std::error::Error for LeaseConfigError {}

impl Default for LeaseConfig {
    /// A two-second lease renewed every ~666 ms with a 20 ms margin — the same
    /// order as the Hosted-mode host lease, and a sane starting point for a
    /// cache cluster on one network.
    fn default() -> Self {
        Self::for_duration(Duration::from_secs(2))
    }
}

impl LeaseConfig {
    /// The derived tuning for a lease of `duration`: renewed every
    /// `duration / 3`, with a margin of `max(duration / 100, 5ms)`.
    #[must_use]
    pub fn for_duration(duration: Duration) -> Self {
        Self {
            duration,
            renew_every: duration / 3,
            rate_margin: (duration / 100).max(Duration::from_millis(5)),
        }
    }

    /// Whether this configuration is inside the envelope the tier can honour.
    ///
    /// # Errors
    /// [`LeaseConfigError`], one variant per way it is not — each of which
    /// still *runs*, in the fail-closed direction (see the variants).
    pub fn validate(&self) -> Result<(), LeaseConfigError> {
        if self.duration.as_millis() == 0 {
            return Err(LeaseConfigError::DurationTooShort);
        }
        if self.renew_every.is_zero() || self.renew_every * 2 > self.duration {
            return Err(LeaseConfigError::RenewalCadence);
        }
        if self.rate_margin >= self.duration {
            return Err(LeaseConfigError::MarginTooLarge);
        }
        Ok(())
    }

    /// The lease duration in milliseconds, never zero — a zero TTL is the
    /// engine's "never expires".
    #[must_use]
    pub fn duration_ms(&self) -> u64 {
        clamp_ms(self.duration).max(1)
    }

    /// The renewal interval in milliseconds, never zero.
    #[must_use]
    pub fn renew_every_ms(&self) -> u64 {
        clamp_ms(self.renew_every).max(1)
    }

    /// The rate margin in milliseconds. Zero is allowed and means "these
    /// clocks are rate-locked" — see the honesty box.
    #[must_use]
    pub fn rate_margin_ms(&self) -> u64 {
        clamp_ms(self.rate_margin)
    }
}

/// A [`Duration`] as whole milliseconds, saturating.
fn clamp_ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// Whether a reader may serve cached state, and why not when it may not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LeaseState {
    /// A confirmed lease covers this instant and the consumer is
    /// caught up: cached state may be served, including authoritative
    /// negatives.
    Serving,
    /// The serve window ran out — reported **once** per lapse, as an alarm
    /// edge. Serving must stop immediately.
    Lapsed,
    /// Not serving until the consumer re-synchronizes and affirms it
    /// ([`LeaseCore::mark_caught_up`]). Entered at boot and after every lapse,
    /// and *not* left by a fresh lease alone: a lapsed reader missed exactly
    /// the invalidations whose writers proceeded because it had lapsed.
    NeedsResync,
}

/// How a coherent write ended.
///
/// The first two are the guarantee; the third is the only outcome that is not
/// (and only a caller's own deadline can produce it).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoherenceOutcome {
    /// Every lease-holder applied the write — the fast path.
    AllApplied,
    /// Some members never acknowledged, but the writer's engine has expired
    /// their serve-leases: they are out of service until they re-synchronize,
    /// so they cannot serve state this write invalidated.
    LeaseLapsed {
        /// The members excused by lapse rather than acknowledgement.
        stragglers: Vec<NodeId>,
    },
    /// The caller's deadline passed while lease-holders were still live and
    /// still behind. **No coherence guarantee holds**: these members may be
    /// serving state this write invalidated. Either the deadline was shorter
    /// than the lease duration (raise it past `duration` and this outcome
    /// cannot occur) or something is badly wrong.
    TimedOut {
        /// The members still being waited on when the deadline passed.
        waiting_on: Vec<NodeId>,
    },
}

impl CoherenceOutcome {
    /// Whether the tier's guarantee holds for this write: true for
    /// [`AllApplied`](Self::AllApplied) and [`LeaseLapsed`](Self::LeaseLapsed),
    /// false for [`TimedOut`](Self::TimedOut).
    #[must_use]
    pub fn is_coherent(&self) -> bool {
        !matches!(self, CoherenceOutcome::TimedOut { .. })
    }
}

/// The wall clock as a lease epoch, mirroring
/// [`WriteFeed`](crate::WriteFeed): strictly increasing across restarts unless
/// the clock steps backwards.
fn wall_clock_epoch() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{CoherenceOutcome, LeaseConfig, LeaseConfigError};
    use groupnet_core::NodeId;

    #[test]
    fn the_default_config_is_inside_its_own_envelope() {
        let cfg = LeaseConfig::default();
        assert_eq!(cfg.validate(), Ok(()));
        assert_eq!(cfg.duration_ms(), 2_000);
        assert_eq!(cfg.renew_every_ms(), 666, "three renewals per lease");
        assert_eq!(cfg.rate_margin_ms(), 20, "max(D/100, 5ms)");
    }

    #[test]
    fn a_short_lease_still_gets_a_five_millisecond_floor_on_the_margin() {
        let cfg = LeaseConfig::for_duration(Duration::from_millis(100));
        assert_eq!(cfg.rate_margin_ms(), 5, "the floor, not D/100 = 1ms");
        assert_eq!(cfg.renew_every_ms(), 33);
        assert_eq!(cfg.validate(), Ok(()));
    }

    #[test]
    fn validation_names_each_way_a_config_is_unhonourable() {
        let sub_ms = LeaseConfig::for_duration(Duration::from_micros(500));
        assert_eq!(sub_ms.validate(), Err(LeaseConfigError::DurationTooShort));
        // …and the duration the core actually uses is clamped off zero, which
        // is the engine's "never expires".
        assert_eq!(sub_ms.duration_ms(), 1);

        let lazy = LeaseConfig {
            renew_every: Duration::from_millis(1_500),
            ..LeaseConfig::default()
        };
        assert_eq!(lazy.validate(), Err(LeaseConfigError::RenewalCadence));
        let spinning = LeaseConfig {
            renew_every: Duration::ZERO,
            ..LeaseConfig::default()
        };
        assert_eq!(spinning.validate(), Err(LeaseConfigError::RenewalCadence));
        assert_eq!(spinning.renew_every_ms(), 1, "clamped off a spin");

        let paranoid = LeaseConfig {
            rate_margin: Duration::from_secs(2),
            ..LeaseConfig::default()
        };
        assert_eq!(paranoid.validate(), Err(LeaseConfigError::MarginTooLarge));
    }

    #[test]
    fn only_a_timeout_breaks_the_coherence_guarantee() {
        assert!(CoherenceOutcome::AllApplied.is_coherent());
        assert!(
            CoherenceOutcome::LeaseLapsed {
                stragglers: vec![NodeId::new("a")],
            }
            .is_coherent()
        );
        assert!(
            !CoherenceOutcome::TimedOut {
                waiting_on: vec![NodeId::new("a")],
            }
            .is_coherent()
        );
    }
}
