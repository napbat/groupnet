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
//!   semantics, no TTL, plus a row under its own id naming the life that
//!   authored it ([`GranterLife`]). That is a granter saying "I have seen this
//!   reader's lease and I will wait for it". A granter grants a reader that
//!   has not applied one of its outstanding writes no renewal it first saw
//!   after that write began.
//! * **Serve.** The reader may serve iff `now < s_i + duration - rate_margin`
//!   for the newest renewal `i` confirmed by *every* granter in its roster —
//!   and it is not in [`LeaseState::NeedsResync`]. The roster only grows: a
//!   granter leaves it only after it has departed and membership has
//!   forgotten it.
//! * **Invalidate.** A writer's coherent write waits, per reader that may be
//!   serving (a live `~lease` entry, or a grant from this writer within the
//!   last `duration`), for either an applied ack at or past the write's
//!   [`WriteToken`](crate::WriteToken) (the T2 fast path) or a **proven
//!   lapse**: the reader's current life has shown it counts this writer, and
//!   one `duration` has passed on the writer's clock since it first saw the
//!   renewal behind its newest grant to that reader (the slow path).
//! * **Resync.** A reader that lapsed enters [`LeaseState::NeedsResync`] and
//!   stays there — a freshly confirmed lease is *not* enough — until its
//!   consumer has re-synchronized and affirmed it. This is the correctness
//!   rule of the whole tier: a lapsed reader missed exactly the invalidations
//!   whose writers proceeded *because* it had lapsed.
//!
//! The sans-IO halves are [`LeaseCore`] (reader), [`GrantLedger`] (granter)
//! and [`CoherenceCore`] (writer); the tokio shell around them is [`Leases`] /
//! [`LeaseView`].
//!
//! # Honesty box: what this guarantees, and where it stops
//!
//! **The guarantee.** While a reader's [`LeaseView::valid`] answers `true`, no
//! completed write of a participating writer that the reader has ever counted
//! is invisible to it: the writer either waited for this node to apply the
//! invalidation, or it waited until this node's serve-lease had provably run
//! out — and a lapsed node serves nothing cached until its consumer
//! re-synchronizes.
//!
//! *Why the lapse is proven.* Let `w` begin at `T` on writer `W`, and let `R`
//! never acknowledge it. Every grant `W` publishes to `R` while `w` is in
//! flight confirms a renewal `W` first saw no later than `T` (the cap), and
//! every earlier grant confirms one first seen before `T`; a renewal is first
//! seen after its reader recorded `s_i`. `R` counts `W` (its current life's map
//! granted `W`'s current renewal, and a roster only grows), so `R`'s window is
//! bounded by `W`'s grants: it closes by `s_i + D - rate_margin` on `R`'s clock,
//! which is no later than first-seen `+ D` on `W`'s clock within the rate bound
//! below. `W` excuses `R` only once its own clock passes that instant, so `R`
//! has lapsed into `NeedsResync`, and it serves again only on a grant `W` makes
//! after `w` is no longer in flight. Membership does not enter the argument:
//! a reap, a deleted entry or a suspicion changes who `W` *sees*, never when
//! `R`'s window closes.
//!
//! Write-wait under failure is therefore `min(acks, one lease duration)` with a
//! real guarantee at the end — including for a holder that keeps **renewing**
//! while it stops **applying**, which the cap turns into a lapse. What remains
//! for [`CoherenceOutcome::TimedOut`] is a reader whose current life has not
//! shown it counts the writer, a writer still inside its warm-up window, and a
//! writer that has departed: none of them can be excused by lapse, so only
//! acknowledgements end their waits, and the writer knows (and says so in
//! `waiting_on`).
//!
//! It rests on four assumptions, each of which is a failure mode you should
//! know by name:
//!
//! * **Bounded clock *rate* skew — not bounded connectivity.** The reader's
//!   window is computed on its own clock and the writer's excuse on its own.
//!   If a reader's clock runs slow relative to a writer's by more than
//!   [`LeaseConfig::rate_margin`] over one lease duration, the reader can
//!   still believe it holds a lease the writer has already excused. This is
//!   an assumption about *rates* (a few hundred ppm on any healthy host), not
//!   about steps: a wall-clock jump cannot affect it, because every instant
//!   here comes from a monotonic clock. Size `rate_margin` for the worst
//!   drift you accept, not for the typical one.
//! * **A reader counts every writer it has learned.** Its roster is every
//!   [`CAP_LEASE`] advertiser it has known and every reader it has granted,
//!   and a granter leaves it only after declaring its life departed
//!   ([`Leases::leave`]) and being forgotten by membership. An asymmetric
//!   partition that outlives the reap horizon therefore freezes the reader
//!   instead of letting it serve while a live writer stops waiting for it,
//!   and a writer excuses by lapse only readers that showed, in their own map,
//!   that they count it. Two guards cover a node that is still learning:
//!
//!   1. a booting **writer** refuses to resolve on an empty wait set, or to
//!      excuse an unseen [`CAP_LEASE`] advertiser
//!      ([`Leases::invalidated_coherently`]);
//!   2. a booting **reader** cannot reach [`LeaseState::Serving`] at all —
//!      [`LeaseView::mark_caught_up`] declines to take and no serve deadline is
//!      published — which closes the vacuous-confirmation hole an unlearned
//!      roster would otherwise open ([`LeaseCore::set_roster`]).
//!
//!   Both run for the node's first `detection_window_ms + 2 ×
//!   anti_entropy_interval` of participation. The residual is a reader that
//!   boots into a *full* partition from a writer and stays there past that
//!   window: it never learns the writer, the writer never sees its lease, and
//!   nothing ties the two together. A deployment with a fixed membership closes
//!   it by not serving locally until its known peers have granted.
//! * **Ghost echoes over-wait.** The engine's restart recovery re-adopts
//!   un-authored entries from peer echoes, so a departed reader's `~lease`
//!   entry can outlive it in a writer's view, and writers wait for a lease
//!   nobody holds. That costs latency, never correctness — the ghost is
//!   excused like any silent reader. The grant map is immune to the
//!   mirror-image hazard by construction: it is one wholesale entry, so a
//!   granter's first republish after a restart authors over its whole
//!   previous life rather than leaving retired grants to haunt the group.
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
//!   carries gossip but not writes — acknowledges nothing, so every coherent
//!   write that overlaps it holds its grant back and ends on its lapse one
//!   lease duration after the write began. The writes stay coherent and slow
//!   by `D`, and the node spends its time out of service, re-synchronizing.
//!   The remedy is operational — fix its apply loop, or call
//!   [`Leases::leave`] on it.
//! * **A silent granter freezes every reader until it grants again.**
//!   Confirmation is a min over the *whole* roster, and a granter that stops
//!   publishing grants (crashed, hung, partitioned) stays in it — membership's
//!   reap no longer removes it. Every other reader's window closes within one
//!   `D` of the freeze and stays closed until the granter grants again: after a
//!   heal, or in its next life after a restart (a restarted granter grants
//!   under the same id). A granter that dies without departing and never
//!   returns leaves every reader origin-serving until the reader itself
//!   restarts, because only a fresh life learns a roster without it. Reads stay
//!   correct throughout; the remedy is to bring the granter back, or to stop it
//!   with [`Leases::leave`] rather than letting it die.
//!
//! ## What it costs to run
//!
//! In a group of `N` participants, per lease set:
//!
//! * **Renewals** are the cheap half and the one the sketch advertises: one
//!   16-byte entry per reader per [`LeaseConfig::renew_every`], riding the
//!   gossip cadence that already exists.
//! * **Grants are not.** A granter re-folds and republishes its *whole*
//!   `~lease:g` map on every peer renewal it adopts (and when one of its
//!   writes ends), so each member authors up to `N − 1` rewrites of an
//!   `O(N)`-entry value per renewal interval — `O(N²)` bytes per member per
//!   interval, before dissemination charges its own fanout. The granter's
//!   byte-equality check suppresses only genuinely identical re-folds
//!   (membership churn, backstop ticks); it cannot suppress the renewal-driven
//!   ones, because a peer's sequence number has moved.
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
//! [`LeaseCore`], [`GrantLedger`], [`CoherenceCore`] and the codecs are the
//! sans-IO rules; [`Leases`] is the tokio shell that gives them a clock, group
//! entries and three background tasks (renew, grant, ingest), and
//! [`LeaseView`] is the cheap read handle a request path holds. A node
//! participates by constructing one [`Leases`] per lease set, advertising
//! [`CAP_LEASE`], and calling [`Leases::invalidated_coherently`] after each
//! write it must not be stale behind.

mod coherence;
mod core;
mod grants;
mod shell;
mod tasks;
mod wire;

use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use groupnet_core::NodeId;

pub use self::coherence::{CoherenceCore, CoherenceStep, WaitMember};
pub use self::core::{ClockMs, LeaseCore};
pub use self::grants::GrantLedger;
pub use self::shell::{LeaseView, Leases};
pub use self::wire::{
    GrantMap, GranterLife, RenewalId, decode_grants, decode_renewal, encode_grants, encode_renewal,
    grant_entry_key, renewal_entry_key,
};

/// The capability a node advertises (via
/// [`Group::advertise_capabilities`](groupnet_runtime::Group::advertise_capabilities))
/// to declare that it participates in the coherence-lease tier: that it grants
/// readers' leases, and that it blocks its own coherent writes on them.
///
/// Readers wait for a confirmation from **every** not-reaped member
/// advertising this, so the advertisement is load-bearing in both directions —
/// and it carries the same advertisement-lag footgun the ack tier documents: a
/// node that participates but whose advertisement has not landed yet is
/// invisible to readers' rosters and is not waited for. Advertise on every
/// participant first, confirm the
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
    /// Every reader that may have been serving applied the write — the fast
    /// path.
    AllApplied,
    /// Some readers never acknowledged, but each had provably lost its right
    /// to serve — its lease ran out on the writer's clock while it counted the
    /// writer, or its life departed: they are out of service until they
    /// re-synchronize, so they cannot serve state this write invalidated.
    LeaseLapsed {
        /// The members excused by lapse rather than acknowledgement.
        stragglers: Vec<NodeId>,
    },
    /// The caller's deadline passed while readers that may be serving were
    /// still behind. **No coherence guarantee holds**: these members may be
    /// serving state this write invalidated, and the write must not be
    /// reported to its client as coherent. With a deadline past `duration`
    /// this is a reader that has not shown it counts the writer, a writer in
    /// its warm-up window, or a writer that has departed — see
    /// [`Leases::invalidated_coherently`].
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
