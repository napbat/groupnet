//! The reader's sans-IO half of the coherence-lease tier: [`LeaseCore`], a
//! node's own right to serve. (The writer's half is
//! [`CoherenceCore`](super::CoherenceCore), next door.)
//!
//! Neither reads a clock, a socket, or tokio. Instants arrive as
//! [`ClockMs`], the group's state arrives as snapshots, and the answer comes
//! back as a verdict — so the deterministic simulator drives exactly the same
//! logic the tokio shell does, and every rule below is provable in virtual
//! time.
//!
//! # Why milliseconds and not `Instant`
//!
//! [`ClockMs`] is a plain `u64` of milliseconds on whatever monotonic clock the
//! caller feeds in, mirroring [`groupnet_core::Time`] — the same choice the
//! engine makes, and for the same reason: a simulator can fabricate and replay
//! integers, and cannot fabricate an [`Instant`](std::time::Instant). A
//! generic instant parameter would have bought the tokio shell nothing (it
//! holds one process-start `Instant` and subtracts) while forcing an ordered
//! arithmetic trait bound through every signature here and every DST fixture.
//! The unit is milliseconds because the entry TTL the lease rides on is
//! `ttl_ms`: comparing anything finer would compare against a resolution the
//! wire does not carry.

use std::collections::{BTreeMap, BTreeSet};

use groupnet_core::NodeId;

use super::wire::{GrantMap, GranterLife, RenewalId};
use super::{LeaseConfig, LeaseState};

/// How many publish instants one reader remembers. Renewals are `duration/3`
/// apart by default, so this is hundreds of lease durations of history: a
/// confirmation older than that describes a lease that lapsed long ago, and
/// [`LeaseCore::serve_until`] fails **closed** when the instant it would need
/// has been dropped.
const MAX_TRACKED_RENEWALS: usize = 256;

/// A monotonic instant in milliseconds, on whatever clock the caller feeds in.
///
/// The tokio shell subtracts one process-start
/// [`Instant`](std::time::Instant); the deterministic simulator passes virtual
/// milliseconds. Both are monotonic, which is the only property this type
/// asks for — see the module docs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClockMs(pub u64);

impl ClockMs {
    /// The zero instant.
    pub const ZERO: ClockMs = ClockMs(0);

    /// `self + ms`, saturating at [`u64::MAX`].
    #[must_use]
    pub fn saturating_add_ms(self, ms: u64) -> ClockMs {
        ClockMs(self.0.saturating_add(ms))
    }

    /// `self - ms`, saturating at zero.
    #[must_use]
    pub fn saturating_sub_ms(self, ms: u64) -> ClockMs {
        ClockMs(self.0.saturating_sub(ms))
    }

    /// Milliseconds from `earlier` to `self`, clamped at zero.
    #[must_use]
    pub fn since(self, earlier: ClockMs) -> u64 {
        self.0.saturating_sub(earlier.0)
    }
}

impl From<groupnet_core::Time> for ClockMs {
    fn from(time: groupnet_core::Time) -> Self {
        ClockMs(time.0)
    }
}

/// The reader's half: what this node has published, what the group has
/// confirmed, and therefore whether it may serve cached state right now.
///
/// One instance per lease set. Feed it:
///
/// * every renewal *before* the write is enqueued
///   ([`on_renew`](Self::on_renew) — the instant must be taken first, so the
///   window it opens can only be shorter than the one the granters' TTLs
///   actually arm);
/// * the roster of granters that must confirm ([`set_roster`](Self::set_roster));
/// * each granter's advertised grant map
///   ([`observe_grant_map`](Self::observe_grant_map)).
///
/// Then ask [`valid`](Self::valid) before serving anything cached.
///
/// # The rule it enforces
///
/// This node may serve iff `now < s_i + duration - rate_margin`, where `i` is
/// the newest renewal confirmed by **every** granter in the roster and `s_i`
/// is the instant recorded before renewal `i` was enqueued — *and* the node is
/// not in [`LeaseState::NeedsResync`]. Every granter's own copy of renewal `i`
/// expires no earlier than `s_i + duration` (the engine arms the TTL when it
/// adopts the entry, which is after `s_i`), so this window closes before the
/// first granter stops honouring the lease, by at least `rate_margin`.
#[derive(Debug)]
pub struct LeaseCore {
    /// This node's id — the key its renewals occupy in a granter's map.
    me: NodeId,
    /// This lease life's epoch (the reader's boot epoch).
    epoch: u64,
    /// `duration` in milliseconds, never zero.
    duration_ms: u64,
    /// `rate_margin` in milliseconds.
    margin_ms: u64,
    /// The sequence number the next renewal will carry.
    next_seq: u64,
    /// `seq -> s_i`, the instant recorded before renewal `seq` was enqueued.
    published: BTreeMap<u64, ClockMs>,
    /// `granter -> newest seq of *this* epoch that granter has adopted`.
    /// Absent means "confirms nothing", which freezes confirmation entirely.
    granters: BTreeMap<NodeId, u64>,
    /// The granters that must confirm. It only grows — every advertiser this
    /// node has learned and every reader it has granted joins it — and a
    /// granter leaves it only once its map says its life departed (see
    /// [`set_roster`](Self::set_roster)). Never this node itself.
    roster: BTreeSet<NodeId>,
    /// The last [`GranterLife`] each granter's map carried — the departure
    /// flag [`set_roster`](Self::set_roster) needs. A departed life is kept
    /// while membership still advertises it, so the advertisement cannot
    /// re-admit it.
    lives: BTreeMap<NodeId, GranterLife>,
    /// Whether the consumer has affirmed catch-up since the last lapse.
    caught_up: bool,
    /// Whether this life has departed the lease set ([`depart`](Self::depart)):
    /// it never serves again.
    departed: bool,
    /// How many times this core has lapsed — an alarm counter.
    lapses: u64,
}

impl LeaseCore {
    /// A reader core for `me`, leasing under `cfg`, in lease life `epoch`.
    ///
    /// `epoch` must be strictly increasing across this node's restarts — the
    /// wall clock at boot is the default the shell uses, exactly like
    /// [`WriteFeed`](crate::WriteFeed). Grants recorded against another epoch
    /// never confirm, so a stale grant from a previous life cannot open a
    /// window in this one.
    #[must_use]
    pub fn new(me: NodeId, cfg: &LeaseConfig, epoch: u64) -> Self {
        Self {
            me,
            epoch,
            // Zero would be the engine's "never expires" TTL — the footgun the
            // whole tier exists to avoid. `LeaseConfig` clamps it too; this is
            // the belt to that pair of braces.
            duration_ms: cfg.duration_ms().max(1),
            margin_ms: cfg.rate_margin_ms(),
            next_seq: 1,
            published: BTreeMap::new(),
            granters: BTreeMap::new(),
            roster: BTreeSet::new(),
            lives: BTreeMap::new(),
            caught_up: false,
            departed: false,
            lapses: 0,
        }
    }

    /// This lease life's epoch.
    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// How many times this core has lapsed. A rising count means renewals are
    /// not reaching some granter (or the roster contains a member that never
    /// confirms) — the reader is falling back to the origin that often.
    #[must_use]
    pub fn lapses(&self) -> u64 {
        self.lapses
    }

    /// Records that a renewal is about to be published at `at`, returning the
    /// [`RenewalId`] to put on the wire.
    ///
    /// **Call this before enqueueing the write.** The instant is the lower
    /// bound of every granter's TTL arming, and taking it afterwards would
    /// claim a window that starts later than the one the granters gave.
    ///
    /// `at` must be monotonically non-decreasing across calls; a regression is
    /// recorded verbatim and can only shorten the resulting window.
    pub fn on_renew(&mut self, at: ClockMs) -> RenewalId {
        let seq = self.next_seq;
        self.next_seq = self.next_seq.saturating_add(1);
        self.published.insert(seq, at);
        while self.published.len() > MAX_TRACKED_RENEWALS {
            let Some(oldest) = self.published.keys().next().copied() else {
                break;
            };
            self.published.remove(&oldest);
        }
        RenewalId {
            epoch: self.epoch,
            seq,
        }
    }

    /// The newest renewal this node has published, or `None` before the first.
    #[must_use]
    pub fn last_renewal(&self) -> Option<RenewalId> {
        Some(RenewalId {
            epoch: self.epoch,
            seq: *self.published.keys().next_back()?,
        })
    }

    /// Folds in the granters that must confirm before this node may serve:
    /// every known not-reaped member advertising
    /// [`CAP_LEASE`](super::CAP_LEASE). This node is filtered out — a reader
    /// never waits on itself, because a *local* write invalidates local state
    /// before it returns.
    ///
    /// # Roster rules (the safety hinge)
    ///
    /// The roster **only grows**. A granter that this core has once counted —
    /// passed here, or [`pin`](Self::pin)ned because this node granted it —
    /// stays in the min-set when membership suspects it, declares it `Dead`,
    /// or reaps it. A reap is one observer's verdict after a silence, and an
    /// asymmetric partition that outlives the reap horizon reaps a granter that
    /// is still writing: dropping it would let this node serve while a live
    /// writer no longer waits for it, and a writer that has seen this node
    /// count it ([`GrantLedger`](super::GrantLedger)) relies on that never
    /// happening. A silent granter therefore freezes confirmation until it
    /// grants again — after a heal, or in its next life after a restart.
    ///
    /// The one way out is **departure**. A granter whose last map declared
    /// its life departed ([`Leases::leave`](super::Leases::leave)) is dropped
    /// at once, even while membership still lists it: that life excuses no
    /// reader by lapse, so no write of its — drained or not — can complete
    /// without this node's acknowledgement, and counting it longer would only
    /// freeze confirmation until the reap. Its advertisement does not
    /// re-admit it; its next life does, by publishing a live map or by this
    /// node granting it ([`pin`](Self::pin)). Dropping one is reported as a
    /// lapse — the edge [`poll`](Self::poll) would have produced, and the
    /// alarm counter bumps — because a catch-up affirmed while the granter
    /// still froze confirmation may predate writes it completed on this
    /// node's earlier lapse, so the consumer must re-synchronize after the
    /// drop, not reuse that affirmation.
    ///
    /// An **empty** roster confirms vacuously: with nobody to wait on, the
    /// newest published renewal is the confirmed one. That is the right answer
    /// for a node alone in its lease set and the shell refuses it to a node
    /// that has not finished learning its group (the boot guard), but a roster
    /// that never learned a writer at all — a node that booted into a full
    /// partition from it — confirms without it. The honesty box names that
    /// residual.
    pub fn set_roster(&mut self, roster: impl IntoIterator<Item = NodeId>) {
        let advertised: BTreeSet<NodeId> =
            roster.into_iter().filter(|node| *node != self.me).collect();
        let lives = &self.lives;
        let departed = |granter: &NodeId| lives.get(granter).is_some_and(|life| life.departed);
        self.roster
            .extend(advertised.iter().filter(|node| !departed(node)).cloned());
        let dropped: Vec<NodeId> = self
            .roster
            .iter()
            .filter(|granter| departed(granter))
            .cloned()
            .collect();
        for granter in &dropped {
            self.roster.remove(granter);
            self.granters.remove(granter);
        }
        let roster = &self.roster;
        self.lives
            .retain(|node, _| roster.contains(node) || advertised.contains(node));
        if dropped.is_empty() {
            return;
        }
        self.caught_up = false;
        self.lapses += 1;
    }

    /// Counts `granter` from now on, as [`set_roster`](Self::set_roster) would —
    /// called by this node's granter **before** it publishes a map granting
    /// `granter`'s renewal of lease life `life`, because that grant is what a
    /// writer reads as proof that this node counts it.
    ///
    /// A grant to a life whose map already declared it departed counts nothing:
    /// that life excuses no reader by lapse. A grant to any other life of a
    /// departed granter is its next life, which is counted from here on.
    pub fn pin(&mut self, granter: &NodeId, life: u64) {
        if *granter == self.me {
            return;
        }
        if let Some(known) = self.lives.get(granter).filter(|known| known.departed) {
            if known.epoch == life {
                return;
            }
            self.lives.remove(granter);
        }
        self.roster.insert(granter.clone());
    }

    /// The granters that must confirm, in id order.
    pub fn roster(&self) -> impl Iterator<Item = &NodeId> {
        self.roster.iter()
    }

    /// Folds one granter's advertised grant map into this core.
    ///
    /// Only a grant carrying **this** lease life's epoch counts: a grant from
    /// a previous life (or, defensively, from a life this node has not lived)
    /// records as *no confirmation at all*, which freezes confirmation until
    /// the granter adopts a renewal of the current epoch. Maps are state, not
    /// a log — a regression is adopted verbatim, and can only shorten the
    /// serve window.
    pub fn observe_grant_map(&mut self, granter: &NodeId, grants: &GrantMap) {
        match grants.get(&self.me) {
            Some(id) if id.epoch == self.epoch => {
                self.granters.insert(granter.clone(), id.seq);
            }
            _ => {
                self.granters.remove(granter);
            }
        }
        match GranterLife::of(granter, grants) {
            Some(life) => self.lives.insert(granter.clone(), life),
            None => self.lives.remove(granter),
        };
    }

    /// Drops what `granter` had confirmed. Purely a memory reclaim, and **not**
    /// a way out of the roster: a counted granter whose confirmation is
    /// forgotten stays in the min-set and freezes it until it grants again.
    pub fn forget_granter(&mut self, granter: &NodeId) {
        self.granters.remove(granter);
    }

    /// The newest renewal `granter` has confirmed, in this lease life.
    #[must_use]
    pub fn confirmed_by(&self, granter: &NodeId) -> Option<RenewalId> {
        Some(RenewalId {
            epoch: self.epoch,
            seq: *self.granters.get(granter)?,
        })
    }

    /// The newest renewal confirmed by **every** granter in the roster — the
    /// min over the min-set — or `None` if any of them confirms nothing (or
    /// nothing has been published yet).
    #[must_use]
    pub fn confirmed(&self) -> Option<RenewalId> {
        Some(RenewalId {
            epoch: self.epoch,
            seq: self.confirmed_seq()?,
        })
    }

    /// The instant this node's right to serve ends: `s_i + duration -
    /// rate_margin` for the confirmed renewal `i`. `None` when nothing is
    /// confirmed — or when the confirmed renewal is so old that its publish
    /// instant is no longer tracked, which fails closed.
    #[must_use]
    pub fn serve_until(&self) -> Option<ClockMs> {
        let published_at = *self.published.get(&self.confirmed_seq()?)?;
        Some(
            published_at
                .saturating_add_ms(self.duration_ms)
                .saturating_sub_ms(self.margin_ms),
        )
    }

    /// The state this core is in at `now`, **without** consuming the lapse
    /// edge — for observability and tests. Serving decisions use
    /// [`valid`](Self::valid).
    #[must_use]
    pub fn peek(&self, now: ClockMs) -> LeaseState {
        if !self.caught_up || self.departed {
            LeaseState::NeedsResync
        } else if self.live_at(now) {
            LeaseState::Serving
        } else {
            LeaseState::Lapsed
        }
    }

    /// Advances the state machine to `now` and reports the state.
    ///
    /// [`LeaseState::Lapsed`] is an **edge**, returned exactly once per lapse:
    /// the lapse clears the catch-up affirmation, so every later poll reads
    /// [`LeaseState::NeedsResync`] until the consumer has re-synchronized and
    /// called [`mark_caught_up`](Self::mark_caught_up). That is the whole
    /// correctness rule of this tier — a reader that lapsed missed exactly the
    /// invalidations whose writers proceeded *because* it had lapsed, so a
    /// freshly confirmed lease alone must not put it back into service.
    pub fn poll(&mut self, now: ClockMs) -> LeaseState {
        let state = self.peek(now);
        if state == LeaseState::Lapsed {
            self.caught_up = false;
            self.lapses += 1;
        }
        state
    }

    /// Whether this node may serve cached state at `now` — the one question
    /// the tier exists to answer. Advances the state machine like
    /// [`poll`](Self::poll).
    pub fn valid(&mut self, now: ClockMs) -> bool {
        self.poll(now) == LeaseState::Serving
    }

    /// Affirms that the consumer has re-synchronized (flushed its cache,
    /// refetched, rebuilt) and may serve again. Returns whether the
    /// affirmation took.
    ///
    /// It takes **only while a lease is live at `now`**, and that is
    /// deliberate: state fetched while the lease was lapsed can already be
    /// stale — a writer that saw this node's lease lapse proceeded without
    /// waiting for it — so affirming from inside the lapse would put exactly
    /// that state into service. Re-synchronize, call this, and check the
    /// return: `false` means the lease is not live yet, so try again once
    /// [`peek`](Self::peek) reports [`LeaseState::NeedsResync`] with a
    /// confirmed lease behind it.
    ///
    /// A flush is the remediation that is trivially catch-up-complete: nothing
    /// cached, nothing stale, and the next fill happens under a live lease.
    pub fn mark_caught_up(&mut self, now: ClockMs) -> bool {
        let live = !self.departed && self.live_at(now);
        if live {
            self.caught_up = true;
        }
        live
    }

    /// Forces [`LeaseState::NeedsResync`] without a lapse — the hook for every
    /// other way a consumer learns its state may have missed invalidations: a
    /// [`PeerWrite::Gap`](crate::PeerWrite::Gap), a failed apply, a cache
    /// rebuild. Serving stops until the consumer affirms catch-up again.
    pub fn require_resync(&mut self) {
        self.caught_up = false;
    }

    /// Ends this life's right to serve for good —
    /// [`Leases::leave`](super::Leases::leave). Unlike
    /// [`require_resync`](Self::require_resync)
    /// no affirmation reopens it, which is what lets a writer excuse a departed
    /// reader at once rather than waiting out its lease.
    pub fn depart(&mut self) {
        self.departed = true;
        self.caught_up = false;
    }

    /// Whether a confirmed lease covers `now`, ignoring the resync state.
    fn live_at(&self, now: ClockMs) -> bool {
        self.serve_until().is_some_and(|until| now < until)
    }

    /// The min over the roster of each granter's newest confirmed sequence,
    /// capped at what this node has actually published.
    fn confirmed_seq(&self) -> Option<u64> {
        let mut confirmed = *self.published.keys().next_back()?;
        for granter in &self.roster {
            confirmed = confirmed.min(*self.granters.get(granter)?);
        }
        Some(confirmed)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use groupnet_core::NodeId;

    use super::{ClockMs, LeaseCore};
    use crate::lease::wire::{GrantMap, GranterLife, RenewalId};
    use crate::lease::{LeaseConfig, LeaseState};

    /// D = 1000 ms, margin = 10 ms — round numbers for readable arithmetic.
    fn cfg() -> LeaseConfig {
        LeaseConfig {
            duration: Duration::from_secs(1),
            renew_every: Duration::from_millis(333),
            rate_margin: Duration::from_millis(10),
        }
    }

    fn node(name: &str) -> NodeId {
        NodeId::new(name)
    }

    /// A reader core with `granters` in its roster, boot epoch 7.
    fn reader(granters: &[&str]) -> LeaseCore {
        let mut core = LeaseCore::new(node("reader"), &cfg(), 7);
        core.set_roster(granters.iter().map(|g| node(g)));
        core
    }

    /// The grant map a granter would publish, confirming `reader` at `id`.
    fn grant(reader: &str, id: RenewalId) -> GrantMap {
        let mut map = GrantMap::new();
        map.insert(node(reader), id);
        map
    }

    /// Publishes a renewal at `at`, confirms it from every granter, and
    /// affirms catch-up — the healthy steady state.
    fn serve_from(core: &mut LeaseCore, at: ClockMs, granters: &[&str]) -> RenewalId {
        let id = core.on_renew(at);
        for granter in granters {
            core.observe_grant_map(&node(granter), &grant("reader", id));
        }
        assert!(core.mark_caught_up(at), "a fresh lease is live at s_i");
        id
    }

    #[test]
    fn a_booting_reader_needs_resync_before_it_may_serve() {
        let mut core = reader(&["a"]);
        assert_eq!(core.peek(ClockMs::ZERO), LeaseState::NeedsResync);
        assert!(!core.valid(ClockMs::ZERO));
        // Even with a perfectly confirmed lease: boot is a lapse it slept
        // through, so the consumer must affirm catch-up first.
        let id = core.on_renew(ClockMs(100));
        core.observe_grant_map(&node("a"), &grant("reader", id));
        assert_eq!(core.serve_until(), Some(ClockMs(1_090)));
        assert_eq!(core.poll(ClockMs(200)), LeaseState::NeedsResync);
        assert!(core.mark_caught_up(ClockMs(200)));
        assert_eq!(core.poll(ClockMs(200)), LeaseState::Serving);
        assert_eq!(core.lapses(), 0, "boot is not a lapse to alarm about");
    }

    #[test]
    fn the_serve_window_is_publish_plus_duration_minus_margin() {
        let mut core = reader(&["a"]);
        serve_from(&mut core, ClockMs(1_000), &["a"]);
        // s_i = 1000, D = 1000, margin = 10.
        assert_eq!(core.serve_until(), Some(ClockMs(1_990)));
        assert!(core.valid(ClockMs(1_989)));
        assert!(!core.valid(ClockMs(1_990)), "the boundary is exclusive");
    }

    #[test]
    fn a_margin_at_or_past_the_duration_never_serves() {
        // Fail-closed rather than clamped: an unsatisfiable margin is a
        // configuration error `LeaseConfig::validate` reports, and the core
        // must not quietly widen it back.
        let paranoid = LeaseConfig {
            duration: Duration::from_millis(100),
            renew_every: Duration::from_millis(30),
            rate_margin: Duration::from_millis(100),
        };
        let mut core = LeaseCore::new(node("reader"), &paranoid, 7);
        core.set_roster(Vec::new());
        let _ = core.on_renew(ClockMs(1_000));
        assert_eq!(core.serve_until(), Some(ClockMs(1_000)));
        assert!(!core.mark_caught_up(ClockMs(1_000)));
        assert!(!core.valid(ClockMs(1_000)));
    }

    #[test]
    fn confirmation_advances_only_when_every_granter_confirms() {
        let mut core = reader(&["a", "b", "c"]);
        let first = core.on_renew(ClockMs(0));
        // Nobody has confirmed: no lease at all.
        assert_eq!(core.confirmed(), None);
        assert_eq!(core.serve_until(), None);
        for granter in ["a", "b"] {
            core.observe_grant_map(&node(granter), &grant("reader", first));
        }
        assert_eq!(core.confirmed(), None, "one granter still silent");
        core.observe_grant_map(&node("c"), &grant("reader", first));
        assert_eq!(core.confirmed(), Some(first));

        // A second renewal that only two granters adopt freezes confirmation
        // at the first — the min over the min-set, not the max.
        let second = core.on_renew(ClockMs(333));
        for granter in ["a", "b"] {
            core.observe_grant_map(&node(granter), &grant("reader", second));
        }
        assert_eq!(core.confirmed(), Some(first), "a silent granter freezes");
        assert_eq!(core.serve_until(), Some(ClockMs(990)), "window from s_1");
        core.observe_grant_map(&node("c"), &grant("reader", second));
        assert_eq!(core.confirmed(), Some(second), "all confirm ⇒ advance");
        assert_eq!(core.serve_until(), Some(ClockMs(1_323)));
    }

    #[test]
    fn a_reaped_granter_stays_in_the_min_set_until_it_grants_again() {
        let mut core = reader(&["a", "quiet"]);
        let id = serve_from(&mut core, ClockMs(0), &["a", "quiet"]);
        let renewed = core.on_renew(ClockMs(300));
        core.observe_grant_map(&node("a"), &grant("reader", renewed));
        assert_eq!(core.confirmed(), Some(id), "the quiet granter freezes it");

        // Reaped by membership — an asymmetric partition that outlived the reap
        // horizon looks exactly like this, while `quiet` may still be writing.
        core.set_roster([node("a")]);
        core.observe_grant_map(&node("quiet"), &GrantMap::new());
        assert_eq!(core.confirmed(), None, "a reap is not a way out");
        assert!(core.roster().any(|granter| *granter == node("quiet")));
        assert_eq!(core.poll(ClockMs(990)), LeaseState::Lapsed);
        assert!(!core.mark_caught_up(ClockMs(1_000)), "frozen, not vacuous");

        // The partition heals and `quiet` grants again: the lease comes back.
        core.observe_grant_map(&node("quiet"), &grant("reader", renewed));
        assert_eq!(core.confirmed(), Some(renewed));
        assert!(core.mark_caught_up(ClockMs(1_000)));
    }

    #[test]
    fn a_granted_reader_is_counted_before_its_advertisement_lands() {
        let mut core = reader(&[]);
        let id = core.on_renew(ClockMs(0));
        assert_eq!(core.confirmed(), Some(id), "an empty roster is vacuous");
        // This node granted `w`'s renewal, which `w` reads as proof that it is
        // counted here: from now on its grants are required.
        core.pin(&node("w"), 1);
        core.pin(&node("reader"), 1);
        assert_eq!(core.confirmed(), None, "pinned, and it has granted nothing");
        assert!(
            core.roster().all(|granter| *granter != node("reader")),
            "a reader never waits on itself"
        );
        core.set_roster(Vec::new());
        assert_eq!(
            core.confirmed(),
            None,
            "still counted with no advertisement"
        );
    }

    #[test]
    fn a_departed_granter_leaves_at_its_farewell_and_that_is_a_lapse() {
        let mut core = reader(&["a", "gone"]);
        let id = serve_from(&mut core, ClockMs(0), &["a", "gone"]);
        let life = |departed| {
            let mut map = grant("reader", id);
            GranterLife { epoch: 3, departed }.stamp(&node("gone"), &mut map);
            map
        };
        core.observe_grant_map(&node("gone"), &life(true));

        // Departed, though membership still lists it (draining its last
        // writes, which end on acknowledgements): it leaves now, and the
        // consumer must re-synchronize after the drop rather than keep
        // serving on it.
        core.set_roster([node("a"), node("gone")]);
        assert!(core.roster().all(|granter| *granter != node("gone")));
        assert_eq!(core.confirmed(), Some(id));
        assert_eq!(core.lapses(), 1, "the drop is an alarm like a lapse");
        assert_eq!(core.poll(ClockMs(100)), LeaseState::NeedsResync);
        assert!(core.mark_caught_up(ClockMs(100)));
        assert_eq!(core.poll(ClockMs(100)), LeaseState::Serving);

        // Its advertisement, and a grant to the departed life, re-admit
        // nothing — and the drop is reported once.
        core.set_roster([node("a"), node("gone")]);
        core.pin(&node("gone"), 3);
        core.set_roster([node("a"), node("gone")]);
        assert!(core.roster().all(|granter| *granter != node("gone")));
        assert_eq!(core.lapses(), 1);

        // Its next life is counted from the first grant this node gives it.
        core.pin(&node("gone"), 4);
        core.set_roster([node("a"), node("gone")]);
        assert!(core.roster().any(|granter| *granter == node("gone")));
        assert_eq!(core.confirmed(), None, "the next life has granted nothing");

        // A live life's row, or none at all, never lets a forgotten granter go.
        let mut core = reader(&["a", "crashed"]);
        let mut alive = grant("reader", id);
        GranterLife {
            epoch: 3,
            departed: false,
        }
        .stamp(&node("crashed"), &mut alive);
        core.observe_grant_map(&node("crashed"), &alive);
        core.set_roster([node("a")]);
        assert!(core.roster().any(|granter| *granter == node("crashed")));
        assert_eq!(core.lapses(), 0);
    }

    #[test]
    fn a_departed_granters_next_life_is_counted_once_its_map_is_live() {
        let mut core = reader(&["a", "gone"]);
        let id = serve_from(&mut core, ClockMs(0), &["a", "gone"]);
        let mut farewell = grant("reader", id);
        GranterLife {
            epoch: 3,
            departed: true,
        }
        .stamp(&node("gone"), &mut farewell);
        core.observe_grant_map(&node("gone"), &farewell);
        core.set_roster([node("a"), node("gone")]);
        assert!(core.roster().all(|granter| *granter != node("gone")));

        let mut next = GrantMap::new();
        GranterLife {
            epoch: 4,
            departed: false,
        }
        .stamp(&node("gone"), &mut next);
        core.observe_grant_map(&node("gone"), &next);
        core.set_roster([node("a"), node("gone")]);
        assert!(core.roster().any(|granter| *granter == node("gone")));
    }

    #[test]
    fn a_departed_life_never_serves_again() {
        let mut core = reader(&["a"]);
        let id = serve_from(&mut core, ClockMs(0), &["a"]);
        core.depart();
        assert_eq!(core.poll(ClockMs(10)), LeaseState::NeedsResync);
        core.observe_grant_map(&node("a"), &grant("reader", id));
        assert!(
            !core.mark_caught_up(ClockMs(10)),
            "a live lease cannot reopen a departed life"
        );
        assert!(!core.valid(ClockMs(10)));
    }

    #[test]
    fn an_empty_roster_confirms_vacuously() {
        let mut core = reader(&[]);
        let id = core.on_renew(ClockMs(500));
        assert_eq!(core.confirmed(), Some(id), "nobody to wait on");
        assert_eq!(core.serve_until(), Some(ClockMs(1_490)));
        assert!(core.mark_caught_up(ClockMs(500)));
        assert!(core.valid(ClockMs(1_000)));
    }

    #[test]
    fn nothing_published_is_no_lease_however_many_granters_confirm() {
        let mut core = reader(&["a"]);
        core.observe_grant_map(&node("a"), &grant("reader", RenewalId { epoch: 7, seq: 1 }));
        assert_eq!(core.confirmed(), None);
        assert_eq!(core.serve_until(), None);
        assert!(!core.mark_caught_up(ClockMs(0)));
    }

    #[test]
    fn a_grant_from_a_previous_life_never_confirms_this_one() {
        let mut core = reader(&["a"]);
        let id = core.on_renew(ClockMs(0));
        assert_eq!(id.epoch, 7);
        // The granter still advertises what it adopted from our last boot.
        core.observe_grant_map(
            &node("a"),
            &grant("reader", RenewalId { epoch: 6, seq: 99 }),
        );
        assert_eq!(core.confirmed_by(&node("a")), None);
        assert_eq!(core.confirmed(), None, "epoch-major: an old life is dead");
        // …and a grant from an impossible future life is refused just as
        // firmly, rather than being trusted because it is larger.
        core.observe_grant_map(&node("a"), &grant("reader", RenewalId { epoch: 8, seq: 1 }));
        assert_eq!(core.confirmed(), None);
        core.observe_grant_map(&node("a"), &grant("reader", id));
        assert_eq!(core.confirmed(), Some(id));
    }

    #[test]
    fn a_granter_that_confirms_someone_else_confirms_nothing_here() {
        let mut core = reader(&["a"]);
        let id = core.on_renew(ClockMs(0));
        core.observe_grant_map(&node("a"), &grant("another-reader", id));
        assert_eq!(core.confirmed(), None);
    }

    #[test]
    fn a_granter_that_regresses_shortens_the_window() {
        let mut core = reader(&["a"]);
        let first = core.on_renew(ClockMs(0));
        let second = core.on_renew(ClockMs(300));
        core.observe_grant_map(&node("a"), &grant("reader", second));
        assert_eq!(core.serve_until(), Some(ClockMs(1_290)));
        // The granter restarted and re-folded a smaller map: state, not a log.
        core.observe_grant_map(&node("a"), &grant("reader", first));
        assert_eq!(core.serve_until(), Some(ClockMs(990)));
        // And a granter that drops us entirely freezes us out.
        core.observe_grant_map(&node("a"), &GrantMap::new());
        assert_eq!(core.serve_until(), None);
    }

    #[test]
    fn a_bogus_grant_beyond_what_we_published_cannot_extend_the_window() {
        let mut core = reader(&["a"]);
        let id = core.on_renew(ClockMs(0));
        core.observe_grant_map(
            &node("a"),
            &grant("reader", RenewalId { epoch: 7, seq: 99 }),
        );
        assert_eq!(core.confirmed(), Some(id), "capped at what we published");
        assert_eq!(core.serve_until(), Some(ClockMs(990)));
    }

    #[test]
    fn a_lapse_is_one_edge_then_needs_resync_until_the_consumer_affirms() {
        let mut core = reader(&["a"]);
        serve_from(&mut core, ClockMs(0), &["a"]);
        assert_eq!(core.poll(ClockMs(989)), LeaseState::Serving);
        // 990 = 0 + 1000 - 10.
        assert_eq!(core.poll(ClockMs(990)), LeaseState::Lapsed, "the edge");
        assert_eq!(core.lapses(), 1);
        assert_eq!(
            core.poll(ClockMs(991)),
            LeaseState::NeedsResync,
            "the level"
        );
        assert_eq!(core.lapses(), 1, "one lapse, one alarm");

        // A fresh, fully confirmed lease is *not* enough on its own.
        let renewed = core.on_renew(ClockMs(1_000));
        core.observe_grant_map(&node("a"), &grant("reader", renewed));
        assert_eq!(core.poll(ClockMs(1_001)), LeaseState::NeedsResync);
        assert!(!core.valid(ClockMs(1_001)));
        assert!(core.mark_caught_up(ClockMs(1_001)));
        assert!(core.valid(ClockMs(1_001)));
    }

    #[test]
    fn catch_up_cannot_be_affirmed_from_inside_a_lapse() {
        let mut core = reader(&["a"]);
        serve_from(&mut core, ClockMs(0), &["a"]);
        assert_eq!(core.poll(ClockMs(990)), LeaseState::Lapsed);
        // The consumer flushed, but its lease is still lapsed: affirming now
        // would put state fetched during the lapse into service.
        assert!(!core.mark_caught_up(ClockMs(995)));
        assert_eq!(core.peek(ClockMs(995)), LeaseState::NeedsResync);
        // Once a renewal is confirmed again, the affirmation takes.
        let renewed = core.on_renew(ClockMs(996));
        core.observe_grant_map(&node("a"), &grant("reader", renewed));
        assert!(core.mark_caught_up(ClockMs(996)));
        assert_eq!(core.peek(ClockMs(996)), LeaseState::Serving);
    }

    #[test]
    fn require_resync_stops_serving_without_a_lapse() {
        let mut core = reader(&["a"]);
        serve_from(&mut core, ClockMs(0), &["a"]);
        assert!(core.valid(ClockMs(100)));
        // A `PeerWrite::Gap`: invalidations were provably missed.
        core.require_resync();
        assert_eq!(core.poll(ClockMs(100)), LeaseState::NeedsResync);
        assert_eq!(core.lapses(), 0, "not a lease lapse — a consumer signal");
        assert!(core.mark_caught_up(ClockMs(100)));
        assert!(core.valid(ClockMs(100)));
    }

    #[test]
    fn a_confirmation_older_than_the_tracked_window_fails_closed() {
        let mut core = reader(&["a"]);
        let ancient = core.on_renew(ClockMs(0));
        core.observe_grant_map(&node("a"), &grant("reader", ancient));
        assert_eq!(core.serve_until(), Some(ClockMs(990)));
        // Renew far past the tracked window while the granter stays silent:
        // the confirmed instant is dropped, and no window survives it.
        for step in 1..=super::MAX_TRACKED_RENEWALS {
            let _ = core.on_renew(ClockMs(step as u64 * 333));
        }
        assert_eq!(core.confirmed(), Some(ancient), "still frozen at renewal 1");
        assert_eq!(core.serve_until(), None, "its publish instant is gone");
        assert!(!core.valid(ClockMs(1)));
    }

    #[test]
    fn last_renewal_tracks_the_newest_publish() {
        let mut core = reader(&[]);
        assert_eq!(core.last_renewal(), None);
        let first = core.on_renew(ClockMs(0));
        assert_eq!(core.last_renewal(), Some(first));
        let second = core.on_renew(ClockMs(1));
        assert!(second > first);
        assert_eq!(core.last_renewal(), Some(second));
        assert_eq!(core.epoch(), 7);
    }

    #[test]
    fn forgetting_a_granter_drops_what_it_confirmed() {
        let mut core = reader(&["a"]);
        let id = core.on_renew(ClockMs(0));
        core.observe_grant_map(&node("a"), &grant("reader", id));
        assert_eq!(core.confirmed_by(&node("a")), Some(id));
        core.forget_granter(&node("a"));
        assert_eq!(core.confirmed_by(&node("a")), None);
        assert_eq!(core.confirmed(), None, "still in the roster, now silent");
    }
}
