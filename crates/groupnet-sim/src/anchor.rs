//! The simulator's **external CAS anchor**: one linearizable register, the
//! driver state a real anchor driver keeps around it, and the faults that make
//! it interesting.
//!
//! [`Activation::External`] closes an epoch with a conditional write to a store
//! outside the cluster. The engine only ever *prompts* — it emits
//! [`Effect::AnchorClaimDue`] and consumes
//! [`Command::AnchorActivated`]/[`Command::AnchorObserved`] — so a simulator
//! that drops the prompt can never elect anybody. This module is the other
//! half: the register, and the driver that turns a prompt into a round.
//!
//! # What is modelled, and what is modelled *away*
//!
//! * **The register is linearizable by construction.** A round loads, plans and
//!   writes **atomically at one scheduled instant**, so the two claim writes —
//!   `If-None-Match: *` into an empty register, and `If-Match: <the etag just
//!   loaded>` — can never lose a race: any other node's round is a different
//!   instant in the schedule and sees the result of this one. That is not a
//!   simplification of the store, it *is* what "linearizable CAS" means; the
//!   simulator gets it for free because the event loop is a total order.
//! * **The write that really can lose is modelled, because it is the one the
//!   tier turns on.** A holder renews against the etag it is *holding*, not one
//!   it has just re-read, and that etag goes stale the moment somebody steals
//!   the record. The mismatch is how a deposed host finds out it has been
//!   deposed while it still believes itself live — see [`AnchorEvent::Yield`].
//! * **The third outcome is modelled too, in both of its readings.**
//!   [`set_unknown_percent`] makes a write **apply and report `Unknown`**, and
//!   [`set_unknown_lost_percent`] makes one **report `Unknown` without
//!   applying** — the two halves of a timed-out conditional `PUT`, which are
//!   indistinguishable to the driver at the instant it happens. It resolves
//!   either exactly as a real one must: a read-back, one anchor latency later,
//!   judged by [`ambiguous_applied`]. Because that read-back is a *separate
//!   scheduled instant*, the record can legitimately have moved on underneath
//!   it, which is the whole reason the rule is fail-closed. The lost half is
//!   the one a *renewal* makes dangerous — an attempted renewal's
//!   `(epoch, host)` is identical to the record it replaces, so only the
//!   expiry distinguishes "it landed" from "my old record is still there".
//! * **Every verdict is core's.** [`plan_claim`], [`renewal_record`] and
//!   [`ambiguous_applied`] are called here unmodified. This module supplies
//!   bytes, etags and a clock — the same division of labour a real object-store
//!   driver has, which is what makes a DST failure here a failure of the
//!   shipped decision rules rather than of a paraphrase of them.
//!
//! # The wall clock is exact, per node, and the only clock in the tier
//!
//! `now_wall(node) = sim.now + skew(node)`, in exact virtual milliseconds. The
//! skew is a *set* offset, not a rate error: it is the assumption
//! [`Activation::External`] states (claimant wall-clock skew ≤
//! `steal_margin_ms`) expressed as the one number that can violate it. Nothing
//! else in the tier reads a clock, so an offset here is the only way to move
//! [`AnchorRecord::stealable`].
//!
//! # Anchor reachability is the availability axis
//!
//! [`block`](AnchorModel::block) is deliberately **orthogonal** to the fabric
//! partitions [`Simulation::block`](crate::Simulation::block) applies: a node
//! can be cut off from every peer and still renew (and so keep hosting), or be
//! perfectly connected to every peer and lose the group because it cannot reach
//! the anchor. That inversion is the tier's whole posture, and it is only
//! testable if the two faults are separate knobs.
//!
//! [`Activation::External`]: groupnet_core::Activation::External
//! [`Command::AnchorActivated`]: groupnet_core::Command::AnchorActivated
//! [`Command::AnchorObserved`]: groupnet_core::Command::AnchorObserved
//! [`Effect::AnchorClaimDue`]: groupnet_core::Effect::AnchorClaimDue
//! [`set_unknown_percent`]: AnchorModel::set_unknown_percent
//! [`set_unknown_lost_percent`]: AnchorModel::set_unknown_lost_percent

use std::collections::{BTreeMap, BTreeSet};

use groupnet_core::anchor::{
    AnchorRecord, ClaimPlan, ambiguous_applied, plan_claim, renewal_record,
};
use groupnet_core::{NodeId, Time};

use crate::rng::SplitMix64;

/// What one anchor round did to the register — the alphabet of
/// [`Simulation::anchor_log`](crate::Simulation::anchor_log).
///
/// The three write arms are distinguished by *what they wrote over*, not by the
/// precondition they used, because that is the distinction the properties are
/// stated in: a [`Steal`](Self::Steal) is a succession (and the only event that
/// consults a clock), a [`Supersede`](Self::Supersede) is the same node
/// re-winning its own record, and a [`Create`](Self::Create) happens once per
/// run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum AnchorEvent {
    /// An `If-None-Match: *` write into an empty register — genesis. Exactly
    /// once per simulation: nothing ever removes the record.
    Create,
    /// An `If-Match` write over a record naming **this same node**, at a
    /// strictly higher epoch. The restart / lost-etag shape: hostship is
    /// re-won, never resumed.
    Supersede,
    /// An `If-Match` write over a record naming **somebody else**, entitled
    /// because the record was past `expires_at_wall_ms + steal_margin_ms` on
    /// the claimant's clock. The one event in the tier a clock decides.
    Steal,
    /// A same-epoch renewal against the etag the holder has been carrying since
    /// it won the epoch. Decides nothing, so it allocates nothing.
    Renew,
    /// The round wrote nothing, for one of the two reasons a round can decline:
    /// a live record names somebody else ([`ClaimPlan::Yield`]), or a renewal's
    /// **held** etag no longer matched — the deposed holder learning, from the
    /// store, that it has been deposed. Either way the driver reports what it
    /// read, which is how a node adopts a pair it never heard gossiped.
    Yield,
}

/// The etag a driver is carrying for the epoch it won: the renewal
/// precondition, and the thing a steal invalidates.
#[derive(Clone, Copy, Debug)]
struct Held {
    epoch: u64,
    token: u64,
}

/// What a completed round has to tell the engine. The simulation turns it into
/// a [`Command`](groupnet_core::Command) — or, for
/// [`Ambiguous`](Self::Ambiguous), into another scheduled event.
#[derive(Clone, Debug)]
pub(crate) enum RoundReport {
    /// The write applied and the driver knows it: report `AnchorActivated`.
    Won {
        /// The epoch the register now names this node at.
        epoch: u64,
    },
    /// The round read a record it did not win: report `AnchorObserved`.
    Observed {
        /// The epoch the record carries.
        epoch: u64,
        /// The node the record names.
        host: NodeId,
    },
    /// Nothing to report — an empty register and nothing written. Defensive:
    /// the register is never emptied, so only a hypothetical backend that lost
    /// the object reaches it.
    Silent,
    /// The store gave **no answer** — the write may or may not have applied,
    /// and the driver cannot tell which. The resolution is a read-back one
    /// anchor latency later, judged by [`ambiguous_applied`].
    Ambiguous {
        /// The record that was written, and the record a read-back must find
        /// standing — to the byte — for the write to count as applied.
        attempted: AnchorRecord,
    },
}

/// The register, the per-node driver state, and the fault knobs.
///
/// Owned by [`Simulation`](crate::Simulation), which schedules the rounds and
/// applies the commands; everything that *decides* is here or in
/// [`groupnet_core::anchor`].
#[derive(Debug, Default)]
pub(crate) struct AnchorModel {
    /// Whether the simulation has an anchor at all. A prompt to a driver with
    /// no anchor configured is dropped — the documented fail-safe posture, and
    /// what keeps every non-`External` suite in this crate unaffected.
    enabled: bool,
    /// The anchor record's TTL, which
    /// [`HostedConfig::lease_ms`](groupnet_core::HostedConfig::lease_ms) also
    /// is: one number, deliberately, so a record and the engine lease earned
    /// from it cannot drift apart.
    ttl_ms: u64,
    /// The activation's `steal_margin_ms`, fed to [`plan_claim`] unmodified.
    steal_margin_ms: u64,
    /// A store round trip, in virtual milliseconds: prompt to write, and write
    /// to read-back.
    latency_ms: u64,
    /// What share of writes apply but report `Unknown` (0..=100).
    unknown_percent: u8,
    /// What share of writes **do not apply** and still report `Unknown`
    /// (0..=100) — the write-throttled or read-only store. Drawn before
    /// [`unknown_percent`](Self::unknown_percent), so a round decided here
    /// never touches the register at all.
    unknown_lost_percent: u8,
    /// The register itself.
    record: Option<AnchorRecord>,
    /// The etag: a counter bumped on every applied write, so a held etag goes
    /// stale exactly when something else is written.
    token: u64,
    /// Per-node driver state: the etag each node is carrying, if any.
    held: BTreeMap<NodeId, Held>,
    /// Per-node wall-clock offset from the simulation's virtual clock.
    skew: BTreeMap<NodeId, i64>,
    /// Nodes that cannot reach the anchor at all.
    blocked: BTreeSet<NodeId>,
    /// Nodes with a round in flight — the debounce the prompt's contract
    /// requires of every driver, without which a store round trip longer than
    /// the anti-entropy interval would stack rounds and burn epochs.
    in_flight: BTreeSet<NodeId>,
    /// How many rounds have been performed, which is also the deterministic
    /// `Unknown` schedule's key.
    rounds: u64,
    /// How many writes reported `Unknown` and had to be resolved by read-back,
    /// whether or not they applied.
    unknown_rounds: u64,
    /// How many of those never applied — the half of
    /// [`unknown_rounds`](Self::unknown_rounds) a read-back must resolve as
    /// *lost*.
    unknown_lost_rounds: u64,
}

impl AnchorModel {
    /// Arms the anchor: from here on a prompt schedules a round.
    ///
    /// `ttl_ms` must be the group's `lease_ms` and `steal_margin_ms` the
    /// activation's — the two are one configuration in
    /// [`Activation::External`](groupnet_core::Activation::External), and a
    /// test that lets them drift is testing a deployment that cannot exist.
    pub(crate) fn enable(&mut self, ttl_ms: u64, steal_margin_ms: u64) {
        self.enabled = true;
        self.ttl_ms = ttl_ms;
        self.steal_margin_ms = steal_margin_ms;
    }

    pub(crate) const fn set_latency(&mut self, ms: u64) {
        self.latency_ms = ms;
    }

    pub(crate) const fn latency_ms(&self) -> u64 {
        self.latency_ms
    }

    pub(crate) const fn ttl_ms(&self) -> u64 {
        self.ttl_ms
    }

    pub(crate) const fn set_unknown_percent(&mut self, percent: u8) {
        self.unknown_percent = if percent > 100 { 100 } else { percent };
    }

    /// The other reading of a timed-out `PUT`: the write **never applied**, and
    /// the store still said nothing. Independent of
    /// [`set_unknown_percent`](Self::set_unknown_percent) — a different draw on
    /// the same round counter — and decided first, because a lost write must
    /// not touch the register.
    ///
    /// [`Simulation::anchor_log`](crate::Simulation::anchor_log) still records
    /// the round, because it is the driver's-eye view of what was *attempted*;
    /// [`record`](Self::record) is the ground truth for what landed, and under
    /// this knob the two deliberately disagree.
    pub(crate) const fn set_unknown_lost_percent(&mut self, percent: u8) {
        self.unknown_lost_percent = if percent > 100 { 100 } else { percent };
    }

    pub(crate) fn set_skew(&mut self, node: &NodeId, ms: i64) {
        self.skew.insert(node.clone(), ms);
    }

    pub(crate) fn block(&mut self, node: &NodeId) {
        self.blocked.insert(node.clone());
    }

    pub(crate) fn heal(&mut self, node: &NodeId) {
        self.blocked.remove(node);
    }

    pub(crate) fn heal_all(&mut self) {
        self.blocked.clear();
    }

    pub(crate) fn is_blocked(&self, node: &NodeId) -> bool {
        self.blocked.contains(node)
    }

    /// Seeds the register directly — the "somebody was already here" fixture.
    /// The etag moves, so nobody is holding one for it.
    pub(crate) fn seed(&mut self, record: AnchorRecord) {
        self.record = Some(record);
        self.token += 1;
        self.held.clear();
    }

    pub(crate) fn record(&self) -> Option<AnchorRecord> {
        self.record.clone()
    }

    pub(crate) const fn unknown_rounds(&self) -> u64 {
        self.unknown_rounds
    }

    pub(crate) const fn unknown_lost_rounds(&self) -> u64 {
        self.unknown_lost_rounds
    }

    /// A crash takes the driver with the process: the etag it was carrying is
    /// gone, and so is any round it had in flight. This is what makes a restart
    /// **re-win** through [`plan_claim`] rather than resume — there is no
    /// node-local storage in this tier, and the model has none either.
    pub(crate) fn forget(&mut self, node: &NodeId) {
        self.held.remove(node);
        self.in_flight.remove(node);
    }

    /// Whether a prompt turns into a round: only with an anchor configured, and
    /// only when this node has no round already in flight.
    pub(crate) fn accept_prompt(&mut self, node: &NodeId) -> bool {
        self.enabled && self.in_flight.insert(node.clone())
    }

    /// Releases the debounce: the round is over, whatever it concluded.
    pub(crate) fn finish(&mut self, node: &NodeId) {
        self.in_flight.remove(node);
    }

    /// This node's wall clock at `now`, in absolute milliseconds. Floors at
    /// zero, so a node whose clock is set behind the start of the simulation is
    /// merely early rather than arithmetically impossible.
    fn now_wall(&self, node: &NodeId, now: Time) -> u64 {
        let base = i64::try_from(now.0).unwrap_or(i64::MAX);
        let skew = self.skew.get(node).copied().unwrap_or(0);
        u64::try_from(base.saturating_add(skew)).unwrap_or(0)
    }

    /// One anchor round, start to finish, at a single instant.
    ///
    /// `hosting` is the epoch the engine currently believes it hosts, if it
    /// hosts at all — the shell reads it off the engine rather than the model
    /// keeping a second copy. A round **renews** exactly when the engine says
    /// it hosts `e` *and* the driver still carries the etag it won `e` with;
    /// anything else is a claim, which re-plans from the record that is
    /// actually there. That is the same either/or a real driver faces, and it
    /// is why a restarted host claims instead of renewing.
    pub(crate) fn round(
        &mut self,
        node: &NodeId,
        epoch_hint: u64,
        now: Time,
        hosting: Option<u64>,
    ) -> (AnchorEvent, RoundReport) {
        self.rounds += 1;
        let now_wall = self.now_wall(node, now);
        match self.held.get(node).copied() {
            Some(held) if hosting == Some(held.epoch) => self.renew(node, held, now_wall),
            _ => self.claim(node, epoch_hint, now_wall),
        }
    }

    /// The holder's path: an `If-Match` write against the etag it is carrying.
    fn renew(&mut self, node: &NodeId, held: Held, now_wall: u64) -> (AnchorEvent, RoundReport) {
        if self.token != held.token {
            // Superseded while we held the etag. Fail-closed: the write is
            // refused, the etag is dropped, and the driver reports what the
            // store actually shows — which deposes this node through row X4.
            self.held.remove(node);
            return (AnchorEvent::Yield, self.observe());
        }
        let record = renewal_record(node, held.epoch, now_wall, self.ttl_ms);
        (AnchorEvent::Renew, self.apply_write(node, record))
    }

    /// The claimant's path: load, [`plan_claim`], and write what it says.
    fn claim(
        &mut self,
        node: &NodeId,
        epoch_hint: u64,
        now_wall: u64,
    ) -> (AnchorEvent, RoundReport) {
        let plan = plan_claim(
            node,
            epoch_hint,
            self.record.as_ref(),
            now_wall,
            self.ttl_ms,
            self.steal_margin_ms,
        );
        match plan {
            // A live record naming somebody else. The driver reports the pair
            // it read, which is how a node that has heard no gossip at all
            // still learns who holds the group.
            ClaimPlan::Yield { .. } => (AnchorEvent::Yield, self.observe()),
            ClaimPlan::Create(record) => (AnchorEvent::Create, self.apply_write(node, record)),
            ClaimPlan::Supersede(record) => {
                let over_self = self.record.as_ref().is_some_and(|r| r.host == *node);
                let event = if over_self {
                    AnchorEvent::Supersede
                } else {
                    AnchorEvent::Steal
                };
                (event, self.apply_write(node, record))
            }
        }
    }

    /// Commits a write to the register and decides what the store *said* about
    /// it.
    ///
    /// The write applies unless the **lost** schedule takes it — see the module
    /// docs on why a conditional write planned and issued at one scheduled
    /// instant cannot lose a *race*; what it can still meet is a store that
    /// refuses or drops it. Then the `Unknown` schedule decides whether the
    /// driver is told what happened. An `Unknown` costs the driver the new etag
    /// as well as the answer, exactly as it would in reality: the etag the
    /// write produced was never in a response anybody saw, so the read-back is
    /// where it is learned — and a driver that has lost its etag cannot renew,
    /// which is why a lost write costs a hold and not merely a round.
    fn apply_write(&mut self, node: &NodeId, record: AnchorRecord) -> RoundReport {
        let epoch = record.epoch;
        // Drawn first, and on its own constant: a write the store never applied
        // must not touch the register, and the two faults must be independently
        // schedulable.
        let lost = self.unknown_lost_percent != 0
            && SplitMix64::hash(self.rounds ^ 0x51ed_270b_2c9e_4d13) % 100
                < u64::from(self.unknown_lost_percent);
        if lost {
            self.unknown_rounds += 1;
            self.unknown_lost_rounds += 1;
            self.held.remove(node);
            return RoundReport::Ambiguous { attempted: record };
        }
        self.record = Some(record.clone());
        self.token += 1;
        let unknown = self.unknown_percent != 0
            && SplitMix64::hash(self.rounds ^ 0xa9c4_07d0_1f5e_2b63) % 100
                < u64::from(self.unknown_percent);
        if unknown {
            self.unknown_rounds += 1;
            self.held.remove(node);
            return RoundReport::Ambiguous { attempted: record };
        }
        self.held.insert(
            node.clone(),
            Held {
                epoch,
                token: self.token,
            },
        );
        RoundReport::Won { epoch }
    }

    /// The read-back that resolves an [`Ambiguous`](RoundReport::Ambiguous)
    /// write, a whole anchor latency after it — long enough for the record to
    /// have moved on, which is the case the rule exists for.
    ///
    /// Applied **iff** the register now holds exactly the record that was
    /// attempted, judged by core's [`ambiguous_applied`]. Anything else reads
    /// as *not applied* and is reported as an observation, so an ambiguous
    /// round can cost a re-plan and can never award a hostship this node did
    /// not win — nor a lease extension it did not earn, which is the half
    /// [`set_unknown_lost_percent`](Self::set_unknown_lost_percent) exercises.
    pub(crate) fn read_back(&mut self, node: &NodeId, attempted: &AnchorRecord) -> RoundReport {
        if ambiguous_applied(node, attempted, self.record.as_ref()) {
            // It stood. The read carried the etag with it, so the driver can
            // renew again from here.
            self.held.insert(
                node.clone(),
                Held {
                    epoch: attempted.epoch,
                    token: self.token,
                },
            );
            return RoundReport::Won {
                epoch: attempted.epoch,
            };
        }
        self.observe()
    }

    /// What the register currently shows, as a report.
    fn observe(&self) -> RoundReport {
        match &self.record {
            Some(rec) => RoundReport::Observed {
                epoch: rec.epoch,
                host: rec.host.clone(),
            },
            None => RoundReport::Silent,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AnchorEvent, AnchorModel, RoundReport};
    use groupnet_core::anchor::AnchorRecord;
    use groupnet_core::{NodeId, Time};

    const TTL: u64 = 1_000;
    const MARGIN: u64 = 200;

    fn n(id: &str) -> NodeId {
        NodeId::new(id)
    }

    fn model() -> AnchorModel {
        let mut model = AnchorModel::default();
        model.enable(TTL, MARGIN);
        model.set_latency(10);
        model
    }

    /// A round for `node` at `now`, hinting `hint`, that the engine does not
    /// believe is hosting anything.
    fn claim(model: &mut AnchorModel, node: &str, hint: u64, now: u64) -> AnchorEvent {
        model.round(&n(node), hint, Time(now), None).0
    }

    /// Genesis, then a renewal, then a steal: the register's whole life in the
    /// order the log records it, and the etag moving under each one.
    #[test]
    fn the_register_creates_renews_and_is_stolen_when_the_record_expires() {
        let mut model = model();
        assert_eq!(claim(&mut model, "a", 1, 0), AnchorEvent::Create);
        assert_eq!(
            model.record(),
            Some(AnchorRecord {
                epoch: 1,
                host: n("a"),
                expires_at_wall_ms: TTL,
            })
        );

        // The holder renews: same epoch, later expiry, nothing allocated.
        let (event, report) = model.round(&n("a"), 1, Time(500), Some(1));
        assert_eq!(event, AnchorEvent::Renew);
        assert!(matches!(report, RoundReport::Won { epoch: 1 }));
        assert_eq!(
            model.record().expect("a record").expires_at_wall_ms,
            500 + TTL
        );

        // Another node is refused while the record is live, and reports the
        // pair it read rather than nothing at all.
        let (event, report) = model.round(&n("b"), 2, Time(1_000), None);
        assert_eq!(event, AnchorEvent::Yield);
        assert!(matches!(report, RoundReport::Observed { epoch: 1, .. }));

        // Expiry (1_500) plus margin (200) is the first entitled instant.
        assert_eq!(claim(&mut model, "b", 2, 1_699), AnchorEvent::Yield);
        assert_eq!(claim(&mut model, "b", 2, 1_700), AnchorEvent::Steal);
        let stolen = model.record().expect("a record");
        assert_eq!((stolen.epoch, stolen.host), (2, n("b")));
    }

    /// The renewal a steal invalidated: the held etag no longer matches, so the
    /// write is refused and the deposed holder is told who is there instead.
    /// This is the only write in the model that can lose.
    #[test]
    fn a_renewal_against_a_stale_etag_is_refused_and_reports_the_standing_pair() {
        let mut model = model();
        claim(&mut model, "a", 1, 0);
        claim(&mut model, "b", 2, 1_200); // steals: 1_000 + 200 margin

        let (event, report) = model.round(&n("a"), 1, Time(1_300), Some(1));
        assert_eq!(event, AnchorEvent::Yield);
        match report {
            RoundReport::Observed { epoch, host } => assert_eq!((epoch, host), (2, n("b"))),
            other => panic!("a refused renewal must report the standing pair: {other:?}"),
        }
        // And the etag is dropped, so the next round re-plans from the record.
        assert_eq!(claim(&mut model, "a", 3, 5_000), AnchorEvent::Steal);
    }

    /// A restart is a re-win: the driver's etag died with the process, so the
    /// round claims — and `plan_claim` bids strictly above the record that
    /// still names this node.
    #[test]
    fn a_forgotten_etag_re_wins_the_record_at_a_higher_epoch() {
        let mut model = model();
        claim(&mut model, "a", 1, 0);
        model.forget(&n("a"));

        // Still `Host` at epoch 1 as far as the engine knows — and it still
        // claims, because the etag is what a renewal needs.
        let (event, report) = model.round(&n("a"), 2, Time(100), Some(1));
        assert_eq!(event, AnchorEvent::Supersede, "over its own record");
        assert!(matches!(report, RoundReport::Won { epoch: 2 }));
    }

    /// An ambiguous write applied; the read-back finds the pair standing and
    /// the round is won after all — and the driver comes away with a usable
    /// etag, so it can renew rather than re-claiming for ever.
    #[test]
    fn an_unknown_write_that_still_stands_resolves_as_won() {
        let mut model = model();
        model.set_unknown_percent(100);
        let (event, report) = model.round(&n("a"), 1, Time(0), None);
        assert_eq!(event, AnchorEvent::Create);
        let RoundReport::Ambiguous { attempted } = report else {
            panic!("a 100% unknown schedule reports nothing else: {report:?}");
        };
        assert_eq!(model.unknown_rounds(), 1);

        model.set_unknown_percent(0);
        assert!(matches!(
            model.read_back(&n("a"), &attempted),
            RoundReport::Won { epoch: 1 }
        ));
        assert_eq!(
            model.round(&n("a"), 1, Time(10), Some(1)).0,
            AnchorEvent::Renew,
            "the read-back carried the etag"
        );
    }

    /// The fail-closed half: the write applied, but by the time the driver
    /// looked the record had moved on. The round is *not* won, and what the
    /// read found is reported instead.
    #[test]
    fn an_unknown_write_superseded_before_the_read_back_resolves_as_observed() {
        let mut model = model();
        model.set_unknown_percent(100);
        let RoundReport::Ambiguous { attempted } = model.round(&n("a"), 1, Time(0), None).1 else {
            panic!("a 100% unknown schedule is ambiguous");
        };
        model.set_unknown_percent(0);
        // Somebody stole it in the interval the driver spent not knowing.
        assert_eq!(claim(&mut model, "b", 2, 1_200), AnchorEvent::Steal);

        match model.read_back(&n("a"), &attempted) {
            RoundReport::Observed { epoch, host } => assert_eq!((epoch, host), (2, n("b"))),
            other => panic!("an ambiguous write must never award a lost record: {other:?}"),
        }
    }

    /// The dangerous half of an ambiguous write: the renewal that **never
    /// applied** and still reported `Unknown`. The read-back finds the holder's
    /// own record standing at the same `(epoch, host)` — and must still call it
    /// lost, because the expiry it attempted is not the one that is there.
    ///
    /// Reading it as won is what would let a lease extend for ever off a record
    /// quietly ageing out beneath it.
    #[test]
    fn a_renewal_that_never_applied_resolves_as_lost_not_as_the_record_it_replaces() {
        let mut model = model();
        claim(&mut model, "a", 1, 0); // expires at wall 1_000
        let standing = model.record().expect("a record");

        model.set_unknown_lost_percent(100);
        let (event, report) = model.round(&n("a"), 1, Time(600), Some(1));
        assert_eq!(event, AnchorEvent::Renew, "the round was a renewal attempt");
        let RoundReport::Ambiguous { attempted } = report else {
            panic!("a lost write reports nothing but ambiguity: {report:?}");
        };
        assert_eq!(model.unknown_lost_rounds(), 1);
        assert_eq!(
            model.record(),
            Some(standing.clone()),
            "a write the store never applied must not touch the register"
        );

        model.set_unknown_lost_percent(0);
        match model.read_back(&n("a"), &attempted) {
            RoundReport::Observed { epoch, host } => assert_eq!((epoch, host), (1, n("a"))),
            other => panic!("a renewal that never landed must not read as won: {other:?}"),
        }
        // And the hold is gone with it, so the next round re-plans from the
        // record instead of renewing an epoch on an etag it no longer has.
        assert_eq!(
            model.round(&n("a"), 1, Time(700), Some(1)).0,
            AnchorEvent::Supersede,
            "the lost write cost the etag as well as the answer"
        );
    }

    /// The wall clock is exact and per node, and floors at zero rather than
    /// wrapping. A claimant whose clock runs fast is entitled to steal earlier
    /// — in exact milliseconds, which is what the skew suite pins.
    #[test]
    fn skew_moves_the_steal_boundary_by_exactly_its_offset() {
        // The record expires at wall 1_000, so an unskewed claimant is entitled
        // at 1_200; a clock 300ms fast reaches that 300ms sooner in virtual
        // time, and one 300ms slow 300ms later. Exactly, not approximately.
        for (skew, boundary) in [(0i64, 1_200u64), (300, 900), (-300, 1_500)] {
            let mut model = model();
            claim(&mut model, "a", 1, 0); // expires at wall 1_000
            model.set_skew(&n("b"), skew);
            assert_eq!(
                claim(&mut model, "b", 2, boundary - 1),
                AnchorEvent::Yield,
                "skew {skew}"
            );
            assert_eq!(
                claim(&mut model, "b", 2, boundary),
                AnchorEvent::Steal,
                "skew {skew}"
            );
        }
        // A clock set behind the epoch floors rather than wrapping into a
        // record that is stealable for ever.
        let mut model = model();
        model.set_skew(&n("a"), -5_000);
        assert_eq!(claim(&mut model, "a", 1, 0), AnchorEvent::Create);
        assert_eq!(model.record().expect("a record").expires_at_wall_ms, TTL);
    }

    /// The debounce every driver owes the prompt: one round in flight per node,
    /// and a crash releases it along with the etag.
    #[test]
    fn prompts_are_debounced_per_node_until_the_round_finishes() {
        let mut model = model();
        assert!(model.accept_prompt(&n("a")));
        assert!(!model.accept_prompt(&n("a")), "a second round would stack");
        assert!(model.accept_prompt(&n("b")), "per node, not global");
        model.finish(&n("a"));
        assert!(model.accept_prompt(&n("a")));

        model.forget(&n("b"));
        assert!(model.accept_prompt(&n("b")), "a crash releases the round");
    }

    /// With no anchor configured a prompt is dropped and no round ever runs —
    /// the fail-safe posture, and what keeps the `Settle` and `Quorum` suites
    /// in this crate untouched by this module's existence.
    #[test]
    fn an_unconfigured_anchor_accepts_no_prompt_at_all() {
        let mut model = AnchorModel::default();
        assert!(!model.accept_prompt(&n("a")));
        assert_eq!(model.record(), None);
    }
}
