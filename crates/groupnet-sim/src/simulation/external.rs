//! The external CAS anchor's driver: its fault knobs, the rounds the
//! [`Effect::AnchorClaimDue`](groupnet_core::Effect::AnchorClaimDue) prompt
//! schedules, and the command each round reports back.
//!
//! Everything here drives `Activation::External`. A simulation that never
//! calls [`enable_anchor`](Simulation::enable_anchor) has no anchor, so the
//! prompt is dropped and no group ever activates a host — the same fail-safe
//! posture a driver with no anchor configured has, and the reason every
//! `Settle` and `Quorum` suite in this crate is unaffected by any of it.
//!
//! The per-node knobs ([`set_anchor_skew`](Simulation::set_anchor_skew),
//! [`block_anchor`](Simulation::block_anchor),
//! [`heal_anchor`](Simulation::heal_anchor)) model the node's **host** — its
//! wall clock and its path to the store — not its engine. They are therefore
//! accepted for any id, deliberately outlive [`crash`](Simulation::crash), and
//! apply to whichever engine that id hosts next.

use super::{Kind, Simulation};
use crate::anchor::RoundReport;
use crate::percent::Percent;

use groupnet_core::anchor::AnchorRecord;
use groupnet_core::{Command, NodeId, Role, Time};

impl Simulation {
    /// Arms the external anchor for this simulation.
    ///
    /// `ttl_ms` is the anchor record's TTL and `steal_margin_ms` the steal
    /// margin — respectively
    /// [`HostedConfig::lease_ms`](groupnet_core::HostedConfig::lease_ms) and
    /// [`Activation::External`](groupnet_core::Activation::External)'s field,
    /// which is the one configuration the tier has. Pass anything else and you
    /// are simulating a deployment that cannot be configured.
    pub fn enable_anchor(&mut self, ttl_ms: u64, steal_margin_ms: u64) {
        self.anchor.enable(ttl_ms, steal_margin_ms);
    }

    /// How long a store round trip takes, in virtual milliseconds: prompt to
    /// write, and (for an ambiguous write) write to read-back. A knob of its
    /// own because it is not a fabric latency — the anchor is not on the
    /// cluster's network, and a deployment where the store is far and the peers
    /// are near is the normal one.
    pub fn set_anchor_latency(&mut self, ms: u64) {
        self.anchor.set_latency(ms);
    }

    /// This node's wall-clock offset from virtual time, in milliseconds, which
    /// may be negative. Zero for every node unless set.
    ///
    /// This is the *only* clock the External tier consults, and it consults it
    /// in exactly one place ([`AnchorRecord::stealable`]). An offset here is
    /// therefore the complete model of the assumption `Activation::External`
    /// states — and of violating it.
    ///
    /// The offset belongs to the node's host, not its engine: it may be set
    /// before the node is [`add`](Self::add)ed and survives
    /// [`crash`](Self::crash), so a restarted engine keeps its machine's clock.
    ///
    /// [`AnchorRecord::stealable`]: groupnet_core::anchor::AnchorRecord::stealable
    pub fn set_anchor_skew(&mut self, node: &NodeId, ms: i64) {
        self.anchor.set_skew(node, ms);
    }

    /// Cuts `node` off from the anchor. **Orthogonal to
    /// [`block`](Self::block)**: this is the availability axis under
    /// `External`, and the CP inversion is only testable because the two are
    /// separate. A node blocked here keeps gossiping, keeps its rank and keeps
    /// its peers — and cannot renew, so its lease lapses and it demotes.
    ///
    /// Like [`set_anchor_skew`](Self::set_anchor_skew) this is host-level
    /// state: accepted for any id, kept across [`crash`](Self::crash), and
    /// applied to whichever engine `node` hosts next.
    pub fn block_anchor(&mut self, node: &NodeId) {
        self.anchor.block(node);
    }

    /// Restores `node`'s access to the anchor. Host-level, like
    /// [`block_anchor`](Self::block_anchor): accepted for any id.
    pub fn heal_anchor(&mut self, node: &NodeId) {
        self.anchor.heal(node);
    }

    /// Restores every node's access to the anchor.
    pub fn heal_anchor_all(&mut self) {
        self.anchor.heal_all();
    }

    /// What share of conditional writes **apply and report `Unknown`** — the
    /// timed-out `PUT` every object store can produce. The driver resolves each
    /// one by reading the record back an anchor latency later and judging it
    /// with [`ambiguous_applied`](groupnet_core::anchor::ambiguous_applied).
    ///
    /// Deterministic: the schedule is keyed on the round counter through the
    /// same [`SplitMix64`](crate::SplitMix64) the loss schedule uses, so a
    /// failing seed replays exactly.
    pub fn set_anchor_unknown_percent(&mut self, share: Percent) {
        self.anchor.set_unknown_percent(share);
    }

    /// What share of conditional writes report `Unknown` **without applying**
    /// — the other half of the same timeout, and the shape a
    /// write-throttled, read-only or expired-credential store produces
    /// indefinitely rather than transiently.
    ///
    /// It is the half a **renewal** makes dangerous: an attempted renewal's
    /// `(epoch, host)` is identical to the record it means to replace, so a
    /// read-back that judged the pair alone would call every failed renewal a
    /// win and extend a lease off a record that is quietly ageing out.
    /// [`ambiguous_applied`](groupnet_core::anchor::ambiguous_applied) compares
    /// the whole record, which is what makes this knob resolve as *lost*.
    ///
    /// Independent of [`set_anchor_unknown_percent`](Self::set_anchor_unknown_percent)
    /// — a separate deterministic draw on the same round counter — and decided
    /// first, so a write it takes never reaches the register.
    pub fn set_anchor_unknown_lost_percent(&mut self, share: Percent) {
        self.anchor.set_unknown_lost_percent(share);
    }

    /// Writes a record into the anchor directly, as if some earlier incarnation
    /// of the cluster — or a node that is not in this simulation at all — had
    /// won it. The etag moves, so nobody holds one for it.
    pub fn seed_anchor(&mut self, record: AnchorRecord) {
        self.anchor.seed(record);
    }

    /// The record the anchor currently holds — the ground truth every observer
    /// is converging on, readable without asking any node.
    #[must_use]
    pub fn anchor_record(&self) -> Option<AnchorRecord> {
        self.anchor.record()
    }

    /// How many conditional writes reported `Unknown` and had to be resolved by
    /// a read-back. The floor that keeps an ambiguity schedule from passing
    /// vacuously.
    #[must_use]
    pub fn anchor_unknown_rounds(&self) -> u64 {
        self.anchor.unknown_rounds()
    }

    /// How many of those never applied — the half of
    /// [`anchor_unknown_rounds`](Self::anchor_unknown_rounds) a read-back has
    /// to resolve as *lost*, and the floor that keeps a
    /// [`set_anchor_unknown_lost_percent`](Self::set_anchor_unknown_lost_percent)
    /// schedule from passing vacuously.
    #[must_use]
    pub fn anchor_unknown_lost_rounds(&self) -> u64 {
        self.anchor.unknown_lost_rounds()
    }

    /// Takes one [`Effect::AnchorClaimDue`](groupnet_core::Effect::AnchorClaimDue)
    /// prompt from `node`.
    ///
    /// The prompt is a *level* signal on the gossip cadence, so the model
    /// debounces it against this node's own in-flight round exactly as the
    /// effect's contract requires a driver to — without that, a store round
    /// trip longer than one gossip interval would stack rounds and burn epochs.
    pub(super) fn prompt_anchor(&mut self, node: &NodeId, epoch_hint: u64) {
        if self.anchor.accept_prompt(node) {
            let at = self.now.saturating_add(self.anchor.latency_ms());
            self.schedule(
                at,
                Kind::AnchorRound {
                    node: node.clone(),
                    epoch_hint,
                    started_at: self.now,
                },
            );
        }
    }

    /// One anchor round: what the driver would do between the prompt and the
    /// command it reports back.
    ///
    /// Two ways a round simply does not happen, and both are load-bearing:
    ///
    /// * **The process crashed.** Nothing performs the round, and the record it
    ///   holds ages out on its own.
    /// * **The node cannot reach the anchor.** Also nothing — and *this* is the
    ///   fail-closed posture the tier is chosen for: the engine is untouched,
    ///   keeps its rank, keeps its peers, and demotes anyway when row 6 finds
    ///   the lease lapsed. A node never hosts on its own say-so here.
    pub(super) fn anchor_round(&mut self, node: &NodeId, epoch_hint: u64, started_at: Time) {
        if !self.engines.contains_key(node) || self.anchor.is_blocked(node) {
            self.anchor.finish(node);
            return;
        }
        // Renew or claim is the engine's answer, not the model's: the driver
        // renews only while the node it is driving still believes it hosts the
        // epoch its etag was won for.
        let hosting = self
            .engines
            .get(node)
            .filter(|e| e.role() == Role::Host)
            .map(|e| e.leadership().0);
        let (event, report) = self.anchor.round(node, epoch_hint, self.now, hosting);
        self.anchor_log.push((self.now, node.clone(), event));
        self.report_anchor(node, report, started_at);
    }

    /// The read-back an ambiguous write is resolved by. A node that crashed or
    /// lost the anchor in the meantime never learns: its etag stays dropped and
    /// its next round re-plans from whatever the record shows, which is the
    /// conservative direction.
    pub(super) fn anchor_read_back(
        &mut self,
        node: &NodeId,
        attempted: &AnchorRecord,
        started_at: Time,
    ) {
        if !self.engines.contains_key(node) || self.anchor.is_blocked(node) {
            self.anchor.finish(node);
            return;
        }
        let report = self.anchor.read_back(node, attempted);
        self.report_anchor(node, report, started_at);
    }

    /// Hands a finished round's verdict to the engine as the command a real
    /// driver would send, and releases the round's debounce.
    ///
    /// `lease_until` is `started_at + ttl`, anchored at the instant the round
    /// **began** rather than the instant the write landed — the rule
    /// [`Command::AnchorActivated`] states. The record's own
    /// `expires_at_wall_ms` was stamped one anchor latency later, so the engine
    /// lease always runs out first and a host never overhangs the record it is
    /// hosting on.
    ///
    /// Only called once the round's caller has seen `node`'s engine present,
    /// and nothing in the same step removes it, so the command always lands.
    fn report_anchor(&mut self, node: &NodeId, report: RoundReport, started_at: Time) {
        let command = match report {
            RoundReport::Won { epoch } => {
                self.anchor.finish(node);
                let lease_until = started_at.saturating_add(self.anchor.ttl_ms());
                Command::AnchorActivated { epoch, lease_until }
            }
            RoundReport::Observed { epoch, host } => {
                self.anchor.finish(node);
                Command::AnchorObserved { epoch, host }
            }
            RoundReport::Silent => {
                self.anchor.finish(node);
                return;
            }
            // Still in flight: an ambiguous write is not over until the
            // read-back, so the debounce deliberately stays held.
            RoundReport::Ambiguous { attempted } => {
                let at = self.now.saturating_add(self.anchor.latency_ms());
                self.schedule(
                    at,
                    Kind::AnchorReadBack {
                        node: node.clone(),
                        attempted,
                        started_at,
                    },
                );
                return;
            }
        };
        let landed = self.command(node, command);
        debug_assert!(
            landed.is_ok(),
            "an anchor round reports only to a live engine"
        );
    }
}
