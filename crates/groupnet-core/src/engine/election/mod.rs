//! The Hosted-mode election: epochs, claims, the settle window, leases, and
//! the epoch-major fencing merge that reconciles two beliefs about who holds
//! the group.
//!
//! Everything here is inert unless the group's [`Config::mode`] is
//! [`GroupMode::Hosted`]: the state below is not even allocated, no election
//! frame is ever built, and inbound wire kinds 8–10 decode and drop. An
//! [`Eventual`] group costs exactly what it cost before this module existed.
//!
//! # What `Settle` activation guarantees, stated honestly
//!
//! The top-ranked live candidate — the same rendezvous ranking the *derived*
//! [`coordinator`] comes from, so there are no dueling randomized timeouts —
//! claims `highest_seen + 1`, broadcasts the claim to every live member, and
//! activates if that claim is still standing `claim_settle_ms` later. That
//! buys exactly this much, and no more:
//!
//! * **A single serializer per epoch *pair*, not per epoch number.** Two sides
//!   of a partition can settle the *same* epoch number under different hosts —
//!   each side counted only the members it could see, so each derived the same
//!   `highest_seen + 1`. The unit that names a serializer is therefore the
//!   pair `(epoch, host)`, and the fencing order is a total order over pairs:
//!   epoch-major, and at equal epochs the [`owner`](crate::placement::owner) of
//!   the group id among the two hosts wins. That tiebreak reads nothing but the
//!   group id and the two host ids, so it is view-independent: every node, on
//!   either side of a heal, picks the same survivor without exchanging
//!   anything.
//! * **Fencing, not prevention.** A deposed host stops being believed the
//!   moment any peer holds a higher pair and teaches it back — but a client
//!   talking to a not-yet-deposed host during the detection window can still
//!   be served by it. The window is bounded by the lease, not by consensus.
//! * **Split-brain is permitted, bounded, and fenced at heal.** Both sides of
//!   a partition may host. At heal exactly one pair survives: the loser adopts
//!   the winner and emits a [`LeadershipChanged`] demotion for the application
//!   to reconcile against. This is the AP posture, chosen deliberately;
//!   `Activation::Quorum` (M3) is the CP one.
//! * **Renewal is a tick-re-rank, not a round-trip.** A host renews its lease
//!   on any tick where it is still the top-ranked live candidate: its own
//!   membership view is the evidence, and nothing is asked of any peer. A host
//!   that *loses* rank keeps serving until `lease_ms` after its last renewal
//!   and then demotes itself. That hysteresis is what keeps leadership from
//!   churning on a transient view wobble — and `lease_ms` is the number that
//!   bounds how long two nodes can both believe they hold the group.
//! * **A node never adopts a pair that names itself — but it always learns its
//!   epoch.** A demoted host is surrounded by echoes of its own hostship;
//!   taking the *hostship* back from one would resurrect an authority it has
//!   already given up, so hostship is only ever *entered* through this node's
//!   own activation. The *epoch* is a different thing: it is the fence the
//!   whole cluster is ordered by, and refusing to learn it is what wedges a
//!   restarted host (row 12b). So a better pair naming this node is taken with
//!   its hostship stripped off — `(epoch, None)` — and the node re-earns the
//!   group by claiming above it, if it can.
//! * **Restart caveat.** Election state is in memory only. A host that
//!   restarts comes back a Follower at epoch 0, and if it is also isolated it
//!   is the top-ranked live candidate of a group of one: it will settle a
//!   *stale* epoch and briefly self-host. That is the legitimate side-of-one —
//!   the same thing any surviving partition side does — and it ends on first
//!   contact, because whatever the rest of the cluster settled meanwhile
//!   carries a higher pair. A higher pair naming *somebody else* deposes it
//!   outright (row 12); a higher pair naming *itself* — the shape a restart
//!   actually produces — steps it down to that epoch hostless and it re-elects
//!   above it (row 12b). Contact costs one extra election either way, and the
//!   stale epoch is gone. Durable epochs — never settling a stale one to begin
//!   with — arrive with `Activation::Quorum` (M3), where persistence is a
//!   safety requirement rather than a nicety.
//!
//! # `Quorum` activation: the same skeleton, a different closing rule
//!
//! [`Activation::Quorum`] shares every row above — the claim guard, the fencing
//! order, adoption, row 12b, the repair beacon, the lease and its step-down —
//! and replaces exactly one thing: what closes an epoch. A `Settle` claim
//! activates because its window shut with the claim still standing; a `Quorum`
//! claim activates because a majority of a **static voter roster** granted it,
//! and expires silently if the window shuts first. The grant ledger, the round
//! tally, the claimant's self-grant and its per-tick retry (row Q4b), the
//! host's renewal round and the voter durability rules all live in
//! [`quorum`](self::quorum), which is where the `Q`-rows are documented; the
//! rows here call into it at the points those rows name and are otherwise
//! untouched.
//!
//! # Layout
//!
//! This file is the [`GroupEngine`] rows and the frame dispatch that routes to
//! them. The two pieces that are pure functions rather than state transitions
//! live beside it: [`order`] holds [`Role`], the fencing order over
//! `(epoch, host)` pairs and row 1's claim guard (each tabulated exhaustively by
//! its own tests), and [`quorum`] holds everything the `Q`-rows add.
//!
//! [`Activation::Quorum`]: crate::Activation::Quorum
//! [`Config::mode`]: crate::Config::mode
//! [`GroupMode::Hosted`]: crate::GroupMode::Hosted
//! [`Eventual`]: crate::GroupMode::Eventual
//! [`coordinator`]: crate::GroupEngine::coordinator
//! [`LeadershipChanged`]: crate::Effect::LeadershipChanged

mod order;
mod quorum;

use std::cmp::Ordering;

use crate::config::{Activation, HostedConfig};
use crate::membership::{Member, Status};
use crate::{NodeId, Time, wire};

use super::effect::Effect;
use super::state::GroupEngine;
use order::{ClaimGuard, cmp_pair};
use quorum::QuorumState;

pub use order::Role;
pub use quorum::RecoveredGrant;

/// A standing bid for an epoch: what row 1 opens and row 4 closes.
#[derive(Clone, Copy, Debug)]
struct Claim {
    /// The epoch being claimed (`highest_seen + 1` at the moment of the bid).
    epoch: u64,
    /// When the claim's window closes: under an activation that closes an
    /// epoch on the window alone, when it activates if nothing has outranked
    /// it by then — see [`Election::closes_on_window`].
    settle_at: Time,
}

/// The per-engine election state. Allocated exactly when the group's mode is
/// [`Hosted`](crate::GroupMode::Hosted).
#[derive(Clone, Debug)]
pub(super) struct Election {
    /// The activation policy and lease length this group was configured with.
    cfg: HostedConfig,
    /// What this node is currently playing.
    role: Role,
    /// The highest epoch this node has *observed* — from its own claims and
    /// from any claim or adopted state a peer taught it. A claim is always
    /// one above this, so a claim never re-litigates a settled epoch.
    highest_seen: u64,
    /// The epoch of the adopted pair. Kept across a demotion, so a later pair
    /// is still fenced against it.
    epoch: u64,
    /// The host of the adopted pair, or `None` when the group is believed
    /// hostless at that epoch.
    host: Option<NodeId>,
    /// The claim this node has standing, if it is a [`Role::Claimant`].
    claim: Option<Claim>,
    /// When this node's authority expires if it stops renewing. Meaningful
    /// only while [`Role::Host`].
    lease_until: Time,
    /// The boot guard: no claim before this instant. Set in
    /// [`GroupEngine::start`] to one [activation
    /// window](Election::activation_window_ms) out, so a node that has just
    /// joined hears the incumbent's state before deciding the group is
    /// vacant.
    no_claim_before: Time,
    /// Row 7's rotation over the dissemination targets — the beacon's **own**
    /// cursor, deliberately not the anti-entropy round's.
    ///
    /// Both fire on the same tick and both would advance one shared cursor by
    /// `anti_entropy_fanout`, so over a candidate list of `2 · fanout` peers
    /// (four, at the default fanout of two) the stride aliases exactly: each
    /// call is pinned to a fixed window and the beacon re-teaches the same
    /// peers forever. The peers outside it never hear the adopted pair again
    /// — which is precisely the node that missed the election.
    beacon_cursor: usize,
    /// The grant ledger and round tally — `Some` exactly under
    /// [`Activation::Quorum`], so a `Settle` group cannot represent a grant at
    /// all. See [`quorum`](self::quorum).
    quorum: Option<QuorumState>,
}

impl Election {
    pub(super) fn new(cfg: HostedConfig) -> Self {
        let quorum = QuorumState::for_activation(&cfg.activation);
        Self {
            cfg,
            role: Role::Follower,
            highest_seen: 0,
            epoch: 0,
            host: None,
            claim: None,
            lease_until: Time::ZERO,
            no_claim_before: Time::ZERO,
            beacon_cursor: 0,
            quorum,
        }
    }

    /// How long this activation's claim window is — the boot guard, and the
    /// span a claim must stand before its window closes. A `match` rather than
    /// a destructuring `let` so a future activation policy fails to compile
    /// here instead of silently inheriting another's timing.
    ///
    /// Under [`Activation::Quorum`] the claim window and the boot guard are
    /// one **lease**, deliberately: the two only need to be long enough that a
    /// claim gets a fair chance to be answered and that a joining node hears
    /// an incumbent out first, and the lease is already sized for exactly that
    /// (it is the number that bounds split-brain, so it comfortably exceeds a
    /// round-trip and the driver's tick). Nothing about Quorum's safety
    /// depends on this number: an epoch is closed by a majority of grants, not
    /// by the passage of time, so a window that is too short or too long costs
    /// election latency and nothing else.
    fn activation_window_ms(&self) -> u64 {
        match &self.cfg.activation {
            Activation::Settle { claim_settle_ms } => *claim_settle_ms,
            Activation::Quorum { .. } => self.cfg.lease_ms,
        }
    }

    /// The instant this node next needs a tick for an election reason — the
    /// settle deadline of a standing claim, or a host's lease expiry.
    pub(super) const fn deadline(&self) -> Option<Time> {
        match self.role {
            Role::Claimant => match self.claim {
                Some(c) => Some(c.settle_at),
                None => None,
            },
            Role::Host => Some(self.lease_until),
            Role::Follower => None,
        }
    }
}

impl GroupEngine {
    /// The `(epoch, host)` pair this node has **adopted** — what it currently
    /// believes the group's leadership to be.
    ///
    /// `(0, None)` before anything has been adopted, and always in an
    /// [`Eventual`](crate::GroupMode::Eventual) group. A `None` host at a
    /// non-zero epoch means the group is believed hostless *at that epoch* (a
    /// lease lapsed, or the incumbent stepped down); the epoch is kept so a
    /// later pair is still fenced against it.
    ///
    /// Observer-local, like every other read on the engine: during a partition
    /// two nodes legitimately return different pairs, and the fencing order
    /// decides which survives the heal.
    #[must_use]
    pub fn leadership(&self) -> (u64, Option<&NodeId>) {
        self.election
            .as_ref()
            .map_or((0, None), |el| (el.epoch, el.host.as_ref()))
    }

    /// What part this node is playing in the election — always
    /// [`Role::Follower`] in an [`Eventual`](crate::GroupMode::Eventual)
    /// group.
    ///
    /// [`Role::Host`] is this node's *own* belief that it holds the group; it
    /// is not proof that peers agree, which is what the lease bounds.
    #[must_use]
    pub fn role(&self) -> Role {
        self.election.as_ref().map_or(Role::Follower, |el| el.role)
    }

    /// The highest epoch this node has observed from any source — its own
    /// claims, a peer's claim, or a peer's adopted state. Never regresses, and
    /// is always at least [`leadership`](Self::leadership)'s epoch. `0` in an
    /// [`Eventual`](crate::GroupMode::Eventual) group.
    #[must_use]
    pub fn observed_epoch(&self) -> u64 {
        self.election.as_ref().map_or(0, |el| el.highest_seen)
    }

    /// When this node's hostship expires if it stops renewing — `Some` exactly
    /// while it is a [`Role::Host`], `None` otherwise.
    ///
    /// A host renews on every tick it is still top-ranked, so in steady state
    /// this reads one `lease_ms` ahead of the last tick.
    #[must_use]
    pub fn host_lease_until(&self) -> Option<Time> {
        self.election
            .as_ref()
            .filter(|el| el.role == Role::Host)
            .map(|el| el.lease_until)
    }

    /// Arms the boot guard: a freshly started node may not claim for one
    /// settle window, so it hears an incumbent's [`wire::Kind::LeadState`]
    /// before concluding the group is vacant. No-op in `Eventual`.
    ///
    /// Under `Quorum` this also arms the **boot blackout** — a freshly started
    /// voter refuses new claimants for one lease, which is what stands in for a
    /// persisted grant when the driver has no store.
    pub(super) fn election_start(&mut self, now: Time) {
        if let Some(el) = self.election.as_mut() {
            el.no_claim_before = now.saturating_add(el.activation_window_ms());
            el.quorum_start(now);
        }
    }

    /// The election's share of a tick. `anti_entropy_due` is whether *this*
    /// tick ran an anti-entropy round, which is the cadence a claim
    /// re-broadcast and a host's state beacon ride.
    pub(super) fn election_tick(&mut self, now: Time, anti_entropy_due: bool) -> Vec<Effect> {
        match self.election.as_ref().map(|el| el.role) {
            Some(Role::Follower) => self.tick_follower(now),
            Some(Role::Claimant) => self.tick_claimant(now, anti_entropy_due),
            Some(Role::Host) => self.tick_host(now, anti_entropy_due),
            None => Vec::new(), // Eventual: no election at all
        }
    }

    /// Row 1: open a claim when this node is the group's top-ranked live
    /// candidate and nothing bars it.
    fn tick_follower(&mut self, now: Time) -> Vec<Effect> {
        let top_ranked = self.is_coordinator();
        let leaving = self.leaving;
        let Some(el) = self.election.as_ref() else {
            return Vec::new();
        };
        let guard = ClaimGuard {
            leaving,
            past_boot_guard: now >= el.no_claim_before,
            top_ranked,
            adopted_host_is_self: el.host.as_ref() == Some(&self.local),
        };
        if !guard.opens() {
            return Vec::new();
        }
        let epoch = el.highest_seen.saturating_add(1);
        let settle_at = now.saturating_add(el.activation_window_ms());
        if let Some(el) = self.election.as_mut() {
            el.highest_seen = epoch;
            el.claim = Some(Claim { epoch, settle_at });
            el.role = Role::Claimant;
        }
        // A claim changes nobody's belief — not even ours — so it emits no
        // effect beyond the frames themselves. Row Q4: under Quorum the claim
        // also opens a grant round, and this node's own grant (if it votes) is
        // written down *before* the claim reaches the wire.
        let mut effects = self.quorum_open_round(epoch, now);
        // A roster of one is closed by that self-grant alone, so the round can
        // be over before the claim is sent — and a bid for an epoch this node
        // has already activated is pure noise. Under `Settle` and under every
        // larger roster the role here is still Claimant, so the broadcast is
        // unchanged.
        if self.role() == Role::Claimant {
            effects.extend(self.broadcast_claim(epoch));
        }
        effects
    }

    /// Rows 2, 3 and 4: abandon on lost rank, activate when the window
    /// closes, otherwise keep the claim visible on the gossip cadence.
    fn tick_claimant(&mut self, now: Time, anti_entropy_due: bool) -> Vec<Effect> {
        let Some(claim) = self.election.as_ref().and_then(|el| el.claim) else {
            self.abandon_claim(); // a Claimant without a claim: repair to Follower
            return Vec::new();
        };
        // Row 2: rank is re-read every tick, so a claim opened when we were
        // top-ranked dies as soon as the view says otherwise.
        if !self.is_coordinator() {
            self.abandon_claim();
            return Vec::new();
        }
        if now >= claim.settle_at {
            // Row Q6: a Quorum window that shut without a majority of grants
            // abandons silently — a claim that never activated changed nobody's
            // belief, so there is nothing to announce. The guard re-fires on the
            // next tick and bids one above the epoch just spent.
            if self.is_quorum() {
                self.abandon_claim();
                return Vec::new();
            }
            // Row 4: under Settle the window closing with the claim still
            // standing *is* the rule that closes the epoch.
            let lease_ms = self.election.as_ref().map_or(0, |el| el.cfg.lease_ms);
            return self.activate(claim.epoch, now.saturating_add(lease_ms));
        }
        // Row Q4b: this node's own grant is re-attempted every tick the round
        // is still open, because the verdict on it flips with time — a
        // successor claiming the instant it buried the dead host is normally
        // still inside its own promise to that host, and Q3 refuses it. Row 3
        // below re-asks the peers; without this nothing ever re-asks us, and a
        // round needing our own vote burns the whole window. A no-op under
        // `Settle` (which has no ledger) and once we are counted.
        let mut effects = self.quorum_retry_self_grant(claim.epoch, now);
        if self.role() != Role::Claimant {
            // The retry closed the round: we are the host now, and the claim
            // the re-offer below would carry no longer exists.
            return effects;
        }
        // Row 3: a claim lost to a dropped frame is re-offered each round.
        if anti_entropy_due {
            effects.extend(self.broadcast_claim(claim.epoch));
        }
        effects
    }

    /// Rows 5, 6 and 7: renew while still top-ranked, self-demote once the
    /// lease lapses, and beacon the adopted pair on the gossip cadence.
    fn tick_host(&mut self, now: Time, anti_entropy_due: bool) -> Vec<Effect> {
        let lease_ms = self.election.as_ref().map_or(0, |el| el.cfg.lease_ms);
        // Row 5: under Settle, renewal is a tick-re-rank — our own view is the
        // evidence. Under Quorum it is not evidence of anything: the lease is
        // extended only by a fresh majority of grants (row Q8), so a host cut
        // off from its voters lapses however top-ranked it still looks to
        // itself. That is the minority side being starved of a host, which is
        // the whole point of the CP posture.
        if self.is_coordinator() && !self.is_quorum() {
            if let Some(el) = self.election.as_mut() {
                el.lease_until = now.saturating_add(lease_ms);
            }
        }
        // Row 6: the lease lapsed (we have not been top-ranked for a whole
        // `lease_ms`). Step down before anyone else can step up.
        if self
            .election
            .as_ref()
            .is_some_and(|el| now >= el.lease_until)
        {
            return self.demote_host();
        }
        if !anti_entropy_due {
            return Vec::new();
        }
        // Row Q7: the Quorum renewal round — a no-op under Settle, whose
        // renewal was the re-rank above.
        let mut effects = self.renewal_round(now);
        // Row 7: repair beacon, so a node that missed the election converges.
        let targets = self.beacon_targets();
        if let Some(body) = self.state_body() {
            effects.extend(
                targets
                    .into_iter()
                    .map(|to| self.send_lead(to, body.clone())),
            );
        }
        effects
    }

    /// Row 7's targets: `anti_entropy_fanout` dissemination targets, rotated on
    /// [`Election::beacon_cursor`] so successive rounds cover every peer.
    ///
    /// Same selection as the anti-entropy round's, on a cursor of its own —
    /// see [`Election::beacon_cursor`] for why sharing one starves peers rather
    /// than rotating past them.
    fn beacon_targets(&mut self) -> Vec<NodeId> {
        let candidates = self.dissemination_targets();
        let n = candidates.len();
        if n == 0 {
            return Vec::new();
        }
        let k = self.config.anti_entropy_fanout.max(1).min(n);
        let start = self.election.as_ref().map_or(0, |el| el.beacon_cursor);
        if let Some(el) = self.election.as_mut() {
            el.beacon_cursor = start.wrapping_add(k);
        }
        (0..k)
            .map(|i| candidates[(start + i) % n].clone())
            .collect()
    }

    /// Row 4's activation: take the epoch, arm the lease, tell the world.
    ///
    /// `lease_until` is passed rather than derived, because the two activations
    /// anchor it differently: Settle's row 4 from the instant the window shut,
    /// Quorum's row Q4 from the instant the claim was *sent* (see
    /// [`quorum`](self::quorum) for why that difference is load-bearing).
    fn activate(&mut self, epoch: u64, lease_until: Time) -> Vec<Effect> {
        let local = self.local.clone();
        if let Some(el) = self.election.as_mut() {
            el.role = Role::Host;
            el.claim = None;
            el.epoch = epoch;
            el.host = Some(local.clone());
            el.highest_seen = el.highest_seen.max(epoch);
            el.lease_until = lease_until;
        }
        let mut effects = vec![Effect::LeadershipChanged {
            epoch,
            host: Some(local),
        }];
        effects.extend(self.broadcast_state());
        effects
    }

    /// Rows 2, 10 and 12: drop a standing claim. Silent — a claim that never
    /// activated changed no one's belief, so there is nothing to notify.
    fn abandon_claim(&mut self) {
        if let Some(el) = self.election.as_mut() {
            el.claim = None;
            el.role = Role::Follower;
        }
    }

    /// Row 6 / row 15: step down, keeping the epoch. The pair becomes
    /// `(epoch, None)`, which any later host's pair still outranks.
    fn demote_host(&mut self) -> Vec<Effect> {
        let Some(el) = self.election.as_mut() else {
            return Vec::new();
        };
        el.role = Role::Follower;
        el.host = None;
        let epoch = el.epoch;
        vec![Effect::LeadershipChanged { epoch, host: None }]
    }

    /// Row 15: a voluntary leave gives up hostship (or a claim) *before* the
    /// leave itself disseminates, so this node never serves an epoch it has
    /// already announced it is gone from.
    pub(super) fn election_on_leave(&mut self) -> Vec<Effect> {
        match self.election.as_ref().map(|el| el.role) {
            Some(Role::Host) => self.demote_host(),
            Some(Role::Claimant) => {
                self.abandon_claim();
                Vec::new()
            }
            Some(Role::Follower) | None => Vec::new(),
        }
    }

    /// Routes an inbound election frame. Kinds 8–10 are dropped outright in an
    /// `Eventual` group — the mode is not a node-wide switch, so a peer that
    /// hosts *its* groups cannot make this one run an election.
    pub(super) fn on_lead(&mut self, from: &NodeId, frame: &wire::Frame, now: Time) -> Vec<Effect> {
        if self.election.is_none() {
            return Vec::new(); // row 16
        }
        match frame.lead.as_ref() {
            Some(wire::LeadBody::Claim { epoch, claimant }) => {
                self.on_lead_claim(from, *epoch, claimant, now)
            }
            Some(wire::LeadBody::State { epoch, host }) => {
                self.on_lead_state(from, *epoch, host.as_ref())
            }
            // Row 14 / rows Q4, Q5, Q8: a grant is meaningless under Settle
            // activation — an epoch is closed there by the settle window, not
            // by endorsements — and is dropped. `Activation::Quorum` is the
            // policy that tallies them.
            Some(wire::LeadBody::Grant {
                epoch,
                claimant,
                granter,
            }) => self.on_lead_grant(*epoch, claimant, granter),
            None => Vec::new(),
        }
    }

    /// Rows 8–11: what a claim does to this node.
    fn on_lead_claim(
        &mut self,
        from: &NodeId,
        epoch: u64,
        claimant: &NodeId,
        now: Time,
    ) -> Vec<Effect> {
        // Row 8: every claim advances what we have seen, so our own next claim
        // outbids it rather than re-litigating an epoch someone is already on.
        if let Some(el) = self.election.as_mut() {
            el.highest_seen = el.highest_seen.max(epoch);
        }
        let mut effects = self.learn_claimant(claimant, now);

        // Row 9: the claimant is behind — teach it the pair we hold rather
        // than letting it settle an epoch we have already passed.
        //
        // "Behind" is measured on the epoch alone, because the epoch alone is
        // the fence: a node holding `(5, None)` — a lapsed lease, a step-down,
        // row 12b's shadow — knows that epochs up to 5 are spent, and staying
        // silent about it would let a claim for 3 settle *inside* a closed
        // epoch and briefly serve it. The one claim a hostless pair does not
        // answer is one at exactly its own epoch: that is the legitimate
        // hostless-recovery bid, and it is how a group with no host gets one
        // again. At equal epochs an adopted *host* is still taught back — the
        // claimant is bidding for an epoch already awarded.
        //
        // Row Q9a carves one case out of that, under Quorum only: a claim at
        // exactly our adopted epoch whose claimant *is* our adopted host is
        // that host renewing, not a stale bid, and teaching it the pair it is
        // renewing would be both noise and a lie. Voters answer it with a
        // re-grant (row Q2) instead.
        let stale = self
            .election
            .as_ref()
            .is_some_and(|el| epoch < el.epoch || (epoch == el.epoch && el.host.is_some()))
            && !self.claim_is_renewal(epoch, claimant);
        if stale {
            if let Some(body) = self.state_body() {
                effects.push(self.send_lead(from.clone(), body));
            }
        }

        // Rows Q1–Q3: the voter's answer. Silent unless this node is in the
        // roster of a Quorum group — and silent then too, whenever the ledger
        // refuses. Ordered after row 9 so a repair and a grant can ride the
        // same batch without either reordering the other.
        effects.extend(self.on_claim_as_voter(epoch, claimant, now));

        // Row 10: a claim that outranks ours ends ours. Both sides apply the
        // same pair order, so exactly one of two duelling claimants yields.
        let outranked = self.election.as_ref().is_some_and(|el| {
            el.role == Role::Claimant
                && el.claim.is_some_and(|c| {
                    cmp_pair(
                        self.group.as_str(),
                        (epoch, Some(claimant)),
                        (c.epoch, Some(&self.local)),
                    ) == Ordering::Greater
                })
        });
        if outranked {
            self.abandon_claim();
        }
        // Row 11: a Host ignores a claim outright, however high its epoch — a
        // claim is a bid, not an activation. Only an adopted pair (row 12)
        // deposes an incumbent, and until then it keeps serving its lease.
        effects
    }

    /// Row 8's membership half: a claim proves its claimant is live, and the
    /// rank every guard here reads must include it. Same shape as
    /// [`Command::AddPeer`](crate::Command::AddPeer) — idempotent, and a known
    /// node (or a `Dead` tombstone) is left exactly as it is.
    fn learn_claimant(&mut self, claimant: &NodeId, now: Time) -> Vec<Effect> {
        if *claimant == self.local || self.members.contains_key(claimant) {
            return Vec::new();
        }
        self.members
            .insert(claimant.clone(), Member::new(0, Status::Alive, now));
        self.stamp(claimant);
        let mut effects = vec![Effect::MembershipChanged];
        effects.extend(self.recompute_coordinator());
        self.nudge_anti_entropy();
        effects
    }

    /// Rows 12, 12b and 13: adopt a pair that outranks ours, learn the epoch of
    /// one that outranks ours by naming *us*, or teach back the one we hold
    /// when it outranks theirs. Equal pairs exchange nothing.
    fn on_lead_state(&mut self, from: &NodeId, epoch: u64, host: Option<&NodeId>) -> Vec<Effect> {
        let names_self = host == Some(&self.local);
        let Some(el) = self.election.as_ref() else {
            return Vec::new();
        };
        let order = cmp_pair(
            self.group.as_str(),
            (epoch, host),
            (el.epoch, el.host.as_ref()),
        );
        match order {
            // Row 12b: a better pair naming us. Hostship is entered only by our
            // own activation, so it is not taken from an echo — the epoch is.
            Ordering::Greater if names_self => self.learn_self_named(epoch),
            // Row 12: a better pair naming somebody else is adopted whole.
            Ordering::Greater => self.adopt(epoch, host),
            // Row 13: we are ahead; repair the sender.
            Ordering::Less => match self.state_body() {
                Some(body) => vec![self.send_lead(from.clone(), body)],
                None => Vec::new(),
            },
            Ordering::Equal => Vec::new(),
        }
    }

    /// Row 12's adoption: take the pair, and stand down from whatever we were
    /// doing that it invalidates.
    fn adopt(&mut self, epoch: u64, host: Option<&NodeId>) -> Vec<Effect> {
        let Some(el) = self.election.as_mut() else {
            return Vec::new();
        };
        let was_host = el.role == Role::Host;
        // A claim at or below the adopted epoch is dead; one strictly above it
        // still stands, and its settle window runs on.
        let outclaimed = el.claim.is_some_and(|c| c.epoch <= epoch);
        el.epoch = epoch;
        el.host = host.cloned();
        el.highest_seen = el.highest_seen.max(epoch);
        if was_host || outclaimed {
            el.role = Role::Follower;
            el.claim = None;
        }
        vec![Effect::LeadershipChanged {
            epoch,
            host: host.cloned(),
        }]
    }

    /// Row 12b: a pair that outranks ours and names **this node** as its host.
    ///
    /// The hostship is still never taken from an echo — but the epoch is, and
    /// that is what closes the restart wedge. A host that comes back at a stale
    /// epoch is told `(3, Some(me))` by every survivor; refusing the pair whole
    /// left it neither adopting *nor learning* that epoch, so the cluster
    /// agreed forever on who held the group and disagreed forever on the fence
    /// that made it authoritative — and, believing itself the adopted host, the
    /// restarted node could not even claim its way out.
    ///
    /// So the pair is taken with its hostship stripped off, as `(epoch, None)`
    /// — the hostless shadow of what was heard — and only while that shadow
    /// still outranks what we hold. Since `(e, None)` is the weakest pair at
    /// epoch `e`, that is exactly when the epoch advances, which makes this rule
    /// monotone in the fencing order: an equal-epoch echo of our own hostship
    /// (the state this very rule leaves behind, re-taught by any peer that
    /// still holds it) changes nothing and announces nothing. From
    /// `(epoch, None)` the ordinary claim guard takes over: no adopted hostship
    /// of our own bars us any more, so a top-ranked node claims `epoch + 1` and
    /// re-earns the group *above* the pair it was fenced by.
    fn learn_self_named(&mut self, epoch: u64) -> Vec<Effect> {
        let Some(el) = self.election.as_mut() else {
            return Vec::new();
        };
        el.highest_seen = el.highest_seen.max(epoch);
        if epoch <= el.epoch {
            return Vec::new(); // the shadow does not outrank what we hold
        }
        let was_host = el.role == Role::Host;
        // Same rule as row 12's: a claim at or below the learned epoch is dead.
        let outclaimed = el.claim.is_some_and(|c| c.epoch <= epoch);
        el.epoch = epoch;
        el.host = None;
        if was_host || outclaimed {
            el.role = Role::Follower;
            el.claim = None;
        }
        vec![Effect::LeadershipChanged { epoch, host: None }]
    }

    /// The adopted pair as a [`wire::LeadBody::State`], or `None` in an
    /// `Eventual` group (which never builds one).
    fn state_body(&self) -> Option<wire::LeadBody> {
        self.election.as_ref().map(|el| wire::LeadBody::State {
            epoch: el.epoch,
            host: el.host.clone(),
        })
    }

    /// A claim to every live member (a `Dead` tombstone is not told; it is not
    /// going to answer, and rank does not count it) — plus, under Quorum, every
    /// voter, live or not: see [`claim_targets`](Self::claim_targets).
    fn broadcast_claim(&self, epoch: u64) -> Vec<Effect> {
        let body = wire::LeadBody::Claim {
            epoch,
            claimant: self.local.clone(),
        };
        self.claim_targets()
            .into_iter()
            .map(|to| self.send_lead(to, body.clone()))
            .collect()
    }

    /// The adopted pair to every live member — what an activation announces.
    fn broadcast_state(&self) -> Vec<Effect> {
        let Some(body) = self.state_body() else {
            return Vec::new();
        };
        self.live_peers()
            .into_iter()
            .map(|to| self.send_lead(to, body.clone()))
            .collect()
    }

    fn live_peers(&self) -> Vec<NodeId> {
        self.probe_candidates().cloned().collect()
    }

    /// Builds a `Send` effect carrying one election frame. The kind is derived
    /// from the body here too, so the pair the codec asserts on can never be
    /// assembled wrong at this end.
    fn send_lead(&self, to: NodeId, body: wire::LeadBody) -> Effect {
        let kind = match body {
            wire::LeadBody::Claim { .. } => wire::Kind::LeadClaim,
            wire::LeadBody::Grant { .. } => wire::Kind::LeadGrant,
            wire::LeadBody::State { .. } => wire::Kind::LeadState,
        };
        Effect::Send {
            to,
            wire: wire::encode(&wire::Frame {
                kind,
                group: self.group.clone(),
                target: None,
                digest: Vec::new(),
                wants: Vec::new(),
                members: Vec::new(),
                metadata: Vec::new(),
                lead: Some(body),
            }),
        }
    }
}
