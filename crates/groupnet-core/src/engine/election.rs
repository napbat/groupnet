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
//!   epoch-major, and at equal epochs the [`placement::owner`] of the group id
//!   among the two hosts wins. That tiebreak reads nothing but the group id
//!   and the two host ids, so it is view-independent: every node, on either
//!   side of a heal, picks the same survivor without exchanging anything.
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
//! [`Config::mode`]: crate::Config::mode
//! [`GroupMode::Hosted`]: crate::GroupMode::Hosted
//! [`Eventual`]: crate::GroupMode::Eventual
//! [`coordinator`]: crate::GroupEngine::coordinator
//! [`LeadershipChanged`]: crate::Effect::LeadershipChanged

use std::cmp::Ordering;
use std::collections::BTreeSet;

use crate::config::{Activation, HostedConfig};
use crate::membership::{Member, Status};
use crate::{NodeId, Time, placement, wire};

use super::effect::Effect;
use super::state::GroupEngine;

/// What part the local node is playing in its group's election right now.
///
/// Always [`Follower`](Role::Follower) in an
/// [`Eventual`](crate::GroupMode::Eventual) group, which runs no election.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// Neither claiming nor hosting: following whatever pair this node has
    /// adopted (which may be no host at all).
    Follower,
    /// A claim of this node's own is standing, waiting out its settle window.
    Claimant,
    /// This node activated its claim and holds the group for the adopted
    /// epoch, until its lease lapses or a higher pair fences it.
    Host,
}

/// A standing bid for an epoch: what row 1 opens and row 4 closes.
#[derive(Clone, Copy, Debug)]
struct Claim {
    /// The epoch being claimed (`highest_seen + 1` at the moment of the bid).
    epoch: u64,
    /// When the claim activates if nothing has outranked it by then.
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
    /// [`GroupEngine::start`] to one settle window out, so a node that has
    /// just joined hears the incumbent's state before deciding the group is
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
}

impl Election {
    pub(super) fn new(cfg: HostedConfig) -> Self {
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
        }
    }

    /// The configured settle window. A `match` rather than a destructuring
    /// `let` so a future activation policy fails to compile here instead of
    /// silently inheriting Settle's timing.
    const fn settle_ms(&self) -> u64 {
        match self.cfg.activation {
            Activation::Settle { claim_settle_ms } => claim_settle_ms,
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

/// The fencing order over `(epoch, host)` pairs, as a total order.
///
/// Epoch-major; at equal epochs a `None` host sorts below any `Some` (a
/// hostless belief never displaces a live one), and two named hosts are
/// separated by the [`placement::owner`] of `group` among just those two.
/// That tiebreak is a pure function of the group id and the two ids, so it is
/// view-independent: every node agrees on it, whatever it believes about
/// membership.
fn cmp_pair(group: &str, a: (u64, Option<&NodeId>), b: (u64, Option<&NodeId>)) -> Ordering {
    match a.0.cmp(&b.0) {
        Ordering::Equal => {}
        by_epoch => return by_epoch,
    }
    match (a.1, b.1) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(x), Some(y)) if x == y => Ordering::Equal,
        (Some(x), Some(y)) => {
            let pair: BTreeSet<NodeId> = [x.clone(), y.clone()].into_iter().collect();
            if placement::owner(group, &pair).as_ref() == Some(x) {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
    }
}

/// Everything row 1's claim guard reads, gathered so the rule itself is one
/// auditable expression rather than a condition smeared across a function.
#[derive(Clone, Copy, Debug)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "this type IS the guard's truth table — four independent vetoes, each \
              enumerated by the unit test. Two-variant enums would say the same thing \
              at four times the length and make the table unreadable."
)]
struct ClaimGuard {
    /// This node has voluntarily left.
    leaving: bool,
    /// The boot guard has elapsed.
    past_boot_guard: bool,
    /// This node is the group's top-ranked live candidate.
    top_ranked: bool,
    /// The adopted pair already names this node as host.
    adopted_host_is_self: bool,
}

impl ClaimGuard {
    /// Whether a claim may be opened: only by a top-ranked live candidate that
    /// has not left, is past its boot guard, and does not already believe
    /// *itself* to be the adopted host — that belief is entered only by
    /// activating, and re-claiming on top of it would churn the epoch for
    /// nothing.
    const fn opens(self) -> bool {
        !self.leaving && self.past_boot_guard && self.top_ranked && !self.adopted_host_is_self
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
    pub(super) fn election_start(&mut self, now: Time) {
        if let Some(el) = self.election.as_mut() {
            el.no_claim_before = now.saturating_add(el.settle_ms());
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
        let settle_at = now.saturating_add(el.settle_ms());
        if let Some(el) = self.election.as_mut() {
            el.highest_seen = epoch;
            el.claim = Some(Claim { epoch, settle_at });
            el.role = Role::Claimant;
        }
        // A claim changes nobody's belief — not even ours — so it emits no
        // effect beyond the frames themselves.
        self.broadcast_claim(epoch)
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
        // Row 4: the window closed with the claim still standing.
        if now >= claim.settle_at {
            return self.activate(claim.epoch, now);
        }
        // Row 3: a claim lost to a dropped frame is re-offered each round.
        if anti_entropy_due {
            return self.broadcast_claim(claim.epoch);
        }
        Vec::new()
    }

    /// Rows 5, 6 and 7: renew while still top-ranked, self-demote once the
    /// lease lapses, and beacon the adopted pair on the gossip cadence.
    fn tick_host(&mut self, now: Time, anti_entropy_due: bool) -> Vec<Effect> {
        let lease_ms = self.election.as_ref().map_or(0, |el| el.cfg.lease_ms);
        // Row 5: renewal is a tick-re-rank — our own view is the evidence.
        if self.is_coordinator() {
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
        // Row 7: repair beacon, so a node that missed the election converges.
        if !anti_entropy_due {
            return Vec::new();
        }
        let targets = self.beacon_targets();
        let Some(body) = self.state_body() else {
            return Vec::new();
        };
        targets
            .into_iter()
            .map(|to| self.send_lead(to, body.clone()))
            .collect()
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
    fn activate(&mut self, epoch: u64, now: Time) -> Vec<Effect> {
        let local = self.local.clone();
        let lease_ms = self.election.as_ref().map_or(0, |el| el.cfg.lease_ms);
        if let Some(el) = self.election.as_mut() {
            el.role = Role::Host;
            el.claim = None;
            el.epoch = epoch;
            el.host = Some(local.clone());
            el.highest_seen = el.highest_seen.max(epoch);
            el.lease_until = now.saturating_add(lease_ms);
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
            // Row 14: a grant is meaningless under Settle activation — an
            // epoch is closed by the settle window, not by endorsements.
            // Reserved for `Activation::Quorum` (M3), which tallies them.
            Some(wire::LeadBody::Grant { .. }) | None => Vec::new(),
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
        let stale = self
            .election
            .as_ref()
            .is_some_and(|el| epoch < el.epoch || (epoch == el.epoch && el.host.is_some()));
        if stale {
            if let Some(body) = self.state_body() {
                effects.push(self.send_lead(from.clone(), body));
            }
        }

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
    /// going to answer, and rank does not count it).
    fn broadcast_claim(&self, epoch: u64) -> Vec<Effect> {
        let body = wire::LeadBody::Claim {
            epoch,
            claimant: self.local.clone(),
        };
        self.live_peers()
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

#[cfg(test)]
mod tests {
    use super::{ClaimGuard, Ordering, cmp_pair};
    use crate::{NodeId, placement};
    use std::collections::BTreeSet;

    const GROUP: &str = "g";

    fn n(id: &str) -> NodeId {
        NodeId::new(id)
    }

    /// The two ids of a pair, ranked: the one `placement::owner` picks for the
    /// group first. This is exactly the tiebreak `cmp_pair` applies.
    fn ranked_pair(a: &str, b: &str) -> (NodeId, NodeId) {
        let (a, b) = (n(a), n(b));
        let set: BTreeSet<NodeId> = [a.clone(), b.clone()].into_iter().collect();
        let winner = placement::owner(GROUP, &set).expect("two candidates");
        let loser = if winner == a { b } else { a };
        (winner, loser)
    }

    /// Epoch dominates the host entirely: a higher epoch wins even when the
    /// host it names would lose the equal-epoch tiebreak, and even when it
    /// names no host at all.
    #[test]
    fn the_order_is_epoch_major() {
        let (winner, loser) = ranked_pair("x", "y");
        assert_eq!(
            cmp_pair(GROUP, (8, Some(&loser)), (7, Some(&winner))),
            Ordering::Greater
        );
        assert_eq!(
            cmp_pair(GROUP, (8, None), (7, Some(&winner))),
            Ordering::Greater
        );
        assert_eq!(
            cmp_pair(GROUP, (7, Some(&winner)), (8, Some(&loser))),
            Ordering::Less
        );
    }

    /// At equal epochs the placement owner of the group among the two hosts
    /// wins — the view-independent tiebreak that lets both sides of a heal
    /// agree without exchanging anything.
    #[test]
    fn equal_epochs_break_by_placement_owner() {
        let (winner, loser) = ranked_pair("x", "y");
        assert_eq!(
            cmp_pair(GROUP, (7, Some(&winner)), (7, Some(&loser))),
            Ordering::Greater
        );
        assert_eq!(
            cmp_pair(GROUP, (7, Some(&loser)), (7, Some(&winner))),
            Ordering::Less
        );
        assert_eq!(
            cmp_pair(GROUP, (7, Some(&winner)), (7, Some(&winner))),
            Ordering::Equal
        );
    }

    /// A hostless belief never displaces a live one at the same epoch, and two
    /// hostless beliefs are indistinguishable.
    #[test]
    fn none_sorts_below_some_at_equal_epoch() {
        let host = n("x");
        assert_eq!(cmp_pair(GROUP, (7, None), (7, Some(&host))), Ordering::Less);
        assert_eq!(
            cmp_pair(GROUP, (7, Some(&host)), (7, None)),
            Ordering::Greater
        );
        assert_eq!(cmp_pair(GROUP, (7, None), (7, None)), Ordering::Equal);
    }

    /// The order really is a total order over a spread of pairs: antisymmetric
    /// and transitive. Anything less and two nodes could disagree about which
    /// of two pairs survives a heal.
    #[test]
    fn the_order_is_total_and_transitive() {
        let ids: Vec<Option<NodeId>> = [None, Some(n("x")), Some(n("y")), Some(n("z"))].into();
        let pairs: Vec<(u64, Option<&NodeId>)> = (6u64..=8)
            .flat_map(|e| ids.iter().map(move |h| (e, h.as_ref())))
            .collect();
        for a in &pairs {
            for b in &pairs {
                assert_eq!(
                    cmp_pair(GROUP, *a, *b).reverse(),
                    cmp_pair(GROUP, *b, *a),
                    "asymmetry at {a:?} vs {b:?}"
                );
                for c in &pairs {
                    let (ab, bc) = (cmp_pair(GROUP, *a, *b), cmp_pair(GROUP, *b, *c));
                    if ab == bc && ab != Ordering::Equal {
                        assert_eq!(
                            cmp_pair(GROUP, *a, *c),
                            ab,
                            "intransitive at {a:?} {b:?} {c:?}"
                        );
                    }
                }
            }
        }
    }

    /// The whole claim guard, tabulated: exactly one of the sixteen states
    /// opens a claim, and each of the four inputs vetoes on its own.
    #[test]
    fn only_a_live_top_ranked_node_past_its_boot_guard_may_claim() {
        for leaving in [false, true] {
            for past_boot_guard in [false, true] {
                for top_ranked in [false, true] {
                    for adopted_host_is_self in [false, true] {
                        let guard = ClaimGuard {
                            leaving,
                            past_boot_guard,
                            top_ranked,
                            adopted_host_is_self,
                        };
                        let want =
                            !leaving && past_boot_guard && top_ranked && !adopted_host_is_self;
                        assert_eq!(guard.opens(), want, "{guard:?}");
                    }
                }
            }
        }
        // The one state that claims, spelled out rather than inferred.
        assert!(
            ClaimGuard {
                leaving: false,
                past_boot_guard: true,
                top_ranked: true,
                adopted_host_is_self: false,
            }
            .opens()
        );
    }
}
