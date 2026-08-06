//! [`Activation::Quorum`]: closing an epoch on a **majority of a static voter
//! roster** instead of on the passage of time.
//!
//! The skeleton in the parent module is untouched — same rendezvous-ranked
//! candidate, same claim, same `(epoch, host)` fencing order, same lease and
//! step-down. What changes is the one rule that turns a claim into a hostship:
//! under [`Settle`](Activation::Settle) a claim that is still standing when its
//! window shuts activates; under `Quorum` it activates the instant a majority
//! of the roster has *granted* it, and expires silently if the window shuts
//! first.
//!
//! # The rows
//!
//! **The voter half** (rows Q1–Q3, Q15a) is a one-slot ledger — the pair
//! `granted`, plus the `grant_promise_until` instant that pair is promised to:
//!
//! * **Q1 (grant).** A claim whose epoch is strictly above the granted one is
//!   granted, provided it comes from the claimant we already granted (a
//!   claimant may always advance its own epoch) or our promise to the previous
//!   one has expired. Persist, then send.
//! * **Q2 (re-grant).** The exact pair we already granted is re-sent verbatim
//!   and the promise slides — nothing new is written down, because nothing new
//!   was decided. This is what a host's renewal round collects.
//! * **Q3 (refusal).** Everything else is refused **silently**: a lower epoch,
//!   the same epoch under a different claimant (the one-grant-per-epoch rule),
//!   a higher epoch from a stranger while we are still promised to someone
//!   else, and — Q15a — anything at all once this node is leaving. Silence, not
//!   a NACK: a refusal that answered would tell a claimant to try again sooner,
//!   which is precisely the churn the promise exists to damp.
//! * **Q0.** A node outside the roster, and *any* node under `Settle`, never
//!   grants at all.
//!
//! **The candidate half** (rows Q4–Q6) counts them. A claim opens a *round*:
//! the instant it was broadcast (`round_sent_at`) and the set of voters that
//! have answered it (`round_grants`). At `majority()` the claimant activates
//! exactly as row 4 does — with one difference that carries the whole safety
//! argument, below. A window that shuts first abandons silently and the guard
//! re-bids one epoch higher.
//!
//! * **Q4b (the self-grant retry).** A claimant's own grant is re-attempted on
//!   every tick its round is still open, not only when the round is opened.
//!   The shape that makes this necessary is the ordinary one: a successor
//!   claims the instant it has buried the dead host, and at that instant it is
//!   usually still inside its *own* promise to the node it just buried, so Q3
//!   refuses its self-grant. Attempting it once would strand any round that
//!   needs the claimant's own vote until the window shut — a whole `lease_ms`
//!   of failover latency, spent waiting for a verdict that flips on its own a
//!   few ticks later.
//! * **The tally is checked after *every* grant lands in the round**, a
//!   self-grant included — [`close_round_if_majority`](GroupEngine::close_round_if_majority)
//!   is the single place that turns a count into a hostship. A tally read only
//!   when a *peer's* grant arrives leaves a **roster of one** — whose majority
//!   the self-grant alone satisfies — permanently hostless, because no peer
//!   grant is ever coming.
//!
//! **The host half** (rows Q7–Q8) is why renewal costs a round trip here and
//! not under `Settle`. A `Settle` host renews by re-reading its own rank; a
//! `Quorum` host re-broadcasts its adopted epoch as a claim each anti-entropy
//! round and extends its lease only when a majority answers again. A host cut
//! off from its voters therefore lapses (row 6) however top-ranked it still
//! looks to itself — which is the minority side being starved of a host, the
//! entire point of the CP posture.
//!
//! # Send-instant attribution, and why the lease is anchored to it
//!
//! An activated lease runs to `round_sent_at + lease_ms` — **not** to
//! `now + lease_ms` at the moment the majority landed. That is the invariant
//! that makes at most one host per epoch a *time* guarantee and not only a
//! counting one:
//!
//! > a voter that granted at `t` refuses every other claimant until
//! > `t + lease_ms`, and `t` is necessarily at or after the claim was sent, so
//! > `round_sent_at + lease_ms ≤ t + lease_ms` for **every** voter in the
//! > majority.
//!
//! The host's authority therefore expires no later than the promise of the
//! earliest-promising voter that made it host. Anchoring on the arrival of the
//! last grant instead would hand the host a lease outliving the promises it was
//! built from — and two majorities of one roster always intersect, so the
//! overhang is exactly where a second host could appear.
//!
//! Row Q4b's retry is anchored the same way and is sound for the same reason.
//! The premise the argument rests on is only that *every counted grant's
//! instant is at or after `round_sent_at`* — and a retry's instant is strictly
//! after the send, since the round was opened before it. So the promise
//! arithmetic is untouched: the lease a retried round produces is *shorter*
//! than one closed at the send instant (the round's age has already elapsed),
//! never longer, and it is strictly positive whenever the round is still open,
//! because the window is exactly one `lease_ms`. Q7/Q8's renewal round pushes
//! it back out on the very next anti-entropy tick.
//!
//! The one place this is approximate is a *renewal* round (Q8), which resets
//! `round_sent_at` every anti-entropy tick: a grant answering the previous
//! round can be counted into the current one, over-attributing the lease by at
//! most one anti-entropy interval. That is a bounded fraction of a lease that
//! is already sized well above the gossip cadence, and it is stated here rather
//! than papered over. The mis-attribution is impossible at all while the
//! **claim→grant round trip — ≈2·(latency + jitter) — stays under
//! `anti_entropy_interval_ms`**: a grant can only be counted into the wrong
//! round if it is still in flight when the next one opens, and it has a claim's
//! flight time *and* its own to cover before then. It is a round trip that has
//! to fit inside the cadence, not a one-way hop.
//!
//! An election round (Q4) has no such slack: its `round_sent_at` is the instant
//! the claim was *opened* and row 3's re-offers deliberately do not move it.
//!
//! # Durability, and what recovery actually buys
//!
//! A voter that grants, crashes, and restarts inside a claim window could
//! otherwise grant a second claimant for the same epoch. Two defences, in
//! order of strength:
//!
//! * **Persisted grants.** [`Effect::PersistGrant`] is emitted before the grant
//!   frame is sent, and [`GroupEngine::with_recovered`] restores the pair on
//!   boot. This is the posture for any deployment with a store.
//! * **The boot blackout.** A freshly started voter arms
//!   `grant_promise_until` one `lease_ms` out, so it refuses every *new*
//!   claimant for a full lease after boot — long enough for any grant it might
//!   have made before the crash to have expired. This is the storage-free
//!   fallback: a timing rule rather than a durability one, and honest about
//!   being so.
//!
//! [`Activation::Quorum`]: crate::Activation::Quorum
//! [`Effect::PersistGrant`]: crate::Effect::PersistGrant

use std::collections::BTreeSet;

use crate::config::{Activation, VoterRoster};
use crate::{Config, Effect, GroupEngine, GroupId, NodeId, Time, wire};

use super::{Election, Role};

/// The Quorum-activation state: this node's voter ledger and the grant round it
/// is currently collecting. Allocated exactly when the group's activation is
/// [`Activation::Quorum`], so a `Settle` group cannot even represent a grant.
#[derive(Clone, Debug)]
pub(super) struct QuorumState {
    /// The one `(epoch, claimant)` pair this voter has granted — the whole
    /// ledger, because a voter grants at most one claimant per epoch and never
    /// goes backwards. `None` until it has granted anything.
    granted: Option<(u64, NodeId)>,
    /// When this node's promise to `granted`'s claimant expires. Until then a
    /// *different* claimant is refused however high its epoch; the claimant we
    /// already granted is exempt, so a host can always advance its own epoch.
    ///
    /// [`Time::MAX`] is the **unarmed** state: an engine that has not been
    /// started has never observed a clock and grants nothing.
    /// [`GroupEngine::start`] lowers it to one lease out (the boot blackout);
    /// [`RecoveredGrant::none`] writes [`Time::ZERO`] instead, which `start`
    /// leaves alone, because storage has attested there is nothing to black
    /// out.
    grant_promise_until: Time,
    /// When the claim of the round currently being collected was broadcast —
    /// the instant an activated lease is anchored to. See the module doc.
    round_sent_at: Time,
    /// The voters whose grants this round has collected, this node's own
    /// self-grant included. Reset when a round opens.
    round_grants: BTreeSet<NodeId>,
}

/// What a voter does with one inbound claim: rows Q1, Q2 and Q3.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Verdict {
    /// Q1: a new pair. Write it down, then answer it.
    Fresh,
    /// Q2: the pair already granted, asked for again. Answer it again; there is
    /// nothing new to write down.
    Repeat,
    /// Q3 / Q15a: refused, silently.
    Refuse,
}

/// Rows Q1–Q3 as one pure predicate over the ledger — the auditable form of the
/// only rule that decides whether an epoch can be closed.
///
/// Ordered so each row's veto is visible: leaving first, then the idempotent
/// re-grant, then monotonicity (which subsumes one-grant-per-epoch: an equal
/// epoch under a *different* claimant is not an advance), then the promise.
fn grant_verdict(
    leaving: bool,
    granted: Option<&(u64, NodeId)>,
    grant_promise_until: Time,
    now: Time,
    epoch: u64,
    claimant: &NodeId,
) -> Verdict {
    if leaving {
        return Verdict::Refuse; // Q15a
    }
    let Some((granted_epoch, granted_claimant)) = granted else {
        // Nothing granted yet: the promise window is the only gate, and it is
        // the boot blackout unless storage attested otherwise.
        return if now >= grant_promise_until {
            Verdict::Fresh
        } else {
            Verdict::Refuse
        };
    };
    if *granted_epoch == epoch && granted_claimant == claimant {
        return Verdict::Repeat; // Q2
    }
    if epoch <= *granted_epoch {
        return Verdict::Refuse; // Q3: monotone, and one grant per epoch
    }
    if granted_claimant == claimant || now >= grant_promise_until {
        Verdict::Fresh // Q1
    } else {
        Verdict::Refuse // Q3: promised to somebody else, and the promise stands
    }
}

impl QuorumState {
    /// The state for `activation` — `Some` exactly under
    /// [`Activation::Quorum`]. A `match` rather than an `if let`, so a future
    /// activation policy has to decide here rather than silently inherit.
    pub(super) fn for_activation(activation: &Activation) -> Option<Self> {
        match activation {
            // `External` allocates its epochs at the anchor, so there is
            // nothing to grant and no ledger to keep — the anchor *is* the
            // ledger. Structurally inert, exactly as `Settle` is.
            Activation::Settle { .. } | Activation::External { .. } => None,
            Activation::Quorum { .. } => Some(Self {
                granted: None,
                // Unarmed until `start`; see the field doc.
                grant_promise_until: Time::MAX,
                round_sent_at: Time::ZERO,
                round_grants: BTreeSet::new(),
            }),
        }
    }
}

/// What a restarting voter's storage says it had granted before the crash —
/// the durable half of Quorum's one-grant-per-epoch rule.
///
/// Handed to [`GroupEngine::with_recovered`]. Meaningful **only** under
/// [`Activation::Quorum`]: a `Settle` or
/// [`Eventual`](crate::GroupMode::Eventual) group has no voter ledger to
/// restore and ignores it entirely.
///
/// # Recovery restores the pair, not the time
///
/// A recovered voter still applies the boot-anchored blackout to any **new**
/// claimant — `start + lease_ms`, exactly as a storage-free one does. That is
/// deliberate, and it is conservative rather than lazy: the store records
/// *what* was granted, never *when*, and boot is at or after the crash, which
/// is at or after the grant, so a blackout measured from boot always covers the
/// promise the pre-crash grant implied.
///
/// What recovery buys is therefore not a shorter blackout. It is:
///
/// * **Epoch uniqueness across restarts.** The recovered pair is a floor: this
///   voter can never grant that epoch to a second claimant, whatever the
///   blackout says, and however long the process was down.
/// * **Immediate availability to the incumbent.** The claimant named in the
///   recovered pair is exempt from the promise (a claimant may always advance
///   its own epoch), so a restarted voter can re-grant the *sitting* host at
///   once instead of starving it for a lease.
///
/// [`RecoveredGrant::none`] is the stronger statement of the two — storage
/// attesting this voter has **never** granted — and it is what lifts the boot
/// blackout altogether.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveredGrant(Option<(u64, NodeId)>);

impl RecoveredGrant {
    /// Storage attests this voter has never granted: no pair to restore, and no
    /// boot blackout either — there is provably nothing to black out.
    ///
    /// Only a driver that really did persist every grant may say this. A driver
    /// with no store must not: it would be claiming a durability it does not
    /// have, and the blackout is the only thing standing in for it.
    #[must_use]
    pub const fn none() -> Self {
        Self(None)
    }

    /// Storage attests this voter last granted `epoch` to `claimant`.
    ///
    /// Restores the pair as a floor; the boot blackout still applies to every
    /// *other* claimant. See the type doc for what that does and does not buy.
    #[must_use]
    pub const fn granted(epoch: u64, claimant: NodeId) -> Self {
        Self(Some((epoch, claimant)))
    }
}

impl Election {
    /// The roster this group's epochs are closed by — `Some` exactly under
    /// [`Activation::Quorum`].
    pub(super) fn voters(&self) -> Option<&VoterRoster> {
        match &self.cfg.activation {
            // No roster under `External` either: consensus is outsourced to
            // the anchor, so there is nobody in the fabric to count.
            Activation::Settle { .. } | Activation::External { .. } => None,
            Activation::Quorum { voters } => Some(voters),
        }
    }

    /// Arms the boot blackout at `start`: a freshly booted voter refuses every
    /// new claimant for one lease, which is the storage-free stand-in for a
    /// persisted grant. A promise a recovery already settled ([`Time::ZERO`],
    /// from [`RecoveredGrant::none`]) is left exactly as it is.
    pub(super) fn quorum_start(&mut self, now: Time) {
        let lease_ms = self.cfg.lease_ms;
        if let Some(q) = self.quorum.as_mut() {
            if q.grant_promise_until == Time::MAX {
                q.grant_promise_until = now.saturating_add(lease_ms);
            }
        }
    }
}

impl GroupEngine {
    /// Builds an engine that **recovers a voter's persisted grant**, for a
    /// driver that provides durability under
    /// [`Activation::Quorum`](crate::Activation::Quorum).
    ///
    /// Identical to [`new`](GroupEngine::new) in every other respect, and
    /// identical to it *outright* unless the group's activation is `Quorum` —
    /// a `Settle` or [`Eventual`](crate::GroupMode::Eventual) group has no
    /// voter ledger, so `recovered` is ignored rather than half-applied.
    ///
    /// See [`RecoveredGrant`] for the contract, and in particular for why
    /// recovery restores the granted **pair** and not the instant it was
    /// granted at.
    #[must_use]
    pub fn with_recovered(
        group: GroupId,
        local: NodeId,
        seeds: impl IntoIterator<Item = NodeId>,
        config: Config,
        recovered: RecoveredGrant,
    ) -> Self {
        let mut engine = Self::new(group, local, seeds, config);
        if let Some(q) = engine.quorum_mut() {
            match recovered.0 {
                // Attested never-granted: nothing to restore, and the boot
                // blackout is what durability was standing in for.
                None => q.grant_promise_until = Time::ZERO,
                // The pair is a floor; the blackout still runs from `start`.
                Some(pair) => q.granted = Some(pair),
            }
        }
        engine
    }

    /// The `(epoch, claimant)` pair this node has granted as a voter, if any.
    ///
    /// `None` outside [`Activation::Quorum`](crate::Activation::Quorum), for a
    /// node outside the voter roster, and for a voter that has not yet granted
    /// anything. A driver providing durability writes this pair down — though
    /// it should do so from [`Effect::PersistGrant`], which is ordered against
    /// the grant frame, rather than by polling this.
    #[must_use]
    pub fn voter_grant(&self) -> Option<(u64, &NodeId)> {
        self.election
            .as_ref()
            .and_then(|el| el.quorum.as_ref())
            .and_then(|q| q.granted.as_ref())
            .map(|(epoch, claimant)| (*epoch, claimant))
    }

    /// The voter roster, `Some` exactly under `Quorum` activation.
    fn quorum_voters(&self) -> Option<&VoterRoster> {
        self.election.as_ref().and_then(Election::voters)
    }

    /// Whether this group closes its epochs by grant majority.
    pub(super) fn is_quorum(&self) -> bool {
        self.quorum_voters().is_some()
    }

    /// Row Q0: whether this node ever grants at all.
    fn is_voter(&self) -> bool {
        self.quorum_voters()
            .is_some_and(|voters| voters.contains(&self.local))
    }

    fn quorum_mut(&mut self) -> Option<&mut QuorumState> {
        self.election.as_mut().and_then(|el| el.quorum.as_mut())
    }

    /// Rows Q1–Q3 applied to the ledger, reported rather than announced: the
    /// caller shapes the effects, because a self-grant writes itself down
    /// without putting a frame on the wire while a peer's grant does both.
    fn record_grant(&mut self, epoch: u64, claimant: &NodeId, now: Time) -> Verdict {
        if !self.is_voter() {
            return Verdict::Refuse; // Q0
        }
        let leaving = self.leaving;
        let lease_ms = self.election.as_ref().map_or(0, |el| el.cfg.lease_ms);
        let Some(q) = self.quorum_mut() else {
            return Verdict::Refuse;
        };
        let verdict = grant_verdict(
            leaving,
            q.granted.as_ref(),
            q.grant_promise_until,
            now,
            epoch,
            claimant,
        );
        match verdict {
            Verdict::Refuse => return verdict,
            // Q1: the new pair is written down before anything is answered.
            Verdict::Fresh => q.granted = Some((epoch, claimant.clone())),
            // Q2: nothing new to write down.
            Verdict::Repeat => {}
        }
        // Both a grant and a re-grant renew the promise: the claimant answered
        // is the one this voter is committed to for the next lease.
        q.grant_promise_until = now.saturating_add(lease_ms);
        verdict
    }

    /// Rows Q1–Q3 on an inbound claim: the voter's answer, or silence.
    ///
    /// The effect order is the write-ahead contract — [`Effect::PersistGrant`]
    /// first, the grant frame second. A driver with storage must not send
    /// before the persist completes.
    pub(super) fn on_claim_as_voter(
        &mut self,
        epoch: u64,
        claimant: &NodeId,
        now: Time,
    ) -> Vec<Effect> {
        let verdict = self.record_grant(epoch, claimant, now);
        if verdict == Verdict::Refuse {
            return Vec::new(); // Q3: silence, not a NACK
        }
        let mut effects = Vec::new();
        if verdict == Verdict::Fresh {
            effects.push(Effect::PersistGrant {
                epoch,
                claimant: claimant.clone(),
            });
        }
        effects.push(self.send_lead(
            claimant.clone(),
            wire::LeadBody::Grant {
                epoch,
                claimant: claimant.clone(),
                granter: self.local.clone(),
            },
        ));
        effects
    }

    /// Whether this node's own grant is already counted in the round it is
    /// collecting — the guard row Q4b's retry reads.
    fn self_in_round(&self) -> bool {
        self.election
            .as_ref()
            .and_then(|el| el.quorum.as_ref())
            .is_some_and(|q| q.round_grants.contains(&self.local))
    }

    /// This node's own grant for its own claim, counted straight into the round
    /// rather than sent to itself. Refused exactly as a peer's would be — a
    /// candidate still promised to *another* claimant may bid, but may not
    /// count itself.
    ///
    /// The tally is checked here as well as in
    /// [`on_lead_grant`](Self::on_lead_grant), because a self-grant is a grant:
    /// a roster whose majority it alone satisfies has no peer grant coming to
    /// trigger the check. See
    /// [`close_round_if_majority`](Self::close_round_if_majority).
    fn self_grant(&mut self, epoch: u64, now: Time) -> Vec<Effect> {
        let local = self.local.clone();
        let verdict = self.record_grant(epoch, &local, now);
        if verdict == Verdict::Refuse {
            return Vec::new();
        }
        if let Some(q) = self.quorum_mut() {
            q.round_grants.insert(local.clone());
        }
        let mut effects = match verdict {
            Verdict::Fresh => vec![Effect::PersistGrant {
                epoch,
                claimant: local,
            }],
            Verdict::Repeat | Verdict::Refuse => Vec::new(),
        };
        effects.extend(self.close_round_if_majority(epoch));
        effects
    }

    /// Row Q4b: re-attempt this node's own grant for a round of its own that is
    /// still open. Called on every Claimant tick; a no-op under `Settle`, for a
    /// node outside the roster (row Q0), and for a claimant already counted in
    /// its own round.
    ///
    /// # Why once is not enough
    ///
    /// The claim guard and the grant ledger answer to different clocks. A
    /// successor claims as soon as it has *buried* the dead host, and it grants
    /// only once its own **promise** to that host has run out — and after a
    /// crash the promise is normally the later of the two (it lapses one
    /// `lease_ms` after the host's last collected grant, while detection is
    /// sized well under a lease). So the self-grant at row Q4's round open is
    /// refused by Q3 in exactly the common case, and a round that needs the
    /// claimant's own vote could never close: row 3 re-offers the claim to the
    /// *peers* every anti-entropy round but re-asked nobody at home. The whole
    /// window burnt before the guard re-bid one epoch higher with a fresh
    /// round — a `lease_ms` of failover latency for a verdict that flips on its
    /// own a few ticks in.
    ///
    /// # Why the retry keeps the send-instant anchor exact
    ///
    /// An activation is still anchored to `round_sent_at`, not to the retry.
    /// The safety argument (see the module doc) needs only that every counted
    /// grant was made at or after `round_sent_at`, and a retry's instant is
    /// *strictly* after it — the round was opened first. The promise arithmetic
    /// is therefore unchanged, and the lease a retried round yields is shorter
    /// than one closed at the send instant rather than longer. It is still
    /// strictly positive, because the window a retry can happen in is exactly
    /// one `lease_ms` wide, and rows Q7/Q8 push it back out on the next
    /// anti-entropy tick.
    pub(super) fn quorum_retry_self_grant(&mut self, epoch: u64, now: Time) -> Vec<Effect> {
        if !self.is_voter() || self.self_in_round() {
            return Vec::new();
        }
        self.self_grant(epoch, now)
    }

    /// The one place a round's tally becomes a hostship — shared by every entry
    /// point that can put a grant into the round: a peer's grant arriving (rows
    /// Q4/Q8), the self-grant a claim opens with (Q4), its re-attempt (Q4b) and
    /// the self-grant a renewal round opens with (Q7).
    ///
    /// Anchored on `round_sent_at` in every one of them, which is the invariant
    /// the module doc's safety argument is built on.
    fn close_round_if_majority(&mut self, epoch: u64) -> Vec<Effect> {
        let Some(el) = self.election.as_ref() else {
            return Vec::new();
        };
        let role = el.role;
        let majority = el.voters().map_or(usize::MAX, VoterRoster::majority);
        let lease_ms = el.cfg.lease_ms;
        let Some(q) = el.quorum.as_ref() else {
            return Vec::new();
        };
        if q.round_grants.len() < majority {
            return Vec::new();
        }
        let lease_until = q.round_sent_at.saturating_add(lease_ms);
        match role {
            // Q4: the epoch is closed. Activation is row 4's, to the byte,
            // except that the lease is anchored to the send instant.
            Role::Claimant => self.activate(epoch, lease_until),
            // Q8: the renewal round confirmed. A round can only ever push the
            // lease out, never pull it in.
            Role::Host => {
                if let Some(el) = self.election.as_mut() {
                    el.lease_until = el.lease_until.max(lease_until);
                }
                Vec::new()
            }
            Role::Follower => Vec::new(),
        }
    }

    /// Row Q4's bookkeeping: a fresh claim opens a grant round anchored to this
    /// instant, and this node's own grant is the first one in it.
    ///
    /// Returns before the claim is broadcast, so the self-grant's
    /// [`Effect::PersistGrant`] precedes the claim on the wire — a voter that
    /// crashes mid-election never has a claim outstanding that its own store
    /// cannot account for. A no-op under `Settle`.
    ///
    /// On a **roster of one** the round is already closed when this returns:
    /// the self-grant is a majority, so the effects carry the activation too
    /// and the caller has no claim left to broadcast.
    pub(super) fn quorum_open_round(&mut self, epoch: u64, now: Time) -> Vec<Effect> {
        let Some(q) = self.quorum_mut() else {
            return Vec::new();
        };
        q.round_sent_at = now;
        q.round_grants.clear();
        self.self_grant(epoch, now)
    }

    /// Row Q4's targets: every live peer **and** every voter, whether or not
    /// gossip has shown it alive. A roster member this node has never heard
    /// from is exactly the grant it cannot afford to skip.
    ///
    /// Under `Settle` this is the live peer set unchanged.
    pub(super) fn claim_targets(&self) -> Vec<NodeId> {
        let live = self.live_peers();
        let Some(voters) = self.quorum_voters() else {
            return live;
        };
        let mut targets: BTreeSet<NodeId> = live.into_iter().collect();
        targets.extend(voters.iter().filter(|v| **v != self.local).cloned());
        targets.into_iter().collect()
    }

    /// Row Q9a: whether an inbound claim is the adopted host **renewing** its
    /// own epoch rather than bidding for a spent one.
    ///
    /// Row 9 would teach such a claim back the very pair it is renewing, which
    /// under Quorum is both noise and a lie about the claimant being behind.
    /// The voter answers it with a grant (Q2) instead. Gated on Quorum, because
    /// a `Settle` host never re-claims its epoch and the row must stay
    /// byte-identical there.
    pub(super) fn claim_is_renewal(&self, epoch: u64, claimant: &NodeId) -> bool {
        self.is_quorum()
            && self
                .election
                .as_ref()
                .is_some_and(|el| el.epoch == epoch && el.host.as_ref() == Some(claimant))
    }

    /// Rows Q4, Q5 and Q8: an inbound grant, dropped or counted.
    ///
    /// A counted grant either activates a standing claim (Q4) or extends a
    /// host's lease (Q8); both are anchored to the round's send instant, never
    /// to the arrival of the grant that closed it.
    pub(super) fn on_lead_grant(
        &mut self,
        epoch: u64,
        claimant: &NodeId,
        granter: &NodeId,
    ) -> Vec<Effect> {
        // Row 14 / Q0: `Settle` closes an epoch by time, not by endorsements,
        // and an `Eventual` group has no election at all.
        if !self.is_quorum() {
            return Vec::new();
        }
        // Q5: a grant is addressed evidence — for us, from a voter, about an
        // epoch we are actually running a round for. Anything else is noise
        // from a stale round, a third party's election, or a stranger.
        if *claimant != self.local {
            return Vec::new();
        }
        if !self
            .quorum_voters()
            .is_some_and(|voters| voters.contains(granter))
        {
            return Vec::new();
        }
        let Some(el) = self.election.as_ref() else {
            return Vec::new();
        };
        let role = el.role;
        let standing = match role {
            Role::Claimant => el.claim.is_some_and(|c| c.epoch == epoch),
            Role::Host => el.epoch == epoch,
            Role::Follower => false,
        };
        if !standing {
            return Vec::new();
        }
        if let Some(q) = self.quorum_mut() {
            q.round_grants.insert(granter.clone());
        }
        self.close_round_if_majority(epoch)
    }

    /// Row Q7: a host's renewal round. Re-claims the epoch it already holds
    /// from the roster on the anti-entropy cadence, and counts its own grant
    /// into the round.
    ///
    /// A no-op under `Settle` (whose renewal is row 5's tick-re-rank), and for
    /// a host that has lost rank — a host that no longer ranks should be
    /// letting its lease lapse, not asking the roster to extend it.
    ///
    /// On a **roster of one** the round's own self-grant is already a majority,
    /// so the lease is extended here rather than by an answer that is never
    /// coming — and a lost rank still lapses it, because this returns before it
    /// gets that far.
    pub(super) fn renewal_round(&mut self, now: Time) -> Vec<Effect> {
        if !self.is_quorum() || !self.is_coordinator() {
            return Vec::new();
        }
        let Some(el) = self.election.as_ref() else {
            return Vec::new();
        };
        if el.role != Role::Host {
            return Vec::new();
        }
        let epoch = el.epoch;
        let targets: Vec<NodeId> = el.voters().map_or_else(Vec::new, |voters| {
            voters
                .iter()
                .filter(|v| **v != self.local)
                .cloned()
                .collect()
        });
        if let Some(q) = self.quorum_mut() {
            q.round_sent_at = now;
            q.round_grants.clear();
        }
        let mut effects = self.self_grant(epoch, now);
        let body = wire::LeadBody::Claim {
            epoch,
            claimant: self.local.clone(),
        };
        effects.extend(
            targets
                .into_iter()
                .map(|to| self.send_lead(to, body.clone())),
        );
        effects
    }
}

#[cfg(test)]
mod tests {
    use super::{QuorumState, Verdict, grant_verdict};
    use crate::config::{Activation, VoterRoster};
    use crate::{NodeId, Time};

    fn n(id: &str) -> NodeId {
        NodeId::new(id)
    }

    /// `(5, "b")` granted, promised until `t=100`.
    fn granted() -> (u64, NodeId) {
        (5, n("b"))
    }

    const PROMISED_UNTIL: Time = Time(100);

    fn verdict(now: u64, epoch: u64, claimant: &str) -> Verdict {
        grant_verdict(
            false,
            Some(&granted()),
            PROMISED_UNTIL,
            Time(now),
            epoch,
            &n(claimant),
        )
    }

    /// Q1: a strictly higher epoch from a stranger is granted once the promise
    /// to the incumbent has run out — and not one millisecond before.
    #[test]
    fn a_stranger_waits_out_the_promise_exactly() {
        assert_eq!(verdict(99, 6, "c"), Verdict::Refuse);
        assert_eq!(verdict(100, 6, "c"), Verdict::Fresh);
        assert_eq!(verdict(101, 6, "c"), Verdict::Fresh);
    }

    /// Q1's exemption: the claimant we already granted may advance its own
    /// epoch immediately. Without it a restarted voter would starve the sitting
    /// host for a whole lease — and there is nothing to protect, since granting
    /// the same claimant a higher epoch cannot produce a second host.
    #[test]
    fn the_granted_claimant_advances_its_own_epoch_inside_the_promise() {
        assert_eq!(verdict(0, 6, "b"), Verdict::Fresh);
        assert_eq!(verdict(99, 9_999, "b"), Verdict::Fresh);
    }

    /// Q2: the exact pair again is a re-grant, whatever the promise says —
    /// answering it a second time decides nothing new, which is what makes a
    /// host's renewal round free of writes.
    #[test]
    fn the_same_pair_is_always_a_re_grant() {
        assert_eq!(verdict(0, 5, "b"), Verdict::Repeat);
        assert_eq!(verdict(1_000, 5, "b"), Verdict::Repeat);
    }

    /// Q3: one grant per epoch. The same epoch under a different claimant is
    /// refused for ever — not merely until the promise lapses, which is the
    /// distinction the whole one-host-per-epoch guarantee rests on.
    #[test]
    fn a_rival_at_the_granted_epoch_is_refused_for_ever() {
        for now in [0, 99, 100, 10_000] {
            assert_eq!(verdict(now, 5, "c"), Verdict::Refuse, "at {now}");
        }
    }

    /// Q3: monotone. Anything below the granted epoch is refused whoever asks
    /// and whenever, incumbent included.
    #[test]
    fn nothing_below_the_granted_epoch_is_ever_granted() {
        for claimant in ["b", "c"] {
            for epoch in [0, 4] {
                for now in [0, 10_000] {
                    assert_eq!(
                        verdict(now, epoch, claimant),
                        Verdict::Refuse,
                        "{claimant} at epoch {epoch}, t={now}"
                    );
                }
            }
        }
    }

    /// An unwritten ledger is gated by the promise window alone — which is the
    /// boot blackout on a fresh voter, and nothing at all once storage has
    /// attested there is nothing to black out.
    #[test]
    fn an_empty_ledger_is_gated_only_by_the_blackout() {
        let blackout =
            |now: u64, until: Time| grant_verdict(false, None, until, Time(now), 1, &n("c"));
        assert_eq!(blackout(499, Time(500)), Verdict::Refuse);
        assert_eq!(blackout(500, Time(500)), Verdict::Fresh);
        assert_eq!(blackout(0, Time::ZERO), Verdict::Fresh, "attested none");
        // The unarmed sentinel: no instant a driver can plausibly reach passes
        // it, which is what makes an unstarted voter refuse everything.
        assert_eq!(
            blackout(u64::MAX - 1, Time::MAX),
            Verdict::Refuse,
            "unarmed"
        );
    }

    /// Q15a: leaving vetoes every row above it. A node on its way out must not
    /// hand out authority it will not be present to fence.
    #[test]
    fn leaving_vetoes_every_row() {
        for (epoch, claimant) in [(5, "b"), (6, "b"), (6, "c"), (4, "b")] {
            assert_eq!(
                grant_verdict(
                    true,
                    Some(&granted()),
                    PROMISED_UNTIL,
                    Time(10_000),
                    epoch,
                    &n(claimant)
                ),
                Verdict::Refuse,
                "({epoch}, {claimant})"
            );
            assert_eq!(
                grant_verdict(true, None, Time::ZERO, Time(10_000), epoch, &n(claimant)),
                Verdict::Refuse,
                "({epoch}, {claimant}) with an empty ledger"
            );
        }
    }

    /// The state exists exactly under Quorum activation — a `Settle` group
    /// cannot represent a grant at all, which is what makes row Q0's "Settle
    /// engines never grant" structural rather than conventional.
    #[test]
    fn the_state_is_allocated_only_under_quorum() {
        assert!(
            QuorumState::for_activation(&Activation::Settle {
                claim_settle_ms: 500
            })
            .is_none()
        );
        let quorum = QuorumState::for_activation(&Activation::Quorum {
            voters: VoterRoster::new([n("a")]),
        })
        .expect("quorum allocates");
        assert_eq!(quorum.granted, None);
        assert!(quorum.round_grants.is_empty());
        assert_eq!(
            quorum.grant_promise_until,
            Time::MAX,
            "an unstarted voter must grant nothing"
        );
    }
}
