//! `Activation::Quorum`: the **grant round's closing rule** — row Q4b's
//! self-grant retry, and the tally check that every entry point shares.
//!
//! Split out of `election_quorum.rs` (which owns the ledger rows Q1–Q3 and the
//! ordinary Q4–Q8 arithmetic) because these two are about *when* a round is
//! re-examined rather than about what a single grant decides:
//!
//! * **Q4b.** A claimant's own grant is attempted at round open and re-attempted
//!   on every tick the round is still open, because the verdict on it moves with
//!   time. The lease is still anchored to the instant the claim was **sent** —
//!   the retry buys latency, never authority.
//! * **The shared tally.** A self-grant is a grant, so the majority check runs
//!   after it too. On a **roster of one** that is the only check there will ever
//!   be: no peer grant is coming to trigger the one in `on_lead_grant`.
//!
//! Every test drives a real engine, so what is asserted is the behaviour a peer
//! (and a driver's store) would actually observe.

use groupnet_core::{Effect, GroupEngine, NodeId, Role, Status, Time};
use groupnet_testkit::frames::quorum_voter_engine as voter;
use groupnet_testkit::frames::*;

/// The lease every fixture here runs on. Under Quorum this one number is the
/// lease, the claim window, the boot guard, and the post-boot grant blackout.
const LEASE: u64 = 500;

/// The instant the promise-blocked claim is opened — the anchor its lease must
/// run from, whatever instant actually closes the round.
const SENT_AT: u64 = 900;
/// The instant the promise made to the rival lapses, and so the first instant
/// row Q4b's retry can succeed. Strictly inside the window, which shuts at
/// `SENT_AT + LEASE`.
const RETRY_AT: u64 = 1_000;

fn claim_frame(epoch: u64, claimant: &NodeId) -> Vec<u8> {
    lead_claim_frame(epoch, claimant.as_str())
}

// --------------------------------------------------------------------------
// Row Q4b: the self-grant, re-attempted.
// --------------------------------------------------------------------------

/// **Row Q4b.** A claimant whose own grant is refused at round open — the
/// ordinary shape after a host crash, where the successor is still inside its
/// own promise to the node it just buried — re-attempts it every tick and closes
/// the round the moment the promise lapses. And the lease it activates on is
/// anchored to the instant the claim was **sent**, not to the retry that closed
/// it.
///
/// The timeline, all inside one window:
///
/// ```text
/// 500   grant epoch 5 to a rival  -> promised to it until 1000
/// 900   claim epoch 6 opens       -> self-grant REFUSED (Q3), window shuts 1400
/// 910   one peer grant arrives    -> 1 of the 2 this roster needs
/// 950   tick, promise still live  -> nothing: the retry is refused too
/// 1000  tick, promise lapsed      -> retry GRANTS, round closes, host
/// ```
///
/// Without the retry the round is stuck at one grant until 1400, when the window
/// shuts and the guard re-bids one epoch higher with a fresh round — a whole
/// `lease_ms` of failover latency for a verdict that flipped on its own at 1000.
#[test]
fn a_promise_blocked_claimant_retries_its_self_grant_until_the_promise_lapses() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let mut e = voter(&rank[0], &rank[1..], &rank, LEASE);

    // Granted to a rival first, so our own claim is refused by our own ledger.
    e.on_message(rank[1].clone(), &claim_frame(5, &rank[1]), Time(LEASE));
    assert_eq!(e.voter_grant(), Some((5, &rank[1])), "promised until 1000");

    // The claim opens well inside that promise: the self-grant is refused and
    // the round is anchored to *this* instant regardless.
    let opened = e.on_tick(Time(SENT_AT));
    assert_eq!(e.role(), Role::Claimant);
    assert!(
        persisted_grants(&opened).is_empty(),
        "the self-grant was refused, so nothing was written down"
    );
    for (_, epoch, claimant) in claim_frames(&opened) {
        assert_eq!((epoch, claimant), (6, rank[0].clone()));
    }

    // One peer grant: half of the majority of three, and all this round can
    // collect from elsewhere.
    let one = e.on_message(
        rank[2].clone(),
        &lead_grant_frame(6, rank[0].as_str(), rank[2].as_str()),
        Time(SENT_AT + 10),
    );
    assert!(
        leadership_changes(&one).is_empty(),
        "one grant is not a majority while the candidate cannot count itself"
    );

    // The regression half of row Q3: a tick *before* the promise lapses must
    // still refuse — the retry re-asks the ledger, it does not overrule it.
    let early = e.on_tick(Time(950));
    assert!(
        persisted_grants(&early).is_empty(),
        "the retry granted inside a live promise"
    );
    assert!(leadership_changes(&early).is_empty());
    assert_eq!(e.role(), Role::Claimant);
    assert_eq!(e.voter_grant(), Some((5, &rank[1])), "the ledger is intact");

    // The promise lapses exactly at RETRY_AT, and the retry on that tick closes
    // the round.
    let closed = e.on_tick(Time(RETRY_AT));
    assert_eq!(
        persisted_grants(&closed),
        vec![(6, rank[0].clone())],
        "the retry is a fresh grant, and it is written down like any other"
    );
    assert_eq!(
        leadership_changes(&closed),
        vec![(6, Some(rank[0].clone()))]
    );
    assert!(
        closed
            .iter()
            .position(|e| matches!(e, Effect::PersistGrant { .. }))
            < closed
                .iter()
                .position(|e| matches!(e, Effect::LeadershipChanged { .. })),
        "the persist must precede the activation it licensed"
    );
    assert_eq!(e.role(), Role::Host);
    assert_eq!(e.voter_grant(), Some((6, &rank[0])));
    assert_eq!(
        e.host_lease_until(),
        Some(Time(SENT_AT + LEASE)),
        "the lease is anchored to the claim's send instant, not to the retry"
    );
    assert_ne!(
        e.host_lease_until(),
        Some(Time(RETRY_AT + LEASE)),
        "anchoring on the retry would hand the host an overhang past the \
         promises the round was built from"
    );
    assert!(
        claim_frames(&closed).is_empty(),
        "a round the retry closed must not also re-offer the claim it closed"
    );
}

/// The retry is attempted only while it can still matter: a claimant already
/// counted in its own round never re-grants, so the promise it made is not slid
/// forward tick after tick for nothing.
#[test]
fn a_claimant_already_in_its_own_round_does_not_re_grant() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let mut e = voter(&rank[0], &rank[1..], &rank, LEASE);

    let opened = e.on_tick(Time(LEASE));
    assert_eq!(
        persisted_grants(&opened),
        vec![(1, rank[0].clone())],
        "the round opened with our own grant in it"
    );

    for at in [LEASE + 1, LEASE + 100, 2 * LEASE - 1] {
        let tick = e.on_tick(Time(at));
        assert!(
            persisted_grants(&tick).is_empty(),
            "re-granted a round it is already counted in, at {at}"
        );
        assert_eq!(e.role(), Role::Claimant, "at {at}");
    }
}

// --------------------------------------------------------------------------
// The shared tally: a roster of one.
// --------------------------------------------------------------------------

/// A **roster of one** activates on its own self-grant, at the instant the claim
/// opens — its majority is one, and the only grant it will ever see is its own.
///
/// The round is therefore already closed when the claim would have been
/// broadcast, so nothing bids for an epoch this node has just taken; what goes
/// on the wire is the activation.
#[test]
fn a_single_voter_roster_activates_on_its_own_grant_at_claim_open() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let solo = [rank[0].clone()];
    let mut e = voter(&rank[0], &rank[1..], &solo, LEASE);

    let opened = e.on_tick(Time(LEASE)); // the blackout lapses exactly here
    assert_eq!(
        persisted_grants(&opened),
        vec![(1, rank[0].clone())],
        "the self-grant is written down before anything else happens"
    );
    assert_eq!(
        leadership_changes(&opened),
        vec![(1, Some(rank[0].clone()))],
        "and it is a majority of one, so the epoch closes on it"
    );
    assert!(
        opened
            .iter()
            .position(|e| matches!(e, Effect::PersistGrant { .. }))
            < opened
                .iter()
                .position(|e| matches!(e, Effect::LeadershipChanged { .. })),
        "the write-ahead order holds here too"
    );
    assert_eq!(e.role(), Role::Host);
    assert_eq!(e.leadership(), (1, Some(&rank[0])));
    assert_eq!(e.voter_grant(), Some((1, &rank[0])));
    assert_eq!(
        e.host_lease_until(),
        Some(Time(2 * LEASE)),
        "anchored to the send instant, exactly as a multi-voter round is"
    );

    assert!(
        claim_frames(&opened).is_empty(),
        "the round was over before the claim was sent: bidding for an epoch \
         already activated is pure noise"
    );
    let announced = state_frames(&opened);
    assert_eq!(announced.len(), 2, "the new pair still goes to every peer");
    for (to, epoch, host) in announced {
        assert!(rank[1..].contains(&to));
        assert_eq!((epoch, host), (1, Some(rank[0].clone())));
    }
}

/// The same roster renews through rounds nobody answers: the renewal round's own
/// self-grant is its majority, so the lease moves on the anti-entropy cadence
/// with not one frame on the wire — and the host outlives the lease it activated
/// on, which is what a tally checked only on an inbound grant could never do.
#[test]
fn a_single_voter_roster_renews_through_its_own_rounds() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let solo = [rank[0].clone()];
    let mut e = voter(&rank[0], &rank[1..], &solo, LEASE);

    e.on_tick(Time(LEASE));
    assert_eq!(e.host_lease_until(), Some(Time(2 * LEASE)));

    // Well past the lease it activated on: without renewal it would demote at
    // 2·LEASE, so reaching the end of this loop as a Host *is* the assertion.
    let mut first_renewal = None;
    for at in (LEASE + 100..=4 * LEASE).step_by(100) {
        let round = e.on_tick(Time(at));
        assert!(
            claim_frames(&round).is_empty(),
            "a roster of one has nobody to ask, yet asked at {at}"
        );
        assert!(
            leadership_changes(&round).is_empty(),
            "the group changed hands at {at}"
        );
        assert!(
            persisted_grants(&round).is_empty(),
            "a renewal re-grants the pair it already holds, so it writes \
             nothing down — but did at {at}"
        );
        assert_eq!(e.role(), Role::Host, "stopped hosting at {at}");
        let until = e.host_lease_until().expect("a host has a lease");
        if until > Time(2 * LEASE) && first_renewal.is_none() {
            first_renewal = Some((at, until));
        }
    }

    let (at, until) = first_renewal.expect("no renewal round ever extended the lease");
    assert_eq!(
        until,
        Time(at + LEASE),
        "a renewal round anchors on its own send instant, like every other round"
    );
    assert!(
        e.host_lease_until().is_some_and(|u| u > Time(4 * LEASE)),
        "the lease must still be running out ahead of the clock"
    );
}

/// A roster of one is not a licence to host for ever: renewal is still gated on
/// rank, so a better-ranked member turning up stops the rounds and the lease
/// lapses on schedule.
///
/// The roster is `{rank[1]}` over a four-node cluster, so the sole voter can be
/// outranked by a node that cannot vote at all — hosting and voting are
/// independent.
#[test]
fn a_single_voter_roster_still_demotes_when_it_loses_rank() {
    let rank = rank_by_placement(&["a", "b", "c", "d"]);
    let solo = [rank[1].clone()];
    let mut e = voter(&rank[1], &rank[2..], &solo, LEASE);

    e.on_tick(Time(LEASE));
    assert_eq!(e.role(), Role::Host);
    assert_eq!(e.host_lease_until(), Some(Time(2 * LEASE)));

    // A better-ranked member turns up — and it is not in the roster, so the
    // group's only voter is now its second-best candidate.
    e.on_message(
        rank[0].clone(),
        &digest_frame(vec![ndigest(rank[0].as_str(), 0, Status::Alive, 0)], vec![]),
        Time(LEASE + 10),
    );
    assert_eq!(e.coordinator(), Some(&rank[0]));

    for at in (LEASE + 100..2 * LEASE).step_by(100) {
        let round = e.on_tick(Time(at));
        assert!(
            leadership_changes(&round).is_empty(),
            "stepped down early, at {at}"
        );
        assert_eq!(
            e.host_lease_until(),
            Some(Time(2 * LEASE)),
            "an outranked host must not renew itself, and did at {at}"
        );
    }

    let lapsed = e.on_tick(Time(2 * LEASE));
    assert_eq!(leadership_changes(&lapsed), vec![(1, None)]);
    assert_eq!(e.role(), Role::Follower);
    assert_eq!(e.host_lease_until(), None);
    assert_eq!(
        e.leadership(),
        (1, None),
        "the epoch is kept, so a later pair is still fenced against it"
    );
}

/// The tally is a *majority*, not "any grant": a two-voter roster needs both, so
/// a self-grant alone closes nothing. Without this the shared check would read
/// as "one grant activates", which is the misreading the roster-of-one case
/// invites.
#[test]
fn a_two_voter_roster_is_not_closed_by_the_self_grant_alone() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let pair = [rank[0].clone(), rank[1].clone()];
    let mut e: GroupEngine = voter(&rank[0], &rank[1..], &pair, LEASE);

    let opened = e.on_tick(Time(LEASE));
    assert_eq!(persisted_grants(&opened), vec![(1, rank[0].clone())]);
    assert!(
        leadership_changes(&opened).is_empty(),
        "one of two voters is not a majority"
    );
    assert_eq!(e.role(), Role::Claimant);
    assert!(
        !claim_frames(&opened).is_empty(),
        "and the claim still has to be asked"
    );

    let closed = e.on_message(
        rank[1].clone(),
        &lead_grant_frame(1, rank[0].as_str(), rank[1].as_str()),
        Time(LEASE + 20),
    );
    assert_eq!(
        leadership_changes(&closed),
        vec![(1, Some(rank[0].clone()))]
    );
    assert_eq!(e.host_lease_until(), Some(Time(2 * LEASE)));
}
