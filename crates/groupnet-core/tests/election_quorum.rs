//! `Activation::Quorum`: the voter ledger (rows Q1–Q3, Q15a), the candidate's
//! grant round (Q4–Q6), the host's renewal round (Q7–Q8), row 9's renewal
//! carve-out (Q9a), and the boot blackout / recovered-grant postures.
//!
//! Every test drives a real engine through the fixture frames, so what is
//! asserted is the wire behaviour a peer would actually observe — and, for the
//! durability rows, the exact effect *order* a driver with a store must honour.

use groupnet_core::{
    Command, Config, Effect, GroupEngine, NodeId, RecoveredGrant, Role, Status, Time, wire,
};
use groupnet_testkit::frames::quorum_voter_engine as voter;
use groupnet_testkit::frames::*;

/// The lease every fixture here runs on. Under Quorum this one number is the
/// lease, the claim window, the boot guard, and the post-boot grant blackout.
const LEASE: u64 = 500;

/// Where the first effect satisfying `pred` sits in the batch — the order
/// assertions the write-ahead contract is made of.
fn position(effects: &[Effect], pred: impl Fn(&Effect) -> bool) -> usize {
    effects
        .iter()
        .position(pred)
        .expect("the effect the order is asserted on")
}

fn is_grant_send(effect: &Effect) -> bool {
    match effect {
        Effect::Send { wire, .. } => wire::decode(wire)
            .and_then(|f| f.lead)
            .is_some_and(|b| matches!(b, wire::LeadBody::Grant { .. })),
        _ => false,
    }
}

fn is_claim_send(effect: &Effect) -> bool {
    match effect {
        Effect::Send { wire, .. } => wire::decode(wire)
            .and_then(|f| f.lead)
            .is_some_and(|b| matches!(b, wire::LeadBody::Claim { .. })),
        _ => false,
    }
}

// --------------------------------------------------------------------------
// The voter half: rows Q1, Q2, Q3, Q15a, Q0.
// --------------------------------------------------------------------------

/// Row Q1. A voter past its blackout grants the first claim it hears, and the
/// persist **precedes** the frame — a grant that reached the wire but not the
/// store is exactly the double-grant a crash-restart turns into two hosts.
#[test]
fn a_grant_is_persisted_before_it_is_sent() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let mut e = voter(&rank[0], &rank[1..], &rank, LEASE);

    let effects = e.on_message(
        rank[1].clone(),
        &lead_claim_frame(4, rank[1].as_str()),
        Time(LEASE),
    );
    assert_eq!(
        persisted_grants(&effects),
        vec![(4, rank[1].clone())],
        "a new pair is written down"
    );
    assert_eq!(
        grant_frames(&effects),
        vec![(rank[1].clone(), 4, rank[1].clone(), rank[0].clone())],
        "and answered to the claimant, naming this node as granter"
    );
    assert!(
        position(&effects, |e| matches!(e, Effect::PersistGrant { .. }))
            < position(&effects, is_grant_send),
        "the persist must precede the send it is write-ahead of"
    );
    assert_eq!(e.voter_grant(), Some((4, &rank[1])));
    assert_eq!(
        e.leadership(),
        (0, None),
        "granting adopts nothing — a grant is a promise, not a belief"
    );
    assert_eq!(e.observed_epoch(), 4, "row 8 still learns the epoch");
}

/// Row Q3, the one-grant-per-epoch half: a second claimant at the epoch already
/// granted is refused, silently and for ever — no frame, and nothing written
/// down. Two majorities of one roster always intersect, so this rule alone is
/// what makes two hosts at one epoch impossible.
#[test]
fn a_rival_at_the_granted_epoch_is_refused_silently() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let mut e = voter(&rank[0], &rank[1..], &rank, LEASE);
    e.on_message(
        rank[1].clone(),
        &lead_claim_frame(4, rank[1].as_str()),
        Time(LEASE),
    );

    for at in [LEASE + 1, LEASE * 10, LEASE * 100] {
        let effects = e.on_message(
            rank[2].clone(),
            &lead_claim_frame(4, rank[2].as_str()),
            Time(at),
        );
        assert!(
            grant_frames(&effects).is_empty(),
            "answered a rival at {at}"
        );
        assert!(
            persisted_grants(&effects).is_empty(),
            "wrote a rival down at {at}"
        );
        assert_eq!(e.voter_grant(), Some((4, &rank[1])), "at {at}");
    }
}

/// Row Q2, and the promise it slides. The identical pair asked for again is
/// re-answered without a persist — nothing new was decided — and the promise
/// runs a fresh lease from *that* answer, which is what keeps a renewing host's
/// voters committed to it.
#[test]
fn a_re_grant_re_answers_without_persisting_and_slides_the_promise() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let mut e = voter(&rank[0], &rank[1..], &rank, LEASE);
    let claim = |epoch: u64, id: &NodeId| lead_claim_frame(epoch, id.as_str());

    e.on_message(rank[1].clone(), &claim(4, &rank[1]), Time(LEASE)); // promise -> 1000

    let again = e.on_message(rank[1].clone(), &claim(4, &rank[1]), Time(800));
    assert_eq!(
        grant_frames(&again),
        vec![(rank[1].clone(), 4, rank[1].clone(), rank[0].clone())],
        "a re-grant is answered identically"
    );
    assert!(
        persisted_grants(&again).is_empty(),
        "a re-grant decides nothing new, so it writes nothing down"
    );

    // The promise now runs to 800 + LEASE, not to the original 1000.
    let early = e.on_message(rank[2].clone(), &claim(5, &rank[2]), Time(1_000));
    assert!(
        grant_frames(&early).is_empty(),
        "the slid promise must still bar a rival at the original expiry"
    );
    let late = e.on_message(rank[2].clone(), &claim(5, &rank[2]), Time(800 + LEASE));
    assert_eq!(
        grant_frames(&late),
        vec![(rank[2].clone(), 5, rank[2].clone(), rank[0].clone())],
        "and must release it exactly when it lapses"
    );
    assert_eq!(persisted_grants(&late), vec![(5, rank[2].clone())]);
    assert_eq!(e.voter_grant(), Some((5, &rank[2])));
}

/// Row Q3's monotone half: nothing at or below the granted epoch is ever
/// granted, whoever asks and however long the promise has been spent.
#[test]
fn nothing_below_the_granted_epoch_is_ever_granted() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let mut e = voter(&rank[0], &rank[1..], &rank, LEASE);
    e.on_message(
        rank[1].clone(),
        &lead_claim_frame(4, rank[1].as_str()),
        Time(LEASE),
    );

    for claimant in [&rank[1], &rank[2]] {
        for epoch in [1, 3] {
            let effects = e.on_message(
                claimant.clone(),
                &lead_claim_frame(epoch, claimant.as_str()),
                Time(100_000), // long past every promise
            );
            assert!(
                grant_frames(&effects).is_empty(),
                "{claimant:?} was granted epoch {epoch}, below the granted 4"
            );
        }
    }
    assert_eq!(e.voter_grant(), Some((4, &rank[1])));
}

/// Row Q1's exemption: the claimant already granted may advance its own epoch
/// immediately, promise or no promise. Granting the same node a higher epoch
/// cannot produce a second host, and refusing would starve the sitting host of
/// its own renewal for a whole lease.
#[test]
fn the_granted_claimant_advances_its_epoch_inside_its_own_promise() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let mut e = voter(&rank[0], &rank[1..], &rank, LEASE);
    e.on_message(
        rank[1].clone(),
        &lead_claim_frame(4, rank[1].as_str()),
        Time(LEASE),
    );

    let advanced = e.on_message(
        rank[1].clone(),
        &lead_claim_frame(5, rank[1].as_str()),
        Time(LEASE + 1), // deep inside the promise
    );
    assert_eq!(persisted_grants(&advanced), vec![(5, rank[1].clone())]);
    assert_eq!(
        grant_frames(&advanced),
        vec![(rank[1].clone(), 5, rank[1].clone(), rank[0].clone())]
    );
    assert_eq!(e.voter_grant(), Some((5, &rank[1])));
}

/// The three boot postures, side by side on the same first claim.
///
/// * **Blackout** (no store): refuses for one lease after `start`, which is the
///   timing rule that stands in for durability.
/// * **Recovered-none** (storage attests never-granted): no blackout at all.
/// * **Recovered-some**: the pair is a floor and the blackout still bars *new*
///   claimants — but the recovered claimant is exempt, so the sitting host is
///   re-grantable at once.
#[test]
fn the_boot_blackout_and_the_two_recovered_postures() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let cfg = || quorum_config(&["a", "b", "c"], LEASE);
    let claim = |epoch: u64, id: &NodeId| lead_claim_frame(epoch, id.as_str());
    let start = |mut e: GroupEngine| {
        learn_peers(&mut e, &rank[1..], Time::ZERO);
        e.start(Time::ZERO);
        e
    };

    // --- Blackout. ---
    let mut blackout = voter(&rank[0], &rank[1..], &rank, LEASE);
    assert_eq!(blackout.voter_grant(), None);
    assert!(
        grant_frames(&blackout.on_message(rank[1].clone(), &claim(4, &rank[1]), Time(LEASE - 1)))
            .is_empty(),
        "a freshly booted voter must refuse for a whole lease"
    );
    assert!(
        !grant_frames(&blackout.on_message(rank[1].clone(), &claim(4, &rank[1]), Time(LEASE)))
            .is_empty(),
        "and grant the instant the blackout lapses"
    );

    // --- Recovered: attested never-granted. ---
    let mut fresh = start(recovered_engine(
        rank[0].as_str(),
        &[],
        cfg(),
        RecoveredGrant::none(),
    ));
    assert_eq!(fresh.voter_grant(), None);
    assert_eq!(
        grant_frames(&fresh.on_message(rank[1].clone(), &claim(4, &rank[1]), Time::ZERO)).len(),
        1,
        "storage attested there is nothing to black out"
    );

    // --- Recovered: a pair to honour. ---
    let mut restored = start(recovered_engine(
        rank[0].as_str(),
        &[],
        cfg(),
        RecoveredGrant::granted(4, rank[1].clone()),
    ));
    assert_eq!(
        restored.voter_grant(),
        Some((4, &rank[1])),
        "the pair survives the restart"
    );
    assert!(
        grant_frames(&restored.on_message(rank[2].clone(), &claim(5, &rank[2]), Time::ZERO))
            .is_empty(),
        "recovery restores the pair, not the time: a new claimant still waits out the blackout"
    );
    for at in [1, 10 * LEASE] {
        assert!(
            grant_frames(&restored.on_message(rank[2].clone(), &claim(4, &rank[2]), Time(at)))
                .is_empty(),
            "the recovered epoch is a floor that outlives the blackout (t={at})"
        );
    }
    assert_eq!(
        grant_frames(&restored.on_message(rank[1].clone(), &claim(5, &rank[1]), Time(2))).len(),
        1,
        "the recovered claimant is exempt, so the sitting host is re-grantable at once — \
         inside the blackout and without waiting it out"
    );
}

/// Row Q0, both halves. A node outside the roster never grants however
/// perfectly the claim reads, and a `Settle` engine never grants at all — the
/// state that would record one is not even allocated there.
#[test]
fn a_non_voter_and_a_settle_engine_never_grant() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let outsider = NodeId::new("outsider");
    let peers = [rank[0].clone(), rank[1].clone(), rank[2].clone()];

    let mut non_voter = voter(&outsider, &peers, &rank, LEASE);
    let effects = non_voter.on_message(
        rank[1].clone(),
        &lead_claim_frame(4, rank[1].as_str()),
        Time(LEASE),
    );
    assert!(grant_frames(&effects).is_empty(), "a non-voter granted");
    assert!(persisted_grants(&effects).is_empty());
    assert_eq!(non_voter.voter_grant(), None);

    let mut settle = hosted_engine(rank[0].as_str(), &[], 500, LEASE);
    learn_peers(&mut settle, &rank[1..], Time::ZERO);
    settle.start(Time::ZERO);
    let effects = settle.on_message(
        rank[1].clone(),
        &lead_claim_frame(4, rank[1].as_str()),
        Time(LEASE),
    );
    assert!(grant_frames(&effects).is_empty(), "a Settle engine granted");
    assert!(persisted_grants(&effects).is_empty());
    assert_eq!(settle.voter_grant(), None);
}

/// Row Q15a. A node on its way out refuses every grant — it must not hand out
/// authority it will not be present to fence.
#[test]
fn a_leaving_voter_refuses() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let mut e = voter(&rank[0], &rank[1..], &rank, LEASE);
    e.apply(Command::Leave);

    let effects = e.on_message(
        rank[1].clone(),
        &lead_claim_frame(4, rank[1].as_str()),
        Time(LEASE),
    );
    assert!(grant_frames(&effects).is_empty());
    assert!(persisted_grants(&effects).is_empty());
    assert_eq!(e.voter_grant(), None);
}

// --------------------------------------------------------------------------
// The candidate half: rows Q4, Q5, Q6.
// --------------------------------------------------------------------------

/// Row Q4. A majority of the roster closes the epoch, and the lease is anchored
/// to the instant the claim was **sent** — not to the arrival of the grant that
/// closed it. Every voter in the majority granted at or after that instant, so
/// its promise outlives the lease it created; anchoring on arrival would hand
/// the host an overhang no voter is promised through.
#[test]
fn a_majority_activates_with_a_lease_anchored_to_the_send_instant() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let mut e = voter(&rank[0], &rank[1..], &rank, LEASE);

    let opened = e.on_tick(Time(LEASE)); // the claim's send instant
    assert_eq!(e.role(), Role::Claimant);
    assert_eq!(
        persisted_grants(&opened),
        vec![(1, rank[0].clone())],
        "the self-grant is written down"
    );
    assert!(
        position(&opened, |e| matches!(e, Effect::PersistGrant { .. }))
            < position(&opened, is_claim_send),
        "and written down before the claim it belongs to reaches the wire"
    );

    // One peer grant is a majority of three, counting our own.
    let closed = e.on_message(
        rank[1].clone(),
        &lead_grant_frame(1, rank[0].as_str(), rank[1].as_str()),
        Time(LEASE + 137), // late arrival: the lease must not follow it
    );
    assert_eq!(
        leadership_changes(&closed),
        vec![(1, Some(rank[0].clone()))]
    );
    assert_eq!(e.role(), Role::Host);
    assert_eq!(e.leadership(), (1, Some(&rank[0])));
    assert_eq!(
        e.host_lease_until(),
        Some(Time(LEASE + LEASE)),
        "the lease runs from the send instant, not from the grant's arrival"
    );
    let announced = state_frames(&closed);
    assert_eq!(announced.len(), 2, "the new pair goes to every live member");
    for (to, epoch, host) in announced {
        assert!(rank[1..].contains(&to));
        assert_eq!((epoch, host), (1, Some(rank[0].clone())));
    }
}

/// Row Q4's targets. A claim goes to every voter whether or not gossip has
/// shown it alive — a roster member this node has never heard of is exactly the
/// grant it cannot afford to skip — and to every live peer, roster or not.
#[test]
fn a_claim_reaches_every_voter_and_every_live_peer() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let unheard = NodeId::new("unheard-voter");
    let roster = [rank[0].clone(), rank[1].clone(), unheard.clone()];
    let mut e = voter(&rank[0], &rank[1..], &roster, LEASE);

    let opened = e.on_tick(Time(LEASE));
    let mut told: Vec<NodeId> = claim_frames(&opened)
        .into_iter()
        .map(|(to, _, _)| to)
        .collect();
    told.sort();
    let mut want = vec![rank[1].clone(), rank[2].clone(), unheard];
    want.sort();
    assert_eq!(
        told, want,
        "the union of the roster and the live peers, and nothing else"
    );
}

/// Row Q5. A grant is addressed evidence: for us, from a voter, about an epoch
/// we are actually running a round for. Everything else is dropped — and the
/// contrast is sharp, because a single *valid* grant would have closed this
/// round.
#[test]
fn grants_that_are_not_evidence_are_dropped() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let mut e = voter(&rank[0], &rank[1..], &rank, LEASE);
    e.on_tick(Time(LEASE)); // claims epoch 1
    assert_eq!(e.role(), Role::Claimant);

    let junk = [
        // A granter outside the roster.
        lead_grant_frame(1, rank[0].as_str(), "stranger"),
        // A grant for somebody else's claim.
        lead_grant_frame(1, rank[1].as_str(), rank[1].as_str()),
        // A grant for an epoch we hold no round for.
        lead_grant_frame(2, rank[0].as_str(), rank[1].as_str()),
        lead_grant_frame(0, rank[0].as_str(), rank[1].as_str()),
    ];
    for (i, frame) in junk.iter().enumerate() {
        let effects = e.on_message(rank[1].clone(), frame, Time(LEASE + 10));
        assert!(effects.is_empty(), "junk grant {i} was not dropped");
        assert_eq!(e.role(), Role::Claimant, "junk grant {i} closed the round");
        assert_eq!(e.observed_epoch(), 1, "a grant teaches no epoch ({i})");
    }

    let closed = e.on_message(
        rank[1].clone(),
        &lead_grant_frame(1, rank[0].as_str(), rank[1].as_str()),
        Time(LEASE + 20),
    );
    assert_eq!(
        leadership_changes(&closed),
        vec![(1, Some(rank[0].clone()))],
        "the round was still open — only the junk was refused"
    );
}

/// A grant with no round behind it changes nothing. A Follower has no claim to
/// close, so the tally has nowhere to put it.
#[test]
fn a_grant_to_a_follower_is_dropped() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let mut e = voter(&rank[0], &rank[1..], &rank, LEASE);
    assert_eq!(e.role(), Role::Follower);

    let effects = e.on_message(
        rank[1].clone(),
        &lead_grant_frame(1, rank[0].as_str(), rank[1].as_str()),
        Time(10),
    );
    assert!(effects.is_empty());
    assert_eq!(e.role(), Role::Follower);
    assert_eq!(e.leadership(), (0, None));
}

/// Row Q6. A window that shuts without a majority abandons **silently** — a
/// claim that never activated changed nobody's belief — and the guard re-fires
/// on the next tick, one epoch higher. The re-bid's self-grant is free: it is
/// the same claimant advancing its own epoch.
#[test]
fn a_window_that_shuts_without_a_majority_abandons_and_re_bids_one_higher() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let mut e = voter(&rank[0], &rank[1..], &rank, LEASE);
    e.on_tick(Time(LEASE)); // claims epoch 1, window shuts at 2·LEASE
    assert_eq!(e.role(), Role::Claimant);

    let shut = e.on_tick(Time(2 * LEASE));
    assert!(
        election_frames(&shut).is_empty(),
        "an abandoned claim announces nothing"
    );
    assert!(leadership_changes(&shut).is_empty());
    assert_eq!(e.role(), Role::Follower);
    assert_eq!(e.leadership(), (0, None), "no host was ever entered");
    assert_eq!(e.host_lease_until(), None);

    let rebid = e.on_tick(Time(2 * LEASE + 1));
    assert_eq!(e.role(), Role::Claimant);
    assert_eq!(persisted_grants(&rebid), vec![(2, rank[0].clone())]);
    for (_, epoch, claimant) in claim_frames(&rebid) {
        assert_eq!(
            (epoch, claimant),
            (2, rank[0].clone()),
            "the next bid is one above the epoch just spent"
        );
    }
}

/// A candidate still promised to **another** claimant may bid, but may not
/// count itself: its own claim is refused by its own ledger (row Q3), so it
/// needs a full majority from elsewhere.
#[test]
fn a_candidate_under_a_live_promise_bids_without_counting_itself() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let mut e = voter(&rank[0], &rank[1..], &rank, LEASE);

    // Granted to a rival first, so the promise stands when we come to bid.
    e.on_message(
        rank[1].clone(),
        &lead_claim_frame(5, rank[1].as_str()),
        Time(LEASE),
    );
    assert_eq!(e.voter_grant(), Some((5, &rank[1])));

    let opened = e.on_tick(Time(LEASE + 10));
    assert_eq!(e.role(), Role::Claimant);
    assert!(
        persisted_grants(&opened).is_empty(),
        "the self-grant was refused, so nothing was written down"
    );
    assert_eq!(e.voter_grant(), Some((5, &rank[1])), "and nothing changed");

    // One peer grant is no longer enough: this node is not in its own round.
    let one = e.on_message(
        rank[1].clone(),
        &lead_grant_frame(6, rank[0].as_str(), rank[1].as_str()),
        Time(LEASE + 20),
    );
    assert!(
        leadership_changes(&one).is_empty(),
        "one grant is not a majority when the candidate cannot count itself"
    );
    assert_eq!(e.role(), Role::Claimant);

    let two = e.on_message(
        rank[2].clone(),
        &lead_grant_frame(6, rank[0].as_str(), rank[2].as_str()),
        Time(LEASE + 30),
    );
    assert_eq!(
        leadership_changes(&two),
        vec![(6, Some(rank[0].clone()))],
        "two of three closes it without us"
    );
    assert_eq!(e.host_lease_until(), Some(Time(LEASE + 10 + LEASE)));
}

/// An empty roster asks for one grant it can never collect: the group is
/// permanently hostless, and nothing about that path panics.
#[test]
fn an_empty_roster_never_activates() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let mut e = voter(&rank[0], &rank[1..], &[], LEASE);

    for at in (100..=20 * LEASE).step_by(100) {
        let effects = e.on_tick(Time(at));
        assert!(
            leadership_changes(&effects).is_empty(),
            "an empty roster announced leadership at {at}"
        );
        assert!(persisted_grants(&effects).is_empty(), "granted at {at}");
        assert_ne!(e.role(), Role::Host, "activated at {at}");
    }
    // And nothing a peer can say closes it, since no peer is a voter.
    let effects = e.on_message(
        rank[1].clone(),
        &lead_grant_frame(e.observed_epoch(), rank[0].as_str(), rank[1].as_str()),
        Time(20 * LEASE),
    );
    assert!(effects.is_empty());
    assert_ne!(e.role(), Role::Host);
    assert_eq!(e.leadership(), (0, None));
}

// --------------------------------------------------------------------------
// The host half: rows Q7, Q8, and row 6 under Quorum.
// --------------------------------------------------------------------------

/// A host that has just activated, for the renewal rows.
fn activated(rank: &[NodeId], lease_ms: u64) -> GroupEngine {
    let mut e = voter(&rank[0], &rank[1..], rank, lease_ms);
    e.on_tick(Time(lease_ms)); // claims epoch 1 at its send instant
    e.on_message(
        rank[1].clone(),
        &lead_grant_frame(1, rank[0].as_str(), rank[1].as_str()),
        Time(lease_ms + 10),
    );
    assert_eq!(e.role(), Role::Host);
    assert_eq!(e.host_lease_until(), Some(Time(2 * lease_ms)));
    e
}

/// Rows Q7 and Q8. A Quorum host renews by *asking*: it re-claims the epoch it
/// already holds from the roster each anti-entropy round, and its lease moves
/// only when a majority answers again. A round that collects only its own grant
/// moves nothing.
#[test]
fn a_renewal_round_extends_the_lease_only_on_a_majority() {
    const RENEW: u64 = 400;
    let rank = rank_by_placement(&["a", "b", "c"]);
    let mut e = activated(&rank, RENEW);

    // The first anti-entropy tick after activation re-claims the adopted epoch.
    let round = e.on_tick(Time(600));
    let mut asked: Vec<NodeId> = claim_frames(&round)
        .into_iter()
        .map(|(to, _, _)| to)
        .collect();
    asked.sort();
    assert_eq!(
        asked,
        vec![rank[1].clone(), rank[2].clone()],
        "a renewal round asks the roster, not the membership view"
    );
    for (_, epoch, claimant) in claim_frames(&round) {
        assert_eq!(
            (epoch, claimant),
            (1, rank[0].clone()),
            "a renewal re-claims the epoch it holds, it does not bid a new one"
        );
    }
    assert!(
        persisted_grants(&round).is_empty(),
        "the host's own re-grant decides nothing new"
    );
    assert_eq!(
        e.host_lease_until(),
        Some(Time(2 * RENEW)),
        "asking is not renewing: one grant of three moves nothing"
    );

    let confirmed = e.on_message(
        rank[1].clone(),
        &lead_grant_frame(1, rank[0].as_str(), rank[1].as_str()),
        Time(650),
    );
    assert!(
        leadership_changes(&confirmed).is_empty(),
        "a renewal is not a leadership change"
    );
    assert_eq!(
        e.host_lease_until(),
        Some(Time(600 + RENEW)),
        "the majority extends the lease to this round's send instant plus a lease"
    );
    assert_eq!(e.role(), Role::Host);

    // A stale re-delivery of the same grant can only ever push the lease out,
    // never pull it in.
    e.on_message(
        rank[1].clone(),
        &lead_grant_frame(1, rank[0].as_str(), rank[1].as_str()),
        Time(660),
    );
    assert_eq!(e.host_lease_until(), Some(Time(600 + RENEW)));
}

/// Row 6 under Quorum, which is the whole CP posture in one test: a host whose
/// voters have gone silent keeps asking, gets no majority, and steps down when
/// its lease lapses — however top-ranked it still looks **to itself**. Under
/// Settle that same re-rank would have renewed it for ever.
#[test]
fn a_host_cut_off_from_its_voters_lapses() {
    const RENEW: u64 = 400;
    let rank = rank_by_placement(&["a", "b", "c"]);
    let mut e = activated(&rank, RENEW);

    let mut lapse = None;
    for at in (600..=2_000).step_by(50) {
        let effects = e.on_tick(Time(at));
        if let Some(change) = leadership_changes(&effects).first() {
            lapse = Some((Time(at), change.clone()));
            break;
        }
    }
    assert!(
        e.is_coordinator(),
        "the point of the test: it never stopped being top-ranked in its own view"
    );
    assert_eq!(
        lapse,
        Some((Time(2 * RENEW), (1, None))),
        "the lease lapsed on time and the host stepped down, hostless"
    );
    assert_eq!(e.role(), Role::Follower);
    assert_eq!(e.host_lease_until(), None);
    assert_eq!(
        e.leadership(),
        (1, None),
        "the epoch is kept, so a later pair is still fenced against it"
    );
}

/// A host that has lost rank stops asking. Renewal is for the node the group
/// would elect anyway; an outranked host should be letting its lease run out,
/// not lobbying the roster to extend it.
#[test]
fn an_outranked_host_stops_renewing() {
    const RENEW: u64 = 400;
    let rank = rank_by_placement(&["a", "b", "c", "d"]);
    // The roster deliberately excludes `rank[0]`, so it can outrank the host
    // without being able to grant.
    let roster = [rank[1].clone(), rank[2].clone(), rank[3].clone()];
    let mut e = voter(&rank[1], &roster[1..], &roster, RENEW);
    e.on_tick(Time(RENEW));
    e.on_message(
        rank[2].clone(),
        &lead_grant_frame(1, rank[1].as_str(), rank[2].as_str()),
        Time(RENEW + 10),
    );
    assert_eq!(e.role(), Role::Host);

    // A better-ranked member turns up.
    e.on_message(
        rank[0].clone(),
        &digest_frame(vec![ndigest(rank[0].as_str(), 0, Status::Alive, 0)], vec![]),
        Time(RENEW + 20),
    );
    assert_eq!(e.coordinator(), Some(&rank[0]));

    let round = e.on_tick(Time(600));
    assert!(
        claim_frames(&round).is_empty(),
        "an outranked host must not ask the roster to keep it"
    );
    assert_eq!(e.host_lease_until(), Some(Time(2 * RENEW)));
    let lapsed = e.on_tick(Time(2 * RENEW));
    assert_eq!(leadership_changes(&lapsed), vec![(1, None)]);
    assert_eq!(e.role(), Role::Follower);
}

/// Row Q9a, both modes on the same frame. A claim at the adopted epoch from the
/// adopted host is that host renewing; row 9 would answer it with the very pair
/// it is renewing. Under Quorum the answer is a grant instead — and under
/// Settle nothing changes at all, which is the byte-identity half of the row.
#[test]
fn the_renewal_carve_out_is_gated_on_quorum() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let host = &rank[1];
    let adopt = |e: &mut GroupEngine| {
        e.on_message(
            host.clone(),
            &lead_state_frame(3, Some(host.as_str())),
            Time(10),
        );
        assert_eq!(e.leadership(), (3, Some(host)));
    };

    // --- Settle: unchanged. The claimant is bidding for an epoch already
    // awarded, and is taught the pair that awarded it. ---
    let mut settle = hosted_engine(rank[0].as_str(), &[], 500, LEASE);
    learn_peers(&mut settle, &rank[1..], Time::ZERO);
    settle.start(Time::ZERO);
    adopt(&mut settle);
    let taught = settle.on_message(
        host.clone(),
        &lead_claim_frame(3, host.as_str()),
        Time(LEASE),
    );
    assert_eq!(
        state_frames(&taught),
        vec![(host.clone(), 3, Some(host.clone()))],
        "Settle must still teach the pair back, byte for byte"
    );

    // --- Quorum, non-voter: the carve-out alone, with no grant to confuse it.
    let outsiders = [rank[1].clone(), rank[2].clone()];
    let mut watcher = voter(&rank[0], &rank[1..], &outsiders, LEASE);
    adopt(&mut watcher);
    let quiet = watcher.on_message(
        host.clone(),
        &lead_claim_frame(3, host.as_str()),
        Time(LEASE),
    );
    assert!(
        quiet.is_empty(),
        "a renewal is not a stale bid: no teach-back, and a non-voter has nothing else to say"
    );
    assert_eq!(watcher.leadership(), (3, Some(host)));

    // --- Quorum, voter: answered with a grant rather than a repair. ---
    let mut granter = voter(&rank[0], &rank[1..], &rank, LEASE);
    adopt(&mut granter);
    let answered = granter.on_message(
        host.clone(),
        &lead_claim_frame(3, host.as_str()),
        Time(LEASE),
    );
    assert!(
        state_frames(&answered).is_empty(),
        "the voter answers the renewal, it does not repair it"
    );
    assert_eq!(
        grant_frames(&answered),
        vec![(host.clone(), 3, host.clone(), rank[0].clone())]
    );

    // The carve-out is exactly one case wide: a *different* claimant at the
    // adopted epoch is still bidding for an epoch already awarded.
    let stale = granter.on_message(
        rank[2].clone(),
        &lead_claim_frame(3, rank[2].as_str()),
        Time(LEASE + 1),
    );
    assert_eq!(
        state_frames(&stale),
        vec![(rank[2].clone(), 3, Some(host.clone()))],
        "a rival at the adopted epoch is still taught back"
    );
}

// --------------------------------------------------------------------------
// Shared machinery under Quorum: rows 12, 12b, 13.
// --------------------------------------------------------------------------

/// Rows 12b and 13 under Quorum, and the recovery they exist for. A host taught
/// a higher pair naming itself steps down to that epoch hostless, then re-earns
/// the group **above** the fence — this time by collecting fresh grants, with
/// its own self-grant free because it is the same claimant advancing.
#[test]
fn a_fenced_host_re_earns_the_group_above_the_fence_with_fresh_grants() {
    const WIN: u64 = 400;
    let rank = rank_by_placement(&["a", "b", "c"]);
    let mut e = activated(&rank, WIN);

    let fenced = e.on_message(
        rank[1].clone(),
        &lead_state_frame(6, Some(rank[0].as_str())),
        Time(500),
    );
    assert_eq!(
        leadership_changes(&fenced),
        vec![(6, None)],
        "row 12b: the epoch is learned, the hostship in it is not"
    );
    assert_eq!(e.role(), Role::Follower);

    let rebid = e.on_tick(Time(600));
    assert_eq!(e.role(), Role::Claimant);
    assert_eq!(
        persisted_grants(&rebid),
        vec![(7, rank[0].clone())],
        "the self-grant advances our own epoch, so the promise never bars it"
    );
    for (_, epoch, claimant) in claim_frames(&rebid) {
        assert_eq!((epoch, claimant), (7, rank[0].clone()));
    }

    let closed = e.on_message(
        rank[2].clone(),
        &lead_grant_frame(7, rank[0].as_str(), rank[2].as_str()),
        Time(610),
    );
    assert_eq!(
        leadership_changes(&closed),
        vec![(7, Some(rank[0].clone()))]
    );
    assert_eq!(e.host_lease_until(), Some(Time(600 + WIN)));

    // Row 13: a worse pair is repaired, not adopted.
    let taught = e.on_message(
        rank[2].clone(),
        &lead_state_frame(2, Some(rank[2].as_str())),
        Time(620),
    );
    assert_eq!(
        state_frames(&taught),
        vec![(rank[2].clone(), 7, Some(rank[0].clone()))]
    );
}

/// Row 12 under Quorum: a better pair naming somebody else deposes the host
/// outright, grants or no grants. Adoption is fencing, not election — it does
/// not go through the roster.
#[test]
fn a_better_pair_deposes_a_quorum_host_without_a_vote() {
    const WIN: u64 = 400;
    let rank = rank_by_placement(&["a", "b", "c"]);
    let mut e = activated(&rank, WIN);

    let deposed = e.on_message(
        rank[2].clone(),
        &lead_state_frame(9, Some(rank[1].as_str())),
        Time(500),
    );
    assert_eq!(
        leadership_changes(&deposed),
        vec![(9, Some(rank[1].clone()))]
    );
    assert_eq!(e.role(), Role::Follower);
    assert_eq!(e.leadership(), (9, Some(&rank[1])));
    assert_eq!(e.host_lease_until(), None);
    assert_eq!(e.observed_epoch(), 9);
}

// --------------------------------------------------------------------------
// Recovery outside Quorum.
// --------------------------------------------------------------------------

/// A recovered grant is meaningful only under Quorum activation. A `Settle`
/// group and an `Eventual` group have no voter ledger to restore, so the
/// constructor is `new` with a longer name — not a half-applied policy.
#[test]
fn a_recovered_grant_is_ignored_outside_quorum() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let recovered = || RecoveredGrant::granted(9, NodeId::new("elsewhere"));

    // --- Settle: still closes its epoch on the window alone. ---
    let mut settle = recovered_engine(
        rank[0].as_str(),
        &[],
        settle_config(500, 4_000),
        recovered(),
    );
    learn_peers(&mut settle, &rank[1..], Time::ZERO);
    settle.start(Time::ZERO);
    assert_eq!(settle.voter_grant(), None, "no ledger to restore");
    settle.on_tick(Time(500));
    let activated = settle.on_tick(Time(1_000));
    assert_eq!(
        leadership_changes(&activated),
        vec![(1, Some(rank[0].clone()))],
        "a recovered grant must not disturb Settle activation"
    );
    assert_eq!(settle.host_lease_until(), Some(Time(5_000)));

    // --- Eventual: runs no election at all, recovered or not. ---
    let mut eventual = recovered_engine(rank[0].as_str(), &[], Config::default(), recovered());
    learn_peers(&mut eventual, &rank[1..], Time::ZERO);
    eventual.start(Time::ZERO);
    assert_eq!(eventual.voter_grant(), None);
    for at in (100..=3_000).step_by(100) {
        let effects = eventual.on_tick(Time(at));
        assert!(
            election_frames(&effects).is_empty(),
            "election frame at {at}"
        );
        assert!(persisted_grants(&effects).is_empty(), "persist at {at}");
    }
    assert_eq!(eventual.role(), Role::Follower);
    assert_eq!(eventual.leadership(), (0, None));
}
