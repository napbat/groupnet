//! `Activation::Quorum`: the host's renewal round (rows Q7–Q8) and row 6 under
//! Quorum, row 9's renewal carve-out (Q9a), the shared fencing rows (12, 12b,
//! 13) under Quorum, and the recovered-grant posture outside it.
//!
//! Split out of `election_quorum.rs` (which owns the voter ledger and the
//! candidate's grant round) the same way `election_quorum_round.rs` is: these
//! rows are about a host *keeping* — or losing — the group it already holds,
//! rather than about what a single grant decides.
//!
//! Every test drives a real engine through the fixture frames, so what is
//! asserted is the wire behaviour a peer would actually observe — and, for the
//! durability rows, the exact effect *order* a driver with a store must honour.

use groupnet_core::{Config, GroupEngine, NodeId, RecoveredGrant, Role, Status, Time};
use groupnet_testkit::frames::quorum_voter_engine as voter;
use groupnet_testkit::frames::*;

/// The lease every fixture here runs on. Under Quorum this one number is the
/// lease, the claim window, the boot guard, and the post-boot grant blackout.
const LEASE: u64 = 500;

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
