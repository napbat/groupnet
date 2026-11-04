//! `Activation::Quorum`: row Q16, the resign of a leaving host
//! (napbat/groupnet#13).
//!
//! A host that leaves demotes itself (row 15) and then resigns its epoch to
//! every claim target. A voter whose grant is exactly that pair ends its
//! promise at once, so a successor wins without waiting for the lease, and it
//! never grants the resigned pair again.
//!
//! Every test drives a real engine through the fixture frames, so what is
//! asserted is the wire behaviour a peer would actually observe.

use groupnet_core::{Command, Effect, GroupEngine, NodeId, Role, Time, wire};
use groupnet_testkit::frames::quorum_voter_engine as voter;
use groupnet_testkit::frames::*;

/// The lease every fixture here runs on. Under Quorum this one number is the
/// lease, the claim window, the boot guard, and the post-boot grant blackout.
const LEASE: u64 = 500;

/// When the fixture host activates: one lease after the boot, with the grant
/// of `rank[1]` arriving ten milliseconds after its claim.
const ACTIVATED: u64 = LEASE + 10;

/// The host `rank[0]`, activated at epoch 1 by its own grant and `rank[1]`'s.
fn host(rank: &[NodeId]) -> GroupEngine {
    let mut e = voter(&rank[0], &rank[1..], rank, LEASE);
    e.on_tick(Time(LEASE));
    e.on_message(
        rank[1].clone(),
        &lead_grant_frame(1, rank[0].as_str(), rank[1].as_str()),
        Time(ACTIVATED),
    );
    assert_eq!(e.role(), Role::Host);
    e
}

/// The voter `rank[1]`, which granted `(1, rank[0])` at [`ACTIVATED`] and
/// adopted that pair: its promise to `rank[0]` runs to `ACTIVATED + LEASE`.
fn promised(rank: &[NodeId]) -> GroupEngine {
    let peers = [rank[0].clone(), rank[2].clone()];
    let mut e = voter(&rank[1], &peers, rank, LEASE);
    let granted = e.on_message(
        rank[0].clone(),
        &lead_claim_frame(1, rank[0].as_str()),
        Time(ACTIVATED),
    );
    assert_eq!(persisted_grants(&granted), vec![(1, rank[0].clone())]);
    e.on_message(
        rank[0].clone(),
        &lead_state_frame(1, Some(rank[0].as_str())),
        Time(ACTIVATED + 1),
    );
    assert_eq!(e.leadership(), (1, Some(&rank[0])));
    e
}

fn is_resign_send(effect: &Effect) -> bool {
    match effect {
        Effect::Send { wire, .. } => wire::decode(wire)
            .and_then(|f| f.lead)
            .is_some_and(|b| matches!(b, wire::LeadBody::Resign { .. })),
        _ => false,
    }
}

/// Row Q16a. A leaving host demotes itself first and then resigns its epoch
/// to every other voter, in that order: the resign leaves only after the node
/// stopped being host.
#[test]
fn a_leaving_host_demotes_and_then_resigns_its_epoch() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let mut e = host(&rank);

    let effects = e.apply(Command::Leave);
    assert_eq!(e.role(), Role::Follower);
    assert_eq!(leadership_changes(&effects), vec![(1, None)]);
    let mut resigned = resign_frames(&effects);
    resigned.sort();
    assert_eq!(
        resigned,
        vec![
            (rank[1].clone(), 1, rank[0].clone()),
            (rank[2].clone(), 1, rank[0].clone()),
        ],
        "the resign names the epoch given up and goes to every voter"
    );
    let demoted = effects
        .iter()
        .position(|e| matches!(e, Effect::LeadershipChanged { .. }))
        .expect("the demotion");
    let first_resign = effects
        .iter()
        .position(is_resign_send)
        .expect("a resign frame");
    assert!(demoted < first_resign, "the demotion precedes the resign");
}

/// Row Q16a under `Settle`: no voter holds a promise, so a leaving host sends
/// no resign. The Settle wire stays as it was.
#[test]
fn a_settle_host_leaves_without_a_resign() {
    let rank = rank_by_placement(&["a", "b"]);
    let mut e = hosted_engine(rank[0].as_str(), &[], 100, LEASE);
    learn_peers(&mut e, &rank[1..], Time::ZERO);
    e.start(Time::ZERO);
    e.on_tick(Time(100));
    e.on_tick(Time(200));
    assert_eq!(e.role(), Role::Host);

    let effects = e.apply(Command::Leave);
    assert_eq!(leadership_changes(&effects), vec![(1, None)]);
    assert_eq!(resign_frames(&effects), Vec::new());
}

/// Row Q16b, the reason for the row: a voter still promised to the old host
/// refuses a successor's claim, and after the resign it grants the same
/// claim at once, well before the promise would have ended.
#[test]
fn a_resign_ends_the_promise_at_once() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let mut e = promised(&rank);
    let promise_ends = ACTIVATED + LEASE;

    let refused = e.on_message(
        rank[2].clone(),
        &lead_claim_frame(2, rank[2].as_str()),
        Time(ACTIVATED + 20),
    );
    assert!(
        grant_frames(&refused).is_empty() && persisted_grants(&refused).is_empty(),
        "the promise to the host stands"
    );

    let released = e.on_message(
        rank[0].clone(),
        &lead_resign_frame(1, rank[0].as_str()),
        Time(ACTIVATED + 30),
    );
    assert_eq!(
        leadership_changes(&released),
        vec![(1, None)],
        "row Q16c: the group has no host at the resigned epoch"
    );
    assert_eq!(e.leadership(), (1, None));

    let at = ACTIVATED + 40;
    assert!(at < promise_ends);
    let granted = e.on_message(
        rank[2].clone(),
        &lead_claim_frame(2, rank[2].as_str()),
        Time(at),
    );
    assert_eq!(persisted_grants(&granted), vec![(2, rank[2].clone())]);
    assert_eq!(
        grant_frames(&granted),
        vec![(rank[2].clone(), 2, rank[2].clone(), rank[1].clone())],
        "the successor is granted at {at}, before the promise end at {promise_ends}"
    );
}

/// Row Q16b's fence: once resigned, the pair is never granted again — not as
/// a renewal of the old host, and not as a floor answer that would count as
/// its grant. A higher epoch of the same node is a new pair and is granted.
#[test]
fn a_resigned_pair_is_never_renewed() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let mut e = promised(&rank);
    e.on_message(
        rank[0].clone(),
        &lead_resign_frame(1, rank[0].as_str()),
        Time(ACTIVATED + 30),
    );

    let renewal = e.on_message(
        rank[0].clone(),
        &lead_claim_frame(1, rank[0].as_str()),
        Time(ACTIVATED + 40),
    );
    assert!(
        grant_frames(&renewal).is_empty(),
        "a late renewal of the resigned pair is not answered: {:?}",
        grant_frames(&renewal)
    );
    assert_eq!(
        e.voter_grant(),
        Some((1, &rank[0])),
        "the pair stays the floor"
    );

    let rival = e.on_message(
        rank[2].clone(),
        &lead_claim_frame(1, rank[2].as_str()),
        Time(ACTIVATED + 50),
    );
    assert_eq!(
        grant_frames(&rival),
        vec![(rank[2].clone(), 1, rank[0].clone(), rank[1].clone())],
        "a rival at the resigned epoch is refused and told the floor (row Q3a)"
    );
    assert_eq!(persisted_grants(&rival), Vec::new());

    let higher = e.on_message(
        rank[0].clone(),
        &lead_claim_frame(2, rank[0].as_str()),
        Time(ACTIVATED + 60),
    );
    assert_eq!(persisted_grants(&higher), vec![(2, rank[0].clone())]);
}

/// Row Q16b's guards: a resign from a node other than the one it names, and
/// a resign of an epoch other than the granted one, release nothing.
#[test]
fn only_the_granted_host_can_resign_its_own_pair() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let mut e = promised(&rank);

    let forged = e.on_message(
        rank[2].clone(),
        &lead_resign_frame(1, rank[0].as_str()),
        Time(ACTIVATED + 20),
    );
    assert_eq!(
        forged,
        Vec::<Effect>::new(),
        "a resign on another node's behalf is dropped"
    );
    let earlier = e.on_message(
        rank[0].clone(),
        &lead_resign_frame(0, rank[0].as_str()),
        Time(ACTIVATED + 30),
    );
    assert_eq!(
        earlier,
        Vec::<Effect>::new(),
        "a resign of another epoch is dropped"
    );
    assert_eq!(e.leadership(), (1, Some(&rank[0])));

    let refused = e.on_message(
        rank[2].clone(),
        &lead_claim_frame(2, rank[2].as_str()),
        Time(ACTIVATED + 40),
    );
    assert!(
        persisted_grants(&refused).is_empty(),
        "the promise to the host still stands"
    );
}

/// Row Q16b, a duplicated resign: the second copy changes nothing and
/// announces nothing, and the successor is still granted.
#[test]
fn a_duplicated_resign_changes_nothing() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let mut e = promised(&rank);
    let first = e.on_message(
        rank[0].clone(),
        &lead_resign_frame(1, rank[0].as_str()),
        Time(ACTIVATED + 20),
    );
    assert_eq!(leadership_changes(&first), vec![(1, None)]);
    let second = e.on_message(
        rank[0].clone(),
        &lead_resign_frame(1, rank[0].as_str()),
        Time(ACTIVATED + 30),
    );
    assert_eq!(second, Vec::<Effect>::new(), "the copy is a no-op");
    assert_eq!(e.leadership(), (1, None));

    let granted = e.on_message(
        rank[2].clone(),
        &lead_claim_frame(2, rank[2].as_str()),
        Time(ACTIVATED + 40),
    );
    assert_eq!(persisted_grants(&granted), vec![(2, rank[2].clone())]);
}

/// Row Q16b, a delayed resign: it arrives after the promise to the old host
/// ended on its own and the voter granted a successor. The resign names a
/// pair the voter no longer holds, so it releases nothing: the promise to
/// the successor stands, and another claimant is still refused.
#[test]
fn a_delayed_resign_releases_no_later_promise() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let mut e = promised(&rank);
    let after = ACTIVATED + LEASE;
    let granted = e.on_message(
        rank[2].clone(),
        &lead_claim_frame(2, rank[2].as_str()),
        Time(after),
    );
    assert_eq!(persisted_grants(&granted), vec![(2, rank[2].clone())]);

    e.on_message(
        rank[0].clone(),
        &lead_resign_frame(1, rank[0].as_str()),
        Time(after + 10),
    );
    assert_eq!(e.voter_grant(), Some((2, &rank[2])), "the ledger holds");

    let rival = e.on_message(
        rank[0].clone(),
        &lead_claim_frame(3, rank[0].as_str()),
        Time(after + 20),
    );
    assert_eq!(
        persisted_grants(&rival),
        Vec::new(),
        "the promise to the successor still stands"
    );
    assert_eq!(grant_frames(&rival), Vec::new());
}
