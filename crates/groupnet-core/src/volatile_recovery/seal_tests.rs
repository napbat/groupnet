//! A writer's delivered seal accounts for its whole life: the lapse proof
//! follows that writer out of the roster and into its next life, while an
//! unsealed departure still forces the full fallback.

use std::collections::VecDeque;

use super::*;
use crate::NodeId;

fn engine() -> RecoveryEngine {
    let config = RecoveryConfig {
        max_members: 4,
        max_member_bytes: 32,
        max_barrier_rounds: 3,
        total_ms: 100,
        attempt_ms: 20,
        settle_ms: 5,
        poll_ms: 2,
    };
    RecoveryEngine::new(config, RecoveryMode::Leased, NodeId::from("me"), 7).unwrap()
}

fn mark(epoch: u64, sequence: u64) -> Mark {
    Mark { epoch, sequence }
}

fn operation(step: &RecoveryStep) -> RecoveryOperation {
    step.effects
        .iter()
        .find_map(|effect| match effect {
            RecoveryEffect::Invalidate { op, .. }
            | RecoveryEffect::RebuildOrigin { op }
            | RecoveryEffect::ObservePeers { op }
            | RecoveryEffect::Affirm { op } => Some(*op),
            _ => None,
        })
        .expect("current operation effect")
}

/// The writer `a` as the observer sees it: its life `1` wrote through
/// `(1, 4)`, so that life's seal, once delivered, is `(1, 5)`.
fn writer() -> Peer {
    Peer {
        node: NodeId::from("a"),
        alive: true,
        grants_lease: true,
        old_nonlive: false,
        grant: Some(mark(1, 1)),
        head: Some(mark(1, 4)),
        renewal: None,
        sealed: None,
    }
}

/// `a` after it stopped, its grant frozen: through a seal the observer
/// delivered (`sealed`) or a crash, and already non-live when the lapse began
/// (`exempt`) or only just.
fn stopped(sealed: bool, exempt: bool) -> Peer {
    Peer {
        alive: false,
        old_nonlive: exempt,
        sealed: sealed.then_some(mark(1, 5)),
        ..writer()
    }
}

/// `a` reaped, then relearned by the observer's seed resolver before its next
/// life gossips: a live-looking member with no state at all.
fn relearned(sealed: bool) -> Peer {
    Peer {
        grants_lease: false,
        grant: None,
        head: None,
        sealed: sealed.then_some(mark(1, 5)),
        ..writer()
    }
}

/// One peer observation and the roster-wide lease confirmation beside it,
/// which stays frozen while the stopped granter is still in the roster.
type Observation = (Vec<Peer>, u64);

/// How one lapse turn ended.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    /// The turn affirmed its retained baseline: no origin work at all.
    Affirmed,
    /// The turn gave up the lapse proof for the full gap-style fallback.
    FellBack(RecoveryFallback),
}

/// Builds a Ready engine, lapses it, and drives the lapse turn. Each peer
/// observation takes the next of `observations` (the last repeats); frontiers
/// reach every head and the lease affirms at once. Returns how the turn ended
/// and every frontier target it waited for.
fn lapse_turn(observations: &[Observation]) -> (Outcome, Vec<Vec<(NodeId, Mark)>>) {
    let mut recovery = engine();
    let invalidate = recovery.step(RecoveryEvent::Start);
    let rebuild = recovery.step(RecoveryEvent::Invalidated {
        op: operation(&invalidate),
    });
    let affirm = recovery.step(RecoveryEvent::Materialized {
        op: operation(&rebuild),
    });
    recovery.step(RecoveryEvent::Affirmed {
        op: operation(&affirm),
        accepted: true,
    });
    assert!(recovery.state().recovered);
    let mut queue: VecDeque<RecoveryEffect> = recovery
        .step(RecoveryEvent::LeaseLapse { count: 1 })
        .effects
        .into();
    let mut observed = 0_usize;
    let mut targets = Vec::new();
    while !recovery.state().recovered {
        let Some(effect) = queue.pop_front() else {
            let due = recovery
                .next_deadline()
                .expect("a waiting turn arms a timer");
            queue.extend(recovery.step(RecoveryEvent::Tick(due)).effects);
            continue;
        };
        let event = match effect {
            RecoveryEffect::Invalidate { op, .. } => RecoveryEvent::Invalidated { op },
            RecoveryEffect::ObservePeers { op } => {
                let (peers, confirmed) = observations[observed.min(observations.len() - 1)].clone();
                observed += 1;
                RecoveryEvent::PeersObserved {
                    op,
                    peers,
                    confirmed: Some(mark(1, confirmed)),
                }
            }
            RecoveryEffect::WaitFrontiers { op, heads } => {
                targets.push(heads);
                RecoveryEvent::FrontiersReached { op }
            }
            RecoveryEffect::Affirm { op } => RecoveryEvent::Affirmed { op, accepted: true },
            other => {
                assert!(
                    matches!(
                        other,
                        RecoveryEffect::CloseGate { .. }
                            | RecoveryEffect::SuspendLocalBaseline { .. }
                            | RecoveryEffect::ArmTimer(_)
                    ),
                    "unexpected lapse work: {other:?}"
                );
                continue;
            }
        };
        let step = recovery.step(event);
        assert_eq!(step.rejection, None, "{step:?}");
        if let Some(reason) = step.effects.iter().find_map(|effect| match effect {
            RecoveryEffect::FellBack { reason, .. } => Some(*reason),
            _ => None,
        }) {
            return (Outcome::FellBack(reason), targets);
        }
        queue.extend(step.effects);
    }
    assert_eq!(recovery.state().stage, RecoveryStage::Ready);
    (Outcome::Affirmed, targets)
}

#[test]
fn a_sealed_writer_relearned_with_no_state_is_not_lost_evidence() {
    // The 2026-10-01 rollout: the survivor waits on the stopped writer's
    // frozen grant until membership reaps it, and the seed resolver relearns
    // it, empty, before its next life gossips.
    for (case, observations) in [
        (
            "relearned as it is reaped, during the renewal wait",
            vec![
                (vec![stopped(true, true)], 1),
                (vec![stopped(true, true)], 1),
                (vec![relearned(true)], 2),
            ],
        ),
        (
            "relearned after the reap, during the settle",
            vec![
                (vec![stopped(true, true)], 1),
                (vec![stopped(true, true)], 1),
                (Vec::new(), 2),
                (vec![relearned(true)], 2),
            ],
        ),
        (
            "relearned after the reap of a writer only just non-live at the lapse",
            vec![
                (vec![stopped(true, false)], 1),
                (vec![stopped(true, false)], 1),
                (Vec::new(), 2),
                (vec![relearned(true)], 2),
            ],
        ),
    ] {
        let (outcome, targets) = lapse_turn(&observations);
        assert_eq!(outcome, Outcome::Affirmed, "{case}");
        assert!(targets.iter().all(Vec::is_empty), "{case}: {targets:?}");
    }
}

#[test]
fn a_sealed_writer_reaped_from_the_roster_is_not_a_membership_change() {
    let (outcome, _) = lapse_turn(&[
        (vec![stopped(true, false)], 1),
        (vec![stopped(true, false)], 1),
        (Vec::new(), 2),
    ]);
    assert_eq!(outcome, Outcome::Affirmed);
}

#[test]
fn a_sealed_writers_next_life_seen_before_its_renewal_is_a_barrier_target() {
    let next = Peer {
        head: Some(mark(2, 1)),
        ..relearned(true)
    };
    let renewed = Peer {
        renewal: Some(Renewal {
            sealed: mark(1, 5),
            epoch: 2,
        }),
        sealed: None,
        ..next.clone()
    };
    let (outcome, targets) = lapse_turn(&[
        (vec![stopped(true, true)], 1),
        (vec![stopped(true, true)], 1),
        (Vec::new(), 2),
        (vec![next], 2),
        (vec![renewed], 2),
    ]);
    assert_eq!(outcome, Outcome::Affirmed);
    assert_eq!(targets, vec![vec![(NodeId::from("a"), mark(2, 1))]]);
}

#[test]
fn a_writer_that_leaves_its_life_without_a_covering_seal_still_falls_back() {
    let next_life = Peer {
        head: Some(mark(2, 1)),
        ..relearned(false)
    };
    let early_seal = Peer {
        sealed: Some(mark(1, 4)),
        ..stopped(false, true)
    };
    for (case, observations, reason) in [
        (
            "a crash, relearned with no state",
            vec![
                (vec![stopped(false, true)], 1),
                (vec![stopped(false, true)], 1),
                (Vec::new(), 2),
                (vec![relearned(false)], 2),
            ],
            RecoveryFallback::EvidenceRejected,
        ),
        (
            "a crash, reaped from the roster",
            vec![
                (vec![stopped(false, false)], 1),
                (vec![stopped(false, false)], 1),
                (Vec::new(), 2),
            ],
            RecoveryFallback::MembershipChanged,
        ),
        (
            "a crash, its next life already written",
            vec![
                (vec![stopped(false, true)], 1),
                (vec![stopped(false, true)], 1),
                (Vec::new(), 2),
                (vec![next_life.clone()], 2),
            ],
            RecoveryFallback::EvidenceRejected,
        ),
        (
            "a seal that does not follow the last head",
            vec![
                (vec![early_seal.clone()], 1),
                (vec![early_seal], 1),
                (Vec::new(), 2),
                (
                    vec![Peer {
                        sealed: Some(mark(1, 4)),
                        ..relearned(false)
                    }],
                    2,
                ),
            ],
            RecoveryFallback::EvidenceRejected,
        ),
        (
            "a seal a later gap withdrew before the writer left",
            vec![
                (vec![stopped(true, false)], 1),
                (vec![stopped(false, false)], 1),
                (Vec::new(), 2),
            ],
            RecoveryFallback::MembershipChanged,
        ),
        (
            "a sealed writer whose unrenewed next life left too",
            vec![
                (vec![stopped(true, false)], 1),
                (vec![stopped(true, false)], 1),
                (Vec::new(), 2),
                (
                    vec![Peer {
                        sealed: Some(mark(1, 5)),
                        ..next_life
                    }],
                    2,
                ),
                (Vec::new(), 2),
            ],
            RecoveryFallback::MembershipChanged,
        ),
    ] {
        let (outcome, _) = lapse_turn(&observations);
        assert_eq!(outcome, Outcome::FellBack(reason), "{case}");
    }
}

#[test]
fn a_seal_at_sequence_zero_is_invalid_evidence() {
    let mut recovery = engine();
    let invalidate = recovery.step(RecoveryEvent::Start);
    let rebuild = recovery.step(RecoveryEvent::Invalidated {
        op: operation(&invalidate),
    });
    let affirm = recovery.step(RecoveryEvent::Materialized {
        op: operation(&rebuild),
    });
    recovery.step(RecoveryEvent::Affirmed {
        op: operation(&affirm),
        accepted: true,
    });
    let lapse = recovery.step(RecoveryEvent::LeaseLapse { count: 1 });
    let initial = recovery.step(RecoveryEvent::Invalidated {
        op: operation(&lapse),
    });
    let refused = recovery.step(RecoveryEvent::PeersObserved {
        op: operation(&initial),
        peers: vec![Peer {
            sealed: Some(mark(1, 0)),
            ..writer()
        }],
        confirmed: Some(mark(1, 1)),
    });
    assert_eq!(refused.rejection, Some(RecoveryError::InvalidEvidence));
    assert_eq!(recovery.state().stage, RecoveryStage::SamplingInitial);
}
