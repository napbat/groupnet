//! The writer's half of the coherence-lease tier: how far one coherent write
//! has got through the set of readers that may be serving.
//!
//! Sans-IO like its reader-side sibling ([`LeaseCore`](super::LeaseCore)): it
//! is fed snapshots of the writer's own view of the group and returns a
//! verdict per poll. Nothing here waits, sleeps, or reads a clock — the tokio
//! shell polls it, the deterministic simulator calls it directly, and both see
//! the same rules.

use std::collections::{BTreeMap, BTreeSet};

use groupnet_core::NodeId;

use super::core::ClockMs;
use crate::token::WriteToken;

/// One member of a coherent write's wait set, as the **writer's own** view of
/// the group shows it: a reader that may be serving — it holds a live
/// `~lease` entry, or the writer granted it within the last lease duration —
/// how far it advertises having applied this writer's feed, and from when a
/// lapse can excuse it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitMember {
    /// The reader.
    pub member: NodeId,
    /// The highest token of the writer's feed this member advertises having
    /// applied (`None`: no ledger, or nothing from this writer yet).
    pub applied: Option<WriteToken>,
    /// The instant, on the writer's clock, from which this member provably
    /// cannot be serving state the write invalidated — or `None` if nothing
    /// but its acknowledgement can end the wait on it
    /// ([`GrantLedger::excusable_at`](super::GrantLedger::excusable_at)).
    pub excusable_at: Option<ClockMs>,
}

/// One poll's verdict on a coherent write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoherenceStep {
    /// Every member of the wait set has applied the write — the fast path, and
    /// exactly the cost of a T2 ack round when the cluster is healthy.
    AllApplied,
    /// The wait is over, but not everyone acknowledged: `stragglers` never
    /// applied the write, and each had provably lost its right to serve — its
    /// lease ran out on the writer's clock while it counted the writer, or its
    /// life departed. A lapsed reader is out of service until it
    /// re-synchronizes.
    LeaseLapsed {
        /// The members excused by lapse rather than by acknowledgement, in id
        /// order.
        stragglers: Vec<NodeId>,
    },
    /// Still waiting: these members may be serving and have not applied the
    /// write yet, in id order.
    Waiting {
        /// The members still being waited on.
        on: Vec<NodeId>,
    },
}

/// One in-flight write's progress through its wait set.
#[derive(Debug, Default)]
struct Progress {
    /// Members that have not applied the write yet, with the newest
    /// [`WaitMember::excusable_at`] each was reported with.
    waiting: BTreeMap<NodeId, Option<ClockMs>>,
    /// Members excused by lapse. Permanent for this write: see
    /// [`CoherenceCore::step`].
    lapsed: BTreeSet<NodeId>,
}

/// The writer's half: how far a coherent write has got through its wait set.
///
/// A pure function of the snapshots and instants it is fed, plus the memory of
/// what it has already seen — the tokio shell polls [`step`](Self::step) and
/// the simulator calls it directly. Nothing here waits, sleeps, or reads a
/// clock; a deadline is the caller's business ([`abandon`](Self::abandon)
/// turns one into
/// [`CoherenceOutcome::TimedOut`](super::CoherenceOutcome::TimedOut)).
#[derive(Debug)]
pub struct CoherenceCore {
    /// The writer these waits belong to — never waited on.
    writer: NodeId,
    /// Per in-flight token, who is left. Terminal verdicts drop their entry.
    inflight: BTreeMap<WriteToken, Progress>,
}

impl CoherenceCore {
    /// A coherence core for writes authored by `writer`.
    #[must_use]
    pub fn new(writer: NodeId) -> Self {
        Self {
            writer,
            inflight: BTreeMap::new(),
        }
    }

    /// How many writes are currently mid-wait.
    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.inflight.len()
    }

    /// Folds one snapshot of the members that may be serving into `token`'s
    /// wait at `now` (the writer's clock) and reports the verdict. A terminal
    /// verdict ([`CoherenceStep::AllApplied`], [`CoherenceStep::LeaseLapsed`])
    /// drops the write's bookkeeping, so a poll loop leaks nothing; a caller
    /// that gives up on a [`CoherenceStep::Waiting`] must call
    /// [`abandon`](Self::abandon).
    ///
    /// The rules, in the order they apply:
    ///
    /// 1. A member that advertises having applied `token` is satisfied and
    ///    leaves the wait set.
    /// 2. A member in the snapshot that has **not** applied it joins (or stays
    ///    in) the wait set with its [`WaitMember::excusable_at`] — including
    ///    one that appears late, which is the conservative direction: a reader
    ///    that took a lease mid-write is waited for rather than assumed clean.
    /// 3. A member that **leaves** the snapshot stays in the wait set on the
    ///    last instant it was reported with. Leaving the writer's view proves
    ///    nothing on its own — a reap or a deleted entry removes a reader from
    ///    view without closing the window the writer granted it.
    /// 4. A waiting member whose `excusable_at` has come is excused, and it is
    ///    excused **permanently for this write**: it lost its right to serve,
    ///    and a lapse forces it into
    ///    [`LeaseState::NeedsResync`](super::LeaseState::NeedsResync), so even
    ///    if it renews a moment later it may not serve until it has
    ///    re-synchronized — re-entering the wait set could only stall the
    ///    writer for nothing. A member reported with `None` is never excused:
    ///    only its acknowledgement, or the caller's deadline, ends the wait.
    pub fn step(
        &mut self,
        token: WriteToken,
        now: ClockMs,
        snapshot: &[WaitMember],
    ) -> CoherenceStep {
        let writer = self.writer.clone();
        let progress = self.inflight.entry(token).or_default();
        for holder in snapshot {
            if holder.member == writer || progress.lapsed.contains(&holder.member) {
                continue;
            }
            if holder.applied.is_some_and(|applied| applied >= token) {
                progress.waiting.remove(&holder.member);
            } else {
                progress
                    .waiting
                    .insert(holder.member.clone(), holder.excusable_at);
            }
        }
        let excused: Vec<NodeId> = progress
            .waiting
            .iter()
            .filter(|(_, excusable_at)| excusable_at.is_some_and(|at| now >= at))
            .map(|(member, _)| member.clone())
            .collect();
        for member in excused {
            progress.waiting.remove(&member);
            progress.lapsed.insert(member);
        }
        if !progress.waiting.is_empty() {
            return CoherenceStep::Waiting {
                on: progress.waiting.keys().cloned().collect(),
            };
        }
        let stragglers = self
            .inflight
            .remove(&token)
            .map(|progress| progress.lapsed)
            .unwrap_or_default();
        if stragglers.is_empty() {
            CoherenceStep::AllApplied
        } else {
            CoherenceStep::LeaseLapsed {
                stragglers: stragglers.into_iter().collect(),
            }
        }
    }

    /// Drops `token`'s bookkeeping — for a caller whose deadline passed —
    /// returning who it was still waiting on, in id order. `None` if the write
    /// was not mid-wait (it had already reached a terminal verdict, or never
    /// started).
    pub fn abandon(&mut self, token: WriteToken) -> Option<Vec<NodeId>> {
        Some(self.inflight.remove(&token)?.waiting.into_keys().collect())
    }
}

#[cfg(test)]
mod tests {
    use groupnet_core::NodeId;

    use super::{CoherenceCore, CoherenceStep, WaitMember};
    use crate::lease::core::ClockMs;
    use crate::token::WriteToken;

    fn node(name: &str) -> NodeId {
        NodeId::new(name)
    }

    const TOKEN: WriteToken = WriteToken { epoch: 2, seq: 5 };
    const NOW: ClockMs = ClockMs(1_000);

    /// A member that can be excused by lapse from `excusable_at` on.
    fn holder(name: &str, applied: Option<WriteToken>, excusable_at: Option<u64>) -> WaitMember {
        WaitMember {
            member: node(name),
            applied,
            excusable_at: excusable_at.map(ClockMs),
        }
    }

    #[test]
    fn an_empty_wait_set_is_immediately_coherent() {
        let mut core = CoherenceCore::new(node("writer"));
        assert_eq!(core.step(TOKEN, NOW, &[]), CoherenceStep::AllApplied);
        assert_eq!(core.in_flight(), 0, "a terminal verdict drops its state");
    }

    #[test]
    fn every_member_applying_is_the_fast_path() {
        let mut core = CoherenceCore::new(node("writer"));
        let behind = [
            holder("a", Some(WriteToken { epoch: 2, seq: 4 }), Some(5_000)),
            holder("b", None, None),
        ];
        assert_eq!(
            core.step(TOKEN, NOW, &behind),
            CoherenceStep::Waiting {
                on: vec![node("a"), node("b")],
            }
        );
        assert_eq!(core.in_flight(), 1);
        let applied = [
            holder("a", Some(TOKEN), Some(5_000)),
            holder("b", Some(WriteToken { epoch: 2, seq: 9 }), None),
        ];
        assert_eq!(core.step(TOKEN, NOW, &applied), CoherenceStep::AllApplied);
        assert_eq!(core.in_flight(), 0);
    }

    #[test]
    fn a_proven_lapse_ends_the_wait_and_names_the_straggler() {
        let mut core = CoherenceCore::new(node("writer"));
        let waiting = [
            holder("a", Some(TOKEN), None),
            holder("silent", None, Some(1_500)),
        ];
        assert_eq!(
            core.step(TOKEN, NOW, &waiting),
            CoherenceStep::Waiting {
                on: vec![node("silent")],
            }
        );
        assert_eq!(
            core.step(TOKEN, ClockMs(1_499), &waiting),
            CoherenceStep::Waiting {
                on: vec![node("silent")],
            },
            "the boundary is the instant itself"
        );
        assert_eq!(
            core.step(TOKEN, ClockMs(1_500), &waiting),
            CoherenceStep::LeaseLapsed {
                stragglers: vec![node("silent")],
            }
        );
        assert_eq!(core.in_flight(), 0);
    }

    #[test]
    fn leaving_the_writer_s_view_is_not_a_lapse() {
        let mut core = CoherenceCore::new(node("writer"));
        let both = [holder("a", None, None), holder("reaped", None, Some(1_500))];
        assert!(matches!(
            core.step(TOKEN, NOW, &both),
            CoherenceStep::Waiting { .. }
        ));
        // `reaped` vanished from the writer's view — membership reaped it, or
        // its entry was deleted — well before the grant it may still be serving
        // on runs out.
        let a_only = [holder("a", Some(TOKEN), None)];
        assert_eq!(
            core.step(TOKEN, ClockMs(1_100), &a_only),
            CoherenceStep::Waiting {
                on: vec![node("reaped")],
            }
        );
        assert_eq!(
            core.step(TOKEN, ClockMs(1_500), &a_only),
            CoherenceStep::LeaseLapsed {
                stragglers: vec![node("reaped")],
            }
        );
    }

    #[test]
    fn a_member_with_no_proof_is_only_ever_released_by_its_acknowledgement() {
        let mut core = CoherenceCore::new(node("writer"));
        let unproven = [holder("stranger", None, None)];
        for at in [1_000, 10_000, 1_000_000] {
            assert_eq!(
                core.step(TOKEN, ClockMs(at), &unproven),
                CoherenceStep::Waiting {
                    on: vec![node("stranger")],
                }
            );
        }
        // Gone from view, still unproven: no amount of time excuses it.
        assert!(matches!(
            core.step(TOKEN, ClockMs(u64::MAX), &[]),
            CoherenceStep::Waiting { .. }
        ));
        assert_eq!(
            core.step(TOKEN, NOW, &[holder("stranger", Some(TOKEN), None)]),
            CoherenceStep::AllApplied
        );
    }

    #[test]
    fn a_renewed_lease_does_not_re_enter_a_wait_it_already_lapsed_out_of() {
        let mut core = CoherenceCore::new(node("writer"));
        let both = [
            holder("a", None, None),
            holder("flapper", None, Some(1_200)),
        ];
        assert!(matches!(
            core.step(TOKEN, NOW, &both),
            CoherenceStep::Waiting { .. }
        ));
        // `flapper`'s lease runs out on the writer's clock…
        assert_eq!(
            core.step(TOKEN, ClockMs(1_200), &both),
            CoherenceStep::Waiting {
                on: vec![node("a")],
            }
        );
        // …and it renews a moment later, still not having applied the write.
        // It lapsed, so it is in `NeedsResync` and cannot serve: waiting on it
        // again would stall the writer for nothing.
        let renewed = [
            holder("a", None, None),
            holder("flapper", None, Some(9_000)),
        ];
        assert_eq!(
            core.step(TOKEN, ClockMs(1_300), &renewed),
            CoherenceStep::Waiting {
                on: vec![node("a")],
            }
        );
        assert_eq!(
            core.step(
                TOKEN,
                ClockMs(1_300),
                &[
                    holder("a", Some(TOKEN), None),
                    holder("flapper", None, None)
                ]
            ),
            CoherenceStep::LeaseLapsed {
                stragglers: vec![node("flapper")],
            }
        );
    }

    #[test]
    fn a_reader_that_takes_a_lease_mid_write_is_waited_for() {
        let mut core = CoherenceCore::new(node("writer"));
        assert_eq!(
            core.step(TOKEN, NOW, &[holder("a", None, None)]),
            CoherenceStep::Waiting {
                on: vec![node("a")],
            }
        );
        // `late` shows up holding a lease it took after the write began: the
        // conservative direction is to wait for it too.
        assert_eq!(
            core.step(
                TOKEN,
                NOW,
                &[holder("a", Some(TOKEN), None), holder("late", None, None)]
            ),
            CoherenceStep::Waiting {
                on: vec![node("late")],
            }
        );
    }

    #[test]
    fn a_writer_never_waits_on_itself() {
        let mut core = CoherenceCore::new(node("writer"));
        assert_eq!(
            core.step(TOKEN, NOW, &[holder("writer", None, None)]),
            CoherenceStep::AllApplied
        );
    }

    #[test]
    fn abandoning_a_wait_reports_who_was_left() {
        let mut core = CoherenceCore::new(node("writer"));
        let _ = core.step(
            TOKEN,
            NOW,
            &[holder("a", None, None), holder("b", Some(TOKEN), None)],
        );
        assert_eq!(core.abandon(TOKEN), Some(vec![node("a")]));
        assert_eq!(core.in_flight(), 0);
        assert_eq!(core.abandon(TOKEN), None, "no such wait");
    }

    #[test]
    fn writes_in_flight_are_tracked_independently() {
        let mut core = CoherenceCore::new(node("writer"));
        let older = WriteToken { epoch: 2, seq: 4 };
        let snapshot = [holder("a", Some(older), None)];
        assert_eq!(core.step(older, NOW, &snapshot), CoherenceStep::AllApplied);
        assert!(matches!(
            core.step(TOKEN, NOW, &snapshot),
            CoherenceStep::Waiting { .. }
        ));
        assert_eq!(core.in_flight(), 1);
    }
}
