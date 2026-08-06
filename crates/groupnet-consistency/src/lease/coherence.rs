//! The writer's half of the coherence-lease tier: how far one coherent write
//! has got through the set of readers holding a live serve-lease.
//!
//! Sans-IO like its reader-side sibling ([`LeaseCore`](super::LeaseCore)): it
//! is fed snapshots of the writer's own view of the group and returns a
//! verdict per poll. Nothing here waits, sleeps, or reads a clock — the tokio
//! shell polls it, the deterministic simulator calls it directly, and both see
//! the same rules.

use std::collections::{BTreeMap, BTreeSet};

use groupnet_core::NodeId;

use crate::token::WriteToken;

/// One member of a coherent write's wait set, as the **writer's own** view of
/// the group shows it: a member currently holding a live `~lease` entry, and
/// the highest token of this writer's feed it advertises having applied.
///
/// The snapshot carries only live lease-holders. A member that drops out of it
/// between polls is one whose lease the writer's engine has expired — the slow
/// path, and the guarantee that ends the wait when acks never arrive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitMember {
    /// The lease-holding member.
    pub member: NodeId,
    /// The highest token of the writer's feed this member advertises having
    /// applied (`None`: no ledger, or nothing from this writer yet).
    pub applied: Option<WriteToken>,
}

/// One poll's verdict on a coherent write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoherenceStep {
    /// Every lease-holder in the wait set has applied the write — the fast
    /// path, and exactly the cost of a T2 ack round when the cluster is
    /// healthy.
    AllApplied,
    /// The wait is over, but not everyone acknowledged: `stragglers` never
    /// applied the write, and the writer's own engine has since expired their
    /// serve-leases. They cannot serve stale state — a lapsed reader is out of
    /// service until it re-synchronizes.
    LeaseLapsed {
        /// The members excused by lapse rather than by acknowledgement, in id
        /// order.
        stragglers: Vec<NodeId>,
    },
    /// Still waiting: these members hold live leases and have not applied the
    /// write yet, in id order.
    Waiting {
        /// The members still being waited on.
        on: Vec<NodeId>,
    },
}

/// One in-flight write's progress through its wait set.
#[derive(Debug, Default)]
struct Progress {
    /// Lease-holders that have not applied the write yet.
    waiting: BTreeSet<NodeId>,
    /// Lease-holders excused because their lease lapsed in the writer's view.
    /// Permanent for this write: see [`CoherenceCore::step`].
    lapsed: BTreeSet<NodeId>,
}

/// The writer's half: how far a coherent write has got through its wait set.
///
/// A pure function of the snapshots it is fed, plus the memory of what it has
/// already seen — the tokio shell polls [`step`](Self::step) and the simulator
/// calls it directly. Nothing here waits, sleeps, or reads a clock; a deadline
/// is the caller's business ([`abandon`](Self::abandon) turns one into
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

    /// Folds one snapshot of the live lease-holders into `token`'s wait and
    /// reports the verdict. A terminal verdict ([`CoherenceStep::AllApplied`],
    /// [`CoherenceStep::LeaseLapsed`]) drops the write's bookkeeping, so a
    /// poll loop leaks nothing; a caller that gives up on a
    /// [`CoherenceStep::Waiting`] must call [`abandon`](Self::abandon).
    ///
    /// The rules, in the order they apply:
    ///
    /// 1. A member that advertises having applied `token` is satisfied and
    ///    leaves the wait set.
    /// 2. A member in the snapshot that has **not** applied it joins (or stays
    ///    in) the wait set — including one that appears late, which is the
    ///    conservative direction: a reader that took a lease mid-write is
    ///    waited for rather than assumed clean.
    /// 3. A member the wait set holds that has **left** the snapshot has had
    ///    its lease expired by the writer's own engine. It is excused, and it
    ///    is excused **permanently for this write** — a lapse forced it into
    ///    [`LeaseState::NeedsResync`](super::LeaseState::NeedsResync), so even
    ///    if it renews a lease a moment
    ///    later it may not serve until it has re-synchronized, and
    ///    re-entering the wait set could only stall the writer for nothing.
    pub fn step(&mut self, token: WriteToken, snapshot: &[WaitMember]) -> CoherenceStep {
        let writer = self.writer.clone();
        let progress = self.inflight.entry(token).or_default();
        for holder in snapshot {
            if holder.member == writer || progress.lapsed.contains(&holder.member) {
                continue;
            }
            if holder.applied.is_some_and(|applied| applied >= token) {
                progress.waiting.remove(&holder.member);
            } else {
                progress.waiting.insert(holder.member.clone());
            }
        }
        let present: BTreeSet<&NodeId> = snapshot.iter().map(|holder| &holder.member).collect();
        let gone: Vec<NodeId> = progress
            .waiting
            .iter()
            .filter(|member| !present.contains(member))
            .cloned()
            .collect();
        for member in gone {
            progress.waiting.remove(&member);
            progress.lapsed.insert(member);
        }
        if !progress.waiting.is_empty() {
            return CoherenceStep::Waiting {
                on: progress.waiting.iter().cloned().collect(),
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
        Some(self.inflight.remove(&token)?.waiting.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use groupnet_core::NodeId;

    use super::{CoherenceCore, CoherenceStep, WaitMember};
    use crate::token::WriteToken;

    fn node(name: &str) -> NodeId {
        NodeId::new(name)
    }

    const TOKEN: WriteToken = WriteToken { epoch: 2, seq: 5 };

    fn holder(name: &str, applied: Option<WriteToken>) -> WaitMember {
        WaitMember {
            member: node(name),
            applied,
        }
    }

    #[test]
    fn an_empty_wait_set_is_immediately_coherent() {
        let mut core = CoherenceCore::new(node("writer"));
        assert_eq!(core.step(TOKEN, &[]), CoherenceStep::AllApplied);
        assert_eq!(core.in_flight(), 0, "a terminal verdict drops its state");
    }

    #[test]
    fn every_lease_holder_applying_is_the_fast_path() {
        let mut core = CoherenceCore::new(node("writer"));
        let behind = [
            holder("a", Some(WriteToken { epoch: 2, seq: 4 })),
            holder("b", None),
        ];
        assert_eq!(
            core.step(TOKEN, &behind),
            CoherenceStep::Waiting {
                on: vec![node("a"), node("b")],
            }
        );
        assert_eq!(core.in_flight(), 1);
        let applied = [
            holder("a", Some(TOKEN)),
            holder("b", Some(WriteToken { epoch: 2, seq: 9 })),
        ];
        assert_eq!(core.step(TOKEN, &applied), CoherenceStep::AllApplied);
        assert_eq!(core.in_flight(), 0);
    }

    #[test]
    fn a_lapsed_lease_ends_the_wait_and_names_the_straggler() {
        let mut core = CoherenceCore::new(node("writer"));
        let waiting = [holder("a", Some(TOKEN)), holder("silent", None)];
        assert_eq!(
            core.step(TOKEN, &waiting),
            CoherenceStep::Waiting {
                on: vec![node("silent")],
            }
        );
        // `silent` never acknowledged, and this writer's engine has now
        // expired its `~lease` entry: it is out of the snapshot, and out of
        // service until it re-synchronizes.
        assert_eq!(
            core.step(TOKEN, &[holder("a", Some(TOKEN))]),
            CoherenceStep::LeaseLapsed {
                stragglers: vec![node("silent")],
            }
        );
        assert_eq!(core.in_flight(), 0);
    }

    #[test]
    fn a_renewed_lease_does_not_re_enter_a_wait_it_already_lapsed_out_of() {
        let mut core = CoherenceCore::new(node("writer"));
        let both = [holder("a", None), holder("flapper", None)];
        assert!(matches!(
            core.step(TOKEN, &both),
            CoherenceStep::Waiting { .. }
        ));
        // `flapper`'s lease expires in the writer's view…
        assert_eq!(
            core.step(TOKEN, &[holder("a", None)]),
            CoherenceStep::Waiting {
                on: vec![node("a")],
            }
        );
        // …and it renews a moment later, still not having applied the write.
        // It lapsed, so it is in `NeedsResync` and cannot serve: waiting on it
        // again would stall the writer for nothing.
        assert_eq!(
            core.step(TOKEN, &both),
            CoherenceStep::Waiting {
                on: vec![node("a")],
            }
        );
        assert_eq!(
            core.step(TOKEN, &[holder("a", Some(TOKEN)), holder("flapper", None)]),
            CoherenceStep::LeaseLapsed {
                stragglers: vec![node("flapper")],
            }
        );
    }

    #[test]
    fn a_reader_that_takes_a_lease_mid_write_is_waited_for() {
        let mut core = CoherenceCore::new(node("writer"));
        assert_eq!(
            core.step(TOKEN, &[holder("a", None)]),
            CoherenceStep::Waiting {
                on: vec![node("a")],
            }
        );
        // `late` shows up holding a lease it took after the write began: the
        // conservative direction is to wait for it too.
        assert_eq!(
            core.step(TOKEN, &[holder("a", Some(TOKEN)), holder("late", None)]),
            CoherenceStep::Waiting {
                on: vec![node("late")],
            }
        );
    }

    #[test]
    fn a_writer_never_waits_on_itself() {
        let mut core = CoherenceCore::new(node("writer"));
        assert_eq!(
            core.step(TOKEN, &[holder("writer", None)]),
            CoherenceStep::AllApplied
        );
    }

    #[test]
    fn abandoning_a_wait_reports_who_was_left() {
        let mut core = CoherenceCore::new(node("writer"));
        let _ = core.step(TOKEN, &[holder("a", None), holder("b", Some(TOKEN))]);
        assert_eq!(core.abandon(TOKEN), Some(vec![node("a")]));
        assert_eq!(core.in_flight(), 0);
        assert_eq!(core.abandon(TOKEN), None, "no such wait");
    }

    #[test]
    fn writes_in_flight_are_tracked_independently() {
        let mut core = CoherenceCore::new(node("writer"));
        let older = WriteToken { epoch: 2, seq: 4 };
        let snapshot = [holder("a", Some(older))];
        assert_eq!(core.step(older, &snapshot), CoherenceStep::AllApplied);
        assert!(matches!(
            core.step(TOKEN, &snapshot),
            CoherenceStep::Waiting { .. }
        ));
        assert_eq!(core.in_flight(), 1);
    }
}
