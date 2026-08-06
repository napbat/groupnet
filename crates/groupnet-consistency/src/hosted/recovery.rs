//! The leader-completeness recovery rule, as a pure predicate: may this
//! freshly-activated host begin serving?
//!
//! Sans-IO, stateless, and fed the same [`LedgerView`] snapshots the commit
//! rule is fed. The simulator drives it directly; the tokio shell polls it. This
//! is the gate behind [`HostedError::Recovering`](super::HostedError::Recovering)
//! — and, per the M4 as-built contract, it gates **hosted service**, never the
//! engine's leadership activation. A node is host the instant the engine says
//! so; it *serves* when this core says `Complete`.
//!
//! # The rule
//!
//! A host activating at epoch `e′` may serve iff
//!
//! ```text
//! |{ v in view : v.lead_epoch >= e' }| >= |view| / 2 + 1        (a fresh majority, S_r)
//! and, for every writer w named by any v in S_r:
//!     own_applied(w) >= max over v in S_r of v.wm(w)            (the target)
//! ```
//!
//! # Why `>= e′` and not `== e′`
//!
//! The test is *freshness*, not agreement. A voter stamped above the serving
//! epoch has adopted a **later** view than this host's — the host is already
//! being deposed, and it will learn so — but its reading is, if anything, newer
//! than one stamped exactly `e′`, so it is exactly as good a witness. A voter
//! stamped **below** `e′` is the one that must be refused: its reading predates
//! the migration, and a majority made of such readings is where a recovering
//! host undershoots its target and drops a write the old host had already
//! acknowledged.
//!
//! The freshness majority is what carries S5, not the arithmetic of the target.
//! Once a majority has stamped `≥ e′`, no commit round at any `e < e′` can ever
//! close again (its predicate needs `lead_epoch == e` from a majority, and a
//! majority has moved past it) — so every write that *was* committed below `e′`
//! had a commit majority that intersects `S_r`, and the intersecting voter's
//! fresh reading is a later publication than the one that counted the ack.
//! Monotone watermarks do the rest. The full argument is in the M4 as-built
//! subsection of `docs/consistency-modes.md`.
//!
//! # Beyond the ring
//!
//! A target past the end of the host's visible window of a writer's feed is
//! reached the way any lagging subscriber reaches one: [`PeerWrite::Gap`], the
//! consumer's own coarse remediation, and a [`Frontier`] advanced into the
//! target. Completeness is satisfied by the remediation, not by replaying the
//! individual writes — a consumer that needs exact replay sizes the ring for the
//! worst migration lag it accepts.
//!
//! A consumer with no store of its own to remediate *from* has a third option,
//! and this core is unchanged by it: the optional `handoff` feature (module
//! `hosted::handoff`) pulls a covering snapshot from a donor over the data
//! plane, and the requester then seeds its ledger from the receipt and asks
//! [`CompletenessCore::step`] again. The rule never learns that a handoff
//! happened; it only ever sees watermarks that moved.
//!
//! [`PeerWrite::Gap`]: crate::PeerWrite::Gap
//! [`Frontier`]: crate::Frontier

use groupnet_core::NodeId;

use super::ledger::{LedgerView, Watermarks};
use crate::token::WriteToken;

/// Whether a freshly-activated host may begin serving.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Completeness {
    /// Not serving. `needed` names, per writer, the watermark this host must
    /// still reach, in writer-id order.
    ///
    /// **An empty `needed` is not "almost there".** It means no fresh majority
    /// has been read yet, so there is no target to compute: the host is waiting
    /// on gossip, not on its own apply loop. The two situations look different
    /// to an operator and identical to the rule — both are "refuse service", and
    /// neither is an error.
    Recovering {
        /// Per writer, the watermark this host must reach. Empty when no fresh
        /// majority has been read yet.
        needed: Vec<(NodeId, WriteToken)>,
    },
    /// A fresh majority has been read and this host's applied state covers
    /// everything it named. Service may begin.
    Complete,
}

impl Completeness {
    /// Whether this host may begin serving.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        matches!(self, Completeness::Complete)
    }
}

/// The recovery rule. Stateless for the same reason
/// [`CommitCore`](super::CommitCore) is: the verdict is a pure function of one
/// snapshot, so there is nothing worth carrying between polls — and a core that
/// remembers nothing cannot be fooled by a reading it has already seen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompletenessCore;

impl CompletenessCore {
    /// Evaluates the recovery rule for a host serving at `serving_epoch`,
    /// against `view` and this host's own applied watermarks.
    ///
    /// `view` must be the **whole** static voter roster — silent voters included
    /// as [`LedgerView::reading`] `None` — because the majority threshold is
    /// derived from its length. `alive` is ignored entirely: a static roster is
    /// liveness-blind, and a voter that has published a fresh reading is a
    /// witness whether or not membership currently believes it up.
    #[must_use]
    pub fn step(serving_epoch: u64, view: &[LedgerView], own_applied: &Watermarks) -> Completeness {
        let fresh: Vec<&LedgerView> = view
            .iter()
            .filter(|member| {
                member
                    .reading
                    .as_ref()
                    .is_some_and(|reading| reading.lead_epoch >= serving_epoch)
            })
            .collect();
        // `len / 2 + 1`: the same strict majority the commit rule uses, so the
        // two always intersect. An empty roster asks for one witness it can
        // never collect, and the host never serves — the fail-safe answer.
        if fresh.len() < view.len() / 2 + 1 {
            return Completeness::Recovering { needed: Vec::new() };
        }
        let mut targets = Watermarks::new();
        for member in fresh {
            let Some(reading) = member.reading.as_ref() else {
                continue;
            };
            for (writer, token) in &reading.applied {
                let entry = targets
                    .entry(writer.clone())
                    .or_insert(WriteToken { epoch: 0, seq: 0 });
                if *token > *entry {
                    *entry = *token;
                }
            }
        }
        let needed: Vec<(NodeId, WriteToken)> = targets
            .into_iter()
            .filter(|(writer, target)| own_applied.get(writer).is_none_or(|own| own < target))
            .collect();
        if needed.is_empty() {
            Completeness::Complete
        } else {
            Completeness::Recovering { needed }
        }
    }
}

#[cfg(test)]
mod tests {
    use groupnet_core::NodeId;

    use super::{Completeness, CompletenessCore};
    use crate::hosted::ledger::{LedgerView, Reading, Watermarks};
    use crate::token::WriteToken;

    /// The epoch the recovering host is activating at.
    const SERVING: u64 = 6;

    /// One row of a verdict table: a name, the recovering host's own applied
    /// watermarks as `(writer, epoch, seq)`, and the expected verdict.
    type Case = (&'static str, Vec<(&'static str, u64, u64)>, Completeness);

    fn node(name: &str) -> NodeId {
        NodeId::new(name)
    }

    fn marks(pairs: &[(&str, u64, u64)]) -> Watermarks {
        pairs
            .iter()
            .map(|(writer, epoch, seq)| {
                (
                    node(writer),
                    WriteToken {
                        epoch: *epoch,
                        seq: *seq,
                    },
                )
            })
            .collect()
    }

    /// A voter whose ledger is stamped `stamp` and names `applied`.
    fn voter(name: &str, stamp: u64, applied: &[(&str, u64, u64)]) -> LedgerView {
        LedgerView {
            member: node(name),
            alive: true,
            reading: Some(Reading {
                lead_epoch: stamp,
                applied: marks(applied),
            }),
        }
    }

    /// A voter that publishes no ledger at all.
    fn silent(name: &str) -> LedgerView {
        LedgerView {
            member: node(name),
            alive: true,
            reading: None,
        }
    }

    fn step(view: &[LedgerView], own: &[(&str, u64, u64)]) -> Completeness {
        CompletenessCore::step(SERVING, view, &marks(own))
    }

    fn needing(pairs: &[(&str, u64, u64)]) -> Completeness {
        Completeness::Recovering {
            needed: pairs
                .iter()
                .map(|(writer, epoch, seq)| {
                    (
                        node(writer),
                        WriteToken {
                            epoch: *epoch,
                            seq: *seq,
                        },
                    )
                })
                .collect(),
        }
    }

    const NO_MAJORITY: Completeness = Completeness::Recovering { needed: Vec::new() };

    #[test]
    fn a_stale_stamped_voter_is_excluded_from_the_fresh_majority() {
        // Three voters, majority two. Only `a` has adopted the new epoch, so
        // there is no fresh majority — and the host waits even though the two
        // stale readings agree with each other and would have "covered" a
        // target numerically. Freshness is a count, not an arithmetic.
        let stale = vec![
            voter("a", SERVING, &[("old-host", 5, 9)]),
            voter("b", 5, &[("old-host", 5, 9)]),
            voter("c", 5, &[("old-host", 5, 9)]),
        ];
        assert_eq!(step(&stale, &[("old-host", 5, 9)]), NO_MAJORITY);
        // One more voter adopts the epoch and the majority is fresh.
        let fresh = vec![
            voter("a", SERVING, &[("old-host", 5, 9)]),
            voter("b", SERVING, &[("old-host", 5, 9)]),
            voter("c", 5, &[("old-host", 5, 9)]),
        ];
        assert_eq!(step(&fresh, &[("old-host", 5, 9)]), Completeness::Complete);
    }

    #[test]
    fn a_stale_voters_watermark_does_not_enter_the_target() {
        // `c` is stale and further ahead than either fresh voter. It is outside
        // `S_r`, so it does not raise the target: anything it applied that was
        // genuinely *committed* has its own witness inside the fresh majority.
        let view = vec![
            voter("a", SERVING, &[("old-host", 5, 4)]),
            voter("b", SERVING, &[("old-host", 5, 4)]),
            voter("c", 5, &[("old-host", 5, 99)]),
        ];
        assert_eq!(step(&view, &[("old-host", 5, 4)]), Completeness::Complete);
    }

    #[test]
    fn a_voter_stamped_above_the_serving_epoch_is_still_a_witness() {
        // This host is already being deposed and will learn so. Meanwhile a
        // reading from a *later* view is, if anything, a better witness than one
        // stamped exactly at `SERVING`.
        let view = vec![
            voter("a", SERVING + 3, &[("old-host", 5, 9)]),
            voter("b", SERVING, &[("old-host", 5, 9)]),
            silent("c"),
        ];
        assert_eq!(step(&view, &[("old-host", 5, 9)]), Completeness::Complete);
    }

    #[test]
    fn the_target_is_the_max_over_the_fresh_majority_per_writer() {
        // Every writer *any* fresh voter names contributes, and the target is
        // the highest watermark any of them holds — the monotonicity fold.
        let view = vec![
            voter("a", SERVING, &[("h1", 5, 4), ("h2", 2, 1)]),
            voter("b", SERVING, &[("h1", 5, 9)]),
            voter("c", SERVING, &[("h3", 1, 7)]),
        ];
        assert_eq!(
            step(&view, &[]),
            needing(&[("h1", 5, 9), ("h2", 2, 1), ("h3", 1, 7)])
        );
        // Reaching every target — at it, or past it — completes.
        assert_eq!(
            step(&view, &[("h1", 5, 9), ("h2", 2, 1), ("h3", 1, 7)]),
            Completeness::Complete
        );
        assert_eq!(
            step(&view, &[("h1", 6, 1), ("h2", 2, 4), ("h3", 1, 7)]),
            Completeness::Complete,
            "epoch-major: a newer life covers an older target"
        );
        // One writer short is enough to keep refusing service, and only that
        // writer is named.
        assert_eq!(
            step(&view, &[("h1", 5, 9), ("h2", 2, 1), ("h3", 1, 6)]),
            needing(&[("h3", 1, 7)])
        );
    }

    #[test]
    fn the_boundary_majorities_are_exact() {
        let fresh = |name: &str| voter(name, SERVING, &[("old-host", 5, 1)]);
        // Three voters, majority two.
        assert_eq!(
            step(&[fresh("a"), silent("b"), silent("c")], &[]),
            NO_MAJORITY
        );
        assert_eq!(
            step(
                &[fresh("a"), fresh("b"), silent("c")],
                &[("old-host", 5, 1)]
            ),
            Completeness::Complete
        );
        // Four voters, majority three — an even roster does not round down.
        assert_eq!(
            step(&[fresh("a"), fresh("b"), silent("c"), silent("d")], &[]),
            NO_MAJORITY
        );
        assert_eq!(
            step(
                &[fresh("a"), fresh("b"), fresh("c"), silent("d")],
                &[("old-host", 5, 1)]
            ),
            Completeness::Complete
        );
        // A roster of one is its own majority; an empty roster never serves.
        assert_eq!(
            step(&[fresh("a")], &[("old-host", 5, 1)]),
            Completeness::Complete
        );
        assert_eq!(step(&[silent("a")], &[]), NO_MAJORITY);
        assert_eq!(step(&[], &[]), NO_MAJORITY, "an empty roster is fail-safe");
    }

    /// The intersection argument, played out as a table.
    ///
    /// Roster `{a, b, c}`, majority two. Old host `h` committed the write
    /// `(5, 9)` at epoch 5 with commit majority `S_c = {a, b}` — both stamped 5,
    /// both applied `(5, 9)`. Then `b` and `c` adopt epoch 6 and republish; `a`
    /// is partitioned and stays stamped 5. The new host recovers at 6 from
    /// `S_r = {b, c}`, and `S_c ∩ S_r = {b}` is what forces the target up to the
    /// acked write — even though `c`, the other half of the fresh majority, was
    /// far behind.
    #[test]
    fn the_intersection_forces_the_target_past_every_acked_write() {
        let acked = ("h", 5, 9);
        let after_migration = vec![
            // `a`: still in the old view. Not a witness.
            voter("a", 5, &[acked]),
            // `b`: the intersection voter. Counted the ack at stamp 5, and its
            // reading at stamp 6 is a strictly later publication of a monotone
            // watermark — so it still names `(5, 9)`.
            voter("b", 6, &[acked]),
            // `c`: fresh, and far behind — it never applied the acked write.
            voter("c", 6, &[("h", 5, 2)]),
        ];
        let cases: Vec<Case> = vec![
            (
                "a new host that applied nothing must reach the acked write",
                vec![],
                needing(&[acked]),
            ),
            (
                "…and one that got as far as `c` did is still short of it",
                vec![("h", 5, 2)],
                needing(&[acked]),
            ),
            (
                "one sequence short is still short",
                vec![("h", 5, 8)],
                needing(&[acked]),
            ),
            (
                "at the acked write, service may begin",
                vec![acked],
                Completeness::Complete,
            ),
            (
                "past it, likewise",
                vec![("h", 5, 20)],
                Completeness::Complete,
            ),
        ];
        for (name, own, expected) in cases {
            assert_eq!(step(&after_migration, &own), expected, "{name}");
        }
        // The load-bearing half, stated as its own assertion: had the rule
        // counted `a`'s stale reading toward `S_r` and dropped the freshness
        // test, `{a, c}` would also be a "majority" — one whose target is `c`'s
        // (5, 2)… and `a`'s (5, 9). The max saves this particular shape, but the
        // shape the freshness test really refuses is the one where the old host
        // commits *after* the recovery read. That is why `S_r` is counted in
        // fresh readings and not in readings at all.
        let all_stale = vec![
            voter("a", 5, &[("h", 5, 2)]),
            voter("b", 5, &[("h", 5, 2)]),
            voter("c", 5, &[("h", 5, 2)]),
        ];
        assert_eq!(
            step(&all_stale, &[("h", 5, 2)]),
            NO_MAJORITY,
            "a pre-migration majority is not a recovery majority"
        );
    }
}
