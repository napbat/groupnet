//! The commit rule, as a pure predicate: has this write been applied widely
//! enough to acknowledge?
//!
//! Sans-IO like the lease tier's cores. [`CommitCore::evaluate`] is fed one
//! snapshot of what the group's members publish and returns one verdict — so
//! the deterministic simulator runs exactly the code the tokio shell runs, and
//! the rule is provable in virtual time rather than argued about in prose.
//!
//! # The rule
//!
//! For a write authored by host `H` at epoch `e`, bearing token `t` (so
//! `t.epoch = e`), one member **counts** iff it publishes a reading
//! `(lead_epoch, wm)` with
//!
//! ```text
//! lead_epoch == t.epoch   and   wm(H) >= t
//! ```
//!
//! and the level decides how many must count:
//!
//! * [`Commit::Local`](super::Commit::Local) — nobody. The host applied it; the
//!   verdict is [`CommitVerdict::Committed`] on sight.
//! * [`Commit::QuorumApplied`](super::Commit::QuorumApplied) — a strict majority
//!   of the view, which the caller must supply as the **whole static voter
//!   roster**. Liveness plays no part: a voter's reading counts whether or not
//!   membership believes it alive, which is exactly what makes a static roster
//!   the denominator rather than a rumour-derived set.
//! * [`Commit::AllApplied`](super::Commit::AllApplied) — every member of the
//!   view that is currently `alive`. Unanimity over a rumour-derived set, and it
//!   inherits the ack tier's honesty box wholesale.
//!
//! # Why the stamp equality is `==` and not `>=`
//!
//! A **lower** stamp is a stale view: the voter has not yet adopted the epoch
//! this write belongs to, so its watermark for `H` describes some previous life
//! of the feed. A **higher** stamp is the view-stamp fence: the voter has moved
//! on to a later epoch, and counting it would let a round opened at `e` close
//! after a successor has already recovered — the late-ack race, which is how a
//! committed write gets lost. Both directions refuse, and the second is the one
//! that carries S5.
//!
//! # Where the host sits
//!
//! The core does not know which member is hosting; it only uses `host` as the
//! key into each reading's watermark map. Whether the host appears in its own
//! view is the caller's choice, and the core treats it like any other member: a
//! host that applies its own writes and records them satisfies its own
//! predicate, and a host that does not is waited on by name. That is the
//! fail-closed direction, and it is the same one the deployment contract asks
//! of every voter.

use groupnet_core::NodeId;

use super::Commit;
use super::ledger::LedgerView;
use crate::token::WriteToken;

/// One evaluation's verdict on a write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitVerdict {
    /// The level's threshold is met: the write may be acknowledged.
    Committed,
    /// Not yet. `waiting_on` names the members of the view that do not count,
    /// in id order — the honest answer to "what is this write waiting for",
    /// and what a timeout reports back to the caller.
    Pending {
        /// The members that do not (yet) count, in id order.
        waiting_on: Vec<NodeId>,
    },
}

impl CommitVerdict {
    /// Whether the write may be acknowledged.
    #[must_use]
    pub fn is_committed(&self) -> bool {
        matches!(self, CommitVerdict::Committed)
    }
}

/// The commit rule. Stateless — unlike the lease tier's `CoherenceCore`, which
/// must remember who lapsed out of a wait, a commit verdict is a pure function
/// of one snapshot and there is nothing worth carrying between polls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitCore;

impl CommitCore {
    /// Evaluates `level` for the write `host` authored at `token`, against
    /// `view`.
    ///
    /// `view` is the caller's roster snapshot: the **whole** static voter roster
    /// for [`Commit::QuorumApplied`](super::Commit::QuorumApplied) (silent
    /// voters included, as [`LedgerView::reading`] `None`), or the members a
    /// selector admits for
    /// [`Commit::AllApplied`](super::Commit::AllApplied). The majority threshold
    /// is derived from the view's length, so omitting silent voters
    /// manufactures majorities — see [`LedgerView`].
    #[must_use]
    pub fn evaluate(
        view: &[LedgerView],
        host: &NodeId,
        token: WriteToken,
        level: Commit,
    ) -> CommitVerdict {
        if level == Commit::Local {
            return CommitVerdict::Committed;
        }
        let mut counted = 0usize;
        let mut waiting_on = Vec::new();
        let mut required = 0usize;
        for member in view {
            let considered = match level {
                // A static roster is liveness-blind by construction.
                Commit::QuorumApplied => true,
                Commit::AllApplied => member.alive,
                Commit::Local => unreachable!("returned above"),
            };
            if !considered {
                continue;
            }
            required += 1;
            if counts(member, host, token) {
                counted += 1;
            } else {
                waiting_on.push(member.member.clone());
            }
        }
        let threshold = match level {
            // `len / 2 + 1`, the same strict majority the voter roster's own
            // `majority()` computes — so two majorities always intersect, which
            // is the whole safety argument. An empty roster asks for one vote it
            // can never collect: the fail-safe answer, chosen over a `0` that
            // would turn a misconfiguration into a silent loss of the property
            // Quorum is picked for.
            Commit::QuorumApplied => required / 2 + 1,
            Commit::AllApplied => required,
            Commit::Local => unreachable!("returned above"),
        };
        if counted >= threshold {
            CommitVerdict::Committed
        } else {
            waiting_on.sort();
            CommitVerdict::Pending { waiting_on }
        }
    }
}

/// Whether one member's reading counts toward `token`: stamped at exactly the
/// write's epoch, and applied at or past the write.
fn counts(member: &LedgerView, host: &NodeId, token: WriteToken) -> bool {
    member.reading.as_ref().is_some_and(|reading| {
        reading.lead_epoch == token.epoch
            && reading.applied.get(host).is_some_and(|wm| *wm >= token)
    })
}

#[cfg(test)]
mod tests {
    use groupnet_core::NodeId;

    use super::{CommitCore, CommitVerdict};
    use crate::hosted::Commit;
    use crate::hosted::ledger::{LedgerView, Reading, Watermarks};
    use crate::token::WriteToken;

    /// The write under evaluation: host `h`, epoch 7, sequence 3.
    const TOKEN: WriteToken = WriteToken { epoch: 7, seq: 3 };

    /// One row of a verdict table: a name, the roster snapshot, and the
    /// expected verdict.
    type Case = (&'static str, Vec<LedgerView>, CommitVerdict);

    fn node(name: &str) -> NodeId {
        NodeId::new(name)
    }

    fn host() -> NodeId {
        node("h")
    }

    /// A voter believed alive, stamped `stamp`, advertising `applied` for the
    /// host's feed.
    fn voter(name: &str, stamp: u64, applied: WriteToken) -> LedgerView {
        let mut marks = Watermarks::new();
        marks.insert(host(), applied);
        LedgerView {
            member: node(name),
            alive: true,
            reading: Some(Reading {
                lead_epoch: stamp,
                applied: marks,
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

    /// A voter stamped correctly whose ledger names no watermark for the host.
    fn empty_handed(name: &str, stamp: u64) -> LedgerView {
        LedgerView {
            member: node(name),
            alive: true,
            reading: Some(Reading {
                lead_epoch: stamp,
                applied: Watermarks::new(),
            }),
        }
    }

    /// One that counts: stamped 7, applied exactly the write.
    fn good(name: &str) -> LedgerView {
        voter(name, 7, TOKEN)
    }

    fn quorum(view: &[LedgerView]) -> CommitVerdict {
        CommitCore::evaluate(view, &host(), TOKEN, Commit::QuorumApplied)
    }

    fn pending(names: &[&str]) -> CommitVerdict {
        CommitVerdict::Pending {
            waiting_on: names.iter().map(|n| node(n)).collect(),
        }
    }

    #[test]
    fn local_commits_without_looking_at_anybody() {
        for view in [Vec::new(), vec![silent("a"), silent("b"), silent("c")]] {
            assert_eq!(
                CommitCore::evaluate(&view, &host(), TOKEN, Commit::Local),
                CommitVerdict::Committed
            );
        }
    }

    /// The verdict table for `QuorumApplied` over a roster of three (majority
    /// two). Each row isolates one way a voter fails to count.
    #[test]
    fn the_quorum_verdict_table() {
        let cases: Vec<Case> = vec![
            (
                "two of three count",
                vec![good("a"), good("b"), silent("c")],
                CommitVerdict::Committed,
            ),
            (
                "a stale-stamped voter does not count",
                vec![good("a"), voter("b", 6, TOKEN), voter("c", 6, TOKEN)],
                pending(&["b", "c"]),
            ),
            (
                "a higher-stamped voter does not count — the view-stamp fence",
                vec![good("a"), voter("b", 8, TOKEN), voter("c", 8, TOKEN)],
                pending(&["b", "c"]),
            ),
            (
                "a voter behind the write does not count",
                vec![
                    good("a"),
                    voter("b", 7, WriteToken { epoch: 7, seq: 2 }),
                    silent("c"),
                ],
                pending(&["b", "c"]),
            ),
            (
                "a voter past the write counts",
                vec![
                    good("a"),
                    voter("b", 7, WriteToken { epoch: 7, seq: 9 }),
                    silent("c"),
                ],
                CommitVerdict::Committed,
            ),
            (
                "a stamped voter naming no watermark for this host does not count",
                vec![good("a"), empty_handed("b", 7), empty_handed("c", 7)],
                pending(&["b", "c"]),
            ),
            (
                "a dead voter's stamped reading still counts — the roster is liveness-blind",
                vec![
                    good("a"),
                    LedgerView {
                        alive: false,
                        ..good("b")
                    },
                    silent("c"),
                ],
                CommitVerdict::Committed,
            ),
            (
                "nobody counts",
                vec![silent("a"), silent("b"), silent("c")],
                pending(&["a", "b", "c"]),
            ),
        ];
        for (name, view, expected) in cases {
            assert_eq!(quorum(&view), expected, "{name}");
        }
    }

    #[test]
    fn the_majority_boundary_is_exact() {
        // Three voters, majority two: one short is pending, exactly two commits.
        assert_eq!(
            quorum(&[good("a"), silent("b"), silent("c")]),
            pending(&["b", "c"])
        );
        assert_eq!(
            quorum(&[good("a"), good("b"), silent("c")]),
            CommitVerdict::Committed
        );
        // Four voters, majority three — an even roster does not round down.
        let four = |counting: usize| {
            let names = ["a", "b", "c", "d"];
            let view: Vec<LedgerView> = names
                .iter()
                .enumerate()
                .map(|(i, n)| if i < counting { good(n) } else { silent(n) })
                .collect();
            quorum(&view)
        };
        assert_eq!(
            four(2),
            pending(&["c", "d"]),
            "two of four is not a majority"
        );
        assert_eq!(four(3), CommitVerdict::Committed);
        // A roster of one is its own majority.
        assert_eq!(quorum(&[good("a")]), CommitVerdict::Committed);
        assert_eq!(quorum(&[silent("a")]), pending(&["a"]));
    }

    #[test]
    fn an_empty_roster_never_commits() {
        // `0 / 2 + 1 = 1` grant that can never be collected — the same fail-safe
        // answer the engine's empty voter roster gives, and deliberately not the
        // `0` that would commit on nobody's word.
        assert_eq!(quorum(&[]), pending(&[]));
    }

    #[test]
    fn all_applied_needs_every_alive_member_and_ignores_the_dead() {
        let all =
            |view: &[LedgerView]| CommitCore::evaluate(view, &host(), TOKEN, Commit::AllApplied);
        assert_eq!(
            all(&[good("a"), good("b"), silent("c")]),
            pending(&["c"]),
            "unanimity, not a majority"
        );
        assert_eq!(
            all(&[good("a"), good("b"), good("c")]),
            CommitVerdict::Committed
        );
        // A member membership no longer believes alive drops out of the wait
        // entirely — T2's posture, honesty box included.
        assert_eq!(
            all(&[
                good("a"),
                good("b"),
                LedgerView {
                    alive: false,
                    ..silent("c")
                }
            ]),
            CommitVerdict::Committed
        );
        // An empty selection resolves immediately: nobody to wait on. A real,
        // if weak, answer — the same one `applied_by_selected` gives.
        assert_eq!(all(&[]), CommitVerdict::Committed);
    }

    #[test]
    fn waiting_on_is_reported_in_id_order() {
        let view = vec![silent("z"), good("m"), silent("a"), silent("q")];
        assert_eq!(quorum(&view), pending(&["a", "q", "z"]));
    }
}
