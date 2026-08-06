//! The election's pure vocabulary: what part a node is playing, the total order
//! two beliefs about the group are reconciled by, and the truth table row 1's
//! claim guard is.
//!
//! Nothing here reads engine state or emits an effect. Everything is a function
//! of its arguments, which is what lets the rows in the parent module be one
//! auditable expression each — and lets these be tabulated exhaustively by the
//! tests at the bottom of this file rather than inferred from a scenario.

use std::cmp::Ordering;
use std::collections::BTreeSet;

use crate::{NodeId, placement};

/// What part the local node is playing in its group's election right now.
///
/// Always [`Follower`](Role::Follower) in an
/// [`Eventual`](crate::GroupMode::Eventual) group, which runs no election.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// Neither claiming nor hosting: following whatever pair this node has
    /// adopted (which may be no host at all).
    Follower,
    /// A claim of this node's own is standing, waiting out its settle window.
    Claimant,
    /// This node activated its claim and holds the group for the adopted
    /// epoch, until its lease lapses or a higher pair fences it.
    Host,
}

/// The fencing order over `(epoch, host)` pairs, as a total order.
///
/// Epoch-major; at equal epochs a `None` host sorts below any `Some` (a
/// hostless belief never displaces a live one), and two named hosts are
/// separated by the [`placement::owner`] of `group` among just those two.
/// That tiebreak is a pure function of the group id and the two ids, so it is
/// view-independent: every node agrees on it, whatever it believes about
/// membership.
pub(super) fn cmp_pair(
    group: &str,
    a: (u64, Option<&NodeId>),
    b: (u64, Option<&NodeId>),
) -> Ordering {
    match a.0.cmp(&b.0) {
        Ordering::Equal => {}
        by_epoch => return by_epoch,
    }
    match (a.1, b.1) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(x), Some(y)) if x == y => Ordering::Equal,
        (Some(x), Some(y)) => {
            let pair: BTreeSet<NodeId> = [x.clone(), y.clone()].into_iter().collect();
            if placement::owner(group, &pair).as_ref() == Some(x) {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
    }
}

/// Everything row 1's claim guard reads, gathered so the rule itself is one
/// auditable expression rather than a condition smeared across a function.
#[derive(Clone, Copy, Debug)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "this type IS the guard's truth table — four independent vetoes, each \
              enumerated by the unit test. Two-variant enums would say the same thing \
              at four times the length and make the table unreadable."
)]
pub(super) struct ClaimGuard {
    /// This node has voluntarily left.
    pub leaving: bool,
    /// The boot guard has elapsed.
    pub past_boot_guard: bool,
    /// This node is the group's top-ranked live candidate.
    pub top_ranked: bool,
    /// The adopted pair already names this node as host.
    pub adopted_host_is_self: bool,
}

impl ClaimGuard {
    /// Whether a claim may be opened: only by a top-ranked live candidate that
    /// has not left, is past its boot guard, and does not already believe
    /// *itself* to be the adopted host — that belief is entered only by
    /// activating, and re-claiming on top of it would churn the epoch for
    /// nothing.
    pub(super) const fn opens(self) -> bool {
        !self.leaving && self.past_boot_guard && self.top_ranked && !self.adopted_host_is_self
    }
}

#[cfg(test)]
mod tests {
    use super::{ClaimGuard, Ordering, cmp_pair};
    use crate::{NodeId, placement};
    use std::collections::BTreeSet;

    const GROUP: &str = "g";

    fn n(id: &str) -> NodeId {
        NodeId::new(id)
    }

    /// The two ids of a pair, ranked: the one `placement::owner` picks for the
    /// group first. This is exactly the tiebreak `cmp_pair` applies.
    fn ranked_pair(a: &str, b: &str) -> (NodeId, NodeId) {
        let (a, b) = (n(a), n(b));
        let set: BTreeSet<NodeId> = [a.clone(), b.clone()].into_iter().collect();
        let winner = placement::owner(GROUP, &set).expect("two candidates");
        let loser = if winner == a { b } else { a };
        (winner, loser)
    }

    /// Epoch dominates the host entirely: a higher epoch wins even when the
    /// host it names would lose the equal-epoch tiebreak, and even when it
    /// names no host at all.
    #[test]
    fn the_order_is_epoch_major() {
        let (winner, loser) = ranked_pair("x", "y");
        assert_eq!(
            cmp_pair(GROUP, (8, Some(&loser)), (7, Some(&winner))),
            Ordering::Greater
        );
        assert_eq!(
            cmp_pair(GROUP, (8, None), (7, Some(&winner))),
            Ordering::Greater
        );
        assert_eq!(
            cmp_pair(GROUP, (7, Some(&winner)), (8, Some(&loser))),
            Ordering::Less
        );
    }

    /// At equal epochs the placement owner of the group among the two hosts
    /// wins — the view-independent tiebreak that lets both sides of a heal
    /// agree without exchanging anything.
    #[test]
    fn equal_epochs_break_by_placement_owner() {
        let (winner, loser) = ranked_pair("x", "y");
        assert_eq!(
            cmp_pair(GROUP, (7, Some(&winner)), (7, Some(&loser))),
            Ordering::Greater
        );
        assert_eq!(
            cmp_pair(GROUP, (7, Some(&loser)), (7, Some(&winner))),
            Ordering::Less
        );
        assert_eq!(
            cmp_pair(GROUP, (7, Some(&winner)), (7, Some(&winner))),
            Ordering::Equal
        );
    }

    /// A hostless belief never displaces a live one at the same epoch, and two
    /// hostless beliefs are indistinguishable.
    #[test]
    fn none_sorts_below_some_at_equal_epoch() {
        let host = n("x");
        assert_eq!(cmp_pair(GROUP, (7, None), (7, Some(&host))), Ordering::Less);
        assert_eq!(
            cmp_pair(GROUP, (7, Some(&host)), (7, None)),
            Ordering::Greater
        );
        assert_eq!(cmp_pair(GROUP, (7, None), (7, None)), Ordering::Equal);
    }

    /// The order really is a total order over a spread of pairs: antisymmetric
    /// and transitive. Anything less and two nodes could disagree about which
    /// of two pairs survives a heal.
    #[test]
    fn the_order_is_total_and_transitive() {
        let ids: Vec<Option<NodeId>> = [None, Some(n("x")), Some(n("y")), Some(n("z"))].into();
        let pairs: Vec<(u64, Option<&NodeId>)> = (6u64..=8)
            .flat_map(|e| ids.iter().map(move |h| (e, h.as_ref())))
            .collect();
        for a in &pairs {
            for b in &pairs {
                assert_eq!(
                    cmp_pair(GROUP, *a, *b).reverse(),
                    cmp_pair(GROUP, *b, *a),
                    "asymmetry at {a:?} vs {b:?}"
                );
                for c in &pairs {
                    let (ab, bc) = (cmp_pair(GROUP, *a, *b), cmp_pair(GROUP, *b, *c));
                    if ab == bc && ab != Ordering::Equal {
                        assert_eq!(
                            cmp_pair(GROUP, *a, *c),
                            ab,
                            "intransitive at {a:?} {b:?} {c:?}"
                        );
                    }
                }
            }
        }
    }

    /// The whole claim guard, tabulated: exactly one of the sixteen states
    /// opens a claim, and each of the four inputs vetoes on its own.
    #[test]
    fn only_a_live_top_ranked_node_past_its_boot_guard_may_claim() {
        for leaving in [false, true] {
            for past_boot_guard in [false, true] {
                for top_ranked in [false, true] {
                    for adopted_host_is_self in [false, true] {
                        let guard = ClaimGuard {
                            leaving,
                            past_boot_guard,
                            top_ranked,
                            adopted_host_is_self,
                        };
                        let want =
                            !leaving && past_boot_guard && top_ranked && !adopted_host_is_self;
                        assert_eq!(guard.opens(), want, "{guard:?}");
                    }
                }
            }
        }
        // The one state that claims, spelled out rather than inferred.
        assert!(
            ClaimGuard {
                leaving: false,
                past_boot_guard: true,
                top_ranked: true,
                adopted_host_is_self: false,
            }
            .opens()
        );
    }
}
