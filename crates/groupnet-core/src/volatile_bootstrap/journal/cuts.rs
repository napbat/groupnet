//! Follower-side comparison of live native writer positions with a barrier.
//!
//! A native writer's position is `(epoch, sequence)`, ordered epoch-major: a
//! writer that restarts opens a new epoch, and every position of the new life
//! follows every position of the old one. Moving a writer from one life into
//! the next is never this comparison's decision. A subscriber crosses either
//! with a gap, which covers the whole previous life and remediates it
//! coarsely, or with a sealed renewal, after the previous life promised it
//! ended and every write up to that promise was delivered. A donor's capture
//! records only a sealed renewal and withdraws on a gap; a follower's gap
//! restarts its own recovery and discards its stage. So two positions of one
//! writer in different epochs are only a side that has not crossed yet:
//! pending, like two positions of one life.

use std::cmp::Ordering;

use super::types::NativeCut;

/// How a follower's live native writer positions compare with one donor
/// barrier's covered cuts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CutAlignment {
    /// Every writer is at the same epoch and sequence. A writer known to
    /// only one side aligns only as a quiet zero-position feed.
    Exact,
    /// A writer's positions differ, in either direction and in the same or
    /// different epochs, or a non-quiet writer is known to one side only.
    /// The side behind is still applying the same feed, or has yet to cross
    /// into the writer's newer life; a later barrier can align, and a gap on
    /// either side ends the transfer instead.
    Pending,
    /// An input is not strictly sorted by writer.
    Conflict,
}

/// One writer's live and covered `(epoch, sequence)` positions, `None` on
/// the side that holds no cut for it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CutDifference<'a> {
    /// The native writer.
    pub writer: &'a [u8],
    /// The follower's live position.
    pub live: Option<(u64, u64)>,
    /// The barrier's covered position.
    pub covered: Option<(u64, u64)>,
}

/// A verdict and the writer that decided it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Alignment<'a> {
    /// How the two sides compare.
    pub verdict: CutAlignment,
    /// The first writer, in writer order, whose positions made the verdict
    /// [`CutAlignment::Pending`]. `None` otherwise.
    pub deciding: Option<CutDifference<'a>>,
}

/// Compare `local` with a donor barrier's `covered` cuts.
///
/// Both inputs must be strictly ascending by writer, as a `BTreeMap` of the
/// follower's applied positions and every `BarrierReceipt` are. A follower
/// may install a staged image only at [`CutAlignment::Exact`]: behind means
/// its live feed has not applied the barrier's effects yet, and ahead means
/// its live index holds effects the stage lacks. The transfer protocol
/// answers [`CutAlignment::Pending`] with `NativePending`, which samples a
/// later barrier.
///
/// A barrier cut in an older epoch than the live one covers the writer's old
/// life only up to that cut, and none of the new life; a live cut in the
/// older epoch has not applied the new life the barrier covers. Neither may
/// install until both sides stand in one life at one sequence.
pub fn align_cuts<'a>(
    local: impl IntoIterator<Item = (&'a [u8], u64, u64)>,
    covered: &'a [NativeCut],
) -> Alignment<'a> {
    const UNSORTED: Alignment<'static> = Alignment {
        verdict: CutAlignment::Conflict,
        deciding: None,
    };
    if covered
        .windows(2)
        .any(|pair| pair[0].writer >= pair[1].writer)
    {
        return UNSORTED;
    }
    let mut local = local.into_iter().peekable();
    let mut covered = covered.iter().peekable();
    let mut previous: Option<&[u8]> = None;
    let mut pending = None;
    loop {
        let next_local = local.peek().copied();
        let next_covered = covered.peek().copied();
        let order = match (next_local, next_covered) {
            (None, None) => {
                return Alignment {
                    verdict: if pending.is_some() {
                        CutAlignment::Pending
                    } else {
                        CutAlignment::Exact
                    },
                    deciding: pending,
                };
            }
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (Some((writer, _, _)), Some(cut)) => writer.cmp(cut.writer.as_slice()),
        };
        let (difference, quiet) = match (order, next_local, next_covered) {
            (Ordering::Less, Some((writer, epoch, sequence)), _) => {
                local.next();
                if previous.is_some_and(|last| last >= writer) {
                    return UNSORTED;
                }
                previous = Some(writer);
                let difference = CutDifference {
                    writer,
                    live: Some((epoch, sequence)),
                    covered: None,
                };
                (difference, sequence == 0)
            }
            (Ordering::Equal, Some((writer, epoch, sequence)), Some(cut)) => {
                local.next();
                covered.next();
                if previous.is_some_and(|last| last >= writer) {
                    return UNSORTED;
                }
                previous = Some(writer);
                let difference = CutDifference {
                    writer,
                    live: Some((epoch, sequence)),
                    covered: Some((cut.epoch, cut.sequence)),
                };
                (difference, (epoch, sequence) == (cut.epoch, cut.sequence))
            }
            (Ordering::Greater, _, Some(cut)) => {
                covered.next();
                let difference = CutDifference {
                    writer: &cut.writer,
                    live: None,
                    covered: Some((cut.epoch, cut.sequence)),
                };
                (difference, cut.sequence == 0)
            }
            // `order` was derived from the same two peeks.
            _ => return UNSORTED,
        };
        if !quiet && pending.is_none() {
            pending = Some(difference);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CutAlignment, CutDifference, NativeCut, align_cuts};

    fn cut(writer: &str, epoch: u64, sequence: u64) -> NativeCut {
        NativeCut {
            writer: writer.as_bytes().to_vec(),
            epoch,
            sequence,
        }
    }

    fn local(cuts: &[NativeCut]) -> impl Iterator<Item = (&[u8], u64, u64)> {
        cuts.iter()
            .map(|cut| (cut.writer.as_slice(), cut.epoch, cut.sequence))
    }

    fn verdict(live: &[NativeCut], barrier: &[NativeCut]) -> CutAlignment {
        align_cuts(local(live), barrier).verdict
    }

    #[test]
    fn exact_positions_and_one_sided_quiet_writers_align() {
        let barrier = [cut("a", 1, 4), cut("c", 2, 0)];
        let follower = [cut("a", 1, 4), cut("b", 3, 0)];
        let exact = align_cuts(local(&follower), &barrier);
        assert_eq!(exact.verdict, CutAlignment::Exact);
        assert_eq!(exact.deciding, None);
        assert_eq!(verdict(&[], &[]), CutAlignment::Exact);
    }

    #[test]
    fn a_follower_behind_or_ahead_of_the_barrier_is_pending() {
        let barrier = [cut("a", 1, 4)];
        assert_eq!(verdict(&[cut("a", 1, 3)], &barrier), CutAlignment::Pending);
        assert_eq!(verdict(&[cut("a", 1, 5)], &barrier), CutAlignment::Pending);
        // A writer only one side has applied is still in flight to the other.
        assert_eq!(verdict(&[], &barrier), CutAlignment::Pending);
        assert_eq!(
            verdict(&[cut("a", 1, 4), cut("b", 1, 1)], &barrier),
            CutAlignment::Pending
        );
    }

    /// The production rejoin: the donor still covers the restarted writer's
    /// old life, through its fourth write, while the rejoiner stands at the
    /// start of the writer's new life. The donor covers the old life only up
    /// to its cut and none of the new one, so the pair pends until the donor
    /// crosses into the new life; the reverse pair pends until the follower
    /// does. A quiet new life does not make an old-life barrier exact.
    #[test]
    fn a_writer_in_different_lives_pends_in_both_directions() {
        let old_barrier = [cut("b", 7, 4)];
        let rejoiner = [cut("b", 9, 0)];
        let pending = align_cuts(local(&rejoiner), &old_barrier);
        assert_eq!(pending.verdict, CutAlignment::Pending);
        assert_eq!(
            pending.deciding,
            Some(CutDifference {
                writer: b"b",
                live: Some((9, 0)),
                covered: Some((7, 4)),
            })
        );
        assert_eq!(
            verdict(&[cut("b", 9, 2)], &old_barrier),
            CutAlignment::Pending
        );
        // The follower still in the old life, the donor already renewed.
        assert_eq!(
            verdict(&[cut("b", 7, 4)], &[cut("b", 9, 0)]),
            CutAlignment::Pending
        );
        assert_eq!(
            verdict(&[cut("b", 7, 9)], &[cut("b", 9, 0)]),
            CutAlignment::Pending
        );
        // Once both stand at one position of the new life, they align.
        assert_eq!(verdict(&rejoiner, &[cut("b", 9, 0)]), CutAlignment::Exact);
    }

    #[test]
    fn only_unsorted_input_conflicts_even_when_others_pend() {
        let barrier = [cut("a", 1, 4), cut("b", 1, 2)];
        assert_eq!(
            verdict(&[cut("b", 1, 2), cut("a", 1, 4)], &barrier),
            CutAlignment::Conflict
        );
        assert_eq!(
            verdict(&[cut("a", 1, 4)], &[cut("b", 1, 0), cut("a", 1, 4)]),
            CutAlignment::Conflict
        );
        assert_eq!(
            verdict(&[cut("a", 1, 3), cut("b", 1, 2), cut("a", 2, 0)], &barrier),
            CutAlignment::Conflict
        );
    }

    #[test]
    fn the_first_misaligned_writer_decides_and_is_named() {
        let barrier = [cut("a", 1, 4), cut("b", 1, 2), cut("c", 1, 7)];
        let behind = [cut("a", 1, 4), cut("b", 1, 1)];
        let pending = align_cuts(local(&behind), &barrier);
        assert_eq!(pending.verdict, CutAlignment::Pending);
        assert_eq!(
            pending.deciding,
            Some(CutDifference {
                writer: b"b",
                live: Some((1, 1)),
                covered: Some((1, 2)),
            })
        );
        let one_sided = [cut("a", 1, 4), cut("b", 1, 2)];
        assert_eq!(
            align_cuts(local(&one_sided), &barrier).deciding,
            Some(CutDifference {
                writer: b"c",
                live: None,
                covered: Some((1, 7)),
            })
        );
        let unsorted = [cut("b", 1, 2), cut("a", 1, 4)];
        let unsorted = align_cuts(local(&unsorted), &barrier);
        assert_eq!(unsorted.deciding, None);
    }
}
