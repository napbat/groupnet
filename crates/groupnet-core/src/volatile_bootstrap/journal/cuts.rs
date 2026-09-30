//! Follower-side comparison of live native writer positions with a barrier.

use std::cmp::Ordering;

use super::types::NativeCut;

/// How a follower's live native writer positions compare with one donor
/// barrier's covered cuts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CutAlignment {
    /// Every writer is at the same incarnation and position. A writer known
    /// to only one side aligns only as a quiet zero-position feed.
    Exact,
    /// Same writer incarnations at different positions, or a non-quiet
    /// writer known to one side only. Either side may still be applying
    /// the same feed, so a later barrier can align.
    Pending,
    /// A writer incarnation differs, or an input is not strictly sorted by
    /// writer. No later barrier of this capture can align.
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
    /// The first writer, in writer order, that made the verdict other than
    /// [`CutAlignment::Exact`]. `None` when exact, or when an input is not
    /// strictly sorted.
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
                if epoch != cut.epoch {
                    return Alignment {
                        verdict: CutAlignment::Conflict,
                        deciding: Some(difference),
                    };
                }
                (difference, sequence == cut.sequence)
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

    #[test]
    fn a_changed_incarnation_or_unsorted_input_conflicts_even_when_others_pend() {
        let barrier = [cut("a", 1, 4), cut("b", 1, 2)];
        assert_eq!(
            verdict(&[cut("a", 1, 3), cut("b", 2, 2)], &barrier),
            CutAlignment::Conflict
        );
        assert_eq!(
            verdict(&[cut("b", 1, 2), cut("a", 1, 4)], &barrier),
            CutAlignment::Conflict
        );
        assert_eq!(
            verdict(&[cut("a", 1, 4)], &[cut("b", 1, 0), cut("a", 1, 4)]),
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
        let changed = [cut("a", 1, 3), cut("c", 2, 7)];
        let conflict = align_cuts(local(&changed), &barrier);
        assert_eq!(conflict.verdict, CutAlignment::Conflict);
        assert_eq!(
            conflict.deciding,
            Some(CutDifference {
                writer: b"c",
                live: Some((2, 7)),
                covered: Some((1, 7)),
            })
        );
        let unsorted = [cut("b", 1, 2), cut("a", 1, 4)];
        let unsorted = align_cuts(local(&unsorted), &barrier);
        assert_eq!(unsorted.deciding, None);
    }
}
