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
    covered: &[NativeCut],
) -> CutAlignment {
    if covered
        .windows(2)
        .any(|pair| pair[0].writer >= pair[1].writer)
    {
        return CutAlignment::Conflict;
    }
    let mut local = local.into_iter().peekable();
    let mut covered = covered.iter().peekable();
    let mut previous: Option<&[u8]> = None;
    let mut alignment = CutAlignment::Exact;
    loop {
        let next_local = local.peek().copied();
        let next_covered = covered.peek().copied();
        let order = match (next_local, next_covered) {
            (None, None) => return alignment,
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (Some((writer, _, _)), Some(cut)) => writer.cmp(cut.writer.as_slice()),
        };
        let quiet = match (order, next_local, next_covered) {
            (Ordering::Less, Some((writer, _, sequence)), _) => {
                local.next();
                if previous.is_some_and(|last| last >= writer) {
                    return CutAlignment::Conflict;
                }
                previous = Some(writer);
                sequence == 0
            }
            (Ordering::Equal, Some((writer, epoch, sequence)), Some(cut)) => {
                local.next();
                covered.next();
                if previous.is_some_and(|last| last >= writer) || epoch != cut.epoch {
                    return CutAlignment::Conflict;
                }
                previous = Some(writer);
                sequence == cut.sequence
            }
            (Ordering::Greater, _, Some(cut)) => {
                covered.next();
                cut.sequence == 0
            }
            // `order` was derived from the same two peeks.
            _ => return CutAlignment::Conflict,
        };
        if !quiet {
            alignment = CutAlignment::Pending;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CutAlignment, NativeCut, align_cuts};

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

    #[test]
    fn exact_positions_and_one_sided_quiet_writers_align() {
        let barrier = [cut("a", 1, 4), cut("c", 2, 0)];
        let follower = [cut("a", 1, 4), cut("b", 3, 0)];
        assert_eq!(align_cuts(local(&follower), &barrier), CutAlignment::Exact);
        assert_eq!(align_cuts(local(&[]), &[]), CutAlignment::Exact);
    }

    #[test]
    fn a_follower_behind_or_ahead_of_the_barrier_is_pending() {
        let barrier = [cut("a", 1, 4)];
        assert_eq!(
            align_cuts(local(&[cut("a", 1, 3)]), &barrier),
            CutAlignment::Pending
        );
        assert_eq!(
            align_cuts(local(&[cut("a", 1, 5)]), &barrier),
            CutAlignment::Pending
        );
        // A writer only one side has applied is still in flight to the other.
        assert_eq!(align_cuts(local(&[]), &barrier), CutAlignment::Pending);
        assert_eq!(
            align_cuts(local(&[cut("a", 1, 4), cut("b", 1, 1)]), &barrier),
            CutAlignment::Pending
        );
    }

    #[test]
    fn a_changed_incarnation_or_unsorted_input_conflicts_even_when_others_pend() {
        let barrier = [cut("a", 1, 4), cut("b", 1, 2)];
        assert_eq!(
            align_cuts(local(&[cut("a", 1, 3), cut("b", 2, 2)]), &barrier),
            CutAlignment::Conflict
        );
        assert_eq!(
            align_cuts(local(&[cut("b", 1, 2), cut("a", 1, 4)]), &barrier),
            CutAlignment::Conflict
        );
        assert_eq!(
            align_cuts(local(&[cut("a", 1, 4)]), &[cut("b", 1, 0), cut("a", 1, 4)]),
            CutAlignment::Conflict
        );
    }
}
