//! The handoff's pure verdicts, and the order they must be taken in.
//!
//! Sans-IO and stateless, for the same reason
//! [`CommitCore`](crate::hosted::CommitCore) and
//! [`CompletenessCore`](crate::hosted::CompletenessCore) are: every answer is a
//! function of one snapshot of the arguments, so there is nothing worth carrying
//! between calls, and a core that remembers nothing cannot be fooled by a frame
//! it has already seen. The simulator can drive this directly; the stream driver
//! (slice C) is a shell around it.
//!
//! Three verdicts, taken at three points, and one table that fixes their order:
//!
//! | Point | Verdict | Refusing means |
//! |---|---|---|
//! | the donor's `Offer` arrives | [`HandoffCore::staleness`] | this donor's view is superseded |
//! | the same frame | [`HandoffCore::coverage`] | this donor is behind what we need |
//! | the donor's `Done` arrives | [`HandoffCore::done_consistent`] | what landed is not what was sent |
//!
//! # What the staleness check proves
//!
//! It proves **staleness**, never freshness — the module's honesty box says this
//! at length and it is worth repeating where the code lives. A `Stale` verdict
//! is a proof that this donor's state belongs to a view that has been
//! superseded. An `Ok` verdict is the *absence* of such a proof, which is a
//! strictly weaker thing: two nodes inside one stale partition agree perfectly
//! and neither can tell.
//!
//! ## The hostless donor, pinned
//!
//! A fence stamp is `(epoch, Option<host>)`, and the `None` is real: a donor
//! publishes its offer from whatever it has adopted, and a node can have adopted
//! epoch `e` while still believing the group hostless at it — a follower that
//! saw the epoch bump before the leadership entry naming its winner, which is
//! one gossip round wide and entirely ordinary.
//!
//! **A hostless-stamped donor at our own epoch is not provably stale, and this
//! core answers [`Staleness::Ok`].** The epoch *is* the fence; the host name is
//! a strictly weaker piece of information about the same hostship, and lagging
//! on the name is not lagging on the fence. Under the posture S5 already
//! presumes (an epoch is a unique name for one hostship), nothing the donor
//! applied at `e` can belong to a *later* hostship, because a later hostship
//! carries a higher epoch — which the epoch-major test catches on its own. The
//! dangerous direction is the reverse, accepting a donor that is genuinely
//! behind, and that requires a *lower* epoch. So refusing the hostless donor
//! would buy no safety and would cost exactly the availability this module
//! exists to restore: it would refuse the most likely donor in the seconds after
//! a migration, which is precisely when a handoff is wanted.
//!
//! The symmetric case is answered the same way and for the mirrored reason: a
//! donor that names a host at an epoch we believe hostless is the *better*
//! informed party, and there is nothing to refuse.
//!
//! ## Two named hosts at one epoch: refused, though it need not be
//!
//! The one same-epoch refusal is **two different named hosts at one epoch**.
//! That is not a lag, it is a contradiction — an epoch names one hostship — so
//! one of the two beliefs is already dead.
//!
//! It is worth being exact about what this core declines to do here, because the
//! engine is not stuck on this case. **The fencing order can resolve it**: it is
//! a total order over `(epoch, host)` pairs — epoch-major, and at equal epochs
//! the [`owner`](groupnet_core::placement::owner) of the group id among the two
//! hosts wins. That tiebreak reads nothing but the group id and the two host
//! ids, so it is view-independent: every node, on either side of a heal, picks
//! the same survivor without exchanging anything (`engine/election/mod.rs`). A
//! handoff could apply the very same rule and accept a donor whose host is the
//! one that will win.
//!
//! It deliberately does not, and the refusal is **stricter on purpose**. This
//! core is fail-closed and its refusals are transient: the requester is about to
//! adopt a whole image on the strength of one verdict, and "our two stamps
//! contradict each other" is as much a statement about *our* view being unhealed
//! as about the donor's. Waiting costs a retry — the engine's own merge closes
//! the disagreement, after which the two stamps agree and the same donor is
//! accepted on the next attempt — where adopting across it rests the whole check
//! on a tiebreak whose outcome the requester has not yet seen its own engine
//! apply. Taking the tiebreak here is an available **availability** win, not a
//! correctness requirement, and it is not taken.
//!
//! The case is narrow to begin with, and `Settle`-only. Two same-epoch hostships
//! exist because each side of a partition counted the members it could see and
//! derived the same `highest_seen + 1`. A `Quorum` epoch is granted by a majority
//! of a static roster and an `External` one is a compare-and-swap on a real
//! allocator; neither can mint the pair. So `Stale` is a slight abuse of the word
//! — the wrong belief may be *ours* — and still exactly the right action: do not
//! adopt state from a peer whose view of the fence contradicts ours.

use groupnet_core::NodeId;

use crate::hosted::Watermarks;
use crate::token::WriteToken;

/// Whether a donor's fence stamp proves its state superseded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Staleness {
    /// Nothing in the stamp proves this donor stale. **Not a proof of
    /// freshness** — see the module docs.
    Ok,
    /// Provably superseded: the donor is stamped below the requester's adopted
    /// epoch, or it names a different host at the same epoch.
    Stale,
}

/// Whether a donor's snapshot reaches everything the requester asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Coverage {
    /// Every needed watermark is met or exceeded.
    Ok,
    /// At least one is not. `missing` names, per writer, the watermark that was
    /// **needed** (not what the donor has) in writer-id order — the same shape
    /// [`Completeness::Recovering`](crate::hosted::Completeness::Recovering)
    /// uses, so the two read alike in a log.
    Short {
        /// Per writer, the needed watermark this donor does not reach.
        missing: Vec<(NodeId, WriteToken)>,
    },
}

/// What a transfer moved: the chunk and byte counts, as counted by one side.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct DoneCounts {
    /// Chunks.
    pub chunks: u64,
    /// Payload bytes across those chunks — the framing's own overhead excluded,
    /// so both sides count the same number without agreeing on framing.
    pub bytes: u64,
}

/// Whether the requester staged exactly what the donor says it sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DoneCheck {
    /// The counts agree exactly.
    Ok,
    /// They do not. Named for the common cause; the rule is equality, so a
    /// *surplus* — a re-framing, a duplicated chunk — fails here too, and
    /// should.
    Truncated,
}

/// Where a handoff has got to. The success path, and nothing else.
///
/// Refusals, stale donors, short coverage and I/O errors are **exits**, not
/// transitions: they end the handoff with a [`HandoffError`](super::HandoffError)
/// and drop the sink from whatever phase they were reached in. Keeping them out
/// of the table is what makes it a table — six phases, six steps, and one legal
/// answer per cell.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HandoffPhase {
    /// Nothing sent yet. The starting phase.
    Request,
    /// The request is out; the donor's answer has not been verified.
    OfferVerify,
    /// The offer passed both checks; chunks are being staged.
    Stage,
    /// A `Done` frame arrived; its counts have not been checked.
    DoneVerify,
    /// The counts agreed; the sink has not been finished.
    Finish,
    /// Finished and installed. Terminal.
    Complete,
}

/// The events that move a handoff forward, one per legal transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HandoffStep {
    /// The `Request` frame was written.
    Requested,
    /// An `Offer` arrived and passed staleness **and** coverage. There is no
    /// step for an offer that merely arrived: an unverified offer never moves
    /// the phase, which is what stops a chunk from being staged behind it.
    OfferAccepted,
    /// One `Chunk` arrived and was applied to the sink.
    Chunk,
    /// A `Done` frame arrived.
    DoneSeen,
    /// [`HandoffCore::done_consistent`] answered [`DoneCheck::Ok`].
    CountsOk,
    /// [`SnapshotSink::finish`](super::SnapshotSink::finish) returned.
    Finished,
}

impl HandoffPhase {
    /// The phase after `step`, or `None` if that step is not legal here.
    ///
    /// `None` is a protocol violation, not a retry: the driver turns it into
    /// [`HandoffError::Protocol`](super::HandoffError::Protocol) and drops the
    /// sink. The cells that matter are the refusals — a chunk before a verified
    /// offer, a second offer mid-stream, a chunk after `Done`, anything at all
    /// after [`Complete`](Self::Complete).
    #[must_use]
    #[expect(
        clippy::match_same_arms,
        reason = "one arm per cell of the ordered table — (OfferVerify, OfferAccepted) and the \
                  (Stage, Chunk) self-loop both land on Stage for unrelated reasons, and merging \
                  them would hide a row"
    )]
    pub fn advance(self, step: HandoffStep) -> Option<Self> {
        match (self, step) {
            (HandoffPhase::Request, HandoffStep::Requested) => Some(HandoffPhase::OfferVerify),
            (HandoffPhase::OfferVerify, HandoffStep::OfferAccepted) => Some(HandoffPhase::Stage),
            // The one self-loop: a snapshot is many chunks.
            (HandoffPhase::Stage, HandoffStep::Chunk) => Some(HandoffPhase::Stage),
            (HandoffPhase::Stage, HandoffStep::DoneSeen) => Some(HandoffPhase::DoneVerify),
            (HandoffPhase::DoneVerify, HandoffStep::CountsOk) => Some(HandoffPhase::Finish),
            (HandoffPhase::Finish, HandoffStep::Finished) => Some(HandoffPhase::Complete),
            _ => None,
        }
    }

    /// Whether the handoff is over and installed.
    #[must_use]
    pub fn is_complete(self) -> bool {
        matches!(self, HandoffPhase::Complete)
    }
}

/// The handoff's verdicts. A unit type for the same reason
/// [`CompletenessCore`](crate::hosted::CompletenessCore) is one: there is no
/// state, and grouping the rules under a name is what makes them findable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HandoffCore;

impl HandoffCore {
    /// Whether `donor_fence` is provably superseded by `adopted`.
    ///
    /// Epoch-major, exactly like [`Fence`](crate::hosted::Fence) and
    /// [`WriteToken`]: the epoch is the ordering and the host is only the name.
    ///
    /// * donor below us ⇒ [`Stale`](Staleness::Stale). Its state predates a
    ///   migration we have already adopted.
    /// * donor above us ⇒ [`Ok`](Staleness::Ok). *We* are the behind party and
    ///   will learn it; its state is, if anything, newer than ours.
    /// * equal epochs, two different **named** hosts ⇒
    ///   [`Stale`](Staleness::Stale). A contradiction, refused fail-closed.
    /// * equal epochs, either side hostless ⇒ [`Ok`](Staleness::Ok). The pinned
    ///   decision; the module docs carry the argument.
    #[must_use]
    pub fn staleness(
        donor_fence: (u64, Option<&NodeId>),
        adopted: (u64, Option<&NodeId>),
    ) -> Staleness {
        let (donor_epoch, donor_host) = donor_fence;
        let (adopted_epoch, adopted_host) = adopted;
        if donor_epoch < adopted_epoch {
            return Staleness::Stale;
        }
        if donor_epoch > adopted_epoch {
            return Staleness::Ok;
        }
        match (donor_host, adopted_host) {
            // One epoch cannot name two hostships. One of the two views is
            // wrong and neither node can tell which, so nothing is adopted.
            (Some(donor), Some(ours)) if donor != ours => Staleness::Stale,
            _ => Staleness::Ok,
        }
    }

    /// Whether `covers` reaches every watermark in `need`.
    ///
    /// A per-writer floor: for each `(writer, target)` the donor must hold that
    /// writer at or above `target`. A writer the donor does not name at all is
    /// short — absence is "this donor has applied nothing of that feed", not
    /// "nothing was needed", which is exactly the reading
    /// [`CompletenessCore::step`](crate::hosted::CompletenessCore::step) gives
    /// absence, and deliberately so: a target that rule computed must not become
    /// covered by passing through this one. Writers the donor covers and `need`
    /// does not ask for are free; a snapshot is allowed to bring more than was
    /// requested, and usually does.
    ///
    /// An empty `need` is [`Ok`](Coverage::Ok): asking for nothing is answered
    /// by anything, which is the honest reading of a requester that has no
    /// target yet.
    #[must_use]
    pub fn coverage(need: &Watermarks, covers: &Watermarks) -> Coverage {
        let missing: Vec<(NodeId, WriteToken)> = need
            .iter()
            .filter(|(writer, target)| covers.get(*writer).is_none_or(|have| have < *target))
            .map(|(writer, target)| (writer.clone(), *target))
            .collect();
        if missing.is_empty() {
            Coverage::Ok
        } else {
            Coverage::Short { missing }
        }
    }

    /// Whether the requester staged exactly what the donor claims to have sent.
    ///
    /// Equality on both counters, and nothing weaker. A short count is the
    /// truncation the name describes; a long one is a re-framing or a duplicated
    /// chunk, and adopting a snapshot whose byte count nobody agrees on is the
    /// same mistake in a different direction. Both answer
    /// [`Truncated`](DoneCheck::Truncated), and both drop the sink.
    #[must_use]
    pub fn done_consistent(staged: DoneCounts, claimed: DoneCounts) -> DoneCheck {
        if staged == claimed {
            DoneCheck::Ok
        } else {
            DoneCheck::Truncated
        }
    }
}

#[cfg(test)]
mod tests {
    use groupnet_core::NodeId;

    use super::{
        Coverage, DoneCheck, DoneCounts, HandoffCore, HandoffPhase, HandoffStep, Staleness,
    };
    use crate::hosted::Watermarks;
    use crate::token::WriteToken;

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

    fn short(pairs: &[(&str, u64, u64)]) -> Coverage {
        Coverage::Short {
            missing: pairs
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

    /// `staleness` with owned ids, so a table row can be written flat.
    fn stale(donor: (u64, Option<&str>), ours: (u64, Option<&str>)) -> Staleness {
        let donor_host = donor.1.map(node);
        let our_host = ours.1.map(node);
        HandoffCore::staleness((donor.0, donor_host.as_ref()), (ours.0, our_host.as_ref()))
    }

    #[test]
    fn the_epoch_is_the_ordering_and_the_host_is_only_the_name() {
        // A donor below our adopted epoch is superseded, whoever it names.
        assert_eq!(stale((4, Some("a")), (6, Some("a"))), Staleness::Stale);
        assert_eq!(stale((4, Some("a")), (6, Some("b"))), Staleness::Stale);
        assert_eq!(stale((4, None), (6, None)), Staleness::Stale);
        assert_eq!(stale((5, Some("a")), (6, None)), Staleness::Stale);
        // A donor above it is not: we are the behind party, and we will learn.
        assert_eq!(stale((7, Some("a")), (6, Some("b"))), Staleness::Ok);
        assert_eq!(stale((7, None), (6, Some("b"))), Staleness::Ok);
        assert_eq!(stale((u64::MAX, None), (0, None)), Staleness::Ok);
    }

    #[test]
    fn one_epoch_naming_two_hosts_is_the_only_same_epoch_refusal() {
        assert_eq!(stale((6, Some("a")), (6, Some("b"))), Staleness::Stale);
        // The same host at the same epoch is the ordinary agreeing case.
        assert_eq!(stale((6, Some("a")), (6, Some("a"))), Staleness::Ok);
    }

    /// The pinned decision, asserted in both directions so a future change to it
    /// has to change a test that says why.
    #[test]
    fn a_hostless_stamp_at_our_epoch_is_not_provably_stale() {
        // The donor has adopted our epoch but not yet learned who won it — one
        // gossip round wide, and the likeliest donor right after a migration.
        // The epoch is the fence; the name is weaker information about the same
        // hostship, and lagging on the name is not lagging on the fence.
        assert_eq!(stale((6, None), (6, Some("a"))), Staleness::Ok);
        // Mirrored: a donor that names a host at an epoch we believe hostless
        // is the better-informed party. Nothing to refuse.
        assert_eq!(stale((6, Some("a")), (6, None)), Staleness::Ok);
        // Both ignorant of the name, agreeing on the fence.
        assert_eq!(stale((6, None), (6, None)), Staleness::Ok);
        // And the check that carries the safety is untouched by any of it: a
        // hostless donor one epoch back is still refused.
        assert_eq!(stale((5, None), (6, None)), Staleness::Stale);
        assert_eq!(stale((5, None), (6, Some("a"))), Staleness::Stale);
    }

    #[test]
    fn coverage_is_a_per_writer_floor() {
        let covers = marks(&[("h1", 5, 9), ("h2", 2, 4)]);
        // Nothing asked, anything answers.
        assert_eq!(
            HandoffCore::coverage(&Watermarks::new(), &covers),
            Coverage::Ok
        );
        // At the floor, and past it.
        assert_eq!(
            HandoffCore::coverage(&marks(&[("h1", 5, 9)]), &covers),
            Coverage::Ok
        );
        assert_eq!(
            HandoffCore::coverage(&marks(&[("h1", 5, 8), ("h2", 2, 1)]), &covers),
            Coverage::Ok
        );
        // Epoch-major: a newer life covers an older target.
        assert_eq!(
            HandoffCore::coverage(&marks(&[("h1", 4, 9_999)]), &covers),
            Coverage::Ok
        );
        // One sequence short is short, and only that writer is named.
        assert_eq!(
            HandoffCore::coverage(&marks(&[("h1", 5, 10), ("h2", 2, 4)]), &covers),
            short(&[("h1", 5, 10)])
        );
        // …and `missing` carries what was *needed*, not what the donor has.
        assert_eq!(
            HandoffCore::coverage(&marks(&[("h1", 6, 1)]), &covers),
            short(&[("h1", 6, 1)])
        );
    }

    #[test]
    fn a_writer_the_donor_never_names_is_short() {
        let covers = marks(&[("h1", 5, 9)]);
        // Absence is "applied nothing of that feed", not "nothing to apply".
        assert_eq!(
            HandoffCore::coverage(&marks(&[("h2", 1, 1)]), &covers),
            short(&[("h2", 1, 1)])
        );
        // Even a bottom target is short against an absent writer — the same
        // answer `CompletenessCore::step` gives, and deliberately the same, so
        // a target that rule computed cannot be "covered" by a gap here.
        assert_eq!(
            HandoffCore::coverage(&marks(&[("h2", 0, 0)]), &covers),
            short(&[("h2", 0, 0)])
        );
        // Multiple shortfalls come back in writer-id order.
        assert_eq!(
            HandoffCore::coverage(&marks(&[("h3", 1, 1), ("h2", 1, 1)]), &covers),
            short(&[("h2", 1, 1), ("h3", 1, 1)])
        );
    }

    #[test]
    fn a_donor_bringing_more_than_was_asked_for_still_covers() {
        let covers = marks(&[("h1", 5, 9), ("h2", 2, 4), ("h3", 9, 9)]);
        assert_eq!(
            HandoffCore::coverage(&marks(&[("h1", 5, 1)]), &covers),
            Coverage::Ok,
            "a snapshot is allowed to be wider than the request"
        );
    }

    #[test]
    fn the_done_counts_must_agree_exactly() {
        let sent = DoneCounts {
            chunks: 12,
            bytes: 4096,
        };
        assert_eq!(HandoffCore::done_consistent(sent, sent), DoneCheck::Ok);
        assert_eq!(
            HandoffCore::done_consistent(DoneCounts::default(), DoneCounts::default()),
            DoneCheck::Ok,
            "an empty snapshot is a legitimate snapshot"
        );
        // Short in either counter.
        for staged in [
            DoneCounts {
                chunks: 11,
                bytes: 4096,
            },
            DoneCounts {
                chunks: 12,
                bytes: 4095,
            },
        ] {
            assert_eq!(
                HandoffCore::done_consistent(staged, sent),
                DoneCheck::Truncated
            );
        }
        // Long in either counter: a re-framing or a duplicate is refused by the
        // same rule, because the rule is equality and not "at least".
        for staged in [
            DoneCounts {
                chunks: 13,
                bytes: 4096,
            },
            DoneCounts {
                chunks: 12,
                bytes: 4097,
            },
        ] {
            assert_eq!(
                HandoffCore::done_consistent(staged, sent),
                DoneCheck::Truncated,
                "equality, not a floor"
            );
        }
    }

    const PHASES: [HandoffPhase; 6] = [
        HandoffPhase::Request,
        HandoffPhase::OfferVerify,
        HandoffPhase::Stage,
        HandoffPhase::DoneVerify,
        HandoffPhase::Finish,
        HandoffPhase::Complete,
    ];

    const STEPS: [HandoffStep; 6] = [
        HandoffStep::Requested,
        HandoffStep::OfferAccepted,
        HandoffStep::Chunk,
        HandoffStep::DoneSeen,
        HandoffStep::CountsOk,
        HandoffStep::Finished,
    ];

    /// Every legal cell of the 6×6 table, written out. Anything not here is
    /// `None`, and the assertion below proves the two sets partition the table.
    const LEGAL: [(HandoffPhase, HandoffStep, HandoffPhase); 6] = [
        (
            HandoffPhase::Request,
            HandoffStep::Requested,
            HandoffPhase::OfferVerify,
        ),
        (
            HandoffPhase::OfferVerify,
            HandoffStep::OfferAccepted,
            HandoffPhase::Stage,
        ),
        (HandoffPhase::Stage, HandoffStep::Chunk, HandoffPhase::Stage),
        (
            HandoffPhase::Stage,
            HandoffStep::DoneSeen,
            HandoffPhase::DoneVerify,
        ),
        (
            HandoffPhase::DoneVerify,
            HandoffStep::CountsOk,
            HandoffPhase::Finish,
        ),
        (
            HandoffPhase::Finish,
            HandoffStep::Finished,
            HandoffPhase::Complete,
        ),
    ];

    #[test]
    fn the_phase_table_is_exhaustive_and_admits_exactly_six_cells() {
        for phase in PHASES {
            for step in STEPS {
                let expected = LEGAL
                    .iter()
                    .find(|(from, on, _)| *from == phase && *on == step)
                    .map(|(_, _, to)| *to);
                assert_eq!(phase.advance(step), expected, "cell ({phase:?}, {step:?})");
            }
        }
    }

    #[test]
    fn the_ordered_path_is_the_only_way_to_complete() {
        let mut phase = HandoffPhase::Request;
        for step in [
            HandoffStep::Requested,
            HandoffStep::OfferAccepted,
            HandoffStep::Chunk,
            HandoffStep::Chunk,
            HandoffStep::Chunk,
            HandoffStep::DoneSeen,
            HandoffStep::CountsOk,
            HandoffStep::Finished,
        ] {
            phase = phase.advance(step).expect("the ordered path");
        }
        assert_eq!(phase, HandoffPhase::Complete);
        assert!(phase.is_complete());
        // …and it is terminal: nothing at all follows an install.
        for step in STEPS {
            assert_eq!(HandoffPhase::Complete.advance(step), None, "{step:?}");
        }
    }

    /// The cells whose refusal is the point, each with the rule it enforces.
    #[test]
    fn the_load_bearing_refusals() {
        // No byte is staged behind an unverified offer — there is no step for
        // "an offer arrived", only for one that passed both checks.
        assert_eq!(
            HandoffPhase::OfferVerify.advance(HandoffStep::Chunk),
            None,
            "a chunk before a verified offer"
        );
        assert_eq!(
            HandoffPhase::OfferVerify.advance(HandoffStep::DoneSeen),
            None,
            "a Done before a verified offer"
        );
        // One offer per handoff: a second one mid-stream would re-open a check
        // that bytes have already been staged behind.
        assert_eq!(
            HandoffPhase::Stage.advance(HandoffStep::OfferAccepted),
            None,
            "a second offer mid-stream"
        );
        // Done closes the stream: nothing arrives behind it.
        assert_eq!(
            HandoffPhase::DoneVerify.advance(HandoffStep::Chunk),
            None,
            "a chunk after Done"
        );
        // The counts are checked before the sink is finished, never after.
        assert_eq!(
            HandoffPhase::DoneVerify.advance(HandoffStep::Finished),
            None,
            "finishing without checking the counts"
        );
        assert_eq!(
            HandoffPhase::Stage.advance(HandoffStep::CountsOk),
            None,
            "counting before a Done"
        );
        // And nothing is staged before a request goes out.
        for step in STEPS {
            if step == HandoffStep::Requested {
                continue;
            }
            assert_eq!(HandoffPhase::Request.advance(step), None, "{step:?}");
        }
    }
}
