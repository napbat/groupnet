//! Snapshot handoff (feature `handoff`): the covering state transfer M4's
//! recovery deliberately left out.
//!
//! # What problem this solves
//!
//! [`CompletenessCore`](super::CompletenessCore) tells a freshly-activated host
//! *what* it must have applied before it may serve. It does not tell it how to
//! get there, and the machinery underneath — the gossiped
//! [`WriteFeed`](crate::WriteFeed) ring — has a hard bound. A target past the
//! end of a writer's visible window is reached by [`Gap`](crate::PeerWrite::Gap)
//! plus the consumer's own coarse remediation, never by replay.
//!
//! For a consumer whose remediation is "rebuild from my own store", that is the
//! whole story and this module is unnecessary — the store is the state transfer.
//! For one whose state *is* the groupnet-carried state, a ring overrun leaves it
//! with nothing to rebuild from and no honest option but to keep refusing
//! service. This module is the way out: pull a **covering snapshot** from a peer
//! that already holds one, over the data plane
//! ([`BulkTransport`](groupnet_transport::bulk::BulkTransport)), and verify what
//! arrives before adopting a byte of it.
//!
//! # The shape
//!
//! One request/response exchange on one bulk stream, five frame kinds, and three
//! verdicts:
//!
//! ```text
//! requester                                  donor
//!   │── Request { group, name, need } ─────────▶│
//!   │◀────────── Offer { fence, covers } ───────│   or Refuse { code, have }
//!   │  (verify: staleness, coverage)            │
//!   │◀────────── Chunk × n ─────────────────────│
//!   │◀────────── Done { chunks, bytes, fence } ─│
//!   │  (verify: counts)                         │
//!   │  sink.finish()                            │
//! ```
//!
//! * The consumer supplies both ends of the *data*: a [`SnapshotSource`] on the
//!   donor and a [`SnapshotSink`] on the requester. This module never
//!   interprets a chunk.
//! * [`HandoffCore`] holds the three pure verdicts, and
//!   [`HandoffPhase`] the order they must be taken in. Both are sans-IO and
//!   unit-tested without a runtime, exactly like the two M4 cores.
//! * [`Handoff`] is the shell that drives them over a stream — both halves of
//!   the exchange, plus [`donors`](Handoff::donors) to choose whom to ask and
//!   [`seed`](Handoff::seed) to fold a completed transfer into this node's own
//!   evidence. It decides nothing the core has not already decided; what it adds
//!   is *order* — a linear exchange, every forward move of which is checked
//!   against the phase table as it runs.
//! * A successful transfer answers with a [`HandoffReceipt`]: the donor's fence
//!   stamp and the watermarks the snapshot covers, which is what the requester
//!   feeds into its own [`CommitLedger`](super::CommitLedger) before re-asking
//!   the recovery rule.
//!
//! # Honesty box: what this guarantees, and where it stops
//!
//! **Snapshot atomicity is the [`SnapshotSource`]'s contract, and this module
//! cannot check it.** A [`Snapshot`] claims that its `chunks`, applied in order
//! to a [`SnapshotSink`], produce state at or above the watermarks in its
//! `covers`. Nothing here verifies that: the chunks are opaque bytes, and a
//! source that reads its state while its own apply loop mutates it will produce
//! a torn image with an honest-looking `covers` map. Take the `covers` reading
//! **at or before** the instant the image is fixed — a copy-on-write handle, a
//! store snapshot, a lock held across the read of both — and take it *after*
//! nothing. Over-claiming `covers` is the one failure this design cannot detect
//! and cannot survive, because the whole point of the receipt is that the
//! requester then tells the group it has that state.
//!
//! **Verification proves staleness, never freshness.** The three-point check —
//! [`HandoffCore::staleness`] on the donor's fence,
//! [`HandoffCore::coverage`] on its `covers` against the requester's `need`, and
//! [`HandoffCore::done_consistent`] on the transfer counts — refuses donors that
//! can be *shown* wrong. It cannot show one right. A donor stamped at the
//! requester's own epoch, whose `covers` clears the requester's `need`, and
//! whose byte counts tally, may still be handing over a view both nodes are
//! equally ignorant of being behind: two nodes inside the same stale partition
//! agree perfectly. The check narrows that window; it does not close it, and no
//! check made of the two participants' own beliefs can.
//!
//! The residue is not new, and it is not unbounded. It is exactly M4's
//! drain-window divergence: state applied under a view that no surviving host
//! will hold. Nothing acknowledged is at risk — the view-stamp fence means such
//! a write was never committed — and the reconciliation is the standing one, the
//! authoritative [`Gap`](super::HostedRead::Gap) that opens the next lineage and
//! the consumer's own remediation behind it. A handoff can move the divergence
//! faster than gossip would have; it cannot make it permanent, and a consumer
//! that treats the `Gap` as advisory keeps that divergence with or without this
//! module.
//!
//! **A follower donor can donate a doomed tail.** A donor need not be the host.
//! That is deliberate — the host is often the busiest node in the group and
//! often the very node that is recovering — but a follower's applied state can
//! include the previous host's un-replicated tail, which is precisely the state
//! no surviving host will hold. So `donors()` orders the **serving host first**
//! and followers behind it, and a requester takes the first donor that answers.
//! The host's state is the one state that is definitionally survivable; a
//! follower's is a best-effort second choice, taken when the host cannot serve,
//! and it inherits the drain-window paragraph above rather than escaping it.
//!
//! **A ring that turns faster than the transfer is a livelock, and the fix is
//! sizing.** The requester's `need` is computed from a recovery target that is
//! itself moving: while a snapshot streams, the group keeps writing. If the
//! writers advance further than the snapshot covers before it lands, the
//! requester finishes, re-asks the recovery rule, and is short again — and if
//! that is *durably* true the handoff repeats forever, transferring state and
//! never catching up. There is no clever fix here and this module does not
//! pretend to one: the transfer must be able to outrun the write rate, which
//! makes it a capacity decision (snapshot size, link bandwidth, ring depth)
//! exactly like the ring-sizing decision it replaces. A caller that wants the
//! failure to be loud bounds its own retries and surfaces the stall.
//!
//! **Chunks may overlap what the sink already has.** A snapshot is a covering
//! image, not a delta: re-applying state the requester had already applied is
//! normal, and on a retried handoff it is the common case. That is safe under
//! this crate's standing contract — **applying a write notification must be
//! idempotent** — and this module adds no exception to it. A sink that is not
//! idempotent is broken for the session tier already.
//!
//! **A sink that is dropped without [`finish`](SnapshotSink::finish) discards.**
//! Every failure path here drops the sink: a refusal, a stale donor, a short
//! `covers`, a count mismatch, an I/O error mid-stream. The consumer's contract
//! is that a dropped, unfinished sink leaves the node's state exactly as it was
//! — staged bytes thrown away, nothing half-adopted. Stage to a side location
//! and swap on `finish`; do not stream into live state.

mod core;
mod stream;
mod wire;

use std::fmt;
use std::future::Future;
use std::io;

use bytes::Bytes;
use groupnet_core::NodeId;

pub use self::core::{
    Coverage, DoneCheck, DoneCounts, HandoffCore, HandoffPhase, HandoffStep, Staleness,
};
pub use self::stream::{Handoff, Offered, is_request};
use super::Watermarks;

/// A consumer's snapshot producer: the donor half of the data contract.
///
/// One instance per write path, held by whatever serves handoff requests. Each
/// [`open`](Self::open) must yield an image that is **internally consistent** and
/// a `covers` map that the image is at or above — see the module's honesty box,
/// which is where the whole weight of this trait sits.
pub trait SnapshotSource: Send + Sync + 'static {
    /// The chunk stream one [`open`](Self::open) produces.
    type Chunks: SnapshotChunks;

    /// Opens a snapshot: the watermarks it covers, and the chunks that carry it.
    ///
    /// Called once per served request. An `Err` is answered to the requester as
    /// [`RefusalCode::Unavailable`] — a donor that cannot open is not a donor,
    /// and the requester tries the next one.
    fn open(&self) -> impl Future<Output = io::Result<Snapshot<Self::Chunks>>> + Send;
}

/// The chunk half of a [`Snapshot`]: opaque bytes, in order, until `None`.
///
/// Chunk boundaries are the source's business and carry no meaning on the wire —
/// the sink sees exactly the chunks the source produced, in exactly that order.
/// Size them for the data plane's framing rather than for the consumer's
/// records, and note that the framing puts a hard ceiling on that: one chunk
/// becomes one frame, and
/// [`DataStream`](groupnet_transport::bulk::DataStream) refuses to *read* a
/// frame past its **256 MiB** `MAX_FRAME`, so a chunk must stay under that less
/// the six-byte `GNHO` prefix it is written behind.
///
/// Nothing checks that on the way out. An oversized chunk is sent perfectly
/// happily and then kills the transfer at the **receiver**, as
/// [`HandoffError::Io`] wrapping an
/// [`InvalidData`](io::ErrorKind::InvalidData) — with the sink dropped
/// unfinished, so nothing is installed and the whole image has to cross again.
/// The cap is a guard against a source that streams its state as one enormous
/// chunk, not a size to aim at.
pub trait SnapshotChunks: Send + 'static {
    /// The next chunk, or `None` at the end of the snapshot.
    ///
    /// An `Err` mid-stream aborts the transfer; the requester sees the stream
    /// end without a `Done` frame and drops its sink unfinished.
    fn next(&mut self) -> impl Future<Output = io::Result<Option<Bytes>>> + Send;
}

/// One opened snapshot: what it covers, and the bytes that carry it.
///
/// `covers` is a claim about the *image*, not about the donor's current state:
/// a donor that keeps applying while the snapshot streams is still offering the
/// watermarks it read when it fixed the image, and that is the honest number.
pub struct Snapshot<C> {
    /// The applied watermarks the image is at or above, per writer. This is
    /// what [`HandoffCore::coverage`] is checked against.
    pub covers: Watermarks,
    /// The image itself.
    pub chunks: C,
}

impl<C> fmt::Debug for Snapshot<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Snapshot")
            .field("covers", &self.covers)
            .finish_non_exhaustive()
    }
}

/// A consumer's snapshot installer: the requester half of the data contract.
///
/// Chunks arrive in the source's order and are applied one at a time; the
/// transfer ends at exactly one [`finish`](Self::finish), or at a drop.
///
/// **Drop-without-`finish` must discard.** Every verification failure and every
/// I/O error on this path drops the sink, and the node's state must be
/// unchanged afterwards. Stage into a scratch location and make `finish` the
/// swap.
pub trait SnapshotSink: Send + 'static {
    /// Stages one chunk.
    ///
    /// An `Err` aborts the transfer and the sink is dropped — which discards.
    fn apply(&mut self, chunk: Bytes) -> impl Future<Output = io::Result<()>> + Send;

    /// Adopts everything staged. Called once, only after every verification has
    /// passed.
    ///
    /// An `Err` here is a consumer-side install failure: the handoff reports it
    /// as [`HandoffError::Io`] and the state is whatever the consumer's own
    /// swap left behind, which is why the swap should be the atomic part.
    fn finish(self) -> impl Future<Output = io::Result<()>> + Send;
}

/// What a completed handoff returns.
///
/// The receipt is the requester's evidence, and it is what feeds the next step:
/// seed a [`CommitLedger`](super::CommitLedger) from `covers`, refresh it, and
/// re-ask [`CompletenessCore::step`](super::CompletenessCore::step). The fence
/// stamp is kept so an operator can see *whose* view was adopted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandoffReceipt {
    /// The leadership epoch the donor had adopted when it opened the snapshot.
    pub fence_epoch: u64,
    /// The host the donor believed held that epoch, or `None` when it believed
    /// the group hostless. See [`HandoffCore::staleness`] for what a hostless
    /// stamp does and does not prove.
    pub fence_host: Option<NodeId>,
    /// The watermarks the installed snapshot covers.
    pub covers: Watermarks,
}

/// Why a donor refused, as one byte on the wire.
///
/// A refusal is a *donor's* verdict about itself, and the requester treats every
/// variant the same way: drop the sink, try the next donor. The codes exist so
/// an operator can tell "nobody has the state yet" from "somebody is speaking
/// the wrong protocol".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RefusalCode {
    /// The donor's own state does not cover what the requester asked for. The
    /// refusal carries the donor's watermarks, so the requester learns how far
    /// behind this donor is rather than merely that it is.
    NotCovered,
    /// The donor cannot serve a snapshot right now: no
    /// [`SnapshotSource`] bound, [`open`](SnapshotSource::open) failed, or a
    /// concurrency limit is full. Transient by construction — retry, or ask
    /// someone else.
    Unavailable,
    /// The request did not name a group or write path this donor serves.
    BadRequest,
    /// The requester's protocol version is not one this donor speaks.
    Version,
}

impl RefusalCode {
    /// The byte this code occupies in a `Refuse` frame.
    #[must_use]
    pub const fn code(self) -> u8 {
        match self {
            RefusalCode::NotCovered => 1,
            RefusalCode::Unavailable => 2,
            RefusalCode::BadRequest => 3,
            RefusalCode::Version => 4,
        }
    }

    /// The code that byte names, or `None` for a byte this version does not
    /// define — which the codec refuses loudly rather than folding into a
    /// plausible neighbour.
    #[must_use]
    pub const fn from_code(byte: u8) -> Option<Self> {
        match byte {
            1 => Some(RefusalCode::NotCovered),
            2 => Some(RefusalCode::Unavailable),
            3 => Some(RefusalCode::BadRequest),
            4 => Some(RefusalCode::Version),
            _ => None,
        }
    }
}

impl fmt::Display for RefusalCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            RefusalCode::NotCovered => "donor does not cover the request",
            RefusalCode::Unavailable => "donor cannot serve a snapshot now",
            RefusalCode::BadRequest => "donor does not serve that group or write path",
            RefusalCode::Version => "donor does not speak that protocol version",
        })
    }
}

/// How a handoff failed.
///
/// Every variant is an availability failure with a name attached — the same
/// posture the rest of this tier keeps. **None of them installs anything**: the
/// sink is dropped unfinished on every one, so a failed handoff leaves the
/// requester exactly as it was, still refusing service, still able to try
/// another donor.
#[derive(Debug)]
pub enum HandoffError {
    /// The donor's fence stamp is provably behind the requester's adopted one,
    /// so its state belongs to a view that has already been superseded.
    StaleDonor {
        /// The epoch the donor stamped its offer with.
        donor_epoch: u64,
        /// The host the donor believed held that epoch.
        donor_host: Option<NodeId>,
        /// The epoch the requester has adopted.
        adopted_epoch: u64,
        /// The host the requester believes holds it.
        adopted_host: Option<NodeId>,
    },
    /// The donor's offer does not cover what was asked for. `have` is the
    /// donor's own map, so the caller can pick a better donor rather than
    /// guessing.
    NotCovered {
        /// What the donor does cover.
        have: Watermarks,
    },
    /// The donor refused before offering anything.
    Refused {
        /// Why.
        code: RefusalCode,
    },
    /// The stream ended, or the counts did not tally, before a whole snapshot
    /// had arrived. Nothing staged is adopted.
    Truncated,
    /// The peer is not speaking this protocol: a foreign magic, a version or
    /// frame kind this build does not define, or a frame arriving out of the
    /// order [`HandoffPhase`] fixes. Loud on purpose — the alternative is
    /// waiting forever on a stream that will never say `Done`.
    Protocol(&'static str),
    /// The transport, the source, or the sink failed.
    Io(io::Error),
}

impl fmt::Display for HandoffError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HandoffError::StaleDonor {
                donor_epoch,
                adopted_epoch,
                ..
            } => write!(
                f,
                "donor's fence (epoch {donor_epoch}) is behind the adopted one (epoch \
                 {adopted_epoch})"
            ),
            HandoffError::NotCovered { have } => {
                write!(f, "donor covers only {} writers", have.len())
            }
            HandoffError::Refused { code } => write!(f, "handoff refused: {code}"),
            HandoffError::Truncated => f.write_str("snapshot ended before it was whole"),
            HandoffError::Protocol(what) => write!(f, "handoff protocol error: {what}"),
            HandoffError::Io(err) => write!(f, "handoff i/o error: {err}"),
        }
    }
}

impl std::error::Error for HandoffError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            HandoffError::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for HandoffError {
    fn from(err: io::Error) -> Self {
        HandoffError::Io(err)
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use groupnet_core::NodeId;

    use super::{HandoffError, HandoffReceipt, RefusalCode, Snapshot, Watermarks};
    use crate::token::WriteToken;

    fn marks(pairs: &[(&str, u64, u64)]) -> Watermarks {
        pairs
            .iter()
            .map(|(writer, epoch, seq)| {
                (
                    NodeId::new(*writer),
                    WriteToken {
                        epoch: *epoch,
                        seq: *seq,
                    },
                )
            })
            .collect()
    }

    #[test]
    fn refusal_codes_round_trip_through_their_byte() {
        let codes = [
            RefusalCode::NotCovered,
            RefusalCode::Unavailable,
            RefusalCode::BadRequest,
            RefusalCode::Version,
        ];
        for code in codes {
            assert_eq!(RefusalCode::from_code(code.code()), Some(code));
        }
        // Distinct bytes, and `NotCovered` is 1 — the one code the wire format
        // gives a body to.
        assert_eq!(RefusalCode::NotCovered.code(), 1);
        for (i, a) in codes.iter().enumerate() {
            for (j, b) in codes.iter().enumerate() {
                assert_eq!(a.code() == b.code(), i == j);
            }
        }
        // Undefined bytes are refused, never folded into a neighbour.
        assert_eq!(RefusalCode::from_code(0), None);
        for byte in 5..=u8::MAX {
            assert_eq!(RefusalCode::from_code(byte), None, "byte {byte}");
        }
    }

    #[test]
    fn an_io_error_is_a_handoff_error_with_a_source() {
        let err: HandoffError = io::Error::new(io::ErrorKind::BrokenPipe, "gone").into();
        assert!(matches!(err, HandoffError::Io(_)));
        assert!(std::error::Error::source(&err).is_some());
        assert!(err.to_string().contains("gone"));
        // Every other variant is self-contained: no hidden cause to chase.
        assert!(std::error::Error::source(&HandoffError::Truncated).is_none());
        assert!(std::error::Error::source(&HandoffError::Protocol("x")).is_none());
    }

    #[test]
    fn errors_name_the_thing_that_went_wrong() {
        let stale = HandoffError::StaleDonor {
            donor_epoch: 4,
            donor_host: Some(NodeId::new("old")),
            adopted_epoch: 6,
            adopted_host: Some(NodeId::new("new")),
        };
        assert!(stale.to_string().contains("epoch 4"), "{stale}");
        assert!(stale.to_string().contains("epoch 6"), "{stale}");
        let refused = HandoffError::Refused {
            code: RefusalCode::Unavailable,
        };
        assert!(refused.to_string().contains("cannot serve"), "{refused}");
    }

    #[test]
    fn a_snapshots_debug_shows_covers_and_never_the_bytes() {
        // Chunks are opaque and can be enormous; the covers map is the part an
        // operator needs in a log line.
        let snapshot = Snapshot {
            covers: marks(&[("h", 5, 9)]),
            chunks: (),
        };
        let shown = format!("{snapshot:?}");
        assert!(shown.contains("covers"), "{shown}");
        assert!(shown.contains('h'), "{shown}");
    }

    #[test]
    fn a_receipt_carries_the_stamp_and_the_coverage() {
        let receipt = HandoffReceipt {
            fence_epoch: 6,
            fence_host: None,
            covers: marks(&[("h", 5, 9)]),
        };
        // Hostless is a first-class stamp, not a missing one.
        assert_eq!(receipt.fence_host, None);
        assert_eq!(receipt.covers, marks(&[("h", 5, 9)]));
        assert_ne!(
            receipt,
            HandoffReceipt {
                fence_epoch: 6,
                fence_host: Some(NodeId::new("h")),
                covers: marks(&[("h", 5, 9)]),
            },
            "a named host and a hostless stamp are different receipts"
        );
    }
}
