//! The stream driver: the requester's [`Handoff::fetch`] and the donor's
//! [`Handoff::offer`], over one data-plane stream.
//!
//! The shell around [`HandoffCore`]'s three verdicts, in the same relationship
//! [`HostedReads`](crate::hosted::HostedReads) has to
//! [`lineage`](crate::hosted): everything here is send, read and hand off, and
//! every *decision* is a call into the sans-IO core next door. There is one rule
//! this module adds on top, and it is a rule about order rather than about
//! judgement — see below.
//!
//! # The phase table guards the driver; the driver refuses the frames
//!
//! Two mechanisms live here, and it is worth being exact about which does which.
//!
//! **What actually refuses an out-of-order frame is the driver's own reads.**
//! [`fetch_on`](Handoff::fetch_on) is a straight line of `match`es, so a frame
//! arriving where the protocol does not admit one is caught by the arm that
//! reads it: "a chunk before a verified offer" and "a second offer mid-stream"
//! are the `_` arms of the two matches below, each returning
//! [`HandoffError::Protocol`] and dropping the sink. "A chunk after `Done`" is
//! refused more simply still — the staging loop breaks at the terminator and
//! **no further frame is ever read**.
//!
//! **[`HandoffPhase::advance`] is the guard on top of that**, and what it
//! protects is the *next edit to this file*. Every forward move goes through
//! `stepped`, so on every single transfer the order this driver walks is checked
//! against the core's six-cell table: a refactor that reordered a verification,
//! staged behind an unverified offer, or finished a sink twice stops being a
//! review question and becomes a `Protocol` error that the suites next door
//! catch. The table is never consulted for advice about a peer's frame — it is
//! the table checking the driver, not the driver checking the peer.
//!
//! # The two re-verifications, and why there are two
//!
//! A donor is checked against **`group.leadership()` read afresh at both ends of
//! the stream**, not once at the start:
//!
//! 1. **At the `Offer`.** [`HandoffCore::staleness`] on the donor's fence stamp,
//!    then [`HandoffCore::coverage`] on its `covers` against the requester's
//!    `need` — the order the core's own table fixes. Nothing has been staged
//!    yet, so a refusal here costs a connection.
//! 2. **At the `Done`.** The counts first ([`HandoffCore::done_consistent`]),
//!    then the **final** stamp the donor put in that frame, against a **freshly
//!    re-read** `leadership()`. This is the check the whole design turns on: a
//!    snapshot takes real time to stream, and a donor that was the serving host
//!    when it opened the image can be deposed while it is still sending. Both
//!    halves move — the donor stamps what *it* now believes, and the requester
//!    compares against what *it* now believes — so a migration that either side
//!    has learned about lands as [`HandoffError::StaleDonor`] with nothing
//!    installed. Verifying once, at the offer, would adopt exactly the state a
//!    surviving host has already ruled out.
//!
//! Only then is [`SnapshotSink::finish`] called. Every path before it — a
//! refusal, either staleness verdict, short coverage, a count mismatch, an
//! out-of-order frame, an I/O error — returns without finishing, and the sink is
//! **owned by the driver**, so returning drops it. That is why
//! [`fetch_on`](Handoff::fetch_on) takes the sink by value rather than by
//! reference: "a dropped, unfinished sink discards" is a contract nobody can
//! keep if the caller is still holding one.
//!
//! # Refusals and errors are one vocabulary
//!
//! A donor's [`RefusalCode`] is mapped to the error the requester raises, and
//! `offer` answers its *own* caller with the very same error it just put on the
//! wire — so the two ends of a refused handoff read identically in two logs:
//!
//! | `Refuse` frame | both sides see |
//! |---|---|
//! | [`NotCovered`](RefusalCode::NotCovered) | [`HandoffError::NotCovered`] `{ have }` — the donor's own map, carried |
//! | [`Unavailable`](RefusalCode::Unavailable) | [`HandoffError::Refused`] `{ code }` |
//! | [`BadRequest`](RefusalCode::BadRequest) | [`HandoffError::Refused`] `{ code }` |
//! | [`Version`](RefusalCode::Version) | [`HandoffError::Refused`] `{ code }` |
//!
//! `NotCovered` folding into [`HandoffError::NotCovered`] rather than into
//! `Refused { code }` is deliberate: that is also what the requester raises when
//! it detects short coverage *itself*, and the two are the same fact learned two
//! ways. A caller picking its next donor reads one variant, not two.
//!
//! [`RefusalCode::Version`] is the one code this driver never sends. A frame
//! carrying a version this build does not know is refused by the codec before
//! any driver sees it, and answering it in-band would mean encoding a reply in
//! the version the peer has just shown it cannot read. The code exists so that
//! a *future* version has a word for it, and so this one can decode it.
//!
//! # What `offer` does not do
//!
//! The phase table is the requester's: `OfferAccepted` names a *verification*
//! the donor cannot perform, because the donor is the party being verified. The
//! donor's side is therefore a straight line — read the request, open, check
//! coverage, offer, stream, terminate — and its only judgement call is the same
//! [`HandoffCore::coverage`] the requester will re-take on the same numbers.
//! Demultiplexing an accepted stream is the caller's business and
//! [`is_request`] is the cheap test for it; `offer` itself decodes the opener
//! and refuses anything that is not a request for a group and write path it
//! serves.

use std::fmt;
use std::io;

use bytes::Bytes;
use futures_util::io::{AsyncRead, AsyncWrite};
use groupnet_core::{NodeId, placement};
use groupnet_runtime::Group;
use groupnet_transport::bulk::{BulkTransport, DataPlane, DataStream};

use super::wire::{self, Frame};
use super::{
    Coverage, DoneCheck, DoneCounts, HandoffCore, HandoffError, HandoffPhase, HandoffReceipt,
    HandoffStep, RefusalCode, Snapshot, SnapshotChunks, SnapshotSink, SnapshotSource, Staleness,
};
use crate::Frontier;
use crate::hosted::{CommitLedger, Watermarks, commit_reading_named};

/// What a donor served, for its own logs and metrics.
///
/// The mirror image of the requester's [`HandoffReceipt`]: the same `covers`
/// claim, plus what it actually cost to hand over. A donor that wants to know
/// whether its snapshots are the size it thinks they are reads `bytes`; one
/// watching for a livelock (the module's ring-turns-faster-than-the-transfer
/// paragraph) watches how often it serves the same requester.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Offered {
    /// The watermarks the snapshot claimed to cover — the donor's half of the
    /// contract this module cannot verify.
    pub covers: Watermarks,
    /// Chunks sent.
    pub chunks: u64,
    /// Payload bytes across those chunks, framing excluded.
    pub bytes: u64,
}

/// One write path's snapshot handoff: the requester's puller and the donor's
/// server, bound to a group.
///
/// Hold one per write path (the name must match the
/// [`CommitLedger`]'s, because the coverage both ends check is that ledger's
/// map). It owns no stream and no task: the requester supplies a
/// [`DataPlane`] or a stream per transfer, and the donor supplies an accepted
/// stream per request, so a consumer decides its own concurrency limit rather
/// than inheriting one.
pub struct Handoff {
    group: Group,
    me: NodeId,
    name: String,
}

impl fmt::Debug for Handoff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Handoff")
            .field("group", &self.group.id())
            .field("me", &self.me)
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl Handoff {
    /// A handoff over `group`'s default write path.
    ///
    /// `me` is this node's id, in the same position
    /// [`HostedWrites::new`](crate::hosted::HostedWrites::new) and
    /// [`HostedReads::new`](crate::hosted::HostedReads::new) take it. It is used
    /// for one thing: [`donors`](Self::donors) never names this node.
    #[must_use]
    pub fn new(group: Group, me: NodeId) -> Self {
        Self::named("", group, me)
    }

    /// A handoff over a named write path — the counterpart of
    /// [`CommitLedger::named`] and
    /// [`HostedWrites::named`](crate::hosted::HostedWrites::named) under the
    /// same name.
    ///
    /// `name` must not contain `:`, which is the layout's own separator (a name
    /// that does would merge with a neighbouring path's key space); an empty
    /// name is the default path. It travels on the wire, and a donor refuses a
    /// request naming a path it does not serve with
    /// [`RefusalCode::BadRequest`].
    ///
    /// `me` is this node's id — see [`new`](Self::new).
    #[must_use]
    pub fn named(name: &str, group: Group, me: NodeId) -> Self {
        debug_assert!(
            !name.contains(':'),
            "a write path name must not contain ':' — it is the layout's separator"
        );
        Self {
            group,
            me,
            name: name.to_owned(),
        }
    }

    /// Serves one accepted stream: reads its request, and streams `source`'s
    /// snapshot back if it can honour it.
    ///
    /// Call it from an accept loop, on a stream whose first frame
    /// [`is_request`] has claimed. It runs to the end of the snapshot and
    /// returns what it sent; the stream is the caller's to close.
    ///
    /// A refusal is written to the stream **and** returned, as the same error
    /// the requester will raise — see the module's mapping table. A donor that
    /// refuses is working correctly; `Err` here means "this stream carried no
    /// snapshot", not "this node is broken".
    ///
    /// # Errors
    /// [`HandoffError::NotCovered`] when `source`'s image does not reach the
    /// request's `need`; [`HandoffError::Refused`] with
    /// [`RefusalCode::Unavailable`] when [`SnapshotSource::open`] fails, or with
    /// [`RefusalCode::BadRequest`] when the request names another group or write
    /// path; [`HandoffError::Protocol`] when the opener is not a request;
    /// [`HandoffError::Truncated`] when the opener never arrives whole; and
    /// [`HandoffError::Io`] for a transport or source failure — which the
    /// requester sees as a stream that ended without saying `Done`.
    pub async fn offer<S, Src>(
        &self,
        stream: &mut DataStream<S>,
        source: &Src,
    ) -> Result<Offered, HandoffError>
    where
        S: AsyncRead + AsyncWrite + Send + Unpin,
        Src: SnapshotSource,
    {
        let Frame::Request { group, name, need } = read_frame(stream).await? else {
            return Err(HandoffError::Protocol("a handoff opens with a request"));
        };
        if group != *self.group.id() || name != self.name {
            return Err(refuse(stream, RefusalCode::BadRequest, Watermarks::new()).await);
        }
        // A source that cannot open is not a donor. The requester tries the next
        // one, which is why this is transient by construction.
        let Ok(snapshot) = source.open().await else {
            return Err(refuse(stream, RefusalCode::Unavailable, Watermarks::new()).await);
        };
        let Snapshot { covers, mut chunks } = snapshot;
        // Refused *before* a byte of the image is read: the coverage verdict is
        // the same one the requester would take on the same two maps, and taking
        // it here is what stops a whole snapshot crossing the link to be thrown
        // away on arrival.
        if let Coverage::Short { .. } = HandoffCore::coverage(&need, &covers) {
            return Err(refuse(stream, RefusalCode::NotCovered, covers).await);
        }

        let lead = self.group.leadership();
        stream
            .send(wire::encode(&Frame::Offer {
                fence_epoch: lead.epoch,
                host: lead.host,
                covers: covers.clone(),
            }))
            .await?;

        let mut sent = DoneCounts::default();
        while let Some(chunk) = chunks.next().await? {
            sent.chunks = sent.chunks.saturating_add(1);
            sent.bytes = sent.bytes.saturating_add(payload_len(&chunk));
            stream.send(wire::encode(&Frame::Chunk(chunk))).await?;
        }
        // Re-read, deliberately: the terminator carries what this donor believes
        // *now*, not what it believed when it opened. A donor deposed mid-stream
        // stamps its own supersession into the frame that ends the transfer.
        let lead = self.group.leadership();
        stream
            .send(wire::encode(&Frame::Done {
                chunks: sent.chunks,
                bytes: sent.bytes,
                final_epoch: lead.epoch,
                final_host: lead.host,
            }))
            .await?;
        Ok(Offered {
            covers,
            chunks: sent.chunks,
            bytes: sent.bytes,
        })
    }

    /// Connects to `donor` over `plane` and pulls a covering snapshot into
    /// `sink`.
    ///
    /// [`connect`](DataPlane::connect) plus [`fetch_on`](Self::fetch_on), which
    /// is the whole of it — a handoff is one request/response exchange on one
    /// fresh stream, and nothing here pools or reuses one.
    ///
    /// # Errors
    /// As [`fetch_on`](Self::fetch_on), plus [`HandoffError::Io`] wrapping the
    /// transport's own connect error — a donor that cannot be reached is not
    /// distinguished from one that cannot serve, because the caller's answer to
    /// both is the next donor.
    pub async fn fetch<B, Snk>(
        &self,
        plane: &DataPlane<B>,
        donor: &NodeId,
        need: &Watermarks,
        sink: Snk,
    ) -> Result<HandoffReceipt, HandoffError>
    where
        B: BulkTransport,
        Snk: SnapshotSink,
    {
        let mut stream = plane.connect(donor).await.map_err(io::Error::other)?;
        self.fetch_on(&mut stream, need, sink).await
    }

    /// Pulls a covering snapshot over an already-open `stream`.
    ///
    /// The ordered exchange, in full: send the request; read the offer and take
    /// both verdicts on it; stage every chunk into `sink`; read the terminator
    /// and check its counts and its **re-read** fence; and only then
    /// [`finish`](SnapshotSink::finish). The module docs carry the argument for
    /// the second verification.
    ///
    /// `sink` is consumed. On success it is finished exactly once; on **every**
    /// failure it is dropped unfinished, which the [`SnapshotSink`] contract
    /// says must discard — so a failed handoff leaves this node's state exactly
    /// as it was.
    ///
    /// # Errors
    /// [`HandoffError::Refused`] or [`HandoffError::NotCovered`] for a donor
    /// that refused; [`HandoffError::StaleDonor`] at either verification point;
    /// [`HandoffError::NotCovered`] for an offer short of `need`;
    /// [`HandoffError::Truncated`] for a stream that ended before `Done` or
    /// counts that do not tally; [`HandoffError::Protocol`] for a peer speaking
    /// something else, or the right frames in the wrong order; and
    /// [`HandoffError::Io`] for the transport or the sink.
    pub async fn fetch_on<S, Snk>(
        &self,
        stream: &mut DataStream<S>,
        need: &Watermarks,
        sink: Snk,
    ) -> Result<HandoffReceipt, HandoffError>
    where
        S: AsyncRead + AsyncWrite + Send + Unpin,
        Snk: SnapshotSink,
    {
        let mut sink = sink;
        let mut phase = HandoffPhase::Request;
        stream
            .send(wire::encode(&Frame::Request {
                group: self.group.id().clone(),
                name: self.name.clone(),
                need: need.clone(),
            }))
            .await?;
        phase = stepped(phase, HandoffStep::Requested)?;

        // --- the offer, and the two verdicts taken on it ---
        let (fence_epoch, fence_host, covers) = match read_frame(stream).await? {
            Frame::Offer {
                fence_epoch,
                host,
                covers,
            } => (fence_epoch, host, covers),
            Frame::Refuse { code, have } => return Err(refused(code, have)),
            _ => return Err(HandoffError::Protocol("a request is answered by an offer")),
        };
        if let Some(stale) = self.staleness(fence_epoch, fence_host.clone()) {
            return Err(stale);
        }
        if let Coverage::Short { .. } = HandoffCore::coverage(need, &covers) {
            return Err(HandoffError::NotCovered { have: covers });
        }
        phase = stepped(phase, HandoffStep::OfferAccepted)?;

        // --- the image, staged one chunk at a time ---
        let mut staged = DoneCounts::default();
        let (claimed, final_epoch, final_host) = loop {
            match read_frame(stream).await? {
                Frame::Chunk(data) => {
                    staged.chunks = staged.chunks.saturating_add(1);
                    staged.bytes = staged.bytes.saturating_add(payload_len(&data));
                    sink.apply(data).await?;
                    phase = stepped(phase, HandoffStep::Chunk)?;
                }
                Frame::Done {
                    chunks,
                    bytes,
                    final_epoch,
                    final_host,
                } => {
                    phase = stepped(phase, HandoffStep::DoneSeen)?;
                    break (DoneCounts { chunks, bytes }, final_epoch, final_host);
                }
                _ => return Err(HandoffError::Protocol("a stage ends at a done frame")),
            }
        };

        // --- the terminator: what landed, and whose view it belongs to ---
        if HandoffCore::done_consistent(staged, claimed) == DoneCheck::Truncated {
            return Err(HandoffError::Truncated);
        }
        if let Some(stale) = self.staleness(final_epoch, final_host) {
            return Err(stale);
        }
        phase = stepped(phase, HandoffStep::CountsOk)?;
        sink.finish().await?;
        phase = stepped(phase, HandoffStep::Finished)?;
        debug_assert!(phase.is_complete(), "the ordered path ends installed");

        // The stamp the *image* was fixed under is the honest one to keep: it is
        // the view whose `covers` this receipt is about. The final stamp was
        // verified above and has nothing further to say.
        Ok(HandoffReceipt {
            fence_epoch,
            fence_host,
            covers,
        })
    }

    /// The members whose **published** commit ledgers already cover `need`, best
    /// donor first.
    ///
    /// The order is the module's honesty box made operational: the group's
    /// **serving host first**, because its state is the one state definitionally
    /// survivable, then everyone else in rendezvous order over the group id — the
    /// same deterministic ranking the engine's own candidate order uses, so every
    /// requester agrees on who to ask second without agreeing on anything.
    ///
    /// A member is a candidate only if the reading *this node currently sees*
    /// clears `need` under [`HandoffCore::coverage`]. That excludes silent
    /// members, undecodable ledgers and short ones alike — and **this node is
    /// excluded by construction**, whatever its own published reading says.
    ///
    /// That last exclusion is not tidiness. A caller that took itself off this
    /// list would [`connect`](DataPlane::connect) to its own endpoint, which no
    /// transport refuses: in-process the stream lands on this node's *own accept
    /// queue*, and over TCP it is an ordinary loopback connection. So the
    /// failure is not an error but a **silent hang** — the fetch waits for an
    /// offer only this node's own accept loop could write, which is at best a
    /// node streaming its state to itself and at worst (a serving loop that is
    /// busy, absent, or draining an endpoint that has since been replaced) a
    /// wait with nothing under it. Nothing downstream would catch it, so the
    /// list simply never names the caller. It stays a *filter* rather than a
    /// special case: a self-covering caller is merely absent, exactly as a
    /// short member is.
    ///
    /// A snapshot, and stale the instant it is taken: a donor can be short by
    /// the time it is asked, which is why the answer is a *list* and why every
    /// refusal names the next thing to try.
    #[must_use]
    pub fn donors(&self, need: &Watermarks) -> Vec<NodeId> {
        let members = self.group.members();
        let host = self.group.leadership().host;
        rank_donors(
            self.group.id().as_str(),
            &members,
            &self.me,
            host.as_ref(),
            |member| {
                commit_reading_named(&self.name, &self.group, member).is_some_and(|reading| {
                    matches!(HandoffCore::coverage(need, &reading.applied), Coverage::Ok)
                })
            },
        )
    }

    /// Folds an installed snapshot's `receipt` into this node's own evidence:
    /// the [`CommitLedger`] its peers read, and the [`Frontier`] its own readers
    /// barrier on.
    ///
    /// Call it **after** [`fetch`](Self::fetch) has returned `Ok` and never
    /// before: the receipt is the claim that the sink installed state at or
    /// above `covers`, and this publishes that claim to the group. It is the
    /// step that turns a transfer into a recovery — a host that was
    /// [`Recovering`](crate::hosted::HostedError::Recovering) re-asks
    /// [`CompletenessCore::step`](crate::hosted::CompletenessCore::step) against
    /// the ledger this just moved.
    ///
    /// Both folds are monotone, so a handoff that brought less than this node
    /// already had lowers nothing, and a retried one is a no-op. The closing
    /// [`refresh`](CommitLedger::refresh) is the freshness half of the
    /// deployment contract: it re-stamps with the group's current leadership
    /// epoch, which is what makes the new reading count for a recovering host
    /// even when no watermark moved.
    pub async fn seed(receipt: &HandoffReceipt, ledger: &CommitLedger, frontier: &Frontier) {
        for (writer, token) in &receipt.covers {
            ledger.record(writer, *token).await;
            frontier.advance(writer, *token);
        }
        ledger.refresh().await;
    }

    /// [`HandoffCore::staleness`] against a **freshly read** adopted pair, as
    /// the error it means. `None` is "not provably stale", which is all this
    /// check ever proves — see the core's module docs.
    fn staleness(&self, donor_epoch: u64, donor_host: Option<NodeId>) -> Option<HandoffError> {
        let lead = self.group.leadership();
        match HandoffCore::staleness(
            (donor_epoch, donor_host.as_ref()),
            (lead.epoch, lead.host.as_ref()),
        ) {
            Staleness::Ok => None,
            Staleness::Stale => Some(HandoffError::StaleDonor {
                donor_epoch,
                donor_host,
                adopted_epoch: lead.epoch,
                adopted_host: lead.host,
            }),
        }
    }
}

/// Whether `payload` opens a handoff — the cheap demux a data-plane accept loop
/// runs on the first frame of a stream before deciding whose business it is.
///
/// A prefix test, and **not** a validity claim: a `true` still goes through
/// [`Handoff::offer`], which is what refuses a truncated or malformed request.
/// A `false` is conclusive, though, so a consumer multiplexing its own protocols
/// onto one bulk transport can dispatch on it without reading further.
#[must_use]
pub fn is_request(payload: &[u8]) -> bool {
    wire::is_request(payload)
}

/// `phase.advance(step)`, with the table's `None` turned into the protocol error
/// it means.
///
/// Every forward move in [`Handoff::fetch_on`] goes through here, which is how
/// the core's six-cell table is checked against this driver on every transfer.
/// It is a guard, not the refusal mechanism: an out-of-order *frame* is already
/// refused by the arm that read it (see the module docs), and what a `None` here
/// catches is this file walking its own steps in an order the table does not
/// admit.
fn stepped(phase: HandoffPhase, step: HandoffStep) -> Result<HandoffPhase, HandoffError> {
    phase
        .advance(step)
        .ok_or(HandoffError::Protocol("handoff frame out of order"))
}

/// One whole frame, or the error the absence of one means.
///
/// Collapses the codec's three answers into the two a driver can act on: a
/// clean end of stream and a partial frame are both [`Truncated`] — silence is
/// never success on this protocol, which is exactly why it carries its own
/// terminator — while "not this protocol at all" stays
/// [`Protocol`](HandoffError::Protocol) and is not retried.
///
/// [`Truncated`]: HandoffError::Truncated
async fn read_frame<S: AsyncRead + Unpin>(
    stream: &mut DataStream<S>,
) -> Result<Frame, HandoffError> {
    let Some(payload) = stream.recv().await? else {
        return Err(HandoffError::Truncated);
    };
    wire::decode(&payload)?.ok_or(HandoffError::Truncated)
}

/// Writes a refusal and answers with the error it means, so the donor's own
/// caller sees what the requester will see.
///
/// A write failure on the refusal replaces it: the requester is already gone and
/// the I/O error is the more informative of the two.
async fn refuse<S: AsyncWrite + Unpin>(
    stream: &mut DataStream<S>,
    code: RefusalCode,
    have: Watermarks,
) -> HandoffError {
    let frame = Frame::Refuse {
        code,
        have: have.clone(),
    };
    match stream.send(wire::encode(&frame)).await {
        Ok(()) => refused(code, have),
        Err(err) => HandoffError::Io(err),
    }
}

/// What a `Refuse` frame means, on either side of the stream — the module's
/// mapping table, as code.
fn refused(code: RefusalCode, have: Watermarks) -> HandoffError {
    match code {
        // The same variant local detection raises: one fact, learned two ways,
        // and a caller choosing its next donor should not have to match twice.
        RefusalCode::NotCovered => HandoffError::NotCovered { have },
        code => HandoffError::Refused { code },
    }
}

/// A chunk's payload length as the `u64` both sides tally in.
///
/// The framing layer caps a frame well below `u64::MAX`, so the conversion is
/// exact by construction; saturating is the honest answer to an impossible input
/// and it fails the count check rather than panicking mid-transfer.
fn payload_len(chunk: &Bytes) -> u64 {
    u64::try_from(chunk.len()).unwrap_or(u64::MAX)
}

/// The donor order, as a function of the five things it depends on — pure, so
/// the rule can be tested against hosts that are *not* the rendezvous-top node,
/// which a healthy group never produces.
///
/// Host first (when it is a member and it covers), then the rest in rendezvous
/// order, then `me` and everything that does not cover removed. Filtering last
/// is what makes a non-covering host — or a caller that happens to be the host —
/// merely absent rather than promoted-then-dropped.
fn rank_donors(
    group: &str,
    members: &[NodeId],
    me: &NodeId,
    host: Option<&NodeId>,
    covers: impl Fn(&NodeId) -> bool,
) -> Vec<NodeId> {
    let weighted: Vec<(NodeId, u32)> = members.iter().map(|id| (id.clone(), 1)).collect();
    let ranked = placement::owners(group, &weighted, weighted.len());
    let mut ordered = Vec::with_capacity(ranked.len());
    if let Some(host) = host {
        if ranked.iter().any(|member| member == host) {
            ordered.push(host.clone());
        }
    }
    ordered.extend(ranked.into_iter().filter(|member| Some(member) != host));
    // `me` goes with the short members: the caller is never its own donor, and
    // a coverage answer about itself is not the question. See `Handoff::donors`
    // for what a self-connect actually does.
    ordered.retain(|member| member != me && covers(member));
    ordered
}

#[cfg(test)]
mod tests {
    use groupnet_core::NodeId;

    use super::{rank_donors, refused, stepped};
    use crate::hosted::Watermarks;
    use crate::hosted::handoff::{HandoffError, HandoffPhase, HandoffStep, RefusalCode};
    use crate::token::WriteToken;

    const GROUP: &str = "donor-order";

    fn nodes(ids: &[&str]) -> Vec<NodeId> {
        ids.iter().map(|id| NodeId::new(*id)).collect()
    }

    fn names(ids: &[NodeId]) -> Vec<&str> {
        ids.iter().map(NodeId::as_str).collect()
    }

    /// The caller, for the tests that are not about the caller: an id outside
    /// every membership below, so the self-filter takes nothing away.
    fn outsider() -> NodeId {
        NodeId::new("d-caller")
    }

    /// The rendezvous ranking these tests are written against, taken from the
    /// same primitive the function under test uses — so the assertions below are
    /// about *ordering*, never about which id happens to hash highest.
    fn ranked(members: &[NodeId]) -> Vec<NodeId> {
        rank_donors(GROUP, members, &outsider(), None, |_| true)
    }

    #[test]
    fn a_hostless_group_orders_by_rendezvous_alone() {
        let members = nodes(&["d-a", "d-b", "d-c", "d-d"]);
        let order = rank_donors(GROUP, &members, &outsider(), None, |_| true);
        assert_eq!(order.len(), members.len(), "everybody covers");
        assert_eq!(order, ranked(&members));
        // Order-independent: the same set in any order ranks identically, which
        // is what lets two requesters agree without talking.
        let mut shuffled = members.clone();
        shuffled.reverse();
        assert_eq!(
            rank_donors(GROUP, &shuffled, &outsider(), None, |_| true),
            order
        );
    }

    /// The filter that closes the self-connect: a caller is not a donor, and
    /// answering `true` for it does not make it one.
    #[test]
    fn the_caller_is_never_its_own_donor() {
        let members = nodes(&["d-a", "d-b", "d-c"]);
        for me in &members {
            let order = rank_donors(GROUP, &members, me, None, |_| true);
            assert!(
                !order.contains(me),
                "{me} listed itself: {:?}",
                names(&order)
            );
            // Removed, and nothing else disturbed — the rest keep their order.
            let expected: Vec<NodeId> = ranked(&members)
                .into_iter()
                .filter(|member| member != me)
                .collect();
            assert_eq!(order, expected);
        }
        // …including when the caller is the serving host, which is precisely a
        // recovering host looking for somebody to fill it in.
        let me = ranked(&members)[0].clone();
        let order = rank_donors(GROUP, &members, &me, Some(&me), |_| true);
        assert!(!order.contains(&me), "{:?}", names(&order));
        assert_eq!(order.len(), members.len() - 1);
        // A group of one, where the one is the caller, is nobody.
        assert!(rank_donors(GROUP, &members[..1], &members[0], None, |_| true).is_empty());
    }

    /// The load-bearing half of the rule, and the one a live cluster cannot
    /// arrange: the engine elects the rendezvous-top candidate, so a *healthy*
    /// group never shows a host that ordinary ranking would not have put first.
    #[test]
    fn the_host_is_first_even_when_rendezvous_would_rank_it_last() {
        let members = nodes(&["d-a", "d-b", "d-c", "d-d"]);
        let ranking = ranked(&members);
        let last = ranking.last().expect("four members").clone();
        let order = rank_donors(GROUP, &members, &outsider(), Some(&last), |_| true);
        assert_eq!(order[0], last, "the serving host is asked first: {order:?}");
        // …and behind it, everyone else keeps the rendezvous order exactly.
        let behind: Vec<NodeId> = ranking.into_iter().filter(|id| *id != last).collect();
        assert_eq!(order[1..], behind[..], "{order:?}");
        // Every member appears exactly once — the host is hoisted, not doubled.
        assert_eq!(order.len(), members.len());
    }

    #[test]
    fn a_host_that_does_not_cover_is_absent_rather_than_first() {
        let members = nodes(&["d-a", "d-b", "d-c"]);
        let host = ranked(&members).last().expect("three members").clone();
        let order = rank_donors(GROUP, &members, &outsider(), Some(&host), |id| *id != host);
        assert!(
            !order.contains(&host),
            "coverage is a filter the host does not escape: {:?}",
            names(&order)
        );
        assert_eq!(order.len(), 2);
    }

    #[test]
    fn a_host_outside_the_membership_is_not_a_candidate() {
        let members = nodes(&["d-a", "d-b"]);
        let stranger = NodeId::new("d-gone");
        let order = rank_donors(GROUP, &members, &outsider(), Some(&stranger), |_| true);
        assert_eq!(order, ranked(&members), "{:?}", names(&order));
    }

    #[test]
    fn nothing_covering_is_an_empty_list_and_not_a_guess() {
        let members = nodes(&["d-a", "d-b", "d-c"]);
        let host = members[0].clone();
        assert!(rank_donors(GROUP, &members, &outsider(), Some(&host), |_| false).is_empty());
        assert!(rank_donors(GROUP, &[], &outsider(), None, |_| true).is_empty());
    }

    #[test]
    fn only_a_not_covered_refusal_becomes_the_not_covered_error() {
        let have: Watermarks = [(NodeId::new("w"), WriteToken { epoch: 1, seq: 2 })]
            .into_iter()
            .collect();
        // The one code that folds into the same variant local detection raises,
        // carrying the donor's map so the caller can choose a better donor.
        match refused(RefusalCode::NotCovered, have.clone()) {
            HandoffError::NotCovered { have: carried } => assert_eq!(carried, have),
            other => panic!("unexpected {other:?}"),
        }
        for code in [
            RefusalCode::Unavailable,
            RefusalCode::BadRequest,
            RefusalCode::Version,
        ] {
            match refused(code, Watermarks::new()) {
                HandoffError::Refused { code: got } => assert_eq!(got, code),
                other => panic!("unexpected {other:?}"),
            }
        }
    }

    #[test]
    fn an_illegal_step_is_a_protocol_error_and_never_a_retry() {
        assert_eq!(
            stepped(HandoffPhase::Request, HandoffStep::Requested).expect("the first move"),
            HandoffPhase::OfferVerify
        );
        // The cell the whole staging order rests on: nothing is applied behind
        // an offer that has not passed both checks.
        assert!(matches!(
            stepped(HandoffPhase::OfferVerify, HandoffStep::Chunk),
            Err(HandoffError::Protocol(_))
        ));
        assert!(matches!(
            stepped(HandoffPhase::Complete, HandoffStep::Finished),
            Err(HandoffError::Protocol(_))
        ));
    }
}
