//! The Hosted write path (M4): fenced, epoch-scoped writes through the group's
//! elected host, priced by a commit level.
//!
//! Everything below this module is *leaderless*. The session tier orders one
//! writer's own feed; the coherence tiers make every reader agree about what a
//! writer has said. Neither answers "who may write" — and for a group that
//! needs one authoritative serializer, that is the whole question.
//!
//! A [`Hosted`](groupnet_core::GroupMode::Hosted) group answers it by electing
//! an **epoch-fenced host** (M1–M3, in the engine). This module is the write
//! path on top: the host publishes through the existing
//! [`WriteFeed`](crate::WriteFeed) with the *leadership* epoch as the feed
//! epoch, so a migration is — to every subscriber — exactly a writer-restart
//! [`Gap`](crate::PeerWrite::Gap), handled by machinery that already exists.
//! What is new is the verdict: when may a write be acknowledged, and when may a
//! newly-elected host serve at all.
//!
//! # The two knobs, and what each buys
//!
//! **Activation** (the engine's, set per group) decides *who may host*.
//! **[`Commit`]** (this module's, set per write path) decides *when a hosted
//! write may be acknowledged*. They are orthogonal, and true strong consistency
//! needs both: an election can be perfectly CP and the write still lost, if the
//! host acknowledged from local state alone and died before any follower applied
//! it.
//!
//! | [`Commit`] | Acknowledged once… | The write path pays |
//! |---|---|---|
//! | [`Local`](Commit::Local) | the host applied it | nothing extra; a migration may lose the acked tail, surfaced as the epoch `Gap` |
//! | [`QuorumApplied`](Commit::QuorumApplied) | a majority of the **static voter roster** applied it | one voter-majority round per write |
//! | [`AllApplied`](Commit::AllApplied) | every selected `Alive` member applied it | one cluster round per write, over a rumour-derived set |
//!
//! `Quorum` activation × [`Commit::QuorumApplied`] is the **strong profile**:
//! the commit majority and the recovery majority intersect, so no write acked at
//! that level is ever lost across a migration (property S5). Named plainly, that
//! profile *is* consensus — view-stamped primary-backup over the existing feed —
//! and it is the ceiling: fixed, single-digit rosters, no general replicated log
//! behind it. `docs/consistency-modes.md` is the contract of record and carries
//! the full comparison.
//!
//! # The pieces
//!
//! * [`HostedWrites`] — the write half. Publishes through a [`WriteFeed`]
//!   whose epoch *is* the leadership epoch, refuses with a named reason when
//!   this node may not serve, and holds a committed write until its level's
//!   threshold is met.
//! * [`HostedReads`] — the read half: the group's authority as one ordered
//!   stream, with migrations surfaced as
//!   [`Migrated`](HostedRead::Migrated) + one
//!   [`Gap`](HostedRead::Gap). Bind it to the write half
//!   ([`HostedWrites::bind`]) so that a node which becomes host cuts its
//!   predecessor's lineage at the instant it starts serving.
//! * [`CommitLedger`] — the publisher half of the evidence. One gossiped,
//!   **epoch-stamped** entry per participant: the leadership epoch it has
//!   adopted, plus its applied watermark per writer.
//! * [`CommitCore`] — the commit rule, as a pure predicate over a snapshot.
//! * [`CompletenessCore`] — the leader-completeness recovery rule, likewise.
//!
//! The two cores are sans-IO: fed [`LedgerView`] snapshots, they return
//! verdicts. The deterministic simulator drives exactly the code the tokio shell
//! does, which is why S5 is a property over snapshots rather than an emergent
//! behaviour of an actor.
//!
//! [`WriteFeed`]: crate::WriteFeed
//!
//! # Recovery gates *service*, not election
//!
//! A candidate that collects a voter majority becomes host exactly when the
//! engine says it does; [`Group::leadership`](groupnet_runtime::Group::leadership)
//! reports it immediately. What waits is this write path: under
//! [`Commit::QuorumApplied`] it refuses service with [`HostedError::Recovering`]
//! until [`CompletenessCore::step`] answers [`Completeness::Complete`].
//!
//! That cut is deliberate. Committed state lives in entries the engine does not
//! interpret, so the engine cannot evaluate the predicate without learning this
//! crate's schema; a group whose consumers never build a write path must not sit
//! hostless; and leadership was never permission to serve in the first place —
//! M3's own contract says a consumer must not read a non-`None` host as licence
//! to act until the write path gives it a verdict. `Recovering` *is* that
//! verdict, and it is what "elected but not yet serving" looks like.
//!
//! # Honesty box: what this guarantees, and where it stops
//!
//! **Every outcome fails closed.** There is no path here whose failure mode is a
//! silent stale serve. A host that cannot prove completeness refuses service
//! ([`HostedError::Recovering`]); a write that cannot assemble its majority runs
//! out the caller's deadline and says so
//! ([`CommitOutcome::TimedOut`], naming `waiting_on`); a host that has been
//! fenced learns it as [`CommitOutcome::Deposed`]; a node that is not the host
//! is told who is, or told there is nobody
//! ([`HostedError::NotHost`] — with `host: None`, that is the `NoLeader` the
//! design promised a minority side). Every one of those is an availability
//! failure with a name attached.
//!
//! **Leadership is read from a watch, and the watch can lag.** A write is
//! admitted against [`Group::leadership`](groupnet_runtime::Group::leadership),
//! which the driver republishes after the engine changes its belief — so a
//! deposed host can admit one more write before it knows. That is fenced
//! **downstream**, not prevented upstream: the write is stamped with the epoch
//! it was admitted under, and a voter that has adopted a higher epoch stops
//! counting it, so the commit can never close and the caller sees `Deposed`.
//! Under [`Commit::Local`] there is nothing downstream to fence it with, and the
//! write is exactly the acked tail a migration may lose — surfaced to every
//! subscriber as the epoch `Gap`, which is the honest contract that level
//! carries.
//!
//! **Read semantics, per level, stated so nobody infers more.**
//!
//! * **Host reads are linearizable only under a valid lease at read time.** The
//!   host's authority expires `lease_ms` after its last successful renewal; a
//!   read served after that instant, from a host that has not yet noticed, is
//!   stale. Proving the lease still holds at the instant of the read (or taking
//!   a per-read renewal) is the consumer's job and is **documented, not
//!   automated** — this crate exposes [`Fence`] and
//!   [`Group::leadership`](groupnet_runtime::Group::leadership) and does not
//!   pretend to time the read for you.
//! * **Follower reads are sequentially consistent at a commit watermark, never
//!   linearizable.** A follower that barriers on the host's token with
//!   [`FrontierView::reached`](crate::FrontierView::reached) sees a prefix of the
//!   host's order, consistent and monotone, and arbitrarily far behind the
//!   present.
//! * [`Commit::AllApplied`] **inherits the ack tier's honesty box verbatim.**
//!   "Every alive member" means every member *this node currently believes*
//!   alive; under an asymmetric partition inside the probe window the guarantee
//!   is bounded-time, not absolute; one degraded-but-alive peer taxes every
//!   write. See the ack tier's `AckLedger`, and prefer the lease tier where
//!   that bargain does not fit.
//!
//! **The lineage's cut is a *service* boundary, and a serving host must take
//! it.** [`HostedReads`] closes a lineage when the successor's first write is
//! delivered — which never happens on the successor **itself**, because that
//! subscriber excludes this node's own feed. Left uncut, a predecessor's
//! un-replicated tail (gossiped state: it can arrive long after a partition
//! heals) is delivered to the serving host and applied *behind* the writes it
//! has authored at its own epoch. The mechanism is
//! [`HostedReads::cut_below`], and
//! [`HostedWrites::bind`] takes it automatically at the instant this node is
//! admitted to serve — the earliest honest instant, because a host that is still
//! [`Recovering`](HostedError::Recovering) genuinely needs that tail. **A
//! deployment that neither binds nor calls it keeps the divergence window open
//! for as long as it hosts**, and this box would be lying about that node.
//!
//! **A follower's drain window can apply writes that are doomed.** Until the new
//! lineage speaks, a follower still delivers the old host's tail — and a voter
//! outside the recovery majority can be *ahead* of the successor, so what it
//! applies there may be state no surviving host will ever hold. Nothing
//! acknowledged is at risk (the view-stamp fence means such a write was never
//! committed), and a cache is safe by construction: the [`Gap`](HostedRead::Gap)
//! that opens the next lineage is **authoritative**, and remediating it — flush,
//! rebuild, refetch from the consumer's own store — is what reconciles the
//! doomed tail away. A consumer that treats the stream as an exact replay log
//! and skips that rebuild keeps the divergence permanently; for it, the `Gap` is
//! not advisory.
//!
//! **Durability is majority-durability.** [`Commit::QuorumApplied`] cannot
//! outlive the simultaneous loss of a majority of the applied copies — the
//! watermarks are gossiped state, and a majority crashing amnesiac at once takes
//! the evidence with them. A durable application reseeds from its own store
//! ([`CommitLedger::with_recovered`]).
//!
//! **S5 presumes the `GrantStore` posture.** The commit predicate compares a
//! stamp to the write's epoch by *equality*, so an epoch that has stopped being
//! a unique name for a hostship — the storage-free blackout posture's one gap in
//! M3's property matrix — would let a reading stamped by a different hostship of
//! the same integer count. Storage-free Quorum keeps S4c; it does not keep S5.
//!
//! **Every voter must run the follower loop.** Apply the host's feed, then
//! [`CommitLedger::record`]; on an epoch change, [`CommitLedger::refresh`]. A
//! voter that votes but never publishes is invisible to both rules, and the tier
//! fails closed around it: commits time out and a new host stalls in recovery.
//! Loud, and never a lost write.

mod commit;
mod ledger;
mod lineage;
mod reads;
mod recovery;
mod writes;

use std::fmt;

use groupnet_core::NodeId;

pub use self::commit::{CommitCore, CommitVerdict};
pub use self::ledger::{
    CommitLedger, LedgerView, Reading, Watermarks, commit_applied_by, commit_reading,
    commit_reading_named, decode_ledger, encode_ledger, ledger_entry_key,
};
pub use self::lineage::HostedRead;
pub use self::reads::HostedReads;
pub use self::recovery::{Completeness, CompletenessCore};
pub use self::writes::{HostedSetupError, HostedWrites, hosted_feed_name};
use crate::token::WriteToken;

/// The capability a node advertises (via
/// [`Group::advertise_capabilities`](groupnet_runtime::Group::advertise_capabilities))
/// to declare that it participates in the Hosted write path: that it runs the
/// follower loop and publishes a [`CommitLedger`].
///
/// It carries the same rolling-upgrade footgun the other tiers document: a node
/// that participates but has not advertised yet is invisible to a selector and
/// is not waited for. It is **advisory here and nowhere else** — the
/// [`Commit::QuorumApplied`] denominator is the static voter roster, which no
/// advertisement can move. Use it to build the
/// [`Commit::AllApplied`] selection, where the set genuinely is rumour-derived.
///
/// Independent of the ack tier's `CAP_ACKS`: `hosted` does not imply `acks`,
/// the two ledgers are separate entries under separate rules, and a node may
/// advertise either, both, or neither.
pub const CAP_HOSTED: &str = "hosted";

/// When a hosted write may be acknowledged.
///
/// Orthogonal to the group's activation policy: activation answers *who may
/// host*, this answers *when a host's write is done*. See the module docs for
/// the price of each.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Commit {
    /// Acknowledged once the host applied it. Followers trail via the session
    /// tier, and a migration may lose the acked tail — surfaced honestly as the
    /// epoch [`Gap`](crate::PeerWrite::Gap). The game-lobby default: cheap, and
    /// clients rebase.
    Local,
    /// Acknowledged once a majority of the **static voter roster** has applied
    /// it. With `Activation::Quorum`, the commit majority and any later recovery
    /// majority intersect, so no write acked at this level is ever lost across a
    /// migration. This is the strong profile, and it costs a voter-majority
    /// round per write.
    QuorumApplied,
    /// Acknowledged once every selected `Alive` member has applied it —
    /// unanimity over a rumour-derived set, for read-anywhere-after-ack.
    /// Inherits the ack tier's honesty box verbatim; prefer it leased, and only
    /// on small fixed rosters.
    AllApplied,
}

/// The fencing token: the `(epoch, host)` pair that names one hostship.
///
/// # What it is for
///
/// Gossip cannot reject a doomed writer's disk I/O. A host that has been deposed
/// but not yet noticed can still issue a store write, and no amount of
/// membership machinery inside this fabric will stop it — so the fence token is
/// the bridge that makes "strong" real *end to end*: stamp it onto every
/// data-plane operation and, above all, onto **external stores**.
///
/// * An object store with conditional writes (S3/R2 `If-Match` /
///   `If-None-Match`, a CAS-claimed key) rejects the stale epoch itself, which
///   is where the guarantee then lives.
/// * A record written under a fence carries the epoch that authored it, so a
///   later reader can tell a doomed host's tail from the surviving history.
///
/// The philosophy the README already holds: gossip carries liveness and
/// coherence signals; stores own truth. A fence token is how the two meet.
///
/// # Ordering
///
/// Fences are compared **epoch-major**, exactly like
/// [`WriteToken`] — an epoch is the ordering, and the host is
/// the name. Same-epoch pairs from two partition sides are broken by the
/// deterministic rendezvous tiebreak in the engine; nothing here re-derives it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Fence {
    /// The leadership epoch this hostship was activated at.
    pub epoch: u64,
    /// The node hosting at that epoch.
    pub host: NodeId,
}

impl fmt::Display for Fence {
    /// `epoch:host` — a compact form for an `If-Match` header, a lock record, or
    /// a log line.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.epoch, self.host.as_str())
    }
}

/// Why a hosted write could not be attempted.
///
/// Every variant is a refusal *before* the write entered the feed — the
/// fail-closed direction. An error that arrives after the write was published is
/// a [`CommitOutcome`], not one of these.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostedError {
    /// This node is not the host of `epoch`.
    ///
    /// `host` is where to redirect the caller — and **`host: None` is the
    /// `NoLeader` this design promised**: the group is believed hostless at this
    /// epoch, which is exactly what a minority side under `Quorum` activation
    /// reports once its incumbent's lease has lapsed. Fail fast on it; do not
    /// wait for a host that a minority cannot elect.
    NotHost {
        /// The epoch this node has adopted.
        epoch: u64,
        /// The host of that epoch, or `None` when the group is believed
        /// hostless — the `NoLeader` case.
        host: Option<NodeId>,
    },
    /// This node *was* the host and has been fenced out: some peer holds an
    /// epoch above `epoch`, so nothing this node publishes under it will be
    /// adopted. Stop writing; the successor's state is the surviving one.
    Deposed {
        /// The epoch this node was deposed from.
        epoch: u64,
    },
    /// This node is the host and is **not serving yet**: the leader-completeness
    /// recovery rule has not been satisfied for this epoch. Retry — the state is
    /// transient by construction — or surface it as "leader elected, not
    /// ready". See [`CompletenessCore`] and the module docs.
    Recovering,
    /// The group actor's bounded inbox refused the enqueue. Backpressure, never
    /// a consistency verdict: the write did not happen, and retrying after a
    /// beat is correct.
    Rejected,
}

impl fmt::Display for HostedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HostedError::NotHost {
                epoch,
                host: Some(host),
            } => write!(f, "not the host at epoch {epoch}: {} is", host.as_str()),
            HostedError::NotHost { epoch, host: None } => {
                write!(f, "no host at epoch {epoch}")
            }
            HostedError::Deposed { epoch } => {
                write!(f, "deposed from epoch {epoch}")
            }
            HostedError::Recovering => {
                f.write_str("host is recovering: not serving at this epoch yet")
            }
            HostedError::Rejected => f.write_str("the group actor's inbox refused the write"),
        }
    }
}

impl std::error::Error for HostedError {}

/// How a hosted write ended, once it was published.
///
/// The first is the guarantee the write's [`Commit`] level promised; the other
/// two are the two honest ways it can fail to arrive, and neither is silent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitOutcome {
    /// The level's threshold was met. The write may be acknowledged to the
    /// caller's own client.
    Committed,
    /// The caller's deadline passed with the threshold unmet. **No commit
    /// guarantee holds** — the write is in the host's feed and may yet be
    /// applied everywhere, or may be lost at the next migration. `waiting_on`
    /// names the members that had not applied it, which is also the operational
    /// signal: a voter that appears here every time is voting without applying.
    TimedOut {
        /// The members still being waited on when the deadline passed, in id
        /// order.
        waiting_on: Vec<NodeId>,
    },
    /// The host was fenced out before the threshold was met: a peer holds an
    /// epoch above `epoch`, so no further reading can ever count this write.
    /// Treat it as not committed — at [`Commit::QuorumApplied`] the successor's
    /// recovery provably did not need it, and at [`Commit::Local`] it is the
    /// acked tail a migration loses.
    Deposed {
        /// The epoch the write was published under.
        epoch: u64,
    },
}

impl CommitOutcome {
    /// Whether the write's commit level was honoured: true only for
    /// [`Committed`](Self::Committed).
    #[must_use]
    pub fn is_committed(&self) -> bool {
        matches!(self, CommitOutcome::Committed)
    }
}

/// What a hosted write returns: the token it was published under, and how it
/// ended.
///
/// The token is issued even when the outcome is not [`CommitOutcome::Committed`]
/// — the write *is* in the host's feed, and a caller that wants to barrier on it
/// later, or report it, needs its name. Check
/// [`is_committed`](Self::is_committed) before acknowledging anything to a
/// client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitReceipt {
    /// The write's position in the host's feed. Its `epoch` is the **leadership
    /// epoch**, which is what makes a migration a writer-restart
    /// [`Gap`](crate::PeerWrite::Gap) to every subscriber.
    pub token: WriteToken,
    /// How the write ended.
    pub outcome: CommitOutcome,
}

impl CommitReceipt {
    /// Whether the write's commit level was honoured.
    #[must_use]
    pub fn is_committed(&self) -> bool {
        self.outcome.is_committed()
    }
}

#[cfg(test)]
mod tests {
    use groupnet_core::NodeId;

    use super::{Commit, CommitOutcome, CommitReceipt, Fence, HostedError};
    use crate::token::WriteToken;

    fn node(name: &str) -> NodeId {
        NodeId::new(name)
    }

    #[test]
    fn fences_order_epoch_major_and_print_compactly() {
        let old = Fence {
            epoch: 1,
            host: node("zzz"),
        };
        let new = Fence {
            epoch: 2,
            host: node("aaa"),
        };
        assert!(new > old, "epoch first, host only as the name");
        assert_eq!(new.to_string(), "2:aaa");
    }

    #[test]
    fn a_hostless_not_host_is_the_promised_no_leader() {
        let no_leader = HostedError::NotHost {
            epoch: 4,
            host: None,
        };
        assert_eq!(no_leader.to_string(), "no host at epoch 4");
        let redirect = HostedError::NotHost {
            epoch: 4,
            host: Some(node("b")),
        };
        assert_eq!(redirect.to_string(), "not the host at epoch 4: b is");
        assert_ne!(no_leader, redirect, "a redirect is not a NoLeader");
    }

    #[test]
    fn only_committed_is_a_commit() {
        let token = WriteToken { epoch: 7, seq: 1 };
        let receipt = |outcome: CommitOutcome| CommitReceipt { token, outcome };
        assert!(receipt(CommitOutcome::Committed).is_committed());
        assert!(
            !receipt(CommitOutcome::TimedOut {
                waiting_on: vec![node("a")],
            })
            .is_committed()
        );
        assert!(!receipt(CommitOutcome::Deposed { epoch: 7 }).is_committed());
        // The token survives every outcome: the write is in the feed either way.
        assert_eq!(receipt(CommitOutcome::Deposed { epoch: 7 }).token, token);
    }

    #[test]
    fn commit_levels_are_distinct_and_copy() {
        let levels = [Commit::Local, Commit::QuorumApplied, Commit::AllApplied];
        for (i, a) in levels.iter().enumerate() {
            for (j, b) in levels.iter().enumerate() {
                assert_eq!(a == b, i == j);
            }
        }
        let copied = levels[1];
        assert_eq!(copied, Commit::QuorumApplied, "Copy, not moved");
    }
}
