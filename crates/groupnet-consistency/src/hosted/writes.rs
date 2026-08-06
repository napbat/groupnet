//! The tokio shell around the write half: [`HostedWrites`], one node's fenced
//! participation in a hosted write path.
//!
//! Everything here is glue. The rules live next door in [`CommitCore`] (when a
//! write is done) and [`CompletenessCore`] (when a new host may start); this
//! module supplies the three things a sans-IO core cannot — the group's gossip,
//! a feed to publish into, and a deadline to poll against.
//!
//! # The feed life *is* the hostship
//!
//! A hosted write path is an ordinary [`WriteFeed`] with one substitution: its
//! epoch is not a wall clock but the **leadership epoch**. Every time this node
//! activates as host at `e′` the shell starts a fresh feed life
//! ([`WriteFeed::with_epoch`]) at `e′`, sequences from 1, and publishes into
//! `~writes:hosted` (`~writes:hosted:<name>` for a named path).
//!
//! That one substitution is what makes a migration free. Epoch-major token
//! ordering already turns a writer restart into a
//! [`PeerWrite::Gap`](crate::PeerWrite::Gap) covering the whole previous life,
//! and every subscriber already handles it — so a *host* migration is, to the
//! machinery below, a writer restart, and the lineage it produces is what
//! [`HostedReads`](super::HostedReads) reads back.
//!
//! # Two regimes, and why the committed one takes your ledger
//!
//! [`new`](HostedWrites::new) / [`named`](HostedWrites::named) build the
//! **Local-only** regime: no recovery gate, no ledger, no roster. Right for
//! `Settle` × [`Commit::Local`] — the lobby shape the mode was named for.
//!
//! [`committed`](HostedWrites::committed) /
//! [`named_committed`](HostedWrites::named_committed) build the **committed**
//! regime, and refuse to build at all unless the group is
//! [`Hosted`](groupnet_core::GroupMode::Hosted) with
//! [`Quorum`](groupnet_core::Activation::Quorum) activation — because
//! [`Commit::QuorumApplied`]'s denominator is that activation's static voter
//! roster, and property S5 rests on the commit majority and the recovery
//! majority being majorities of the *same* roster. There is no honest way to
//! synthesize one.
//!
//! They also take **this node's own [`CommitLedger`]**, and that coupling is
//! deliberate rather than incidental: the recovery rule compares the roster's
//! readings against *the recovering host's own applied watermarks*, and a host
//! is also a follower — its own ledger **is** its applied state. Passing it in
//! makes that explicit and checkable (the constructor verifies the ledger
//! publishes under this path's entry key) instead of leaving the shell to guess
//! at a second, private copy that could silently disagree with the one the
//! node's follower loop actually feeds.
//!
//! # The gate arms every publish, including `Local`
//!
//! An un-recovered host refuses [`Commit::Local`] writes too. That looks strict
//! — `Local` promises nothing about followers — but the promise it *does* make
//! is that the host serialized the write against its own state, and a host that
//! has not finished recovering does not yet have the state to serialize against.
//! Admitting a `Local` write there would order it after a prefix of history the
//! host cannot see, which is precisely the silent-stale-serve this tier refuses
//! everywhere else.
//!
//! # Where the verdicts are computed
//!
//! Both are computed **on demand, synchronously**, off the always-current gossip
//! snapshots — no background task, so this type spawns nothing, requires no
//! runtime to construct, and needs no `Drop`. The recovery verdict is then
//! **latched per epoch**: once [`Completeness::Complete`](super::Completeness)
//! answers for `e′` this node serves `e′` for as long as it holds it, and a
//! later reading that would raise the target does not re-close the gate. That
//! is correct as well as cheap — the intersection argument only ever needs *one*
//! fresh majority, read once, and a laggard's watermark arriving afterwards
//! describes an *uncommitted* tail, not a write the successor owes anybody.
//!
//! # Admission is also what cuts the read half's lineage
//!
//! The same instant does one more job. A host's own
//! [`HostedReads`](super::HostedReads) excludes its own feed, so nothing that
//! subscriber can be *delivered* will ever close the **predecessor's** lineage —
//! and a late tail arriving afterwards would be applied behind this host's own
//! writes. [`HostedWrites::bind`] shares this handle's serving epoch with a
//! subscriber, which cuts there. Being a latch on the admission verdict makes it
//! exactly as late as it must be — a `Recovering` host still drains the tail it
//! is measured against — and no later.

use std::fmt;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use groupnet_core::{Activation, GroupMode, NodeId, Status, VoterRoster};
use groupnet_runtime::{Group, Leadership};

use super::ledger::{CommitLedger, LedgerView, commit_reading_named, ledger_entry_key};
use super::{
    CAP_HOSTED, Commit, CommitCore, CommitOutcome, CommitReceipt, CommitVerdict, CompletenessCore,
    Fence, HostedError,
};
use crate::WriteFeed;
use crate::token::WriteToken;

/// How often a committed write re-examines the gossiped ledgers — the same
/// cadence [`applied_by_selected`](crate::applied_by_selected) and the lease
/// tier's coherent write poll at, so the healthy path costs one ack round and
/// not a beat more.
const COMMIT_POLL: Duration = Duration::from_millis(2);

type EncodeFn<K> = dyn Fn(&K) -> Vec<u8> + Send + Sync;

/// The feed name a hosted write path occupies: `hosted` for the default path,
/// `hosted:<name>` for a named one — so its entry key is
/// `~writes:hosted[:<name>]`, and a subscriber pairs with it through
/// [`HostedReads`](super::HostedReads) (or reads its head with
/// [`advertised_head_named`](crate::advertised_head_named)).
///
/// The bare feed name `hosted` is **reserved** by this tier: a consumer that
/// also runs a plain [`WriteFeed::named`]`("hosted", …)` would author into the
/// same entry. Name your paths, or leave that one alone.
#[must_use]
pub fn hosted_feed_name(name: &str) -> String {
    if name.is_empty() {
        "hosted".to_owned()
    } else {
        format!("hosted:{name}")
    }
}

/// Why a committed-regime [`HostedWrites`] could not be constructed.
///
/// Every variant is a **configuration** fault, caught once at construction
/// rather than per write: the group this handle was handed cannot support the
/// guarantee the constructor's name promises. None of them is transient, and
/// retrying changes nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostedSetupError {
    /// The group is not a [`Hosted`](groupnet_core::GroupMode::Hosted) group, so
    /// it elects nobody and there is no hostship to write under. Join it with
    /// [`Node::join_group_with`](groupnet_runtime::Node::join_group_with).
    NotHosted,
    /// The group is Hosted but does not use
    /// [`Quorum`](groupnet_core::Activation::Quorum) activation, so it has no
    /// **static voter roster** — and [`Commit::QuorumApplied`]'s denominator is
    /// exactly that roster. Under any other activation the commit majority and
    /// the recovery majority are not majorities of one fixed set, so they need
    /// not intersect and S5 does not hold. Use
    /// [`HostedWrites::new`] for [`Commit::Local`] service instead.
    NotQuorum,
    /// The [`CommitLedger`] handed in publishes under a different entry key than
    /// this write path reads: the two were built with different names, so the
    /// recovery rule would compare this node's own applied map against a roster
    /// view assembled from entries nobody writes.
    LedgerMismatch {
        /// The key this write path reads its peers' ledgers under.
        expected: String,
        /// The key the ledger handed in publishes under.
        found: String,
    },
}

impl fmt::Display for HostedSetupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HostedSetupError::NotHosted => {
                f.write_str("the group is not a Hosted group: it elects no host")
            }
            HostedSetupError::NotQuorum => f.write_str(
                "the group does not use Quorum activation, so it has no static voter roster",
            ),
            HostedSetupError::LedgerMismatch { expected, found } => write!(
                f,
                "the commit ledger publishes under {found}, but this write path reads {expected}"
            ),
        }
    }
}

impl std::error::Error for HostedSetupError {}

/// The committed regime's two couplings: the roster the rules are denominated
/// over, and this node's own applied state.
#[derive(Debug)]
struct Committed {
    voters: VoterRoster,
    ledger: Arc<CommitLedger>,
}

/// Everything the handle mutates: the current feed life, and the two latches.
struct State<K> {
    /// The feed life for [`Self::feed_epoch`], built on first use of an epoch.
    feed: Option<Arc<WriteFeed<K>>>,
    /// The leadership epoch [`Self::feed`] was armed at.
    feed_epoch: u64,
    /// The highest epoch this handle has ever observed itself hosting at.
    ///
    /// The whole reason [`HostedError::Deposed`] can be told apart from
    /// [`HostedError::NotHost`]: without it, a fenced-out host and a node that
    /// never held the group look identical through
    /// [`Group::leadership`](groupnet_runtime::Group::leadership).
    last_hosted: Option<u64>,
    /// The epoch the recovery gate opened for; see the module docs on latching.
    recovered_at: Option<u64>,
}

impl<K> fmt::Debug for State<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("State")
            .field("feed_epoch", &self.feed_epoch)
            .field("last_hosted", &self.last_hosted)
            .field("recovered_at", &self.recovered_at)
            .finish_non_exhaustive()
    }
}

/// This node's participation in one hosted write path: it publishes as host
/// when it is one, refuses with a named reason when it is not, and holds a
/// committed write until its level's threshold is met.
///
/// Cheap to construct and cheap to hold — no tasks, no timers, no `Drop`
/// behaviour. Hold one wherever the group handle lives; a node that is not
/// currently the host keeps its handle and simply gets [`HostedError::NotHost`]
/// until it is.
///
/// # The deployment contract this handle assumes
///
/// **Every voter runs the follower loop** — [`HostedReads`](super::HostedReads)
/// into the apply, then [`CommitLedger::record`], and
/// [`CommitLedger::refresh`] on a
/// [`Migrated`](super::HostedRead::Migrated). This node included: the host is a
/// voter like any other, and its own fresh reading is what a *later* host's
/// recovery majority may be built from.
///
/// # This host counts itself
///
/// A successful [`publish`](Self::publish) records the write into this node's
/// own [`CommitLedger`] before returning, so the host's own reading satisfies
/// the commit predicate for its own writes. That is the deliberate reading of
/// [`CommitCore`]'s "a host that applies its own writes and records them
/// satisfies its own predicate": [`publish`](Self::publish) is called *after*
/// the caller's local durable write (the [`WriteFeed`] contract), so by the time
/// it returns the host genuinely has applied it.
///
/// The alternative — never recording — is also safe, and strictly stricter: it
/// turns [`Commit::QuorumApplied`] over a roster of three into unanimity among
/// the two followers. This handle takes the majority the cost model promises.
/// Nothing is lost either way; the intersection argument holds for both, because
/// any two majorities of the roster meet whether or not the host is in one of
/// them.
pub struct HostedWrites<K> {
    group: Group,
    me: NodeId,
    /// The write path's name (`""` for the default), which fixes both the feed
    /// name and the commit-ledger entry key.
    name: String,
    feed_name: String,
    capacity: NonZeroUsize,
    encode: Arc<EncodeFn<K>>,
    /// `None` in the Local-only regime.
    committed: Option<Committed>,
    /// The highest epoch this handle has ever been **admitted to serve** at —
    /// the one signal a bound [`HostedReads`](super::HostedReads) needs to cut
    /// its own lineage. Zero until the first admission. See [`Self::bind`].
    serving: Arc<AtomicU64>,
    state: Mutex<State<K>>,
}

impl<K> fmt::Debug for HostedWrites<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HostedWrites")
            .field("group", &self.group.id())
            .field("me", &self.me)
            .field("feed", &self.feed_name)
            .field("committed", &self.committed.is_some())
            .field("serving", &self.serving.load(Ordering::Relaxed))
            .field("state", &self.lock())
            .finish_non_exhaustive()
    }
}

/// The admission verdict, as a **pure function** of what the leadership watch
/// reports, the highest epoch this handle has hosted at, and whether the
/// recovery gate has opened for the epoch on offer.
///
/// The whole error table lives here, so it is unit-testable without a group:
///
/// | this node | `last_hosted` | `recovered` | verdict |
/// |---|---|---|---|
/// | is the host | any | yes | `Ok(epoch)` |
/// | is the host | any | no | [`HostedError::Recovering`] |
/// | is not | `Some(e)` | — | [`HostedError::Deposed`] `{ epoch: e }` |
/// | is not | `None` | — | [`HostedError::NotHost`] `{ epoch, host }` |
///
/// "Is the host" is the adopted pair naming this node, which is exactly what
/// [`Role::Host`](groupnet_runtime::Role) means observer-locally.
///
/// The `Deposed` row is the one that needs the memory: `(e, None)` — a host that
/// lapsed and stepped down — and `(e′, Some(peer))` — a successor — are both
/// "not the host" through the watch, and a caller that once *was* the host must
/// be told it has been fenced rather than politely redirected. A node that never
/// held the group gets the redirect, and `host: None` there is the promised
/// `NoLeader`.
fn admit(
    lead: &Leadership,
    me: &NodeId,
    last_hosted: Option<u64>,
    recovered: bool,
) -> Result<u64, HostedError> {
    if lead.host.as_ref() == Some(me) {
        return if recovered {
            Ok(lead.epoch)
        } else {
            Err(HostedError::Recovering)
        };
    }
    match last_hosted {
        Some(epoch) => Err(HostedError::Deposed { epoch }),
        None => Err(HostedError::NotHost {
            epoch: lead.epoch,
            host: lead.host.clone(),
        }),
    }
}

/// `K: 'static` throughout: a feed life is rebuilt per epoch from a stored
/// encoder, so the encoder — and therefore the datum it encodes — outlives every
/// individual feed.
impl<K: 'static> HostedWrites<K> {
    /// The default hosted write path over `group`, in the **Local-only** regime:
    /// no recovery gate, no ledger, no roster.
    ///
    /// `capacity` sizes the feed's ring exactly as [`WriteFeed::new`]'s does — a
    /// subscriber that falls further behind than it holds is
    /// [`Gap`](super::HostedRead::Gap)-remediated rather than sent the
    /// individual keys, and a *recovering* host in that position is remediated
    /// the same way, so size it for the worst migration lag you accept.
    ///
    /// [`publish_committed`](Self::publish_committed) still accepts every level
    /// here, and two of them behave differently for want of a roster:
    /// [`Commit::AllApplied`] works normally (its set is rumour-derived), while
    /// [`Commit::QuorumApplied`] has an **empty** view and can therefore only
    /// ever run out the caller's deadline — the same fail-safe an empty roster
    /// gets everywhere in this tier. Use [`committed`](Self::committed) for that
    /// level.
    pub fn new(
        group: Group,
        me: NodeId,
        capacity: NonZeroUsize,
        encode: impl Fn(&K) -> Vec<u8> + Send + Sync + 'static,
    ) -> Self {
        Self::named("", group, me, capacity, encode)
    }

    /// A named hosted write path — independent subsystems sharing one group must
    /// name their paths so their feeds and ledgers stay apart.
    ///
    /// `name` must not contain `:`, which is the layout's own separator (a name
    /// that does would merge with a neighbouring path's key space); an empty
    /// name is the default path.
    pub fn named(
        name: &str,
        group: Group,
        me: NodeId,
        capacity: NonZeroUsize,
        encode: impl Fn(&K) -> Vec<u8> + Send + Sync + 'static,
    ) -> Self {
        debug_assert!(
            !name.contains(':'),
            "a write path name must not contain ':' — it is the layout's separator"
        );
        Self {
            group,
            me,
            name: name.to_owned(),
            feed_name: hosted_feed_name(name),
            capacity,
            encode: Arc::new(encode),
            committed: None,
            serving: Arc::new(AtomicU64::new(0)),
            state: Mutex::new(State {
                feed: None,
                feed_epoch: 0,
                last_hosted: None,
                recovered_at: None,
            }),
        }
    }

    /// The default hosted write path in the **committed** regime: the recovery
    /// gate is armed, and [`Commit::QuorumApplied`] is denominated over the
    /// group's static voter roster.
    ///
    /// `ledger` is this node's own [`CommitLedger`] — the very one its follower
    /// loop feeds. See the module docs for why the coupling is a parameter
    /// rather than something this type builds for itself.
    ///
    /// # Errors
    /// [`HostedSetupError::NotHosted`] if the group elects no host,
    /// [`HostedSetupError::NotQuorum`] if it elects one without a static voter
    /// roster, and [`HostedSetupError::LedgerMismatch`] if `ledger` publishes
    /// under a different write path's key.
    pub fn committed(
        group: Group,
        me: NodeId,
        capacity: NonZeroUsize,
        encode: impl Fn(&K) -> Vec<u8> + Send + Sync + 'static,
        ledger: Arc<CommitLedger>,
    ) -> Result<Self, HostedSetupError> {
        Self::named_committed("", group, me, capacity, encode, ledger)
    }

    /// [`committed`](Self::committed) for a named write path. `ledger` must be
    /// the [`CommitLedger::named`] of the same name.
    ///
    /// # Errors
    /// As [`committed`](Self::committed).
    pub fn named_committed(
        name: &str,
        group: Group,
        me: NodeId,
        capacity: NonZeroUsize,
        encode: impl Fn(&K) -> Vec<u8> + Send + Sync + 'static,
        ledger: Arc<CommitLedger>,
    ) -> Result<Self, HostedSetupError> {
        let voters = quorum_roster(&group)?;
        let expected = ledger_entry_key(name);
        if ledger.entry_key() != expected {
            return Err(HostedSetupError::LedgerMismatch {
                expected,
                found: ledger.entry_key().to_owned(),
            });
        }
        let mut path = Self::named(name, group, me, capacity, encode);
        path.committed = Some(Committed { voters, ledger });
        Ok(path)
    }

    /// The fencing token for this node's *current* authority, or `None` when it
    /// has none.
    ///
    /// `Some` exactly when a write would be admitted: this node is the host of
    /// the epoch the token names **and**, in the committed regime, has completed
    /// recovery for it. So a `Some` here is the same verdict
    /// [`publish`](Self::publish) would give, taken without publishing anything
    /// — which is what a consumer stamping an external CAS, or deciding whether
    /// it may serve a read at all, needs.
    ///
    /// It is a snapshot and it can be stale the instant it is taken: the
    /// leadership watch lags the engine, and the host's authority expires
    /// `lease_ms` after its last renewal whatever this returned. Stamp it onto
    /// the store operation and let the **store** reject the stale epoch; do not
    /// read it as a lock.
    #[must_use]
    pub fn fence(&self) -> Option<Fence> {
        self.admit_now().ok().map(|epoch| Fence {
            epoch,
            host: self.me.clone(),
        })
    }

    /// Why this node is not serving yet — the recovery rule's verdict for the
    /// epoch it currently holds.
    ///
    /// `None` when the question does not arise: this node is not the host, or it
    /// is in the Local-only regime, which has no gate. Otherwise
    /// [`Completeness::Recovering`](super::Completeness::Recovering) names, per
    /// writer, the watermark this host must still reach — and an **empty**
    /// `needed` is not "almost there", it is "no fresh majority has been read at
    /// all", which is the operator-visible difference between waiting on this
    /// node's own apply loop and waiting on its peers'.
    ///
    /// Observability only, and deliberately non-latching: a `Complete` here is
    /// the same verdict [`fence`](Self::fence) would take and hold, but reading
    /// it does not take it. Decisions go through [`fence`](Self::fence) or
    /// [`publish`](Self::publish).
    ///
    /// # The one stall this cannot clear by itself
    ///
    /// A target names a *writer*, and reaching it means applying that writer's
    /// feed. A predecessor that membership has already **reaped** can no longer
    /// be drained — its entries are gone, and the session tier stops scanning a
    /// member it no longer knows — so a host whose follower loop was down across
    /// a whole migration can find a target it cannot replay its way to. The
    /// remedy is the one the ring's boundedness already documents: remediate
    /// coarsely from the consumer's own authority and record the target with
    /// [`CommitLedger::record`], which is the consumer asserting the coverage the
    /// tier cannot verify. This accessor is what makes that stall diagnosable
    /// rather than mysterious.
    #[must_use]
    pub fn recovery(&self) -> Option<super::Completeness> {
        let committed = self.committed.as_ref()?;
        let lead = self.group.leadership();
        if lead.host.as_ref() != Some(&self.me) {
            return None;
        }
        if self.lock().recovered_at == Some(lead.epoch) {
            return Some(super::Completeness::Complete);
        }
        Some(CompletenessCore::step(
            lead.epoch,
            &self.roster_view(&committed.voters),
            &committed.ledger.watermarks(),
        ))
    }

    /// Publishes `op` into this node's hosted feed, resolving to the write's
    /// [`WriteToken`] — whose `epoch` **is** the leadership epoch.
    ///
    /// This is the [`Commit::Local`] path: it returns as soon as the write is in
    /// the feed, with no wait on anybody. In the committed regime it also
    /// records the write into this node's own [`CommitLedger`] (see the type
    /// docs), which both makes the host count toward its own writes' commits and
    /// keeps its reading fresh for the next host's recovery.
    ///
    /// **Call it *after* the local durable write**, which is the [`WriteFeed`]
    /// contract this path inherits and the reason the self-record above is
    /// honest: the ledger entry it publishes asserts that this node has applied
    /// the write, so a caller that publishes first and applies second is
    /// counting a witness that does not yet exist. See the type docs.
    ///
    /// # Errors
    /// [`HostedError::NotHost`] (with `host: None` as the promised `NoLeader`),
    /// [`HostedError::Deposed`], or [`HostedError::Recovering`] — see
    /// the admission table in this module's source.
    /// [`HostedError::Rejected`] is **not** produced here:
    /// the feed is best-effort under the group actor's backpressure and
    /// re-carries a dropped advertisement on the next publish (the ring is
    /// state, not a log), so there is no enqueue failure for this path to
    /// report. The variant is reserved for a write path that cannot make that
    /// promise.
    pub async fn publish(&self, op: &K) -> Result<WriteToken, HostedError> {
        let epoch = self.admit_now()?;
        let token = self.feed_for(epoch).publish(op).await;
        if let Some(committed) = &self.committed {
            committed.ledger.record(&self.me, token).await;
        }
        Ok(token)
    }

    /// Publishes `op` and waits, bounded by `timeout`, for `level`'s threshold.
    ///
    /// The returned [`CommitReceipt`] always carries the token — the write *is*
    /// in the feed whatever the outcome — so check
    /// [`is_committed`](CommitReceipt::is_committed) before acknowledging
    /// anything to a client. The three outcomes are the three honest endings:
    ///
    /// * [`CommitOutcome::Committed`] — the level's threshold was met.
    /// * [`CommitOutcome::Deposed`] — a peer holds a higher epoch, so no further
    ///   reading can ever count this write. Not committed.
    /// * [`CommitOutcome::TimedOut`] — the deadline passed with the threshold
    ///   unmet, naming the members still being waited on. **No guarantee
    ///   holds**; the write may yet land everywhere, or be lost at the next
    ///   migration. A voter that appears here every time is voting without
    ///   applying, which is the deployment contract being broken and is exactly
    ///   what this outcome exists to make loud.
    ///
    /// The verdict is evaluated **before** the deposition check on every turn,
    /// and that order is deliberate: a write whose majority has already closed
    /// at this epoch is committed even if this node is being fenced out in the
    /// same instant — the intersection argument guarantees the successor's
    /// recovery carries it — so reporting `Deposed` there would be a false
    /// negative on a write that is genuinely durable.
    ///
    /// # Errors
    /// As [`publish`](Self::publish): the error surface is refusal *before* the
    /// write entered the feed. Anything that happens afterwards is an outcome,
    /// not an error.
    pub async fn publish_committed(
        &self,
        op: &K,
        level: Commit,
        timeout: Duration,
    ) -> Result<CommitReceipt, HostedError> {
        let token = self.publish(op).await?;
        let outcome = self.await_commit(token, level, timeout).await;
        Ok(CommitReceipt { token, outcome })
    }

    /// The wait loop: poll the gossiped ledgers at [`COMMIT_POLL`] until the
    /// rule says committed, the watch says deposed, or the caller's deadline
    /// passes.
    async fn await_commit(
        &self,
        token: WriteToken,
        level: Commit,
        timeout: Duration,
    ) -> CommitOutcome {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let waiting_on = match CommitCore::evaluate(&self.view(level), &self.me, token, level) {
                CommitVerdict::Committed => return CommitOutcome::Committed,
                CommitVerdict::Pending { waiting_on } => waiting_on,
            };
            // Only a *pending* write cares about the fence: see the ordering
            // note on `publish_committed`.
            let lead = self.group.leadership();
            if lead.epoch != token.epoch || lead.host.as_ref() != Some(&self.me) {
                return CommitOutcome::Deposed { epoch: token.epoch };
            }
            if tokio::time::Instant::now() >= deadline {
                return CommitOutcome::TimedOut { waiting_on };
            }
            tokio::time::sleep(COMMIT_POLL).await;
        }
    }

    /// The admission verdict against the current watch, arming both latches on
    /// the way through.
    fn admit_now(&self) -> Result<u64, HostedError> {
        let lead = self.group.leadership();
        let hosting = lead.host.as_ref() == Some(&self.me);
        // Computed outside the state lock: the recovery verdict sweeps the
        // roster's gossiped readings, and nothing here may hold a std mutex
        // across work the rest of the handle waits on.
        let recovered = !hosting || self.recovered_for(lead.epoch);
        let last_hosted = {
            let mut state = self.lock();
            if hosting {
                // Recorded whether or not service has opened: a node elected at
                // `e` that is fenced out while still recovering *was* the host
                // of `e`, and must be told `Deposed` rather than redirected.
                state.last_hosted = Some(lead.epoch);
            }
            state.last_hosted
        };
        let verdict = admit(&lead, &self.me, last_hosted, recovered);
        if let Ok(epoch) = verdict {
            // The serving latch, published for a bound subscriber: *this* is the
            // instant everything below `epoch` became dead state, and it is the
            // earliest honest one — a `Recovering` host never reaches here, so a
            // still-draining node can never cut the tail it is being measured
            // against. Relaxed: the epoch is the whole message.
            self.serving.fetch_max(epoch, Ordering::Relaxed);
        }
        verdict
    }

    /// Whether the recovery gate is open for `epoch` — vacuously true in the
    /// Local-only regime, latched once true in the committed one.
    fn recovered_for(&self, epoch: u64) -> bool {
        let Some(committed) = &self.committed else {
            return true;
        };
        if self.lock().recovered_at == Some(epoch) {
            return true;
        }
        let view = self.roster_view(&committed.voters);
        let own = committed.ledger.watermarks();
        if !CompletenessCore::step(epoch, &view, &own).is_complete() {
            return false;
        }
        self.lock().recovered_at = Some(epoch);
        true
    }

    /// The view [`CommitCore`] is handed for `level`.
    fn view(&self, level: Commit) -> Vec<LedgerView> {
        match level {
            // The core commits on sight; assembling a view would be waste.
            Commit::Local => Vec::new(),
            Commit::QuorumApplied => self
                .committed
                .as_ref()
                .map(|committed| self.roster_view(&committed.voters))
                .unwrap_or_default(),
            Commit::AllApplied => self.selected_view(),
        }
    }

    /// The **whole** static voter roster, silent voters included as
    /// [`LedgerView::reading`] `None` — the denominator both rules derive their
    /// majority from, and the one place omitting a member would manufacture a
    /// majority out of a minority.
    fn roster_view(&self, voters: &VoterRoster) -> Vec<LedgerView> {
        voters
            .iter()
            .map(|member| LedgerView {
                member: member.clone(),
                alive: self.group.member_status(member) == Some(Status::Alive),
                reading: commit_reading_named(&self.name, &self.group, member),
            })
            .collect()
    }

    /// [`Commit::AllApplied`]'s rumour-derived set: every **other** member this
    /// node currently believes `Alive` that advertises [`CAP_HOSTED`].
    ///
    /// This node is excluded because it is the author, not a witness. The
    /// capability selector carries the ack tier's rolling-upgrade footgun
    /// verbatim: a peer that runs the follower loop but has not advertised yet
    /// is invisible here and **silently skipped**, so the guarantee weakens
    /// quietly instead of failing loudly. Advertise fleet-wide and confirm the
    /// advertisements have landed before relying on this level. An empty
    /// selection resolves immediately — a real, if weak, answer, and the same
    /// one [`applied_by_selected`](crate::applied_by_selected) gives.
    fn selected_view(&self) -> Vec<LedgerView> {
        self.group
            .statuses()
            .into_iter()
            .filter(|(member, status)| {
                *status == Status::Alive
                    && *member != self.me
                    && self.group.node_has_capability(member, CAP_HOSTED)
            })
            .map(|(member, _)| LedgerView {
                reading: commit_reading_named(&self.name, &self.group, &member),
                member,
                alive: true,
            })
            .collect()
    }

    /// The feed life for `epoch`, started fresh (ring empty, sequences from 1)
    /// the first time this node hosts at it.
    fn feed_for(&self, epoch: u64) -> Arc<WriteFeed<K>> {
        let mut state = self.lock();
        if state.feed.is_none() || state.feed_epoch != epoch {
            let encode = Arc::clone(&self.encode);
            state.feed = Some(Arc::new(
                WriteFeed::named(
                    &self.feed_name,
                    self.group.clone(),
                    self.capacity,
                    move |key: &K| encode(key),
                )
                .with_epoch(epoch),
            ));
            state.feed_epoch = epoch;
        }
        Arc::clone(state.feed.as_ref().expect("armed just above"))
    }
}

impl<K> HostedWrites<K> {
    /// Binds `reads` to this write path's **serving epoch**, so the subscriber
    /// cuts its own lineage the instant this node is admitted to serve. One
    /// builder step, before the follower loop takes the handle:
    ///
    /// ```no_run
    /// # use groupnet_consistency::hosted::{HostedReads, HostedWrites};
    /// # fn demo(writes: &HostedWrites<String>, mut reads: HostedReads<String>) {
    /// writes.bind(&mut reads);
    /// # }
    /// ```
    ///
    /// # What it fixes
    ///
    /// [`HostedReads`](super::HostedReads) excludes this node's own feed, so a
    /// node that is itself the host never sees a write of its own lineage — and
    /// the *first delivered write of the new lineage*, which is what closes the
    /// old one everywhere else, therefore never arrives. Without this (or a
    /// hand-placed [`HostedReads::cut_below`](super::HostedReads::cut_below))
    /// the predecessor's un-replicated tail — gossiped state, which can land
    /// long after a partition heals — is delivered to the **serving** host and
    /// applied behind its own writes: a fenced epoch-`e` write reordered after
    /// the authority's own epoch-`e′` ones.
    ///
    /// # Why the latch is the honest signal
    ///
    /// It moves exactly when the admission table admits this node to serve —
    /// recovery latched, [`fence`](Self::fence) `Some`, [`publish`](Self::publish)
    /// accepted — and never while it answers
    /// [`Recovering`](HostedError::Recovering). That matters in both directions:
    /// a recovering host **needs** the predecessor's tail (it is what the
    /// recovery rule measures it against), and a serving host must not have it.
    /// Leadership alone cannot tell those apart; admission can, and it is the
    /// only thing that can.
    ///
    /// Cheap and passive: one shared `AtomicU64`, read at the top of each turn
    /// of [`HostedReads::next`](super::HostedReads::next). Binding a subscriber
    /// on a node that never hosts costs an atomic load per event and cuts
    /// nothing, so it is safe to wire unconditionally — which is what the
    /// example and the tier's own tests do.
    pub fn bind<R>(&self, reads: &mut super::HostedReads<R>) {
        reads.bind_serving(Arc::clone(&self.serving));
    }

    /// The mutable half. Poisoning is irrelevant: every mutation is a
    /// whole-field update, so a panicking holder leaves it consistent — the same
    /// reasoning the lease tier's core lock records.
    fn lock(&self) -> MutexGuard<'_, State<K>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The group's static voter roster, or why it has none.
fn quorum_roster(group: &Group) -> Result<VoterRoster, HostedSetupError> {
    let GroupMode::Hosted(cfg) = &group.config().mode else {
        return Err(HostedSetupError::NotHosted);
    };
    let Activation::Quorum { voters } = &cfg.activation else {
        return Err(HostedSetupError::NotQuorum);
    };
    Ok(voters.clone())
}

#[cfg(test)]
mod tests {
    use groupnet_core::NodeId;
    use groupnet_runtime::{Leadership, Role};

    use super::{HostedSetupError, admit, hosted_feed_name};
    use crate::hosted::HostedError;

    fn node(name: &str) -> NodeId {
        NodeId::new(name)
    }

    fn me() -> NodeId {
        node("me")
    }

    /// The leadership watch's report, built the way the driver builds it — so a
    /// row of the table below can never disagree with the runtime about what
    /// `Role::Host` means.
    fn lead(epoch: u64, host: Option<&str>) -> Leadership {
        let host = host.map(node);
        let role = if host.as_ref() == Some(&me()) {
            Role::Host
        } else {
            Role::Follower
        };
        Leadership { epoch, host, role }
    }

    #[test]
    fn the_admission_table() {
        type Case = (
            &'static str,
            Leadership,
            Option<u64>,
            bool,
            Result<u64, HostedError>,
        );
        let cases: Vec<Case> = vec![
            (
                "the host of a recovered epoch serves it",
                lead(7, Some("me")),
                Some(7),
                true,
                Ok(7),
            ),
            (
                "…and a host that has never hosted before is no different",
                lead(7, Some("me")),
                None,
                true,
                Ok(7),
            ),
            (
                "the host of an unrecovered epoch is elected, not serving",
                lead(7, Some("me")),
                None,
                false,
                Err(HostedError::Recovering),
            ),
            (
                "a node that never held the group is redirected",
                lead(7, Some("peer")),
                None,
                true,
                Err(HostedError::NotHost {
                    epoch: 7,
                    host: Some(node("peer")),
                }),
            ),
            (
                "…and with no host at all, that redirect is the promised NoLeader",
                lead(7, None),
                None,
                true,
                Err(HostedError::NotHost {
                    epoch: 7,
                    host: None,
                }),
            ),
            (
                "a host succeeded by a peer is deposed from its own epoch",
                lead(9, Some("peer")),
                Some(7),
                true,
                Err(HostedError::Deposed { epoch: 7 }),
            ),
            (
                "…and a host that lapsed into (e, None) likewise: it is fenced, \
                 not merely hostless",
                lead(7, None),
                Some(7),
                true,
                Err(HostedError::Deposed { epoch: 7 }),
            ),
            (
                "a host elected, deposed, and elected again serves the new epoch",
                lead(9, Some("me")),
                Some(7),
                true,
                Ok(9),
            ),
            (
                "the recovery gate binds a re-elected host too",
                lead(9, Some("me")),
                Some(7),
                false,
                Err(HostedError::Recovering),
            ),
            (
                "before any election, a fresh node is a NoLeader at epoch zero",
                lead(0, None),
                None,
                true,
                Err(HostedError::NotHost {
                    epoch: 0,
                    host: None,
                }),
            ),
        ];
        for (name, leadership, last_hosted, recovered, expected) in cases {
            assert_eq!(
                admit(&leadership, &me(), last_hosted, recovered),
                expected,
                "{name}"
            );
        }
    }

    /// The gate is only ever consulted for the node's *own* hostship, so a
    /// follower's verdict cannot depend on it — the property that lets
    /// `admit_now` skip the roster sweep entirely when this node is not host.
    #[test]
    fn the_recovery_gate_never_changes_a_non_hosts_verdict() {
        for leadership in [lead(4, Some("peer")), lead(4, None)] {
            for last_hosted in [None, Some(3)] {
                assert_eq!(
                    admit(&leadership, &me(), last_hosted, true),
                    admit(&leadership, &me(), last_hosted, false),
                    "{leadership:?} / {last_hosted:?}"
                );
            }
        }
    }

    #[test]
    fn write_paths_map_to_distinct_feeds() {
        assert_eq!(hosted_feed_name(""), "hosted");
        assert_eq!(hosted_feed_name("docs"), "hosted:docs");
        assert_ne!(hosted_feed_name("a"), hosted_feed_name("b"));
        // Distinct from the session tier's default feed: a hosted path and a
        // plain one coexist on one node.
        assert_ne!(hosted_feed_name(""), "");
    }

    #[test]
    fn setup_errors_say_which_configuration_is_wrong() {
        assert!(
            HostedSetupError::NotHosted
                .to_string()
                .contains("not a Hosted group")
        );
        assert!(
            HostedSetupError::NotQuorum
                .to_string()
                .contains("static voter roster")
        );
        let mismatch = HostedSetupError::LedgerMismatch {
            expected: "~hosted:applied:docs".to_owned(),
            found: "~hosted:applied".to_owned(),
        };
        assert_eq!(
            mismatch.to_string(),
            "the commit ledger publishes under ~hosted:applied, but this write path reads \
             ~hosted:applied:docs"
        );
        assert_ne!(mismatch, HostedSetupError::NotQuorum);
    }
}
