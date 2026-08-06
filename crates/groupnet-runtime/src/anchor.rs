//! The **external CAS anchor**, driver side: the trait a deployment implements
//! to give an [`Activation::External`] group its linearizable register, and the
//! per-group task that turns the engine's prompts into conditional writes.
//!
//! The engine holds no connection, no etag and no wall clock, so it *asks*:
//! [`Effect::AnchorClaimDue`] is a prompt, and the answer comes back as
//! [`Command::AnchorActivated`] or [`Command::AnchorObserved`]. Everything in
//! between — which precondition to write under, which epoch to bid, whether an
//! expired record may be stolen, whether an ambiguous write applied — is
//! decided by the pure functions in [`groupnet_core::anchor`], so this task and
//! the deterministic simulator run one copy of the rules and differ only in
//! what performs the I/O.
//!
//! # There is no write-ahead half here
//!
//! [`GrantStore`](crate::GrantStore) exists because a `Quorum` voter's promise
//! lives in its own memory and a restart forgets it; the driver therefore
//! blocks on the persist and withholds the frame it precedes. **Nothing of that
//! shape applies here, and none is coming.** The anchor *is* the ledger: an
//! epoch exists only because a conditional write created it, so there is
//! nothing local to write ahead of, no recovery constructor, and no boot
//! blackout. The two effects behave oppositely on purpose —
//! [`Effect::PersistGrant`](groupnet_core::Effect::PersistGrant) blocks the
//! group actor by contract, [`Effect::AnchorClaimDue`] must never block it at
//! all (see [`anchor_task`] for the debounce that guarantees it).
//!
//! [`Activation::External`]: groupnet_core::Activation::External
//! [`Effect::AnchorClaimDue`]: groupnet_core::Effect::AnchorClaimDue

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use groupnet_core::anchor::{
    AnchorRecord, ClaimPlan, ambiguous_applied, plan_claim, renewal_record,
};
use groupnet_core::{Command, NodeId, Role, Time};
use tokio::sync::{mpsc, watch};

use crate::driver::{Event, now_since};
use crate::group::Leadership;

/// A future returned by an [`Anchor`] method.
///
/// Boxed so the trait stays **dyn-compatible**: a group is configured with an
/// `Arc<dyn Anchor>`, which rules out the return-position `impl Future` shape
/// [`Transport`](groupnet_transport::Transport) uses. One allocation per store
/// round trip is not a cost worth a dependency on `async-trait` (which boxes
/// the same way) — and an anchor round happens at most once per anti-entropy
/// interval, against a network round trip that dwarfs it.
pub type AnchorFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The store's opaque version marker for the anchor object: an S3/R2 `ETag`, a
/// GCS generation, an etcd `mod_revision`, a row version.
///
/// Groupnet never parses it, compares it for ordering, or gives it meaning — it
/// is carried from a [`load`](Anchor::load) back to the
/// [`store`](Anchor::store) that must be conditional on it, and nothing else.
/// A backend whose version is not a string renders it however it likes, as long
/// as the round trip is lossless.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AnchorToken(String);

impl AnchorToken {
    /// Wraps a store's version marker.
    #[must_use]
    pub fn new(token: impl Into<String>) -> Self {
        Self(token.into())
    }

    /// The marker, as the store rendered it.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The precondition one [`Anchor::store`] is issued under — the two conditional
/// writes every object store already offers, and the only two this tier needs.
///
/// Which one to use is never the driver's guess: it is what
/// [`plan_claim`](groupnet_core::anchor::plan_claim) decided, carried across
/// unchanged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AnchorWriteIf {
    /// Write only if the object does not exist yet — `If-None-Match: *`. The
    /// genesis write, and the one whose race a fresh cluster resolves.
    Absent,
    /// Write only if the object is still at this version — `If-Match: <etag>`.
    /// Both a supersede (the token came from the record being replaced) and a
    /// renewal (the token is the one this node's own last write returned).
    Matches(AnchorToken),
}

/// What a conditional write did.
///
/// Three outcomes, because the third is real: a `PUT` whose connection dropped
/// or whose deadline expired genuinely has no answer, and pretending otherwise
/// is how a node either abandons an epoch it holds or serves one it lost.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AnchorCas {
    /// Applied. The object now holds the record, at this version — which the
    /// next renewal writes against.
    Stored(AnchorToken),
    /// Refused: the precondition did not hold. Somebody else created the object
    /// first, or superseded the record this write was conditional on. A
    /// definite answer, and always "not you".
    Mismatch,
    /// **No answer.** The write may or may not have applied. The driver
    /// resolves it by reading the record back — see
    /// [`Anchor::store`] for why an implementation is free to answer this
    /// liberally.
    Unknown,
}

/// A linearizable compare-and-set register holding **one group's** anchor
/// record: the driver-side half of
/// [`Activation::External`](groupnet_core::Activation::External).
///
/// The shape is deliberately the smallest thing a real object store already
/// does — a read that returns a version marker, and a write conditional on one:
///
/// ```text
/// load()                  -> Option<(AnchorRecord, AnchorToken)>   // GET
/// store(Absent,  record)  -> Stored | Mismatch | Unknown           // PUT If-None-Match: *
/// store(Matches, record)  -> Stored | Mismatch | Unknown           // PUT If-Match: <etag>
/// ```
///
/// That is S3/R2/GCS reality with nothing added. A lock service with sessions,
/// a lease API or a compare-and-swap with a callback would be a second protocol
/// per backend; this one is written once, and a deployment on etcd or
/// `ZooKeeper` adapts *down* to it.
///
/// # One implementation, one object, one group
///
/// An implementation **closes over its object key**. Nothing here names the
/// group, because a group's anchor is configured on the
/// [`GroupProfile`](crate::GroupProfile) it is joined under — a process hosting
/// several `External` groups gives each its own `Anchor` over its own key. Two
/// groups sharing one key would fight over one epoch sequence, which no rule
/// here can detect.
///
/// The record itself is a handful of bytes ([`AnchorRecord`] is three fields);
/// encode it however you like, as long as a `load` returns what the last
/// `store` wrote.
///
/// # `Err` is [`Unknown`](AnchorCas::Unknown), and that is safe
///
/// A transport error from `store` — a timeout, a reset connection, a 500 —
/// is treated as `Unknown` by the driver, exactly as if the implementation had
/// returned it. So an implementation that **cannot distinguish a request that
/// never left from one that may have applied is still correct**: report the
/// error and the driver reads the record back, and
/// [`ambiguous_applied`](groupnet_core::anchor::ambiguous_applied) settles it
/// on evidence rather than on the implementation's guess. The cost of being
/// liberal with `Unknown` is one extra `load`; the cost of being liberal with
/// `Mismatch` is a node that abandons an epoch it actually holds.
///
/// An `Err` from `load` decides nothing at all: the round ends, no claim is
/// made, and the next prompt re-reads. A node that cannot reach its anchor
/// therefore cannot renew, and its engine lease lapses — the fail-closed
/// posture the whole tier rests on.
///
/// # No durability contract, unlike [`GrantStore`](crate::GrantStore)
///
/// There is no write-ahead rule to honour and no frame to withhold. The anchor
/// is the ledger: this node holds an epoch exactly while the object says so.
/// Blocking is *not* expected either — the driver awaits these futures on its
/// own task, never on the group actor, so a slow store costs failover latency
/// and never gossip.
///
/// ```no_run
/// use std::io;
/// use groupnet_core::anchor::AnchorRecord;
/// use groupnet_runtime::{Anchor, AnchorCas, AnchorFuture, AnchorToken, AnchorWriteIf};
///
/// /// One object in one bucket — the key is closed over, never passed in.
/// struct S3Anchor {
///     bucket: String,
///     key: String,
/// }
///
/// impl Anchor for S3Anchor {
///     fn load(&self) -> AnchorFuture<'_, io::Result<Option<(AnchorRecord, AnchorToken)>>> {
///         Box::pin(async move {
///             // GET self.key; decode the body; the response ETag is the token.
///             // A 404 is `Ok(None)` — an absent anchor is a state, not an error.
///             let _ = (&self.bucket, &self.key);
///             Ok(None)
///         })
///     }
///
///     fn store(
///         &self,
///         pre: AnchorWriteIf,
///         record: AnchorRecord,
///     ) -> AnchorFuture<'_, io::Result<AnchorCas>> {
///         Box::pin(async move {
///             // PUT with `If-None-Match: *` for `Absent` and `If-Match: <etag>`
///             // for `Matches`; 200/201 is `Stored(new_etag)`, 412 is
///             // `Mismatch`, and a timeout is `Unknown` (or just return the
///             // error — the driver reads back either way).
///             let _ = (pre, record);
///             Ok(AnchorCas::Unknown)
///         })
///     }
/// }
/// ```
pub trait Anchor: Send + Sync + 'static {
    /// Reads the anchor object: the record it holds and the version marker a
    /// conditional write against it must carry.
    ///
    /// `Ok(None)` means the object does not exist — a state, not a failure, and
    /// the one that [`AnchorWriteIf::Absent`] claims from.
    ///
    /// # Errors
    /// Whatever the underlying store reports. The driver treats an error as "no
    /// information": it makes no claim this round and re-reads on the next
    /// prompt.
    fn load(&self) -> AnchorFuture<'_, io::Result<Option<(AnchorRecord, AnchorToken)>>>;

    /// Writes `record` if `pre` holds, and reports which of the three things
    /// happened.
    ///
    /// # Errors
    /// Whatever the underlying store reports. An error is read as
    /// [`AnchorCas::Unknown`] — the write may have applied — and the driver
    /// resolves it with a read-back. Returning an error is therefore always
    /// *safe*; it is never a way to say "definitely not applied".
    fn store(
        &self,
        pre: AnchorWriteIf,
        record: AnchorRecord,
    ) -> AnchorFuture<'_, io::Result<AnchorCas>>;
}

/// Everything one group's anchor task is spawned with.
///
/// Built by `spawn_group` in `node.rs`, and only for a group that is both
/// [`External`](groupnet_core::Activation::External) **and** configured with an
/// anchor. An `External` group with no anchor spawns nothing and never claims.
pub(crate) struct AnchorTask {
    /// The register this group's epochs are allocated by.
    pub anchor: Arc<dyn Anchor>,
    /// The node whose hostship is being won.
    pub local: NodeId,
    /// Where [`Command::AnchorActivated`] / [`Command::AnchorObserved`] are
    /// fed back into the group actor.
    ///
    /// **Weak on purpose.** The group actor stops when its inbox closes, i.e.
    /// when every sender is dropped; a strong sender here would be one the
    /// actor itself keeps alive (it owns this task's prompt channel), so
    /// neither could ever end and a killed node would keep gossiping — and
    /// keep renewing its anchor record — for ever.
    pub commands: mpsc::WeakSender<Event>,
    /// [`Effect::AnchorClaimDue`](groupnet_core::Effect::AnchorClaimDue)'s
    /// epoch hints, one at a time — see [`anchor_task`] for why the capacity is
    /// the debounce.
    pub prompts: mpsc::Receiver<u64>,
    /// The group's published leadership, watched for the edge that releases a
    /// record this node is no longer host under.
    pub leadership: watch::Receiver<Leadership>,
    /// [`HostedConfig::lease_ms`](groupnet_core::HostedConfig::lease_ms): the
    /// record's TTL *and* the engine lease, which is one knob by design.
    pub lease_ms: u64,
    /// How far past a record's expiry a claimant waits before stealing.
    pub steal_margin_ms: u64,
    /// The node's logical-time origin — the same [`Instant`] the driver
    /// measures the engine's [`Time`] from.
    pub start: Instant,
}

/// Runs one group's anchor rounds until the group actor goes away.
///
/// # The prompt channel is the debounce
///
/// [`Effect::AnchorClaimDue`](groupnet_core::Effect::AnchorClaimDue) is a
/// *level* signal on the anti-entropy cadence, not an edge, because a prompt
/// lost to a busy driver must self-heal. The engine therefore keeps asking, and
/// the driver is required to debounce it against its own in-flight round. That
/// is what the **capacity-one channel plus `try_send`** is: a prompt arriving
/// while this task is mid-round finds the slot full (or is the one already
/// waiting) and is dropped, so a store round trip that outlives an anti-entropy
/// interval cannot stack claims and burn an epoch per prompt.
///
/// It also never blocks the group actor. That is the sharp difference from
/// [`Effect::PersistGrant`](groupnet_core::Effect::PersistGrant), which blocks
/// it *by contract* — a grant must not outrun its own durability. Here there is
/// nothing to be ahead of: the anchor decides, and until it answers this node
/// is not host, so making the actor wait would buy nothing and stall gossip.
///
/// # Ending
///
/// The task ends when the prompt sender drops, which happens when the group
/// actor returns. It holds only a [`mpsc::WeakSender`] back into that actor, so
/// nothing here keeps a dead node alive.
pub(crate) async fn anchor_task(task: AnchorTask) {
    let AnchorTask {
        anchor,
        local,
        commands,
        mut prompts,
        leadership,
        lease_ms,
        steal_margin_ms,
        start,
    } = task;

    // Two receivers on one channel: `edges` drives the select (which holds a
    // mutable borrow for as long as the branch futures live), while the
    // claimant reads the current snapshot inside a round.
    let mut edges = leadership.clone();
    let mut hosting = edges.borrow_and_update().role == Role::Host;
    let mut claimant = Claimant {
        anchor,
        local,
        commands,
        leadership,
        lease_ms,
        steal_margin_ms,
        start,
        held: None,
        retry_at_wall_ms: None,
    };

    loop {
        tokio::select! {
            prompt = prompts.recv() => {
                let Some(epoch_hint) = prompt else { return }; // the actor is gone
                claimant.round(epoch_hint).await;
            }
            changed = edges.changed() => {
                if changed.is_err() {
                    return; // the actor dropped its publishers
                }
                let now_hosting = edges.borrow_and_update().role == Role::Host;
                // Host → not host: the engine has given the group up (lease
                // lapse, a deposing pair, or a voluntary leave), so the record
                // this node is still named in should stop blocking succession.
                if hosting && !now_hosting {
                    claimant.release().await;
                    // Discard a prompt issued *while this node was host*. The
                    // slot can hold a renewal prompt the actor emitted before
                    // it demoted, and running it now would take the claim path
                    // — where `plan_claim` finds a record naming this node,
                    // dutifully supersedes it, and undoes the release that
                    // just happened. A prompt the engine issues *after* the
                    // demotion is a real re-claim and still arrives.
                    let _ = prompts.try_recv();
                }
                hosting = now_hosting;
            }
        }
    }
}

/// The anchor record this node has won and has not been told it lost.
///
/// The token is what makes a renewal a renewal — a holder with its etag extends
/// the epoch it has; a holder that has lost it must re-plan through
/// [`plan_claim`], which bids strictly higher.
#[derive(Clone)]
struct Held {
    /// The epoch the anchor awarded.
    epoch: u64,
    /// The version this node's own last write returned.
    token: AnchorToken,
    /// The expiry that write stamped, on the wall clock — the outer bound on
    /// how long [`Posture::Wait`] may hold this round back.
    expires_at_wall_ms: u64,
}

/// What one prompted round is allowed to do — see
/// [`Claimant::posture`](Claimant::posture) for the rule.
enum Posture {
    /// Extend the epoch this hold names.
    Renew(Held),
    /// Go win one.
    Claim,
    /// Neither: the hold and the engine's belief have not converged yet.
    Wait,
}

/// Whether the write being executed extends an epoch this node already holds.
/// Only the meaning of [`AnchorCas::Mismatch`] differs between the two.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Renewing {
    /// A renewal: a mismatch means somebody superseded us.
    Yes,
    /// A claim: a mismatch means we lost a race and decides nothing.
    No,
}

/// One group's anchor rounds, and the little state that survives between them.
struct Claimant {
    anchor: Arc<dyn Anchor>,
    local: NodeId,
    commands: mpsc::WeakSender<Event>,
    leadership: watch::Receiver<Leadership>,
    lease_ms: u64,
    steal_margin_ms: u64,
    start: Instant,
    /// The record this node holds, if any.
    held: Option<Held>,
    /// The wall-clock instant a yielded round may next attempt a steal —
    /// [`ClaimPlan::Yield`]'s hint. Prompts before it are dropped without
    /// touching the store, which is what stops a whole cluster hammering an
    /// anchor it cannot win.
    retry_at_wall_ms: Option<u64>,
}

impl Claimant {
    /// One prompted round: renew what we hold, or go win an epoch.
    ///
    /// # Both clocks are read before any I/O
    ///
    /// `t0` is the engine's logical [`Time`] — the very
    /// [`now_since`](crate::driver::now_since) the driver feeds `on_tick`, so a
    /// `lease_until` derived from it is directly comparable to the engine's own
    /// lapse check. `w0` is absolute wall-clock milliseconds, the base the
    /// anchor record's `expires_at_wall_ms` is judged against by *other* nodes'
    /// clocks.
    ///
    /// Both are sampled **before the load**, not after the write returns. The
    /// record's expiry is computed from `w0`, and the engine lease from `t0`,
    /// so a lease anchored at round initiation always expires no later than the
    /// record it was earned from. Anchoring after the round would hand this
    /// node an overhang past its own record — precisely the window a
    /// successor's steal is entitled to use. (Quorum's send-instant attribution
    /// argument, in a different dress.)
    async fn round(&mut self, epoch_hint: u64) {
        let t0 = now_since(self.start);
        let w0 = wall_ms();

        match self.posture(w0) {
            Posture::Renew(held) => {
                self.renew(held, t0, w0).await;
                return;
            }
            Posture::Wait => return,
            Posture::Claim => {}
        }
        if self.retry_at_wall_ms.is_some_and(|at| w0 < at) {
            return; // a live record we are not entitled to: nothing to ask yet
        }
        self.retry_at_wall_ms = None;
        self.claim(epoch_hint, t0, w0).await;
    }

    /// What this round may do, from the hold and the published leadership
    /// **together**. Neither alone is enough, and the two failure modes they
    /// guard are opposite ones:
    ///
    /// * The **hold** is what blocks the claim path. A round ends by feeding
    ///   [`Command::AnchorActivated`] into a bounded inbox the actor has not
    ///   necessarily drained yet, so the watch is routinely one hop behind this
    ///   task. Letting a holder fall through to `plan_claim` there would have
    ///   it dutifully supersede its *own* record and burn an epoch per round.
    /// * **Leadership** is what licenses the renewal. A node the engine has
    ///   demoted — a lapsed lease, a deposing pair, or a
    ///   [`Leave`](groupnet_core::Command::Leave) — must not renew, and above
    ///   all must not re-report an activation, because that would hand the
    ///   group straight back to a node that has just given it up.
    ///
    /// So a hold the engine is not showing yields [`Posture::Wait`]: this round
    /// does nothing at all. That is the right answer for both readings of it —
    /// the actor is one hop behind (the next prompt resolves it) or the release
    /// edge is on its way (and claiming would undo it).
    ///
    /// **The wait is bounded by the record itself.** If the engine never
    /// adopted the activation — a report `try_send` dropped under load — the
    /// hold lapses when the record it is based on does, and this node re-claims
    /// from scratch at a strictly higher epoch. One lease is the worst the lost
    /// report costs, and it costs it without a special case.
    fn posture(&mut self, w0: u64) -> Posture {
        let Some(held) = self.held.clone() else {
            return Posture::Claim;
        };
        let lead = self.leadership.borrow().clone();
        if lead.epoch > held.epoch {
            // A strictly better adopted pair: the hold is spent whatever the
            // store still says.
            self.held = None;
            return Posture::Claim;
        }
        if lead.epoch == held.epoch && lead.host.as_ref() == Some(&self.local) {
            return Posture::Renew(held);
        }
        if w0 >= held.expires_at_wall_ms {
            self.held = None;
            return Posture::Claim;
        }
        Posture::Wait
    }

    /// Extend the epoch this node already holds, on the pacing floor below.
    async fn renew(&mut self, held: Held, t0: Time, w0: u64) {
        // The pacing floor. The engine prompts on the anti-entropy cadence,
        // which is the *ceiling* on how often a renewal may happen; this is the
        // floor on how late it may be left. Half a lease leaves a full half
        // spare for a slow round trip, a dropped report, or a retry — and stops
        // a brisk gossip interval turning into a store write every few
        // milliseconds.
        if held.expires_at_wall_ms.saturating_sub(w0) > self.lease_ms / 2 {
            return;
        }
        let record = renewal_record(&self.local, held.epoch, w0, self.lease_ms);
        self.write(
            AnchorWriteIf::Matches(held.token),
            record,
            t0,
            Renewing::Yes,
        )
        .await;
    }

    /// Read the anchor and act on [`plan_claim`]'s verdict.
    async fn claim(&mut self, epoch_hint: u64, t0: Time, w0: u64) {
        let Ok(loaded) = self.anchor.load().await else {
            // An unreachable anchor decides nothing: no claim, no observation,
            // no host. The next prompt re-reads, and if this persists the
            // engine lease lapses and this node steps down.
            return;
        };
        let (record, token) = match loaded {
            Some((record, token)) => (Some(record), Some(token)),
            None => (None, None),
        };
        match plan_claim(
            &self.local,
            epoch_hint,
            record.as_ref(),
            w0,
            self.lease_ms,
            self.steal_margin_ms,
        ) {
            ClaimPlan::Yield { retry_at_wall_ms } => {
                self.retry_at_wall_ms = Some(retry_at_wall_ms);
                // Teach the engine who does hold it. Fence-ordered, not
                // liveness-ordered: an expired record still names its holder
                // until somebody supersedes it.
                if let Some(record) = &record {
                    self.observe(record);
                }
            }
            ClaimPlan::Create(record) => {
                self.write(AnchorWriteIf::Absent, record, t0, Renewing::No)
                    .await;
            }
            ClaimPlan::Supersede(record) => {
                // Unreachable: `plan_claim` supersedes only a record it was
                // given, which arrived with its token.
                let Some(token) = token else { return };
                self.write(AnchorWriteIf::Matches(token), record, t0, Renewing::No)
                    .await;
            }
        }
    }

    /// Perform one conditional write and turn its outcome into engine input.
    async fn write(
        &mut self,
        pre: AnchorWriteIf,
        record: AnchorRecord,
        t0: Time,
        renewing: Renewing,
    ) {
        // Kept whole rather than as its epoch: an ambiguous outcome is resolved
        // against the *record* that was attempted, because a renewal's
        // `(epoch, host)` is byte-identical to the one it replaces and only the
        // expiry can tell the two apart. See [`ambiguous_applied`].
        let attempted = record.clone();
        match self.anchor.store(pre, record).await {
            Ok(AnchorCas::Stored(token)) => {
                self.won(attempted.epoch, token, attempted.expires_at_wall_ms, t0);
            }
            Ok(AnchorCas::Mismatch) => {
                if renewing == Renewing::Yes {
                    // A renewal that mismatches is not a lost race: the etag we
                    // wrote against is gone, so somebody superseded us. Hard
                    // signal — abdicate and learn who took it.
                    self.superseded().await;
                }
                // A *claim* that mismatches lost a race and decides nothing.
                // The next prompt re-reads and re-plans against whatever is
                // actually there now.
            }
            Ok(AnchorCas::Unknown) | Err(_) => self.resolve(&attempted, t0).await,
        }
    }

    /// Record a won epoch and report it to the engine.
    fn won(&mut self, epoch: u64, token: AnchorToken, expires_at_wall_ms: u64, t0: Time) {
        self.held = Some(Held {
            epoch,
            token,
            expires_at_wall_ms,
        });
        self.feed(Command::AnchorActivated {
            epoch,
            lease_until: t0.saturating_add(self.lease_ms),
        });
    }

    /// Abdicate: the record we held is somebody else's now.
    async fn superseded(&mut self) {
        self.held = None;
        if let Ok(Some((record, _))) = self.anchor.load().await {
            self.observe(&record);
        }
    }

    /// Settle an ambiguous write by reading the record back.
    ///
    /// The verdict is [`ambiguous_applied`]'s and not this task's: applied
    /// **iff** the object now holds exactly the record that was attempted —
    /// the whole record, expiry included, because a *renewal* attempts the
    /// `(epoch, host)` pair that is already standing and only its expiry says
    /// whether the write landed. Everything else — including a read-back that
    /// fails in its own right — reads as *not applied*, which is the
    /// fail-closed direction: an ambiguous round costs a re-plan, never a
    /// hostship this node did not win, and never a lease extension it did not
    /// earn.
    ///
    /// That last clause is the one a real store exercises: a deployment whose
    /// `PUT`s fail while its `GET`s work (a write throttle, a read-only window,
    /// expired write credentials) reports `Unknown` for every renewal, and a
    /// pair-only verdict would call each of them a win — extending this node's
    /// engine lease indefinitely while the record it is supposedly renewing
    /// ages out and a rival becomes entitled to steal it.
    ///
    /// A win resolved here takes its lease from `t0`, the instant the round
    /// *began*, exactly as an unambiguous one does. The round trip was longer,
    /// not the authority.
    async fn resolve(&mut self, attempted: &AnchorRecord, t0: Time) {
        let Ok(loaded) = self.anchor.load().await else {
            self.held = None;
            return;
        };
        if ambiguous_applied(&self.local, attempted, loaded.as_ref().map(|(r, _)| r)) {
            let (record, token) =
                loaded.expect("`ambiguous_applied` matches a present record only");
            self.won(attempted.epoch, token, record.expires_at_wall_ms, t0);
            return;
        }
        // Lost, or never sent — the yield posture either way.
        self.held = None;
        if let Some((record, _)) = &loaded {
            self.observe(record);
        }
    }

    /// Stamp the record this node no longer hosts under as already expired, so
    /// a successor may take it after `steal_margin_ms` instead of waiting out a
    /// whole TTL.
    ///
    /// Best-effort, and failures are **ignored on purpose**: a release is a
    /// courtesy that shortens succession, never a step the safety of anything
    /// depends on. A node that cannot reach the anchor to release (which is
    /// usually *why* it demoted) simply leaves the record to lapse, which is
    /// the same outcome one TTL later.
    ///
    /// The epoch is unchanged — a release decides nothing, so it allocates
    /// nothing — and the write is still conditional on our own etag, so a
    /// successor that has already superseded us cannot have its record
    /// clobbered by a late release.
    async fn release(&mut self) {
        let Some(held) = self.held.take() else { return };
        let record = AnchorRecord {
            epoch: held.epoch,
            host: self.local.clone(),
            expires_at_wall_ms: wall_ms(),
        };
        let _ = self
            .anchor
            .store(AnchorWriteIf::Matches(held.token), record)
            .await;
    }

    /// Report a record this node read and did not win.
    fn observe(&self, record: &AnchorRecord) {
        self.feed(Command::AnchorObserved {
            epoch: record.epoch,
            host: record.host.clone(),
        });
    }

    /// Hand one command to the group actor.
    ///
    /// `try_send` into the actor's bounded inbox, never a blocking send: this
    /// task must not be able to stall on an actor that is itself waiting on
    /// something. A dropped report is **self-healing** — the engine prompts
    /// again on the next anti-entropy cadence, an activation is re-reported by
    /// [`renew`](Self::renew), and an observation is re-read by the next claim
    /// round. An upgrade that fails means the actor has already stopped, and
    /// this task is about to notice.
    fn feed(&self, cmd: Command) {
        let Some(commands) = self.commands.upgrade() else {
            return;
        };
        let _ = commands.try_send(Event::Local(cmd));
    }
}

/// Absolute wall-clock milliseconds — the base every `expires_at_wall_ms` in an
/// anchor record is written and judged on.
///
/// Deliberately **not** the engine's [`Time`]: that one is process-local and
/// means nothing to the node that reads this record after a round trip through
/// a store. A clock set before the Unix epoch reads as 0, which makes every
/// record it writes instantly stealable — fail-safe, and a symptom nobody could
/// miss.
#[expect(
    clippy::cast_possible_truncation,
    reason = "u64 milliseconds since 1970 overflow in the year 584942417"
)]
fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::{AnchorCas, AnchorToken, AnchorWriteIf, wall_ms};

    /// The token is carried, never interpreted: whatever a store said comes
    /// back out unchanged, including shapes no parser would accept.
    #[test]
    fn a_token_round_trips_whatever_the_store_called_it() {
        for raw in ["\"deadbeef\"", "W/\"weak\"", "17", "", "a b\tc"] {
            assert_eq!(AnchorToken::new(raw).as_str(), raw);
        }
        assert_eq!(AnchorToken::new("x"), AnchorToken::new(String::from("x")));
        assert_ne!(AnchorToken::new("x"), AnchorToken::new("y"));
    }

    /// The two preconditions are distinguished by the token they carry, so a
    /// driver can never confuse "create" with "replace the version I read".
    #[test]
    fn the_write_preconditions_are_distinct() {
        let matches = AnchorWriteIf::Matches(AnchorToken::new("e1"));
        assert_ne!(AnchorWriteIf::Absent, matches);
        assert_eq!(matches, AnchorWriteIf::Matches(AnchorToken::new("e1")));
        assert_ne!(matches, AnchorWriteIf::Matches(AnchorToken::new("e2")));
    }

    /// A stored outcome is only equal to itself at the same version — the
    /// property a renewal's etag bookkeeping rests on.
    #[test]
    fn a_stored_outcome_carries_the_new_version() {
        let stored = AnchorCas::Stored(AnchorToken::new("e2"));
        assert_ne!(stored, AnchorCas::Mismatch);
        assert_ne!(stored, AnchorCas::Unknown);
        assert_eq!(stored, AnchorCas::Stored(AnchorToken::new("e2")));
    }

    /// The wall clock is a real one: past the epoch, and non-decreasing across
    /// two reads. (Not a *monotonic* clock — `SystemTime` can step — which is
    /// exactly why `steal_margin_ms` exists.)
    #[test]
    fn the_wall_clock_reads_absolute_milliseconds() {
        let first = wall_ms();
        assert!(
            first > 1_700_000_000_000,
            "a plausible wall clock is past 2023: {first}"
        );
        assert!(wall_ms() >= first);
    }
}
