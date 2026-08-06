//! The tokio shell around the two sans-IO cores: [`Leases`], this node's
//! participation in one lease set, and [`LeaseView`], the read handle a
//! request path holds.
//!
//! Everything here is glue. The rules live next door in
//! [`LeaseCore`] (reader) and
//! [`CoherenceCore`] (writer); this module supplies the
//! three things a sans-IO core cannot supply itself — a clock, group entries,
//! and tasks to move between them.
//!
//! # The three tasks
//!
//! [`Leases::new`] spawns one task per half of the protocol:
//!
//! * **The renewal ticker.** Every [`LeaseConfig::renew_every`] it records the
//!   publish instant `s_i` and advertises the resulting [`RenewalId`] under a
//!   TTL of one lease duration. The instant is taken *before* the write is
//!   enqueued, which is the inequality the reader's whole window rests on.
//! * **The granter.** On any change to a peer's renewal entry (and on
//!   membership churn, on a lagged event stream, and on a slow backstop tick)
//!   it re-folds every renewal it can see into one wholesale `~lease:g` entry
//!   and republishes it — but only when the bytes changed, so a group whose
//!   readers renew in lockstep does not re-author an identical map per event.
//! * **The view.** On any group change (and on the same backstop tick) it
//!   advances the reader's state machine, folds in the roster and every
//!   granter's map, and publishes the resulting *serve deadline* into a
//!   `watch`. That deadline is what makes [`LeaseView::valid`] a lock-free
//!   borrow plus one integer compare — cheap enough for a per-request check.
//!
//! The view task **polls before it ingests**, and that ordering is load-bearing
//! rather than incidental: a lapse must be latched by the state machine before
//! any newly-arrived grant can extend the window past it. Ingesting first would
//! let a reader whose window closed at `T` silently sail through it on a
//! confirmation that landed at `T + ε`, having never observed
//! [`LeaseState::Lapsed`] and so never entering
//! [`LeaseState::NeedsResync`] — precisely the stale-serve this tier exists to
//! prevent. With the poll first, the latch is exact at any tick cadence.
//!
//! # The reader's boot guard
//!
//! "Readers observe before serving" is enforced here, not left to the
//! deployment. [`LeaseCore`] confirms an **empty** roster vacuously — the right
//! rule for a group where nobody advertises [`CAP_LEASE`], and a hole for a node
//! that has simply not finished *learning* who does: for its first moments a
//! booting reader knows no granters, so its own freshly-published renewal is
//! "confirmed by everyone", and an eager consumer calling
//! [`LeaseView::mark_caught_up`] would put it into service under a window no
//! granter ever gave.
//!
//! So the shell fails that window closed until this node has participated for a
//! whole [`warmup_window`] — the same interval the writer's guard uses, for the
//! same reason, read off the group's effective [`Config`]. Two gates, both
//! needed: [`LeaseView::mark_caught_up`] refuses to take (so the core stays in
//! [`LeaseState::NeedsResync`] and every observer of it agrees), and
//! [`publish_serve`] publishes no deadline (so [`LeaseView::valid`] answers
//! `false` even if a core were affirmed some other way).
//!
//! The guard **latches**: once released it never re-arms. The writer's
//! [`Shared::warmed_up`] deliberately re-arms when the group grows, because a
//! writer that has just learned of more members may have learned of fewer lease
//! landscapes than exist; a *reader* that has learned a roster is already
//! fail-closed by its granters' silence, so re-closing a serving reader's window
//! on a scale-out would cost availability and buy no safety.
//!
//! # What the tasks own, and what a `Drop` means
//!
//! The tasks hold an [`Arc`] of the shared state, never a [`Leases`] — so
//! dropping the handle really does run its [`Drop`], which aborts them. A drop
//! **does not** retract the `~lease` entry: it is exactly the shape of this
//! process dying, and the entry must lapse on the *granters'* clocks, which is
//! the guarantee the writer's slow path is built on. [`Leases::leave`] is the
//! graceful counterpart for a planned departure.
//!
//! A [`LeaseView`] outlives its [`Leases`] and stays honest when it does: the
//! serve deadline in the `watch` stops being refreshed, so the view answers
//! `false` from the confirmed deadline onward, forever. A read handle whose
//! renewer has gone away lapses on its own — the type behaves like the lease.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use groupnet_core::{Config, NodeId};
use groupnet_runtime::{CommandRejected, Group};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::applied_by;
use crate::token::WriteToken;

use super::coherence::{CoherenceCore, CoherenceStep, WaitMember};
use super::core::{ClockMs, LeaseCore};
use super::tasks;
use super::wire::{
    GrantMap, RenewalId, decode_grants, decode_renewal, encode_renewal, grant_entry_key,
    renewal_entry_key, validate_name,
};
use super::{CAP_LEASE, CoherenceOutcome, LeaseConfig, LeaseState, wall_clock_epoch};

/// How often a coherent write re-examines its wait set — the same cadence
/// [`applied_by_selected`](crate::applied_by_selected) polls at, so the healthy
/// path costs exactly a T2 ack round and not a beat more.
const COHERENCE_POLL: Duration = Duration::from_millis(2);

/// The state the three background tasks and both handles share.
///
/// Behind one [`Arc`] that the tasks clone, so no task holds a [`Leases`] —
/// a task that did would keep the handle's [`Drop`] (and therefore its own
/// abort) from ever running. The fields and steps the [`tasks`]
/// module drives are `pub(super)`; everything else belongs to the handles here.
pub(super) struct Shared {
    pub(super) group: Group,
    pub(super) me: NodeId,
    pub(super) cfg: LeaseConfig,
    /// The entry this node's own renewals occupy.
    pub(super) renewal_key: String,
    /// The entry this node's grant map occupies.
    grant_key: String,
    /// The origin [`ClockMs`] measures from — one monotonic reference for the
    /// whole set, so the reader's arithmetic never touches a wall clock. It is
    /// also when this node started participating, which is what both warm-up
    /// guards measure against.
    started: Instant,
    /// The reader's boot guard, latched once this node has participated for a
    /// whole [`warmup_window`] (see the module docs). Shared with every
    /// [`LeaseView`], because a view holds no [`Group`] to recompute the window
    /// from — and because the answer must be the same one the view task
    /// published under.
    warmed: Arc<AtomicBool>,
    core: Arc<Mutex<LeaseCore>>,
    /// The published serve deadline: `Some(until)` when this node may serve up
    /// to `until`, `None` when it may not serve at all. Shared with every
    /// [`LeaseView`], which is why it is an [`Arc`] inside an [`Arc`] — a view
    /// outlives the [`Leases`] that made it.
    serve: Arc<watch::Sender<Option<ClockMs>>>,
    /// Set by [`Leases::leave`], so a task that is mid-turn when the abort
    /// lands still declines to publish.
    pub(super) left: AtomicBool,
}

/// The core behind the mutex. Poisoning is irrelevant: every mutation is a
/// whole-value update, so a panicking holder leaves it consistent.
fn lock(core: &Mutex<LeaseCore>) -> MutexGuard<'_, LeaseCore> {
    core.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Recomputes the serve deadline at `now` and publishes it — the **one** place
/// the `watch` is written, so the view task and the consumer-facing mutations
/// ([`LeaseView::mark_caught_up`], [`LeaseView::require_resync`]) can never
/// disagree about what it carries.
///
/// `peek` rather than `poll`: publishing must not consume a lapse edge, which
/// belongs to whoever advanced the machine a moment earlier.
///
/// `warmed` is the reader's boot guard: an unset flag publishes **no** window
/// whatever the core believes, which is what makes "readers observe before
/// serving" a property of this shell rather than a deployment rule (see the
/// module docs).
///
/// The lock is held **across** the publish, deliberately. Computing the
/// deadline and writing it have to be one critical section: otherwise a
/// consumer's [`LeaseView::require_resync`] and the view task's turn can
/// compute in one order and write in the other, and the write that lands last
/// could be the *stale* window — the one direction this tier must never fail
/// in. Nothing here awaits, so there is no suspension point a std mutex is
/// being held across (the same reasoning [`SeqFloors::publish`](crate::SeqFloors::publish)
/// records), and waking a `watch` receiver schedules a task rather than
/// re-entering this.
fn publish_serve(
    core: &Mutex<LeaseCore>,
    serve: &watch::Sender<Option<ClockMs>>,
    now: ClockMs,
    warmed: &AtomicBool,
) {
    let core = lock(core);
    let until = (warmed.load(Ordering::Relaxed) && core.peek(now) == LeaseState::Serving)
        .then(|| core.serve_until())
        .flatten();
    serve.send_if_modified(|current| {
        let changed = *current != until;
        if changed {
            *current = until;
        }
        changed
    });
}

/// The interval a node must have participated before "nobody here holds a
/// lease" is a fact rather than an artefact of not having looked long enough:
/// one failure-detection window (so every member it will ever learn about has
/// had time to appear) plus two anti-entropy rounds (so their entries have had
/// time to arrive).
///
/// Both halves measure against it, for the same reason from opposite ends: a
/// writer must not resolve on a wait set it has not finished learning, and a
/// reader must not serve on a roster it has not finished learning.
///
/// Read off the group's **effective** [`Config`], never the defaults — a
/// deployment that retunes its probe timings retunes this with it.
fn warmup_window(config: &Config, members: usize) -> Duration {
    Duration::from_millis(
        config
            .detection_window_ms(members)
            .saturating_add(config.anti_entropy_interval_ms.saturating_mul(2)),
    )
}

impl Shared {
    /// This node's monotonic now, as the cores measure it.
    fn now(&self) -> ClockMs {
        ClockMs(u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX))
    }

    /// One turn of the renewal ticker: record the publish instant, then
    /// advertise the renewal under a TTL of one lease duration.
    ///
    /// The instant is taken *before* the enqueue, which is the inequality the
    /// reader's arithmetic rests on. A rejected write is **not** retried with
    /// the same id: the renewal is state, so the next turn carries a fresh one,
    /// and the sequence number it burned simply never confirms — the
    /// fail-closed direction.
    pub(super) fn renew(&self) -> Result<(), CommandRejected> {
        let id = lock(&self.core).on_renew(self.now());
        self.group.set_entry(
            self.renewal_key.clone(),
            encode_renewal(id),
            Some(self.cfg.duration_ms()),
        )
    }

    /// Every renewal this node can currently see, folded into one grant map:
    /// "I have seen these readers' leases and I will wait for them".
    pub(super) fn grant_map(&self) -> GrantMap {
        let mut grants = GrantMap::new();
        for (node, entries) in self.group.all_entries().iter() {
            if *node == self.me {
                continue;
            }
            if let Some(id) = entries
                .get(&self.renewal_key)
                .and_then(|bytes| decode_renewal(bytes))
            {
                grants.insert(node.clone(), id);
            }
        }
        grants
    }

    /// Publishes an encoded grant map. Wholesale and TTL-less, exactly like
    /// `~caps`: one write authors over this node's whole previous life, so a
    /// restart cannot leave retired grants haunting the group.
    pub(super) fn publish_grants(&self, encoded: Vec<u8>) -> Result<(), CommandRejected> {
        self.group.set_entry(self.grant_key.clone(), encoded, None)
    }

    /// One turn of the reader's ingest: refresh the roster of granters that
    /// must confirm, then fold in what each of them advertises.
    ///
    /// The roster is every member this node still *knows about* — `Suspect` and
    /// `Dead`-but-not-reaped included, because either may still be writing —
    /// that advertises [`CAP_LEASE`]. Only a reap removes a granter, which is
    /// why [`Group::statuses`] (which lists tombstones) is the right source and
    /// [`Group::members_with_capability`] (which is built on the not-`Dead`
    /// set) is not: it would drop a dead-but-unreaped granter out of the
    /// min-set while a writer behind it may still be waiting on this node.
    fn ingest_grants(&self) {
        let roster: Vec<NodeId> = self
            .group
            .statuses()
            .into_iter()
            .map(|(node, _)| node)
            .filter(|node| *node != self.me && self.group.node_has_capability(node, CAP_LEASE))
            .collect();
        let mut core = lock(&self.core);
        core.set_roster(roster.iter().cloned());
        for granter in &roster {
            let grants = self
                .group
                .node_entry(granter, &self.grant_key)
                .map(|bytes| decode_grants(&bytes))
                .unwrap_or_default();
            core.observe_grant_map(granter, &grants);
        }
    }

    /// One turn of the view task: release the boot guard if it is due, advance
    /// the state machine, ingest, publish.
    ///
    /// The poll comes **first** and that is the safety hinge — see the module
    /// docs: a lapse latches before any fresh grant can paper over it. The guard
    /// check comes before *that*, so the turn a reader warms up on is also the
    /// turn its window can open, rather than the one after.
    pub(super) fn refresh_view(&self) {
        let now = self.now();
        let _warm = self.warmed_up();
        lock(&self.core).poll(now);
        self.ingest_grants();
        publish_serve(&self.core, &self.serve, now, &self.warmed);
    }

    /// Every **other** node holding a live renewal entry in this node's view.
    fn holders(&self) -> Vec<NodeId> {
        let mut holders: Vec<NodeId> = self
            .group
            .all_entries()
            .iter()
            .filter(|(node, _)| **node != self.me)
            .filter_map(|(node, entries)| {
                decode_renewal(entries.get(&self.renewal_key)?).map(|_| node.clone())
            })
            .collect();
        holders.sort_unstable();
        holders
    }

    /// The [`WaitMember`] snapshot [`CoherenceCore::step`] consumes: every live
    /// lease-holder in this node's view, paired with how far it advertises
    /// having applied `writer`'s feed.
    fn wait_snapshot(&self, writer: &NodeId) -> Vec<WaitMember> {
        self.holders()
            .into_iter()
            .map(|member| {
                let applied = applied_by(&self.group, &member, writer);
                WaitMember { member, applied }
            })
            .collect()
    }

    /// Whether this node has participated long enough for the absence of a
    /// lease to mean something. Recomputed per call, because the window depends
    /// on the current member count: a group that grows re-arms the **writer's**
    /// guard, which is the conservative direction for a writer.
    ///
    /// The reader's guard is the latch this also sets, and it deliberately does
    /// *not* re-arm — see the module docs for why the two halves differ.
    fn warmed_up(&self) -> bool {
        let warm = self.started.elapsed()
            >= warmup_window(self.group.config(), self.group.members().len());
        if warm {
            // Relaxed: this flag guards no data of its own (the core it gates is
            // behind a mutex), so there is nothing for an acquire/release pair
            // to order — exactly the reasoning `left` is stored under.
            self.warmed.store(true, Ordering::Relaxed);
        }
        warm
    }

    /// The warm-up guard: `None` to let the wait resolve normally, or
    /// `Some(unseen)` to hold it — naming the [`CAP_LEASE`] advertisers whose
    /// lease this node has not seen yet (possibly empty, when what is missing
    /// is the whole landscape rather than one member of it).
    ///
    /// A booting node is a *writer* before it is a converged *observer*. For
    /// its first moments it knows few members and fewer entries, so "my wait
    /// set is empty" is indistinguishable from "I have not looked long
    /// enough" — and resolving on that would complete a coherent write while a
    /// reader it has never heard of is serving the state the write
    /// invalidated. Until the window closes, this refuses two fast paths: an
    /// empty wait set, and excusing an advertiser whose `~lease` entry has not
    /// arrived. Both then wait for the caller's deadline, so a warm-up-era
    /// write either finds its holders or reports
    /// [`CoherenceOutcome::TimedOut`] honestly.
    ///
    /// What it does **not** close is the residual the module's honesty box
    /// names: a granter that this node *reaps* while it is in fact still
    /// writing. The guard bounds divergence at boot, not divergence that
    /// outlives the reap horizon.
    fn warmup_hold(&self, writer: &NodeId, snapshot: &[WaitMember]) -> Option<Vec<NodeId>> {
        if self.warmed_up() {
            return None;
        }
        let unseen: Vec<NodeId> = self
            .group
            .members_with_capability(CAP_LEASE)
            .into_iter()
            .filter(|node| *node != self.me && node != writer)
            .filter(|node| !snapshot.iter().any(|held| held.member == *node))
            .collect();
        if unseen.is_empty() && !snapshot.is_empty() {
            return None;
        }
        Some(unseen)
    }
}

/// This node's participation in one lease set: it renews its own right to
/// serve, it grants every other reader's, and it holds coherent writes until
/// every lease-holder has either applied them or lapsed.
///
/// One handle is both halves, like [`SeqFloors`](crate::SeqFloors). Hand
/// [`view`](Self::view) to whatever answers reads; keep this one wherever the
/// group handle lives — **dropping it stops the protocol** (see [`Drop`]).
///
/// # The advertisement is a contract
///
/// A node that advertises [`CAP_LEASE`] promises two running things: this
/// object (so it grants its peers' renewals) *and* an
/// [`AckLedger`](crate::AckLedger) fed by its apply loop (so a writer's fast
/// path can resolve on an acknowledgement instead of on a lapse).
///
/// Advertising **without the ledger** is safe for the reader and expensive for
/// everyone else, and not in the way a first reading suggests: this node keeps
/// renewing, so its `~lease` entry never expires in any writer's engine and the
/// *lapse* path never fires either. A coherent write behind it gets neither
/// excuse and runs to the caller's own deadline —
/// [`CoherenceOutcome::TimedOut`], the one outcome carrying no guarantee — for
/// as long as the node keeps renewing. That is the fail-slow reader the module's
/// honesty box names; the remedy is to stop the renewals (drop this handle) or
/// [`leave`](Self::leave), not to wait longer.
///
/// Advertising **without the granter** is worse still: readers put this node in
/// their min-sets and their confirmations freeze against a map that never
/// advances.
pub struct Leases {
    shared: Arc<Shared>,
    tasks: Vec<JoinHandle<()>>,
}

impl fmt::Debug for Leases {
    /// Deliberately reports `confirmed` rather than the state: reading the
    /// state consumes the lapse edge (see [`LeaseCore::poll`]), and a debug
    /// print must never swallow an alarm.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Leases")
            .field("group", &self.shared.group.id())
            .field("me", &self.shared.me)
            .field("renewal_key", &self.shared.renewal_key)
            .field("confirmed", &self.confirmed())
            .finish_non_exhaustive()
    }
}

impl Drop for Leases {
    /// Stops renewing, granting and ingesting — and **leaves the `~lease` entry
    /// alone**.
    ///
    /// That is deliberate. A drop is the shape of this process dying, and the
    /// writer's slow path is built on the entry lapsing at one lease duration
    /// past its last adoption *on each granter's own clock*. Retracting it here
    /// would make an ordinary drop look like a graceful departure and quietly
    /// remove the bound this tier exists to provide from the one case that
    /// tests it. [`Leases::leave`] is the graceful counterpart.
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl Leases {
    /// Joins the default lease set of `group` as `me`, under `cfg`, and starts
    /// renewing, granting and ingesting.
    ///
    /// The lease life's epoch is the wall clock at construction (see
    /// [`with_epoch`](Self::with_epoch)); the state starts at
    /// [`LeaseState::NeedsResync`], because a node that has just booted has
    /// missed every invalidation issued while it was down.
    ///
    /// # Panics
    /// If called outside a Tokio runtime — this spawns the tier's tasks.
    #[must_use]
    pub fn new(group: Group, me: NodeId, cfg: LeaseConfig) -> Self {
        Self::named("", group, me, cfg)
    }

    /// A named lease set — independent subsystems sharing one group name their
    /// sets so their entries stay apart. An empty name is the default set.
    ///
    /// A `cfg` outside [`LeaseConfig::validate`]'s envelope still *runs*, in the
    /// fail-closed direction each variant documents; a debug build asserts on it
    /// rather than letting a typo'd margin turn into a reader that never serves.
    ///
    /// # Panics
    /// If `name` contains `:`, or if it is `"g"`: a set named `g` would author
    /// its renewals into `~lease:g`, which is the default set's grant entry.
    /// Also if called outside a Tokio runtime.
    #[must_use]
    pub fn named(name: &str, group: Group, me: NodeId, cfg: LeaseConfig) -> Self {
        validate_name(name);
        debug_assert!(
            cfg.validate().is_ok(),
            "lease config outside its own envelope: {:?}",
            cfg.validate()
        );
        let core = LeaseCore::new(me.clone(), &cfg, wall_clock_epoch());
        let (serve, _) = watch::channel(None);
        let shared = Arc::new(Shared {
            group,
            me,
            cfg,
            renewal_key: renewal_entry_key(name),
            grant_key: grant_entry_key(name),
            started: Instant::now(),
            warmed: Arc::new(AtomicBool::new(false)),
            core: Arc::new(Mutex::new(core)),
            serve: Arc::new(serve),
            left: AtomicBool::new(false),
        });
        let tasks = tasks::spawn(&shared);
        Self { shared, tasks }
    }

    /// Replaces the lease life's epoch. Use a durable, strictly-increasing
    /// per-node counter (a boot counter, a WAL generation) when the wall-clock
    /// default is not trustworthy across restarts, exactly as with
    /// [`WriteFeed::with_epoch`](crate::WriteFeed::with_epoch).
    ///
    /// Chain it directly onto the constructor. The renewal ticker's first turn
    /// fires at construction, so one renewal of the boot epoch may already be
    /// on the wire; this re-seeds the core and immediately publishes a renewal
    /// of the new epoch over it. Both directions are safe — grants against the
    /// abandoned epoch never confirm ([`LeaseCore::observe_grant_map`]) — so
    /// the worst case is a few milliseconds of frozen confirmation, never a
    /// window nobody granted.
    #[must_use]
    pub fn with_epoch(self, epoch: u64) -> Self {
        *lock(&self.shared.core) = LeaseCore::new(self.shared.me.clone(), &self.shared.cfg, epoch);
        let _ = self.shared.renew();
        publish_serve(
            &self.shared.core,
            &self.shared.serve,
            self.shared.now(),
            &self.shared.warmed,
        );
        self
    }

    /// This lease set's tuning.
    #[must_use]
    pub fn config(&self) -> &LeaseConfig {
        &self.shared.cfg
    }

    /// This lease life's epoch.
    #[must_use]
    pub fn epoch(&self) -> u64 {
        lock(&self.shared.core).epoch()
    }

    /// A cheap, cloneable read handle for whatever serves reads.
    #[must_use]
    pub fn view(&self) -> LeaseView {
        LeaseView {
            core: Arc::clone(&self.shared.core),
            serve: Arc::clone(&self.shared.serve),
            started: self.shared.started,
            warmed: Arc::clone(&self.shared.warmed),
        }
    }

    /// Whether this node may serve cached state right now — the one question
    /// the tier answers. See [`LeaseView::valid`].
    #[must_use]
    pub fn valid(&self) -> bool {
        self.view().valid()
    }

    /// This node's lease state right now, consuming the lapse edge exactly
    /// like [`LeaseCore::poll`].
    #[must_use]
    pub fn state(&self) -> LeaseState {
        self.view().state()
    }

    /// Affirms that the consumer has re-synchronized; see
    /// [`LeaseCore::mark_caught_up`] for why this can only take while a lease
    /// is live, and [`LeaseView::mark_caught_up`] for why it also cannot take
    /// inside this node's warm-up window.
    #[must_use]
    pub fn mark_caught_up(&self) -> bool {
        self.view().mark_caught_up()
    }

    /// Forces [`LeaseState::NeedsResync`] — the hook for a
    /// [`PeerWrite::Gap`](crate::PeerWrite::Gap) or any other proof that
    /// invalidations were missed.
    pub fn require_resync(&self) {
        self.view().require_resync();
    }

    /// The newest renewal of this node's confirmed by every granter in its
    /// roster, or `None` while confirmation is frozen.
    #[must_use]
    pub fn confirmed(&self) -> Option<RenewalId> {
        lock(&self.shared.core).confirmed()
    }

    /// The newest renewal of **this** node's that `granter` advertises having
    /// adopted, read straight from the group. The first thing to look at when
    /// a reader is not serving: the granter that lags (or that is missing
    /// entirely) is the one freezing the lease.
    #[must_use]
    pub fn granted_by(&self, granter: &NodeId) -> Option<RenewalId> {
        decode_grants(
            &self
                .shared
                .group
                .node_entry(granter, &self.shared.grant_key)?,
        )
        .get(&self.shared.me)
        .copied()
    }

    /// Every **other** node holding a live renewal entry in this node's view —
    /// the wait set of a coherent write, as this writer sees it.
    ///
    /// A node drops out of this list when its entry expires *here*, which **is**
    /// the lapse [`invalidated_coherently`](Self::invalidated_coherently) waits
    /// for.
    #[must_use]
    pub fn holders(&self) -> Vec<NodeId> {
        self.shared.holders()
    }

    /// Waits until every lease-holder in this node's view has either applied
    /// `writer`'s write through `token` or had its serve-lease expire here.
    ///
    /// This is the whole point of the tier. Call it after the local durable
    /// write and after [`WriteFeed::publish`](crate::WriteFeed::publish) has
    /// handed back `token`; when it returns
    /// [`CoherenceOutcome::is_coherent`], no participating node can still be
    /// serving state this write invalidated — the responsive ones applied it,
    /// and the silent ones are out of service until they re-synchronize.
    ///
    /// The wait set is re-read on every poll rather than snapshotted, so a
    /// reader that takes a lease mid-write joins it (the conservative
    /// direction) and one whose lease lapses leaves it permanently — see
    /// [`CoherenceCore::step`] for the rules and why a re-acquired lease does
    /// not re-enter a wait it already lapsed out of.
    ///
    /// `timeout` is the caller's own deadline and the only way to get
    /// [`CoherenceOutcome::TimedOut`], which is the one outcome that carries no
    /// guarantee. Set it comfortably past [`LeaseConfig::duration`] and it
    /// covers the two failure shapes this tier *bounds*: a responsive holder
    /// (one ack round) and a silent one (one lease remainder). Setting it
    /// shorter is a deliberate choice to abandon the guarantee rather than wait
    /// for it.
    ///
    /// What no deadline covers is the third shape: a holder that keeps
    /// **renewing** while it stops **applying**. Its lease never lapses and its
    /// watermark never advances, so the wait ends at the deadline whatever the
    /// deadline is — the fail-slow reader in the module's honesty box, whose
    /// remedy is operational (kill its renewals, or have it
    /// [`leave`](Self::leave)) rather than a longer `timeout`. The warm-up
    /// window below is a *fourth* way to see this outcome, and the only one that
    /// clears on its own.
    ///
    /// # Warm-up
    ///
    /// For the first [`Config::detection_window_ms`] plus two anti-entropy
    /// rounds of this node's participation, an empty wait set — and an unseen
    /// [`CAP_LEASE`] advertiser — will not resolve the write; both wait for the
    /// caller's deadline instead, so a warm-up-era write either finds its
    /// holders or reports [`CoherenceOutcome::TimedOut`] honestly.
    ///
    /// A booting node is a **writer** before it is a converged observer: for its
    /// first moments "my wait set is empty" is indistinguishable from "I have
    /// not looked long enough", and resolving on that would complete a coherent
    /// write while a reader this node has never heard of serves the state the
    /// write invalidated. What the guard does *not* close is the residual the
    /// module's honesty box names — a granter this node **reaps** while it is in
    /// fact still writing. It bounds divergence at boot, not divergence that
    /// outlives the reap horizon.
    ///
    /// Each call gets its own [`CoherenceCore`]: one write's wait shares no
    /// state with another's, so nothing is held across an await and a
    /// cancelled call leaks nothing.
    pub async fn invalidated_coherently(
        &self,
        writer: &NodeId,
        token: WriteToken,
        timeout: Duration,
    ) -> CoherenceOutcome {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut core = CoherenceCore::new(writer.clone());
        loop {
            let snapshot = self.shared.wait_snapshot(writer);
            let held = self.shared.warmup_hold(writer, &snapshot);
            if held.is_none() {
                match core.step(token, &snapshot) {
                    CoherenceStep::AllApplied => return CoherenceOutcome::AllApplied,
                    CoherenceStep::LeaseLapsed { stragglers } => {
                        return CoherenceOutcome::LeaseLapsed { stragglers };
                    }
                    CoherenceStep::Waiting { .. } => {}
                }
            }
            if tokio::time::Instant::now() >= deadline {
                let mut waiting_on = core.abandon(token).unwrap_or_default();
                waiting_on.extend(held.unwrap_or_default());
                waiting_on.sort_unstable();
                waiting_on.dedup();
                return CoherenceOutcome::TimedOut { waiting_on };
            }
            tokio::time::sleep(COHERENCE_POLL).await;
        }
    }

    /// Departs the lease set gracefully: stops the tasks, retracts this node's
    /// `~lease` entry so no writer waits out a lapse for a reader that is
    /// leaving on purpose, and closes this node's own serve window immediately.
    ///
    /// The **grant** entry is left behind on purpose. It carries no TTL and is
    /// harmless: it says only "here is the newest renewal I had adopted from
    /// each reader", which can never *extend* anyone's window (a reader's
    /// confirmation is capped at what it published, and this node leaves every
    /// reader's roster the moment membership reaps it). Retracting it would
    /// buy nothing and would briefly freeze every reader that still counts this
    /// node as a granter.
    ///
    /// One residual, in the safe direction: a renewal already in flight on
    /// another thread can out-version the retraction, in which case the entry
    /// lingers until its TTL expires and writers over-wait by at most one lease
    /// duration. They never under-wait.
    ///
    /// # Errors
    /// [`CommandRejected`] if the group actor's bounded inbox is full or the
    /// actor has shut down; the retraction was not enqueued and the entry will
    /// lapse by TTL instead.
    pub fn leave(&self) -> Result<(), CommandRejected> {
        self.shared.left.store(true, Ordering::Relaxed);
        for task in &self.tasks {
            task.abort();
        }
        lock(&self.shared.core).require_resync();
        publish_serve(
            &self.shared.core,
            &self.shared.serve,
            self.shared.now(),
            &self.shared.warmed,
        );
        self.shared
            .group
            .delete_entry(self.shared.renewal_key.clone())
    }
}

/// The read half of a lease set: "may I serve this from cache?"
///
/// Cloneable and cheap — hand one to every request path.
/// [`valid`](Self::valid) is a lock-free `watch` borrow plus one integer
/// compare, so a hot read path can ask per request; the rest go through the
/// same [`LeaseCore`] the owning [`Leases`] renews.
///
/// A view outlives its [`Leases`], and lapses honestly when it does: nothing
/// refreshes the published deadline, so it answers `false` from that deadline
/// onward.
#[derive(Debug, Clone)]
pub struct LeaseView {
    core: Arc<Mutex<LeaseCore>>,
    serve: Arc<watch::Sender<Option<ClockMs>>>,
    started: Instant,
    /// The reader's boot guard, set by the owning [`Leases`]' view task — see
    /// the module docs.
    warmed: Arc<AtomicBool>,
}

impl LeaseView {
    /// Whether this node may serve cached state right now.
    ///
    /// `false` is the answer to fall back on — go to the origin, ask the
    /// authority, serve uncached — and it covers every flavour of "no lease":
    /// booting, still inside the warm-up window, lapsed, awaiting a resync
    /// affirmation, a granter gone silent, a renewal that never converged, an
    /// owning [`Leases`] that was dropped.
    #[must_use]
    pub fn valid(&self) -> bool {
        let now = self.now();
        (*self.serve.borrow()).is_some_and(|until| now < until)
    }

    /// The lease state right now.
    ///
    /// [`LeaseState::Lapsed`] is an edge reported once per lapse (see
    /// [`LeaseCore::poll`]) — and the view task advances the same state
    /// machine, so in a running set the edge is usually consumed *there* and a
    /// consumer polling afterwards reads [`LeaseState::NeedsResync`]. That is
    /// the right split: the latch must not depend on anyone remembering to ask.
    /// Count lapses with [`lapses`](Self::lapses), which is monotone and misses
    /// none of them.
    #[must_use]
    pub fn state(&self) -> LeaseState {
        let now = self.now();
        let state = lock(&self.core).poll(now);
        publish_serve(&self.core, &self.serve, now, &self.warmed);
        state
    }

    /// Affirms that the consumer has re-synchronized and may serve again,
    /// returning whether the affirmation took — see
    /// [`LeaseCore::mark_caught_up`]. A `true` here opens
    /// [`valid`](Self::valid) immediately rather than at the next task turn.
    ///
    /// Two things can refuse it, and both mean "not yet, try again": a lease
    /// that is not live at `now` (the core's rule), and this node's own
    /// **warm-up window** (the shell's — see the module docs). The second is
    /// what keeps a booting reader from serving under the vacuous confirmation
    /// an unlearned roster produces, so poll it rather than treating one `false`
    /// as a verdict.
    #[must_use]
    pub fn mark_caught_up(&self) -> bool {
        let now = self.now();
        let took = self.warmed.load(Ordering::Relaxed) && lock(&self.core).mark_caught_up(now);
        publish_serve(&self.core, &self.serve, now, &self.warmed);
        took
    }

    /// Forces [`LeaseState::NeedsResync`] — see [`LeaseCore::require_resync`].
    /// Closes the serve window immediately: a consumer that has just learned it
    /// missed invalidations must not serve one more request under the old
    /// deadline.
    pub fn require_resync(&self) {
        let now = self.now();
        lock(&self.core).require_resync();
        publish_serve(&self.core, &self.serve, now, &self.warmed);
    }

    /// How much of the serve window is left, or `None` when this node may not
    /// serve at all. Observability only — a decision reads
    /// [`valid`](Self::valid), which is the same borrow without the
    /// subtraction.
    #[must_use]
    pub fn remaining(&self) -> Option<Duration> {
        let now = self.now();
        let until = (*self.serve.borrow())?;
        (now < until).then(|| Duration::from_millis(until.since(now)))
    }

    /// How many times this node's lease has lapsed.
    #[must_use]
    pub fn lapses(&self) -> u64 {
        lock(&self.core).lapses()
    }

    /// This node's monotonic now, as the cores measure it: milliseconds since
    /// the owning [`Leases`] was constructed.
    fn now(&self) -> ClockMs {
        ClockMs(u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use groupnet_core::Config;

    use super::warmup_window;

    #[test]
    fn the_warmup_window_is_one_detection_window_plus_two_anti_entropy_rounds() {
        let cfg = Config::default();
        // 2 peers × (100 + 2×50) + 500 suspect = 900, + 2×200 anti-entropy.
        assert_eq!(cfg.detection_window_ms(3), 900);
        assert_eq!(warmup_window(&cfg, 3), Duration::from_millis(1_300));
        // A group of one has nobody to detect, so the window degenerates to
        // the refutation window plus the two rounds — still non-zero, which is
        // the point: a solo writer must not fast-resolve before it has had a
        // chance to hear from anyone at all.
        assert_eq!(warmup_window(&cfg, 1), Duration::from_millis(900));
    }

    #[test]
    fn the_warmup_window_tracks_the_effective_config_not_the_defaults() {
        // A deployment that tightens its probe timings tightens this with it —
        // the reason it is read off `Group::config()` rather than `Config::default`.
        let brisk = Config {
            probe_interval_ms: 20,
            probe_timeout_ms: 10,
            suspect_timeout_ms: 50,
            anti_entropy_interval_ms: 25,
            ..Config::default()
        };
        // 2 × (20 + 20) + 50 = 130, + 50.
        assert_eq!(warmup_window(&brisk, 3), Duration::from_millis(180));
        assert!(warmup_window(&brisk, 3) < warmup_window(&Config::default(), 3));
    }

    #[test]
    fn an_absurd_config_saturates_rather_than_wrapping_to_no_warmup_at_all() {
        let absurd = Config {
            probe_interval_ms: u64::MAX,
            anti_entropy_interval_ms: u64::MAX,
            ..Config::default()
        };
        assert_eq!(warmup_window(&absurd, 3), Duration::from_millis(u64::MAX));
    }
}
