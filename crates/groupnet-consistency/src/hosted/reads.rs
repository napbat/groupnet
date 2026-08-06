//! The read half: [`HostedReads`], a subscriber to the group's **hosted
//! lineage** rather than to a peer.
//!
//! [`PeerWrites`] answers "what did node X write?". A follower of a hosted group
//! wants a different question answered — "what did *the group* write?" — and the
//! answer changes writer every migration. This type is that translation: one
//! ordered stream of the authority's writes, with the hand-over made explicit
//! instead of appearing as a second peer that started talking.
//!
//! Everything this shell does is select, poll and hand off: the judgement — the
//! adopted pair, the cursor, the cut, the [`Gap`](HostedRead::Gap) that opens
//! every lineage — is [`Lineage`](super::lineage), sans-IO next door, where its
//! whole truth table is unit-tested without a group or a runtime. Read that
//! module for the semantics; read this one for the plumbing.
//!
//! # The follower loop this tier asks for
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use groupnet_consistency::hosted::{CommitLedger, HostedRead, HostedReads, HostedWrites};
//! # use groupnet_core::NodeId;
//! # async fn demo(
//! #     mut reads: HostedReads<String>,
//! #     ledger: Arc<CommitLedger>,
//! #     writes: Arc<HostedWrites<String>>,
//! # ) {
//! // One builder step, before the loop takes the handle: when this node begins
//! // serving, its own lineage is cut there and the predecessor's late tail dies
//! // instead of being applied behind this host's own writes.
//! writes.bind(&mut reads);
//!
//! // The lineage's host, as the last `Migrated` named it: a `Gap` belongs to
//! // the lineage rather than to a peer, so the watermark it raises is the
//! // host's.
//! let mut host: Option<NodeId> = None;
//! while let Some(event) = reads.next().await {
//!     match event {
//!         HostedRead::Wrote { host: writer, token, key } => {
//!             let _ = key;                    // apply it
//!             ledger.record(&writer, token).await;
//!         }
//!         HostedRead::Gap { missed_through } => {
//!             // coarse remediation: flush, rebuild, refetch
//!             if let Some(host) = &host {
//!                 ledger.record(host, missed_through).await;
//!             }
//!         }
//!         HostedRead::Migrated { host: adopted, .. } => {
//!             host = adopted;
//!             // Re-stamp, so a recovering host can see this voter's view is
//!             // *fresh* even while no new writes arrive.
//!             ledger.refresh().await;
//!         }
//!     }
//! }
//! # }
//! ```

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use groupnet_core::NodeId;
use groupnet_runtime::{Group, GroupEvent};
use tokio::sync::broadcast::Receiver;
use tokio::sync::broadcast::error::RecvError;

use super::lineage::{HostedRead, Lineage};
use super::writes::hosted_feed_name;
use crate::peers::{PeerWrite, PeerWrites};

/// Subscriber half of a hosted write path: the group's authority as one ordered
/// stream, with migrations named.
///
/// Drive it from a task — `while let Some(event) = reads.next().await { … }` —
/// and pair it with a [`CommitLedger`](super::CommitLedger) exactly as the
/// module docs show. **Every voter of a `Quorum` group must run this loop**: the
/// commit rule and the recovery rule are both predicates over what voters
/// publish, and a voter that votes without applying is invisible to both. The
/// tier fails closed around it — commits time out naming it, and a new host
/// stalls in recovery — but that is availability spent for nothing.
pub struct HostedReads<K> {
    group: Group,
    inner: PeerWrites<K>,
    events: Receiver<GroupEvent>,
    lineage: Lineage<K>,
    /// The serving epoch of a [`HostedWrites`](super::HostedWrites) this
    /// subscriber has been bound to, or `None` when nothing wired one in. Read
    /// at the top of every turn; see [`HostedReads::cut_below`].
    serving: Option<Arc<AtomicU64>>,
    /// Set once the group is gone, so the queued tail is still drained first.
    closed: bool,
}

impl<K> fmt::Debug for HostedReads<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (epoch, host) = self.lineage.adopted();
        f.debug_struct("HostedReads")
            .field("group", &self.group.id())
            .field("epoch", &epoch)
            .field("host", &host)
            .field("gaps_seen", &self.lineage.gaps_seen())
            .field("cut_below", &self.lineage.cut_at())
            .finish_non_exhaustive()
    }
}

impl<K> HostedReads<K> {
    /// Subscribes to the default hosted write path in `group`. `me` is this
    /// node's id — its own feed is ignored, so a node that is *itself* the host
    /// sees only the migrations, never an echo of its own writes. That exclusion
    /// is why a host must cut its predecessor's lineage explicitly:
    /// [`HostedWrites::bind`](super::HostedWrites::bind) or
    /// [`cut_below`](Self::cut_below).
    pub fn new(
        group: Group,
        me: NodeId,
        decode: impl Fn(&[u8]) -> Option<K> + Send + Sync + 'static,
    ) -> Self {
        Self::named("", group, me, decode)
    }

    /// [`new`](Self::new) for a named write path — the counterpart of
    /// [`HostedWrites::named`](super::HostedWrites::named) under the same name.
    pub fn named(
        name: &str,
        group: Group,
        me: NodeId,
        decode: impl Fn(&[u8]) -> Option<K> + Send + Sync + 'static,
    ) -> Self {
        let events = group.events();
        let inner = PeerWrites::named(&hosted_feed_name(name), group.clone(), me, decode);
        Self {
            group,
            inner,
            events,
            lineage: Lineage::new(),
            serving: None,
            closed: false,
        }
    }

    /// Closes every lineage below `epoch`: nothing authored under an earlier
    /// hostship is delivered again, and whatever this subscriber had queued from
    /// one is dropped unread. Emits nothing — the events are dead, not missed,
    /// so there is no [`Gap`](HostedRead::Gap) to remediate.
    ///
    /// **Call it with this node's fence epoch the moment it begins serving**, or
    /// let [`HostedWrites::bind`](super::HostedWrites::bind) do it at exactly
    /// that instant, which is the form to prefer. The module docs carry the
    /// reasoning: a host never sees its own lineage through this subscriber, so
    /// nothing else can ever close the predecessor's, and a late tail delivered
    /// afterwards would be applied *behind* this host's own writes.
    ///
    /// Monotone and idempotent — a cut at or below one already taken changes
    /// nothing — so an unconditional call per event is a perfectly good way to
    /// use it. It is also the wrong tool for a *follower*: a node that is not
    /// serving must keep draining the previous host's tail, which is what its
    /// own recovery would be measured against.
    pub fn cut_below(&mut self, epoch: u64) {
        self.lineage.cut(epoch);
    }

    /// Binds this subscriber to a write path's serving epoch. Called by
    /// [`HostedWrites::bind`](super::HostedWrites::bind), which is the public
    /// spelling.
    pub(super) fn bind_serving(&mut self, serving: Arc<AtomicU64>) {
        self.serving = Some(serving);
    }

    /// Applies the bound write path's serving epoch, if it has risen since the
    /// last turn. A no-op when nothing is bound.
    fn cut_to_service(&mut self) {
        if let Some(serving) = &self.serving {
            // Relaxed is the whole ordering this needs: the epoch *is* the
            // message — no other state is published through the cell — and a cut
            // read one turn late costs nothing, because a turn is exactly what
            // it takes for an event to be delivered.
            let epoch = serving.load(Ordering::Relaxed);
            self.lineage.cut(epoch);
        }
    }

    /// The `(epoch, host)` pair this subscriber has adopted — what it will
    /// accept writes under right now.
    #[must_use]
    pub fn adopted(&self) -> (u64, Option<NodeId>) {
        self.lineage.adopted()
    }

    /// How many [`HostedRead::Gap`]s this subscriber has emitted. At least one
    /// per lineage epoch it has opened; more than that means the host's ring is
    /// undersized for its write rate.
    #[must_use]
    pub fn gaps_seen(&self) -> u64 {
        self.lineage.gaps_seen()
    }

    /// The next event of the lineage, or `None` once the group is gone.
    ///
    /// Selects over the peer-write stream and the group's event stream, so a
    /// leadership change wakes this even while the feed is silent — which is
    /// what makes [`Migrated`](HostedRead::Migrated) prompt enough to drive a
    /// [`CommitLedger::refresh`](super::CommitLedger::refresh) rather than
    /// arriving with the next write.
    ///
    /// The watch is re-read at the top of every turn rather than trusted to the
    /// event: the snapshot is always current, so a missed or lagged edge costs a
    /// turn's latency and never a missed migration.
    ///
    /// **Cancel-safe.** Dropping the returned future loses nothing: everything
    /// this has decided is queued on the subscriber itself and is handed back by
    /// the next call, and both branches it awaits are themselves cancel-safe. So
    /// it composes in a `select!` — with a shutdown signal, or with a consumer's
    /// own pause — which is the shape an apply loop actually wants.
    pub async fn next(&mut self) -> Option<HostedRead<K>> {
        loop {
            let lead = self.group.leadership();
            self.lineage.adopt(lead.epoch, lead.host);
            // Before anything is handed back, and before anything new is
            // admitted below: a lineage this node has outlived by *serving* must
            // not survive one more turn.
            self.cut_to_service();
            if let Some(event) = self.lineage.pop() {
                return Some(event);
            }
            if self.closed {
                return None;
            }
            // The two borrows are of distinct fields and end with the select, so
            // the lineage below is free to be mutated.
            let wake = {
                let inner = &mut self.inner;
                let events = &mut self.events;
                tokio::select! {
                    event = inner.next() => match event {
                        Some(event) => Wake::Wrote(event),
                        None => Wake::Gone,
                    },
                    alive = leadership_signal(events) => {
                        if alive { Wake::Leadership } else { Wake::Gone }
                    }
                }
            };
            match wake {
                Wake::Wrote(event) => self.lineage.admit(event),
                // Re-read the watch at the top of the loop; that is the whole
                // handling a leadership edge needs.
                Wake::Leadership => {}
                // Drain what is already queued before answering `None`.
                Wake::Gone => self.closed = true,
            }
        }
    }
}

/// What woke one turn of [`HostedReads::next`].
enum Wake<K> {
    /// The inner subscriber produced a peer write.
    Wrote(PeerWrite<K>),
    /// Something happened that may have changed the adopted pair.
    Leadership,
    /// The group is gone; no further event will ever arrive.
    Gone,
}

/// Waits for something that could have changed the group's leadership, so the
/// loop above re-reads the watch. `false` once the group is gone.
///
/// Non-leadership events are consumed and ignored here rather than waking the
/// caller, so an ordinary write storm (which republishes entries, and therefore
/// events, per write per peer) does not spin the select. Cancel-safe: a dropped
/// future loses at most an event this function was going to discard anyway.
async fn leadership_signal(events: &mut Receiver<GroupEvent>) -> bool {
    loop {
        match events.recv().await {
            // A lagged stream is a missed *edge*, never missed state: the watch
            // read at the top of the loop is the whole resync.
            Ok(GroupEvent::LeadershipChanged { .. }) | Err(RecvError::Lagged(_)) => return true,
            Ok(_) => {}
            Err(RecvError::Closed) => return false,
        }
    }
}
