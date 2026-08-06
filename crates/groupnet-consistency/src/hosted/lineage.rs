//! The lineage state machine: the sans-IO half of the read path.
//!
//! [`HostedReads`](super::HostedReads) is the tokio shell; everything that
//! *decides* lives here. Fed [`PeerWrite`]s and leadership pairs, [`Lineage`]
//! returns [`HostedRead`]s — so the whole table below is unit-tested without a
//! group, a runtime or a transport, which is the same posture
//! [`CommitCore`](super::CommitCore) and
//! [`CompletenessCore`](super::CompletenessCore) take for the two rules.
//!
//! # The lineage cursor
//!
//! Two positions, and keeping them apart is the whole design:
//!
//! * the **adopted pair** — `(epoch, host)` as
//!   [`Group::leadership`](groupnet_runtime::Group::leadership) reports it, i.e.
//!   who this node believes may write now;
//! * the **cursor** — `(epoch, host, next)`, the lineage epoch this subscriber
//!   has actually *delivered* from, and how far into it.
//!
//! An incoming [`PeerWrite`] is judged against both:
//!
//! | the write's epoch | verdict |
//! |---|---|
//! | below the **cut** | **dropped** — this node serves a later hostship, so every earlier lineage is dead ([`cut_below`](super::HostedReads::cut_below)) |
//! | below the cursor | **dropped** — a deposed host's late publish, arriving after the successor's lineage has begun |
//! | at the cursor | delivered, if it comes from that epoch's host; otherwise dropped |
//! | above the cursor, below the adopted epoch | dropped — a hostship this node skipped and can never authorize |
//! | at the adopted epoch, from the adopted host | **opens** the new lineage: one [`Gap`](HostedRead::Gap), then the writes |
//! | above the adopted epoch | **held**, until the watch catches up and adopts it |
//!
//! ## Why the cursor is where it was *delivered*, not where the watch is
//!
//! The tempting simplification — drop anything below the *adopted* epoch — is
//! wrong in one direction that matters: it strands a **recovering host**. A node
//! elected at `e′` whose apply loop was behind must still drain the previous
//! host's tail to reach the recovery target its peers report, and if adopting
//! `e′` blinded it to epoch-`e` writes it could never reach that target and would
//! sit in [`Recovering`](super::HostedError::Recovering) forever. The engine
//! would say host; the write path would refuse service; nothing would move.
//!
//! Deferring the cut to the first *delivered* write of the new lineage keeps
//! both properties. The tail drains in order, ahead of anything the new host
//! says — and the instant the new lineage speaks, the old one is dead to this
//! subscriber, which is the fencing the table promises. A late write that slips
//! through in the window before then cannot be *acknowledged* by anyone: the
//! commit rule needs a majority stamped at *its* epoch, and this node has
//! already stamped higher, so it can never be counted (see
//! [`CommitLedger`](super::CommitLedger)'s view-stamp fence). Safety is carried
//! by the stamp; ordering is carried here.
//!
//! What the window does cost is stated plainly, because it is real: a follower
//! still draining may **apply a write that is doomed** — one no successor's
//! recovery will ever carry, because it was never committed. That is invisible
//! to a cache (the next `Gap` rebuilds it) and permanent for an exact-replay
//! consumer that treats the stream as a log. The remedy is the one the tier
//! already names: the [`Gap`](HostedRead::Gap) that opens the next lineage is
//! **authoritative**, and remediating it (flush, rebuild, refetch from the
//! consumer's own store) is what reconciles the doomed tail away.
//!
//! ## The cut a *serving* host must take for itself
//!
//! There is one subscriber the rule above can never fire for: the host's own.
//! [`HostedReads`](super::HostedReads) excludes this node's feed, so a node that
//! is itself the host of `e′` never sees a write of its own lineage — the "first
//! delivered write of the new lineage" simply never arrives, and the open
//! lineage stays the **predecessor's** for as long as the process lives. Its
//! predecessor's un-replicated tail is gossiped state that can land minutes
//! later, when a partition heals; delivered then, it would be applied *after*
//! the writes this node has authored at `e′` — a fenced epoch-`e` write
//! reordered behind the authority's own, which is the silent stale state this
//! tier refuses everywhere else.
//!
//! The signal that closes it is **service**, not delivery. The instant the write
//! path admits this node to serve `e′` — recovery latched, the predecessor's
//! tail already drained as far as the recovery rule demanded — everything below
//! `e′` is dead by construction, and [`super::HostedReads::cut_below`] is the
//! cut that says so. [`HostedWrites::bind`](super::HostedWrites::bind) wires it
//! to that exact instant, which is why the automatic form is the one to prefer:
//! it cannot fire while the node is still
//! [`Recovering`](super::HostedError::Recovering) (nothing is admitted then, so
//! the latch has not moved) and it cannot be forgotten. **Bind it or call it**:
//! a serving host that does neither keeps the divergence window above open for
//! as long as it hosts.
//!
//! # One `Gap` opens every lineage
//!
//! Opening a lineage epoch always emits exactly one
//! [`Gap`](HostedRead::Gap) before its first write, even when nothing was
//! provably missed. That is honest rather than pessimistic: a migration is
//! precisely the moment the previous host's un-replicated tail may have been
//! lost, and the successor's ring may not start at sequence 1 either. Its
//! `missed_through` is epoch-major, so advancing a [`Frontier`](crate::Frontier)
//! to it covers **every** token of every earlier epoch in one step — which is
//! what makes the coarse remediation a consumer already implements for a writer
//! restart the whole of its migration handling too.

use std::collections::VecDeque;

use groupnet_core::NodeId;

use crate::peers::PeerWrite;
use crate::token::WriteToken;

/// How many above-the-watch events are held before the oldest is discarded.
///
/// Overflow costs nothing but precision: a dropped held event simply raises the
/// `first_visible` the lineage's opening [`Gap`](HostedRead::Gap) is computed
/// from, so it is *covered* by that gap rather than lost. The cap exists because
/// the held queue is fed by every peer publishing above this node's watch, and a
/// subscriber that never adopts must not grow without bound.
const HELD_CAP: usize = 1024;

/// One notification from [`HostedReads::next`](super::HostedReads::next) — a
/// hosted group's write stream, with its hand-overs named.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostedRead<K> {
    /// The lineage's host wrote `key` at `token`, whose `epoch` is the
    /// **leadership epoch** it was authored under. Apply it, then record it:
    /// `CommitLedger::record(&host, token)`.
    Wrote {
        /// The host that authored the write.
        host: NodeId,
        /// The write's position in that hostship's feed.
        token: WriteToken,
        /// The written key.
        key: K,
    },
    /// Writes of the lineage up to `missed_through` are not being delivered
    /// individually. Remediate coarsely (flush, rebuild, refetch), then advance
    /// the frontier and the ledger to `missed_through`.
    ///
    /// Emitted once when a lineage epoch opens — a migration, or this
    /// subscriber's first sight of the group's authority — and again if the
    /// host's ring ever advances past this subscriber's cursor.
    ///
    /// It names no peer, because it belongs to the **lineage** and not to a
    /// writer: the host it should be recorded against is the one the preceding
    /// [`Migrated`](Self::Migrated) named. `missed_through` is epoch-major, so
    /// one advance covers every earlier epoch entirely.
    Gap {
        /// After remediating, every write of the lineage up to and including
        /// this token is covered.
        missed_through: WriteToken,
    },
    /// This subscriber adopted a new `(epoch, host)` pair — the group's
    /// authority changed hands, or lapsed.
    ///
    /// `host: None` is a hostless epoch: nothing may be delivered under it, and
    /// the stream simply goes quiet until a successor activates. Either way,
    /// call [`CommitLedger::refresh`](super::CommitLedger::refresh) — that is
    /// how a voter tells a recovering host its view is *fresh* while no writes
    /// are arriving, and half of the deployment contract both rules rest on.
    Migrated {
        /// The leadership epoch now adopted.
        epoch: u64,
        /// Its host, or `None` when the group is believed hostless at it.
        host: Option<NodeId>,
    },
}

/// The lineage epoch this subscriber has actually delivered from.
#[derive(Debug, Clone)]
struct Open {
    epoch: u64,
    host: NodeId,
    /// Next undelivered sequence within `epoch`.
    next: u64,
}

/// The lineage state machine: adopted pair, cursor, and the two queues.
///
/// Sans-IO on purpose — it is fed [`PeerWrite`]s and leadership pairs and
/// returns [`HostedRead`]s, so the whole table in the module docs is unit-tested
/// without a group, a runtime or a transport. Its state stays private to this
/// module even from the shell that drives it: everything
/// [`HostedReads`](super::HostedReads) needs to report is an accessor below.
pub(super) struct Lineage<K> {
    adopted_epoch: u64,
    adopted_host: Option<NodeId>,
    open: Option<Open>,
    pending: VecDeque<HostedRead<K>>,
    held: VecDeque<PeerWrite<K>>,
    gaps: u64,
    /// The floor a [`cut`](Lineage::cut) has left behind: nothing authored below
    /// it is ever delivered again. Zero until this node serves.
    floor: u64,
}

impl<K> Lineage<K> {
    /// A subscriber that has adopted nothing: `(0, None)`, the same initial
    /// belief [`Group::leadership`](groupnet_runtime::Group::leadership) reports
    /// before any election. A group that has *already* elected therefore emits
    /// one [`Migrated`](HostedRead::Migrated) on the first poll, so a subscriber
    /// constructed late still learns who holds the group.
    pub(super) fn new() -> Self {
        Self {
            adopted_epoch: 0,
            adopted_host: None,
            open: None,
            pending: VecDeque::new(),
            held: VecDeque::new(),
            gaps: 0,
            floor: 0,
        }
    }

    pub(super) fn pop(&mut self) -> Option<HostedRead<K>> {
        self.pending.pop_front()
    }

    /// The `(epoch, host)` pair currently adopted.
    pub(super) fn adopted(&self) -> (u64, Option<NodeId>) {
        (self.adopted_epoch, self.adopted_host.clone())
    }

    /// How many [`HostedRead::Gap`]s have been emitted.
    pub(super) fn gaps_seen(&self) -> u64 {
        self.gaps
    }

    /// The epoch the last [`cut`](Self::cut) left behind.
    pub(super) fn cut_at(&self) -> u64 {
        self.floor
    }

    /// Closes every lineage below `epoch`, emitting nothing.
    ///
    /// Three things move together, and all three are required for the cut to
    /// mean what it says: the **floor** rises, so nothing below it is ever
    /// admitted again; the **cursor** comes off a lineage the floor has killed,
    /// so a later epoch can open cleanly; and anything already **queued** from a
    /// dead lineage is dropped where it stands, because the consumer must not be
    /// handed an event this subscriber has just declared dead. A
    /// [`Migrated`](HostedRead::Migrated) survives regardless — it belongs to
    /// the watch, not to a lineage, and a consumer that skipped it would skip
    /// the [`refresh`](super::CommitLedger::refresh) the tier's other rule
    /// depends on.
    ///
    /// Monotone and idempotent: a cut at or below the floor changes nothing.
    pub(super) fn cut(&mut self, epoch: u64) {
        if epoch <= self.floor {
            return;
        }
        self.floor = epoch;
        if self.open.as_ref().is_some_and(|open| open.epoch < epoch) {
            self.open = None;
        }
        self.pending.retain(|event| match event {
            HostedRead::Wrote { token, .. } => token.epoch >= epoch,
            HostedRead::Gap { missed_through } => missed_through.epoch >= epoch,
            HostedRead::Migrated { .. } => true,
        });
        self.held.retain(|event| match event {
            PeerWrite::Wrote { token, .. } => token.epoch >= epoch,
            PeerWrite::Gap { missed_through, .. } => missed_through.epoch >= epoch,
        });
    }

    /// Adopts `(epoch, host)` if it differs from the pair already held,
    /// announcing it and re-judging everything that was waiting for it.
    pub(super) fn adopt(&mut self, epoch: u64, host: Option<NodeId>) {
        if epoch == self.adopted_epoch && host == self.adopted_host {
            return;
        }
        self.adopted_epoch = epoch;
        self.adopted_host.clone_from(&host);
        self.pending.push_back(HostedRead::Migrated { epoch, host });
        let held: Vec<PeerWrite<K>> = self.held.drain(..).collect();
        for event in held {
            self.admit(event);
        }
    }

    /// Judges one peer-write against the cursor and the adopted pair — the
    /// table in the module docs, in order.
    pub(super) fn admit(&mut self, event: PeerWrite<K>) {
        let (peer, epoch) = match &event {
            PeerWrite::Wrote { peer, token, .. } => (peer, token.epoch),
            PeerWrite::Gap {
                peer,
                missed_through,
            } => (peer, missed_through.epoch),
        };
        if epoch < self.floor {
            return; // this node serves above it: the whole lineage is dead
        }
        if let Some(open) = &self.open {
            if epoch < open.epoch {
                return; // the deposed host's late publish dies here
            }
            if epoch == open.epoch {
                if *peer == open.host {
                    self.deliver(event);
                }
                return;
            }
        }
        // Above the cursor (or nothing delivered yet): only the adopted pair
        // may open a lineage, and only from the epoch the watch is at.
        if epoch < self.adopted_epoch {
            return; // a hostship this node skipped over
        }
        if epoch > self.adopted_epoch {
            self.hold(event);
            return;
        }
        if self.adopted_host.as_ref() == Some(peer) {
            self.open_with(peer.clone(), epoch, event);
        }
    }

    /// Opens the lineage at `epoch` under `peer`: one [`HostedRead::Gap`]
    /// covering everything below the first visible write, then that write.
    fn open_with(&mut self, peer: NodeId, epoch: u64, event: PeerWrite<K>) {
        let first = match &event {
            PeerWrite::Wrote { token, .. } => token.seq,
            // A ring that has already overflowed: the first *visible* write is
            // the one after what was missed.
            PeerWrite::Gap { missed_through, .. } => missed_through.seq.saturating_add(1),
        };
        let missed_through = WriteToken {
            epoch,
            seq: first.saturating_sub(1),
        };
        self.pending.push_back(HostedRead::Gap { missed_through });
        self.gaps += 1;
        self.open = Some(Open {
            epoch,
            host: peer,
            next: first,
        });
        // The opening gap already subsumes an inner `Gap`; a `Wrote` is the
        // lineage's first delivered write.
        if matches!(event, PeerWrite::Wrote { .. }) {
            self.deliver(event);
        }
    }

    /// Delivers an event that belongs to the open lineage epoch.
    fn deliver(&mut self, event: PeerWrite<K>) {
        let Some(next) = self.open.as_ref().map(|open| open.next) else {
            return;
        };
        let advanced = match event {
            PeerWrite::Wrote { peer, token, key } => {
                if token.seq < next {
                    return; // already delivered; the entry is state, not a log
                }
                if token.seq > next {
                    // The inner subscriber does not skip inside one life, but if
                    // it ever did, a silent hole is the one outcome this tier
                    // must never produce.
                    self.pending.push_back(HostedRead::Gap {
                        missed_through: WriteToken {
                            epoch: token.epoch,
                            seq: token.seq - 1,
                        },
                    });
                    self.gaps += 1;
                }
                let seq = token.seq;
                self.pending.push_back(HostedRead::Wrote {
                    host: peer,
                    token,
                    key,
                });
                seq.saturating_add(1)
            }
            PeerWrite::Gap { missed_through, .. } => {
                if missed_through.seq < next {
                    return; // covered by what has already been delivered
                }
                self.pending.push_back(HostedRead::Gap { missed_through });
                self.gaps += 1;
                missed_through.seq.saturating_add(1)
            }
        };
        if let Some(open) = self.open.as_mut() {
            open.next = advanced;
        }
    }

    /// Parks an event authored above this node's watch. See [`HELD_CAP`] for why
    /// dropping the oldest is safe.
    fn hold(&mut self, event: PeerWrite<K>) {
        if self.held.len() >= HELD_CAP {
            self.held.pop_front();
        }
        self.held.push_back(event);
    }
}
#[cfg(test)]
mod tests {
    use groupnet_core::NodeId;

    use super::{HELD_CAP, HostedRead, Lineage};
    use crate::peers::PeerWrite;
    use crate::token::WriteToken;

    fn node(name: &str) -> NodeId {
        NodeId::new(name)
    }

    fn wrote(peer: &str, epoch: u64, seq: u64) -> PeerWrite<String> {
        PeerWrite::Wrote {
            peer: node(peer),
            token: WriteToken { epoch, seq },
            key: format!("{peer}/{epoch}/{seq}"),
        }
    }

    fn inner_gap(peer: &str, epoch: u64, seq: u64) -> PeerWrite<String> {
        PeerWrite::Gap {
            peer: node(peer),
            missed_through: WriteToken { epoch, seq },
        }
    }

    fn read(peer: &str, epoch: u64, seq: u64) -> HostedRead<String> {
        HostedRead::Wrote {
            host: node(peer),
            token: WriteToken { epoch, seq },
            key: format!("{peer}/{epoch}/{seq}"),
        }
    }

    fn gap(epoch: u64, seq: u64) -> HostedRead<String> {
        HostedRead::Gap {
            missed_through: WriteToken { epoch, seq },
        }
    }

    fn migrated(epoch: u64, host: Option<&str>) -> HostedRead<String> {
        HostedRead::Migrated {
            epoch,
            host: host.map(node),
        }
    }

    /// Everything the lineage has queued, drained.
    fn drain(lineage: &mut Lineage<String>) -> Vec<HostedRead<String>> {
        let mut out = Vec::new();
        while let Some(event) = lineage.pop() {
            out.push(event);
        }
        out
    }

    /// A lineage that has adopted `(epoch, host)` and delivered nothing yet.
    fn adopted(epoch: u64, host: &str) -> Lineage<String> {
        let mut lineage = Lineage::new();
        lineage.adopt(epoch, Some(node(host)));
        lineage
    }

    #[test]
    fn adopting_a_pair_announces_it_once_and_only_when_it_changes() {
        let mut lineage = Lineage::new();
        // The initial belief is already `(0, None)`, so re-adopting it is silent
        // — a subscriber in an unelected group emits nothing at all.
        lineage.adopt(0, None);
        assert!(drain(&mut lineage).is_empty());

        lineage.adopt(5, Some(node("h1")));
        assert_eq!(drain(&mut lineage), vec![migrated(5, Some("h1"))]);
        lineage.adopt(5, Some(node("h1")));
        assert!(
            drain(&mut lineage).is_empty(),
            "an unchanged pair is silent"
        );

        // A hostless epoch is a migration like any other, and must be announced:
        // it is when a follower learns the group has no authority.
        lineage.adopt(6, None);
        assert_eq!(drain(&mut lineage), vec![migrated(6, None)]);
        lineage.adopt(7, Some(node("h2")));
        assert_eq!(drain(&mut lineage), vec![migrated(7, Some("h2"))]);
    }

    #[test]
    fn opening_a_lineage_emits_one_gap_then_the_writes() {
        let mut lineage = adopted(5, "h1");
        assert_eq!(drain(&mut lineage), vec![migrated(5, Some("h1"))]);
        for seq in 1..=3 {
            lineage.admit(wrote("h1", 5, seq));
        }
        assert_eq!(
            drain(&mut lineage),
            vec![
                // `(5, 0)` — nothing of epoch 5 was missed, and epoch-major
                // ordering makes it cover every earlier epoch outright.
                gap(5, 0),
                read("h1", 5, 1),
                read("h1", 5, 2),
                read("h1", 5, 3),
            ]
        );
        assert_eq!(lineage.gaps, 1, "one gap opens a lineage, and only one");
    }

    #[test]
    fn a_lineage_joined_mid_life_gaps_over_everything_before_it() {
        // The subscriber's first sight of an established host: its ring starts
        // at 40, so writes 1..=39 were never seen and must be remediated.
        let mut lineage = adopted(5, "h1");
        lineage.admit(wrote("h1", 5, 40));
        assert_eq!(
            drain(&mut lineage),
            vec![migrated(5, Some("h1")), gap(5, 39), read("h1", 5, 40)]
        );
    }

    #[test]
    fn only_the_adopted_host_of_an_epoch_may_write_into_it() {
        let mut lineage = adopted(5, "h1");
        lineage.admit(wrote("impostor", 5, 1));
        assert_eq!(drain(&mut lineage), vec![migrated(5, Some("h1"))]);
        // …and a hostless epoch authorizes nobody at all.
        let mut hostless = Lineage::new();
        hostless.adopt(5, None);
        hostless.admit(wrote("h1", 5, 1));
        assert_eq!(drain(&mut hostless), vec![migrated(5, None)]);
    }

    #[test]
    fn writes_above_the_watch_are_held_until_the_pair_is_adopted() {
        let mut lineage = adopted(5, "h1");
        lineage.admit(wrote("h1", 5, 1));
        assert_eq!(
            drain(&mut lineage),
            vec![migrated(5, Some("h1")), gap(5, 0), read("h1", 5, 1)]
        );
        // The successor is already publishing; this node's watch has not caught
        // up. Nothing is delivered, and nothing is lost.
        lineage.admit(wrote("h2", 6, 1));
        lineage.admit(wrote("h2", 6, 2));
        assert!(drain(&mut lineage).is_empty(), "held, not delivered");
        assert_eq!(lineage.held.len(), 2);

        lineage.adopt(6, Some(node("h2")));
        assert_eq!(
            drain(&mut lineage),
            vec![
                migrated(6, Some("h2")),
                gap(6, 0),
                read("h2", 6, 1),
                read("h2", 6, 2),
            ],
            "the migration is announced, gapped once, then replayed in order"
        );
        assert_eq!(lineage.gaps, 2, "one gap per lineage epoch opened");
    }

    /// The refinement the recovering host depends on: adopting `e′` does **not**
    /// blind this subscriber to epoch-`e` writes it has not delivered yet. Only
    /// the first delivered write of the new lineage closes the old one.
    #[test]
    fn the_previous_hosts_tail_drains_until_the_successors_lineage_speaks() {
        let mut lineage = adopted(5, "h1");
        lineage.admit(wrote("h1", 5, 1));
        drain(&mut lineage);

        // Elected elsewhere; this node adopts the new pair while still behind.
        lineage.adopt(6, Some(node("h2")));
        assert_eq!(drain(&mut lineage), vec![migrated(6, Some("h2"))]);

        // The tail the recovery target is measured against still arrives.
        lineage.admit(wrote("h1", 5, 2));
        lineage.admit(wrote("h1", 5, 3));
        assert_eq!(
            drain(&mut lineage),
            vec![read("h1", 5, 2), read("h1", 5, 3)],
            "a host elected at 6 must still be able to reach its target at 5"
        );

        // The successor speaks: the lineage moves, and from here the old host is
        // dead to this subscriber.
        lineage.admit(wrote("h2", 6, 1));
        assert_eq!(drain(&mut lineage), vec![gap(6, 0), read("h2", 6, 1)]);
        lineage.admit(wrote("h1", 5, 4));
        assert!(
            drain(&mut lineage).is_empty(),
            "the deposed host's late publish dies at the cursor"
        );
    }

    /// The me-is-host case, which no delivered write can ever close: this node
    /// is the adopted host of `6`, so nothing of *its* lineage arrives here.
    /// Service is the signal, and the cut takes the predecessor's tail with it —
    /// silently, because the events are dead rather than missed.
    #[test]
    fn cutting_at_service_kills_the_predecessors_lineage_silently() {
        let mut lineage = adopted(5, "h1");
        lineage.admit(wrote("h1", 5, 1));
        drain(&mut lineage);

        // Elected here. The tail still drains — this node may need it to reach
        // the recovery target its peers report — and it is delivered in order.
        lineage.adopt(6, Some(node("me")));
        lineage.admit(wrote("h1", 5, 2));
        assert_eq!(
            drain(&mut lineage),
            vec![migrated(6, Some("me")), read("h1", 5, 2)],
            "a recovering host still drains its predecessor"
        );

        // One epoch-5 write arrives and is queued, and *then* this node starts
        // serving 6. The queued event dies with the lineage: handing it back
        // would order it behind writes this node has already authored at 6.
        lineage.admit(wrote("h1", 5, 3));
        lineage.cut(6);
        assert!(
            drain(&mut lineage).is_empty(),
            "the cut emits nothing and keeps nothing"
        );
        assert_eq!(lineage.gaps, 1, "a cut is not a gap: nothing was missed");
        assert!(
            lineage.open.is_none(),
            "the cursor came off the dead lineage"
        );

        // …and from here the whole hostship is dead, writes and inner gaps
        // alike, however late they arrive.
        lineage.admit(wrote("h1", 5, 4));
        lineage.admit(inner_gap("h1", 5, 9));
        assert!(drain(&mut lineage).is_empty());
        assert!(lineage.held.is_empty(), "dropped, not held");
    }

    /// The cut is monotone and idempotent, so an apply loop may take it
    /// unconditionally on every turn — and a *higher* epoch than the watch has
    /// adopted still binds, which is what makes it safe against a lagging watch.
    #[test]
    fn a_cut_is_monotone_and_outranks_a_lagging_watch() {
        let mut lineage = adopted(5, "h1");
        drain(&mut lineage);
        lineage.cut(7);
        lineage.cut(6);
        lineage.cut(0);
        assert_eq!(lineage.floor, 7, "a lower cut never lowers the floor");

        // The watch still says (5, h1) — this node's own leadership reading is
        // behind the service it has already been admitted to. An epoch-5 write
        // must not open a lineage under it.
        lineage.admit(wrote("h1", 5, 1));
        assert!(drain(&mut lineage).is_empty());
        // A held write at or above the floor survives the cut and is replayed
        // when its pair is adopted, exactly as an uncut one would be.
        lineage.admit(wrote("h2", 8, 1));
        lineage.cut(8);
        assert_eq!(lineage.held.len(), 1, "at the floor, so still held");
        lineage.adopt(8, Some(node("h2")));
        assert_eq!(
            drain(&mut lineage),
            vec![migrated(8, Some("h2")), gap(8, 0), read("h2", 8, 1)]
        );
    }

    /// A migration announcement outlives the cut that kills its lineage: it is
    /// the watch's, not a writer's, and dropping it would cost the consumer the
    /// `CommitLedger::refresh` the recovery rule is built on.
    #[test]
    fn a_cut_keeps_the_migration_it_finds_queued() {
        let mut lineage = adopted(5, "h1");
        lineage.admit(wrote("h1", 5, 1));
        lineage.adopt(6, Some(node("me")));
        lineage.cut(6);
        assert_eq!(
            drain(&mut lineage),
            vec![migrated(5, Some("h1")), migrated(6, Some("me"))],
            "both hand-overs survive; neither of h1's writes does"
        );
    }

    #[test]
    fn a_skipped_hostship_is_never_delivered() {
        // This node went straight from 5 to 7 (it heard 7 first). Epoch 6's
        // writes can never be authorized — nothing ever names 6's host — so they
        // are dropped rather than held forever.
        let mut lineage = adopted(5, "h1");
        lineage.admit(wrote("h1", 5, 1));
        drain(&mut lineage);
        lineage.adopt(7, Some(node("h3")));
        drain(&mut lineage);
        lineage.admit(wrote("h2", 6, 1));
        assert!(drain(&mut lineage).is_empty());
        assert!(lineage.held.is_empty(), "dropped, not held");
    }

    #[test]
    fn duplicate_and_stale_sequences_inside_one_life_are_idempotent() {
        let mut lineage = adopted(5, "h1");
        for seq in [1, 2, 1, 2, 3, 2] {
            lineage.admit(wrote("h1", 5, seq));
        }
        assert_eq!(
            drain(&mut lineage),
            vec![
                migrated(5, Some("h1")),
                gap(5, 0),
                read("h1", 5, 1),
                read("h1", 5, 2),
                read("h1", 5, 3),
            ],
            "the feed entry is state, so a re-scan re-offers what was delivered"
        );
    }

    #[test]
    fn a_ring_that_overflows_mid_lineage_gaps_again() {
        let mut lineage = adopted(5, "h1");
        lineage.admit(wrote("h1", 5, 1));
        drain(&mut lineage);
        // The host's ring advanced past this subscriber: the inner subscriber
        // says so, and the lineage passes it through.
        lineage.admit(inner_gap("h1", 5, 9));
        lineage.admit(wrote("h1", 5, 10));
        assert_eq!(drain(&mut lineage), vec![gap(5, 9), read("h1", 5, 10)]);
        // A gap already covered by what was delivered changes nothing.
        lineage.admit(inner_gap("h1", 5, 5));
        assert!(drain(&mut lineage).is_empty());
        assert_eq!(lineage.gaps, 2);
    }

    #[test]
    fn a_lineage_opened_by_a_gap_emits_exactly_that_one_gap() {
        // The successor's first *visible* write is 8: everything below it is one
        // gap, not two.
        let mut lineage = adopted(6, "h2");
        lineage.admit(inner_gap("h2", 6, 7));
        assert_eq!(
            drain(&mut lineage),
            vec![migrated(6, Some("h2")), gap(6, 7)]
        );
        assert_eq!(lineage.gaps, 1);
        lineage.admit(wrote("h2", 6, 8));
        assert_eq!(drain(&mut lineage), vec![read("h2", 6, 8)]);
    }

    #[test]
    fn the_held_queue_is_bounded_and_its_overflow_is_absorbed_by_the_gap() {
        let mut lineage = adopted(5, "h1");
        drain(&mut lineage);
        for seq in 1..=(HELD_CAP as u64 + 5) {
            lineage.admit(wrote("h2", 9, seq));
        }
        assert_eq!(lineage.held.len(), HELD_CAP, "bounded");
        lineage.adopt(9, Some(node("h2")));
        let events = drain(&mut lineage);
        assert_eq!(events[0], migrated(9, Some("h2")));
        assert_eq!(
            events[1],
            gap(9, 5),
            "the five discarded writes are covered by the opening gap, not lost"
        );
        assert_eq!(events[2], read("h2", 9, 6));
        assert_eq!(events.len(), HELD_CAP + 2);
    }
}
