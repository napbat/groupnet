//! The writer's half of the shell: a coherent write's wait, polled against
//! [`CoherenceCore`] with the wait set and the excuses the
//! [`GrantLedger`](crate::lease::GrantLedger) provides.

use std::time::Duration;

use groupnet_core::NodeId;

use crate::applied_by;
use crate::lease::coherence::{CoherenceCore, CoherenceStep, WaitMember};
use crate::lease::core::ClockMs;
use crate::lease::{CAP_LEASE, CoherenceOutcome};
use crate::token::WriteToken;

use super::{Leases, Shared, decode_renewal, lock};

/// How often a coherent write re-examines its wait set — the same cadence
/// [`applied_by_selected`](crate::applied_by_selected) polls at, so the healthy
/// path costs exactly a T2 ack round and not a beat more.
const COHERENCE_POLL: Duration = Duration::from_millis(2);

/// One coherent write's registration in the
/// [`GrantLedger`](crate::lease::GrantLedger): while it lives, no reader that
/// has not applied the write is granted a renewal first seen after the write
/// began. Dropping it —
/// on any verdict, or when the waiting future is cancelled — unregisters the
/// write and wakes the granter, so the cap lifts at once rather than at the
/// next renewal.
struct Registration<'a> {
    shared: &'a Shared,
    writer: NodeId,
    token: WriteToken,
}

impl Drop for Registration<'_> {
    fn drop(&mut self) {
        lock(&self.shared.ledger).end(&self.writer, self.token);
        self.shared.refold.notify_one();
    }
}

impl Shared {
    /// Every **other** node holding a live renewal entry in this node's view.
    pub(super) fn holders(&self) -> Vec<NodeId> {
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

    /// The [`WaitMember`] snapshot [`CoherenceCore::step`] consumes at `now`:
    /// every reader that may be serving — a live lease-holder in this node's
    /// view, or one this node granted within the last lease duration whose
    /// entry has since left the view — with how far it advertises having
    /// applied `writer`'s feed and from when a lapse excuses it.
    fn wait_snapshot(&self, writer: &NodeId, now: ClockMs) -> Vec<WaitMember> {
        let ledger = lock(&self.ledger);
        let mut members = self.holders();
        members.extend(ledger.open_grants(now).cloned());
        members.sort_unstable();
        members.dedup();
        members
            .into_iter()
            .filter(|member| *member != self.me)
            .map(|member| {
                let applied = applied_by(&self.group, &member, writer);
                let excusable_at = ledger.excusable_at(&member);
                WaitMember {
                    member,
                    applied,
                    excusable_at,
                }
            })
            .collect()
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
    /// names: a reader that boots into a full partition from this node never
    /// learns it, and this node never sees that reader's lease.
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

    /// Registers a coherent write with the ledger until the returned guard
    /// drops.
    fn register(&self, writer: &NodeId, token: WriteToken) -> Registration<'_> {
        lock(&self.ledger).begin(writer, token, self.now());
        Registration {
            shared: self,
            writer: writer.clone(),
            token,
        }
    }
}

impl Leases {
    /// Every **other** node holding a live renewal entry in this node's view —
    /// the readers a coherent write starts out waiting on, as this writer sees
    /// them.
    #[must_use]
    pub fn holders(&self) -> Vec<NodeId> {
        self.shared.holders()
    }

    /// Waits until every reader that may be serving has either applied
    /// `writer`'s write through `token` or provably lost its right to serve.
    ///
    /// This is the whole point of the tier. Call it after the local durable
    /// write and after [`WriteFeed::publish`](crate::WriteFeed::publish) has
    /// handed back `token`; when it returns
    /// [`CoherenceOutcome::is_coherent`], no participating node that counts
    /// this one can still be serving state this write invalidated — the
    /// responsive ones applied it, and the silent ones are out of service until
    /// they re-synchronize.
    ///
    /// # Who is waited on, and what excuses them
    ///
    /// The wait set is every live lease-holder in this node's view plus every
    /// reader this node granted within the last lease duration, re-read on
    /// every poll: a reader that takes a lease mid-write joins it (the
    /// conservative direction), and a reader that leaves the view stays in it.
    /// A reader that does not acknowledge is excused only by a **proven lapse**
    /// ([`GrantLedger::excusable_at`](super::GrantLedger::excusable_at)): its
    /// current life has shown, in its own grant map, that it counts this node,
    /// and one lease duration has passed on this node's clock since the
    /// renewal behind the newest grant this node gave it was first seen. While
    /// this write waits, that grant cannot grow — this node grants a reader that
    /// has not applied the write no renewal first seen after the write began —
    /// so a reader that keeps renewing but stops applying lapses one lease
    /// duration into the write rather than holding it forever. A departed
    /// reader is excused at once. See [`CoherenceCore::step`] for why a lapsed
    /// reader never re-enters a wait it lapsed out of.
    ///
    /// # Deadline
    ///
    /// `timeout` is the caller's own deadline and the only way to get
    /// [`CoherenceOutcome::TimedOut`], which is the one outcome that carries no
    /// guarantee. Set it comfortably past
    /// [`LeaseConfig::duration`](crate::lease::LeaseConfig::duration) and every
    /// reader that counts this node is resolved by it — one ack round, or one
    /// lease duration. What remains are readers that have not proven they count
    /// this node (a reader whose current life has not yet granted this node's
    /// renewal, or one running a build that does not stamp its map), which only
    /// an acknowledgement resolves, the warm-up window below, and a writer that
    /// has departed ([`leave`](Self::leave)), whose remaining writes end on
    /// acknowledgements alone. A `TimedOut` write must not be reported to its
    /// client as coherent.
    ///
    /// # Warm-up
    ///
    /// For the first
    /// [`Config::detection_window_ms`](groupnet_core::Config::detection_window_ms)
    /// plus two anti-entropy rounds of this node's participation, an empty wait
    /// set — and an unseen [`CAP_LEASE`] advertiser — will not resolve the
    /// write; both wait for the caller's deadline instead, so a warm-up-era
    /// write either finds its holders or reports [`CoherenceOutcome::TimedOut`]
    /// honestly.
    ///
    /// Each call gets its own [`CoherenceCore`]: one write's wait shares no
    /// state with another's beyond its registration in the grant ledger, which
    /// a cancelled call drops like any other.
    pub async fn invalidated_coherently(
        &self,
        writer: &NodeId,
        token: WriteToken,
        timeout: Duration,
    ) -> CoherenceOutcome {
        let deadline = tokio::time::Instant::now() + timeout;
        let _registration = self.shared.register(writer, token);
        let mut core = CoherenceCore::new(writer.clone());
        loop {
            let now = self.shared.now();
            let snapshot = self.shared.wait_snapshot(writer, now);
            let held = self.shared.warmup_hold(writer, &snapshot);
            if held.is_none() {
                match core.step(token, now, &snapshot) {
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
}
