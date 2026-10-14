//! Membership and status reads over the group actor's published snapshots.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use groupnet_core::{NodeId, Status};

use super::{BoundedRosterError, Group};
use crate::driver::now_since;

#[cfg(doc)]
use groupnet_core::Config;

impl Group {
    /// The current live members (anything not `Dead`), in id order. Failed and
    /// departed nodes drop out once failure detection converges.
    ///
    /// Clones every identity into a new vector; frequent readers such as
    /// placement loops should prefer the shared [`roster`](Self::roster).
    #[must_use]
    pub fn members(&self) -> Vec<NodeId> {
        self.members_rx.borrow().to_vec()
    }

    /// The same live-member snapshot as [`members`](Self::members), shared
    /// rather than copied: a reference-count increment on the immutable roster
    /// the group actor last published, never a clone of its identities.
    ///
    /// Pair it with [`events`](Self::events) for change-driven reads: subscribe
    /// first, read the roster, then re-read after each
    /// [`GroupEvent::MembershipChanged`](crate::GroupEvent::MembershipChanged)
    /// and after `Lagged`. Every edge is published after the roster it
    /// announces, so the re-read observes at least that change.
    #[must_use]
    pub fn roster(&self) -> Arc<[NodeId]> {
        self.members_rx.borrow().clone()
    }

    /// The status ([`Status::Alive`]/`Suspect`/`Dead`) this node currently perceives
    /// for `node`, or `None` if it is unknown. Unlike [`members`](Self::members)
    /// (the not-`Dead` set), this exposes the Alive/Suspect distinction a router
    /// needs to route *around* a suspected peer before it is declared dead.
    #[must_use]
    pub fn member_status(&self, node: &NodeId) -> Option<Status> {
        self.statuses_rx
            .borrow()
            .get(node)
            .map(|(status, _)| *status)
    }

    /// A snapshot of every known member and its status, in id order (includes
    /// `Suspect` members and not-yet-reaped `Dead` tombstones).
    #[must_use]
    pub fn statuses(&self) -> Vec<(NodeId, Status)> {
        self.statuses_rx
            .borrow()
            .iter()
            .map(|(n, (s, _))| (n.clone(), *s))
            .collect()
    }

    /// The status this node perceives for `node`, **and how long it has held
    /// that status uninterrupted** — the fencing-verdict primitive: "dead for
    /// long enough ⇒ safe to fence", "healthy for long enough ⇒ safe to
    /// unfence".
    ///
    /// The duration only restarts when the status *value* changes. Repeated
    /// gossip re-asserting the same status leaves it running, so
    /// `Some((Status::Dead, d))` with `d` past
    /// [`Config::detection_window_ms`] is a real "this observer has been sure
    /// for `d`" — not "the last message about it arrived `d` ago".
    ///
    /// **Observer-local, by construction.** This is *this* node's verdict from
    /// *its* probes and the gossip it received. Two nodes can honestly report
    /// different durations for the same peer, and a consumer that needs a
    /// cluster-wide verdict must combine observers itself (or anchor the
    /// durable act in a CAS-capable store, which is the pattern this feeds).
    ///
    /// **For the local node the duration is process lifetime, not group
    /// tenure**: this node has been `Alive` since its own logical origin, so a
    /// group joined an hour after boot still reports self as Alive for an
    /// hour. Nothing observes the local node into a status, so there is no
    /// observation to date it from. Fencing consumers read *peers*, where the
    /// stamp is observation-based and means what it says.
    ///
    /// **Reap horizon — the one sharp edge.** A `Dead` tombstone is removed
    /// `2 × dead_timeout_ms` after death, and from then on this returns
    /// `None`: the duration is only readable *inside* that horizon, so a
    /// long-departed node is indistinguishable from one never heard of. Two
    /// remedies, both fine:
    ///
    /// * raise [`Config::dead_timeout_ms`] above the longest verdict window
    ///   you need (it is already required to exceed the longest survivable
    ///   partition), or
    /// * treat `None` for a node you *know* was registered as "dead for at
    ///   least `2 × dead_timeout_ms`" — which is what the reap actually
    ///   proves, and is monotone in the safe direction for fencing.
    ///
    /// The duration is measured on the same logical clock the driver feeds the
    /// engine, so it is directly comparable to
    /// [`Config::detection_window_ms`]. Clamped at zero: a stamp cannot be in
    /// the future.
    #[must_use]
    pub fn status_held_for(&self, node: &NodeId) -> Option<(Status, Duration)> {
        let (status, since) = *self.statuses_rx.borrow().get(node)?;
        let now = now_since(self.start);
        Some((status, Duration::from_millis(now.0.saturating_sub(since.0))))
    }

    /// Every known member with its status **and how long this node has held
    /// that status for it**, in id order — the roster-shaped
    /// [`status_held_for`](Self::status_held_for), for a consumer that sweeps
    /// the whole membership rather than asking about one peer.
    ///
    /// One borrow of the published snapshot and one clock read for the whole
    /// roster, so every duration is measured against the same instant and the
    /// membership cannot shift between entries — unlike `status_held_for` in a
    /// loop, which takes a fresh borrow per node and can straddle a
    /// republish, listing a member at one moment and a peer at another. That
    /// consistency is the point for a fence maintainer (shardstore's, for
    /// instance) deciding a whole shard map in one pass.
    ///
    /// Includes `Suspect` members and not-yet-reaped `Dead` tombstones, and
    /// the local node — whose duration is process lifetime, with the same
    /// caveats (and the same reap horizon) that
    /// [`status_held_for`](Self::status_held_for) documents.
    ///
    /// **Policy stays with the caller.** This reports what is observed and for
    /// how long; which threshold counts as "dead long enough to fence", and
    /// what to do when it is met, is the consumer's decision and deliberately
    /// not encoded here.
    #[must_use]
    pub fn statuses_held(&self) -> Vec<(NodeId, Status, Duration)> {
        let now = now_since(self.start);
        self.statuses_rx
            .borrow()
            .iter()
            .map(|(node, (status, since))| {
                (
                    node.clone(),
                    *status,
                    Duration::from_millis(now.0.saturating_sub(since.0)),
                )
            })
            .collect()
    }

    /// A single coherent status-and-held-age snapshot with allocation bounds.
    ///
    /// This checks the count and every identity against the borrowed published
    /// view *before* cloning any identities. The local node counts toward
    /// `max_members`; callers can remove it after receiving the snapshot.
    ///
    /// # Errors
    /// Returns [`BoundedRosterError`] if the observed view exceeds either
    /// limit. An error is not a truncated roster.
    pub fn statuses_held_bounded(
        &self,
        max_members: NonZeroUsize,
        max_member_bytes: NonZeroUsize,
    ) -> Result<Vec<(NodeId, Status, Duration)>, BoundedRosterError> {
        let view = self.statuses_rx.borrow();
        if view.len() > max_members.get() {
            return Err(BoundedRosterError::TooManyMembers {
                members: view.len(),
                max: max_members.get(),
            });
        }
        if let Some(node) = view
            .keys()
            .find(|node| node.as_str().len() > max_member_bytes.get())
        {
            return Err(BoundedRosterError::IdentityTooLong {
                bytes: node.as_str().len(),
                max: max_member_bytes.get(),
            });
        }
        let now = now_since(self.start);
        Ok(view
            .iter()
            .map(|(node, (status, since))| {
                (
                    node.clone(),
                    *status,
                    Duration::from_millis(now.0.saturating_sub(since.0)),
                )
            })
            .collect())
    }
}
