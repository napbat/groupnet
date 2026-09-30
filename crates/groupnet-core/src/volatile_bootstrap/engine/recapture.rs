//! A completed local image's Ready donor recapture.
//!
//! From a local build until its Ready claim, across a recapture that a
//! membership change interrupts, and from a retired Ready capture until its
//! replacement, the builder keeps a renewed Building claim visible. A follower
//! that samples in any of those windows waits for this image instead of
//! scanning the origin itself. A pending recapture makes no progress, so the
//! claim is renewed for at most the builder's own stall bound from when the
//! image became pending; a failed attempt does not extend that window.
//!
//! Attempts are paced. Each clones and encodes the whole index, which on a
//! loaded one-CPU node costs a CPU for a second or more and can itself make
//! the membership layer suspect this node. A failed attempt is retried only
//! after a backoff: one observation interval, doubling, capped at a quarter
//! of the stall bound. Inside the claim window the retry runs under any
//! complete cut; after it, only under a cut binding a different membership,
//! and that attempt opens a fresh window for the member that changed it.

use super::ClaimEngine;
use crate::Time;
use crate::volatile_bootstrap::{
    BootstrapEffect, BootstrapError, BootstrapOperation, BootstrapStage, BootstrapStep,
    ClaimIdentity, ClaimPhase, same_membership,
};

impl ClaimEngine {
    pub(super) fn local_only_built(
        &mut self,
        op: BootstrapOperation,
        selected: ClaimIdentity,
    ) -> BootstrapStep {
        if self.stage != BootstrapStage::Building
            || self.operation != Some(op)
            || self.selected.as_ref() != Some(&selected)
            || selected != self.identity()
        {
            return Self::reject(BootstrapError::StaleOperation);
        }
        self.operation = None;
        self.operation_due = None;
        self.total_due = None;
        self.settle_due = None;
        self.follow_due = None;
        self.claim_poll_due = None;
        self.claim_refresh_due = None;
        self.roster_poll = None;
        self.roster_poll_due = None;
        // The completed local image remains usable, but its pre-scan roster
        // must not prevent a later Ready recapture from accepting a new cut.
        self.participant_roster = None;
        self.failed_recapture = None;
        self.recapture_retry_due = None;
        self.recapture_failures = 0;
        self.stage = BootstrapStage::DonorAvailable;
        self.local_phase = ClaimPhase::Building;
        if self.participation_required {
            // A Ready recapture follows, and its claim supersedes this one.
            // Withdrawing it now would let a follower sampling before the
            // recapture scan the origin again.
            if self.await_recapture().is_err() {
                return self.terminate();
            }
            return self.ok(Vec::new());
        }
        self.renew_due = None;
        self.ok(vec![BootstrapEffect::WithdrawClaim(selected)])
    }

    /// Start one bounded recapture of the completed local image under the
    /// complete participation cut the worker just verified. That cut is
    /// consumed here. Nothing starts before the backoff after a failed
    /// attempt has passed. Once the claim window has closed, a cut binding
    /// the same membership as the one the last attempt failed under starts
    /// nothing either: only a membership change or a retired capture retries
    /// then, so a failure that neither caused never loops, and no joiner
    /// waits on it. A membership change opens a fresh claim window.
    pub(super) fn start_ready_recapture(&mut self) -> BootstrapStep {
        if !self.ready_recapture_due() {
            return Self::reject(BootstrapError::Stage);
        }
        let Some(roster) = self.participant_roster.take() else {
            return Self::reject(BootstrapError::Stage);
        };
        if self.recapture_due.is_none() {
            let changed = self
                .failed_recapture
                .as_deref()
                .is_none_or(|failed| !same_membership(failed, &roster));
            if !changed {
                return self.ok(Vec::new());
            }
            if self.await_recapture().is_err() {
                return self.terminate();
            }
        }
        let Some(generation) = self.generation.checked_add(1) else {
            return self.terminate();
        };
        self.generation = generation;
        self.local_renewal = 0;
        self.local_progress = 0;
        self.stage = BootstrapStage::Building;
        self.observed.retain(|identity, _| identity.node != self.me);
        self.recapture_roster = Some(roster);
        let selected = self.identity();
        self.selected = Some(selected.clone());
        let Ok(claim) = self.publish_renewal() else {
            return self.terminate();
        };
        let Ok(op) = self.operation(self.config.donor_wait_ms) else {
            return self.terminate();
        };
        self.ok(vec![
            claim,
            BootstrapEffect::RecaptureCurrent { op, selected },
        ])
    }

    /// Whether the running Building operation is a Ready recapture rather
    /// than an origin build.
    pub(super) fn recapturing(&self) -> bool {
        self.stage == BootstrapStage::Building && self.recapture_roster.is_some()
    }

    /// A Ready recapture failed or outlived its bound. The local image and
    /// local serving are unaffected, so this is not an origin fallback: the
    /// recapture becomes pending again and waits out its backoff. Inside the
    /// claim window its claim stays renewed; once the window has passed, the
    /// claim is withdrawn and only a membership change retries.
    pub(super) fn recapture_failed(&mut self) -> BootstrapStep {
        let mut effects = Vec::new();
        if let Some(op) = self.operation.take() {
            effects.push(BootstrapEffect::CancelWork { op });
        }
        self.failed_recapture = self.recapture_roster.take();
        self.operation_due = None;
        self.participant_roster = None;
        self.stage = BootstrapStage::DonorAvailable;
        self.recapture_failures = self.recapture_failures.saturating_add(1);
        let (Some(retry), Some(renew)) = (
            self.now.0.checked_add(self.recapture_backoff()),
            self.now.0.checked_add(self.config.renew_ms),
        ) else {
            return self.terminate();
        };
        self.recapture_retry_due = Some(Time(retry));
        if self.recapture_due.is_some_and(|due| self.now < due) {
            self.renew_due.get_or_insert(Time(renew));
            return self.ok(effects);
        }
        self.recapture_due = None;
        self.renew_due = None;
        effects.push(BootstrapEffect::WithdrawClaim(self.identity()));
        self.ok(effects)
    }

    /// The pause after the latest of `recapture_failures` consecutive failed
    /// attempts: one observation interval, doubling, capped at a quarter of
    /// the builder's stall bound but never under one interval. A claim window
    /// of `donor_wait_ms` so holds a bounded few attempts, and after it each
    /// membership change costs at most one capture per capped backoff.
    fn recapture_backoff(&self) -> u64 {
        let cap = (self.config.donor_wait_ms / 4).max(self.config.observe_ms);
        let doubling = 1_u64
            .checked_shl(self.recapture_failures.saturating_sub(1))
            .unwrap_or(u64::MAX);
        self.config.observe_ms.saturating_mul(doubling).min(cap)
    }

    /// The capture behind this node's donor role was retired while its local
    /// image still serves: its membership changed, it expired, or a recovery
    /// lapse suspended the Ready generation it was taken under. One recapture
    /// follows under the next complete verified cut, whatever an earlier
    /// recapture failed under, once any backoff from such a failure has
    /// passed; a newly Alive member may not have presence yet, so the old cut
    /// is dropped. Until then a joiner waits for this image: a Ready claim, or
    /// one already withdrawn, is superseded by a fresh attempt's Building
    /// claim, and a pending Building claim stays, for a fresh claim window.
    pub(super) fn capture_retired(&mut self) -> BootstrapStep {
        self.participant_roster = None;
        self.failed_recapture = None;
        let mut effects = Vec::new();
        if self.local_phase == ClaimPhase::Ready || self.recapture_due.is_none() {
            let Some(generation) = self.generation.checked_add(1) else {
                return self.terminate();
            };
            self.generation = generation;
            self.local_phase = ClaimPhase::Building;
            self.local_renewal = 0;
            self.local_progress = 0;
            self.observed.retain(|identity, _| identity.node != self.me);
            self.selected = Some(self.identity());
            let Ok(claim) = self.publish_renewal() else {
                return self.terminate();
            };
            effects.push(claim);
        }
        if self.await_recapture().is_err() {
            return self.terminate();
        }
        self.ok(effects)
    }

    /// Keep this node's claim renewed while its completed local image awaits
    /// a Ready recapture, until the builder's own stall bound from now.
    fn await_recapture(&mut self) -> Result<(), BootstrapError> {
        let (Some(due), Some(renew)) = (
            self.now.0.checked_add(self.config.donor_wait_ms),
            self.now.0.checked_add(self.config.renew_ms),
        ) else {
            return Err(BootstrapError::Exhausted);
        };
        self.recapture_due = Some(Time(due));
        self.renew_due.get_or_insert(Time(renew));
        Ok(())
    }

    /// Stop renewing a pending recapture's claim and withdraw it. The
    /// recapture stays pending: a later cut binding a different membership,
    /// or a retired capture, still starts it under a fresh claim.
    pub(super) fn release_pending_claim(&mut self) -> BootstrapStep {
        self.recapture_due = None;
        self.renew_due = None;
        self.ok(vec![BootstrapEffect::WithdrawClaim(self.identity())])
    }
}
