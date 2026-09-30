//! A completed local image's Ready donor recapture.
//!
//! Between a local build and its Ready claim, and across a recapture that a
//! membership change interrupts, the builder's claim stays visible: the next
//! recapture's claim supersedes it, or its native TTL expires it. A follower
//! that samples in that window therefore waits for this image instead of
//! scanning the origin itself.

use super::ClaimEngine;
use crate::volatile_bootstrap::{
    BootstrapEffect, BootstrapError, BootstrapOperation, BootstrapStage, BootstrapStep,
    ClaimIdentity, ClaimPhase,
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
        self.renew_due = None;
        self.follow_due = None;
        self.claim_poll_due = None;
        self.claim_refresh_due = None;
        self.roster_poll = None;
        self.roster_poll_due = None;
        // The completed local image remains usable, but its pre-scan roster
        // must not prevent a later Ready recapture from accepting a new cut.
        self.participant_roster = None;
        self.stage = BootstrapStage::DonorAvailable;
        self.local_phase = ClaimPhase::Building;
        if self.participation_required {
            // A Ready recapture follows. Its claim supersedes this unrenewed
            // one, or native TTL expires it; withdrawing it now would let a
            // follower sampling before the recapture scan the origin again.
            return self.ok(Vec::new());
        }
        self.ok(vec![BootstrapEffect::WithdrawClaim(selected)])
    }

    /// Start one bounded recapture of the completed local image under the
    /// complete participation cut the worker just verified. That cut is
    /// consumed here. A cut equal to the one a previous recapture failed
    /// under starts nothing: only a membership change retries a recapture,
    /// so a failure the roster did not cause never loops.
    pub(super) fn start_ready_recapture(&mut self) -> BootstrapStep {
        if !self.ready_recapture_pending() {
            return Self::reject(BootstrapError::Stage);
        }
        let Some(roster) = self.participant_roster.take() else {
            return Self::reject(BootstrapError::Stage);
        };
        if self.failed_recapture.as_ref() == Some(&roster) {
            return self.ok(Vec::new());
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
    /// recapture becomes pending again and retries only under a different
    /// complete participation cut. Its claim is not withdrawn, for the same
    /// reason as at [`Self::local_only_built`].
    pub(super) fn recapture_failed(&mut self) -> BootstrapStep {
        let mut effects = Vec::new();
        if let Some(op) = self.operation.take() {
            effects.push(BootstrapEffect::CancelWork { op });
        }
        self.failed_recapture = self.recapture_roster.take();
        self.operation_due = None;
        self.renew_due = None;
        self.participant_roster = None;
        self.stage = BootstrapStage::DonorAvailable;
        self.ok(effects)
    }
}
