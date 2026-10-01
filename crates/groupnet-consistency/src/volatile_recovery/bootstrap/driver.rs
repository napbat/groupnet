//! One optional child of the existing volatile recovery worker.
//!
//! The child owns advisory claim and private transfer resources. It never
//! opens the read gate or spawns another recovery task.

use std::sync::Arc;
use std::time::Instant;

use tokio::sync::Notify;

use groupnet_core::volatile_bootstrap::transfer::NativeHandoffReceipt;
use groupnet_core::volatile_bootstrap::{BootstrapMemberIdentity, BootstrapOperation};
use groupnet_core::volatile_recovery::{RecoveryEvent, RecoveryOperation};

use super::admission::Admitted;
use crate::volatile_recovery::{BoxRecoveryFuture, PublicationPermit, ReadyCapturePermit};

/// One bounded acquisition outcome under the original outer operation.
#[derive(Debug)]
pub enum BootstrapOutcome {
    /// Guarded local origin image completed once; donor capture is optional.
    LocalBuilt,
    /// Exact private peer candidate installed and native delivery attached.
    PeerInstalled(Admitted<Box<NativeHandoffReceipt>>),
    /// Peer source, transfer, or admission failed; use guarded origin fallback.
    Declined,
}

/// Current exact mapping between independent recovery and claim allocators.
///
/// The worker creates a fresh mapping for each `AcquireBaseline` effect and
/// clears it before supersession. An old child result cannot be repackaged
/// as the new outer operation, even if its own transfer proof is internally
/// valid. The recovery core independently checks the receipt's outer binding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AcquisitionBinding {
    /// Current outer baseline operation and publication permit.
    pub recovery: RecoveryOperation,
    /// Claim-selected child parent operation from the same active episode.
    pub child: BootstrapOperation,
}

impl AcquisitionBinding {
    /// Whether this receipt belongs to both exact active operation allocators.
    #[must_use]
    pub fn accepts(self, current: RecoveryOperation, handoff: &NativeHandoffReceipt) -> bool {
        self.recovery == current
            && handoff.recovery == current
            && handoff.coverage.parent == self.child
            && handoff.install.session == self.child.session
            && handoff.install.incarnation == self.child.incarnation
            && handoff.install.generation == self.child.generation
            && handoff.install.token > self.child.token
    }

    /// Converts a completed child result into an exact outer recovery event.
    ///
    /// # Errors
    /// Rejects stale or cross-session results before they reach the recovery
    /// engine. The child and outer session numbers need not be equal.
    pub fn installed(
        self,
        current: RecoveryOperation,
        handoff: Box<NativeHandoffReceipt>,
    ) -> Result<RecoveryEvent, BootstrapBindingError> {
        if !self.accepts(current, &handoff) {
            return Err(BootstrapBindingError::Stale);
        }
        Ok(RecoveryEvent::PeerBaselineInstalled {
            op: current,
            handoff,
        })
    }
}

/// A child completion or callback cannot be mapped to the current episode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BootstrapBindingError {
    /// Recovery or child operation has been replaced or cancelled.
    Stale,
}

/// Type-erased, opt-in child held by the one existing recovery worker.
///
/// A concrete implementation owns one `ClaimEngine`, one `TransferSession`
/// child, optional `DonorCapture` after local Ready, a bounded `DonorInbox`, and
/// its worker-owned private stage. Every callback and timer is driven in the
/// existing worker task. The acquisition future is cancelled by dropping it
/// after the public gate was synchronously fenced; cancel then emits exact
/// core cleanup effects. Donor-only expiry withdraws availability without
/// closing an otherwise healthy local read gate.
pub trait BootstrapDriver: Send + 'static {
    /// Begin one bounded acquisition under the parent's permit. Its deadline
    /// is the permit's current one, renewed while the acquisition reports
    /// progress through [`PublicationPermit::progress`].
    fn acquire(
        &mut self,
        recovery: RecoveryOperation,
        permit: PublicationPermit,
    ) -> BoxRecoveryFuture<'_, BootstrapOutcome>;

    /// Retire exact candidate work while keeping this process's scoped presence.
    fn cancel(&mut self, recovery: RecoveryOperation) -> BoxRecoveryFuture<'_, ()>;

    /// Terminal worker shutdown withdraws process presence as well as candidates.
    fn shutdown(&mut self) -> BoxRecoveryFuture<'_, ()>;

    /// Withdraw the exact old donor capture on a locally built baseline's
    /// lease lapse, retaining only presence until the outer proof completes.
    fn suspend_local(&mut self, recovery: RecoveryOperation) -> BoxRecoveryFuture<'_, ()>;

    /// Bind a suspended local image to a newly affirmed outer generation.
    /// No old publication permission or donor capture becomes valid again.
    fn resume_local(&mut self, previous: RecoveryOperation, current: RecoveryOperation) -> bool;

    /// Bind the peer image this child installed, now backing the affirmed
    /// Ready generation of `current`, as its local image: it may then offer a
    /// Ready recapture to a later joiner, like an origin build. Returns
    /// whether the child holds such an image.
    fn adopt_local(&mut self, current: RecoveryOperation) -> bool;

    /// One complete native participation roster for the recovery core's
    /// post-handoff peer check; `None` if participation is not required or
    /// the cut is incomplete, stale, or malformed. A hint, never authority:
    /// the core itself compares it with the handoff's covered members.
    fn peer_roster(
        &mut self,
        due: Instant,
    ) -> BoxRecoveryFuture<'_, Option<Vec<BootstrapMemberIdentity>>>;

    /// Earliest claim renewal, journal expiry, or source operation deadline.
    fn next_deadline(&self) -> Option<Instant>;

    /// Wakes the existing recovery worker for bounded donor ingress or
    /// synchronous journal invalidation; no second worker is spawned.
    fn wake(&self) -> Arc<Notify>;

    /// Drive due source/capture maintenance under the current outer Ready
    /// guard, if that exact recovery generation is open.
    fn maintain(
        &mut self,
        now: Instant,
        ready: Option<ReadyCapturePermit>,
    ) -> BoxRecoveryFuture<'_, ()>;
}
