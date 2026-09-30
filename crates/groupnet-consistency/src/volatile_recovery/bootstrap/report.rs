//! Operator-visible decisions of the one bootstrap worker.
//!
//! A peer bootstrap either saves an origin scan or silently costs one. These
//! reports let a consumer log, at its default level, why a node waited for a
//! peer, stopped waiting, or offered no donor image. They carry no authority
//! and no protocol state: the worker has already acted when it reports.

use groupnet_core::volatile_bootstrap::{ClaimIdentity, ReleaseReason};

/// One peer-bootstrap decision, in the order the worker makes them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BootstrapDecision {
    /// This node waits for `builder`'s origin build, or its pending Ready
    /// recapture, instead of scanning the origin itself.
    Following {
        /// The exact builder attempt being followed.
        builder: ClaimIdentity,
    },
    /// This node stopped waiting for `builder`'s image. It selects again,
    /// which may be its own origin scan.
    Released {
        /// The builder or donor attempt this node was waiting for.
        builder: ClaimIdentity,
        /// Why the wait ended.
        reason: ReleaseReason,
    },
    /// This node's completed local image starts one Ready donor recapture.
    RecaptureStarted {
        /// The new donor claim attempt the recapture advertises.
        donor: ClaimIdentity,
    },
    /// This node's completed local image did not start, or did not finish,
    /// its Ready donor recapture. A reason repeats only after another
    /// decision was reported in between.
    RecaptureDeclined {
        /// Why no Ready donor image was offered.
        reason: RecaptureDecline,
    },
    /// This node's peer bootstrap ended without an image for the reason
    /// given; the recovery continues on its own origin path, which the
    /// recovery adapter's fallback report names.
    Declined {
        /// Why the acquisition ended.
        reason: DeclineReason,
    },
}

/// Why one peer-bootstrap acquisition ended without an image.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeclineReason {
    /// The parent recovery operation expired or was fenced, by a gap, a
    /// lapse, or cancellation, before the acquisition finished.
    ParentExpired,
    /// The acquisition had no current parent binding to run under.
    Unbound,
    /// The claim core ended the selection in its origin fallback; any wait
    /// it abandoned was reported as `Released` first.
    SelectionEnded,
    /// The claim core was cancelled and could not start a fresh selection.
    Cancelled,
    /// The claim core refused a result as stale or invalid.
    Refused,
    /// Publishing this node's claim failed or timed out.
    ClaimPublishFailed,
    /// Publishing this node's presence failed or timed out.
    PresencePublishFailed,
    /// No complete participation cut was available for the local build or
    /// the peer transfer.
    NoParticipation,
    /// The local build finished, but its image could not be offered or
    /// was not accepted by the claim core.
    BuildNotAccepted,
    /// Memory admission refused the installed handoff's receipt.
    Admission,
}

/// Why a completed local image offered no Ready donor capture.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecaptureDecline {
    /// No complete participation cut was available: a live member had no
    /// current presence, or the source read failed. Each maintenance turn
    /// retries.
    NoCompleteCut,
    /// The claim window has closed, and the cut binds the same membership as
    /// the one the last recapture failed under; only a membership change or
    /// a retired capture retries.
    SameCut,
    /// The consumer refused or failed the capture; its own log names why.
    CaptureFailed,
    /// The recapture outlived its donor-wait bound, or could not keep its
    /// claim renewed meanwhile.
    TimedOut,
    /// Membership changed while the image was captured and encoded: a member
    /// joined, left, or restarted, or its presence lapsed. SWIM status and
    /// incarnation changes alone do not count.
    RosterChanged,
    /// The local Ready generation or the recapture's operation ended first.
    Superseded,
}

/// Receives the worker's decisions. It is called synchronously on the
/// worker, so it must return promptly and must not block.
pub trait BootstrapObserver: Send + Sync + 'static {
    /// One decision the worker just made.
    fn decided(&self, decision: &BootstrapDecision);
}
