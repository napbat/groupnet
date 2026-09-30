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
}

/// Why a completed local image offered no Ready donor capture.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecaptureDecline {
    /// No complete participation cut was available: a live member had no
    /// current presence, or the source read failed. Each maintenance turn
    /// retries.
    NoCompleteCut,
    /// The cut equals the one the last recapture failed under; only a
    /// membership change or a retired capture retries.
    SameCut,
    /// The consumer refused or failed the capture; its own log names why.
    CaptureFailed,
    /// The recapture outlived its donor-wait bound, or could not keep its
    /// claim renewed meanwhile.
    TimedOut,
    /// Membership changed while the image was captured and encoded.
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
