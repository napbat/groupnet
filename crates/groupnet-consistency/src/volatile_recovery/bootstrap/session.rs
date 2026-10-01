//! One concrete claim/transfer child driven inside the recovery worker.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use groupnet_core::volatile_bootstrap::transfer::{TransferConfig, TransferEffect, TransferEvent};
use groupnet_core::volatile_bootstrap::{
    BootId, BootstrapConfig, BootstrapEffect, BootstrapEvent, BootstrapMemberIdentity,
    BootstrapOperation, BootstrapScope, BootstrapStage, ClaimEngine, ClaimIdentity,
};
use groupnet_core::volatile_recovery::RecoveryOperation;
use groupnet_core::{NodeId, Time};
use tokio::sync::Notify;

use super::admission::{AdmissionClass, AdmissionError, Reservation};
use super::driver::{AcquisitionBinding, BootstrapDriver, BootstrapOutcome};
use super::inbox::{DonorInbox, DonorInboxError, DonorSender};
use super::ports::{
    BootstrapCapabilities, ClaimObservationLimits, ClaimSource, DonorCapture, DonorPort,
    LogicalClock, TransferContext, TransferResources,
};
use super::report::{BootstrapDecision, BootstrapObserver, DeclineReason, RecaptureDecline};
use crate::volatile_recovery::{BoxRecoveryFuture, PublicationPermit, ReadyCapturePermit};

const MAX_DONOR_SERVICE_BATCH: usize = 32;

/// Source and memory limits for one optional bootstrap child.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BootstrapRuntimeConfig {
    /// Sans-IO advisory claim limits.
    pub claim: BootstrapConfig,
    /// Sans-IO private transfer limits.
    pub transfer: TransferConfig,
    /// Complete native claim snapshot bytes, checked before source allocation.
    pub max_claim_metadata_bytes: usize,
    /// Bounded incoming donor requests served by the same worker.
    pub donor_inbox_capacity: usize,
    /// Require a complete native participation cut. Donor capture and peer
    /// transfer then bind to that roster, which the bulk Offer and Barrier
    /// carry; a missing or changed roster declines peer transfer to guarded
    /// origin recovery rather than downgrading to claims-only proof. Without
    /// it, selection is claims-only and a peer handoff has no roster to
    /// verify, so it also fails closed to guarded origin recovery.
    pub require_participation: bool,
}

/// Opening the optional child failed before it could publish a claim.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BootstrapSessionError {
    /// Core identity or finite policy was invalid.
    Core,
    /// Aggregate memory admission could not reserve retained core metadata.
    Admission(AdmissionError),
    /// Donor ingress capacity was invalid.
    Inbox(DonorInboxError),
    /// A required finite absolute deadline is not representable.
    ClockRange,
}

/// `ClaimEngine`, `TransferSession`, and actual resource owners in one worker.
///
/// This type has no task or poll loop of its own. The existing recovery worker
/// calls `acquire`, `maintain`, and `cancel` under the original outer deadline.
pub struct BootstrapSession<C: ClaimSource, D: DonorPort> {
    claims: Arc<C>,
    donor: Arc<D>,
    admission: super::admission::ByteAdmission,
    config: BootstrapRuntimeConfig,
    scope: BootstrapScope,
    me: NodeId,
    boot: BootId,
    session: u64,
    engine: ClaimEngine,
    clock: LogicalClock,
    effects: VecDeque<BootstrapEffect>,
    inbox: DonorInbox,
    wake: Arc<Notify>,
    capture: Option<DonorCapture<D::Image>>,
    resources: TransferResources<D::Stage, D::Attachment, D::NativeBuffer>,
    recovery: Option<RecoveryOperation>,
    suspended_recovery: Option<RecoveryOperation>,
    permit: Option<PublicationPermit>,
    ready_guard: Option<ReadyCapturePermit>,
    child_parent: Option<BootstrapOperation>,
    transfer_context: Option<TransferContext>,
    observer: Option<Arc<dyn BootstrapObserver>>,
    /// The builder attempt last reported as followed.
    followed: Option<ClaimIdentity>,
    /// The recapture outcome last reported, so a pending recapture that keeps
    /// retrying on every maintenance turn reports each reason once.
    recapture_report: Option<RecaptureDecline>,
    _core_metadata: Reservation,
}

impl<C: ClaimSource, D: DonorPort> std::fmt::Debug for BootstrapSession<C, D> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BootstrapSession")
            .field("stage", &self.engine.stage())
            .field("recovery", &self.recovery)
            .finish_non_exhaustive()
    }
}

impl<C: ClaimSource, D: DonorPort> Drop for BootstrapSession<C, D> {
    fn drop(&mut self) {
        // Task cancellation cannot await source withdrawal. Unlink the local
        // ingress synchronously; native claim TTL bounds any lost withdrawal.
        self.drop_capture();
    }
}

impl<C: ClaimSource, D: DonorPort> BootstrapSession<C, D> {
    /// Creates one child and its bounded donor request sender.
    ///
    /// # Errors
    /// Rejects invalid identity, limits, global admission, or clock range.
    pub fn new(
        capabilities: BootstrapCapabilities<C, D>,
        config: BootstrapRuntimeConfig,
        scope: BootstrapScope,
        me: NodeId,
        boot: BootId,
        session: u64,
    ) -> Result<(Self, DonorSender), BootstrapSessionError> {
        let mut engine = ClaimEngine::new(config.claim, scope.clone(), me.clone(), boot, session)
            .map_err(|_| BootstrapSessionError::Core)?;
        engine
            .enable_transfer(config.transfer)
            .map_err(|_| BootstrapSessionError::Core)?;
        if config.require_participation {
            engine
                .require_participation()
                .map_err(|_| BootstrapSessionError::Core)?;
        }
        if config.max_claim_metadata_bytes == 0 {
            return Err(BootstrapSessionError::Core);
        }
        // Core retains an observed claim map plus binding, offer, barrier,
        // coverage, an effect copy, and transient completion copies. Twelve
        // complete transfer-metadata sets is deliberately conservative for
        // those simultaneous owners. Vec/BTreeMap element headers have their
        // own finite count-bound charge. Real image, suffix, native and wire
        // buffers are charged separately to their actual owners. This is an
        // admission bound, not a claim of byte-exact allocator RSS.
        let core_bytes = config
            .transfer
            .max_metadata_bytes
            .checked_mul(12)
            .and_then(|value| value.checked_add(config.max_claim_metadata_bytes))
            .and_then(|value| value.checked_add(config.claim.max_members.checked_mul(128)?))
            .and_then(|value| value.checked_add(config.transfer.max_cuts.checked_mul(64)?))
            .and_then(|value| value.checked_add(512))
            .ok_or(BootstrapSessionError::Core)?;
        let core_metadata = capabilities
            .admission
            .reserve(AdmissionClass::Inflight, core_bytes)
            .map_err(BootstrapSessionError::Admission)?;
        let (sender, inbox) =
            DonorInbox::new(config.donor_inbox_capacity).map_err(BootstrapSessionError::Inbox)?;
        let wake = inbox.wake();
        let clock = LogicalClock::start();
        if clock.absolute(Time(config.claim.total_ms)).is_none() {
            return Err(BootstrapSessionError::ClockRange);
        }
        Ok((
            Self {
                claims: capabilities.claims,
                donor: capabilities.donor,
                admission: capabilities.admission,
                config,
                scope,
                me,
                boot,
                session,
                engine,
                clock,
                effects: VecDeque::new(),
                inbox,
                wake,
                capture: None,
                resources: TransferResources::default(),
                recovery: None,
                suspended_recovery: None,
                permit: None,
                ready_guard: None,
                child_parent: None,
                transfer_context: None,
                observer: None,
                followed: None,
                recapture_report: None,
                _core_metadata: core_metadata,
            },
            sender,
        ))
    }

    /// Report every peer-bootstrap decision to `observer`, for example as
    /// operator log lines.
    #[must_use]
    pub fn with_observer(mut self, observer: Arc<dyn BootstrapObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    fn report(&self, decision: &BootstrapDecision) {
        if let Some(observer) = &self.observer {
            observer.decided(decision);
        }
    }

    /// Report why a completed local image offered no Ready capture, once per
    /// reason until another recapture outcome is reported.
    fn report_recapture(&mut self, reason: RecaptureDecline) {
        if self.recapture_report != Some(reason) {
            self.recapture_report = Some(reason);
            self.report(&BootstrapDecision::RecaptureDeclined { reason });
        }
    }

    /// End the acquisition without an image and report why.
    fn declined(&self, reason: DeclineReason) -> BootstrapOutcome {
        self.report(&BootstrapDecision::Declined { reason });
        BootstrapOutcome::Declined
    }

    fn now(&self) -> Time {
        self.clock.now()
    }

    fn absolute(&self, due: Time) -> Option<Instant> {
        self.clock.absolute(due)
    }

    fn operation_due(&self, op: BootstrapOperation, outer: Instant) -> Option<Instant> {
        self.engine
            .operation_deadline(op)
            .and_then(|due| self.absolute(due))
            .map(|due| due.min(outer))
    }

    fn tick_after_io(&mut self) -> bool {
        self.accept(BootstrapEvent::Tick(self.now()))
    }

    fn accept(&mut self, event: BootstrapEvent) -> bool {
        let step = self.engine.step(event);
        for effect in &step.effects {
            match effect {
                BootstrapEffect::FollowBuilder { selected, .. }
                    if self.followed.as_ref() != Some(selected) =>
                {
                    self.followed = Some(selected.clone());
                    self.report(&BootstrapDecision::Following {
                        builder: selected.clone(),
                    });
                }
                BootstrapEffect::Released { builder, reason } => {
                    self.followed = None;
                    self.report(&BootstrapDecision::Released {
                        builder: builder.clone(),
                        reason: *reason,
                    });
                }
                BootstrapEffect::RecaptureCurrent { selected, .. } => {
                    self.recapture_report = None;
                    self.report(&BootstrapDecision::RecaptureStarted {
                        donor: selected.clone(),
                    });
                }
                _ => {}
            }
        }
        self.effects.extend(step.effects);
        step.rejection.is_none()
    }

    fn claim_limits(&self, max_members: usize, max_member_bytes: usize) -> ClaimObservationLimits {
        ClaimObservationLimits {
            max_members,
            max_member_bytes,
            max_metadata_bytes: self.config.max_claim_metadata_bytes,
        }
    }

    fn retire_capture_if_invalid(&mut self) {
        if self
            .capture
            .as_ref()
            .is_some_and(|capture| !capture.is_active())
        {
            self.retire_donor_capture();
        }
    }

    fn retire_donor_capture(&mut self) {
        let selected = self.engine.selected().cloned();
        self.drop_capture();
        if let Some(selected) = selected {
            let _ = self.accept(BootstrapEvent::CaptureRetired { selected });
        }
    }

    fn drop_capture(&mut self) {
        self.inbox.set_identity(None);
        if let Some(capture) = self.capture.take() {
            self.donor.retire_local_capture(&capture);
            drop(capture);
        }
    }

    async fn service_pending(&mut self) {
        // Notify coalesces wakeups. Consume a finite batch and rearm once at
        // its end; continuous producers cannot starve recovery timers.
        let budget = self
            .config
            .donor_inbox_capacity
            .min(MAX_DONOR_SERVICE_BATCH);
        let mut served = 0;
        for _ in 0..budget {
            let Some(incoming) = self.inbox.try_recv() else {
                break;
            };
            let needs_roster = matches!(
                incoming.request(),
                super::ports::DonorRequest::Offer { .. }
                    | super::ports::DonorRequest::Barrier { .. }
                    | super::ports::DonorRequest::AdvanceBarrier { .. }
            );
            if self.config.require_participation && self.capture.is_some() && needs_roster {
                let due = Instant::now()
                    .checked_add(Duration::from_millis(self.config.claim.observe_ms))
                    .and_then(|due| {
                        self.capture
                            .as_ref()
                            .and_then(DonorCapture::next_deadline)
                            .and_then(|deadline| self.absolute(deadline))
                            .map_or(Some(due), |deadline| Some(due.min(deadline)))
                    });
                if !matches!(due, Some(due) if self.current_participation(due).await.is_some()) {
                    incoming.respond(Err(crate::volatile_recovery::AdapterError));
                    self.retire_donor_capture();
                    served += 1;
                    continue;
                }
            }
            // A preceding source callback may have held this worker past the
            // capture deadline. Revalidate at the request's linearization
            // point, not only at the start of the maintenance turn.
            if let Some(capture) = &self.capture {
                let _ = capture.tick(self.now());
            }
            self.retire_capture_if_invalid();
            let now = self.now();
            let reply = self.capture.as_ref().and_then(|capture| {
                capture.is_active().then(|| {
                    self.donor
                        .prepare_follower(incoming.request(), capture, now, &self.admission)
                })
            });
            incoming.respond(reply.unwrap_or(Err(crate::volatile_recovery::AdapterError)));
            served += 1;
        }
        if served == budget || !self.effects.is_empty() {
            // A redundant empty wake is harmless; a concurrently refilled
            // bounded inbox or newly queued claim withdrawal is guaranteed
            // another service turn.
            self.wake.notify_one();
        }
    }

    fn reset_engine(&mut self) -> bool {
        let Some(session) = self.session.checked_add(1) else {
            return false;
        };
        let Ok(mut engine) = ClaimEngine::new(
            self.config.claim,
            self.scope.clone(),
            self.me.clone(),
            self.boot,
            session,
        ) else {
            return false;
        };
        if engine.enable_transfer(self.config.transfer).is_err() {
            return false;
        }
        if self.config.require_participation && engine.require_participation().is_err() {
            return false;
        }
        self.session = session;
        self.engine = engine;
        self.effects.clear();
        self.resources = TransferResources::default();
        self.child_parent = None;
        self.transfer_context = None;
        true
    }
}

impl<C: ClaimSource, D: DonorPort> BootstrapDriver for BootstrapSession<C, D> {
    fn acquire(
        &mut self,
        recovery: RecoveryOperation,
        permit: PublicationPermit,
    ) -> BoxRecoveryFuture<'_, BootstrapOutcome> {
        Box::pin(async move { self.run_acquisition(recovery, permit).await })
    }

    fn cancel(&mut self, recovery: RecoveryOperation) -> BoxRecoveryFuture<'_, ()> {
        Box::pin(async move {
            if self.recovery != Some(recovery) {
                return;
            }
            let _ = self.accept(BootstrapEvent::RetireCandidate);
            self.retire_cancel_effects().await;
            self.recovery = None;
            self.suspended_recovery = None;
            self.permit = None;
            self.ready_guard = None;
            self.resources = TransferResources::default();
            self.child_parent = None;
            self.transfer_context = None;
        })
    }

    fn shutdown(&mut self) -> BoxRecoveryFuture<'_, ()> {
        Box::pin(async move {
            let _ = self.accept(BootstrapEvent::Cancel);
            self.retire_cancel_effects().await;
            self.recovery = None;
            self.suspended_recovery = None;
            self.permit = None;
            self.ready_guard = None;
            self.resources = TransferResources::default();
            self.child_parent = None;
            self.transfer_context = None;
        })
    }

    fn suspend_local(&mut self, recovery: RecoveryOperation) -> BoxRecoveryFuture<'_, ()> {
        Box::pin(async move {
            if self.recovery != Some(recovery)
                || self.engine.stage() != BootstrapStage::DonorAvailable
            {
                return;
            }
            // An adopted installed image may hold no capture yet: nothing is
            // retired, and its recapture still waits for a membership change.
            if self.capture.is_some() {
                self.retire_donor_capture();
            }
            self.permit = None;
            self.ready_guard = None;
            self.suspended_recovery = Some(recovery);
            self.drain_maintenance().await;
        })
    }

    fn resume_local(&mut self, previous: RecoveryOperation, current: RecoveryOperation) -> bool {
        if self.suspended_recovery != Some(previous)
            || self.recovery != Some(previous)
            || self.engine.stage() != BootstrapStage::DonorAvailable
            || !self.engine.ready_recapture_pending()
            || current.session != previous.session
            || current.generation != previous.generation.saturating_add(1)
            || current.token <= previous.token
        {
            return false;
        }
        self.recovery = Some(current);
        self.suspended_recovery = None;
        true
    }

    fn adopt_local(&mut self, current: RecoveryOperation) -> bool {
        if self.recovery.is_some()
            || self.suspended_recovery.is_some()
            || self.engine.stage() != BootstrapStage::Participating
            || !self.accept(BootstrapEvent::AdoptInstalled)
        {
            return false;
        }
        self.recovery = Some(current);
        true
    }

    fn peer_roster(
        &mut self,
        due: Instant,
    ) -> BoxRecoveryFuture<'_, Option<Vec<BootstrapMemberIdentity>>> {
        Box::pin(async move {
            if !self.config.require_participation {
                return None;
            }
            self.observe_peer_roster(due).await
        })
    }

    fn next_deadline(&self) -> Option<Instant> {
        let claim = self
            .engine
            .next_deadline()
            .and_then(|due| self.absolute(due));
        let capture = self
            .capture
            .as_ref()
            .and_then(DonorCapture::next_deadline)
            .and_then(|due| self.absolute(due));
        claim.into_iter().chain(capture).min()
    }

    fn wake(&self) -> Arc<Notify> {
        Arc::clone(&self.wake)
    }

    fn maintain(
        &mut self,
        now: Instant,
        ready: Option<ReadyCapturePermit>,
    ) -> BoxRecoveryFuture<'_, ()> {
        Box::pin(async move {
            let claim_timer_due = self
                .engine
                .next_deadline()
                .and_then(|due| self.absolute(due))
                .is_some_and(|due| due <= now);
            if let Some(capture) = &self.capture {
                let _ = capture.tick(self.now());
            }
            self.retire_capture_if_invalid();
            let _ = self.accept(BootstrapEvent::Tick(self.now()));
            // Publish the core's due renewal before sampling it back from the
            // native source; otherwise an ordinary renewal tick appears to
            // contradict this worker's own claim sequence.
            self.drain_maintenance().await;
            // Ready claims outlive the initial acquisition. Recheck their C
            // roster on this existing maintenance turn, before any incoming
            // follower can consume an obsolete offer. A changed or uncertain
            // source cut withdraws donor availability; local serving stays up.
            if claim_timer_due && self.config.require_participation && self.capture.is_some() {
                let due =
                    Instant::now().checked_add(Duration::from_millis(self.config.claim.observe_ms));
                if !matches!(due, Some(due) if self.current_participation(due).await.is_some()) {
                    self.retire_donor_capture();
                }
            }
            self.drain_maintenance().await;
            self.ready_guard = None;
            // A failed recapture waits out its backoff before the next cut
            // is even sampled for it: each attempt clones the whole index.
            if self.engine.ready_recapture_due()
                && let Some(deadline) = Instant::now()
                    .checked_add(Duration::from_millis(self.config.claim.donor_wait_ms))
                && let Some(guard) = ready.map(|guard| guard.restricted_to(deadline))
                && guard
                    .capture(|generation| Some(generation) == self.recovery.map(|op| op.generation))
                    == Some(true)
            {
                if self.current_participation(deadline).await.is_some() {
                    self.ready_guard = Some(guard);
                    let _ = self.accept(BootstrapEvent::StartReadyRecapture);
                    if self.engine.stage() != BootstrapStage::Building {
                        self.report_recapture(RecaptureDecline::SameCut);
                    }
                } else {
                    self.report_recapture(RecaptureDecline::NoCompleteCut);
                }
            }
            self.drain_maintenance().await;
            self.service_pending().await;
        })
    }
}

mod run;
