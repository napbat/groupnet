//! One bounded Ready recapture of a completed local image.

use super::super::RecaptureDecline;
use super::{
    Arc, BootstrapEvent, BootstrapSession, ClaimIdentity, ClaimSource, DonorPort, Instant,
    ReadyCaptureRequest,
};

impl<C: ClaimSource, D: DonorPort> BootstrapSession<C, D> {
    /// One bounded recapture of the completed local image. Any failure only
    /// makes the recapture pending again in the core; it is retried after a
    /// backoff, as the core decides, and never scans the origin.
    pub(super) async fn recapture_current(
        &mut self,
        op: groupnet_core::volatile_bootstrap::BootstrapOperation,
        selected: ClaimIdentity,
        outer: Instant,
    ) {
        let Some(deadline) = self.operation_due(op, outer) else {
            return;
        };
        let Err(reason) = self.capture_ready(op, &selected, deadline).await else {
            return;
        };
        self.report_recapture(reason);
        if self.engine.current_operation() == Some(op) {
            let _ = self.accept(BootstrapEvent::BuildFailed { op, selected });
        }
    }

    /// Capture, encode and publish the Ready image under `deadline`, or say
    /// why it did not.
    async fn capture_ready(
        &mut self,
        op: groupnet_core::volatile_bootstrap::BootstrapOperation,
        selected: &ClaimIdentity,
        deadline: Instant,
    ) -> Result<(), RecaptureDecline> {
        let (Some(guard), Some(recovery_generation)) = (
            self.ready_guard
                .take()
                .map(|guard| guard.restricted_to(deadline)),
            self.recovery.map(|recovery| recovery.generation),
        ) else {
            return Err(RecaptureDecline::Superseded);
        };
        let members = self
            .current_participation(deadline)
            .await
            .ok_or(RecaptureDecline::NoCompleteCut)?;
        let donor = Arc::clone(&self.donor);
        let admission = self.admission.clone();
        let capture = donor.recapture_current_index(
            ReadyCaptureRequest {
                operation: op,
                selected: selected.clone(),
                recovery_generation,
                members: members.clone(),
                guard: guard.clone(),
                deadline,
                clock: self.clock,
                wake: Arc::clone(&self.wake),
            },
            &admission,
        );
        let built = self
            .await_renewing(op, None, || Some(deadline), capture)
            .await;
        let _ = self.tick_after_io();
        let capture = match built {
            Some(Ok(capture)) => capture,
            Some(Err(_)) => return Err(RecaptureDecline::CaptureFailed),
            None => return Err(RecaptureDecline::TimedOut),
        };
        let current =
            guard.valid() && self.engine.current_operation() == Some(op) && capture.is_active();
        let decline = if !current {
            Some(RecaptureDecline::Superseded)
        } else if self.current_participation(deadline).await.as_deref() != Some(members.as_slice())
        {
            Some(RecaptureDecline::RosterChanged)
        } else if !guard.valid() {
            Some(RecaptureDecline::Superseded)
        } else {
            None
        };
        if let Some(decline) = decline {
            self.donor.retire_local_capture(&capture);
            drop(capture);
            return Err(decline);
        }
        self.capture = Some(capture);
        if !self.accept(BootstrapEvent::Built {
            op,
            selected: selected.clone(),
        }) {
            self.drop_capture();
            return Err(RecaptureDecline::Superseded);
        }
        self.recapture_report = None;
        self.inbox.set_identity(Some(selected.clone()));
        self.drain_ready_publication(deadline).await;
        Ok(())
    }
}
