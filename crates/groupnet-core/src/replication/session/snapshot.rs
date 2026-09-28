//! Opt-in snapshot transition handlers; source and application work stay in the shell.

use super::SessionEngine;
use crate::Time;
use crate::replication::{
    ApplyReceipt, Batch, BoundComparison, ChunkReceipt, Comparison, Cursor, Effect, HoldReceipt,
    Mode, Operation, Reject, SnapshotCleanupDisposition, SnapshotOffer, SourceProof, Stage, Step,
};

#[derive(Clone, Debug)]
pub(super) struct Progress {
    attempt: Option<Operation>,
    pub(super) total_due: Time,
    pub(super) attached: bool,
    pub(super) attach_head: Option<Cursor>,
    hold: Option<HoldReceipt>,
    offer: Option<SnapshotOffer>,
    next_index: u32,
    next_offset: u64,
    cursor: Option<Cursor>,
    barrier: Option<SourceProof>,
    batch: Option<Batch>,
    pending_chunk: Option<ChunkReceipt>,
    candidate_id: Option<u64>,
}

#[derive(Clone, Debug)]
pub(super) struct CleanupState {
    pub(super) op: Operation,
    attempt: Operation,
    disposition: SnapshotCleanupDisposition,
    pub(super) due: Time,
    pub(super) discarding: bool,
}

impl SessionEngine {
    fn snapshot_limits(&self) -> crate::replication::SnapshotConfig {
        self.config.snapshot.expect("active snapshot is configured")
    }

    fn snapshot_live(&self, op: Operation, stage: Stage) -> Result<(), Reject> {
        if !self.matches(op, stage) {
            return Err(Reject::StaleOperation);
        }
        if self
            .snapshot
            .as_ref()
            .is_none_or(|progress| self.now >= progress.total_due)
        {
            return Err(Reject::StaleOperation);
        }
        Ok(())
    }

    fn snapshot_effect(&mut self, stage: Stage, effect: impl FnOnce(Operation) -> Effect) -> Step {
        let Ok(op) = self.issue(stage) else {
            return self.abort_snapshot();
        };
        Step::ok(vec![
            effect(op),
            Effect::ArmTimer(self.next_deadline().expect("snapshot deadline")),
        ])
    }

    pub(super) fn start_snapshot(&mut self) -> Step {
        if self.mode != Mode::StateSync
            || self.config.snapshot.is_none()
            || self.snapshot.is_some()
            || self.snapshot_cleanup.is_some()
            || self.outstanding.is_some()
            || !matches!(
                self.state.stage,
                Stage::NeedsSnapshot | Stage::SnapshotAborted | Stage::Unready
            )
        {
            return Step::reject(Reject::Stage);
        }
        let Some(total) = self.now.0.checked_add(self.snapshot_limits().max_total_ms) else {
            return Step::reject(Reject::Exhausted);
        };
        let closed = self.close_gate();
        if closed.rejection.is_some() {
            return closed;
        }
        self.snapshot = Some(Progress {
            attempt: None,
            total_due: Time(total),
            attached: false,
            attach_head: None,
            hold: None,
            offer: None,
            next_index: 0,
            next_offset: 0,
            cursor: None,
            barrier: None,
            batch: None,
            pending_chunk: None,
            candidate_id: None,
        });
        let scope = self.scope.clone();
        let mut step = self.snapshot_effect(Stage::SnapshotHolding, move |op| {
            Effect::AcquireSnapshotHold {
                op,
                scope,
                total_due: Time(total),
            }
        });
        if step.rejection.is_none() {
            let attempt = self.current_operation();
            self.snapshot.as_mut().expect("active").attempt = attempt;
        }
        step.effects.splice(0..0, closed.effects);
        step
    }

    pub(super) fn snapshot_held(&mut self, op: Operation, receipt: HoldReceipt) -> Step {
        if let Err(error) = self.snapshot_live(op, Stage::SnapshotHolding) {
            return Step::reject(error);
        }
        let Some(progress) = self.snapshot.as_ref() else {
            return Step::reject(Reject::Stage);
        };
        if receipt.request != op
            || receipt.history.source.is_empty()
            || receipt.history.source.len() > self.config.max_cursor_bytes
            || receipt.certificate.is_empty()
            || receipt.certificate.len() > self.snapshot_limits().max_metadata_bytes
            || receipt.retained_until < progress.total_due
        {
            return Step::reject(Reject::Discontinuity);
        }
        self.snapshot.as_mut().expect("active").hold = Some(receipt);
        let scope = self.scope.clone();
        self.snapshot_effect(Stage::SnapshotOffering, move |op| Effect::OfferSnapshot {
            op,
            scope,
        })
    }

    pub(super) fn snapshot_offered(&mut self, op: Operation, offer: SnapshotOffer) -> Step {
        if let Err(error) = self.snapshot_live(op, Stage::SnapshotOffering) {
            return Step::reject(error);
        }
        let limit = self.snapshot_limits();
        let Some(hold) = self
            .snapshot
            .as_ref()
            .and_then(|progress| progress.hold.as_ref())
        else {
            return Step::reject(Reject::Stage);
        };
        if offer.scope != self.scope
            || offer.cut.history != hold.history
            || offer.schema.is_empty()
            || offer.digest.is_empty()
            || offer.certificate.is_empty()
            || offer
                .charged_metadata_bytes()
                .is_none_or(|n| n > limit.max_metadata_bytes)
            || offer.total_bytes > limit.max_snapshot_bytes
            || offer.chunks > limit.max_chunks
            || offer.total_bytes == 0
            || offer.chunks == 0
            || offer.total_bytes < u64::from(offer.chunks)
            || u64::from(offer.chunks)
                .checked_mul(limit.max_chunk_bytes as u64)
                .is_none_or(|capacity| offer.total_bytes > capacity)
        {
            return Step::reject(Reject::Backpressure);
        }
        if let Err(error) = offer
            .cut
            .validate(&self.scope, self.config.max_cursor_bytes)
        {
            return Step::reject(Reject::Identity(error));
        }
        if let Err(error) = offer
            .proof
            .validate(&self.scope, self.config.max_cursor_bytes)
        {
            return Step::reject(Reject::Identity(error));
        }
        if offer.proof.head.history != hold.history
            || offer
                .cut_to_head
                .for_operands(&offer.cut, &offer.proof.head, &offer.proof.id)
                .is_none_or(|order| !matches!(order, Comparison::Before | Comparison::Equal))
            || offer
                .retained_to_cut
                .for_operands(&offer.proof.retained_from, &offer.cut, &offer.proof.id)
                .is_none_or(|order| !matches!(order, Comparison::Before | Comparison::Equal))
        {
            return Step::reject(Reject::Discontinuity);
        }
        self.snapshot.as_mut().expect("active").cursor = Some(offer.cut.clone());
        self.snapshot.as_mut().expect("active").offer = Some(offer.clone());
        self.snapshot_effect(Stage::SnapshotOpening, move |op| {
            Effect::OpenSnapshotStage {
                op,
                offer: Box::new(offer),
                max_candidate_bytes: limit.max_candidate_bytes,
            }
        })
    }

    pub(super) fn snapshot_opened(&mut self, op: Operation, charged_bytes: u64) -> Step {
        if let Err(error) = self.snapshot_live(op, Stage::SnapshotOpening) {
            return Step::reject(error);
        }
        if charged_bytes > self.snapshot_limits().max_candidate_bytes {
            return Step::reject(Reject::Backpressure);
        }
        self.snapshot_next_chunk()
    }

    fn snapshot_next_chunk(&mut self) -> Step {
        let Some(progress) = self.snapshot.as_ref() else {
            return Step::reject(Reject::Stage);
        };
        let Some(offer) = progress.offer.as_ref() else {
            return Step::reject(Reject::Stage);
        };
        if progress.next_index == offer.chunks {
            if progress.next_offset != offer.total_bytes {
                return Step::reject(Reject::Discontinuity);
            }
            let digest = offer.digest.clone();
            return self.snapshot_effect(Stage::SnapshotVerifying, move |op| {
                Effect::VerifySnapshotImage { op, digest }
            });
        }
        let index = progress.next_index;
        let offset = progress.next_offset;
        let remaining = offer.total_bytes - offset;
        let max_bytes = usize::try_from(remaining)
            .unwrap_or(usize::MAX)
            .min(self.snapshot_limits().max_chunk_bytes);
        self.snapshot_effect(Stage::SnapshotReading, move |op| {
            Effect::ReadSnapshotChunk {
                op,
                index,
                offset,
                max_bytes,
            }
        })
    }

    pub(super) fn snapshot_read(&mut self, op: Operation, chunk: ChunkReceipt) -> Step {
        if let Err(error) = self.snapshot_live(op, Stage::SnapshotReading) {
            return Step::reject(error);
        }
        let Some(progress) = self.snapshot.as_ref() else {
            return Step::reject(Reject::Stage);
        };
        let Some(offer) = progress.offer.as_ref() else {
            return Step::reject(Reject::Stage);
        };
        if chunk.index != progress.next_index
            || chunk.offset != progress.next_offset
            || chunk.payload_id != op.token
            || chunk.bytes == 0
            || chunk.bytes > self.snapshot_limits().max_chunk_bytes
            || chunk
                .offset
                .checked_add(chunk.bytes as u64)
                .is_none_or(|end| end > offer.total_bytes)
        {
            return Step::reject(Reject::Discontinuity);
        }
        self.snapshot.as_mut().expect("active").pending_chunk = Some(chunk);
        self.snapshot_effect(Stage::SnapshotWriting, move |op| {
            Effect::WriteSnapshotChunk { op, chunk }
        })
    }

    pub(super) fn snapshot_written(
        &mut self,
        op: Operation,
        index: u32,
        through: u64,
        charged_bytes: u64,
    ) -> Step {
        if let Err(error) = self.snapshot_live(op, Stage::SnapshotWriting) {
            return Step::reject(error);
        }
        let max_candidate_bytes = self.snapshot_limits().max_candidate_bytes;
        let Some(progress) = self.snapshot.as_mut() else {
            return Step::reject(Reject::Stage);
        };
        let Some(offer) = progress.offer.as_ref() else {
            return Step::reject(Reject::Stage);
        };
        let Some(chunk) = progress.pending_chunk else {
            return Step::reject(Reject::Stage);
        };
        if index != progress.next_index
            || through != chunk.offset + chunk.bytes as u64
            || through > offer.total_bytes
            || charged_bytes > max_candidate_bytes
        {
            return Step::reject(Reject::Discontinuity);
        }
        let Some(next) = progress.next_index.checked_add(1) else {
            return Step::reject(Reject::Exhausted);
        };
        progress.next_index = next;
        progress.next_offset = through;
        progress.pending_chunk = None;
        self.snapshot_next_chunk()
    }

    pub(super) fn snapshot_verified(&mut self, op: Operation, charged_bytes: u64) -> Step {
        if let Err(error) = self.snapshot_live(op, Stage::SnapshotVerifying) {
            return Step::reject(error);
        }
        let max_candidate_bytes = self.snapshot_limits().max_candidate_bytes;
        let Some(progress) = self.snapshot.as_ref() else {
            return Step::reject(Reject::Stage);
        };
        if charged_bytes > max_candidate_bytes {
            return Step::reject(Reject::Backpressure);
        }
        let from = progress.cursor.clone().expect("offered cut");
        self.snapshot_effect(Stage::SnapshotBarrier, move |op| {
            Effect::SnapshotReplayBarrier { op, from }
        })
    }

    pub(super) fn snapshot_barrier(
        &mut self,
        op: Operation,
        proof: SourceProof,
        comparisons: &[BoundComparison],
    ) -> Step {
        if let Err(error) = self.snapshot_live(op, Stage::SnapshotBarrier) {
            return Step::reject(error);
        }
        if let Err(error) = proof.validate(&self.scope, self.config.max_cursor_bytes) {
            return Step::reject(Reject::Identity(error));
        }
        let Some(progress) = self.snapshot.as_ref() else {
            return Step::reject(Reject::Stage);
        };
        let Some(from) = progress.cursor.as_ref() else {
            return Step::reject(Reject::Stage);
        };
        if progress
            .hold
            .as_ref()
            .is_none_or(|hold| proof.head.history != hold.history)
        {
            return Step::reject(Reject::History);
        }
        let Ok(to_head) = Self::relation(comparisons, from, &proof.head, &proof) else {
            return Step::reject(Reject::Comparison);
        };
        let Ok(to_low) = Self::relation(comparisons, &proof.retained_from, from, &proof) else {
            return Step::reject(Reject::Comparison);
        };
        if !matches!(to_head, Comparison::Before | Comparison::Equal)
            || !matches!(to_low, Comparison::Before | Comparison::Equal)
        {
            return Step::reject(Reject::Discontinuity);
        }
        self.snapshot.as_mut().expect("active").barrier = Some(proof);
        if to_head == Comparison::Equal {
            self.snapshot_seal()
        } else {
            self.snapshot_scan()
        }
    }

    fn snapshot_scan(&mut self) -> Step {
        let from = self
            .snapshot
            .as_ref()
            .and_then(|progress| progress.cursor.clone())
            .expect("snapshot cursor");
        let max_events = self.config.max_batch_events;
        let max_bytes = self.config.max_batch_bytes;
        self.snapshot_effect(Stage::SnapshotScanning, move |op| Effect::SnapshotScan {
            op,
            from,
            max_events,
            max_bytes,
        })
    }

    pub(super) fn snapshot_scanned(&mut self, op: Operation, batch: Batch) -> Step {
        if let Err(error) = self.snapshot_live(op, Stage::SnapshotScanning) {
            return Step::reject(error);
        }
        let Some(progress) = self.snapshot.as_ref() else {
            return Step::reject(Reject::Stage);
        };
        let Some(proof) = progress.barrier.as_ref() else {
            return Step::reject(Reject::Stage);
        };
        let Some(from) = progress.cursor.as_ref() else {
            return Step::reject(Reject::Stage);
        };
        if batch.payload_id != op.token
            || batch.coverage.from != *from
            || batch.coverage.through.history != from.history
            || batch.coverage.through.history != proof.head.history
            || batch.coverage.proof != proof.id
            || batch.coverage.certificate.is_empty()
            || batch.coverage.certificate.len() > self.config.max_cursor_bytes
            || batch.events == 0
            || batch.events > self.config.max_batch_events
            || batch.bytes == 0
            || batch.bytes > self.config.max_batch_bytes
            || batch
                .advance
                .for_operands(from, &batch.coverage.through, &proof.id)
                != Some(Comparison::Before)
            || !matches!(
                batch
                    .end_to_head
                    .for_operands(&batch.coverage.through, &proof.head, &proof.id),
                Some(Comparison::Before | Comparison::Equal)
            )
        {
            return Step::reject(Reject::Discontinuity);
        }
        if let Err(error) = batch
            .coverage
            .through
            .validate(&self.scope, self.config.max_cursor_bytes)
        {
            return Step::reject(Reject::Identity(error));
        }
        self.snapshot.as_mut().expect("active").batch = Some(batch.clone());
        self.snapshot_effect(Stage::SnapshotApplying, move |op| Effect::SnapshotApply {
            op,
            batch: Box::new(batch),
        })
    }

    pub(super) fn snapshot_applied(
        &mut self,
        op: Operation,
        through: Cursor,
        charged_bytes: u64,
    ) -> Step {
        if let Err(error) = self.snapshot_live(op, Stage::SnapshotApplying) {
            return Step::reject(error);
        }
        let max_candidate_bytes = self.snapshot_limits().max_candidate_bytes;
        let Some(progress) = self.snapshot.as_mut() else {
            return Step::reject(Reject::Stage);
        };
        let Some(batch) = progress.batch.take() else {
            return Step::reject(Reject::Stage);
        };
        if through != batch.coverage.through || charged_bytes > max_candidate_bytes {
            progress.batch = Some(batch);
            return Step::reject(Reject::Discontinuity);
        }
        progress.cursor = Some(through);
        if batch.end_to_head.order == Comparison::Equal {
            self.snapshot_seal()
        } else {
            self.snapshot_scan()
        }
    }

    fn snapshot_seal(&mut self) -> Step {
        let through = self
            .snapshot
            .as_ref()
            .and_then(|progress| progress.cursor.clone())
            .expect("snapshot cursor");
        self.snapshot_effect(Stage::SnapshotSealing, move |op| {
            Effect::SealSnapshotStage { op, through }
        })
    }

    pub(super) fn snapshot_sealed(
        &mut self,
        op: Operation,
        through: Cursor,
        payload_id: u64,
        charged_bytes: u64,
    ) -> Step {
        if let Err(error) = self.snapshot_live(op, Stage::SnapshotSealing) {
            return Step::reject(error);
        }
        let max_candidate_bytes = self.snapshot_limits().max_candidate_bytes;
        let Some(progress) = self.snapshot.as_mut() else {
            return Step::reject(Reject::Stage);
        };
        if progress.cursor.as_ref() != Some(&through)
            || payload_id != op.token
            || charged_bytes > max_candidate_bytes
        {
            return Step::reject(Reject::Discontinuity);
        }
        progress.candidate_id = Some(payload_id);
        self.snapshot_effect(Stage::SnapshotInstalling, move |op| {
            Effect::InstallSnapshot {
                op,
                cursor: through,
                payload_id,
            }
        })
    }

    pub(super) fn snapshot_installed(&mut self, op: Operation, receipt: ApplyReceipt) -> Step {
        if let Err(error) = self.snapshot_live(op, Stage::SnapshotInstalling) {
            return Step::reject(error);
        }
        let Some(progress) = self.snapshot.as_ref() else {
            return Step::reject(Reject::Stage);
        };
        if !receipt.durable
            || progress.cursor.as_ref() != Some(&receipt.through)
            || progress.candidate_id.is_none()
        {
            return Step::reject(Reject::Discontinuity);
        }
        self.state.materialized = Some(receipt.through.clone());
        self.state.checkpoint = Some(receipt.through.clone());
        self.state.target = self
            .state
            .target
            .take()
            .filter(|target| target.history == receipt.through.history);
        self.snapshot_effect(Stage::SnapshotAttaching, move |op| Effect::AttachSnapshot {
            op,
            after: receipt.through,
        })
    }

    pub(super) fn snapshot_attached(
        &mut self,
        op: Operation,
        proof: SourceProof,
        comparisons: &[BoundComparison],
    ) -> Step {
        if let Err(error) = self.snapshot_live(op, Stage::SnapshotAttaching) {
            return Step::reject(error);
        }
        if let Err(error) = proof.validate(&self.scope, self.config.max_cursor_bytes) {
            return Step::reject(Reject::Identity(error));
        }
        let Some(progress) = self.snapshot.as_ref() else {
            return Step::reject(Reject::Stage);
        };
        let Some(from) = progress.cursor.as_ref() else {
            return Step::reject(Reject::Stage);
        };
        if progress
            .hold
            .as_ref()
            .is_none_or(|hold| proof.head.history != hold.history)
            || !matches!(
                Self::relation(comparisons, from, &proof.head, &proof),
                Ok(Comparison::Before | Comparison::Equal)
            )
            || !matches!(
                Self::relation(comparisons, &proof.retained_from, from, &proof),
                Ok(Comparison::Before | Comparison::Equal)
            )
        {
            return Step::reject(Reject::Discontinuity);
        }
        let progress = self.snapshot.as_mut().expect("active");
        progress.attached = true;
        progress.attach_head = Some(proof.head);
        self.outstanding = None;
        self.operation_due = None;
        self.check_tail()
    }

    pub(super) fn finish_snapshot(&mut self) -> Step {
        self.live_attachment_attempt = self.snapshot.as_ref().and_then(|progress| progress.attempt);
        let cleanup = self.drop_snapshot(SnapshotCleanupDisposition::Completed);
        Step::ok(cleanup)
    }

    pub(super) fn abort_snapshot(&mut self) -> Step {
        self.live_attachment_attempt = None;
        let cleanup = self.drop_snapshot(SnapshotCleanupDisposition::Aborted);
        self.outstanding = None;
        self.operation_due = None;
        self.state.stage = Stage::SnapshotAborted;
        self.ready = false;
        Step::ok(cleanup)
    }

    pub(super) fn drop_snapshot(&mut self, disposition: SnapshotCleanupDisposition) -> Vec<Effect> {
        let Some(progress) = self.snapshot.take() else {
            return Vec::new();
        };
        let Some(attempt) = progress.attempt else {
            return Vec::new();
        };
        let due = self.now.0.checked_add(self.config.attempt_timeout_ms);
        let op = self.fresh_operation().unwrap_or(attempt);
        let discarding = due.is_none() || op == attempt;
        let due = Time(due.unwrap_or(self.now.0));
        self.snapshot_cleanup = Some(CleanupState {
            op,
            attempt,
            disposition,
            due,
            discarding,
        });
        if discarding {
            vec![Effect::DiscardSnapshotResources {
                op,
                attempt,
                disposition,
            }]
        } else {
            vec![
                Effect::CleanupSnapshot {
                    op,
                    attempt,
                    disposition,
                    due,
                },
                Effect::ArmTimer(due),
            ]
        }
    }

    pub(super) fn cancel_snapshot_resources(&mut self) -> Vec<Effect> {
        if self.snapshot.is_some() {
            return self.drop_snapshot(SnapshotCleanupDisposition::Aborted);
        }
        let Some(attempt) = self.live_attachment_attempt.take() else {
            return Vec::new();
        };
        // Replace a queued Completed cleanup with a different operation. Its
        // late receipt cannot clear this mandatory local disposal.
        let op = self.fresh_operation().unwrap_or(attempt);
        self.snapshot_cleanup = Some(CleanupState {
            op,
            attempt,
            disposition: SnapshotCleanupDisposition::Aborted,
            due: self.now,
            discarding: true,
        });
        vec![Effect::DiscardSnapshotResources {
            op,
            attempt,
            disposition: SnapshotCleanupDisposition::Aborted,
        }]
    }

    pub(super) fn snapshot_cleaned(&mut self, op: Operation) -> Step {
        let Some(cleanup) = self.snapshot_cleanup.as_ref() else {
            return Step::reject(Reject::StaleOperation);
        };
        if cleanup.op != op || cleanup.discarding {
            return Step::reject(Reject::StaleOperation);
        }
        if self.now >= cleanup.due {
            return self.expire_snapshot_cleanup();
        }
        self.snapshot_cleanup = None;
        Step::ok(Vec::new())
    }

    pub(super) fn expire_snapshot_cleanup(&mut self) -> Step {
        let Some(cleanup) = self.snapshot_cleanup.as_mut() else {
            return Step::reject(Reject::Stage);
        };
        if cleanup.discarding {
            return Step::reject(Reject::StaleOperation);
        }
        cleanup.discarding = true;
        Step::ok(vec![Effect::DiscardSnapshotResources {
            op: cleanup.op,
            attempt: cleanup.attempt,
            disposition: cleanup.disposition,
        }])
    }

    pub(super) fn snapshot_discarded(
        &mut self,
        op: Operation,
        disposition: SnapshotCleanupDisposition,
    ) -> Step {
        if self.snapshot_cleanup.as_ref().is_none_or(|cleanup| {
            cleanup.op != op || cleanup.disposition != disposition || !cleanup.discarding
        }) {
            return Step::reject(Reject::StaleOperation);
        }
        self.snapshot_cleanup = None;
        if self.state.stage == Stage::NeedsSnapshot && self.config.snapshot.is_some() {
            self.start_snapshot()
        } else {
            Step::ok(Vec::new())
        }
    }
}
