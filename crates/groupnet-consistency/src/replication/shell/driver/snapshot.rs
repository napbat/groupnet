//! One worker's bounded native snapshot I/O; the core chooses every phase.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use groupnet_core::replication::{
    ApplyReceipt, Batch, ChunkReceipt, Coverage, Cursor, Effect, Event, Operation, Scope,
    SnapshotCleanupDisposition, SourceProof, Stage,
};
use tokio::sync::OwnedSemaphorePermit;

use super::{Driver, Manager, Payload, PrivateCheckpoint};
use crate::replication::api::{
    ApplicationAdapter, CheckpointLimit, FailureClass, ScanLimit, SourceAdapter, SourceBatch,
};
use crate::replication::snapshot_runtime::SnapshotMode;

impl<S, A, M> Driver<S, A, M>
where
    S: SourceAdapter,
    A: ApplicationAdapter<S::Position, S::Batch>,
    M: SnapshotMode<S, A>,
{
    async fn snapshot_op_permits(
        manager: &Manager<S, A, M>,
        deadline: Instant,
    ) -> Option<(OwnedSemaphorePermit, OwnedSemaphorePermit)> {
        let until = tokio::time::Instant::from_std(deadline);
        let quota =
            tokio::time::timeout_at(until, Arc::clone(&manager.snapshot_ops).acquire_owned())
                .await
                .ok()?
                .ok()?;
        let global = Self::operation_permit(manager, deadline).await?;
        Some((quota, global))
    }

    async fn snapshot_byte_permits(
        manager: &Manager<S, A, M>,
        deadline: Instant,
        count: usize,
    ) -> Option<(OwnedSemaphorePermit, OwnedSemaphorePermit)> {
        let count = u32::try_from(count).ok()?;
        let until = tokio::time::Instant::from_std(deadline);
        let quota = tokio::time::timeout_at(
            until,
            Arc::clone(&manager.snapshot_bytes).acquire_many_owned(count),
        )
        .await
        .ok()?
        .ok()?;
        let global =
            tokio::time::timeout_at(until, Arc::clone(&manager.bytes).acquire_many_owned(count))
                .await
                .ok()?
                .ok()?;
        Some((quota, global))
    }

    async fn snapshot_candidate_permits(
        manager: &Manager<S, A, M>,
        deadline: Instant,
        count: usize,
    ) -> Option<(OwnedSemaphorePermit, OwnedSemaphorePermit)> {
        let count = u32::try_from(count).ok()?;
        let until = tokio::time::Instant::from_std(deadline);
        let quota = tokio::time::timeout_at(
            until,
            Arc::clone(&manager.snapshot_checkpoint_bytes).acquire_many_owned(count),
        )
        .await
        .ok()?
        .ok()?;
        let global = tokio::time::timeout_at(
            until,
            Arc::clone(&manager.checkpoint_bytes).acquire_many_owned(count),
        )
        .await
        .ok()?
        .ok()?;
        Some((quota, global))
    }

    fn snapshot_deadline(&mut self, op: Operation) -> Option<Instant> {
        let deadline = self.deadlines.get(&op).copied()?;
        if self.shared.cancelled.load(Ordering::Acquire)
            || Instant::now() >= deadline
            || !self.engine.accepts_operation(op)
        {
            self.tick();
            return None;
        }
        Some(deadline)
    }

    fn snapshot_failure(&mut self, op: Operation, class: FailureClass) {
        self.adapter_failure(op, class);
    }

    async fn snapshot_acquire(
        &mut self,
        op: Operation,
        scope: Scope,
        total_due: groupnet_core::Time,
    ) {
        // A new attempt supersedes any completed continuation immediately,
        // including when admission or the source hold later stalls.
        self.snapshot_attachment = None;
        self.snapshot_attempt = Some(op);
        let Some(deadline) = self.snapshot_deadline(op) else {
            return;
        };
        let Some(_permits) = Self::snapshot_op_permits(&self.manager, deadline).await else {
            return self.step(Event::Failed { op });
        };
        let Some(limit) = self.manager.limits.core.snapshot else {
            return self.stop(FailureClass::Terminal);
        };
        let Some(total_deadline) = self.started.checked_add(Duration::from_millis(total_due.0))
        else {
            return self.stop(FailureClass::Terminal);
        };
        let result = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            M::acquire(
                &self.manager.source,
                scope,
                op,
                total_due,
                total_deadline,
                limit,
            ),
        )
        .await;
        self.tick();
        match result {
            Ok(Ok(hold)) => {
                if !self.engine.accepts_operation(op)
                    || self.shared.cancelled.load(Ordering::Acquire)
                {
                    if let Some(cleanup) = Instant::now().checked_add(self.timeout()) {
                        let cleanup = cleanup.min(total_deadline);
                        if Instant::now() < cleanup {
                            let _ = tokio::time::timeout_at(
                                tokio::time::Instant::from_std(cleanup),
                                M::release(&self.manager.source, hold.handle),
                            )
                            .await;
                        }
                    }
                    return;
                }
                self.snapshot_hold = Some(hold.handle);
                self.step(Event::SnapshotHeld {
                    op,
                    receipt: hold.receipt,
                });
            }
            Ok(Err(class)) => self.snapshot_failure(op, class),
            Err(_) => self.step(Event::Failed { op }),
        }
    }

    async fn snapshot_offer(&mut self, op: Operation, scope: Scope) {
        let Some(deadline) = self.snapshot_deadline(op) else {
            return;
        };
        let Some(_permits) = Self::snapshot_op_permits(&self.manager, deadline).await else {
            return self.step(Event::Failed { op });
        };
        let Some(hold) = self.snapshot_hold.as_mut() else {
            return self.stop(FailureClass::Terminal);
        };
        let Some(limit) = self.manager.limits.core.snapshot else {
            return self.stop(FailureClass::Terminal);
        };
        let result = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            M::offer(&self.manager.source, hold, scope, limit),
        )
        .await;
        self.tick();
        match result {
            Ok(Ok(image)) if self.engine.accepts_operation(op) => {
                self.snapshot_read = Some(image.read);
                self.step(Event::SnapshotOffered {
                    op,
                    offer: Box::new(image.offer),
                });
            }
            Ok(Ok(_)) => {}
            Ok(Err(class)) => self.snapshot_failure(op, class),
            Err(_) => self.step(Event::Failed { op }),
        }
    }

    async fn snapshot_open(
        &mut self,
        op: Operation,
        offer: Box<groupnet_core::replication::SnapshotOffer>,
        max: u64,
    ) {
        let Some(deadline) = self.snapshot_deadline(op) else {
            return;
        };
        let Ok(cap) = usize::try_from(max) else {
            return self.stop(FailureClass::Terminal);
        };
        let Some(permits) = Self::snapshot_candidate_permits(&self.manager, deadline, cap).await
        else {
            return self.step(Event::Failed { op });
        };
        let Some(_operation) = Self::snapshot_op_permits(&self.manager, deadline).await else {
            return self.step(Event::Failed { op });
        };
        let result = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            M::open(
                &self.manager.app,
                self.shared.scope.clone(),
                &offer,
                CheckpointLimit { bytes: cap },
            ),
        )
        .await;
        self.tick();
        match result {
            Ok(Ok(stage)) if self.engine.accepts_operation(op) && stage.charged_bytes <= cap => {
                self.snapshot_stage = Some(stage.handle);
                self.snapshot_candidate_permit = Some(permits);
                self.step(Event::SnapshotOpened {
                    op,
                    charged_bytes: stage.charged_bytes as u64,
                });
            }
            Ok(Ok(_)) => self.stop(FailureClass::Terminal),
            Ok(Err(class)) => self.snapshot_failure(op, class),
            Err(_) => self.step(Event::Failed { op }),
        }
    }

    async fn snapshot_read_chunk(&mut self, op: Operation, index: u32, offset: u64, max: usize) {
        let Some(deadline) = self.snapshot_deadline(op) else {
            return;
        };
        let Some((quota, global)) = Self::snapshot_byte_permits(&self.manager, deadline, max).await
        else {
            return self.step(Event::Failed { op });
        };
        let Some(_operation) = Self::snapshot_op_permits(&self.manager, deadline).await else {
            return self.step(Event::Failed { op });
        };
        let Some(read) = self.snapshot_read.as_mut() else {
            return self.stop(FailureClass::Terminal);
        };
        let result = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            M::read(&self.manager.source, read, offset, max),
        )
        .await;
        self.tick();
        match result {
            Ok(Ok(bytes))
                if self.engine.accepts_operation(op) && !bytes.is_empty() && bytes.len() <= max =>
            {
                let len = bytes.len();
                self.snapshot_chunk = Some((offset, bytes, quota, global));
                self.step(Event::SnapshotRead {
                    op,
                    chunk: ChunkReceipt {
                        index,
                        offset,
                        bytes: len,
                        payload_id: op.token,
                    },
                });
                if self.engine.state().stage != Stage::SnapshotWriting {
                    self.snapshot_chunk = None;
                }
            }
            Ok(Ok(_)) => self.stop(FailureClass::Terminal),
            Ok(Err(class)) => self.snapshot_failure(op, class),
            Err(_) => self.step(Event::Failed { op }),
        }
    }

    async fn snapshot_write_chunk(&mut self, op: Operation, chunk: ChunkReceipt) {
        let Some(deadline) = self.snapshot_deadline(op) else {
            return;
        };
        let Some((offset, bytes, _quota, _global)) = self.snapshot_chunk.take() else {
            return self.stop(FailureClass::Terminal);
        };
        if offset != chunk.offset || bytes.len() != chunk.bytes {
            return self.stop(FailureClass::Terminal);
        }
        let Some(_operation) = Self::snapshot_op_permits(&self.manager, deadline).await else {
            return self.step(Event::Failed { op });
        };
        let Some(stage) = self.snapshot_stage.as_mut() else {
            return self.stop(FailureClass::Terminal);
        };
        let result = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            M::write(&self.manager.app, stage, offset, bytes),
        )
        .await;
        self.tick();
        match result {
            Ok(Ok(charged)) if self.engine.accepts_operation(op) => {
                self.step(Event::SnapshotWritten {
                    op,
                    index: chunk.index,
                    through: offset + chunk.bytes as u64,
                    charged_bytes: charged as u64,
                });
            }
            Ok(Ok(_)) => {}
            Ok(Err(class)) => self.snapshot_failure(op, class),
            Err(_) => self.step(Event::Failed { op }),
        }
    }

    async fn snapshot_verify(&mut self, op: Operation, digest: Vec<u8>) {
        let Some(deadline) = self.snapshot_deadline(op) else {
            return;
        };
        let Some(_operation) = Self::snapshot_op_permits(&self.manager, deadline).await else {
            return self.step(Event::Failed { op });
        };
        let Some(stage) = self.snapshot_stage.as_mut() else {
            return self.stop(FailureClass::Terminal);
        };
        let result = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            M::verify(&self.manager.app, stage, &digest),
        )
        .await;
        self.tick();
        match result {
            Ok(Ok(charged)) if self.engine.accepts_operation(op) => {
                self.step(Event::SnapshotVerified {
                    op,
                    charged_bytes: charged as u64,
                });
            }
            Ok(Ok(_)) => {}
            Ok(Err(class)) => self.snapshot_failure(op, class),
            Err(_) => self.step(Event::Failed { op }),
        }
    }

    fn snapshot_comparisons(
        &self,
        from: &Cursor,
        proof: &SourceProof,
    ) -> Result<Vec<groupnet_core::replication::BoundComparison>, FailureClass> {
        [(from, &proof.head), (&proof.retained_from, from)]
            .into_iter()
            .map(|(left, right)| {
                self.manager
                    .source
                    .compare(left, right, proof)
                    .map_err(|error| error.class())
            })
            .collect()
    }

    async fn snapshot_barrier(&mut self, op: Operation, from: Cursor) {
        let Some(deadline) = self.snapshot_deadline(op) else {
            return;
        };
        let Some(native) = self
            .manager
            .source
            .position(&from)
            .map_err(|e| e.class())
            .ok()
        else {
            return self.stop(FailureClass::Terminal);
        };
        let Some(_operation) = Self::snapshot_op_permits(&self.manager, deadline).await else {
            return self.step(Event::Failed { op });
        };
        let Some(hold) = self.snapshot_hold.as_mut() else {
            return self.stop(FailureClass::Terminal);
        };
        let Some(limit) = self.manager.limits.core.snapshot else {
            return self.stop(FailureClass::Terminal);
        };
        let result = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            M::barrier(&self.manager.source, hold, native, limit),
        )
        .await;
        self.tick();
        match result {
            Ok(Ok(proof)) if self.engine.accepts_operation(op) => {
                let comparisons = match self.snapshot_comparisons(&from, &proof) {
                    Ok(value) => value,
                    Err(class) => return self.snapshot_failure(op, class),
                };
                self.snapshot_proof = Some(proof.clone());
                self.step(Event::SnapshotBarrier {
                    op,
                    proof,
                    comparisons,
                });
            }
            Ok(Ok(_)) => {}
            Ok(Err(class)) => self.snapshot_failure(op, class),
            Err(_) => self.step(Event::Failed { op }),
        }
    }

    async fn snapshot_scan(&mut self, op: Operation, from: Cursor, events: usize, max: usize) {
        let Some(deadline) = self.snapshot_deadline(op) else {
            return;
        };
        let Some(proof) = self.snapshot_proof.clone() else {
            return self.stop(FailureClass::Terminal);
        };
        let native_from = match self.manager.source.position(&from) {
            Ok(value) => value,
            Err(error) => return self.snapshot_failure(op, error.class()),
        };
        let Some((quota, global)) = Self::snapshot_byte_permits(&self.manager, deadline, max).await
        else {
            return self.step(Event::Failed { op });
        };
        let Some(_operation) = Self::snapshot_op_permits(&self.manager, deadline).await else {
            return self.step(Event::Failed { op });
        };
        let result = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            self.manager.source.scan_after(
                self.shared.scope.clone(),
                native_from,
                proof.clone(),
                ScanLimit { events, bytes: max },
            ),
        )
        .await;
        self.tick();
        let source_batch = match result {
            Ok(Ok(batch)) if self.engine.accepts_operation(op) => batch,
            Ok(Ok(_)) => return,
            Ok(Err(error)) => return self.snapshot_failure(op, error.class()),
            Err(_) => return self.step(Event::Failed { op }),
        };
        let SourceBatch {
            from: reported,
            through,
            proof: proof_id,
            certificate,
            events,
            bytes,
            native,
        } = source_batch;
        let from_cursor = match self.manager.source.cursor(&self.shared.scope, &reported) {
            Ok(value) => value,
            Err(error) => return self.snapshot_failure(op, error.class()),
        };
        let through_cursor = match self.manager.source.cursor(&self.shared.scope, &through) {
            Ok(value) => value,
            Err(error) => return self.snapshot_failure(op, error.class()),
        };
        let advance = match self.manager.source.compare(&from, &through_cursor, &proof) {
            Ok(value) => value,
            Err(error) => return self.snapshot_failure(op, error.class()),
        };
        let end_to_head = match self
            .manager
            .source
            .compare(&through_cursor, &proof.head, &proof)
        {
            Ok(value) => value,
            Err(error) => return self.snapshot_failure(op, error.class()),
        };
        self.payloads.insert(
            op.token,
            Payload {
                native,
                _bytes: global,
                _snapshot_bytes: Some(quota),
            },
        );
        self.snapshot_payload_id = Some(op.token);
        self.step(Event::SnapshotScanned {
            op,
            batch: Box::new(Batch {
                coverage: Coverage {
                    from: from_cursor,
                    through: through_cursor,
                    proof: proof_id,
                    certificate,
                },
                payload_id: op.token,
                events,
                bytes,
                advance,
                end_to_head,
            }),
        });
        if self.engine.state().stage != Stage::SnapshotApplying {
            self.payloads.remove(&op.token);
            self.snapshot_payload_id = None;
        }
    }

    async fn snapshot_apply(&mut self, op: Operation, batch: Box<Batch>) {
        let Some(deadline) = self.snapshot_deadline(op) else {
            return;
        };
        let Some(payload) = self.payloads.remove(&batch.payload_id) else {
            return self.stop(FailureClass::Terminal);
        };
        self.snapshot_payload_id = None;
        let from = match self.manager.source.position(&batch.coverage.from) {
            Ok(value) => value,
            Err(error) => return self.snapshot_failure(op, error.class()),
        };
        let through = match self.manager.source.position(&batch.coverage.through) {
            Ok(value) => value,
            Err(error) => return self.snapshot_failure(op, error.class()),
        };
        let Some(_operation) = Self::snapshot_op_permits(&self.manager, deadline).await else {
            return self.step(Event::Failed { op });
        };
        let Some(stage) = self.snapshot_stage.as_mut() else {
            return self.stop(FailureClass::Terminal);
        };
        let result = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            M::apply(&self.manager.app, stage, from, through, payload.native),
        )
        .await;
        self.tick();
        match result {
            Ok(Ok(charged)) if self.engine.accepts_operation(op) => {
                self.step(Event::SnapshotApplied {
                    op,
                    through: batch.coverage.through,
                    charged_bytes: charged as u64,
                });
            }
            Ok(Ok(_)) => {}
            Ok(Err(class)) => self.snapshot_failure(op, class),
            Err(_) => self.step(Event::Failed { op }),
        }
    }

    async fn snapshot_seal(&mut self, op: Operation, through: Cursor) {
        let Some(deadline) = self.snapshot_deadline(op) else {
            return;
        };
        let native = match self.manager.source.position(&through) {
            Ok(value) => value,
            Err(error) => return self.snapshot_failure(op, error.class()),
        };
        let Some(_operation) = Self::snapshot_op_permits(&self.manager, deadline).await else {
            return self.step(Event::Failed { op });
        };
        let Some(stage) = self.snapshot_stage.take() else {
            return self.stop(FailureClass::Terminal);
        };
        let result = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            M::seal(&self.manager.app, stage, native),
        )
        .await;
        self.tick();
        match result {
            Ok(Ok(candidate)) if self.engine.accepts_operation(op) => {
                let Some((quota, global)) = self.snapshot_candidate_permit.take() else {
                    return self.stop(FailureClass::Terminal);
                };
                let max = self
                    .manager
                    .limits
                    .core
                    .snapshot
                    .map_or(0, |limit| limit.max_candidate_bytes);
                if candidate.bytes as u64 > max {
                    return self.stop(FailureClass::Terminal);
                }
                let actual = match self
                    .manager
                    .source
                    .cursor(&self.shared.scope, &candidate.position)
                {
                    Ok(value) => value,
                    Err(error) => return self.snapshot_failure(op, error.class()),
                };
                self.checkpoints.insert(
                    op.token,
                    PrivateCheckpoint {
                        candidate,
                        _bytes: global,
                        _snapshot_bytes: Some(quota),
                    },
                );
                self.snapshot_candidate_id = Some(op.token);
                self.step(Event::SnapshotSealed {
                    op,
                    through: actual,
                    payload_id: op.token,
                    charged_bytes: self.checkpoints[&op.token].candidate.bytes as u64,
                });
                if self.engine.state().stage != Stage::SnapshotInstalling {
                    self.checkpoints.remove(&op.token);
                    self.snapshot_candidate_id = None;
                }
            }
            Ok(Ok(_)) => {}
            Ok(Err(class)) => self.snapshot_failure(op, class),
            Err(_) => self.step(Event::Failed { op }),
        }
    }

    async fn snapshot_install(&mut self, op: Operation, cursor: Cursor, payload_id: u64) {
        let Some(deadline) = self.snapshot_deadline(op) else {
            return;
        };
        let Some(checkpoint) = self.checkpoints.remove(&payload_id) else {
            return self.stop(FailureClass::Terminal);
        };
        self.snapshot_candidate_id = None;
        let PrivateCheckpoint {
            candidate,
            _bytes: _global,
            _snapshot_bytes: _quota,
        } = checkpoint;
        let Some(_operation) = Self::snapshot_op_permits(&self.manager, deadline).await else {
            return self.step(Event::Failed { op });
        };
        if self.shared.cancelled.load(Ordering::Acquire) {
            return;
        }
        let Some(permit) = self.shared.fence.issue(op, deadline) else {
            return self.stop(FailureClass::Terminal);
        };
        if self.shared.cancelled.load(Ordering::Acquire) {
            self.shared.fence.invalidate();
            return;
        }
        let installed = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            self.manager
                .app
                .install_checkpoint(self.shared.scope.clone(), candidate, permit),
        )
        .await;
        self.shared.fence.invalidate();
        self.tick();
        match installed {
            Ok(Ok(receipt)) if self.engine.accepts_operation(op) => {
                let actual = match self
                    .manager
                    .source
                    .cursor(&self.shared.scope, &receipt.position)
                {
                    Ok(value) => value,
                    Err(error) => return self.snapshot_failure(op, error.class()),
                };
                if actual != cursor {
                    return self.stop(FailureClass::Terminal);
                }
                self.step(Event::SnapshotInstalled {
                    op,
                    receipt: ApplyReceipt {
                        through: actual,
                        durable: receipt.durable,
                    },
                });
            }
            Ok(Ok(_)) => {}
            Ok(Err(class)) => self.snapshot_failure(op, class.class()),
            Err(_) => self.step(Event::Failed { op }),
        }
    }

    async fn snapshot_attach(&mut self, op: Operation, after: Cursor) {
        let Some(deadline) = self.snapshot_deadline(op) else {
            return;
        };
        let native = match self.manager.source.position(&after) {
            Ok(value) => value,
            Err(error) => return self.snapshot_failure(op, error.class()),
        };
        let Some(_operation) = Self::snapshot_op_permits(&self.manager, deadline).await else {
            return self.step(Event::Failed { op });
        };
        let Some(hold) = self.snapshot_hold.as_mut() else {
            return self.stop(FailureClass::Terminal);
        };
        let result = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            M::attach(&self.manager.source, hold, native),
        )
        .await;
        self.tick();
        match result {
            Ok(Ok(attached)) if self.engine.accepts_operation(op) => {
                let comparisons = match self.snapshot_comparisons(&after, &attached.proof) {
                    Ok(value) => value,
                    Err(class) => return self.snapshot_failure(op, class),
                };
                self.snapshot_attachment = Some(attached.handle);
                self.step(Event::SnapshotAttached {
                    op,
                    proof: attached.proof,
                    comparisons,
                });
            }
            Ok(Ok(_)) => {}
            Ok(Err(class)) => self.snapshot_failure(op, class),
            Err(_) => self.step(Event::Failed { op }),
        }
    }

    fn discard_snapshot_local(
        &mut self,
        attempt: Operation,
        disposition: SnapshotCleanupDisposition,
    ) {
        if self.snapshot_attempt != Some(attempt) {
            return;
        }
        self.snapshot_read = None;
        if disposition == SnapshotCleanupDisposition::Aborted {
            self.snapshot_attachment = None;
            self.snapshot_attempt = None;
        }
        self.snapshot_stage = None;
        self.snapshot_chunk = None;
        self.snapshot_candidate_permit = None;
        self.snapshot_proof = None;
        if let Some(id) = self.snapshot_payload_id.take() {
            self.payloads.remove(&id);
        }
        if let Some(id) = self.snapshot_candidate_id.take() {
            self.checkpoints.remove(&id);
        }
        self.snapshot_hold = None;
    }

    async fn snapshot_cleanup(
        &mut self,
        op: Operation,
        attempt: Operation,
        disposition: SnapshotCleanupDisposition,
        due: groupnet_core::Time,
    ) {
        if !self.engine.accepts_operation(op) {
            return;
        }
        if self.snapshot_attempt != Some(attempt) {
            self.step(Event::SnapshotCleaned { op });
            return;
        }
        let hold = self.snapshot_hold.take();
        self.discard_snapshot_local(attempt, disposition);
        if let Some(hold) = hold
            && let Some(deadline) = self.started.checked_add(Duration::from_millis(due.0))
            && let Some(_permits) = Self::snapshot_op_permits(&self.manager, deadline).await
        {
            let _ = tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline),
                M::release(&self.manager.source, hold),
            )
            .await;
        }
        self.tick();
        self.step(Event::SnapshotCleaned { op });
    }

    fn snapshot_discard(
        &mut self,
        op: Operation,
        attempt: Operation,
        disposition: SnapshotCleanupDisposition,
    ) {
        self.discard_snapshot_local(attempt, disposition);
        self.step(Event::SnapshotDiscarded { op, disposition });
    }

    pub(super) async fn run_snapshot_effect(&mut self, effect: Effect) {
        match effect {
            Effect::AcquireSnapshotHold {
                op,
                scope,
                total_due,
            } => self.snapshot_acquire(op, scope, total_due).await,
            Effect::OfferSnapshot { op, scope } => self.snapshot_offer(op, scope).await,
            Effect::OpenSnapshotStage {
                op,
                offer,
                max_candidate_bytes,
            } => self.snapshot_open(op, offer, max_candidate_bytes).await,
            Effect::ReadSnapshotChunk {
                op,
                index,
                offset,
                max_bytes,
            } => self.snapshot_read_chunk(op, index, offset, max_bytes).await,
            Effect::WriteSnapshotChunk { op, chunk } => self.snapshot_write_chunk(op, chunk).await,
            Effect::VerifySnapshotImage { op, digest } => self.snapshot_verify(op, digest).await,
            Effect::SnapshotReplayBarrier { op, from } => self.snapshot_barrier(op, from).await,
            Effect::SnapshotScan {
                op,
                from,
                max_events,
                max_bytes,
            } => self.snapshot_scan(op, from, max_events, max_bytes).await,
            Effect::SnapshotApply { op, batch } => self.snapshot_apply(op, batch).await,
            Effect::SealSnapshotStage { op, through } => self.snapshot_seal(op, through).await,
            Effect::InstallSnapshot {
                op,
                cursor,
                payload_id,
            } => self.snapshot_install(op, cursor, payload_id).await,
            Effect::AttachSnapshot { op, after } => self.snapshot_attach(op, after).await,
            Effect::CleanupSnapshot {
                op,
                attempt,
                disposition,
                due,
            } => {
                self.snapshot_cleanup(op, attempt, disposition, due).await;
            }
            Effect::DiscardSnapshotResources {
                op,
                attempt,
                disposition,
            } => {
                self.snapshot_discard(op, attempt, disposition);
            }
            _ => self.stop(FailureClass::Terminal),
        }
    }
}
