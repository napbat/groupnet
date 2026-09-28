//! Deterministic, private image transfer. Application authority stays outside.

use crate::Time;

use super::super::BootstrapOperation;
use super::super::journal::{
    AttachToken, BarrierReceipt, JournalBatch, JournalCursor, NativeCut, ReservationId,
};
use super::native_cuts_cover;
use super::types::{
    NativeCoverageReceipt, TransferBinding, TransferConfig, TransferEffect, TransferError,
    TransferEvent, TransferOffer, TransferStage, TransferStep,
};

/// One bounded child of an exact provisional donor selection. The composite
/// claim engine supplies child operations from its existing token allocator;
/// this session owns no independent token sequence or I/O resource.
#[derive(Debug)]
pub struct TransferSession {
    config: TransferConfig,
    binding: TransferBinding,
    stage: TransferStage,
    now: Time,
    last_token: u64,
    current: Option<BootstrapOperation>,
    offer: Option<TransferOffer>,
    reservation: Option<ReservationId>,
    attachment: Option<AttachToken>,
    barrier: Option<BarrierReceipt>,
    coverage: Option<NativeCoverageReceipt>,
    next_chunk: usize,
    encoded_received: usize,
    replay_cuts: Vec<NativeCut>,
    applied: u64,
    replayed_events: usize,
    pending_batch: Option<JournalCursor>,
    coverage_due: Option<Time>,
}

impl TransferSession {
    /// Construct an unstarted, authority-free transfer under immutable parent
    /// selection and total deadlines.
    ///
    /// # Errors
    /// Rejects malformed identities, bounds, or a deadline already elapsed.
    pub fn new(
        config: TransferConfig,
        binding: TransferBinding,
        now: Time,
    ) -> Result<Self, TransferError> {
        let config = config.validate()?;
        let binding_bytes = binding
            .scope
            .domain
            .len()
            .checked_add(binding.scope.partition.len())
            .and_then(|bytes| bytes.checked_add(binding.donor.node.as_str().len()))
            .and_then(|bytes| bytes.checked_add(binding.follower.node.as_str().len()))
            .ok_or(TransferError::InvalidConfig)?;
        if binding.parent.session == 0
            || binding.parent.incarnation.0 == 0
            || binding.parent.generation == 0
            || binding.parent.token == 0
            || binding.due <= now
            || binding.scope.domain.is_empty()
            || binding.scope.partition.is_empty()
            || binding.donor.node.as_str().is_empty()
            || binding.follower.node.as_str().is_empty()
            || binding.donor.incarnation.0 == 0
            || binding.donor.session == 0
            || binding.donor.attempt == 0
            || binding.follower.incarnation.0 == 0
            || binding.follower.session == 0
            || binding.follower.attempt == 0
            || binding.donor == binding.follower
            || binding.follower.incarnation != binding.parent.incarnation
            || binding.follower.session != binding.parent.session
            || binding.follower.attempt != binding.parent.generation
            || binding_bytes > config.max_metadata_bytes
        {
            return Err(TransferError::InvalidConfig);
        }
        Ok(Self {
            config,
            last_token: binding.parent.token,
            binding,
            stage: TransferStage::Unready,
            now,
            current: None,
            offer: None,
            reservation: None,
            attachment: None,
            barrier: None,
            coverage: None,
            next_chunk: 0,
            encoded_received: 0,
            replay_cuts: Vec::new(),
            applied: 0,
            replayed_events: 0,
            pending_batch: None,
            coverage_due: None,
        })
    }

    /// Current private phase. Completed is not read permission.
    #[must_use]
    pub fn stage(&self) -> TransferStage {
        self.stage
    }

    /// Exact current child operation, if any.
    #[must_use]
    pub fn current_operation(&self) -> Option<BootstrapOperation> {
        self.current
    }

    /// Immutable claim-constrained deadline; every active effect is charged
    /// to this absolute caller clock time.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Time> {
        (!matches!(
            self.stage,
            TransferStage::Completed | TransferStage::Aborted
        ))
        .then_some(
            self.coverage_due
                .map_or(self.binding.due, |due| due.min(self.binding.due)),
        )
    }

    /// Exact original donor operation used to identify this child.
    #[must_use]
    pub fn parent(&self) -> BootstrapOperation {
        self.binding.parent
    }

    fn ok(&self, mut effects: Vec<TransferEffect>) -> TransferStep {
        if let Some(due) = self.next_deadline() {
            effects.push(TransferEffect::ArmTimer(due));
        }
        TransferStep {
            effects,
            rejection: None,
        }
    }

    fn reject(error: TransferError) -> TransferStep {
        TransferStep {
            effects: Vec::new(),
            rejection: Some(error),
        }
    }

    fn abort(&mut self, reason: TransferError) -> TransferStep {
        if self.stage == TransferStage::Aborted {
            return Self::reject(reason);
        }
        self.stage = TransferStage::Aborted;
        self.current = None;
        let mut effects = vec![TransferEffect::DiscardStage {
            parent: self.binding.parent,
        }];
        if let Some(reservation) = self.reservation.take() {
            effects.push(TransferEffect::ReleaseReservation(reservation));
        }
        TransferStep {
            effects,
            rejection: Some(reason),
        }
    }

    fn allocate<F>(&mut self, allocator: &mut F) -> Result<BootstrapOperation, TransferError>
    where
        F: FnMut() -> Option<BootstrapOperation>,
    {
        let op = allocator().ok_or(TransferError::Allocator)?;
        let parent = self.binding.parent;
        if op.session != parent.session
            || op.incarnation != parent.incarnation
            || op.generation != parent.generation
            || op.token <= self.last_token
        {
            return Err(TransferError::Allocator);
        }
        self.last_token = op.token;
        self.current = Some(op);
        Ok(op)
    }

    fn current(&self, op: BootstrapOperation, stage: TransferStage) -> bool {
        self.stage == stage && self.current == Some(op)
    }

    fn offer_valid(&self, offer: &TransferOffer) -> Result<(), TransferError> {
        if offer.schema != self.config.expected_schema {
            return Err(TransferError::Schema);
        }
        if offer.capture.scope != self.binding.scope
            || offer.capture.donor != self.binding.donor
            || offer.capture.recovery_generation == 0
            || offer.capture.serial == 0
            || offer.image_cut.capture != offer.capture
            || offer.image_cut.position != 0
            || offer.encoded_bytes == 0
            || offer.decoded_bytes == 0
            || offer.chunks == 0
            || offer.encoded_bytes > self.config.max_encoded_bytes
            || offer.decoded_bytes > self.config.max_decoded_bytes
            || offer.chunks > self.config.max_chunks
            || offer.encoded_bytes < offer.chunks
            || offer.encoded_bytes
                > offer
                    .chunks
                    .checked_mul(self.config.max_chunk_bytes)
                    .ok_or(TransferError::Capacity)?
            || offer.members.is_empty()
            || offer.members.len() > self.config.max_members
            || offer.cuts.len() > self.config.max_cuts
        {
            return Err(TransferError::Capacity);
        }
        let mut bytes = self
            .binding
            .scope
            .domain
            .len()
            .checked_add(self.binding.scope.partition.len())
            .ok_or(TransferError::Capacity)?;
        for (index, member) in offer.members.iter().enumerate() {
            bytes = bytes
                .checked_add(member.node.as_str().len())
                .ok_or(TransferError::Capacity)?;
            if !member.valid_bounded(self.config.max_metadata_bytes)
                || (index > 0 && offer.members[index - 1].node >= member.node)
            {
                return Err(TransferError::Continuity);
            }
        }
        if !offer
            .members
            .iter()
            .any(|member| member.matches_claim(&self.binding.donor))
            || !offer
                .members
                .iter()
                .any(|member| member.matches_claim(&self.binding.follower))
        {
            return Err(TransferError::Continuity);
        }
        for (index, cut) in offer.cuts.iter().enumerate() {
            bytes = bytes
                .checked_add(cut.writer.len())
                .ok_or(TransferError::Capacity)?;
            if cut.writer.is_empty()
                || cut.epoch == 0
                || (index > 0 && offer.cuts[index - 1].writer >= cut.writer)
            {
                return Err(TransferError::Continuity);
            }
        }
        if bytes > self.config.max_metadata_bytes {
            return Err(TransferError::Capacity);
        }
        Ok(())
    }

    fn barrier_valid(&self, receipt: &BarrierReceipt) -> bool {
        let Some(offer) = &self.offer else {
            return false;
        };
        let Some(reservation) = &self.reservation else {
            return false;
        };
        let Some(attachment) = &self.attachment else {
            return false;
        };
        receipt.reservation == *reservation
            && receipt.attach_operation == attachment.operation
            && receipt.barrier_operation > 0
            && receipt.cursor.capture == offer.capture
            && usize::try_from(receipt.cursor.position)
                .is_ok_and(|position| position <= self.config.max_replay_events)
            && receipt.members == offer.members
            && native_cuts_cover(&receipt.covered_cuts, &offer.cuts)
            && self.barrier.as_ref().is_none_or(|previous| {
                receipt.barrier_operation != previous.barrier_operation
                    && receipt.cursor.position >= previous.cursor.position
                    && native_cuts_cover(&receipt.covered_cuts, &previous.covered_cuts)
                    && self.applied == previous.cursor.position
            })
    }

    fn next_batch_or_coverage<F>(&mut self, allocator: &mut F) -> TransferStep
    where
        F: FnMut() -> Option<BootstrapOperation>,
    {
        let Some(receipt) = self.barrier.clone() else {
            return self.abort(TransferError::Continuity);
        };
        let Ok(op) = self.allocate(allocator) else {
            return self.abort(TransferError::Allocator);
        };
        if self.applied == receipt.cursor.position {
            if self.replay_cuts != receipt.covered_cuts {
                return self.abort(TransferError::Continuity);
            }
            self.stage = TransferStage::AwaitingNativeCoverage;
            self.ok(vec![TransferEffect::CheckNativeCoverage {
                op,
                receipt,
                max_buffer_bytes: self.config.max_native_buffer_bytes,
            }])
        } else {
            self.stage = TransferStage::Replaying;
            self.ok(vec![TransferEffect::FetchBatch { op, receipt }])
        }
    }

    fn batch_valid(&mut self, batch: &JournalBatch) -> bool {
        let Some(barrier) = &self.barrier else {
            return false;
        };
        let Some(reservation) = &self.reservation else {
            return false;
        };
        if batch.reservation != *reservation
            || batch.from.capture != barrier.cursor.capture
            || batch.through.capture != barrier.cursor.capture
            || batch.from.position != self.applied
            || batch.through.position > barrier.cursor.position
            || batch.deltas.is_empty()
            || batch.deltas.len() > self.config.max_batch_events
            || batch.bytes == 0
            || batch.bytes > self.config.max_batch_bytes
            || batch.operation == 0
        {
            return false;
        }
        let mut position = self.applied;
        let mut bytes = 0usize;
        for delta in &batch.deltas {
            let Some(next) = position.checked_add(1) else {
                return false;
            };
            if delta.position != next {
                return false;
            }
            position = next;
            let identity_len = match &delta.identity {
                super::super::journal::DeltaIdentity::Native(cut) => {
                    let Some(covered) = barrier
                        .covered_cuts
                        .iter()
                        .find(|covered| covered.writer == cut.writer)
                    else {
                        return false;
                    };
                    let Some(replayed) = self
                        .replay_cuts
                        .iter_mut()
                        .find(|replayed| replayed.writer == cut.writer)
                    else {
                        return false;
                    };
                    if cut.epoch != covered.epoch
                        || cut.epoch != replayed.epoch
                        || replayed.sequence.checked_add(1) != Some(cut.sequence)
                        || cut.sequence > covered.sequence
                    {
                        return false;
                    }
                    replayed.sequence = cut.sequence;
                    cut.writer.len()
                }
                super::super::journal::DeltaIdentity::Local(id) => id.len(),
            };
            if identity_len == 0 || delta.effect.is_empty() {
                return false;
            }
            let Some(next_bytes) = bytes
                .checked_add(identity_len)
                .and_then(|value| value.checked_add(delta.effect.len()))
            else {
                return false;
            };
            bytes = next_bytes;
        }
        position == batch.through.position
            && bytes == batch.bytes
            && self
                .replayed_events
                .checked_add(batch.deltas.len())
                .is_some_and(|count| count <= self.config.max_replay_events)
    }

    /// Consume a pure event; `allocator` is a private composite-engine hook
    /// into `ClaimEngine`'s one checked operation counter. Every returned
    /// operation is validated against the retained parent identity.
    #[expect(
        clippy::too_many_lines,
        reason = "one event dispatch preserves the phase and operation correlation invariant"
    )]
    pub fn step<F>(&mut self, event: TransferEvent, allocator: &mut F) -> TransferStep
    where
        F: FnMut() -> Option<BootstrapOperation>,
    {
        match event {
            TransferEvent::Tick(now) => {
                if now < self.now {
                    return Self::reject(TransferError::BackwardTime);
                }
                self.now = now;
                if now >= self.binding.due
                    && !matches!(
                        self.stage,
                        TransferStage::Completed | TransferStage::Aborted
                    )
                {
                    return self.abort(TransferError::Expired);
                }
                if self.stage == TransferStage::WaitingCoverage
                    && self.coverage_due.is_some_and(|due| now >= due)
                {
                    self.coverage_due = None;
                    let Some(expected) = self.barrier.clone() else {
                        return self.abort(TransferError::Continuity);
                    };
                    let Ok(op) = self.allocate(allocator) else {
                        return self.abort(TransferError::Allocator);
                    };
                    self.stage = TransferStage::RequestingBarrier;
                    return self.ok(vec![TransferEffect::AdvanceBarrier { op, expected }]);
                }
                self.ok(Vec::new())
            }
            TransferEvent::Cancel => {
                if self.stage == TransferStage::Completed {
                    Self::reject(TransferError::Stage)
                } else {
                    let mut step = self.abort(TransferError::Stage);
                    step.rejection = None;
                    step
                }
            }
            TransferEvent::Start => {
                if self.stage != TransferStage::Unready {
                    return Self::reject(TransferError::Stage);
                }
                let Ok(op) = self.allocate(allocator) else {
                    return self.abort(TransferError::Allocator);
                };
                self.stage = TransferStage::RequestingOffer;
                self.ok(vec![TransferEffect::FetchOffer {
                    op,
                    max_metadata_bytes: self.config.max_metadata_bytes,
                }])
            }
            TransferEvent::Offered { op, offer } => {
                if !self.current(op, TransferStage::RequestingOffer) {
                    return Self::reject(TransferError::Stale);
                }
                if let Err(error) = self.offer_valid(&offer) {
                    return self.abort(error);
                }
                let encoded_bytes = offer.encoded_bytes;
                let decoded_bytes = offer.decoded_bytes;
                let image_cut = offer.image_cut.clone();
                let chunks = offer.chunks;
                self.replay_cuts.clone_from(&offer.cuts);
                self.offer = Some(offer);
                let Ok(next) = self.allocate(allocator) else {
                    return self.abort(TransferError::Allocator);
                };
                self.stage = TransferStage::ReservingStage;
                self.ok(vec![TransferEffect::ReserveStage {
                    op: next,
                    image_cut,
                    chunks,
                    encoded_bytes,
                    decoded_bytes,
                }])
            }
            TransferEvent::StageReserved { op } => {
                if !self.current(op, TransferStage::ReservingStage) {
                    return Self::reject(TransferError::Stale);
                }
                let Some(offer) = &self.offer else {
                    return self.abort(TransferError::Continuity);
                };
                let capture = offer.capture.clone();
                let cut = offer.image_cut.clone();
                let Ok(next) = self.allocate(allocator) else {
                    return self.abort(TransferError::Allocator);
                };
                self.stage = TransferStage::ReservingDonor;
                self.ok(vec![TransferEffect::ReserveDonor {
                    op: next,
                    capture,
                    cut,
                }])
            }
            TransferEvent::DonorReserved { op, reservation } => {
                if !self.current(op, TransferStage::ReservingDonor) {
                    return Self::reject(TransferError::Stale);
                }
                if self.offer.as_ref().is_none_or(|offer| {
                    reservation.capture != offer.capture
                        || reservation.follower != self.binding.follower
                        || reservation.serial == 0
                }) {
                    return self.abort(TransferError::Continuity);
                }
                self.reservation = Some(reservation);
                let Ok(next) = self.allocate(allocator) else {
                    return self.abort(TransferError::Allocator);
                };
                self.stage = TransferStage::Receiving;
                self.ok(vec![TransferEffect::FetchChunk {
                    op: next,
                    sequence: 0,
                    max_bytes: self.config.max_chunk_bytes,
                }])
            }
            TransferEvent::ChunkStored {
                op,
                sequence,
                bytes,
                decoded_charge,
            } => {
                if !self.current(op, TransferStage::Receiving) {
                    return Self::reject(TransferError::Stale);
                }
                let Some(offer) = &self.offer else {
                    return self.abort(TransferError::Continuity);
                };
                let Some(total) = self.encoded_received.checked_add(bytes) else {
                    return self.abort(TransferError::Capacity);
                };
                if sequence != self.next_chunk
                    || bytes == 0
                    || bytes > self.config.max_chunk_bytes
                    || total > offer.encoded_bytes
                    || decoded_charge > offer.decoded_bytes
                    || decoded_charge > self.config.max_decoded_bytes
                {
                    return self.abort(TransferError::Capacity);
                }
                self.encoded_received = total;
                self.next_chunk += 1;
                let complete = self.next_chunk == offer.chunks;
                if complete && total != offer.encoded_bytes {
                    return self.abort(TransferError::Continuity);
                }
                let commitment = offer.commitment;
                let Ok(next) = self.allocate(allocator) else {
                    return self.abort(TransferError::Allocator);
                };
                if complete {
                    self.stage = TransferStage::VerifyingImage;
                    self.ok(vec![TransferEffect::VerifyImage {
                        op: next,
                        commitment,
                    }])
                } else {
                    self.ok(vec![TransferEffect::FetchChunk {
                        op: next,
                        sequence: self.next_chunk,
                        max_bytes: self.config.max_chunk_bytes,
                    }])
                }
            }
            TransferEvent::ImageVerified { op, commitment } => {
                if !self.current(op, TransferStage::VerifyingImage) {
                    return Self::reject(TransferError::Stale);
                }
                if self
                    .offer
                    .as_ref()
                    .is_none_or(|offer| offer.commitment != commitment)
                {
                    return self.abort(TransferError::Continuity);
                }
                let Some(reservation) = self.reservation.clone() else {
                    return self.abort(TransferError::Continuity);
                };
                let Ok(next) = self.allocate(allocator) else {
                    return self.abort(TransferError::Allocator);
                };
                self.stage = TransferStage::Attaching;
                self.ok(vec![TransferEffect::AttachStream {
                    op: next,
                    reservation,
                }])
            }
            TransferEvent::StreamAttached { op, token } => {
                if !self.current(op, TransferStage::Attaching) {
                    return Self::reject(TransferError::Stale);
                }
                if self.reservation.as_ref() != Some(&token.reservation) || token.operation == 0 {
                    return self.abort(TransferError::Continuity);
                }
                self.attachment = Some(token);
                let Some(reservation) = self.reservation.clone() else {
                    return self.abort(TransferError::Continuity);
                };
                let Ok(next) = self.allocate(allocator) else {
                    return self.abort(TransferError::Allocator);
                };
                self.stage = TransferStage::RequestingBarrier;
                self.ok(vec![TransferEffect::FetchBarrier {
                    op: next,
                    reservation,
                }])
            }
            TransferEvent::BarrierReceived { op, receipt } => {
                if !self.current(op, TransferStage::RequestingBarrier) {
                    return Self::reject(TransferError::Stale);
                }
                if !self.barrier_valid(&receipt) {
                    return self.abort(TransferError::Continuity);
                }
                self.barrier = Some(receipt);
                self.coverage = None;
                self.next_batch_or_coverage(allocator)
            }
            TransferEvent::BatchStaged { op, batch } => {
                if !self.current(op, TransferStage::Replaying) {
                    return Self::reject(TransferError::Stale);
                }
                if !self.batch_valid(&batch) {
                    return self.abort(TransferError::Continuity);
                }
                self.replayed_events += batch.deltas.len();
                self.pending_batch = Some(batch.through.clone());
                let Some(reservation) = self.reservation.clone() else {
                    return self.abort(TransferError::Continuity);
                };
                let batch_operation = batch.operation;
                let through = batch.through;
                let Ok(next) = self.allocate(allocator) else {
                    return self.abort(TransferError::Allocator);
                };
                self.stage = TransferStage::AcknowledgingBatch;
                self.ok(vec![TransferEffect::AckBatch {
                    op: next,
                    reservation,
                    batch_operation,
                    through,
                }])
            }
            TransferEvent::BatchAcknowledged { op, through } => {
                if self.stage != TransferStage::AcknowledgingBatch
                    || self.current != Some(op)
                    || self.pending_batch.as_ref() != Some(&through)
                {
                    return Self::reject(TransferError::Stale);
                }
                self.pending_batch = None;
                self.applied = through.position;
                self.next_batch_or_coverage(allocator)
            }
            TransferEvent::NativeCovered { op, coverage } => {
                if !self.current(op, TransferStage::AwaitingNativeCoverage) {
                    return Self::reject(TransferError::Stale);
                }
                let Some(receipt) = self.barrier.clone() else {
                    return self.abort(TransferError::Continuity);
                };
                if coverage.parent != self.binding.parent
                    || coverage.barrier != receipt
                    || coverage.staged_through != receipt.cursor
                    || coverage.members != receipt.members
                    || !native_cuts_cover(&coverage.proven_cuts, &receipt.covered_cuts)
                    || coverage.buffered_bytes > self.config.max_native_buffer_bytes
                {
                    return self.abort(TransferError::Continuity);
                }
                let Ok(next) = self.allocate(allocator) else {
                    return self.abort(TransferError::Allocator);
                };
                self.coverage = Some(coverage.clone());
                self.stage = TransferStage::Installing;
                self.ok(vec![TransferEffect::InstallCandidate {
                    op: next,
                    coverage: Box::new(coverage),
                }])
            }
            TransferEvent::NativePending { op } => {
                if !self.current(op, TransferStage::AwaitingNativeCoverage) {
                    return Self::reject(TransferError::Stale);
                }
                let Some(due) = self
                    .now
                    .0
                    .checked_add(self.config.coverage_poll_ms)
                    .map(Time)
                else {
                    return self.abort(TransferError::Capacity);
                };
                self.coverage_due = Some(due.min(self.binding.due));
                self.stage = TransferStage::WaitingCoverage;
                self.current = None;
                self.ok(Vec::new())
            }
            TransferEvent::Installed { op, handoff } => {
                if !self.current(op, TransferStage::Installing) {
                    return Self::reject(TransferError::Stale);
                }
                if handoff.recovery.session == 0
                    || handoff.recovery.generation == 0
                    || handoff.recovery.token == 0
                    || handoff.install != op
                    || self.coverage.as_ref() != Some(&handoff.coverage)
                    || self.attachment.as_ref() != Some(&handoff.attachment)
                    || handoff.schema != self.config.expected_schema
                    || self
                        .offer
                        .as_ref()
                        .is_none_or(|offer| handoff.schema != offer.schema)
                    || handoff.applier_generation == 0
                    || !native_cuts_cover(&handoff.continued_cuts, &handoff.coverage.proven_cuts)
                    || handoff.buffered_bytes > self.config.max_native_buffer_bytes
                {
                    return self.abort(TransferError::Continuity);
                }
                self.stage = TransferStage::Completed;
                self.current = None;
                let Some(reservation) = self.reservation.take() else {
                    return self.abort(TransferError::Continuity);
                };
                self.ok(vec![TransferEffect::ReleaseReservation(reservation)])
            }
            TransferEvent::Failed { op } => {
                if self.current != Some(op) {
                    return Self::reject(TransferError::Stale);
                }
                self.abort(TransferError::Continuity)
            }
        }
    }
}
