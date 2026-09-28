//! Bounded follower attachment, barrier, and batch lifecycle.

use super::{DonorJournal, Follower, OutstandingBatch};
use crate::Time;
use crate::volatile_bootstrap::ClaimIdentity;
use crate::volatile_bootstrap::journal::types::{
    AttachToken, BarrierReceipt, DeltaIdentity, JournalBatch, JournalCursor, JournalError,
    JournalState, ReservationId, ReservationStage,
};

impl DonorJournal {
    fn follower_index(&self, reservation: &ReservationId) -> Result<usize, JournalError> {
        self.followers
            .iter()
            .position(|follower| follower.id == *reservation)
            .ok_or(JournalError::Stale)
    }

    fn operation_token(&mut self) -> Result<u64, JournalError> {
        let token = self.next_operation;
        self.next_operation = token.checked_add(1).ok_or(JournalError::Exhausted)?;
        Ok(token)
    }

    /// Reserve one exact follower from image cut C, without truncating the
    /// shared suffix. A reopened follower gets a fresh serial.
    ///
    /// # Errors
    /// Rejects forged cursors, duplicate live follower, or finite capacity.
    pub fn reserve(
        &mut self,
        now: Time,
        follower: ClaimIdentity,
        from: &JournalCursor,
    ) -> Result<ReservationId, JournalError> {
        self.advance(now)?;
        if self.state != JournalState::Active {
            return Err(JournalError::Stage);
        }
        if from.capture != self.id || from.position != 0 {
            return Err(JournalError::Stale);
        }
        if follower.node.as_str().is_empty()
            || follower.node.as_str().len() > self.config.max_follower_id_bytes
            || follower.incarnation == 0
            || follower.session == 0
            || follower.attempt == 0
            || self.followers.len() >= self.config.max_followers.saturating_sub(self.aborted.len())
        {
            return Err(JournalError::Capacity);
        }
        if self
            .followers
            .iter()
            .any(|reserved| reserved.id.follower == follower)
        {
            return Err(JournalError::Stale);
        }
        let due = now
            .0
            .checked_add(self.config.max_follower_ms)
            .map(Time)
            .ok_or(JournalError::Exhausted)?
            .min(self.expires.ok_or(JournalError::Stage)?);
        let serial = self.next_reservation;
        self.next_reservation = serial.checked_add(1).ok_or(JournalError::Exhausted)?;
        let id = ReservationId {
            capture: self.id.clone(),
            follower,
            serial,
        };
        self.followers.push(Follower {
            id: id.clone(),
            due,
            stage: ReservationStage::Reserved,
            attach_operation: None,
            barrier: None,
            acked: 0,
            outstanding: None,
        });
        Ok(id)
    }

    /// Issue one exact attachment token. The runtime must establish the live
    /// donor stream before confirming it; no barrier exists yet.
    ///
    /// # Errors
    /// Rejects stale or already attaching reservations.
    pub fn begin_attach(
        &mut self,
        now: Time,
        reservation: &ReservationId,
    ) -> Result<AttachToken, JournalError> {
        self.advance(now)?;
        let index = self.follower_index(reservation)?;
        if self.followers[index].stage != ReservationStage::Reserved {
            return Err(JournalError::Stage);
        }
        let operation = self.operation_token()?;
        self.followers[index].stage = ReservationStage::Attaching;
        self.followers[index].attach_operation = Some(operation);
        Ok(AttachToken {
            reservation: reservation.clone(),
            operation,
        })
    }

    /// Confirm that the exact live stream is attached. Duplicate confirmation
    /// is harmless, but an old reservation or operation cannot attach anew.
    ///
    /// # Errors
    /// Rejects unmatched attachment callbacks.
    pub fn confirm_attach(&mut self, now: Time, token: &AttachToken) -> Result<(), JournalError> {
        self.advance(now)?;
        let index = self.follower_index(&token.reservation)?;
        let follower = &mut self.followers[index];
        if follower.attach_operation != Some(token.operation)
            || !matches!(
                follower.stage,
                ReservationStage::Attaching | ReservationStage::Attached
            )
        {
            return Err(JournalError::Stale);
        }
        follower.stage = ReservationStage::Attached;
        Ok(())
    }

    /// Sample local barrier B and exact native cuts together after stream
    /// attachment. Repeated calls read back the same receipt, even if later
    /// index mutations advance the donor's current cuts.
    ///
    /// # Errors
    /// Rejects unattached or stale follower reservations.
    pub fn barrier(
        &mut self,
        now: Time,
        reservation: &ReservationId,
    ) -> Result<BarrierReceipt, JournalError> {
        self.advance(now)?;
        let index = self.follower_index(reservation)?;
        if self.followers[index].stage != ReservationStage::Attached {
            return Err(JournalError::Stage);
        }
        if let Some(receipt) = &self.followers[index].barrier {
            return Ok(receipt.clone());
        }
        let barrier_operation = self.operation_token()?;
        let receipt = BarrierReceipt {
            reservation: reservation.clone(),
            attach_operation: self.followers[index]
                .attach_operation
                .ok_or(JournalError::Stage)?,
            barrier_operation,
            cursor: self.cursor(self.last_position),
            covered_cuts: self.cuts.clone(),
            members: self.members.clone(),
        };
        self.followers[index].barrier = Some(receipt.clone());
        Ok(receipt)
    }

    /// Sample a later B/cut only after the previous exact barrier was fully
    /// applied and acknowledged. The attached stream stays live throughout.
    ///
    /// # Errors
    /// Rejects stale barriers or an unacknowledged previous suffix.
    pub fn advance_barrier(
        &mut self,
        now: Time,
        reservation: &ReservationId,
        expected: &BarrierReceipt,
    ) -> Result<BarrierReceipt, JournalError> {
        self.advance(now)?;
        let index = self.follower_index(reservation)?;
        let follower = &self.followers[index];
        if follower.stage != ReservationStage::Attached
            || follower.barrier.as_ref() != Some(expected)
            || follower.acked != expected.cursor.position
            || follower.outstanding.is_some()
        {
            return Err(JournalError::Stale);
        }
        let barrier_operation = self.operation_token()?;
        let receipt = BarrierReceipt {
            reservation: reservation.clone(),
            attach_operation: expected.attach_operation,
            barrier_operation,
            cursor: self.cursor(self.last_position),
            covered_cuts: self.cuts.clone(),
            members: self.members.clone(),
        };
        self.followers[index].barrier = Some(receipt.clone());
        Ok(receipt)
    }

    /// Return a bounded contiguous suffix batch with separately charged
    /// in-flight clone memory. At most one batch is outstanding per follower.
    ///
    /// # Errors
    /// Rejects forged barriers, unattached followers, or backpressure.
    pub fn read_batch(
        &mut self,
        now: Time,
        reservation: &ReservationId,
        barrier: &BarrierReceipt,
    ) -> Result<Option<JournalBatch>, JournalError> {
        self.advance(now)?;
        let index = self.follower_index(reservation)?;
        if self.followers[index].stage != ReservationStage::Attached {
            return Err(JournalError::Stage);
        }
        if self.followers[index].barrier.as_ref() != Some(barrier)
            || barrier.reservation != *reservation
            || barrier.cursor.capture != self.id
            || barrier.cursor.position > self.last_position
            || barrier.cursor.position < self.followers[index].acked
        {
            return Err(JournalError::Stale);
        }
        if self.followers[index].outstanding.is_some() {
            return Err(JournalError::Capacity);
        }
        let from = self.followers[index].acked;
        if from == barrier.cursor.position {
            return Ok(None);
        }
        let mut count = 0usize;
        let mut bytes = 0usize;
        let mut last = from;
        for delta in self
            .deltas
            .iter()
            .filter(|delta| delta.position > from && delta.position <= barrier.cursor.position)
        {
            let identity_bytes = match &delta.identity {
                DeltaIdentity::Native(cut) => cut.writer.len(),
                DeltaIdentity::Local(id) => id.len(),
            };
            let event_bytes = identity_bytes
                .checked_add(delta.effect.len())
                .ok_or(JournalError::Capacity)?;
            let next_bytes = bytes
                .checked_add(event_bytes)
                .ok_or(JournalError::Capacity)?;
            if count >= self.config.max_batch_events || next_bytes > self.config.max_batch_bytes {
                break;
            }
            bytes = next_bytes;
            count += 1;
            last = delta.position;
        }
        let new_inflight = self
            .inflight_bytes
            .checked_add(bytes)
            .ok_or(JournalError::Capacity)?;
        if count == 0 || new_inflight > self.config.max_inflight_bytes {
            return Err(JournalError::Capacity);
        }
        let operation = self.operation_token()?;
        self.followers[index].outstanding = Some(OutstandingBatch {
            operation,
            through: last,
            bytes,
        });
        self.inflight_bytes = new_inflight;
        let deltas = self
            .deltas
            .iter()
            .filter(|delta| delta.position > from && delta.position <= last)
            .take(count)
            .cloned()
            .collect();
        Ok(Some(JournalBatch {
            reservation: reservation.clone(),
            operation,
            from: self.cursor(from),
            through: self.cursor(last),
            deltas,
            bytes,
        }))
    }

    /// Advance only the exact returned batch after private follower staging
    /// and application. The candidate keeps the shared suffix for others.
    ///
    /// # Errors
    /// Rejects old or forged batch operations and positions.
    pub fn ack_batch(
        &mut self,
        now: Time,
        reservation: &ReservationId,
        operation: u64,
        through: &JournalCursor,
    ) -> Result<JournalCursor, JournalError> {
        self.advance(now)?;
        let index = self.follower_index(reservation)?;
        let Some(outstanding) = self.followers[index].outstanding.clone() else {
            return Err(JournalError::Stale);
        };
        if through.capture != self.id
            || outstanding.operation != operation
            || outstanding.through != through.position
        {
            return Err(JournalError::Stale);
        }
        self.inflight_bytes -= outstanding.bytes;
        self.followers[index].acked = outstanding.through;
        self.followers[index].outstanding = None;
        Ok(self.cursor(through.position))
    }

    /// Read the exact acknowledged local position for ambiguous ack readback.
    ///
    /// # Errors
    /// Rejects old reservation incarnations.
    pub fn acknowledged(
        &mut self,
        now: Time,
        reservation: &ReservationId,
    ) -> Result<JournalCursor, JournalError> {
        self.advance(now)?;
        let index = self.follower_index(reservation)?;
        Ok(self.cursor(self.followers[index].acked))
    }

    /// Release one exact follower and its in-flight clone charge only after
    /// the runtime has dropped its returned batch and stream resources. The
    /// shared suffix stays intact until candidate invalidation/expiry.
    ///
    /// # Errors
    /// Rejects stale releases, including an old incarnation after reopen.
    pub fn release(&mut self, now: Time, reservation: &ReservationId) -> Result<(), JournalError> {
        self.advance(now)?;
        let index = self.follower_index(reservation)?;
        let follower = self.followers.remove(index);
        if let Some(outstanding) = follower.outstanding {
            self.inflight_bytes -= outstanding.bytes;
        }
        Ok(())
    }
}
