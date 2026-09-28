//! A reader's finite admission window, independent of its ordinary serve lease.

use crate::Time;
use crate::replication::{BoundComparison, Comparison, Cursor, Scope, SourceProof};

use super::types::{valid_receipt, valid_scope};
use super::{AdmissionId, AdmissionPolicy, PolicyError, RecordBinding, SourceReceipt};

/// Exact append request for one finite reader admission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmissionAppend {
    /// Unique process-session operation token.
    pub token: (u64, u64),
    /// Reader and unique admission incarnation.
    pub id: AdmissionId,
    /// Source history and fleet policy fingerprint to persist with the record.
    pub policy_fingerprint: Vec<u8>,
    /// Original local deadline; an append reply cannot extend it.
    pub deadline: Time,
}

/// A rejected reader transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReaderError {
    /// Invalid policy, identity, or finite bound.
    Policy,
    /// Another admission append or catch-up is in flight.
    Busy,
    /// Delayed or previous-process response.
    Stale,
    /// Native source proof, history, or contiguous projection is invalid.
    Proof,
    /// Clock, token, or deadline exhausted.
    Exhausted,
}

#[derive(Clone, Debug)]
struct Window {
    token: (u64, u64),
    id: AdmissionId,
    deadline: Time,
    cursor: Option<Cursor>,
}

/// Sans-IO local admission core. Callers also check lease and domain policy.
#[derive(Clone, Debug)]
pub struct ReaderCore {
    scope: Scope,
    policy: AdmissionPolicy,
    reader: String,
    session: u64,
    next_token: u64,
    now: Time,
    active: Option<Window>,
    pending: Option<Window>,
}

impl ReaderCore {
    /// Construct a process-local reader. `session` is nonzero and unique while
    /// responses from a previous process could still arrive.
    ///
    /// # Errors
    /// Returns an invalid-policy error for empty identities or zero session.
    pub fn new(
        scope: Scope,
        policy: AdmissionPolicy,
        reader: String,
        session: u64,
    ) -> Result<Self, ReaderError> {
        policy.validate().map_err(|_| ReaderError::Policy)?;
        if reader.is_empty()
            || reader.len() > policy.max_cursor_bytes
            || session == 0
            || !valid_scope(&scope, policy.max_cursor_bytes)
        {
            return Err(ReaderError::Policy);
        }
        Ok(Self {
            scope,
            policy,
            reader,
            session,
            next_token: 1,
            now: Time::ZERO,
            active: None,
            pending: None,
        })
    }

    /// Start an append after sampling `now` on the local monotonic clock.
    /// The caller supplies a source-unique incarnation for this admission.
    ///
    /// # Errors
    /// Returns busy, stale-clock, identity, or overflow error without extending
    /// the existing serving window.
    pub fn begin(&mut self, now: Time, incarnation: u64) -> Result<AdmissionAppend, ReaderError> {
        self.tick(now)?;
        if self.pending.is_some() {
            return Err(ReaderError::Busy);
        }
        if incarnation == 0
            || self
                .active
                .as_ref()
                .is_some_and(|active| active.id.incarnation == incarnation)
        {
            return Err(ReaderError::Policy);
        }
        let deadline = Time(
            now.0
                .checked_add(self.policy.max_duration_ms)
                .ok_or(ReaderError::Exhausted)?,
        );
        let token = (self.session, self.next_token);
        self.next_token = self
            .next_token
            .checked_add(1)
            .ok_or(ReaderError::Exhausted)?;
        let id = AdmissionId {
            reader: self.reader.clone(),
            incarnation,
        };
        self.pending = Some(Window {
            token,
            id: id.clone(),
            deadline,
            cursor: None,
        });
        Ok(AdmissionAppend {
            token,
            id,
            policy_fingerprint: self.policy.fingerprint.clone(),
            deadline,
        })
    }

    /// Accept the exact append only while its original deadline is live.
    /// Returns the cursor that the application must project through.
    ///
    /// # Errors
    /// Returns stale or invalid-proof error without opening read admission.
    pub fn confirmed(
        &mut self,
        now: Time,
        token: (u64, u64),
        receipt: &SourceReceipt,
    ) -> Result<Cursor, ReaderError> {
        self.tick(now)?;
        let pending = self.pending.as_mut().ok_or(ReaderError::Stale)?;
        if pending.token != token {
            return Err(ReaderError::Stale);
        }
        if !valid_receipt(receipt, &self.scope, &self.policy)
            || receipt.binding != RecordBinding::Admission(pending.id.clone())
        {
            return Err(ReaderError::Proof);
        }
        match &pending.cursor {
            Some(existing) if existing != &receipt.cursor => return Err(ReaderError::Proof),
            Some(_) => {}
            None => pending.cursor = Some(receipt.cursor.clone()),
        }
        Ok(receipt.cursor.clone())
    }

    /// Activate only after the adapter certifies contiguous materialization
    /// through this admission in the same source history.
    ///
    /// # Errors
    /// Returns stale or invalid-proof error without extending admission.
    pub fn projected(
        &mut self,
        now: Time,
        token: (u64, u64),
        applied: &Cursor,
        proof: &SourceProof,
        admission_to_applied: &BoundComparison,
        applied_to_head: &BoundComparison,
    ) -> Result<(), ReaderError> {
        self.tick(now)?;
        let pending = self.pending.as_ref().ok_or(ReaderError::Stale)?;
        if pending.token != token {
            return Err(ReaderError::Stale);
        }
        let admission = pending.cursor.as_ref().ok_or(ReaderError::Proof)?;
        if applied.history != self.policy.history
            || applied
                .validate(&self.scope, self.policy.max_cursor_bytes)
                .is_err()
            || proof
                .validate(&self.scope, self.policy.max_cursor_bytes)
                .is_err()
            || proof.head.history != self.policy.history
            || !matches!(
                admission_to_applied.for_operands(admission, applied, &proof.id),
                Some(Comparison::Before | Comparison::Equal)
            )
            || !matches!(
                applied_to_head.for_operands(applied, &proof.head, &proof.id),
                Some(Comparison::Before | Comparison::Equal)
            )
        {
            return Err(ReaderError::Proof);
        }
        self.active = self.pending.take();
        Ok(())
    }

    /// Advance local logical time, closing expired pending and active windows.
    ///
    /// # Errors
    /// Returns [`ReaderError::Stale`] for a backward clock event.
    pub fn tick(&mut self, now: Time) -> Result<(), ReaderError> {
        if now < self.now {
            return Err(ReaderError::Stale);
        }
        self.now = now;
        if self
            .active
            .as_ref()
            .is_some_and(|window| now >= window.deadline)
        {
            self.active = None;
        }
        if self
            .pending
            .as_ref()
            .is_some_and(|window| now >= window.deadline)
        {
            self.pending = None;
        }
        Ok(())
    }

    /// Exact active admission after a fresh local monotonic clock sample.
    /// The caller must still check ordinary lease and domain read authority;
    /// a renewal after an intent cannot reopen its fenced key without replay.
    ///
    /// # Errors
    /// Returns stale error for a backward clock sample.
    pub fn admitted_at(&mut self, now: Time) -> Result<Option<&AdmissionId>, ReaderError> {
        self.tick(now)?;
        Ok(self.active.as_ref().map(|window| &window.id))
    }

    /// Revoke all local admission, including any in-flight renewal.
    pub fn cancel(&mut self) {
        self.active = None;
        self.pending = None;
    }
}

impl From<PolicyError> for ReaderError {
    fn from(_: PolicyError) -> Self {
        Self::Policy
    }
}
