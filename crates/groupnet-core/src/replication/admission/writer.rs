//! An intent's bounded pre-mutation fence over source-ordered readers.

use std::collections::BTreeSet;

use crate::Time;
use crate::replication::{BoundComparison, Comparison, Coverage, Cursor, Scope, SourceProof};

use super::types::{valid_receipt, valid_scope};
use super::{AdmissionPolicy, AdmissionRef, RecordBinding, SourceReceipt};

/// Durable intent append requested before any origin mutation is dispatched.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendIntent {
    /// Process-session operation token.
    pub token: (u64, u64),
    /// Stable idempotency identity of this intent.
    pub op_id: String,
    /// Persisted fleet policy fingerprint.
    pub policy_fingerprint: Vec<u8>,
}

/// One prior admission and its source-bound order before the intent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RosterEntry {
    /// Exact prior admission record.
    pub admission: AdmissionRef,
    /// Source comparison bound to the admission and intent positions.
    pub before_intent: BoundComparison,
}

/// Trusted adapter's certified contiguous prefix through this exact intent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RosterReceipt {
    /// Source proof that includes the prefix and its native head.
    pub proof: SourceProof,
    /// Adapter-validated contiguous coverage through the exact intent.
    pub coverage: Coverage,
    /// Exact source comparison of the covered prefix start to the intent.
    pub prefix_to_intent: BoundComparison,
    /// Exact comparison between the intent and proof head.
    pub intent_to_head: BoundComparison,
    /// Earlier admissions still required by this source projection.
    pub entries: Vec<RosterEntry>,
}

/// Reader's acknowledgement that serving was revoked for this exact pair.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AckReceipt {
    /// Writer process-session operation token.
    pub token: (u64, u64),
    /// Exact intent cursor being fenced.
    pub intent: Cursor,
    /// Exact reader admission incarnation and native position revoked.
    pub admission: AdmissionRef,
}

/// Rejected writer transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriterError {
    /// Invalid fleet policy or intent identity.
    Policy,
    /// The transition is too early or already completed.
    Stage,
    /// Delayed or previous-process message or backward clock.
    Stale,
    /// Source identity, exact comparison, or prefix certification is invalid.
    Proof,
    /// Admission roster exceeds its finite bound.
    Backpressure,
    /// Local deadline or token cannot be represented.
    Exhausted,
}

#[derive(Clone, Debug)]
struct Waiter {
    admission: AdmissionRef,
    acknowledged: bool,
}

#[derive(Clone, Debug)]
enum Stage {
    Idle,
    Appending,
    Waiting {
        intent: Cursor,
        wait_until: Time,
        roster: Option<Vec<Waiter>>,
    },
    Released,
    Cancelled,
}

/// Sans-IO writer fence for one intent and one process incarnation.
#[derive(Clone, Debug)]
pub struct WriterCore {
    scope: Scope,
    policy: AdmissionPolicy,
    session: u64,
    op_id: String,
    now: Time,
    stage: Stage,
}

impl WriterCore {
    /// Create a writer for a stable operation ID. On restart construct a new
    /// core with a new session and repeat the full expiry wait after readback.
    ///
    /// # Errors
    /// Returns invalid-policy error for empty identity or zero session.
    pub fn new(
        scope: Scope,
        policy: AdmissionPolicy,
        session: u64,
        op_id: String,
    ) -> Result<Self, WriterError> {
        policy.validate().map_err(|_| WriterError::Policy)?;
        if session == 0
            || op_id.is_empty()
            || op_id.len() > policy.max_cursor_bytes
            || !valid_scope(&scope, policy.max_cursor_bytes)
        {
            return Err(WriterError::Policy);
        }
        Ok(Self {
            scope,
            policy,
            session,
            op_id,
            now: Time::ZERO,
            stage: Stage::Idle,
        })
    }

    /// Request a durable intent append. It does not permit origin dispatch.
    ///
    /// # Errors
    /// Returns stale-clock or stage error.
    pub fn begin(&mut self, now: Time) -> Result<AppendIntent, WriterError> {
        self.tick(now)?;
        if !matches!(self.stage, Stage::Idle) {
            return Err(WriterError::Stage);
        }
        self.stage = Stage::Appending;
        Ok(AppendIntent {
            token: (self.session, 1),
            op_id: self.op_id.clone(),
            policy_fingerprint: self.policy.fingerprint.clone(),
        })
    }

    /// Confirm the exact intent and start the local conservative wait *now*.
    ///
    /// # Errors
    /// Returns stale, proof, stage, or deadline-overflow error.
    pub fn confirmed(
        &mut self,
        now: Time,
        token: (u64, u64),
        receipt: &SourceReceipt,
    ) -> Result<(), WriterError> {
        self.tick(now)?;
        if token != (self.session, 1) {
            return Err(WriterError::Stale);
        }
        if !matches!(self.stage, Stage::Appending) {
            return Err(WriterError::Stage);
        }
        if !valid_receipt(receipt, &self.scope, &self.policy)
            || receipt.binding != RecordBinding::Intent(self.op_id.clone())
        {
            return Err(WriterError::Proof);
        }
        let wait = self
            .policy
            .expiry_wait_ms()
            .map_err(|_| WriterError::Exhausted)?;
        let wait_until = Time(now.0.checked_add(wait).ok_or(WriterError::Exhausted)?);
        self.stage = Stage::Waiting {
            intent: receipt.cursor.clone(),
            wait_until,
            roster: None,
        };
        Ok(())
    }

    /// Install a bounded, adapter-certified prefix through the exact intent.
    /// Returns exact admissions for targeted invalidation messages.
    ///
    /// # Errors
    /// Returns proof, policy, backpressure, or stage error; no partial roster
    /// is installed on rejection.
    pub fn roster(&mut self, receipt: &RosterReceipt) -> Result<Vec<AdmissionRef>, WriterError> {
        let Stage::Waiting { intent, roster, .. } = &mut self.stage else {
            return Err(WriterError::Stage);
        };
        if roster.is_some() {
            return Err(WriterError::Stage);
        }
        if receipt.entries.len() > self.policy.max_waiters {
            return Err(WriterError::Backpressure);
        }
        if receipt
            .proof
            .validate(&self.scope, self.policy.max_cursor_bytes)
            .is_err()
            || receipt.proof.head.history != self.policy.history
            || receipt.coverage.through != *intent
            || receipt.coverage.proof != receipt.proof.id
            || receipt.coverage.certificate.is_empty()
            || receipt.coverage.certificate.len() > self.policy.max_cursor_bytes
            || receipt.coverage.from.history != self.policy.history
            || receipt
                .coverage
                .from
                .validate(&self.scope, self.policy.max_cursor_bytes)
                .is_err()
            || !matches!(
                receipt.prefix_to_intent.for_operands(
                    &receipt.coverage.from,
                    intent,
                    &receipt.proof.id
                ),
                Some(Comparison::Before | Comparison::Equal)
            )
            || !matches!(
                receipt
                    .intent_to_head
                    .for_operands(intent, &receipt.proof.head, &receipt.proof.id),
                Some(Comparison::Before | Comparison::Equal)
            )
        {
            return Err(WriterError::Proof);
        }
        let mut ids = BTreeSet::new();
        let mut waiters = Vec::with_capacity(receipt.entries.len());
        for entry in &receipt.entries {
            let admission = &entry.admission;
            if admission.id.reader.is_empty()
                || admission.id.reader.len() > self.policy.max_cursor_bytes
                || admission.id.incarnation == 0
                || admission.policy_fingerprint != self.policy.fingerprint
                || admission.cursor.history != self.policy.history
                || admission
                    .cursor
                    .validate(&self.scope, self.policy.max_cursor_bytes)
                    .is_err()
                || entry
                    .before_intent
                    .for_operands(&admission.cursor, intent, &receipt.proof.id)
                    != Some(Comparison::Before)
                || !ids.insert((admission.id.reader.clone(), admission.id.incarnation))
            {
                return Err(WriterError::Proof);
            }
            waiters.push(Waiter {
                admission: admission.clone(),
                acknowledged: false,
            });
        }
        let required = waiters.iter().map(|w| w.admission.clone()).collect();
        *roster = Some(waiters);
        Ok(required)
    }

    /// Accept only an invalidation acknowledgement bound to this intent and
    /// one exact earlier admission, after the reader revoked local serving.
    ///
    /// # Errors
    /// Returns stale or stage error without releasing another admission.
    pub fn acknowledge(&mut self, receipt: &AckReceipt) -> Result<(), WriterError> {
        let Stage::Waiting {
            intent,
            roster: Some(waiters),
            ..
        } = &mut self.stage
        else {
            return Err(WriterError::Stage);
        };
        if receipt.token != (self.session, 1) || receipt.intent != *intent {
            return Err(WriterError::Stale);
        }
        let waiter = waiters
            .iter_mut()
            .find(|waiter| waiter.admission == receipt.admission)
            .ok_or(WriterError::Stale)?;
        waiter.acknowledged = true;
        Ok(())
    }

    /// Advance writer-local monotonic time.
    ///
    /// # Errors
    /// Returns stale error for a backward clock event.
    pub fn tick(&mut self, now: Time) -> Result<(), WriterError> {
        if now < self.now {
            return Err(WriterError::Stale);
        }
        self.now = now;
        Ok(())
    }

    /// Release the pre-mutation fence at most once, after every exact ack or
    /// the global conservative wait and a certified roster. This does not
    /// prove the origin operation committed or permit replay of ambiguity.
    ///
    /// # Errors
    /// Returns stage error if the prefix roster has not been certified.
    pub fn take_fence(&mut self, now: Time) -> Result<bool, WriterError> {
        self.tick(now)?;
        let Stage::Waiting {
            wait_until,
            roster: Some(waiters),
            ..
        } = &self.stage
        else {
            return Err(WriterError::Stage);
        };
        if self.now < *wait_until && waiters.iter().any(|waiter| !waiter.acknowledged) {
            return Ok(false);
        }
        self.stage = Stage::Released;
        Ok(true)
    }

    /// Cancel this writer incarnation. No late acknowledgement may release it.
    pub fn cancel(&mut self) {
        self.stage = Stage::Cancelled;
    }
}
