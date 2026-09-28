//! Exact local admission and timeout receipts for named source acknowledgements.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use groupnet_core::replication::{AckWaitError, AckWaitOutcome, RequiredSubscriber};
use tokio::sync::oneshot;

use super::{Command, SessionHandle, SessionShared, lock};
use crate::replication::ack_api::{
    AckWaitStartError, NamedAckRequest, NamedAckResult, validate_named,
};
use crate::replication::api::{ApplicationAdapter, SourceAdapter};

#[derive(Debug)]
pub(super) struct ActiveSlot {
    pub(super) generation: u64,
    pub(super) request_id: Vec<u8>,
    pub(super) progress: Arc<Mutex<AckProgress>>,
}

#[derive(Debug)]
pub(super) struct AckProgress {
    pub(super) waiting: Vec<RequiredSubscriber>,
    pub(super) terminal: Option<AckWaitOutcome>,
}

#[derive(Debug, Default)]
pub(super) struct AckSlot {
    pub(super) next: u64,
    pub(super) active: Option<ActiveSlot>,
    pub(super) cancel_requested: bool,
}

impl AckSlot {
    pub(super) fn matches(&self, generation: u64, request_id: &[u8]) -> bool {
        self.active.as_ref().is_some_and(|active| {
            active.generation == generation && active.request_id.as_slice() == request_id
        })
    }
}

struct AckWaitGuard {
    shared: Arc<SessionShared>,
    generation: u64,
    request_id: Vec<u8>,
}

impl Drop for AckWaitGuard {
    fn drop(&mut self) {
        let mut slot = lock(&self.shared.ack_slot);
        if !slot.matches(self.generation, &self.request_id) {
            return;
        }
        slot.cancel_requested = true;
        drop(slot);
        let _ = self.shared.commands.try_send(Command::CancelAck {
            generation: self.generation,
            request_id: self.request_id.clone(),
        });
        self.shared.hints.notify_one();
    }
}

impl<S, A> SessionHandle<S, A>
where
    S: SourceAdapter,
    A: ApplicationAdapter<S::Position, S::Batch>,
{
    /// Waits for one exact source-certified named roster without changing the
    /// result of the external commit. The deadline covers queue admission,
    /// source certification, and every later evidence poll. Dropping this
    /// future cancels only this wait, not the state-sync session.
    ///
    /// # Errors
    /// Returns unsupported, admission, or invalid-request errors before a
    /// certified wait starts; terminal wait outcomes are returned as values.
    pub async fn wait_named(
        &self,
        request: NamedAckRequest,
        deadline: Instant,
    ) -> Result<NamedAckResult, AckWaitStartError> {
        let Some(limits) = self.shared.ack_limits else {
            return Err(AckWaitStartError::Unsupported);
        };
        if !self.shared.alive.load(Ordering::Acquire)
            || self.shared.cancelled.load(Ordering::Acquire)
        {
            return Err(AckWaitStartError::Closed);
        }
        validate_named(&request, &self.shared.scope, limits).map_err(AckWaitStartError::Invalid)?;
        let receipt_id = request.request_id.clone();
        let receipt_target = request.target.clone();
        let receipt_kind = request.kind;
        let bind = |outcome| NamedAckResult {
            request_id: receipt_id,
            target: receipt_target,
            kind: receipt_kind,
            outcome,
        };
        let now = Instant::now();
        if now >= deadline {
            return Ok(bind(AckWaitOutcome::TimedOut(request.roster.required)));
        }
        if deadline.duration_since(now).as_millis() > u128::from(limits.max_wait_ms) {
            return Err(AckWaitStartError::Invalid(AckWaitError::Deadline));
        }
        let initial_waiting = request.roster.required.clone();
        let progress = Arc::new(Mutex::new(AckProgress {
            waiting: initial_waiting,
            terminal: None,
        }));
        let generation = {
            let mut slot = lock(&self.shared.ack_slot);
            if slot.active.is_some() {
                return Err(AckWaitStartError::Backpressured);
            }
            let generation = slot
                .next
                .checked_add(1)
                .ok_or(AckWaitStartError::Backpressured)?;
            slot.next = generation;
            slot.active = Some(ActiveSlot {
                generation,
                request_id: request.request_id.clone(),
                progress: Arc::clone(&progress),
            });
            slot.cancel_requested = false;
            generation
        };
        let guard = AckWaitGuard {
            shared: Arc::clone(&self.shared),
            generation,
            request_id: request.request_id.clone(),
        };
        let (reply, received) = oneshot::channel();
        if self
            .shared
            .commands
            .try_send(Command::StartAck {
                generation,
                request: Box::new(request),
                deadline,
                reply,
            })
            .is_err()
        {
            let mut slot = lock(&self.shared.ack_slot);
            if slot.matches(generation, &guard.request_id) {
                slot.active = None;
            }
            return if self.shared.alive.load(Ordering::Acquire) {
                Err(AckWaitStartError::Backpressured)
            } else {
                Err(AckWaitStartError::Closed)
            };
        }
        let result =
            tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), received).await;
        let outcome = match result {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(_)) => Err(AckWaitStartError::Closed),
            Err(_) => {
                let progress = lock(&progress);
                Ok(progress
                    .terminal
                    .clone()
                    .unwrap_or_else(|| AckWaitOutcome::TimedOut(progress.waiting.clone())))
            }
        };
        drop(guard);
        outcome.map(bind)
    }
}
