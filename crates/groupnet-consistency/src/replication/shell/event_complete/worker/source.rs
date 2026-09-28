//! Source-authoritative registration, bounded protected replay, and ack I/O.

use std::sync::Arc;
use std::time::Instant;

use groupnet_core::replication::{
    Batch, CommitSubscriberAck, Coverage, Cursor, Event, Operation, RegisterSubscriber, Scope,
    SourceProof, SubscriberKey, SubscriptionError,
};

use super::{Driver, NativePayload};
use crate::replication::ApplicationAdapter;
use crate::replication::api::{ScanLimit, SourceBatch, TailLimit};
use crate::replication::subscription_api::{
    DurableEventSink, DurableSubscriptionSource, SubscriptionSourceResult,
};
use groupnet_core::replication::RegisterReceipt;

impl<S, A, D, M> Driver<S, A, D, M>
where
    S: DurableSubscriptionSource,
    A: ApplicationAdapter<S::Position, S::Batch>,
    D: DurableEventSink<S::Position, S::Batch>,
    M: Send + Sync + 'static,
{
    fn conclusive(&mut self, op: Operation, error: SubscriptionError) {
        self.reply(op, Event::SubscriberRejected { op, error });
    }

    pub(super) fn decode(&mut self, op: Operation, cursor: &Cursor) -> Option<S::Position> {
        match self.manager.source.position(cursor) {
            Ok(position) => Some(position),
            Err(error) => {
                self.source_failure(op, &error);
                None
            }
        }
    }

    pub(super) fn compare(
        &mut self,
        op: Operation,
        left: &Cursor,
        right: &Cursor,
        proof: &SourceProof,
    ) -> Option<groupnet_core::replication::BoundComparison> {
        match self.manager.source.compare(left, right, proof) {
            Ok(comparison) => Some(comparison),
            Err(error) => {
                self.source_failure(op, &error);
                None
            }
        }
    }

    pub(super) async fn read_current(&mut self, op: Operation, key: SubscriberKey, due: Instant) {
        let source = Arc::clone(&self.manager.source);
        let result = self
            .source_call(due, source.read_current_subscriber(key))
            .await;
        match result {
            Some(SubscriptionSourceResult::Accepted(state)) => self.reply(
                op,
                Event::CurrentSubscriberRead {
                    op,
                    state: state.map(Box::new),
                },
            ),
            Some(SubscriptionSourceResult::Rejected(error)) => self.conclusive(op, error),
            Some(SubscriptionSourceResult::Failed(error)) => self.source_failure(op, &error),
            None => self.reply(op, Event::Failed { op }),
        }
    }

    pub(super) async fn register(
        &mut self,
        op: Operation,
        request: RegisterSubscriber,
        due: Instant,
    ) {
        let source = Arc::clone(&self.manager.source);
        let result = self
            .source_call(due, source.register_subscriber(request))
            .await;
        match result {
            Some(SubscriptionSourceResult::Accepted(receipt)) => self.reply(
                op,
                Event::SubscriberRegistered {
                    op,
                    receipt: Box::new(receipt),
                },
            ),
            Some(SubscriptionSourceResult::Rejected(error)) => self.conclusive(op, error),
            Some(SubscriptionSourceResult::Failed(error)) => self.source_failure(op, &error),
            None => self.reply(op, Event::Failed { op }),
        }
    }

    pub(super) async fn read_registration(
        &mut self,
        op: Operation,
        key: SubscriberKey,
        request_id: Vec<u8>,
        due: Instant,
    ) {
        let source = Arc::clone(&self.manager.source);
        let result = self
            .source_call(due, source.read_subscriber_registration(key, request_id))
            .await;
        match result {
            Some(SubscriptionSourceResult::Accepted(receipt)) => self.reply(
                op,
                Event::SubscriberRegistrationRead {
                    op,
                    receipt: receipt.map(Box::new),
                },
            ),
            Some(SubscriptionSourceResult::Rejected(error)) => self.conclusive(op, error),
            Some(SubscriptionSourceResult::Failed(error)) => self.source_failure(op, &error),
            None => self.reply(op, Event::Failed { op }),
        }
    }

    pub(super) async fn check_tail(
        &mut self,
        op: Operation,
        registration: RegisterReceipt,
        from: Cursor,
        due: Instant,
    ) {
        let Some(position) = self.decode(op, &from) else {
            return;
        };
        let source = Arc::clone(&self.manager.source);
        let limit = TailLimit {
            events: self.manager.limits.core.max_batch_events,
            bytes: self.manager.limits.core.max_batch_bytes,
        };
        let result = self
            .source_call_charged(
                due,
                self.manager.limits.core.max_batch_bytes,
                source.subscriber_tail(registration, position, limit),
            )
            .await;
        match result {
            Some(SubscriptionSourceResult::Accepted(proof)) => {
                let Some(retained_to_ack) = self.compare(op, &proof.retained_from, &from, &proof)
                else {
                    return;
                };
                let Some(ack_to_head) = self.compare(op, &from, &proof.head, &proof) else {
                    return;
                };
                let Some(retained_to_head) =
                    self.compare(op, &proof.retained_from, &proof.head, &proof)
                else {
                    return;
                };
                let accepted = proof.clone();
                let applied = self.reply_accepted(
                    op,
                    Event::SubscriberTail {
                        op,
                        proof,
                        comparisons: vec![retained_to_ack, ack_to_head, retained_to_head],
                    },
                );
                if applied {
                    self.proof = Some(accepted);
                }
            }
            Some(SubscriptionSourceResult::Rejected(error)) => self.conclusive(op, error),
            Some(SubscriptionSourceResult::Failed(error)) => self.source_failure(op, &error),
            None => self.reply(op, Event::Failed { op }),
        }
    }

    pub(super) async fn scan(
        &mut self,
        op: Operation,
        registration: RegisterReceipt,
        from: Cursor,
        max_events: usize,
        max_bytes: usize,
        due: Instant,
    ) {
        let Some(position) = self.decode(op, &from) else {
            return;
        };
        let Some(proof) = self.proof.clone() else {
            self.reply(op, Event::Failed { op });
            return;
        };
        let Some(charge) = u32::try_from(max_bytes).ok() else {
            self.conclusive(op, SubscriptionError::Bounds);
            return;
        };
        let source = Arc::clone(&self.manager.source);
        let operations = Arc::clone(&self.manager.operations);
        let bytes = Arc::clone(&self.manager.bytes);
        let result = tokio::time::timeout_at(tokio::time::Instant::from_std(due), async {
            let reserved = bytes.acquire_many_owned(charge).await.ok()?;
            let _operation = operations.acquire_owned().await.ok()?;
            let batch = source
                .scan_subscriber(
                    registration,
                    position,
                    proof.clone(),
                    ScanLimit {
                        events: max_events,
                        bytes: max_bytes,
                    },
                )
                .await;
            Some((batch, reserved))
        })
        .await
        .ok()
        .flatten();
        match result {
            Some((SubscriptionSourceResult::Accepted(batch), reserved)) => {
                self.accept_batch(op, &from, &proof, batch, reserved);
            }
            Some((SubscriptionSourceResult::Rejected(error), _)) => self.conclusive(op, error),
            Some((SubscriptionSourceResult::Failed(error), _)) => self.source_failure(op, &error),
            None => self.reply(op, Event::Failed { op }),
        }
    }

    fn accept_batch(
        &mut self,
        op: Operation,
        from: &Cursor,
        proof: &SourceProof,
        source_batch: SourceBatch<S::Position, S::Batch>,
        reserved: tokio::sync::OwnedSemaphorePermit,
    ) {
        let SourceBatch {
            from: reported_from,
            through,
            proof: reported_proof,
            certificate,
            events,
            bytes,
            native,
        } = source_batch;
        if events == 0
            || events > self.manager.limits.core.max_batch_events
            || bytes == 0
            || bytes > self.manager.limits.core.max_batch_bytes
        {
            self.conclusive(op, SubscriptionError::Backpressured);
            return;
        }
        let scope: &Scope = &self.shared.key.scope;
        let from_cursor = match self.manager.source.cursor(scope, &reported_from) {
            Ok(cursor) => cursor,
            Err(error) => return self.source_failure(op, &error),
        };
        let through_cursor = match self.manager.source.cursor(scope, &through) {
            Ok(cursor) => cursor,
            Err(error) => return self.source_failure(op, &error),
        };
        let Some(advance) = self.compare(op, from, &through_cursor, proof) else {
            return;
        };
        let Some(end_to_head) = self.compare(op, &through_cursor, &proof.head, proof) else {
            return;
        };
        self.reply(
            op,
            Event::SubscriberScanned {
                op,
                batch: Box::new(Batch {
                    coverage: Coverage {
                        from: from_cursor,
                        through: through_cursor,
                        proof: reported_proof,
                        certificate,
                    },
                    payload_id: op.token,
                    events,
                    bytes,
                    advance,
                    end_to_head,
                }),
            },
        );
        if self.engine.state().stage == groupnet_core::replication::Stage::ApplyingSubscriber {
            self.payload = Some(NativePayload {
                id: op.token,
                native,
                bytes_permit: reserved,
            });
        }
    }

    pub(super) async fn commit_ack(
        &mut self,
        op: Operation,
        request: CommitSubscriberAck,
        due: Instant,
    ) {
        let source = Arc::clone(&self.manager.source);
        let result = self
            .source_call(due, source.commit_subscriber_ack(request))
            .await;
        match result {
            Some(SubscriptionSourceResult::Accepted(receipt)) => self.reply(
                op,
                Event::SubscriberAcked {
                    op,
                    receipt: Box::new(receipt),
                },
            ),
            Some(SubscriptionSourceResult::Rejected(error)) => self.conclusive(op, error),
            Some(SubscriptionSourceResult::Failed(error)) => self.source_failure(op, &error),
            None => self.reply(op, Event::Failed { op }),
        }
    }

    pub(super) async fn read_ack(
        &mut self,
        op: Operation,
        request: CommitSubscriberAck,
        due: Instant,
    ) {
        let source = Arc::clone(&self.manager.source);
        let result = self
            .source_call(due, source.read_subscriber_ack(request))
            .await;
        match result {
            Some(SubscriptionSourceResult::Accepted(receipt)) => self.reply(
                op,
                Event::SubscriberAckRead {
                    op,
                    receipt: receipt.map(Box::new),
                },
            ),
            Some(SubscriptionSourceResult::Rejected(error)) => self.conclusive(op, error),
            Some(SubscriptionSourceResult::Failed(error)) => self.source_failure(op, &error),
            None => self.reply(op, Event::Failed { op }),
        }
    }
}
