//! Guarded durable sink binding and whole-batch application.

use std::sync::Arc;
use std::time::Instant;

use groupnet_core::replication::{Batch, Cursor, Event, Operation, RegisterReceipt};

use super::Driver;
use crate::replication::ApplicationAdapter;
use crate::replication::subscription_api::{DurableEventSink, DurableSubscriptionSource};

impl<S, A, D, M> Driver<S, A, D, M>
where
    S: DurableSubscriptionSource,
    A: ApplicationAdapter<S::Position, S::Batch>,
    D: DurableEventSink<S::Position, S::Batch>,
    M: Send + Sync + 'static,
{
    pub(super) async fn bind_sink(
        &mut self,
        op: Operation,
        registration: RegisterReceipt,
        due: Instant,
    ) {
        let Some(permit) = self.shared.fence.issue(op, due) else {
            self.reply(op, Event::Failed { op });
            return;
        };
        let sink = Arc::clone(&self.sink);
        let result = self
            .sink_call(
                due,
                sink.bind_subscriber_epoch(registration.clone(), permit),
            )
            .await;
        self.shared.fence.invalidate();
        match result {
            Some(Ok(checkpoint)) => {
                let Some(source_to_sink) = self.compare(
                    op,
                    &registration.protected,
                    &checkpoint.cursor,
                    &registration.proof,
                ) else {
                    return;
                };
                let Some(sink_to_head) = self.compare(
                    op,
                    &checkpoint.cursor,
                    &registration.proof.head,
                    &registration.proof,
                ) else {
                    return;
                };
                self.reply(
                    op,
                    Event::SinkEpochBound {
                        op,
                        checkpoint: Box::new(checkpoint),
                        source_to_sink: Box::new(source_to_sink),
                        sink_to_head: Box::new(sink_to_head),
                    },
                );
            }
            Some(Err(error)) => self.sink_failure(op, &error),
            None => self.reply(op, Event::Failed { op }),
        }
    }

    pub(super) async fn apply(
        &mut self,
        op: Operation,
        registration: RegisterReceipt,
        batch: Batch,
        previous_sink: Cursor,
        due: Instant,
    ) {
        let Some(payload) = self.payload.take() else {
            self.reply(op, Event::Failed { op });
            return;
        };
        if payload.id != batch.payload_id {
            self.reply(op, Event::Failed { op });
            return;
        }
        let Some(from) = self.decode(op, &batch.coverage.from) else {
            return;
        };
        let Some(through) = self.decode(op, &batch.coverage.through) else {
            return;
        };
        let Some(previous) = self.decode(op, &previous_sink) else {
            return;
        };
        let Some(permit) = self.shared.fence.issue(op, due) else {
            self.reply(op, Event::Failed { op });
            return;
        };
        let sink = Arc::clone(&self.sink);
        let result = self
            .sink_call(
                due,
                sink.apply_subscriber_batch(
                    registration,
                    from,
                    through,
                    previous,
                    payload.native,
                    permit,
                ),
            )
            .await;
        self.shared.fence.invalidate();
        // Keep the native batch's byte reservation through the complete
        // application future, including capacity wait and a timed-out result.
        drop(payload.bytes_permit);
        match result {
            Some(Ok(receipt)) => {
                let Some(proof) = self.proof.clone() else {
                    self.reply(op, Event::Failed { op });
                    return;
                };
                let Some(through_to_sink) =
                    self.compare(op, &batch.coverage.through, &receipt.sink_cursor, &proof)
                else {
                    return;
                };
                let Some(previous_to_sink) =
                    self.compare(op, &previous_sink, &receipt.sink_cursor, &proof)
                else {
                    return;
                };
                self.reply(
                    op,
                    Event::SubscriberApplied {
                        op,
                        receipt: Box::new(receipt),
                        through_to_sink: Box::new(through_to_sink),
                        previous_to_sink: Box::new(previous_to_sink),
                    },
                );
            }
            Some(Err(error)) => self.sink_failure(op, &error),
            None => self.reply(op, Event::Failed { op }),
        }
    }
}
