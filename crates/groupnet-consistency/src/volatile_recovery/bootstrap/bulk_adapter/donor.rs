//! Groupnet-owned donor journal dispatch under one synchronous ingress cut.

use groupnet_core::Time;
use groupnet_core::volatile_bootstrap::journal::JournalError;
use groupnet_transport::bulk::BulkTransport;

use super::{BootstrapStatePort, BulkDonorPort};
use crate::volatile_recovery::AdapterError;
use crate::volatile_recovery::bootstrap::admission::{AdmissionClass, ByteAdmission};
use crate::volatile_recovery::bootstrap::ports::{DonorCapture, DonorReply, DonorRequest};

impl<A: BootstrapStatePort, B: BulkTransport> BulkDonorPort<A, B> {
    #[expect(
        clippy::too_many_lines,
        reason = "one atomic ingress dispatch retains exact journal request ordering"
    )]
    pub(super) fn prepare_donor_request(
        &self,
        request: &DonorRequest,
        capture: &DonorCapture<A::Image>,
        now: Time,
        admission: &ByteAdmission,
    ) -> Result<DonorReply, AdapterError> {
        if !self.admission.same_pool(admission) || !capture.is_active() {
            return Err(AdapterError);
        }
        let compatible = capture.ingress().with_journal(|journal| {
            let config = journal.config();
            config
                .max_scope_bytes
                .checked_add(config.max_membership_bytes)
                .and_then(|bytes| bytes.checked_add(config.max_cut_bytes))
                .is_some_and(|bytes| bytes <= self.policy.max_metadata_bytes)
                && config.max_batch_bytes <= self.policy.max_batch_bytes
                && config.max_batch_events <= self.policy.max_batch_events
        });
        if !compatible {
            return Err(AdapterError);
        }
        match request {
            DonorRequest::Offer { max_metadata_bytes } => {
                if *max_metadata_bytes != self.policy.max_metadata_bytes {
                    return Err(AdapterError);
                }
                let offer = self
                    .state
                    .image_offer(capture, *max_metadata_bytes, admission)?;
                let variable_bytes = offer
                    .get()
                    .capture
                    .scope
                    .domain
                    .len()
                    .checked_add(offer.get().capture.scope.partition.len())
                    .and_then(|bytes| {
                        offer.get().members.iter().try_fold(bytes, |sum, member| {
                            sum.checked_add(member.node.as_str().len())
                        })
                    })
                    .and_then(|bytes| {
                        offer
                            .get()
                            .cuts
                            .iter()
                            .try_fold(bytes, |sum, cut| sum.checked_add(cut.writer.len()))
                    });
                if variable_bytes.is_none_or(|bytes| bytes > *max_metadata_bytes) {
                    return Err(AdapterError);
                }
                if !capture
                    .ingress()
                    .with_journal(|journal| offer.get().capture == *journal.id())
                {
                    return Err(AdapterError);
                }
                Ok(DonorReply::Offer(offer))
            }
            DonorRequest::Chunk {
                reservation,
                sequence,
                max_bytes,
            } => {
                // `acknowledged` materializes an owned cursor even though the
                // wire reply is only a chunk. Keep its allocation admitted
                // until that exact validation result is dropped.
                let cursor_charge = self.reply_charge(admission)?;
                let valid_reservation = capture.ingress().with_journal(|journal| {
                    journal.id() == &reservation.capture
                        && journal.acknowledged(now, reservation).is_ok()
                });
                drop(cursor_charge);
                if *max_bytes == 0 || *max_bytes > self.policy.max_chunk_bytes || !valid_reservation
                {
                    return Err(AdapterError);
                }
                let chunk = self
                    .state
                    .image_chunk(capture, *sequence, *max_bytes, admission)?;
                if chunk.get().is_empty() || chunk.get().len() > *max_bytes {
                    return Err(AdapterError);
                }
                Ok(DonorReply::Chunk(chunk))
            }
            DonorRequest::Reserve { follower, cut } => {
                let charge = self.reply_charge(admission)?;
                let id = capture
                    .ingress()
                    .with_journal(|journal| match journal.reserved_for(now, follower, cut)? {
                        Some(id) => Ok(id),
                        None => journal.reserve(now, follower.clone(), cut),
                    })
                    .map_err(|_| AdapterError)?;
                Ok(DonorReply::Reserved(charge.hold(id)))
            }
            DonorRequest::Attach { reservation } => {
                let charge = self.reply_charge(admission)?;
                let token = capture
                    .ingress()
                    .with_journal(|journal| {
                        let token = match journal.begin_attach(now, reservation) {
                            Ok(token) => token,
                            Err(JournalError::Stage) => journal.attachment_for(now, reservation)?,
                            Err(error) => return Err(error),
                        };
                        journal.confirm_attach(now, &token)?;
                        Ok::<_, JournalError>(token)
                    })
                    .map_err(|_| AdapterError)?;
                Ok(DonorReply::Attached(charge.hold(token)))
            }
            DonorRequest::Barrier {
                attachment,
                max_metadata_bytes,
            } => {
                if *max_metadata_bytes != self.policy.max_metadata_bytes {
                    return Err(AdapterError);
                }
                let charge = self.reply_charge(admission)?;
                let receipt = capture
                    .ingress()
                    .with_journal(|journal| {
                        let receipt = journal.barrier(now, &attachment.reservation)?;
                        if receipt.attach_operation != attachment.operation {
                            return Err(JournalError::Stale);
                        }
                        Ok(receipt)
                    })
                    .map_err(|_| AdapterError)?;
                Ok(DonorReply::Barrier(charge.hold(receipt)))
            }
            DonorRequest::AdvanceBarrier {
                expected,
                max_metadata_bytes,
            } => {
                if *max_metadata_bytes != self.policy.max_metadata_bytes {
                    return Err(AdapterError);
                }
                let charge = self.reply_charge(admission)?;
                let receipt = capture
                    .ingress()
                    .with_journal(|journal| {
                        journal.advance_barrier(now, &expected.reservation, expected)
                    })
                    .map_err(|_| AdapterError)?;
                Ok(DonorReply::Barrier(charge.hold(receipt)))
            }
            DonorRequest::Batch {
                barrier,
                max_bytes,
                max_events,
            } => {
                if *max_bytes != self.policy.max_batch_bytes
                    || *max_events != self.policy.max_batch_events
                {
                    return Err(AdapterError);
                }
                let charge = self.reply_charge(admission)?;
                let batch = capture
                    .ingress()
                    .with_journal(|journal| journal.read_batch(now, &barrier.reservation, barrier))
                    .map_err(|_| AdapterError)?
                    .ok_or(AdapterError)?;
                if batch.bytes > *max_bytes || batch.deltas.len() > *max_events {
                    return Err(AdapterError);
                }
                Ok(DonorReply::Batch(charge.hold(batch)))
            }
            DonorRequest::Ack {
                reservation,
                batch_operation,
                through,
            } => {
                let cursor_charge = self.reply_charge(admission)?;
                let confirmed = capture
                    .ingress()
                    .with_journal(|journal| {
                        journal.ack_batch(now, reservation, *batch_operation, through)
                    })
                    .map_err(|_| AdapterError)?;
                if confirmed != *through {
                    return Err(AdapterError);
                }
                drop(confirmed);
                drop(cursor_charge);
                Ok(DonorReply::Acked)
            }
            DonorRequest::Release { reservation } => {
                capture
                    .ingress()
                    .with_journal(|journal| journal.release(now, reservation))
                    .map_err(|_| AdapterError)?;
                Ok(DonorReply::Released)
            }
        }
    }

    fn reply_charge(
        &self,
        admission: &ByteAdmission,
    ) -> Result<crate::volatile_recovery::bootstrap::admission::Reservation, AdapterError> {
        let bytes = self.limits.decoded_charge().map_err(|_| AdapterError)?;
        admission
            .reserve(AdmissionClass::Inflight, bytes)
            .map_err(|_| AdapterError)
    }
}
