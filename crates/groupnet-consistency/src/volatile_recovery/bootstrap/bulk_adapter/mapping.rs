//! Exact network phase mapping; all retry and deadline decisions stay in core.

use std::time::Instant;

use groupnet_core::volatile_bootstrap::transfer::{TransferEffect, TransferEvent};
use groupnet_transport::bulk::BulkTransport;

use super::{BootstrapStatePort, BulkDonorPort};
use crate::volatile_recovery::bootstrap::admission::{Admitted, ByteAdmission};
use crate::volatile_recovery::bootstrap::bulk_wire::WireReply;
use crate::volatile_recovery::bootstrap::ports::{
    DonorRequest, TransferContext, TransferResources,
};
use crate::volatile_recovery::{AdapterError, PublicationPermit};

impl<A: BootstrapStatePort, B: BulkTransport> BulkDonorPort<A, B> {
    #[expect(
        clippy::too_many_lines,
        reason = "one serial effect dispatch preserves current child correlation"
    )]
    pub(super) async fn execute_one(
        &self,
        context: &TransferContext,
        effect: TransferEffect,
        resources: &mut TransferResources<
            A::Stage,
            groupnet_core::volatile_bootstrap::journal::AttachToken,
            A::NativeBuffer,
        >,
        admission: &ByteAdmission,
        permit: Option<PublicationPermit>,
        deadline: Instant,
    ) -> Result<Option<Admitted<TransferEvent>>, AdapterError> {
        if Instant::now() >= deadline {
            return Err(AdapterError);
        }
        // Source request and correlation copies are allocated only after this
        // separate charge, which lives across the entire network await.
        let _outbound_charge = if matches!(
            &effect,
            TransferEffect::FetchOffer { .. }
                | TransferEffect::ReserveDonor { .. }
                | TransferEffect::FetchChunk { .. }
                | TransferEffect::AttachStream { .. }
                | TransferEffect::FetchBarrier { .. }
                | TransferEffect::AdvanceBarrier { .. }
                | TransferEffect::FetchBatch { .. }
                | TransferEffect::AckBatch { .. }
                | TransferEffect::ReleaseReservation(_)
        ) {
            Some(self.outbound_charge()?)
        } else {
            None
        };
        match effect {
            TransferEffect::FetchOffer {
                op,
                max_metadata_bytes,
            } => {
                let request = DonorRequest::Offer { max_metadata_bytes };
                let reply = self.request(context, op, &request, deadline).await?;
                let event = reply.try_map(|reply| match reply {
                    WireReply::Offer(offer) => Ok(TransferEvent::Offered { op, offer }),
                    _ => Err(AdapterError),
                })?;
                Ok(Some(event))
            }
            TransferEffect::ReserveDonor { op, capture, cut } => {
                if capture != cut.capture {
                    return Err(AdapterError);
                }
                let request = DonorRequest::Reserve {
                    follower: context.follower.clone(),
                    cut,
                };
                let reply = self.request(context, op, &request, deadline).await?;
                let retained = self.retained_charge()?;
                let event = reply.try_map(|reply| match reply {
                    WireReply::Reserved(reservation)
                        if reservation.capture == capture
                            && reservation.follower == context.follower =>
                    {
                        resources.metadata_charge = Some(retained);
                        resources.reservation = Some(reservation.clone());
                        Ok(TransferEvent::DonorReserved { op, reservation })
                    }
                    _ => Err(AdapterError),
                })?;
                Ok(Some(event))
            }
            TransferEffect::FetchChunk {
                op,
                sequence,
                max_bytes,
            } => {
                let reservation = resources.reservation.clone().ok_or(AdapterError)?;
                let request = DonorRequest::Chunk {
                    reservation,
                    sequence,
                    max_bytes,
                };
                let reply = self.request(context, op, &request, deadline).await?;
                let chunk = reply.try_map(|reply| match reply {
                    WireReply::Chunk(chunk) if !chunk.is_empty() && chunk.len() <= max_bytes => {
                        Ok(chunk)
                    }
                    _ => Err(AdapterError),
                })?;
                let bytes = chunk.get().len();
                let decoded_charge = self.state.store_chunk(sequence, &chunk, resources).await?;
                Ok(Some(chunk.map(|_| TransferEvent::ChunkStored {
                    op,
                    sequence,
                    bytes,
                    decoded_charge,
                })))
            }
            TransferEffect::AttachStream { op, reservation } => {
                if resources.reservation.as_ref() != Some(&reservation) {
                    return Err(AdapterError);
                }
                let request = DonorRequest::Attach {
                    reservation: reservation.clone(),
                };
                let reply = self.request(context, op, &request, deadline).await?;
                let event = reply.try_map(|reply| match reply {
                    WireReply::Attached(token) if token.reservation == reservation => {
                        resources.attachment = Some(token.clone());
                        Ok(TransferEvent::StreamAttached { op, token })
                    }
                    _ => Err(AdapterError),
                })?;
                Ok(Some(event))
            }
            TransferEffect::FetchBarrier { op, reservation } => {
                let attachment = resources.attachment.clone().ok_or(AdapterError)?;
                if attachment.reservation != reservation {
                    return Err(AdapterError);
                }
                let request = DonorRequest::Barrier {
                    attachment,
                    max_metadata_bytes: self.policy.max_metadata_bytes,
                };
                self.barrier(context, op, request, deadline).await
            }
            TransferEffect::AdvanceBarrier { op, expected } => {
                let request = DonorRequest::AdvanceBarrier {
                    expected,
                    max_metadata_bytes: self.policy.max_metadata_bytes,
                };
                self.barrier(context, op, request, deadline).await
            }
            TransferEffect::FetchBatch { op, receipt } => {
                if resources
                    .attachment
                    .as_ref()
                    .is_none_or(|attachment| attachment.reservation != receipt.reservation)
                {
                    return Err(AdapterError);
                }
                let request = DonorRequest::Batch {
                    barrier: receipt,
                    max_bytes: self.policy.max_batch_bytes,
                    max_events: self.policy.max_batch_events,
                };
                let reply = self.request(context, op, &request, deadline).await?;
                let batch = reply.try_map(|reply| match reply {
                    WireReply::Batch(batch) => Ok(batch),
                    _ => Err(AdapterError),
                })?;
                self.state.stage_batch(&batch, resources).await?;
                Ok(Some(
                    batch.map(|batch| TransferEvent::BatchStaged { op, batch }),
                ))
            }
            TransferEffect::AckBatch {
                op,
                reservation,
                batch_operation,
                through,
            } => {
                let request = DonorRequest::Ack {
                    reservation,
                    batch_operation,
                    through: through.clone(),
                };
                let reply = self.request(context, op, &request, deadline).await?;
                Ok(Some(reply.try_map(|reply| match reply {
                    WireReply::Acked => Ok(TransferEvent::BatchAcknowledged { op, through }),
                    _ => Err(AdapterError),
                })?))
            }
            TransferEffect::ReleaseReservation(reservation) => {
                let request = DonorRequest::Release {
                    reservation: reservation.clone(),
                };
                let reply = self
                    .request(context, context.parent, &request, deadline)
                    .await?;
                if !matches!(reply.get(), WireReply::Released) {
                    return Err(AdapterError);
                }
                if resources.reservation.as_ref() == Some(&reservation) {
                    resources.attachment = None;
                    resources.reservation = None;
                    resources.metadata_charge = None;
                }
                Ok(None)
            }
            local @ (TransferEffect::ReserveStage { .. }
            | TransferEffect::VerifyImage { .. }
            | TransferEffect::CheckNativeCoverage { .. }
            | TransferEffect::InstallCandidate { .. }
            | TransferEffect::DiscardStage { .. }) => {
                self.state
                    .execute_local(context, local, resources, admission, permit, deadline)
                    .await
            }
            TransferEffect::ArmTimer(_) => Ok(None),
        }
    }

    async fn request(
        &self,
        context: &TransferContext,
        op: groupnet_core::volatile_bootstrap::BootstrapOperation,
        request: &DonorRequest,
        deadline: Instant,
    ) -> Result<Admitted<WireReply>, AdapterError> {
        self.client
            .request(self.correlation(context, op), request, deadline)
            .await
            .map_err(|_| AdapterError)
    }

    async fn barrier(
        &self,
        context: &TransferContext,
        op: groupnet_core::volatile_bootstrap::BootstrapOperation,
        request: DonorRequest,
        deadline: Instant,
    ) -> Result<Option<Admitted<TransferEvent>>, AdapterError> {
        let reply = self.request(context, op, &request, deadline).await?;
        Ok(Some(reply.try_map(|reply| match reply {
            WireReply::Barrier(receipt) => Ok(TransferEvent::BarrierReceived { op, receipt }),
            _ => Err(AdapterError),
        })?))
    }
}
