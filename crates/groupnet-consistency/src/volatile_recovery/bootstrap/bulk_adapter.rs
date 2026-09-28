//! One-effect bulk transport adapter for the existing bootstrap worker.
//!
//! Network phase selection and exact correlation stay here. The consumer
//! implements only private index, native-feed, and guarded publication work.

use std::sync::Arc;
use std::time::Instant;

use groupnet_core::Time;
use groupnet_core::volatile_bootstrap::BootstrapScope;
use groupnet_core::volatile_bootstrap::journal::{AttachToken, JournalBatch};
use groupnet_core::volatile_bootstrap::transfer::{TransferConfig, TransferEffect, TransferEvent};
use groupnet_transport::bulk::{BulkTransport, DataPlane};

use super::admission::{AdmissionClass, Admitted, ByteAdmission, Reservation};
use super::bulk_wire::{BootstrapBulkClient, BulkLimits, Correlation};
use super::ports::{
    DonorCapture, DonorPort, DonorReply, DonorRequest, LocalCaptureRequest, TransferContext,
    TransferResources,
};
use crate::volatile_recovery::{AdapterError, BoxRecoveryFuture, PublicationPermit};

/// Application-only behavior required for a peer bootstrap. It never chooses
/// a donor request, allocates a transfer operation, or schedules a retry.
pub trait BootstrapStatePort: Send + Sync + 'static {
    /// Private encoded image retained by the local donor.
    type Image: Send + 'static;
    /// Volatile follower stage, never the published index.
    type Stage: Send + 'static;
    /// Bounded native effects awaiting the atomic handoff.
    type NativeBuffer: Send + 'static;

    /// Build one guarded complete donor capture at the image cut.
    fn build_local_capture<'a>(
        &'a self,
        request: LocalCaptureRequest,
        admission: &'a ByteAdmission,
    ) -> BoxRecoveryFuture<'a, Result<DonorCapture<Self::Image>, AdapterError>>;

    /// Unlink this exact capture before its charged image is retired.
    fn retire_local_capture(&self, capture: &DonorCapture<Self::Image>);

    /// Encode one complete captured image offer under prior admission.
    ///
    /// # Errors
    /// Rejects an unavailable, incomplete, or over-budget image.
    fn image_offer(
        &self,
        capture: &DonorCapture<Self::Image>,
        max_metadata_bytes: usize,
        admission: &ByteAdmission,
    ) -> Result<Admitted<groupnet_core::volatile_bootstrap::transfer::TransferOffer>, AdapterError>;

    /// Return one bounded encoded chunk from the immutable captured image.
    ///
    /// # Errors
    /// Rejects a missing, corrupt, or over-budget chunk.
    fn image_chunk(
        &self,
        capture: &DonorCapture<Self::Image>,
        sequence: usize,
        max_bytes: usize,
        admission: &ByteAdmission,
    ) -> Result<Admitted<Vec<u8>>, AdapterError>;

    /// Execute only `ReserveStage`, `VerifyImage`, `CheckNativeCoverage`,
    /// `InstallCandidate`, or `DiscardStage` under the worker's deadline.
    fn execute_local<'a>(
        &'a self,
        context: &'a TransferContext,
        effect: TransferEffect,
        resources: &'a mut TransferResources<Self::Stage, AttachToken, Self::NativeBuffer>,
        admission: &'a ByteAdmission,
        permit: Option<PublicationPermit>,
        deadline: Instant,
    ) -> BoxRecoveryFuture<'a, Result<Option<Admitted<TransferEvent>>, AdapterError>>;

    /// Privately verify and store one admitted encoded image chunk. The
    /// charge stays live through this callback; no decoded buffer escapes it.
    fn store_chunk<'a>(
        &'a self,
        sequence: usize,
        chunk: &'a Admitted<Vec<u8>>,
        resources: &'a mut TransferResources<Self::Stage, AttachToken, Self::NativeBuffer>,
    ) -> BoxRecoveryFuture<'a, Result<usize, AdapterError>>;

    /// Apply one admitted donor batch to the private stage. No source ack is
    /// sent until this callback succeeds and the core accepts `BatchStaged`.
    fn stage_batch<'a>(
        &'a self,
        batch: &'a Admitted<JournalBatch>,
        resources: &'a mut TransferResources<Self::Stage, AttachToken, Self::NativeBuffer>,
    ) -> BoxRecoveryFuture<'a, Result<(), AdapterError>>;

    /// Retire the exact logical attachment after source cancellation.
    fn detach<'a>(
        &'a self,
        token: AttachToken,
        resources: &'a mut TransferResources<Self::Stage, AttachToken, Self::NativeBuffer>,
    ) -> BoxRecoveryFuture<'a, Result<(), AdapterError>>;
}

/// Reusable mapping from core transfer effects to bounded bulk exchanges.
#[derive(Debug)]
pub struct BulkDonorPort<A: BootstrapStatePort, B: BulkTransport> {
    state: Arc<A>,
    client: BootstrapBulkClient<B>,
    scope: BootstrapScope,
    policy: TransferConfig,
    admission: ByteAdmission,
    limits: BulkLimits,
}

impl<A: BootstrapStatePort, B: BulkTransport> BulkDonorPort<A, B> {
    /// Binds the client to the worker's one shared admission pool.
    ///
    /// # Errors
    /// Rejects invalid scope, policy, or transport limits.
    pub fn new(
        state: Arc<A>,
        plane: DataPlane<B>,
        admission: ByteAdmission,
        scope: BootstrapScope,
        policy: TransferConfig,
        limits: BulkLimits,
    ) -> Result<Self, AdapterError> {
        if scope.domain.is_empty() || scope.partition.is_empty() {
            return Err(AdapterError);
        }
        let policy = policy.validate().map_err(|_| AdapterError)?;
        if scope
            .domain
            .len()
            .checked_add(scope.partition.len())
            .is_none_or(|bytes| {
                bytes > limits.wire.max_scope_bytes || bytes > limits.phase.max_scope_bytes
            })
            || policy.max_metadata_bytes > limits.phase.max_body_bytes
            || policy.max_chunk_bytes > limits.phase.max_body_bytes
            || policy.max_batch_bytes > limits.phase.max_body_bytes
            || policy.max_batch_events > limits.phase.max_events
            || policy.max_members > limits.phase.max_members
            || policy.max_cuts > limits.phase.max_cuts
        {
            return Err(AdapterError);
        }
        let client =
            BootstrapBulkClient::new(plane, admission.clone(), limits).map_err(|_| AdapterError)?;
        Ok(Self {
            state,
            client,
            scope,
            policy,
            admission,
            limits,
        })
    }

    fn outbound_charge(&self) -> Result<Reservation, AdapterError> {
        // The request and exact Correlation coexist through the network
        // await. Neither can borrow the other's decoded/response reservation.
        let bytes = self
            .limits
            .decoded_charge()
            .map_err(|_| AdapterError)?
            .checked_add(self.limits.correlation_charge().map_err(|_| AdapterError)?)
            .ok_or(AdapterError)?;
        self.admission
            .reserve(AdmissionClass::Inflight, bytes)
            .map_err(|_| AdapterError)
    }

    fn retained_charge(&self) -> Result<Reservation, AdapterError> {
        let bytes = self
            .limits
            .decoded_charge()
            .map_err(|_| AdapterError)?
            .checked_mul(2)
            .ok_or(AdapterError)?;
        self.admission
            .reserve(AdmissionClass::Inflight, bytes)
            .map_err(|_| AdapterError)
    }

    fn correlation(
        &self,
        context: &TransferContext,
        child: groupnet_core::volatile_bootstrap::BootstrapOperation,
    ) -> Correlation {
        Correlation {
            scope: self.scope.clone(),
            donor: context.donor.clone(),
            follower: context.follower.clone(),
            parent: context.parent,
            child,
        }
    }
}

impl<A: BootstrapStatePort, B: BulkTransport> DonorPort for BulkDonorPort<A, B> {
    type Image = A::Image;
    type Stage = A::Stage;
    type Attachment = AttachToken;
    type NativeBuffer = A::NativeBuffer;

    fn build_local_capture<'a>(
        &'a self,
        request: LocalCaptureRequest,
        admission: &'a ByteAdmission,
    ) -> BoxRecoveryFuture<'a, Result<DonorCapture<Self::Image>, AdapterError>> {
        self.state.build_local_capture(request, admission)
    }

    fn retire_local_capture(&self, capture: &DonorCapture<Self::Image>) {
        self.state.retire_local_capture(capture);
    }

    fn prepare_follower(
        &self,
        request: &DonorRequest,
        capture: &DonorCapture<Self::Image>,
        now: Time,
        admission: &ByteAdmission,
    ) -> Result<DonorReply, AdapterError> {
        self.prepare_donor_request(request, capture, now, admission)
    }

    fn execute<'a>(
        &'a self,
        context: &'a TransferContext,
        effect: TransferEffect,
        resources: &'a mut TransferResources<Self::Stage, Self::Attachment, Self::NativeBuffer>,
        admission: &'a ByteAdmission,
        permit: Option<PublicationPermit>,
        deadline: Instant,
    ) -> BoxRecoveryFuture<'a, Result<Option<Admitted<TransferEvent>>, AdapterError>> {
        Box::pin(async move {
            if !self.admission.same_pool(admission) || !self.client.uses_admission(admission) {
                return Err(AdapterError);
            }
            self.execute_one(context, effect, resources, admission, permit, deadline)
                .await
        })
    }

    fn detach<'a>(
        &'a self,
        token: AttachToken,
        resources: &'a mut TransferResources<Self::Stage, Self::Attachment, Self::NativeBuffer>,
    ) -> BoxRecoveryFuture<'a, Result<(), AdapterError>> {
        self.state.detach(token, resources)
    }
}

mod donor;
mod mapping;
