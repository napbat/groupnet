//! One-effect bulk transport adapter for the existing bootstrap worker.
//!
//! Network phase selection and exact correlation stay here. The consumer
//! implements only private index, native-feed, and guarded publication work.

use std::sync::Arc;
use std::time::Instant;

use groupnet_core::Time;
use groupnet_core::volatile_bootstrap::BootstrapScope;
use groupnet_core::volatile_bootstrap::journal::{AttachToken, JournalBatch};
use groupnet_core::volatile_bootstrap::transfer::{
    TransferConfig, TransferConfigError, TransferEffect, TransferEvent,
};
use groupnet_transport::bulk::{BulkTransport, DataPlane};

use super::admission::{AdmissionClass, Admitted, ByteAdmission, Reservation};
use super::bulk_wire::{BootstrapBulkClient, BulkLimits, BulkLimitsError, Correlation};
use super::ports::{
    DonorCapture, DonorPort, DonorReply, DonorRequest, LocalCaptureOutcome, LocalCaptureRequest,
    ReadyCaptureRequest, TransferContext, TransferResources,
};
use crate::volatile_recovery::{AdapterError, AdapterErrorKind, PublicationPermit};

/// Application-only behavior required for a peer bootstrap. It never chooses
/// a donor request, allocates a transfer operation, or schedules a retry.
/// The asynchronous methods return `Send` futures; implement them as
/// `async fn` or with an `async move` body.
pub trait BootstrapStatePort: Send + Sync + 'static {
    /// Private encoded image retained by the local donor.
    type Image: Send + 'static;
    /// Volatile follower stage, never the published index.
    type Stage: Send + 'static;
    /// Bounded native effects awaiting the atomic handoff.
    type NativeBuffer: Send + 'static;

    /// Complete one origin scan, optionally retaining a guarded donor capture.
    fn build_local_capture(
        &self,
        request: LocalCaptureRequest,
        admission: &ByteAdmission,
    ) -> impl Future<Output = Result<LocalCaptureOutcome<Self::Image>, AdapterError>> + Send;

    /// Recapture an already-Ready complete application index through the
    /// fresh recovery-generation guard, without another origin scan. The
    /// default port fails with [`AdapterErrorKind::Unsupported`].
    fn recapture_current_index(
        &self,
        request: ReadyCaptureRequest,
        admission: &ByteAdmission,
    ) -> impl Future<Output = Result<DonorCapture<Self::Image>, AdapterError>> + Send {
        let _ = (request, admission);
        async { Err(AdapterErrorKind::Unsupported.into()) }
    }

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
    fn execute_local(
        &self,
        context: &TransferContext,
        effect: TransferEffect,
        resources: &mut TransferResources<Self::Stage, AttachToken, Self::NativeBuffer>,
        admission: &ByteAdmission,
        permit: Option<PublicationPermit>,
        deadline: Instant,
    ) -> impl Future<Output = Result<Option<Admitted<TransferEvent>>, AdapterError>> + Send;

    /// Privately verify and store one admitted encoded image chunk. The
    /// charge stays live through this callback; no decoded buffer escapes it.
    fn store_chunk(
        &self,
        sequence: usize,
        chunk: &Admitted<Vec<u8>>,
        resources: &mut TransferResources<Self::Stage, AttachToken, Self::NativeBuffer>,
    ) -> impl Future<Output = Result<usize, AdapterError>> + Send;

    /// Apply one admitted donor batch to the private stage. No source ack is
    /// sent until this callback succeeds and the core accepts `BatchStaged`.
    fn stage_batch(
        &self,
        batch: &Admitted<JournalBatch>,
        resources: &mut TransferResources<Self::Stage, AttachToken, Self::NativeBuffer>,
    ) -> impl Future<Output = Result<(), AdapterError>> + Send;

    /// Retire the exact logical attachment after source cancellation.
    fn detach(
        &self,
        token: AttachToken,
        resources: &mut TransferResources<Self::Stage, AttachToken, Self::NativeBuffer>,
    ) -> impl Future<Output = Result<(), AdapterError>> + Send;
}

/// A [`BulkDonorPort`] configuration rule that [`BulkDonorPort::new`] rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BulkDonorPortError {
    /// `scope.domain` or `scope.partition` is empty.
    EmptyScope,
    /// `policy` failed [`TransferConfig::validate`]; the
    /// [`TransferConfigError`] is the [`source`](std::error::Error::source).
    Policy(TransferConfigError),
    /// `scope` exceeds `limits.wire.max_scope_bytes` or
    /// `limits.phase.max_scope_bytes`.
    ScopeTooLong,
    /// `policy.max_metadata_bytes` exceeds `limits.phase.max_body_bytes`.
    MetadataExceedsBody,
    /// `policy.max_chunk_bytes` exceeds `limits.phase.max_body_bytes`.
    ChunkExceedsBody,
    /// `policy.max_batch_bytes` exceeds `limits.phase.max_body_bytes`.
    BatchBytesExceedBody,
    /// `policy.max_batch_events` exceeds `limits.phase.max_events`.
    BatchEventsExceedPhase,
    /// `policy.max_members` exceeds `limits.phase.max_members`.
    MembersExceedPhase,
    /// `policy.max_cuts` exceeds `limits.phase.max_cuts`.
    CutsExceedPhase,
    /// `limits` failed [`BulkLimits::validate`].
    Limits(BulkLimitsError),
}

impl std::fmt::Display for BulkDonorPortError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyScope => formatter.write_str("bootstrap scope domain or partition is empty"),
            Self::Policy(_) => formatter.write_str("invalid transfer policy"),
            Self::ScopeTooLong => {
                formatter.write_str("bootstrap scope exceeds the bulk scope limit")
            }
            Self::MetadataExceedsBody => {
                formatter.write_str("policy.max_metadata_bytes exceeds limits.phase.max_body_bytes")
            }
            Self::ChunkExceedsBody => {
                formatter.write_str("policy.max_chunk_bytes exceeds limits.phase.max_body_bytes")
            }
            Self::BatchBytesExceedBody => {
                formatter.write_str("policy.max_batch_bytes exceeds limits.phase.max_body_bytes")
            }
            Self::BatchEventsExceedPhase => {
                formatter.write_str("policy.max_batch_events exceeds limits.phase.max_events")
            }
            Self::MembersExceedPhase => {
                formatter.write_str("policy.max_members exceeds limits.phase.max_members")
            }
            Self::CutsExceedPhase => {
                formatter.write_str("policy.max_cuts exceeds limits.phase.max_cuts")
            }
            Self::Limits(_) => formatter.write_str("invalid bulk limits"),
        }
    }
}

impl std::error::Error for BulkDonorPortError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Policy(error) => Some(error),
            Self::Limits(error) => Some(error),
            Self::EmptyScope
            | Self::ScopeTooLong
            | Self::MetadataExceedsBody
            | Self::ChunkExceedsBody
            | Self::BatchBytesExceedBody
            | Self::BatchEventsExceedPhase
            | Self::MembersExceedPhase
            | Self::CutsExceedPhase => None,
        }
    }
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
    /// Names the first scope, policy, or transport-limit rule violated.
    pub fn new(
        state: Arc<A>,
        plane: DataPlane<B>,
        admission: ByteAdmission,
        scope: BootstrapScope,
        policy: TransferConfig,
        limits: BulkLimits,
    ) -> Result<Self, BulkDonorPortError> {
        if scope.domain.is_empty() || scope.partition.is_empty() {
            return Err(BulkDonorPortError::EmptyScope);
        }
        policy.validate().map_err(BulkDonorPortError::Policy)?;
        if scope
            .domain
            .len()
            .checked_add(scope.partition.len())
            .is_none_or(|bytes| {
                bytes > limits.wire.max_scope_bytes || bytes > limits.phase.max_scope_bytes
            })
        {
            return Err(BulkDonorPortError::ScopeTooLong);
        }
        let body = limits.phase.max_body_bytes;
        let rules = [
            (
                policy.max_metadata_bytes.get() > body,
                BulkDonorPortError::MetadataExceedsBody,
            ),
            (
                policy.max_chunk_bytes.get() > body,
                BulkDonorPortError::ChunkExceedsBody,
            ),
            (
                policy.max_batch_bytes.get() > body,
                BulkDonorPortError::BatchBytesExceedBody,
            ),
            (
                policy.max_batch_events.get() > limits.phase.max_events,
                BulkDonorPortError::BatchEventsExceedPhase,
            ),
            (
                policy.max_members.get() > limits.phase.max_members,
                BulkDonorPortError::MembersExceedPhase,
            ),
            (
                policy.max_cuts.get() > limits.phase.max_cuts,
                BulkDonorPortError::CutsExceedPhase,
            ),
        ];
        if let Some((_, error)) = rules.into_iter().find(|(violated, _)| *violated) {
            return Err(error);
        }
        let client = BootstrapBulkClient::new(plane, admission.clone(), limits)
            .map_err(BulkDonorPortError::Limits)?;
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
            .decoded_charge()?
            .checked_add(self.limits.correlation_charge()?)
            .ok_or(AdapterErrorKind::Capacity)?;
        Ok(self.admission.reserve(AdmissionClass::Inflight, bytes)?)
    }

    fn retained_charge(&self) -> Result<Reservation, AdapterError> {
        let bytes = self
            .limits
            .decoded_charge()?
            .checked_mul(2)
            .ok_or(AdapterErrorKind::Capacity)?;
        Ok(self.admission.reserve(AdmissionClass::Inflight, bytes)?)
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

    fn build_local_capture(
        &self,
        request: LocalCaptureRequest,
        admission: &ByteAdmission,
    ) -> impl Future<Output = Result<LocalCaptureOutcome<Self::Image>, AdapterError>> + Send {
        self.state.build_local_capture(request, admission)
    }

    fn recapture_current_index(
        &self,
        request: ReadyCaptureRequest,
        admission: &ByteAdmission,
    ) -> impl Future<Output = Result<DonorCapture<Self::Image>, AdapterError>> + Send {
        self.state.recapture_current_index(request, admission)
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

    async fn execute(
        &self,
        context: &TransferContext,
        effect: TransferEffect,
        resources: &mut TransferResources<Self::Stage, Self::Attachment, Self::NativeBuffer>,
        admission: &ByteAdmission,
        permit: Option<PublicationPermit>,
        deadline: Instant,
    ) -> Result<Option<Admitted<TransferEvent>>, AdapterError> {
        if !self.admission.same_pool(admission) || !self.client.uses_admission(admission) {
            return Err(AdapterErrorKind::InvalidInput.into());
        }
        self.execute_one(context, effect, resources, admission, permit, deadline)
            .await
    }

    fn detach(
        &self,
        token: AttachToken,
        resources: &mut TransferResources<Self::Stage, Self::Attachment, Self::NativeBuffer>,
    ) -> impl Future<Output = Result<(), AdapterError>> + Send {
        self.state.detach(token, resources)
    }
}

mod donor;
mod mapping;
