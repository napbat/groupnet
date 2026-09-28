//! Optional source and private-stage capabilities for one recovery worker.
//!
//! Callbacks return one exact core event per requested effect. They never
//! schedule claim retries or extend the parent recovery episode themselves.

use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use groupnet_core::Time;
use groupnet_core::volatile_bootstrap::journal::{
    AttachToken, BarrierReceipt, DonorJournal, Invalidation, JournalBatch, JournalCursor,
    JournalError, JournalState, ReservationId,
};
use groupnet_core::volatile_bootstrap::transfer::{TransferEffect, TransferEvent, TransferOffer};
use groupnet_core::volatile_bootstrap::{
    BootstrapClaim, BootstrapMember, BootstrapOperation, ClaimIdentity,
};
use groupnet_core::volatile_recovery::RecoveryOperation;

use super::admission::{AdmissionClass, Admitted, ByteAdmission, Reservation};
use crate::volatile_recovery::{AdapterError, BoxRecoveryFuture, PublicationPermit};

/// Complete, source-observed roster and native TTL claims for one scope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimSnapshot {
    /// All currently visible members, bounded before callback allocation.
    pub members: Vec<BootstrapMember>,
    /// All currently visible claims, bounded before callback allocation.
    pub claims: Vec<BootstrapClaim>,
}

/// Caller-provided pre-allocation bounds for a complete native observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClaimObservationLimits {
    /// Maximum distinct members and claims.
    pub max_members: usize,
    /// Maximum bytes in one member identity.
    pub max_member_bytes: usize,
    /// Maximum combined encoded roster and claim metadata bytes.
    pub max_metadata_bytes: usize,
}

/// Fallible source access for finite, advisory builder claims.
///
/// The source must enforce member count, identity bytes, and combined
/// metadata bytes before it allocates a snapshot. An unreadable or malformed
/// record is an error, not absence. A TTL claim is never serving authority or
/// an origin write log.
pub trait ClaimSource: Send + Sync + 'static {
    /// Publish or renew one exact local claim under native TTL semantics.
    fn publish_claim(
        &self,
        claim: BootstrapClaim,
    ) -> BoxRecoveryFuture<'_, Result<(), AdapterError>>;

    /// Best-effort withdrawal of one exact local claim incarnation.
    fn withdraw_claim(
        &self,
        selected: ClaimIdentity,
    ) -> BoxRecoveryFuture<'_, Result<(), AdapterError>>;

    /// Read a complete bounded member and claim snapshot.
    fn observe_claims<'a>(
        &'a self,
        op: BootstrapOperation,
        limits: ClaimObservationLimits,
        admission: &'a ByteAdmission,
    ) -> BoxRecoveryFuture<'a, Result<Admitted<ClaimSnapshot>, AdapterError>>;

    /// Read one exact selected claim, including its native TTL and renewal.
    fn observe_selected_claim<'a>(
        &'a self,
        op: BootstrapOperation,
        selected: ClaimIdentity,
        limits: ClaimObservationLimits,
        admission: &'a ByteAdmission,
    ) -> BoxRecoveryFuture<'a, Result<Option<Admitted<BootstrapClaim>>, AdapterError>>;
}

/// Private stage owned by the recovery worker, with real memory charges.
///
/// The stage drops before its encoded and decoded reservations. A callback
/// may lower its actual charge only by retiring buffers first; conservative
/// over-reservation is safe. Nothing here can publish into the live index.
#[derive(Debug)]
pub struct StageResources<S> {
    stage: S,
    encoded: Reservation,
    decoded: Reservation,
}

impl<S> StageResources<S> {
    /// Binds a newly created private stage to pre-acquired memory permits.
    ///
    /// # Errors
    /// Rejects reservations charged to the wrong byte classes.
    pub fn new(stage: S, encoded: Reservation, decoded: Reservation) -> Result<Self, AdapterError> {
        if encoded.class() != AdmissionClass::Encoded || decoded.class() != AdmissionClass::Decoded
        {
            return Err(AdapterError);
        }
        Ok(Self {
            stage,
            encoded,
            decoded,
        })
    }

    /// Borrows the private stage for one bounded callback. The trusted port
    /// must not move its owned buffers out without moving their reservations.
    pub fn stage_mut(&mut self) -> &mut S {
        &mut self.stage
    }

    /// Moves a verified private stage into the application's separately
    /// budgeted live index inside the guarded publication callback. The
    /// private encoded/decoded reservations remain held until that callback
    /// returns; a rejected permit drops the private stage without publishing.
    pub fn install<R>(self, publish: impl FnOnce(S) -> R) -> R {
        let Self {
            stage,
            encoded,
            decoded,
        } = self;
        let result = publish(stage);
        drop(encoded);
        drop(decoded);
        result
    }

    /// Bytes conservatively reserved for image input and decoded state.
    #[must_use]
    pub fn reserved_bytes(&self) -> (usize, usize) {
        (self.encoded.bytes(), self.decoded.bytes())
    }
}

#[derive(Debug)]
struct JournalIngressInner {
    journal: Mutex<DonorJournal>,
    suffix: Reservation,
    changed: Arc<Notify>,
}

/// Shared synchronous journal ingress under the application's index
/// publication coordinator. A clone shares the one candidate and charge.
#[derive(Clone, Debug)]
pub struct JournalIngress(Arc<JournalIngressInner>);

impl JournalIngress {
    /// Exact local candidate identity for guarded unlink after replacement.
    #[must_use]
    pub fn same_candidate(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// Attaches an already active capture and its pre-acquired suffix budget.
    ///
    /// # Errors
    /// A partial capture cannot be exposed to live index publications.
    pub fn new(
        journal: DonorJournal,
        suffix: Reservation,
        changed: Arc<Notify>,
    ) -> Result<Self, AdapterError> {
        if journal.state() != JournalState::Active || suffix.class() != AdmissionClass::Suffix {
            return Err(AdapterError);
        }
        Ok(Self(Arc::new(JournalIngressInner {
            journal: Mutex::new(journal),
            suffix,
            changed,
        })))
    }

    /// Executes one synchronous journal decision. No lock guard can cross an
    /// await: the closure completes before this method returns. Publication
    /// takes the application coordinator before entering here, and source
    /// service takes this lock alone. B and native cuts are sampled from
    /// this journal together; service never reenters the index coordinator
    /// while holding the journal lock. An overflow invalidates the journal
    /// and wakes the worker to withdraw donor Ready.
    pub fn with_journal<R>(&self, apply: impl FnOnce(&mut DonorJournal) -> R) -> R {
        let mut journal = self
            .0
            .journal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let before = journal.state();
        let result = apply(&mut journal);
        let invalidated =
            before != JournalState::Invalidated && journal.state() == JournalState::Invalidated;
        drop(journal);
        if invalidated {
            self.0.changed.notify_one();
        }
        result
    }

    /// Wakes the existing worker after synchronous publication invalidates
    /// this capture; source withdrawal remains async work in that worker.
    pub async fn invalidated(&self) {
        self.0.changed.notified().await;
    }

    /// Exact charged suffix capacity retained as long as ingress is attached.
    #[must_use]
    pub fn reserved_suffix_bytes(&self) -> usize {
        self.0.suffix.bytes()
    }
}

/// One locally built, bounded donor candidate retained after local Ready.
///
/// The guarded origin builder produces this only after a complete scan,
/// private image capture at C, and journal attachment to all later index
/// publications. The image drops before its encoded and decoded permits;
/// shared ingress retains the suffix permit while any coordinator handle
/// remains attached. The journal continues expiring and serving bounded
/// followers under the same recovery worker after the local read gate opens.
#[derive(Debug)]
pub struct DonorCapture<I> {
    image: I,
    ingress: JournalIngress,
    encoded: Reservation,
    decoded: Reservation,
}

impl<I> DonorCapture<I> {
    /// Constructs only from a complete active private capture whose journal
    /// ingress is already attached to the live index publication coordinator.
    ///
    /// # Errors
    /// Returns an adapter error until image C and the suffix are ready.
    pub fn new(
        image: I,
        ingress: JournalIngress,
        encoded: Reservation,
        decoded: Reservation,
    ) -> Result<Self, AdapterError> {
        if ingress.with_journal(|journal| journal.state()) != JournalState::Active
            || encoded.class() != AdmissionClass::Encoded
            || decoded.class() != AdmissionClass::Decoded
        {
            ingress.with_journal(|journal| journal.invalidate(Invalidation::DonorLost));
            return Err(AdapterError);
        }
        Ok(Self {
            image,
            ingress,
            encoded,
            decoded,
        })
    }

    /// Shared synchronous ingress. The index publication adapter records
    /// every effect here before awaiting any network work.
    #[must_use]
    pub fn ingress(&self) -> &JournalIngress {
        &self.ingress
    }

    /// Earliest finite journal or follower reservation expiry.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Time> {
        self.ingress.with_journal(|journal| journal.next_deadline())
    }

    /// Advances finite capture and follower lifetimes in the same worker.
    ///
    /// # Errors
    /// Propagates journal expiry or source-continuity failure; an invalidated
    /// capture must withdraw Ready and reject new reservations.
    pub fn tick(&self, now: Time) -> Result<(), JournalError> {
        self.ingress.with_journal(|journal| journal.tick(now))
    }

    /// Whether this capture can still serve a follower candidate.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.ingress
            .with_journal(|journal| journal.state() == JournalState::Active)
    }

    /// Immutable private image access; never grants local serving.
    #[must_use]
    pub const fn image(&self) -> &I {
        &self.image
    }

    /// Sum of conservative encoded, decoded, and suffix byte reservations.
    #[must_use]
    pub fn reserved_bytes(&self) -> Option<usize> {
        self.encoded
            .bytes()
            .checked_add(self.decoded.bytes())?
            .checked_add(self.ingress.reserved_suffix_bytes())
    }
}

impl<I> Drop for DonorCapture<I> {
    fn drop(&mut self) {
        self.ingress
            .with_journal(|journal| journal.invalidate(Invalidation::DonorLost));
    }
}

/// One follower's worker-owned, finite transfer resources.
///
/// The adapter may not keep an unbounded map of hidden private stages or
/// returned batch clones. Cancellation fences the parent operation before
/// this owner is dropped. Donor reservations are released separately by
/// exact source effects even when private memory has already retired.
#[derive(Debug)]
pub struct TransferResources<S, A, N> {
    /// Optional private stage and its encoded/decoded memory.
    pub stage: Option<StageResources<S>>,
    /// Exact attached donor stream handle.
    pub attachment: Option<A>,
    /// At most one returned batch clone charged until fully processed.
    pub batch: Option<Admitted<JournalBatch>>,
    /// Native overlap bytes and their charge stay owned together.
    pub native_overlap: Option<Admitted<N>>,
}

/// Exact claim-selected peer transfer, retained independently of each child
/// operation token. Source callbacks may not infer the parent from a token.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransferContext {
    /// Original selected claim operation.
    pub parent: BootstrapOperation,
    /// Exact donor incarnation and attempt.
    pub donor: ClaimIdentity,
    /// Exact local follower incarnation and attempt.
    pub follower: ClaimIdentity,
}

/// Exact locally selected builder and its guarded capture budget.
#[derive(Clone, Debug)]
pub struct LocalCaptureRequest {
    /// Outer recovery episode that owns publication.
    pub recovery: RecoveryOperation,
    /// Child builder operation and deadline.
    pub build: BootstrapOperation,
    /// Exact locally selected claim.
    pub selected: ClaimIdentity,
    /// Publication permission narrowed to the child operation deadline.
    pub permit: PublicationPermit,
    /// Current logical time supplied by the worker.
    pub now: Time,
    /// Shared worker wake for synchronous journal invalidation.
    pub wake: Arc<Notify>,
}

impl<S, A, N> Default for TransferResources<S, A, N> {
    fn default() -> Self {
        Self {
            stage: None,
            attachment: None,
            batch: None,
            native_overlap: None,
        }
    }
}

/// One bounded incoming follower request against the active local capture.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DonorRequest {
    /// Read an offer, including complete metadata, before allocation limits.
    Offer {
        /// Maximum returned variable metadata bytes.
        max_metadata_bytes: usize,
    },
    /// Retain the exact suffix starting at C for this follower.
    Reserve {
        /// Fresh follower identity and attempt.
        follower: ClaimIdentity,
        /// Exact image cut C.
        cut: JournalCursor,
    },
    /// Fetch one encoded image chunk with a pre-allocation cap.
    Chunk {
        /// Exact follower reservation.
        reservation: ReservationId,
        /// Sequential chunk index.
        sequence: usize,
        /// Maximum returned encoded bytes.
        max_bytes: usize,
    },
    /// Attach a live donor stream before reading B.
    Attach {
        /// Exact follower reservation.
        reservation: ReservationId,
    },
    /// Sample atomic B, cuts, and membership after attachment.
    Barrier {
        /// Exact attached stream.
        attachment: AttachToken,
        /// Maximum returned complete barrier metadata bytes.
        max_metadata_bytes: usize,
    },
    /// Sample a later B only after the previous B was fully acknowledged.
    AdvanceBarrier {
        /// Exact previously acknowledged barrier.
        expected: BarrierReceipt,
        /// Maximum returned complete barrier metadata bytes.
        max_metadata_bytes: usize,
    },
    /// Read one contiguous bounded suffix batch through exact B.
    Batch {
        /// Exact previously sampled barrier.
        barrier: BarrierReceipt,
        /// Maximum returned bytes.
        max_bytes: usize,
        /// Maximum returned events.
        max_events: usize,
    },
    /// Confirm or read back one exact applied batch acknowledgment.
    Ack {
        /// Exact follower reservation.
        reservation: ReservationId,
        /// Exact donor batch operation.
        batch_operation: u64,
        /// Exact applied-through cursor.
        through: JournalCursor,
    },
    /// Retire one reservation and its attached stream.
    Release {
        /// Exact follower reservation.
        reservation: ReservationId,
    },
}

/// One response to the exact incoming request, with owned byte charges.
#[derive(Debug)]
pub enum DonorReply {
    /// Complete bounded image metadata.
    Offer(Admitted<TransferOffer>),
    /// Confirmed suffix reservation.
    Reserved(ReservationId),
    /// One encoded chunk; its in-flight charge follows the bytes.
    Chunk(Admitted<Vec<u8>>),
    /// Confirmed live-stream attachment.
    Attached(AttachToken),
    /// Atomic B and native writer cuts.
    Barrier(Admitted<BarrierReceipt>),
    /// One contiguous suffix batch and its in-flight charge.
    Batch(Admitted<JournalBatch>),
    /// Exact batch acknowledgment/readback accepted.
    Acked,
    /// Exact reservation release accepted.
    Released,
}

/// One-effect-at-a-time donor and application callbacks.
///
/// The existing recovery worker passes each effect under its original
/// absolute episode deadline. The port receives the exact worker-owned
/// resources and shared admission budget, reserves before allocating any
/// chunk, stage, suffix, native overlap, or returned batch, and leaves no
/// private stage in an adapter-owned map. It may return an event only for the
/// requested exact operation. Cleanup effects return None after resources
/// are retired. `InstallCandidate` must use the supplied publication permit
/// for one fenced swap and normal-feed handoff; a sampled head or boolean
/// cannot stand in for the typed Installed receipt.
pub trait DonorPort: Send + Sync + 'static {
    /// Encoded donor image and its bounded private metadata.
    type Image: Send + 'static;
    /// Consumer-defined private stage, never the live index.
    type Stage: Send + 'static;
    /// Source-owned attachment handle, retained by this worker.
    type Attachment: Send + 'static;
    /// Bounded native-overlap buffer owned through the install handoff.
    type NativeBuffer: Send + 'static;

    /// Runs one guarded origin scan and atomic capture at C. Local Ready
    /// claim publication waits for this complete active capture. The port
    /// retains a cancellation guard while this future runs: if the worker
    /// drops the future after an outer gap/deadline, any partly attached
    /// journal ingress is unlinked from the publication coordinator before
    /// its reserved memory can be reused.
    ///
    /// # Errors
    /// Returns an adapter error when a complete guarded capture cannot be
    /// established within the supplied finite deadline.
    fn build_local_capture<'a>(
        &'a self,
        request: LocalCaptureRequest,
        admission: &'a ByteAdmission,
    ) -> BoxRecoveryFuture<'a, Result<DonorCapture<Self::Image>, AdapterError>>;

    /// Synchronously remove this exact capture's journal ingress from the
    /// application's publication coordinator before dropping its image and
    /// charge. This callback runs outside the recovery gate lock; it may
    /// briefly take coordinator then journal locks, never across an await.
    fn retire_local_capture(&self, capture: &DonorCapture<Self::Image>);

    /// Prepare one bounded incoming follower response synchronously. The
    /// callback may briefly enter `JournalIngress` and read the immutable
    /// image, but cannot hold the journal lock across network send/await.
    /// The worker still owns claim renewal and journal expiry after Ready.
    ///
    /// # Errors
    /// Rejects stale, malformed, or over-budget requests without publishing
    /// an uncharged response.
    fn prepare_follower(
        &self,
        request: &DonorRequest,
        capture: &DonorCapture<Self::Image>,
        admission: &ByteAdmission,
    ) -> Result<DonorReply, AdapterError>;

    /// Execute one bounded transfer effect without a second scheduler.
    fn execute<'a>(
        &'a self,
        context: &'a TransferContext,
        effect: TransferEffect,
        resources: &'a mut TransferResources<Self::Stage, Self::Attachment, Self::NativeBuffer>,
        admission: &'a ByteAdmission,
        permit: Option<PublicationPermit>,
    ) -> BoxRecoveryFuture<'a, Result<Option<Admitted<TransferEvent>>, AdapterError>>;

    /// Immediately retire an exact attachment after source-side cancellation.
    fn detach<'a>(
        &'a self,
        token: AttachToken,
        resources: &'a mut TransferResources<Self::Stage, Self::Attachment, Self::NativeBuffer>,
    ) -> BoxRecoveryFuture<'a, Result<(), AdapterError>>;
}

/// Shared, opt-in capabilities. Cloning this handle shares the same memory
/// budget and source; it does not create an independent fleet worker.
#[derive(Debug)]
pub struct BootstrapCapabilities<C: ClaimSource, D: DonorPort> {
    /// Native advisory claim source.
    pub claims: Arc<C>,
    /// Donor transfer and private-stage adapter.
    pub donor: Arc<D>,
    /// Aggregate memory and resource admission across all scopes.
    pub admission: ByteAdmission,
}
