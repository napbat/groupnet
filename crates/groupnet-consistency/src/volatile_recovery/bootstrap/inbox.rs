//! Bounded incoming donor work owned by the existing recovery worker.

use std::sync::{Arc, Mutex};

use groupnet_core::volatile_bootstrap::ClaimIdentity;
use groupnet_transport::QueueCapacity;

use tokio::sync::{Notify, mpsc, oneshot};

use super::admission::Admitted;
use super::ports::{DonorReply, DonorRequest};
use crate::volatile_recovery::AdapterError;

/// Failed admission to the one worker's bounded donor inbox.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DonorInboxError {
    /// The bounded queue is full; the request was dropped with its charge.
    Full,
    /// The recovery worker ended and cannot serve this capture.
    Closed,
}

impl std::fmt::Display for DonorInboxError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Full => "donor inbox is full",
            Self::Closed => "donor inbox is closed",
        })
    }
}

impl std::error::Error for DonorInboxError {}

/// A completed or failed exact donor request.
pub type DonorResponse = oneshot::Receiver<Result<DonorReply, AdapterError>>;

/// Cloneable ingress handle to the one recovery worker.
#[derive(Clone, Debug)]
pub struct DonorSender {
    sender: mpsc::Sender<IncomingDonorRequest>,
    wake: Arc<Notify>,
    identity: Arc<Mutex<Option<ClaimIdentity>>>,
}

impl DonorSender {
    /// Current complete Ready claim identity published by the owning worker.
    /// A poisoned identity lock fails closed instead of accepting stale work.
    #[must_use]
    pub fn current_identity(&self) -> Option<ClaimIdentity> {
        self.identity.lock().ok()?.clone()
    }

    /// Queues a request that already owns its exact metadata charge.
    ///
    /// # Errors
    /// Returns Full or Closed without retaining the request or charge.
    pub fn try_submit(
        &self,
        request: Admitted<DonorRequest>,
    ) -> Result<DonorResponse, DonorInboxError> {
        let (reply, response) = oneshot::channel();
        let incoming = IncomingDonorRequest { request, reply };
        self.sender
            .try_send(incoming)
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => DonorInboxError::Full,
                mpsc::error::TrySendError::Closed(_) => DonorInboxError::Closed,
            })?;
        self.wake.notify_one();
        Ok(response)
    }
}

/// One request whose byte charge survives queueing and callback execution.
#[derive(Debug)]
pub struct IncomingDonorRequest {
    request: Admitted<DonorRequest>,
    reply: oneshot::Sender<Result<DonorReply, AdapterError>>,
}

impl IncomingDonorRequest {
    /// Borrows the exact request without cloning its variable metadata.
    #[must_use]
    pub fn request(&self) -> &DonorRequest {
        self.request.get()
    }

    /// Completes one request; the response retains its own memory charges
    /// until the receiver consumes or drops it.
    pub fn respond(self, result: Result<DonorReply, AdapterError>) {
        let _ = self.reply.send(result);
    }
}

/// Single-consumer bounded inbox; no adapter-owned request map or poll loop.
#[derive(Debug)]
pub struct DonorInbox {
    receiver: mpsc::Receiver<IncomingDonorRequest>,
    wake: Arc<Notify>,
    identity: Arc<Mutex<Option<ClaimIdentity>>>,
}

impl Drop for DonorInbox {
    fn drop(&mut self) {
        self.set_identity(None);
    }
}

impl DonorInbox {
    /// Opens one bounded queue and its cloneable ingress handle.
    #[must_use]
    pub fn new(capacity: QueueCapacity) -> (DonorSender, Self) {
        let (sender, receiver) = mpsc::channel(capacity.get());
        let wake = Arc::new(Notify::new());
        let identity = Arc::new(Mutex::new(None));
        (
            DonorSender {
                sender,
                wake: Arc::clone(&wake),
                identity: Arc::clone(&identity),
            },
            Self {
                receiver,
                wake,
                identity,
            },
        )
    }

    /// Publishes or withdraws the exact donor identity along with the worker's
    /// captured-image lifecycle. No network caller may mint this identity.
    pub fn set_identity(&self, identity: Option<ClaimIdentity>) {
        if let Ok(mut current) = self.identity.lock() {
            *current = identity;
        }
    }

    /// Receives one already-admitted request in the existing worker loop.
    pub async fn recv(&mut self) -> Option<IncomingDonorRequest> {
        self.receiver.recv().await
    }

    /// Removes one queued request without blocking the recovery worker.
    pub fn try_recv(&mut self) -> Option<IncomingDonorRequest> {
        self.receiver.try_recv().ok()
    }

    /// Wake signal shared with the recovery worker's existing scheduler.
    #[must_use]
    pub fn wake(&self) -> Arc<Notify> {
        Arc::clone(&self.wake)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::super::admission::{AdmissionClass, AdmissionLimits, ByteAdmission};
    use super::*;
    use crate::volatile_recovery::AdapterErrorKind;

    fn admitted_request(admission: &ByteAdmission) -> Admitted<DonorRequest> {
        admission
            .reserve(AdmissionClass::Inflight, NonZeroUsize::MIN)
            .unwrap()
            .hold(DonorRequest::Offer {
                max_metadata_bytes: NonZeroUsize::new(8).unwrap(),
            })
    }

    #[test]
    fn the_largest_capacity_tokio_accepts_opens() {
        let (_sender, _inbox) = DonorInbox::new(QueueCapacity::MAX);
    }

    #[tokio::test]
    async fn full_queue_and_dropped_receiver_release_exact_request_charge() {
        let admission = ByteAdmission::new(AdmissionLimits {
            max_total_bytes: std::num::NonZeroUsize::new(2).unwrap(),
            max_encoded_bytes: 0,
            max_decoded_bytes: 0,
            max_suffix_bytes: 0,
            max_native_overlap_bytes: 0,
            max_inflight_bytes: 2,
            max_reservations: std::num::NonZeroUsize::new(2).unwrap(),
        })
        .unwrap();
        let (sender, mut inbox) = DonorInbox::new(QueueCapacity::MIN);
        let first = sender.try_submit(admitted_request(&admission)).unwrap();
        assert_eq!(
            sender.try_submit(admitted_request(&admission)).unwrap_err(),
            DonorInboxError::Full
        );
        assert_eq!(admission.usage(), (1, [0, 0, 0, 0, 1], 1));
        let incoming = inbox.recv().await.unwrap();
        assert!(matches!(incoming.request(), DonorRequest::Offer { .. }));
        incoming.respond(Err(AdapterErrorKind::Fenced.into()));
        assert_eq!(
            first.await.unwrap().unwrap_err().kind(),
            AdapterErrorKind::Fenced
        );
        assert_eq!(admission.usage(), (0, [0; 5], 0));
    }
}
