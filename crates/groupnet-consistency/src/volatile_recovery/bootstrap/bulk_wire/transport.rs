//! Bounded one-operation-per-stream donor client and worker-owned listener.

use std::future::Future;
use std::mem::size_of;
use std::time::{Duration, Instant};

use groupnet_core::NodeId;
use groupnet_core::volatile_bootstrap::ClaimIdentity;
use groupnet_core::volatile_bootstrap::journal::{JournalDelta, NativeCut};
use groupnet_transport::bulk::{BulkTransport, DataPlane, DataStream};
use tokio::sync::watch;

use super::{
    Correlation, Envelope, ExchangeKind, Message, PhaseLimits, Refusal, ReplyTracker, WireLimits,
    decode, decode_reply, decode_request, encode, encode_reply, encode_request,
};
use crate::volatile_recovery::bootstrap::admission::{AdmissionClass, Admitted, ByteAdmission};
use crate::volatile_recovery::bootstrap::inbox::{DonorInboxError, DonorSender};
use crate::volatile_recovery::bootstrap::ports::DonorRequest;

/// Finite network and decode limits for one transfer operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BulkLimits {
    /// Complete encoded frame and identity limits.
    pub wire: WireLimits,
    /// Typed request/reply collection limits.
    pub phase: PhaseLimits,
    /// Independent server-side maximum for one accepted request.
    pub server_request_ms: u64,
}

impl BulkLimits {
    fn decoded_charge(self) -> Result<usize, BulkError> {
        // The body cap covers variable identity/effect/string bytes. These
        // terms cover the owned enum/header and bounded Vec elements, whose
        // in-memory size need not equal their encoded representation. Scope
        // names are Strings whose headers are in the structs below; member
        // and capture NodeIds are Arc strings with two refcount words per
        // allocation. A batch has at most three embedded captures; twelve
        // fixed Arc headers conservatively cover these and follower IDs.
        let header = size_of::<DonorRequest>().max(size_of::<super::WireReply>());
        let arc_count = self
            .phase
            .max_members
            .checked_add(12)
            .ok_or(BulkError::Config)?;
        self.phase
            .max_body_bytes
            .checked_add(header)
            .and_then(|value| {
                value.checked_add(
                    self.phase
                        .max_members
                        .checked_mul(size_of::<ClaimIdentity>())?,
                )
            })
            .and_then(|value| {
                value.checked_add(self.phase.max_cuts.checked_mul(size_of::<NativeCut>())?)
            })
            .and_then(|value| {
                value.checked_add(
                    self.phase
                        .max_events
                        .checked_mul(size_of::<JournalDelta>())?,
                )
            })
            .and_then(|value| value.checked_add(arc_count.checked_mul(2 * size_of::<usize>())?))
            .ok_or(BulkError::Config)
    }

    fn envelope_charge(self) -> Result<usize, BulkError> {
        // Scope's two String headers are in Envelope. The encoded cap covers
        // variable bytes; four Arc-header slots conservatively cover its two
        // NodeIds and any refcounted identity copy.
        self.wire
            .max_frame_bytes
            .checked_add(size_of::<Envelope>())
            .and_then(|value| value.checked_add(size_of::<[usize; 8]>()))
            .ok_or(BulkError::Config)
    }

    fn tracker_charge(self) -> Result<usize, BulkError> {
        self.wire
            .max_frame_bytes
            .checked_add(size_of::<ReplyTracker>())
            .and_then(|value| value.checked_add(size_of::<[usize; 8]>()))
            .ok_or(BulkError::Config)
    }

    fn correlation_charge(self) -> Result<usize, BulkError> {
        self.wire
            .max_frame_bytes
            .checked_add(size_of::<Correlation>())
            .and_then(|value| value.checked_add(size_of::<[usize; 8]>()))
            .ok_or(BulkError::Config)
    }

    /// Rejects unusable or contradictory finite bounds.
    ///
    /// # Errors
    /// Returns `Config` for zero, excessive transport, or inverted caps.
    pub fn validate(self) -> Result<Self, BulkError> {
        if self.wire.max_frame_bytes == 0
            || self.wire.max_frame_bytes > 256 << 20
            || self.wire.max_scope_bytes == 0
            || self.wire.max_node_bytes == 0
            || self.wire.max_payload_bytes == 0
            || self.phase.max_body_bytes == 0
            || self.phase.max_body_bytes > self.wire.max_payload_bytes
            || self.phase.max_scope_bytes == 0
            || self.phase.max_scope_bytes > self.wire.max_scope_bytes
            || self.phase.max_node_bytes == 0
            || self.phase.max_node_bytes > self.wire.max_node_bytes
            || self.phase.max_cuts == 0
            || self.phase.max_members == 0
            || self.phase.max_events == 0
            || self.phase.max_identity_bytes == 0
            || self.phase.max_effect_bytes == 0
            || self.phase.max_writer_bytes == 0
            || self.server_request_ms == 0
        {
            return Err(BulkError::Config);
        }
        self.decoded_charge()?;
        self.envelope_charge()?;
        self.tracker_charge()?;
        self.correlation_charge()?;
        Ok(self)
    }
}

/// A finite transport, codec, source, or deadline failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BulkError {
    /// Invalid public finite configuration.
    Config,
    /// Global memory or donor inbox admission was exhausted.
    Capacity,
    /// Stream connect, read, or write failed.
    Transport,
    /// Wire correlation, body, sequence, or termination was invalid.
    Protocol,
    /// The caller's immutable or responder's own deadline elapsed.
    Expired,
    /// Exact donor refusal after valid terminator and EOF.
    Refused(Refusal),
    /// The owning recovery worker has closed its donor inbox.
    Closed,
}

async fn before<T>(deadline: Instant, future: impl Future<Output = T>) -> Result<T, BulkError> {
    tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), future)
        .await
        .map_err(|_| BulkError::Expired)
}

fn reserve(
    admission: &ByteAdmission,
    bytes: usize,
) -> Result<crate::volatile_recovery::bootstrap::admission::Reservation, BulkError> {
    admission
        .reserve(AdmissionClass::Inflight, bytes)
        .map_err(|_| BulkError::Capacity)
}

async fn recv_envelope<S: futures_util::io::AsyncRead + Unpin>(
    stream: &mut DataStream<S>,
    admission: &ByteAdmission,
    limits: BulkLimits,
    deadline: Instant,
) -> Result<Option<Admitted<Envelope>>, BulkError> {
    let raw_budget = reserve(admission, limits.wire.max_frame_bytes)?;
    let raw = before(deadline, stream.recv_bounded(limits.wire.max_frame_bytes))
        .await?
        .map_err(|_| BulkError::Transport)?;
    let Some(raw) = raw else {
        return Ok(None);
    };
    let raw = raw_budget.hold(raw);
    let decoded_budget = reserve(admission, limits.envelope_charge()?)?;
    let decoded = decode(raw.get(), limits.wire).map_err(|_| BulkError::Protocol)?;
    drop(raw);
    Ok(Some(decoded_budget.hold(decoded)))
}

async fn send_envelope<S: futures_util::io::AsyncWrite + Unpin>(
    stream: &mut DataStream<S>,
    envelope: &Envelope,
    admission: &ByteAdmission,
    limits: BulkLimits,
    deadline: Instant,
) -> Result<(), BulkError> {
    let raw_budget = reserve(admission, limits.wire.max_frame_bytes)?;
    let raw = encode(envelope, limits.wire).map_err(|_| BulkError::Protocol)?;
    let raw = raw_budget.hold(raw);
    before(
        deadline,
        stream.send_bounded_ref(raw.get(), limits.wire.max_frame_bytes),
    )
    .await?
    .map_err(|_| BulkError::Transport)
}

/// Bounded follower-side peer request driver; it never decides a retry.
#[derive(Debug)]
pub struct BootstrapBulkClient<B: BulkTransport> {
    plane: DataPlane<B>,
    admission: ByteAdmission,
    limits: BulkLimits,
}

impl<B: BulkTransport> BootstrapBulkClient<B> {
    /// Binds a data plane and the existing shared global admission pool.
    ///
    /// # Errors
    /// Rejects invalid finite network or phase limits.
    pub fn new(
        plane: DataPlane<B>,
        admission: ByteAdmission,
        limits: BulkLimits,
    ) -> Result<Self, BulkError> {
        Ok(Self {
            plane,
            admission,
            limits: limits.validate()?,
        })
    }

    /// Sends one current core effect and accepts only its exact terminal reply.
    /// The local absolute deadline covers connect, send, receive, and EOF.
    ///
    /// # Errors
    /// Rejects a stale/invalid response, source refusal, deadline, or capacity.
    pub async fn request(
        &self,
        correlation: Correlation,
        request: &DonorRequest,
        deadline: Instant,
    ) -> Result<Admitted<super::WireReply>, BulkError> {
        if Instant::now() >= deadline {
            return Err(BulkError::Expired);
        }
        let phase_budget = reserve(&self.admission, self.limits.envelope_charge()?)?;
        let envelope = encode_request(request, correlation, self.limits.phase)
            .map_err(|_| BulkError::Protocol)?;
        let envelope = phase_budget.hold(envelope);
        let raw_budget = reserve(&self.admission, self.limits.wire.max_frame_bytes)?;
        let raw = encode(envelope.get(), self.limits.wire).map_err(|_| BulkError::Protocol)?;
        let raw = raw_budget.hold(raw);
        let tracker_budget = reserve(&self.admission, self.limits.tracker_charge()?)?;
        let mut tracker = ReplyTracker::new(
            envelope.get(),
            1,
            u64::try_from(self.limits.phase.max_body_bytes).map_err(|_| BulkError::Config)?,
        )
        .map_err(|_| BulkError::Protocol)?;
        let mut stream = before(
            deadline,
            self.plane.connect(&envelope.get().correlation.donor.node),
        )
        .await?
        .map_err(|_| BulkError::Transport)?;
        before(
            deadline,
            stream.send_bounded_ref(raw.get(), self.limits.wire.max_frame_bytes),
        )
        .await?
        .map_err(|_| BulkError::Transport)?;
        before(deadline, stream.finish_write())
            .await?
            .map_err(|_| BulkError::Transport)?;
        drop(raw);
        drop(envelope);

        let first = recv_envelope(&mut stream, &self.admission, self.limits, deadline)
            .await?
            .ok_or(BulkError::Protocol)?;
        tracker
            .accept(first.get())
            .map_err(|_| BulkError::Protocol)?;
        let typed = if matches!(&first.get().message, Message::Reply(_)) {
            let typed_budget = reserve(&self.admission, self.limits.decoded_charge()?)?;
            let typed =
                decode_reply(first.get(), self.limits.phase).map_err(|_| BulkError::Protocol)?;
            Some(typed_budget.hold(typed))
        } else {
            None
        };
        drop(first);
        let terminal = recv_envelope(&mut stream, &self.admission, self.limits, deadline)
            .await?
            .ok_or(BulkError::Protocol)?;
        tracker
            .accept(terminal.get())
            .map_err(|_| BulkError::Protocol)?;
        drop(terminal);
        if recv_envelope(&mut stream, &self.admission, self.limits, deadline)
            .await?
            .is_some()
        {
            return Err(BulkError::Protocol);
        }
        let refusal = tracker.finish_eof().map_err(|_| BulkError::Protocol)?;
        drop(tracker_budget);
        if let Some(reason) = refusal {
            return Err(BulkError::Refused(reason));
        }
        typed.ok_or(BulkError::Protocol)
    }
}

/// One serial, bounded donor ingress loop tied to the recovery worker's life.
#[derive(Debug)]
pub struct BootstrapBulkListener<B: BulkTransport> {
    plane: DataPlane<B>,
    sender: DonorSender,
    admission: ByteAdmission,
    limits: BulkLimits,
}

impl<B: BulkTransport> BootstrapBulkListener<B> {
    /// Binds the existing worker inbox. Its current Ready claim identity is
    /// read at each request, so a new capture/attempt needs no new listener.
    ///
    /// # Errors
    /// Rejects invalid finite network limits.
    pub fn new(
        plane: DataPlane<B>,
        sender: DonorSender,
        admission: ByteAdmission,
        limits: BulkLimits,
    ) -> Result<Self, BulkError> {
        Ok(Self {
            plane,
            sender,
            admission,
            limits: limits.validate()?,
        })
    }

    /// Serves at most one stream at a time. The shared `DonorInbox` remains
    /// bounded; cancellation drops any in-flight stream and its byte permits.
    ///
    /// # Errors
    /// Returns a transport failure when the binding can no longer accept.
    pub async fn run(self, mut shutdown: watch::Receiver<bool>) -> Result<(), BulkError> {
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            let accepted = tokio::select! {
                result = self.plane.accept() => result.map_err(|_| BulkError::Transport)?,
                _ = shutdown.changed() => return Ok(()),
            };
            let deadline = Instant::now()
                .checked_add(Duration::from_millis(self.limits.server_request_ms))
                .ok_or(BulkError::Config)?;
            // Individual malformed or expired peers cannot end donor service.
            tokio::select! {
                _ = shutdown.changed() => return Ok(()),
                _ = self.serve_connection(accepted.0, accepted.1, deadline) => {},
            }
        }
    }

    async fn serve_connection(
        &self,
        peer: NodeId,
        mut stream: DataStream<B::Stream>,
        deadline: Instant,
    ) -> Result<(), BulkError> {
        let first = recv_envelope(&mut stream, &self.admission, self.limits, deadline)
            .await?
            .ok_or(BulkError::Protocol)?;
        if first.get().correlation.follower.node != peer
            || self.sender.current_identity().as_ref() != Some(&first.get().correlation.donor)
            || !matches!(&first.get().message, Message::Request(_))
        {
            return Err(BulkError::Protocol);
        }
        let corr_budget = reserve(&self.admission, self.limits.correlation_charge()?)?;
        let correlation = corr_budget.hold(first.get().correlation.clone());
        let exchange = first.get().exchange;
        let request_budget = reserve(&self.admission, self.limits.decoded_charge()?)?;
        let request =
            decode_request(first.get(), self.limits.phase).map_err(|_| BulkError::Protocol)?;
        let request = request_budget.hold(request);
        drop(first);
        // The connector must close its write half after its sole request.
        if recv_envelope(&mut stream, &self.admission, self.limits, deadline)
            .await?
            .is_some()
        {
            return Err(BulkError::Protocol);
        }
        let result = match self.sender.try_submit(request) {
            Ok(response) => match before(deadline, response).await {
                Ok(Ok(Ok(reply))) => Ok(reply),
                Ok(Err(_)) => Err(Refusal::Stale),
                Err(BulkError::Expired) => Err(Refusal::Expired),
                Ok(Ok(Err(_))) | Err(_) => Err(Refusal::Continuity),
            },
            Err(DonorInboxError::Full | DonorInboxError::InvalidCapacity) => Err(Refusal::Capacity),
            Err(DonorInboxError::Closed) => Err(Refusal::Stale),
        };
        self.send_result(&mut stream, correlation.get(), exchange, result, deadline)
            .await
    }

    async fn send_result(
        &self,
        stream: &mut DataStream<B::Stream>,
        correlation: &Correlation,
        exchange: ExchangeKind,
        result: Result<crate::volatile_recovery::bootstrap::ports::DonorReply, Refusal>,
        deadline: Instant,
    ) -> Result<(), BulkError> {
        let phase_budget = reserve(&self.admission, self.limits.envelope_charge()?)?;
        let response = match result {
            Ok(reply) => encode_reply(&reply, correlation.clone(), exchange, self.limits.phase)
                .map_err(|_| BulkError::Protocol)?,
            Err(reason) => Envelope {
                exchange,
                correlation: correlation.clone(),
                message: Message::Refused(reason),
            },
        };
        let response = phase_budget.hold(response);
        let (frames, bytes) = match &response.get().message {
            Message::Reply(payload) => (
                1,
                u64::try_from(payload.len()).map_err(|_| BulkError::Capacity)?,
            ),
            Message::Refused(_) => (0, 0),
            _ => return Err(BulkError::Protocol),
        };
        send_envelope(
            stream,
            response.get(),
            &self.admission,
            self.limits,
            deadline,
        )
        .await?;
        let terminal_budget = reserve(&self.admission, self.limits.envelope_charge()?)?;
        let terminal = terminal_budget.hold(Envelope {
            exchange,
            correlation: correlation.clone(),
            message: Message::Terminator { frames, bytes },
        });
        send_envelope(
            stream,
            terminal.get(),
            &self.admission,
            self.limits,
            deadline,
        )
        .await?;
        before(deadline, stream.finish_write())
            .await?
            .map_err(|_| BulkError::Transport)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> BulkLimits {
        BulkLimits {
            wire: WireLimits {
                max_frame_bytes: 4096,
                max_scope_bytes: 64,
                max_node_bytes: 32,
                max_payload_bytes: 2048,
            },
            phase: PhaseLimits {
                max_body_bytes: 2048,
                max_scope_bytes: 64,
                max_node_bytes: 32,
                max_writer_bytes: 32,
                max_cuts: 4,
                max_members: 4,
                max_events: 4,
                max_identity_bytes: 32,
                max_effect_bytes: 128,
            },
            server_request_ms: 1000,
        }
    }

    #[test]
    fn decoded_collection_and_header_charges_are_finite_and_checked() {
        let limits = limits().validate().unwrap();
        assert!(limits.decoded_charge().unwrap() > limits.phase.max_body_bytes);
        assert!(limits.envelope_charge().unwrap() > limits.wire.max_frame_bytes);
        assert!(limits.tracker_charge().unwrap() > limits.wire.max_frame_bytes);
        assert!(limits.correlation_charge().unwrap() > limits.wire.max_frame_bytes);
        assert_eq!(
            BulkLimits {
                phase: PhaseLimits {
                    max_members: usize::MAX,
                    ..limits.phase
                },
                ..limits
            }
            .validate(),
            Err(BulkError::Config)
        );
    }
}
