//! Bounded one-operation-per-stream donor client and worker-owned listener.

use std::mem::size_of;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::time::{Duration, Instant};

use groupnet_core::NodeId;
use groupnet_core::volatile_bootstrap::ClaimIdentity;
use groupnet_core::volatile_bootstrap::journal::{JournalDelta, NativeCut};
use groupnet_transport::bulk::{BulkTransport, DataPlane, DataStream};
use tokio::sync::watch;

use super::{
    Correlation, Envelope, ExchangeKind, Message, PhaseLimits, PhaseLimitsError, Refusal,
    ReplyTracker, WireLimits, WireLimitsError, decode, decode_reply, decode_request, encode,
    encode_reply, encode_request,
};
use crate::volatile_recovery::bootstrap::admission::{AdmissionClass, Admitted, ByteAdmission};
use crate::volatile_recovery::bootstrap::inbox::{DonorInboxError, DonorSender};
use crate::volatile_recovery::bootstrap::ports::DonorRequest;
use crate::volatile_recovery::{AdapterError, AdapterErrorKind};

/// Finite network and decode limits for one transfer operation.
///
/// [`validate`](Self::validate) runs [`WireLimits::validate`] and
/// [`PhaseLimits::validate`], then the rules relating the two, the
/// responder's request deadline and the memory charges they derive.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BulkLimits {
    /// Complete encoded frame and identity limits.
    pub wire: WireLimits,
    /// Typed request/reply collection limits.
    pub phase: PhaseLimits,
    /// Independent server-side maximum for one accepted request; nonzero and
    /// representable from now on this platform's clock.
    pub server_request: Duration,
}

impl Default for BulkLimits {
    /// The default wire and phase limits, with a 30 s responder deadline.
    fn default() -> Self {
        Self {
            wire: WireLimits::default(),
            phase: PhaseLimits::default(),
            server_request: Duration::from_secs(30),
        }
    }
}

impl BulkLimits {
    pub(crate) fn decoded_charge(self) -> Result<NonZeroUsize, BulkLimitsError> {
        // The body cap covers variable identity/effect/string bytes. These
        // terms cover the owned enum/header and bounded Vec elements, whose
        // in-memory size need not equal their encoded representation. Scope
        // names are Strings whose headers are in the structs below; member
        // and capture NodeIds are Arc strings with two refcount words per
        // allocation. A batch has at most three embedded captures; twelve
        // fixed Arc headers conservatively cover these and follower IDs.
        let header = size_of::<DonorRequest>()
            .max(size_of::<super::WireReply>())
            .max(size_of::<
                groupnet_core::volatile_bootstrap::transfer::TransferEvent,
            >());
        let arc_count = self
            .phase
            .max_members
            .get()
            .checked_add(12)
            .ok_or(BulkLimitsError::ChargeOverflow)?;
        self.phase
            .max_body_bytes
            .checked_add(header)
            .and_then(|value| {
                value.checked_add(
                    self.phase
                        .max_members
                        .get()
                        .checked_mul(size_of::<ClaimIdentity>())?,
                )
            })
            .and_then(|value| {
                value.checked_add(
                    self.phase
                        .max_cuts
                        .get()
                        .checked_mul(size_of::<NativeCut>())?,
                )
            })
            .and_then(|value| {
                value.checked_add(
                    self.phase
                        .max_events
                        .get()
                        .checked_mul(size_of::<JournalDelta>())?,
                )
            })
            .and_then(|value| value.checked_add(arc_count.checked_mul(2 * size_of::<usize>())?))
            .ok_or(BulkLimitsError::ChargeOverflow)
    }

    fn envelope_charge(self) -> Result<NonZeroUsize, BulkLimitsError> {
        // Scope's two String headers are in Envelope. The encoded cap covers
        // variable bytes; four Arc-header slots conservatively cover its two
        // NodeIds and any refcounted identity copy.
        self.wire
            .max_frame_bytes
            .checked_add(size_of::<Envelope>())
            .and_then(|value| value.checked_add(size_of::<[usize; 8]>()))
            .ok_or(BulkLimitsError::ChargeOverflow)
    }

    fn tracker_charge(self) -> Result<NonZeroUsize, BulkLimitsError> {
        self.wire
            .max_frame_bytes
            .checked_add(size_of::<ReplyTracker>())
            .and_then(|value| value.checked_add(size_of::<[usize; 8]>()))
            .ok_or(BulkLimitsError::ChargeOverflow)
    }

    pub(crate) fn correlation_charge(self) -> Result<NonZeroUsize, BulkLimitsError> {
        self.wire
            .max_frame_bytes
            .checked_add(size_of::<Correlation>())
            .and_then(|value| value.checked_add(size_of::<[usize; 8]>()))
            .ok_or(BulkLimitsError::ChargeOverflow)
    }

    /// Rejects unrepresentable or contradictory finite bounds.
    ///
    /// # Errors
    /// The wire or phase rule (as its own error, the
    /// [`source`](std::error::Error::source)), the first phase bound above
    /// its wire bound, a zero or clock-unrepresentable `server_request`, or
    /// a derived memory charge that overflows.
    pub fn validate(&self) -> Result<(), BulkLimitsError> {
        let (wire, phase) = (self.wire, self.phase);
        wire.validate().map_err(BulkLimitsError::Wire)?;
        phase.validate().map_err(BulkLimitsError::Phase)?;
        let rules = [
            (
                phase.max_body_bytes > wire.max_payload_bytes,
                BulkLimitsError::BodyAbovePayload,
            ),
            (
                phase.max_scope_bytes > wire.max_scope_bytes,
                BulkLimitsError::PhaseScopeAboveWire,
            ),
            (
                phase.max_node_bytes > wire.max_node_bytes,
                BulkLimitsError::PhaseNodeAboveWire,
            ),
            (
                self.server_request.is_zero(),
                BulkLimitsError::ZeroServerRequest,
            ),
            (
                Instant::now().checked_add(self.server_request).is_none(),
                BulkLimitsError::ServerRequestRange,
            ),
        ];
        if let Some((_, error)) = rules.into_iter().find(|(violated, _)| *violated) {
            return Err(error);
        }
        self.decoded_charge()?;
        self.envelope_charge()?;
        self.tracker_charge()?;
        self.correlation_charge()?;
        Ok(())
    }
}

/// A [`BulkLimits`] rule that [`BulkLimits::validate`] rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BulkLimitsError {
    /// `wire` failed [`WireLimits::validate`]; the [`WireLimitsError`] is the
    /// [`source`](std::error::Error::source).
    Wire(WireLimitsError),
    /// `phase` failed [`PhaseLimits::validate`]; the [`PhaseLimitsError`] is
    /// the [`source`](std::error::Error::source).
    Phase(PhaseLimitsError),
    /// `phase.max_body_bytes` exceeds `wire.max_payload_bytes`.
    BodyAbovePayload,
    /// `phase.max_scope_bytes` exceeds `wire.max_scope_bytes`.
    PhaseScopeAboveWire,
    /// `phase.max_node_bytes` exceeds `wire.max_node_bytes`.
    PhaseNodeAboveWire,
    /// `server_request` is zero.
    ZeroServerRequest,
    /// `server_request` from now is not representable by the clock.
    ServerRequestRange,
    /// A memory charge derived from these limits overflows `usize`.
    ChargeOverflow,
}

impl std::fmt::Display for BulkLimitsError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Wire(_) => "invalid wire limits",
            Self::Phase(_) => "invalid phase limits",
            Self::BodyAbovePayload => "phase.max_body_bytes exceeds wire.max_payload_bytes",
            Self::PhaseScopeAboveWire => "phase.max_scope_bytes exceeds wire.max_scope_bytes",
            Self::PhaseNodeAboveWire => "phase.max_node_bytes exceeds wire.max_node_bytes",
            Self::ZeroServerRequest => "server_request is zero",
            Self::ServerRequestRange => "server_request exceeds the clock range",
            Self::ChargeOverflow => "a memory charge derived from the bulk limits overflows",
        })
    }
}

impl std::error::Error for BulkLimitsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Wire(error) => Some(error),
            Self::Phase(error) => Some(error),
            _ => None,
        }
    }
}

impl From<BulkLimitsError> for BulkError {
    fn from(error: BulkLimitsError) -> Self {
        Self::Limits(error)
    }
}

/// A finite transport, codec, source, or deadline failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BulkError {
    /// The finite limits violate the named rule.
    Limits(BulkLimitsError),
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

impl std::fmt::Display for BulkError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Limits(_) => formatter.write_str("invalid bulk bootstrap limits"),
            Self::Capacity => formatter.write_str("bulk admission or donor inbox exhausted"),
            Self::Transport => formatter.write_str("bulk stream connect, read, or write failed"),
            Self::Protocol => formatter.write_str("invalid bulk bootstrap wire exchange"),
            Self::Expired => formatter.write_str("bulk operation deadline elapsed"),
            Self::Refused(refusal) => write!(formatter, "donor refused the request: {refusal:?}"),
            Self::Closed => formatter.write_str("donor inbox closed"),
        }
    }
}

impl std::error::Error for BulkError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Limits(error) => Some(error),
            _ => None,
        }
    }
}

/// Classifies a bulk failure for the adapter that drove it, keeping the bulk
/// error as the source.
impl From<BulkError> for AdapterError {
    fn from(error: BulkError) -> Self {
        let kind = match error {
            BulkError::Limits(_) => AdapterErrorKind::InvalidInput,
            BulkError::Capacity => AdapterErrorKind::Capacity,
            BulkError::Transport | BulkError::Refused(_) => AdapterErrorKind::Unavailable,
            BulkError::Protocol => AdapterErrorKind::Malformed,
            BulkError::Expired => AdapterErrorKind::Expired,
            BulkError::Closed => AdapterErrorKind::Fenced,
        };
        Self::with_source(kind, error)
    }
}

/// A limit-derived charge failure seen by the adapter driving the transfer.
impl From<BulkLimitsError> for AdapterError {
    fn from(error: BulkLimitsError) -> Self {
        Self::with_source(AdapterErrorKind::Capacity, error)
    }
}

async fn before<T>(deadline: Instant, future: impl Future<Output = T>) -> Result<T, BulkError> {
    tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), future)
        .await
        .map_err(|_| BulkError::Expired)
}

fn reserve(
    admission: &ByteAdmission,
    bytes: NonZeroUsize,
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
    let max_frame_bytes = limits.wire.max_frame_bytes;
    let raw_budget = reserve(admission, max_frame_bytes)?;
    let raw = before(deadline, stream.recv_bounded(max_frame_bytes))
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
    /// Whether this client charges the same global pool as its recovery worker.
    #[must_use]
    pub fn uses_admission(&self, admission: &ByteAdmission) -> bool {
        self.admission.same_pool(admission)
    }

    /// Binds a data plane and the existing shared global admission pool.
    ///
    /// # Errors
    /// Names the invalid finite network or phase limit.
    pub fn new(
        plane: DataPlane<B>,
        admission: ByteAdmission,
        limits: BulkLimits,
    ) -> Result<Self, BulkLimitsError> {
        limits.validate()?;
        Ok(Self::validated(plane, admission, limits))
    }

    /// [`new`](Self::new) for limits the caller has already validated.
    pub(in crate::volatile_recovery::bootstrap) fn validated(
        plane: DataPlane<B>,
        admission: ByteAdmission,
        limits: BulkLimits,
    ) -> Self {
        Self {
            plane,
            admission,
            limits,
        }
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
            NonZeroU32::MIN,
            NonZeroU64::try_from(self.limits.phase.max_body_bytes)
                .map_err(|_| BulkLimitsError::ChargeOverflow)?,
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
    /// Names the invalid finite network or phase limit.
    pub fn new(
        plane: DataPlane<B>,
        sender: DonorSender,
        admission: ByteAdmission,
        limits: BulkLimits,
    ) -> Result<Self, BulkLimitsError> {
        limits.validate()?;
        Ok(Self {
            plane,
            sender,
            admission,
            limits,
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
                .checked_add(self.limits.server_request)
                .ok_or(BulkLimitsError::ServerRequestRange)?;
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
            Err(DonorInboxError::Full) => Err(Refusal::Capacity),
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
    use std::num::NonZeroUsize;

    use super::*;

    fn nz(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).unwrap()
    }

    fn limits() -> BulkLimits {
        BulkLimits {
            wire: WireLimits {
                max_frame_bytes: nz(4096),
                max_scope_bytes: nz(64),
                max_node_bytes: nz(32),
                max_payload_bytes: nz(2048),
            },
            phase: PhaseLimits {
                max_body_bytes: nz(2048),
                max_scope_bytes: nz(64),
                max_node_bytes: nz(32),
                max_writer_bytes: nz(32),
                max_cuts: nz(4),
                max_members: nz(4),
                max_events: nz(4),
                max_identity_bytes: nz(32),
                max_effect_bytes: nz(128),
            },
            server_request: Duration::from_secs(1),
        }
    }

    #[test]
    fn decoded_collection_and_header_charges_are_finite_and_checked() {
        let limits = limits();
        limits.validate().unwrap();
        assert!(limits.decoded_charge().unwrap() > limits.phase.max_body_bytes);
        assert!(limits.envelope_charge().unwrap() > limits.wire.max_frame_bytes);
        assert!(limits.tracker_charge().unwrap() > limits.wire.max_frame_bytes);
        assert!(limits.correlation_charge().unwrap() > limits.wire.max_frame_bytes);
        assert_eq!(BulkLimits::default().validate(), Ok(()));
    }

    #[test]
    fn validation_composes_the_wire_and_phase_rules_and_keeps_them_as_source() {
        let limits = limits();
        let wire = BulkLimits {
            wire: WireLimits {
                max_payload_bytes: nz(4097),
                ..limits.wire
            },
            ..limits
        };
        let error = wire.validate().unwrap_err();
        assert_eq!(
            error,
            BulkLimitsError::Wire(WireLimitsError::PayloadAboveFrame)
        );
        assert!(std::error::Error::source(&error).is_some());

        let phase = BulkLimits {
            phase: PhaseLimits {
                max_members: nz(usize::MAX),
                ..limits.phase
            },
            ..limits
        };
        assert_eq!(
            phase.validate(),
            Err(BulkLimitsError::Phase(
                PhaseLimitsError::MembersAboveCountField
            ))
        );

        let inverted = BulkLimits {
            phase: PhaseLimits {
                max_body_bytes: nz(limits.wire.max_payload_bytes.get() + 1),
                ..limits.phase
            },
            ..limits
        };
        let error = BulkError::from(inverted.validate().unwrap_err());
        assert_eq!(error, BulkError::Limits(BulkLimitsError::BodyAbovePayload));
        assert!(std::error::Error::source(&error).is_some());
    }

    #[test]
    fn the_responder_deadline_is_nonzero_and_on_the_clock() {
        let limits = limits();
        assert_eq!(
            BulkLimits {
                server_request: Duration::ZERO,
                ..limits
            }
            .validate(),
            Err(BulkLimitsError::ZeroServerRequest)
        );
        assert_eq!(
            BulkLimits {
                server_request: Duration::MAX,
                ..limits
            }
            .validate(),
            Err(BulkLimitsError::ServerRequestRange)
        );
    }
}
