//! Typed, bounded donor request bodies carried by the bulk envelope.

use groupnet_core::NodeId;
use groupnet_core::volatile_bootstrap::journal::{
    AttachToken, BarrierReceipt, CaptureId, JournalCursor, NativeCut, ReservationId,
};
use groupnet_core::volatile_bootstrap::{BootId, BootstrapScope, ClaimIdentity};

use super::{Correlation, Envelope, ExchangeKind, Message, Reader, WireError};
use crate::volatile_recovery::bootstrap::ports::DonorRequest;

mod reply;
pub use reply::{WireReply, decode_reply, encode_reply};

/// Finite phase metadata and collection limits applied before decoding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PhaseLimits {
    /// Maximum complete phase body bytes.
    pub max_body_bytes: usize,
    /// Maximum bytes in one scope's two names.
    pub max_scope_bytes: usize,
    /// Maximum bytes in one node identity.
    pub max_node_bytes: usize,
    /// Maximum native writer identity bytes.
    pub max_writer_bytes: usize,
    /// Maximum native writer cuts in one barrier.
    pub max_cuts: usize,
    /// Maximum complete members in one barrier.
    pub max_members: usize,
    /// Maximum returned journal effects in one batch.
    pub max_events: usize,
    /// Maximum bytes in one native/local mutation identity.
    pub max_identity_bytes: usize,
    /// Maximum bytes in one final index effect.
    pub max_effect_bytes: usize,
}

impl PhaseLimits {
    fn valid(self) -> bool {
        self.max_body_bytes > 0
            && self.max_scope_bytes > 0
            && self.max_node_bytes > 0
            && self.max_writer_bytes > 0
            && self.max_cuts > 0
            && self.max_members > 0
            && self.max_events > 0
            && self.max_identity_bytes > 0
            && self.max_effect_bytes > 0
    }
}

struct Writer {
    bytes: Vec<u8>,
    cap: usize,
}

impl Writer {
    fn new(cap: usize) -> Self {
        Self {
            bytes: Vec::new(),
            cap,
        }
    }

    fn put(&mut self, bytes: &[u8]) -> Result<(), WireError> {
        let next = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .ok_or(WireError::Capacity)?;
        if next > self.cap {
            return Err(WireError::Capacity);
        }
        self.bytes
            .try_reserve_exact(bytes.len())
            .map_err(|_| WireError::Capacity)?;
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    fn number(&mut self, value: usize) -> Result<(), WireError> {
        self.put(
            &u64::try_from(value)
                .map_err(|_| WireError::Capacity)?
                .to_be_bytes(),
        )
    }

    fn string(&mut self, value: &str, cap: usize) -> Result<(), WireError> {
        if value.is_empty() || value.len() > cap {
            return Err(WireError::Capacity);
        }
        let len = u16::try_from(value.len()).map_err(|_| WireError::Capacity)?;
        self.put(&len.to_be_bytes())?;
        self.put(value.as_bytes())
    }

    fn blob(&mut self, value: &[u8], cap: usize) -> Result<(), WireError> {
        if value.is_empty() || value.len() > cap {
            return Err(WireError::Capacity);
        }
        let len = u16::try_from(value.len()).map_err(|_| WireError::Capacity)?;
        self.put(&len.to_be_bytes())?;
        self.put(value)
    }

    fn identity(&mut self, id: &ClaimIdentity, limits: PhaseLimits) -> Result<(), WireError> {
        self.string(id.node.as_str(), limits.max_node_bytes)?;
        self.put(&id.incarnation.0.to_be_bytes())?;
        self.put(&id.session.to_be_bytes())?;
        self.put(&id.attempt.to_be_bytes())
    }

    fn scope(&mut self, scope: &BootstrapScope, limits: PhaseLimits) -> Result<(), WireError> {
        let total = scope
            .domain
            .len()
            .checked_add(scope.partition.len())
            .ok_or(WireError::Capacity)?;
        if total > limits.max_scope_bytes {
            return Err(WireError::Capacity);
        }
        self.string(&scope.domain, limits.max_scope_bytes)?;
        self.string(&scope.partition, limits.max_scope_bytes)
    }

    fn capture(&mut self, id: &CaptureId, limits: PhaseLimits) -> Result<(), WireError> {
        self.scope(&id.scope, limits)?;
        self.identity(&id.donor, limits)?;
        self.put(&id.recovery_generation.to_be_bytes())?;
        self.put(&id.serial.to_be_bytes())
    }

    fn cursor(&mut self, cursor: &JournalCursor, limits: PhaseLimits) -> Result<(), WireError> {
        self.capture(&cursor.capture, limits)?;
        self.put(&cursor.position.to_be_bytes())
    }

    fn reservation(&mut self, id: &ReservationId, limits: PhaseLimits) -> Result<(), WireError> {
        self.capture(&id.capture, limits)?;
        self.identity(&id.follower, limits)?;
        self.put(&id.serial.to_be_bytes())
    }

    fn attachment(&mut self, token: &AttachToken, limits: PhaseLimits) -> Result<(), WireError> {
        self.reservation(&token.reservation, limits)?;
        self.put(&token.operation.to_be_bytes())
    }

    fn barrier(&mut self, receipt: &BarrierReceipt, limits: PhaseLimits) -> Result<(), WireError> {
        if receipt.covered_cuts.len() > limits.max_cuts
            || receipt.members.len() > limits.max_members
        {
            return Err(WireError::Capacity);
        }
        self.reservation(&receipt.reservation, limits)?;
        self.put(&receipt.attach_operation.to_be_bytes())?;
        self.put(&receipt.barrier_operation.to_be_bytes())?;
        self.cursor(&receipt.cursor, limits)?;
        self.put(
            &u16::try_from(receipt.covered_cuts.len())
                .map_err(|_| WireError::Capacity)?
                .to_be_bytes(),
        )?;
        for cut in &receipt.covered_cuts {
            self.blob(&cut.writer, limits.max_writer_bytes)?;
            self.put(&cut.epoch.to_be_bytes())?;
            self.put(&cut.sequence.to_be_bytes())?;
        }
        self.put(
            &u16::try_from(receipt.members.len())
                .map_err(|_| WireError::Capacity)?
                .to_be_bytes(),
        )?;
        for member in &receipt.members {
            self.identity(member, limits)?;
        }
        Ok(())
    }
}

fn read_size(reader: &mut Reader<'_>) -> Result<usize, WireError> {
    usize::try_from(u64::from_be_bytes(reader.number()?)).map_err(|_| WireError::Capacity)
}

fn read_blob(reader: &mut Reader<'_>, cap: usize) -> Result<Vec<u8>, WireError> {
    let length = usize::from(u16::from_be_bytes(reader.number()?));
    if length == 0 || length > cap {
        return Err(WireError::Capacity);
    }
    let data = reader.take(length)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|_| WireError::Capacity)?;
    bytes.extend_from_slice(data);
    Ok(bytes)
}

fn read_identity(reader: &mut Reader<'_>, limits: PhaseLimits) -> Result<ClaimIdentity, WireError> {
    Ok(ClaimIdentity {
        node: NodeId::new(reader.string(limits.max_node_bytes)?),
        incarnation: BootId(u128::from_be_bytes(reader.number()?)),
        session: u64::from_be_bytes(reader.number()?),
        attempt: u64::from_be_bytes(reader.number()?),
    })
}

fn read_scope(reader: &mut Reader<'_>, limits: PhaseLimits) -> Result<BootstrapScope, WireError> {
    let domain = reader.string(limits.max_scope_bytes)?;
    let partition = reader.string(limits.max_scope_bytes)?;
    if domain
        .len()
        .checked_add(partition.len())
        .ok_or(WireError::Capacity)?
        > limits.max_scope_bytes
    {
        return Err(WireError::Capacity);
    }
    Ok(BootstrapScope { domain, partition })
}

fn read_capture(reader: &mut Reader<'_>, limits: PhaseLimits) -> Result<CaptureId, WireError> {
    Ok(CaptureId {
        scope: read_scope(reader, limits)?,
        donor: read_identity(reader, limits)?,
        recovery_generation: u64::from_be_bytes(reader.number()?),
        serial: u64::from_be_bytes(reader.number()?),
    })
}

fn read_cursor(reader: &mut Reader<'_>, limits: PhaseLimits) -> Result<JournalCursor, WireError> {
    Ok(JournalCursor {
        capture: read_capture(reader, limits)?,
        position: u64::from_be_bytes(reader.number()?),
    })
}

fn read_reservation(
    reader: &mut Reader<'_>,
    limits: PhaseLimits,
) -> Result<ReservationId, WireError> {
    Ok(ReservationId {
        capture: read_capture(reader, limits)?,
        follower: read_identity(reader, limits)?,
        serial: u64::from_be_bytes(reader.number()?),
    })
}

fn read_attachment(reader: &mut Reader<'_>, limits: PhaseLimits) -> Result<AttachToken, WireError> {
    Ok(AttachToken {
        reservation: read_reservation(reader, limits)?,
        operation: u64::from_be_bytes(reader.number()?),
    })
}

fn read_barrier(reader: &mut Reader<'_>, limits: PhaseLimits) -> Result<BarrierReceipt, WireError> {
    let reservation = read_reservation(reader, limits)?;
    let attach_operation = u64::from_be_bytes(reader.number()?);
    let barrier_operation = u64::from_be_bytes(reader.number()?);
    let cursor = read_cursor(reader, limits)?;
    let cuts_len = usize::from(u16::from_be_bytes(reader.number()?));
    if cuts_len > limits.max_cuts {
        return Err(WireError::Capacity);
    }
    let mut covered_cuts = Vec::new();
    covered_cuts
        .try_reserve_exact(cuts_len)
        .map_err(|_| WireError::Capacity)?;
    for _ in 0..cuts_len {
        covered_cuts.push(NativeCut {
            writer: read_blob(reader, limits.max_writer_bytes)?,
            epoch: u64::from_be_bytes(reader.number()?),
            sequence: u64::from_be_bytes(reader.number()?),
        });
    }
    let members_len = usize::from(u16::from_be_bytes(reader.number()?));
    if members_len > limits.max_members {
        return Err(WireError::Capacity);
    }
    let mut members = Vec::new();
    members
        .try_reserve_exact(members_len)
        .map_err(|_| WireError::Capacity)?;
    for _ in 0..members_len {
        members.push(read_identity(reader, limits)?);
    }
    Ok(BarrierReceipt {
        reservation,
        attach_operation,
        barrier_operation,
        cursor,
        covered_cuts,
        members,
    })
}

fn cut_before(left: &NativeCut, right: &NativeCut) -> bool {
    (&left.writer, left.epoch) < (&right.writer, right.epoch)
}

fn valid_barrier(value: &BarrierReceipt) -> bool {
    value.reservation.serial != 0
        && value.attach_operation != 0
        && value.barrier_operation != 0
        && value.cursor.capture == value.reservation.capture
        && value
            .covered_cuts
            .windows(2)
            .all(|pair| cut_before(&pair[0], &pair[1]))
        && value
            .members
            .windows(2)
            .all(|pair| pair[0].node < pair[1].node)
}

fn kind(request: &DonorRequest) -> ExchangeKind {
    match request {
        DonorRequest::Offer { .. } => ExchangeKind::Offer,
        DonorRequest::Reserve { .. } => ExchangeKind::Reserve,
        DonorRequest::Chunk { .. } => ExchangeKind::Chunk,
        DonorRequest::Attach { .. } => ExchangeKind::Attach,
        DonorRequest::Barrier { .. } => ExchangeKind::Barrier,
        DonorRequest::AdvanceBarrier { .. } => ExchangeKind::AdvanceBarrier,
        DonorRequest::Batch { .. } => ExchangeKind::Batch,
        DonorRequest::Ack { .. } => ExchangeKind::Ack,
        DonorRequest::Release { .. } => ExchangeKind::Release,
    }
}

fn capture_of(request: &DonorRequest) -> Option<&CaptureId> {
    match request {
        DonorRequest::Offer { .. } => None,
        DonorRequest::Reserve { cut, .. } => Some(&cut.capture),
        DonorRequest::Chunk { reservation, .. }
        | DonorRequest::Attach { reservation }
        | DonorRequest::Ack { reservation, .. }
        | DonorRequest::Release { reservation } => Some(&reservation.capture),
        DonorRequest::Barrier { attachment, .. } => Some(&attachment.reservation.capture),
        DonorRequest::AdvanceBarrier { expected, .. } => Some(&expected.reservation.capture),
        DonorRequest::Batch { barrier, .. } => Some(&barrier.reservation.capture),
    }
}

fn reservation_of(request: &DonorRequest) -> Option<&ReservationId> {
    match request {
        DonorRequest::Chunk { reservation, .. }
        | DonorRequest::Attach { reservation }
        | DonorRequest::Ack { reservation, .. }
        | DonorRequest::Release { reservation } => Some(reservation),
        DonorRequest::Barrier { attachment, .. } => Some(&attachment.reservation),
        DonorRequest::AdvanceBarrier { expected, .. } => Some(&expected.reservation),
        DonorRequest::Batch { barrier, .. } => Some(&barrier.reservation),
        DonorRequest::Offer { .. } | DonorRequest::Reserve { .. } => None,
    }
}

fn validate_request(request: &DonorRequest, correlation: &Correlation) -> Result<(), WireError> {
    if let Some(capture) = capture_of(request) {
        if capture.scope != correlation.scope
            || capture.donor != correlation.donor
            || capture.serial == 0
            || capture.recovery_generation == 0
        {
            return Err(WireError::Invalid);
        }
    }
    if let Some(reservation) = reservation_of(request) {
        if reservation.follower != correlation.follower || reservation.serial == 0 {
            return Err(WireError::Invalid);
        }
    }
    match request {
        DonorRequest::Offer { max_metadata_bytes } if *max_metadata_bytes == 0 => {
            Err(WireError::Invalid)
        }
        DonorRequest::Reserve { cut, .. } if cut.position != 0 => Err(WireError::Invalid),
        DonorRequest::Chunk { max_bytes, .. } if *max_bytes == 0 => Err(WireError::Invalid),
        DonorRequest::Reserve { follower, .. } if *follower != correlation.follower => {
            Err(WireError::Invalid)
        }
        DonorRequest::Barrier {
            max_metadata_bytes, ..
        }
        | DonorRequest::AdvanceBarrier {
            max_metadata_bytes, ..
        } if *max_metadata_bytes == 0 => Err(WireError::Invalid),
        DonorRequest::Batch {
            max_bytes,
            max_events,
            ..
        } if *max_bytes == 0 || *max_events == 0 => Err(WireError::Invalid),
        DonorRequest::AdvanceBarrier { expected, .. } if !valid_barrier(expected) => {
            Err(WireError::Invalid)
        }
        DonorRequest::Batch { barrier, .. } if !valid_barrier(barrier) => Err(WireError::Invalid),
        DonorRequest::Ack {
            through,
            reservation,
            batch_operation,
        } if through.capture != reservation.capture || *batch_operation == 0 => {
            Err(WireError::Invalid)
        }
        DonorRequest::Barrier { attachment, .. } if attachment.operation == 0 => {
            Err(WireError::Invalid)
        }
        _ => Ok(()),
    }
}

/// Encodes one typed donor request inside its exact correlation envelope.
///
/// # Errors
/// Rejects inconsistent nested identities or a finite body limit.
pub fn encode_request(
    request: &DonorRequest,
    correlation: Correlation,
    limits: PhaseLimits,
) -> Result<Envelope, WireError> {
    if !limits.valid() {
        return Err(WireError::Capacity);
    }
    validate_request(request, &correlation)?;
    let mut body = Writer::new(limits.max_body_bytes);
    match request {
        DonorRequest::Offer { max_metadata_bytes } => body.number(*max_metadata_bytes)?,
        DonorRequest::Reserve { follower, cut } => {
            body.identity(follower, limits)?;
            body.cursor(cut, limits)?;
        }
        DonorRequest::Chunk {
            reservation,
            sequence,
            max_bytes,
        } => {
            body.reservation(reservation, limits)?;
            body.number(*sequence)?;
            body.number(*max_bytes)?;
        }
        DonorRequest::Attach { reservation } | DonorRequest::Release { reservation } => {
            body.reservation(reservation, limits)?;
        }
        DonorRequest::Barrier {
            attachment,
            max_metadata_bytes,
        } => {
            body.attachment(attachment, limits)?;
            body.number(*max_metadata_bytes)?;
        }
        DonorRequest::AdvanceBarrier {
            expected,
            max_metadata_bytes,
        } => {
            body.barrier(expected, limits)?;
            body.number(*max_metadata_bytes)?;
        }
        DonorRequest::Batch {
            barrier,
            max_bytes,
            max_events,
        } => {
            body.barrier(barrier, limits)?;
            body.number(*max_bytes)?;
            body.number(*max_events)?;
        }
        DonorRequest::Ack {
            reservation,
            batch_operation,
            through,
        } => {
            body.reservation(reservation, limits)?;
            body.put(&batch_operation.to_be_bytes())?;
            body.cursor(through, limits)?;
        }
    }
    Ok(Envelope {
        exchange: kind(request),
        correlation,
        message: Message::Request(body.bytes),
    })
}

/// Decodes a complete typed request after the outer envelope was validated.
///
/// # Errors
/// Rejects malformed, excessive, noncanonical, or mismatched nested proof.
pub fn decode_request(frame: &Envelope, limits: PhaseLimits) -> Result<DonorRequest, WireError> {
    let Message::Request(bytes) = &frame.message else {
        return Err(WireError::Invalid);
    };
    if !limits.valid() || bytes.len() > limits.max_body_bytes {
        return Err(WireError::Capacity);
    }
    let mut reader = Reader { bytes, position: 0 };
    let request = match frame.exchange {
        ExchangeKind::Offer => DonorRequest::Offer {
            max_metadata_bytes: read_size(&mut reader)?,
        },
        ExchangeKind::Reserve => DonorRequest::Reserve {
            follower: read_identity(&mut reader, limits)?,
            cut: read_cursor(&mut reader, limits)?,
        },
        ExchangeKind::Chunk => DonorRequest::Chunk {
            reservation: read_reservation(&mut reader, limits)?,
            sequence: read_size(&mut reader)?,
            max_bytes: read_size(&mut reader)?,
        },
        ExchangeKind::Attach => DonorRequest::Attach {
            reservation: read_reservation(&mut reader, limits)?,
        },
        ExchangeKind::Barrier => DonorRequest::Barrier {
            attachment: read_attachment(&mut reader, limits)?,
            max_metadata_bytes: read_size(&mut reader)?,
        },
        ExchangeKind::AdvanceBarrier => DonorRequest::AdvanceBarrier {
            expected: read_barrier(&mut reader, limits)?,
            max_metadata_bytes: read_size(&mut reader)?,
        },
        ExchangeKind::Batch => DonorRequest::Batch {
            barrier: read_barrier(&mut reader, limits)?,
            max_bytes: read_size(&mut reader)?,
            max_events: read_size(&mut reader)?,
        },
        ExchangeKind::Ack => DonorRequest::Ack {
            reservation: read_reservation(&mut reader, limits)?,
            batch_operation: u64::from_be_bytes(reader.number()?),
            through: read_cursor(&mut reader, limits)?,
        },
        ExchangeKind::Release => DonorRequest::Release {
            reservation: read_reservation(&mut reader, limits)?,
        },
    };
    if reader.position != bytes.len() {
        return Err(WireError::Invalid);
    }
    validate_request(&request, &frame.correlation)?;
    Ok(request)
}

#[cfg(test)]
mod tests {
    use super::*;
    use groupnet_core::volatile_bootstrap::BootstrapOperation;

    fn limits() -> PhaseLimits {
        PhaseLimits {
            max_body_bytes: 4096,
            max_scope_bytes: 32,
            max_node_bytes: 32,
            max_writer_bytes: 16,
            max_cuts: 4,
            max_members: 4,
            max_events: 4,
            max_identity_bytes: 16,
            max_effect_bytes: 64,
        }
    }

    fn identities() -> (Correlation, CaptureId, ReservationId, BarrierReceipt) {
        let scope = BootstrapScope {
            domain: "origin".into(),
            partition: "bucket".into(),
        };
        let donor = ClaimIdentity {
            node: NodeId::from("donor"),
            incarnation: BootId(11),
            session: 12,
            attempt: 13,
        };
        let follower = ClaimIdentity {
            node: NodeId::from("follower"),
            incarnation: BootId(21),
            session: 22,
            attempt: 23,
        };
        let correlation = Correlation {
            scope: scope.clone(),
            donor: donor.clone(),
            follower: follower.clone(),
            parent: BootstrapOperation {
                incarnation: BootId(21),
                session: 22,
                generation: 1,
                token: 31,
            },
            child: BootstrapOperation {
                incarnation: BootId(21),
                session: 22,
                generation: 1,
                token: 32,
            },
        };
        let capture = CaptureId {
            scope,
            donor,
            recovery_generation: 2,
            serial: 3,
        };
        let reservation = ReservationId {
            capture: capture.clone(),
            follower: follower.clone(),
            serial: 4,
        };
        let barrier = BarrierReceipt {
            reservation: reservation.clone(),
            attach_operation: 5,
            barrier_operation: 6,
            cursor: JournalCursor {
                capture: capture.clone(),
                position: 7,
            },
            covered_cuts: vec![NativeCut {
                writer: b"writer".to_vec(),
                epoch: 1,
                sequence: 0,
            }],
            members: vec![follower],
        };
        (correlation, capture, reservation, barrier)
    }

    #[test]
    fn all_nine_request_bodies_round_trip_with_exact_nested_identity() {
        let (correlation, capture, reservation, barrier) = identities();
        let requests = [
            DonorRequest::Offer {
                max_metadata_bytes: 100,
            },
            DonorRequest::Reserve {
                follower: correlation.follower.clone(),
                cut: JournalCursor {
                    capture: capture.clone(),
                    position: 0,
                },
            },
            DonorRequest::Chunk {
                reservation: reservation.clone(),
                sequence: 0,
                max_bytes: 40,
            },
            DonorRequest::Attach {
                reservation: reservation.clone(),
            },
            DonorRequest::Barrier {
                attachment: AttachToken {
                    reservation: reservation.clone(),
                    operation: 5,
                },
                max_metadata_bytes: 100,
            },
            DonorRequest::AdvanceBarrier {
                expected: barrier.clone(),
                max_metadata_bytes: 100,
            },
            DonorRequest::Batch {
                barrier,
                max_bytes: 50,
                max_events: 2,
            },
            DonorRequest::Ack {
                reservation: reservation.clone(),
                batch_operation: 8,
                through: JournalCursor {
                    capture,
                    position: 7,
                },
            },
            DonorRequest::Release { reservation },
        ];
        for request in requests {
            let frame = encode_request(&request, correlation.clone(), limits()).unwrap();
            assert_eq!(decode_request(&frame, limits()).unwrap(), request);
        }
    }

    #[test]
    fn stale_nested_capture_and_noncanonical_barrier_fail_closed() {
        let (correlation, capture, reservation, mut barrier) = identities();
        let mut wrong = capture.clone();
        wrong.serial += 1;
        wrong.scope.partition = "other".into();
        let request = DonorRequest::Reserve {
            follower: correlation.follower.clone(),
            cut: JournalCursor {
                capture: wrong,
                position: 0,
            },
        };
        assert_eq!(
            encode_request(&request, correlation.clone(), limits()),
            Err(WireError::Invalid)
        );
        barrier.covered_cuts.push(barrier.covered_cuts[0].clone());
        assert_eq!(
            encode_request(
                &DonorRequest::Batch {
                    barrier,
                    max_bytes: 1,
                    max_events: 1,
                },
                correlation.clone(),
                limits(),
            ),
            Err(WireError::Invalid)
        );
        let mut frame = encode_request(
            &DonorRequest::Release { reservation },
            correlation,
            limits(),
        )
        .unwrap();
        if let Message::Request(body) = &mut frame.message {
            body.push(0);
        }
        assert_eq!(decode_request(&frame, limits()), Err(WireError::Invalid));
    }
}
