//! Typed donor replies; private image bytes remain opaque to Groupnet.

use groupnet_core::volatile_bootstrap::journal::{
    AttachToken, BarrierReceipt, DeltaIdentity, JournalBatch, JournalDelta, NativeCut,
    ReservationId,
};
use groupnet_core::volatile_bootstrap::transfer::TransferOffer;

use super::{
    PhaseLimits, Reader, Writer, read_barrier, read_blob, read_capture, read_cursor, read_identity,
    read_reservation, read_size, valid_barrier,
};
use crate::volatile_recovery::bootstrap::bulk_wire::{
    Correlation, Envelope, ExchangeKind, Message, WireError,
};
use crate::volatile_recovery::bootstrap::ports::DonorReply;

/// A decoded source response; its owned bytes require a separate admission
/// charge before the runtime places it in a queued callback or private stage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WireReply {
    /// Complete bounded image offer.
    Offer(TransferOffer),
    /// Exact donor suffix reservation.
    Reserved(ReservationId),
    /// One opaque encoded image chunk.
    Chunk(Vec<u8>),
    /// Exact live donor attachment.
    Attached(AttachToken),
    /// Atomic B and native cuts.
    Barrier(BarrierReceipt),
    /// One contiguous journal batch.
    Batch(JournalBatch),
    /// Exact batch acknowledgment/readback succeeded.
    Acked,
    /// Exact reservation release succeeded.
    Released,
}

fn write_cuts(
    writer: &mut Writer,
    cuts: &[NativeCut],
    limits: PhaseLimits,
) -> Result<(), WireError> {
    if cuts.len() > limits.max_cuts
        || cuts
            .windows(2)
            .any(|pair| (&pair[0].writer, pair[0].epoch) >= (&pair[1].writer, pair[1].epoch))
    {
        return Err(WireError::Invalid);
    }
    writer.put(
        &u16::try_from(cuts.len())
            .map_err(|_| WireError::Capacity)?
            .to_be_bytes(),
    )?;
    for cut in cuts {
        writer.blob(&cut.writer, limits.max_writer_bytes)?;
        writer.put(&cut.epoch.to_be_bytes())?;
        writer.put(&cut.sequence.to_be_bytes())?;
    }
    Ok(())
}

fn read_cuts(reader: &mut Reader<'_>, limits: PhaseLimits) -> Result<Vec<NativeCut>, WireError> {
    let count = usize::from(u16::from_be_bytes(reader.number()?));
    if count > limits.max_cuts {
        return Err(WireError::Capacity);
    }
    let mut cuts = Vec::new();
    cuts.try_reserve_exact(count)
        .map_err(|_| WireError::Capacity)?;
    for _ in 0..count {
        let cut = NativeCut {
            writer: read_blob(reader, limits.max_writer_bytes)?,
            epoch: u64::from_be_bytes(reader.number()?),
            sequence: u64::from_be_bytes(reader.number()?),
        };
        if cuts
            .last()
            .is_some_and(|last: &NativeCut| (&last.writer, last.epoch) >= (&cut.writer, cut.epoch))
        {
            return Err(WireError::Invalid);
        }
        cuts.push(cut);
    }
    Ok(cuts)
}

fn write_members(
    writer: &mut Writer,
    members: &[groupnet_core::volatile_bootstrap::ClaimIdentity],
    limits: PhaseLimits,
) -> Result<(), WireError> {
    if members.len() > limits.max_members
        || members.windows(2).any(|pair| pair[0].node >= pair[1].node)
    {
        return Err(WireError::Invalid);
    }
    writer.put(
        &u16::try_from(members.len())
            .map_err(|_| WireError::Capacity)?
            .to_be_bytes(),
    )?;
    for member in members {
        writer.identity(member, limits)?;
    }
    Ok(())
}

fn read_members(
    reader: &mut Reader<'_>,
    limits: PhaseLimits,
) -> Result<Vec<groupnet_core::volatile_bootstrap::ClaimIdentity>, WireError> {
    let count = usize::from(u16::from_be_bytes(reader.number()?));
    if count > limits.max_members {
        return Err(WireError::Capacity);
    }
    let mut members = Vec::new();
    members
        .try_reserve_exact(count)
        .map_err(|_| WireError::Capacity)?;
    for _ in 0..count {
        let member = read_identity(reader, limits)?;
        if members
            .last()
            .is_some_and(|last: &groupnet_core::volatile_bootstrap::ClaimIdentity| {
                last.node >= member.node
            })
        {
            return Err(WireError::Invalid);
        }
        members.push(member);
    }
    Ok(members)
}

fn valid_offer(offer: &TransferOffer, correlation: &Correlation) -> bool {
    offer.capture.scope == correlation.scope
        && offer.capture.donor == correlation.donor
        && offer.capture.serial != 0
        && offer.capture.recovery_generation != 0
        && offer.image_cut.capture == offer.capture
        && offer.image_cut.position == 0
        && offer.schema != 0
        && offer.encoded_bytes != 0
        && offer.decoded_bytes != 0
        && offer.chunks != 0
}

fn valid_reservation(value: &ReservationId, correlation: &Correlation) -> bool {
    value.capture.scope == correlation.scope
        && value.capture.donor == correlation.donor
        && value.capture.serial != 0
        && value.capture.recovery_generation != 0
        && value.follower == correlation.follower
        && value.serial != 0
}

fn valid_batch(value: &JournalBatch, correlation: &Correlation, limits: PhaseLimits) -> bool {
    if !valid_reservation(&value.reservation, correlation)
        || value.operation == 0
        || value.from.capture != value.reservation.capture
        || value.through.capture != value.reservation.capture
        || value.deltas.len() > limits.max_events
        || value.bytes > limits.max_body_bytes
    {
        return false;
    }
    let mut position = value.from.position;
    for delta in &value.deltas {
        let Some(next) = position.checked_add(1) else {
            return false;
        };
        if delta.position != next
            || delta.effect.len() > limits.max_effect_bytes
            || match &delta.identity {
                DeltaIdentity::Native(cut) => {
                    cut.writer.is_empty() || cut.writer.len() > limits.max_writer_bytes
                }
                DeltaIdentity::Local(id) => id.is_empty() || id.len() > limits.max_identity_bytes,
            }
        {
            return false;
        }
        position = next;
    }
    position == value.through.position
}

fn write_offer(
    writer: &mut Writer,
    offer: &TransferOffer,
    limits: PhaseLimits,
) -> Result<(), WireError> {
    writer.capture(&offer.capture, limits)?;
    writer.cursor(&offer.image_cut, limits)?;
    writer.put(&offer.schema.to_be_bytes())?;
    writer.number(offer.encoded_bytes)?;
    writer.number(offer.decoded_bytes)?;
    writer.number(offer.chunks)?;
    writer.put(&offer.commitment)?;
    write_members(writer, &offer.members, limits)?;
    write_cuts(writer, &offer.cuts, limits)
}

fn read_offer(reader: &mut Reader<'_>, limits: PhaseLimits) -> Result<TransferOffer, WireError> {
    Ok(TransferOffer {
        capture: read_capture(reader, limits)?,
        image_cut: read_cursor(reader, limits)?,
        schema: u32::from_be_bytes(reader.number()?),
        encoded_bytes: read_size(reader)?,
        decoded_bytes: read_size(reader)?,
        chunks: read_size(reader)?,
        commitment: reader.number()?,
        members: read_members(reader, limits)?,
        cuts: read_cuts(reader, limits)?,
    })
}

fn write_batch(
    writer: &mut Writer,
    batch: &JournalBatch,
    limits: PhaseLimits,
) -> Result<(), WireError> {
    writer.reservation(&batch.reservation, limits)?;
    writer.put(&batch.operation.to_be_bytes())?;
    writer.cursor(&batch.from, limits)?;
    writer.cursor(&batch.through, limits)?;
    writer.put(
        &u16::try_from(batch.deltas.len())
            .map_err(|_| WireError::Capacity)?
            .to_be_bytes(),
    )?;
    for delta in &batch.deltas {
        writer.put(&delta.position.to_be_bytes())?;
        match &delta.identity {
            DeltaIdentity::Native(cut) => {
                writer.put(&[1])?;
                writer.blob(&cut.writer, limits.max_writer_bytes)?;
                writer.put(&cut.epoch.to_be_bytes())?;
                writer.put(&cut.sequence.to_be_bytes())?;
            }
            DeltaIdentity::Local(id) => {
                writer.put(&[2])?;
                writer.blob(id, limits.max_identity_bytes)?;
            }
        }
        if delta.effect.len() > limits.max_effect_bytes {
            return Err(WireError::Capacity);
        }
        writer.put(
            &u32::try_from(delta.effect.len())
                .map_err(|_| WireError::Capacity)?
                .to_be_bytes(),
        )?;
        writer.put(&delta.effect)?;
    }
    writer.number(batch.bytes)
}

fn read_batch(reader: &mut Reader<'_>, limits: PhaseLimits) -> Result<JournalBatch, WireError> {
    let reservation = read_reservation(reader, limits)?;
    let operation = u64::from_be_bytes(reader.number()?);
    let from = read_cursor(reader, limits)?;
    let through = read_cursor(reader, limits)?;
    let count = usize::from(u16::from_be_bytes(reader.number()?));
    if count > limits.max_events {
        return Err(WireError::Capacity);
    }
    let mut deltas = Vec::new();
    deltas
        .try_reserve_exact(count)
        .map_err(|_| WireError::Capacity)?;
    for _ in 0..count {
        let position = u64::from_be_bytes(reader.number()?);
        let identity = match reader.number::<1>()?[0] {
            1 => DeltaIdentity::Native(NativeCut {
                writer: read_blob(reader, limits.max_writer_bytes)?,
                epoch: u64::from_be_bytes(reader.number()?),
                sequence: u64::from_be_bytes(reader.number()?),
            }),
            2 => DeltaIdentity::Local(read_blob(reader, limits.max_identity_bytes)?),
            _ => return Err(WireError::Invalid),
        };
        let effect_len = usize::try_from(u32::from_be_bytes(reader.number()?))
            .map_err(|_| WireError::Capacity)?;
        if effect_len > limits.max_effect_bytes {
            return Err(WireError::Capacity);
        }
        let effect_slice = reader.take(effect_len)?;
        let mut effect = Vec::new();
        effect
            .try_reserve_exact(effect_len)
            .map_err(|_| WireError::Capacity)?;
        effect.extend_from_slice(effect_slice);
        deltas.push(JournalDelta {
            position,
            identity,
            effect,
        });
    }
    Ok(JournalBatch {
        reservation,
        operation,
        from,
        through,
        deltas,
        bytes: read_size(reader)?,
    })
}

/// Encodes exactly one admitted donor reply as a correlated data frame.
///
/// # Errors
/// Rejects a wrong phase, inconsistent nested identity, or a byte cap.
pub fn encode_reply(
    reply: &DonorReply,
    correlation: Correlation,
    exchange: ExchangeKind,
    limits: PhaseLimits,
) -> Result<Envelope, WireError> {
    if !limits.valid() {
        return Err(WireError::Capacity);
    }
    let mut writer = Writer::new(limits.max_body_bytes);
    match (exchange, reply) {
        (ExchangeKind::Offer, DonorReply::Offer(offer)) => {
            if !valid_offer(offer.get(), &correlation) {
                return Err(WireError::Invalid);
            }
            write_offer(&mut writer, offer.get(), limits)?;
        }
        (ExchangeKind::Reserve, DonorReply::Reserved(value)) => {
            if !valid_reservation(value, &correlation) {
                return Err(WireError::Invalid);
            }
            writer.reservation(value, limits)?;
        }
        (ExchangeKind::Chunk, DonorReply::Chunk(bytes)) => {
            if bytes.get().is_empty() {
                return Err(WireError::Invalid);
            }
            writer.put(bytes.get())?;
        }
        (ExchangeKind::Attach, DonorReply::Attached(value)) => {
            if !valid_reservation(&value.reservation, &correlation) || value.operation == 0 {
                return Err(WireError::Invalid);
            }
            writer.attachment(value, limits)?;
        }
        (ExchangeKind::Barrier | ExchangeKind::AdvanceBarrier, DonorReply::Barrier(value)) => {
            if !valid_barrier(value.get())
                || !valid_reservation(&value.get().reservation, &correlation)
            {
                return Err(WireError::Invalid);
            }
            writer.barrier(value.get(), limits)?;
        }
        (ExchangeKind::Batch, DonorReply::Batch(value)) => {
            if !valid_batch(value.get(), &correlation, limits) {
                return Err(WireError::Invalid);
            }
            write_batch(&mut writer, value.get(), limits)?;
        }
        (ExchangeKind::Ack, DonorReply::Acked) | (ExchangeKind::Release, DonorReply::Released) => {}
        _ => return Err(WireError::Invalid),
    }
    Ok(Envelope {
        exchange,
        correlation,
        message: Message::Reply(writer.bytes),
    })
}

/// Decodes one complete typed donor reply under the original request binding.
///
/// # Errors
/// Rejects malformed, excessive, wrong-phase, or inconsistent source data.
pub fn decode_reply(frame: &Envelope, limits: PhaseLimits) -> Result<WireReply, WireError> {
    let Message::Reply(bytes) = &frame.message else {
        return Err(WireError::Invalid);
    };
    if !limits.valid() || bytes.len() > limits.max_body_bytes {
        return Err(WireError::Capacity);
    }
    let mut reader = Reader { bytes, position: 0 };
    let result = match frame.exchange {
        ExchangeKind::Offer => WireReply::Offer(read_offer(&mut reader, limits)?),
        ExchangeKind::Reserve => WireReply::Reserved(read_reservation(&mut reader, limits)?),
        ExchangeKind::Chunk => {
            if bytes.is_empty() {
                return Err(WireError::Invalid);
            }
            reader.position = bytes.len();
            let mut owned = Vec::new();
            owned
                .try_reserve_exact(bytes.len())
                .map_err(|_| WireError::Capacity)?;
            owned.extend_from_slice(bytes);
            WireReply::Chunk(owned)
        }
        ExchangeKind::Attach => WireReply::Attached(super::read_attachment(&mut reader, limits)?),
        ExchangeKind::Barrier | ExchangeKind::AdvanceBarrier => {
            WireReply::Barrier(read_barrier(&mut reader, limits)?)
        }
        ExchangeKind::Batch => WireReply::Batch(read_batch(&mut reader, limits)?),
        ExchangeKind::Ack => WireReply::Acked,
        ExchangeKind::Release => WireReply::Released,
    };
    if reader.position != bytes.len() {
        return Err(WireError::Invalid);
    }
    match &result {
        WireReply::Offer(value) if !valid_offer(value, &frame.correlation) => {
            return Err(WireError::Invalid);
        }
        WireReply::Reserved(value) if !valid_reservation(value, &frame.correlation) => {
            return Err(WireError::Invalid);
        }
        WireReply::Attached(value)
            if !valid_reservation(&value.reservation, &frame.correlation)
                || value.operation == 0 =>
        {
            return Err(WireError::Invalid);
        }
        WireReply::Barrier(value)
            if !valid_barrier(value)
                || !valid_reservation(&value.reservation, &frame.correlation) =>
        {
            return Err(WireError::Invalid);
        }
        WireReply::Batch(value) if !valid_batch(value, &frame.correlation, limits) => {
            return Err(WireError::Invalid);
        }
        _ => {}
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::volatile_recovery::bootstrap::admission::{
        AdmissionClass, AdmissionLimits, Admitted, ByteAdmission,
    };
    use groupnet_core::NodeId;
    use groupnet_core::volatile_bootstrap::journal::{CaptureId, JournalCursor};
    use groupnet_core::volatile_bootstrap::{
        BootId, BootstrapOperation, BootstrapScope, ClaimIdentity,
    };

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

    fn admitted<T>(value: T) -> Admitted<T> {
        ByteAdmission::new(AdmissionLimits {
            max_total_bytes: 20_000,
            max_encoded_bytes: 20_000,
            max_decoded_bytes: 20_000,
            max_suffix_bytes: 20_000,
            max_native_overlap_bytes: 20_000,
            max_inflight_bytes: 20_000,
            max_reservations: 4,
        })
        .unwrap()
        .reserve(AdmissionClass::Inflight, 4096)
        .unwrap()
        .hold(value)
    }

    fn fixture() -> (Correlation, CaptureId, ReservationId, BarrierReceipt) {
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
                position: 1,
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
    fn all_nine_typed_reply_bodies_round_trip() {
        let (correlation, capture, reservation, barrier) = fixture();
        let offer = TransferOffer {
            capture: capture.clone(),
            image_cut: JournalCursor {
                capture: capture.clone(),
                position: 0,
            },
            schema: 1,
            encoded_bytes: 3,
            decoded_bytes: 3,
            chunks: 1,
            commitment: [7; 32],
            members: barrier.members.clone(),
            cuts: barrier.covered_cuts.clone(),
        };
        let batch = JournalBatch {
            reservation: reservation.clone(),
            operation: 8,
            from: JournalCursor {
                capture: capture.clone(),
                position: 0,
            },
            through: JournalCursor {
                capture,
                position: 1,
            },
            deltas: vec![JournalDelta {
                position: 1,
                identity: DeltaIdentity::Local(b"repair".to_vec()),
                effect: vec![1],
            }],
            bytes: 7,
        };
        let replies = [
            (
                ExchangeKind::Offer,
                DonorReply::Offer(admitted(offer.clone())),
                WireReply::Offer(offer),
            ),
            (
                ExchangeKind::Reserve,
                DonorReply::Reserved(reservation.clone()),
                WireReply::Reserved(reservation.clone()),
            ),
            (
                ExchangeKind::Chunk,
                DonorReply::Chunk(admitted(vec![1, 2, 3])),
                WireReply::Chunk(vec![1, 2, 3]),
            ),
            (
                ExchangeKind::Attach,
                DonorReply::Attached(AttachToken {
                    reservation: reservation.clone(),
                    operation: 5,
                }),
                WireReply::Attached(AttachToken {
                    reservation: reservation.clone(),
                    operation: 5,
                }),
            ),
            (
                ExchangeKind::Barrier,
                DonorReply::Barrier(admitted(barrier.clone())),
                WireReply::Barrier(barrier.clone()),
            ),
            (
                ExchangeKind::AdvanceBarrier,
                DonorReply::Barrier(admitted(barrier.clone())),
                WireReply::Barrier(barrier),
            ),
            (
                ExchangeKind::Batch,
                DonorReply::Batch(admitted(batch.clone())),
                WireReply::Batch(batch),
            ),
            (ExchangeKind::Ack, DonorReply::Acked, WireReply::Acked),
            (
                ExchangeKind::Release,
                DonorReply::Released,
                WireReply::Released,
            ),
        ];
        for (kind, reply, expected) in replies {
            let frame = encode_reply(&reply, correlation.clone(), kind, limits()).unwrap();
            assert_eq!(decode_reply(&frame, limits()).unwrap(), expected);
        }
    }

    #[test]
    fn wrong_capture_and_noncontiguous_batch_are_rejected() {
        let (correlation, _capture, reservation, _barrier) = fixture();
        let mut stale = reservation.clone();
        stale.capture.serial += 1;
        stale.capture.scope.partition = "other".into();
        assert_eq!(
            encode_reply(
                &DonorReply::Reserved(stale),
                correlation.clone(),
                ExchangeKind::Reserve,
                limits()
            ),
            Err(WireError::Invalid)
        );
        let mut wrong = JournalBatch {
            reservation: reservation.clone(),
            operation: 8,
            from: JournalCursor {
                capture: reservation.capture.clone(),
                position: 0,
            },
            through: JournalCursor {
                capture: reservation.capture.clone(),
                position: 2,
            },
            deltas: vec![JournalDelta {
                position: 2,
                identity: DeltaIdentity::Local(b"repair".to_vec()),
                effect: vec![],
            }],
            bytes: 6,
        };
        assert_eq!(
            encode_reply(
                &DonorReply::Batch(admitted(wrong.clone())),
                correlation.clone(),
                ExchangeKind::Batch,
                limits()
            ),
            Err(WireError::Invalid)
        );
        wrong.deltas[0].position = 1;
        // Through still claims a second effect; correcting one position is not enough.
        assert_eq!(
            encode_reply(
                &DonorReply::Batch(admitted(wrong)),
                correlation,
                ExchangeKind::Batch,
                limits()
            ),
            Err(WireError::Invalid)
        );
    }
}
