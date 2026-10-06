//! Adapter worker for the pure codec/receipt state, with bounded reservations.

use super::{Delivery, Frame, Inner, Receipt, Record, error};
use crate::codec::{self, MessageId, Outcome, Packet, ReceiptState, Rejection};
use bytes::Buf;
use groupnet_core::{GroupId, NodeId};
use groupnet_network::{ApplicationPacket, ProtocolIo};
use ring::digest;
use std::{
    io,
    sync::{Arc, Weak},
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub(super) async fn drive(owner: Weak<Inner>, receive: ProtocolIo, cancel: CancellationToken) {
    loop {
        let packet = tokio::select! {
            biased;
            () = cancel.cancelled() => break,
            packet = receive.recv() => match packet { Ok(packet) => packet, Err(_) => break },
        };
        // Never retain a public owner's Arc across an await.
        let Some(inner) = owner.upgrade() else {
            break;
        };
        process(&inner, packet);
    }
    if let Some(inner) = owner.upgrade()
        && let Ok(mut state) = inner.state.lock()
    {
        state.pending.clear();
        state.received.clear();
    }
    receive.shutdown();
}

fn rejection(reason: Rejection) -> io::Error {
    let kind = match reason {
        Rejection::Full => io::ErrorKind::WouldBlock,
        Rejection::Closed => io::ErrorKind::NotConnected,
        Rejection::Permission => io::ErrorKind::PermissionDenied,
        Rejection::Invalid => io::ErrorKind::InvalidInput,
        Rejection::Interrupted => io::ErrorKind::Interrupted,
        Rejection::Other => io::ErrorKind::Other,
    };
    error(kind, "application receiver rejected frame")
}

fn acknowledge(inner: &Inner, from: &NodeId, id: MessageId, outcome: Outcome) {
    let Ok(mut state) = inner.state.lock() else {
        return;
    };
    let Some(pending) = state.pending.get(&id) else {
        return;
    };
    // Raw origin is trusted-fabric attribution, but must still match
    // the exact selected destination and application identity.
    if &pending.to != from || !codec::completes(pending.delivery, outcome) {
        return;
    }
    if let Some(pending) = state.pending.remove(&id) {
        let result = match outcome {
            Outcome::Accepted | Outcome::Applied => Ok(()),
            Outcome::Rejected(reason) => Err(rejection(reason)),
        };
        let _ = pending.result.send(result);
    }
}

pub(super) fn process(inner: &Arc<Inner>, packet: ApplicationPacket) {
    if inner.cancel.is_cancelled() {
        return;
    }
    let Ok(decoded) = codec::decode(&packet.payload) else {
        return;
    };
    match decoded {
        Packet::Ack { id, outcome } => acknowledge(inner, &packet.from, id, outcome),
        Packet::Data {
            id,
            delivery,
            retry_horizon_ms,
            group,
            payload,
        } => {
            // A peer may not extend this receiver's finite retry/retention bound.
            // Fail closed without queueing or acknowledging application work.
            if payload.len() > inner.config.max_payload || retry_horizon_ms > inner.max_timeout_ms {
                return;
            }
            let offset = packet.payload.len() - payload.len();
            let fingerprint: [u8; 32] = if delivery == Delivery::BestEffort {
                [0; 32]
            } else {
                digest::digest(&digest::SHA256, payload)
                    .as_ref()
                    .try_into()
                    .expect("SHA256 digest length")
            };
            let record = if delivery == Delivery::BestEffort {
                Arc::new(Record {
                    from: packet.from.clone(),
                    id,
                    delivery,
                    retry_horizon_ms,
                    group: None,
                    fingerprint,
                    state: std::sync::Mutex::new(ReceiptState::new(
                        delivery,
                        inner.now(),
                        inner.retention_ms,
                    )),
                })
            } else {
                let key = (packet.from.clone(), id);
                let Ok(mut state) = inner.state.lock() else {
                    return;
                };
                let now = inner.now();
                state.received.retain(|_, record| {
                    record.state.lock().map_or(true, |mut receipt| {
                        receipt.retire(now);
                        !receipt.expired(now)
                    })
                });
                if let Some(record) = state.received.get(&key) {
                    // Same identity with a different body is malformed, not a
                    // new operation and not eligible for a success receipt.
                    if record.delivery != delivery
                        || record.retry_horizon_ms != retry_horizon_ms
                        || record.group != group
                        || record.fingerprint != fingerprint
                    {
                        return;
                    }
                    let outcome = record
                        .state
                        .lock()
                        .ok()
                        .and_then(|mut receipt| receipt.replay(now));
                    drop(state);
                    if let Some(outcome) = outcome {
                        let _ = inner.acknowledge(&packet.from, id, outcome);
                    }
                    return;
                }
                // Never evict a live identity to admit more work. Without room
                // for a rejection record, fail closed rather than emit a
                // terminal receipt that could later be contradicted by a retry.
                if state.received.len() >= inner.config.received_records {
                    return;
                }
                let record = Arc::new(Record {
                    from: packet.from.clone(),
                    id,
                    delivery,
                    retry_horizon_ms,
                    group: group.clone(),
                    fingerprint,
                    state: std::sync::Mutex::new(ReceiptState::new(
                        delivery,
                        now,
                        inner.retention_ms,
                    )),
                });
                state.received.insert(key, record.clone());
                record
            };
            enqueue(inner, packet, group, id, record, offset);
        }
    }
}

fn enqueue(
    inner: &Arc<Inner>,
    mut packet: ApplicationPacket,
    group: Option<GroupId>,
    id: MessageId,
    record: Arc<Record>,
    offset: usize,
) {
    packet.payload.advance(offset);
    let frame = Frame {
        id,
        from: packet.from,
        group,
        payload: packet.payload,
        receipt: Receipt {
            owner: Arc::downgrade(inner),
            record,
        },
    };
    if let Err(error) = inner.incoming.try_send(frame) {
        let kind = if matches!(&error, mpsc::error::TrySendError::Full(_)) {
            io::ErrorKind::WouldBlock
        } else {
            io::ErrorKind::NotConnected
        };
        let _ = error.into_inner().receipt.reject(kind);
    }
}
