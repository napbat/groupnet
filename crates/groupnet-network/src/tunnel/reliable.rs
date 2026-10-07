//! Sliding-window reliability with reserved byte credit and delay-controlled
//! congestion (see [`congestion`]): slow start until loss or queueing delay,
//! then a window held near path capacity. Retained ciphertext in both
//! directions is charged to the transport's memory budget; an idle sender
//! relinquishes its credit so the receiver can release it. Idle authenticated
//! streams remain alive through bounded outer heartbeats; a blackholed peer
//! expires independently of application activity. No plaintext is visible to
//! this layer.

mod congestion;
mod receive;

use std::{collections::BTreeMap, future::poll_fn, io, time::Duration};

use groupnet_core::NodeId;
use tokio::{
    sync::mpsc,
    time::{Instant, interval},
};
use tokio_util::sync::CancellationToken;

use super::{
    TunnelLimits,
    budget::Reservation,
    pipe::{SegmentEnd, Taken},
    wire::{Fields, HEADER, Kind, MAX_SEGMENT, Packet, SessionId, cost},
};
use crate::{PacketBuffer, Router};
use bytes::Bytes;
use congestion::{Acknowledgement, Congestion};
use receive::{Delivery, Receiver};

/// Packets processed, or segments read, per wake: bounds acknowledgement
/// coalescing and keeps a busy direction from starving the other.
const BATCH: usize = 16;

/// What an incoming packet asks of the session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Response {
    /// The peer reset the session.
    Close,
    /// Nothing to send.
    Quiet,
    /// In-order data: one acknowledgement may cover the batch.
    Coalesce,
    /// Acknowledge now.
    Acknowledge,
}

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Role {
    Initiator,
    Responder,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Readiness {
    Opening,
    Established,
}

#[derive(Debug)]
pub(super) struct SessionIo {
    pub pipe: SegmentEnd,
    pub packets: mpsc::Receiver<Packet>,
    pub cancel: CancellationToken,
    pub sent: CancellationToken,
    /// Retention of unacknowledged sent segments.
    pub send_memory: Reservation,
    /// Receive credit: reordered and undelivered segments.
    pub receive_memory: Reservation,
}

#[derive(Debug)]
struct Segment {
    kind: Kind,
    payload: Bytes,
    start: usize,
    sampled_at: Option<Instant>,
}

impl Segment {
    /// Credit and memory this segment is charged.
    fn cost(&self) -> usize {
        cost(self.payload.len() - self.start)
    }
}

/// RFC 6298 round-trip estimate, initialized by the first unambiguous sample.
#[derive(Clone, Copy, Debug)]
struct RoundTrip {
    smoothed: Duration,
    variation: Duration,
}

impl RoundTrip {
    fn first(sample: Duration) -> Self {
        Self {
            smoothed: sample,
            variation: sample / 2,
        }
    }

    fn update(self, sample: Duration) -> Self {
        Self {
            smoothed: (self.smoothed.saturating_mul(7).saturating_add(sample)) / 8,
            variation: (self
                .variation
                .saturating_mul(3)
                .saturating_add(self.smoothed.abs_diff(sample)))
                / 4,
        }
    }

    fn timeout(self) -> Duration {
        self.smoothed
            .saturating_add(self.variation.saturating_mul(4))
    }
}

/// Loss recovery in progress: segments below `until` were sent before the loss
/// was detected; `cursor` is the next one to retransmit.
#[derive(Clone, Copy, Debug)]
struct Recovery {
    until: u64,
    cursor: u64,
}

#[derive(Debug)]
struct Reliability {
    id: SessionId,
    readiness: Readiness,
    next_send: u64,
    outstanding: BTreeMap<u64, Segment>,
    outstanding_cost: usize,
    send_memory: Reservation,
    receiver: Receiver,
    /// Credit carried by the latest packet sent.
    advertised: u32,
    /// Peer credit beyond `credit_ack`, in segment-cost bytes.
    credit: usize,
    credit_ack: u64,
    congestion: Congestion,
    duplicate_acks: u8,
    recovery: Option<Recovery>,
    local_fin: bool,
    /// Data was sent since credit was last relinquished.
    relinquishable: bool,
    last_peer: Instant,
    last_send: Instant,
    last_data: Instant,
    retransmit_at: Instant,
    rto: Duration,
    round_trip: Option<RoundTrip>,
    limits: TunnelLimits,
}

impl Reliability {
    fn new(
        id: SessionId,
        role: Role,
        limits: TunnelLimits,
        send_memory: Reservation,
        receive_memory: Reservation,
    ) -> Self {
        let now = Instant::now();
        Self {
            id,
            readiness: if role == Role::Responder {
                Readiness::Established
            } else {
                Readiness::Opening
            },
            next_send: 0,
            outstanding: BTreeMap::new(),
            outstanding_cost: 0,
            send_memory,
            receiver: Receiver::new(receive_memory, limits.max_window.get() as usize),
            advertised: 0,
            credit: 0,
            credit_ack: 0,
            congestion: Congestion::new(
                usize::from(limits.initial_congestion.get()),
                limits.max_segments(),
            ),
            duplicate_acks: 0,
            recovery: None,
            local_fin: false,
            relinquishable: false,
            last_peer: now,
            last_send: now,
            last_data: now,
            retransmit_at: now + limits.retransmit.initial(),
            rto: limits.retransmit.initial(),
            round_trip: None,
            limits,
        }
    }

    /// Header fields for an outgoing packet, recording the credit it carries.
    fn fields(&mut self, kind: Kind, sequence: u64) -> Fields {
        self.advertised = self.receiver.credit();
        Fields {
            id: self.id,
            kind,
            sequence,
            ack: self.receiver.next(),
            credit: self.advertised,
        }
    }

    /// Whether delivery reopened enough credit to announce: half the receive
    /// reservation, or any reopening after credit fell below one maximal
    /// segment (the sender may be stalled on it).
    fn window_update_due(&self) -> bool {
        let credit = self.receiver.credit() as usize;
        let advertised = self.advertised as usize;
        credit > advertised
            && (advertised < cost(MAX_SEGMENT)
                || credit - advertised >= self.receiver.reserved() / 2)
    }

    /// Ciphertext bytes the next segment may carry, reserving its memory; zero
    /// when congestion, credit or memory admit no full segment (a shorter one
    /// only when nothing is unacknowledged).
    fn read_limit(&mut self) -> usize {
        if self.readiness != Readiness::Established
            || self.local_fin
            || self.outstanding.len() >= self.congestion.window()
        {
            return 0;
        }
        let payload = self.limits.payload.get();
        let credit = self
            .credit
            .saturating_sub(self.outstanding_cost)
            .saturating_sub(HEADER);
        let wanted = payload.min(credit);
        if wanted == 0 {
            return 0;
        }
        let max_window = self.limits.max_window.get() as usize;
        self.send_memory
            .grow_to((self.outstanding_cost + cost(wanted)).min(max_window));
        let limit = wanted.min(
            self.send_memory
                .bytes()
                .saturating_sub(self.outstanding_cost + HEADER),
        );
        if limit == payload || (self.outstanding.is_empty() && limit != 0) {
            limit
        } else {
            0
        }
    }

    fn send(&mut self, router: &Router, peer: &NodeId, fields: Fields, payload: &[u8]) {
        if let Ok(mut encoded) = router.tunnel_packet_buffer(peer, cost(payload.len())) {
            Packet::encode(fields, payload, &mut encoded);
            // Local saturation/missing routes are packet loss. Retain ciphertext
            // for the retransmission timer, never re-encrypt plaintext.
            let _ = router.send_tunnel_packet(peer, encoded);
        }
        self.last_send = Instant::now();
    }

    fn control(&mut self, router: &Router, peer: &NodeId, kind: Kind) {
        let fields = self.fields(kind, 0);
        self.send(router, peer, fields, &[]);
    }

    /// Processes `first` and up to [`BATCH`]` - 1` further ready packets,
    /// acknowledging in-order data once for the whole batch.
    fn receive_batch(
        &mut self,
        first: Packet,
        packets: &mut mpsc::Receiver<Packet>,
        router: &Router,
        peer: &NodeId,
    ) -> bool {
        let mut coalesced = false;
        let mut next = Some(first);
        let mut remaining = BATCH;
        while let Some(packet) = next {
            match self.receive(packet, router, peer) {
                Response::Close => return false,
                Response::Quiet => {}
                Response::Coalesce => coalesced = true,
                Response::Acknowledge => {
                    self.control(router, peer, Kind::Ack);
                    coalesced = false;
                }
            }
            remaining -= 1;
            next = if remaining == 0 {
                None
            } else {
                packets.try_recv().ok()
            };
        }
        if coalesced {
            self.control(router, peer, Kind::Ack);
        }
        true
    }

    fn transmit(&mut self, router: &Router, peer: &NodeId, kind: Kind, mut packet: PacketBuffer) {
        let sequence = self.next_send;
        self.next_send += 1;
        Packet::stamp(self.fields(kind, sequence), packet.payload_mut());
        let Ok(encoded) = router.send_tunnel_retained(peer, packet) else {
            return;
        };
        self.last_send = Instant::now();
        if kind == Kind::Data {
            self.last_data = self.last_send;
            self.relinquishable = true;
        }
        if self.outstanding.is_empty() {
            self.retransmit_at = self.last_send + self.rto;
        }
        let segment = Segment {
            kind,
            payload: encoded.slice(HEADER..),
            start: 0,
            sampled_at: Some(self.last_send),
        };
        self.outstanding_cost += segment.cost();
        self.outstanding.insert(sequence, segment);
    }

    /// Transmits segments taken from TLS, starting with `taken`, while credit,
    /// congestion and memory allow, up to [`BATCH`] per wake; TLS closing its
    /// write direction sends FIN. False if the session must end.
    fn send_taken(
        &mut self,
        router: &Router,
        peer: &NodeId,
        pipe: &SegmentEnd,
        mut taken: Taken,
    ) -> bool {
        for sent in 1..=BATCH {
            match taken {
                Taken::Segment(packet) => self.transmit(router, peer, Kind::Data, packet),
                Taken::Closed => {
                    let Ok(packet) = segment_buffer(router, peer, 0) else {
                        return false;
                    };
                    self.local_fin = true;
                    self.transmit(router, peer, Kind::Fin, packet);
                    return true;
                }
            }
            if sent == BATCH {
                break;
            }
            let limit = self.read_limit();
            if limit == 0 {
                break;
            }
            match pipe.try_take(limit) {
                Ok(Some(next)) => taken = next,
                Ok(None) => break,
                Err(_) => return false,
            }
        }
        true
    }

    /// Records `queued` bytes of the front segment handed to TLS, then hands
    /// over further delivered segments while the pipe has room, up to
    /// [`BATCH`]; announces reopened credit once. False if TLS is gone.
    fn deliver(
        &mut self,
        router: &Router,
        peer: &NodeId,
        pipe: &SegmentEnd,
        queued: usize,
    ) -> bool {
        let mut released = self.receiver.delivered(queued);
        for _ in 1..BATCH {
            let Delivery::Data(data) = self.receiver.delivery() else {
                break;
            };
            match pipe.try_deliver(&data) {
                Ok(0) => break,
                Ok(queued) => released |= self.receiver.delivered(queued),
                Err(_) => return false,
            }
        }
        if released && self.window_update_due() {
            self.control(router, peer, Kind::Ack);
        }
        true
    }

    /// Relinquishes peer credit with a sequenced `Idle`; no credit is taken
    /// again until an acknowledgement covers it.
    fn relinquish(&mut self, router: &Router, peer: &NodeId) {
        let Ok(packet) = segment_buffer(router, peer, 0) else {
            return;
        };
        self.transmit(router, peer, Kind::Idle, packet);
        self.relinquishable = false;
        self.credit = 0;
        self.credit_ack = self.next_send;
    }

    fn receive(&mut self, packet: Packet, router: &Router, peer: &NodeId) -> Response {
        if packet.ack > self.next_send {
            return Response::Quiet;
        }
        self.last_peer = Instant::now();
        self.readiness = Readiness::Established;
        // Credit only from the newest acknowledgement: at one acknowledgement
        // the receiver's right edge only grows, so a larger credit is a window
        // update sent as data is delivered — never evidence of a gap — and a
        // reordered stale packet never extends credit.
        let credit = packet.credit as usize;
        let window_update = packet.ack == self.credit_ack && credit > self.credit;
        if packet.ack > self.credit_ack || window_update {
            self.credit = credit;
            self.credit_ack = packet.ack;
        }
        let remaining = self.outstanding.split_off(&packet.ack);
        let acknowledged = std::mem::replace(&mut self.outstanding, remaining);
        if acknowledged.is_empty() {
            if packet.kind == Kind::Ack
                && !window_update
                && self
                    .outstanding
                    .first_key_value()
                    .is_some_and(|(sequence, _)| *sequence == packet.ack)
            {
                self.duplicate_ack(router, peer);
            }
        } else {
            self.acknowledge(&acknowledged);
            // A partial acknowledgement exposes the next gap.
            self.resend(router, peer, usize::MAX);
        }
        match packet.kind {
            Kind::Reset => Response::Close,
            Kind::Open => Response::Acknowledge,
            Kind::Ack => Response::Quiet,
            Kind::Data | Kind::Fin | Kind::Idle => {
                // Duplicates, gaps and gap repairs are acknowledged at once so
                // the sender's duplicate-ACK repair keeps working.
                if self.receiver.accept(packet) {
                    Response::Coalesce
                } else {
                    Response::Acknowledge
                }
            }
        }
    }

    fn acknowledge(&mut self, acknowledged: &BTreeMap<u64, Segment>) {
        self.duplicate_acks = 0;
        self.outstanding_cost -= acknowledged.values().map(Segment::cost).sum::<usize>();
        self.send_memory.shrink_to(self.outstanding_cost);
        let now = Instant::now();
        // Karn's rule: never sample retransmitted ciphertext, because the ACK
        // cannot distinguish its first transmission from a later retry.
        let sample = acknowledged
            .values()
            .find_map(|segment| segment.sampled_at)
            .map(|sent| now.duration_since(sent));
        if let Some(sample) = sample {
            self.round_trip = Some(
                self.round_trip
                    .map_or_else(|| RoundTrip::first(sample), |rtt| rtt.update(sample)),
            );
        }
        if let Some(round_trip) = self.round_trip {
            self.rto = self.limits.retransmit.clamp(round_trip.timeout());
        }
        self.retransmit_at = now + self.rto;
        let unacknowledged = self.unacknowledged();
        self.congestion.acknowledge(
            Acknowledgement {
                acknowledged: acknowledged.len(),
                sample,
                unacknowledged,
                next_send: self.next_send,
                in_flight: self.outstanding.len(),
                recovering: self.recovery.is_some(),
            },
            now,
        );
        if let Some(recovery) = self.recovery
            && unacknowledged >= recovery.until
        {
            self.recovery = None;
        }
    }

    /// The oldest unacknowledged sequence.
    fn unacknowledged(&self) -> u64 {
        self.outstanding
            .first_key_value()
            .map_or(self.next_send, |(sequence, _)| *sequence)
    }

    fn duplicate_ack(&mut self, router: &Router, peer: &NodeId) {
        if self.recovery.is_some() {
            // Retransmitted duplicates echo the cumulative ACK; recovery is
            // already repairing this window.
            return;
        }
        self.duplicate_acks = self.duplicate_acks.saturating_add(1);
        if self.duplicate_acks == 3 {
            // Later packets arrived but the cumulative ACK still identifies a
            // gap: repair the first segment now, later gaps as partial
            // acknowledgements expose them.
            self.enter_recovery();
            self.resend(router, peer, 1);
            self.retransmit_at = Instant::now() + self.rto;
        }
    }

    /// Halves the window and starts retransmitting from the oldest
    /// unacknowledged segment up to everything sent so far.
    fn enter_recovery(&mut self) {
        self.congestion.reduce();
        self.recovery = Some(Recovery {
            until: self.next_send,
            cursor: self.unacknowledged(),
        });
    }

    /// Retransmits up to `limit` segments from the recovery cursor, keeping the
    /// retransmissions unacknowledged within the congestion window. Without
    /// selective acknowledgements this walks the window at the ACK clock,
    /// repairing every gap in one round trip per window instead of one gap per
    /// timeout.
    fn resend(&mut self, router: &Router, peer: &NodeId, limit: usize) {
        let una = self.unacknowledged();
        let Some(recovery) = &mut self.recovery else {
            return;
        };
        recovery.cursor = recovery.cursor.max(una);
        let in_flight = self.outstanding.range(..recovery.cursor).count();
        let budget = self
            .congestion
            .window()
            .saturating_sub(in_flight)
            .min(limit);
        let from = recovery.cursor;
        if let Some(last) = self.retransmit(router, peer, from, budget)
            && let Some(recovery) = &mut self.recovery
        {
            recovery.cursor = last + 1;
        }
    }

    /// Retransmits up to `limit` outstanding segments from `from`; returns the
    /// last sequence sent.
    fn retransmit(
        &mut self,
        router: &Router,
        peer: &NodeId,
        from: u64,
        limit: usize,
    ) -> Option<u64> {
        let ack = self.receiver.next();
        let credit = self.receiver.credit();
        let mut last = None;
        for (sequence, segment) in self.outstanding.range_mut(from..).take(limit) {
            // Karn's rule: a retransmitted segment is never sampled.
            segment.sampled_at = None;
            last = Some(*sequence);
            if let Ok(mut encoded) = router.tunnel_packet_buffer(peer, cost(segment.payload.len()))
            {
                let fields = Fields {
                    id: self.id,
                    kind: segment.kind,
                    sequence: *sequence,
                    ack,
                    credit,
                };
                Packet::encode(fields, &segment.payload, &mut encoded);
                let _ = router.send_tunnel_packet(peer, encoded);
            }
        }
        if last.is_some() {
            self.advertised = credit;
            self.last_send = Instant::now();
        }
        last
    }

    fn tick(&mut self, router: &Router, peer: &NodeId) -> bool {
        let now = Instant::now();
        if now.duration_since(self.last_peer) >= self.limits.peer_timeout {
            return false;
        }
        if self.readiness == Readiness::Opening && now >= self.retransmit_at {
            self.control(router, peer, Kind::Open);
            self.retransmit_at = now + self.rto;
            self.rto = self.limits.retransmit.back_off(self.rto);
        } else if !self.outstanding.is_empty() && now >= self.retransmit_at {
            // Go back to the oldest unacknowledged segment.
            self.enter_recovery();
            self.resend(router, peer, usize::MAX);
            self.rto = self.limits.retransmit.back_off(self.rto);
            self.retransmit_at = now + self.rto;
        }
        if self.readiness == Readiness::Established
            && !self.local_fin
            && self.relinquishable
            && self.outstanding.is_empty()
            && self.credit >= HEADER
            && now.duration_since(self.last_data) >= self.limits.heartbeat_interval
        {
            self.relinquish(router, peer);
        } else if now.duration_since(self.last_send) >= self.limits.heartbeat_interval
            && self.readiness == Readiness::Established
        {
            self.control(router, peer, Kind::Ack);
        }
        true
    }
}

pub(super) async fn run(
    router: Router,
    peer: NodeId,
    id: SessionId,
    role: Role,
    limits: TunnelLimits,
    io: SessionIo,
) {
    let SessionIo {
        pipe,
        mut packets,
        cancel,
        sent,
        send_memory,
        receive_memory,
    } = io;
    let mut timer = interval(limits.retransmit.min());
    let mut state = Reliability::new(id, role, limits, send_memory, receive_memory);
    state.control(
        &router,
        &peer,
        if role == Role::Responder {
            Kind::Ack
        } else {
            Kind::Open
        },
    );
    loop {
        let limit = state.read_limit();
        let data = match state.receiver.delivery() {
            Delivery::Data(data) => data,
            Delivery::Finish => {
                pipe.finish();
                state.receiver.finish_delivered();
                state.control(&router, &peer, Kind::Ack);
                continue;
            }
            Delivery::Empty => Bytes::new(),
        };
        tokio::select! {
            () = cancel.cancelled() => break,
            packet = packets.recv() => {
                let Some(packet) = packet else { break; };
                if !state.receive_batch(packet, &mut packets, &router, &peer) { break; }
            },
            queued = poll_fn(|cx| pipe.poll_deliver(cx, &data)), if !data.is_empty() => {
                let Ok(queued) = queued else { break; };
                if !state.deliver(&router, &peer, &pipe, queued) { break; }
            },
            taken = poll_fn(|cx| pipe.poll_take(cx, limit)), if limit != 0 => {
                let Ok(taken) = taken else { break; };
                if !state.send_taken(&router, &peer, &pipe, taken) { break; }
            },
            _ = timer.tick() => {
                if !state.tick(&router, &peer) { break; }
            },
        }
        if state.local_fin && state.outstanding.is_empty() {
            sent.cancel();
        }
        // Retain the session after reciprocal half-close: the final ACK may be
        // lost, and the peer must still be able to retransmit its FIN. The owner
        // dropping the public stream or the idle peer deadline releases it.
    }
    if !state.local_fin || !state.outstanding.is_empty() {
        state.control(&router, &peer, Kind::Reset);
    }
    cancel.cancel();
}

/// An empty segment: routing headroom, a header placeholder, and capacity for
/// `ciphertext` bytes.
fn segment_buffer(router: &Router, peer: &NodeId, ciphertext: usize) -> io::Result<PacketBuffer> {
    let mut buffer = router.tunnel_packet_buffer(peer, cost(ciphertext))?;
    buffer.extend_from_slice(&[0; HEADER]);
    Ok(buffer)
}
