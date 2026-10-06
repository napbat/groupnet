//! Sliding-window reliability with bounded receive credit and additive-increase /
//! multiplicative-decrease congestion control. Idle authenticated streams remain
//! alive through bounded outer heartbeats; a blackholed peer expires independently
//! of application activity. No plaintext is visible to this layer.

use std::{
    collections::{BTreeMap, VecDeque},
    time::Duration,
};

use groupnet_core::NodeId;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, DuplexStream},
    sync::mpsc,
    time::{Instant, interval},
};
use tokio_util::sync::CancellationToken;

use super::wire::{HEADER, Kind, PAYLOAD, Packet, SessionId, WINDOW};
use crate::Router;

const INITIAL_RTO: Duration = Duration::from_millis(150);
const MAX_RTO: Duration = Duration::from_secs(2);
const PEER_TIMEOUT: Duration = Duration::from_secs(20);

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
    pub raw: DuplexStream,
    pub packets: mpsc::Receiver<Packet>,
    pub cancel: CancellationToken,
    pub sent: CancellationToken,
}

#[derive(Debug)]
struct Segment {
    kind: Kind,
    payload: Vec<u8>,
    start: usize,
    sampled_at: Option<Instant>,
}

#[derive(Debug)]
struct Reliability {
    id: SessionId,
    readiness: Readiness,
    next_send: u64,
    next_receive: u64,
    outstanding: BTreeMap<u64, Segment>,
    reordered: BTreeMap<u64, Segment>,
    delivery: VecDeque<Segment>,
    offset: usize,
    credit: usize,
    congestion: usize,
    increase: usize,
    duplicate_acks: u8,
    local_fin: bool,
    remote_fin: bool,
    last_peer: Instant,
    last_send: Instant,
    retransmit_at: Instant,
    rto: Duration,
    smoothed_rtt: Duration,
    rtt_variation: Duration,
    encoded: Vec<u8>,
}

impl Reliability {
    fn new(id: SessionId, role: Role) -> Self {
        let now = Instant::now();
        Self {
            id,
            readiness: if role == Role::Responder {
                Readiness::Established
            } else {
                Readiness::Opening
            },
            next_send: 0,
            next_receive: 0,
            outstanding: BTreeMap::new(),
            reordered: BTreeMap::new(),
            delivery: VecDeque::new(),
            offset: 0,
            credit: WINDOW,
            congestion: 4,
            increase: 0,
            duplicate_acks: 0,
            local_fin: false,
            remote_fin: false,
            last_peer: now,
            last_send: now,
            retransmit_at: now + INITIAL_RTO,
            rto: INITIAL_RTO,
            smoothed_rtt: Duration::from_millis(50),
            rtt_variation: Duration::from_millis(25),
            encoded: Vec::with_capacity(PAYLOAD + HEADER),
        }
    }

    fn window(&self) -> u16 {
        u16::try_from(WINDOW - self.reordered.len() - self.delivery.len())
            .expect("receive queues are bounded by WINDOW")
    }

    fn send(&mut self, router: &Router, peer: &NodeId, kind: Kind, sequence: u64, payload: &[u8]) {
        Packet::encode(
            self.id,
            kind,
            sequence,
            self.next_receive,
            self.window(),
            payload,
            &mut self.encoded,
        );
        // Queue saturation and temporarily missing routes are packet loss. The
        // retransmission timer retries the same ciphertext on the current route.
        let _ = router.send_tunnel(peer, &self.encoded);
        self.last_send = Instant::now();
    }

    fn control(&mut self, router: &Router, peer: &NodeId, kind: Kind) {
        self.send(router, peer, kind, 0, &[]);
    }

    fn transmit(&mut self, router: &Router, peer: &NodeId, kind: Kind, payload: Vec<u8>) {
        let sequence = self.next_send;
        self.next_send += 1;
        self.send(router, peer, kind, sequence, &payload);
        if self.outstanding.is_empty() {
            self.retransmit_at = Instant::now() + self.rto;
        }
        self.outstanding.insert(
            sequence,
            Segment {
                kind,
                payload,
                start: 0,
                sampled_at: Some(Instant::now()),
            },
        );
    }

    fn receive(&mut self, packet: Packet, router: &Router, peer: &NodeId) -> bool {
        if packet.ack > self.next_send {
            return true;
        }
        self.last_peer = Instant::now();
        self.readiness = Readiness::Established;
        self.credit = usize::from(packet.window);
        let before = self.outstanding.len();
        // Karn's rule: never sample retransmitted ciphertext, because the ACK
        // cannot distinguish its first transmission from a later retry.
        let sample = self
            .outstanding
            .range(..packet.ack)
            .find_map(|(_, segment)| segment.sampled_at)
            .map(|sent| Instant::now().duration_since(sent));
        self.outstanding
            .retain(|sequence, _| *sequence >= packet.ack);
        let acknowledged = before - self.outstanding.len();
        if acknowledged != 0 {
            self.duplicate_acks = 0;
            if let Some(sample) = sample {
                let deviation = self.smoothed_rtt.abs_diff(sample);
                self.rtt_variation = (self.rtt_variation * 3 + deviation) / 4;
                self.smoothed_rtt = (self.smoothed_rtt * 7 + sample) / 8;
            }
            self.rto = (self.smoothed_rtt + self.rtt_variation * 4)
                .clamp(Duration::from_millis(25), MAX_RTO);
            self.retransmit_at = Instant::now() + self.rto;
            self.increase += acknowledged;
            if self.increase >= self.congestion {
                self.increase = 0;
                self.congestion = (self.congestion + 1).min(WINDOW);
            }
        } else if packet.kind == Kind::Ack
            && self
                .outstanding
                .first_key_value()
                .is_some_and(|(sequence, _)| *sequence == packet.ack)
        {
            self.duplicate_acks = self.duplicate_acks.saturating_add(1);
            if self.duplicate_acks == 3 {
                // Later packets arrived but the cumulative ACK still identifies
                // a gap. Repair it once before falling back to the RTO.
                self.congestion = (self.congestion / 2).max(1);
                self.increase = 0;
                self.retransmit(router, peer, 1);
                self.retransmit_at = Instant::now() + self.rto;
            }
        }
        match packet.kind {
            Kind::Reset => return false,
            Kind::Open => self.control(router, peer, Kind::Ack),
            Kind::Ack => {}
            Kind::Data | Kind::Fin => {
                if !self.remote_fin
                    && packet.sequence >= self.next_receive
                    && packet.sequence - self.next_receive
                        < u64::try_from(WINDOW).expect("small constant")
                    && self.reordered.len() + self.delivery.len() < WINDOW
                {
                    self.reordered.entry(packet.sequence).or_insert(Segment {
                        kind: packet.kind,
                        payload: packet.encoded,
                        start: HEADER,
                        sampled_at: None,
                    });
                    while let Some(segment) = self.reordered.remove(&self.next_receive) {
                        self.next_receive += 1;
                        let finished = segment.kind == Kind::Fin;
                        self.delivery.push_back(segment);
                        if finished {
                            // FIN consumes a sequence number and terminates only
                            // this direction, never the reciprocal stream.
                            self.remote_fin = true;
                            self.reordered.clear();
                            break;
                        }
                    }
                }
                self.control(router, peer, Kind::Ack);
            }
        }
        true
    }

    fn retransmit(&mut self, router: &Router, peer: &NodeId, limit: usize) {
        let window = self.window();
        for (sequence, segment) in self.outstanding.iter_mut().take(limit) {
            segment.sampled_at = None;
            Packet::encode(
                self.id,
                segment.kind,
                *sequence,
                self.next_receive,
                window,
                &segment.payload,
                &mut self.encoded,
            );
            let _ = router.send_tunnel(peer, &self.encoded);
        }
        self.last_send = Instant::now();
    }

    fn tick(&mut self, router: &Router, peer: &NodeId) -> bool {
        let now = Instant::now();
        if now.duration_since(self.last_peer) >= PEER_TIMEOUT {
            return false;
        }
        if self.readiness == Readiness::Opening && now >= self.retransmit_at {
            self.control(router, peer, Kind::Open);
            self.retransmit_at = now + self.rto;
            self.rto = (self.rto * 2).min(MAX_RTO);
        } else if !self.outstanding.is_empty() && now >= self.retransmit_at {
            self.congestion = (self.congestion / 2).max(1);
            self.increase = 0;
            self.retransmit(router, peer, self.congestion);
            self.rto = (self.rto * 2).min(MAX_RTO);
            self.retransmit_at = now + self.rto;
        }
        if now.duration_since(self.last_send) >= Duration::from_secs(1)
            && self.readiness == Readiness::Established
        {
            self.control(router, peer, Kind::Ack);
        }
        true
    }
}

enum DeliveryProgress {
    Written(usize),
    Finished,
}

pub(super) async fn run(router: Router, peer: NodeId, id: SessionId, role: Role, io: SessionIo) {
    let SessionIo {
        raw,
        mut packets,
        cancel,
        sent,
    } = io;
    let mut state = Reliability::new(id, role);
    let (mut read, mut write) = tokio::io::split(raw);
    let mut timer = interval(Duration::from_millis(25));
    let mut buffer = vec![0; PAYLOAD];
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
        let can_read = state.readiness == Readiness::Established
            && !state.local_fin
            && state.outstanding.len() < state.congestion.min(state.credit);
        let delivery = state.delivery.front();
        let data = delivery
            .filter(|segment| segment.kind == Kind::Data)
            .map_or(&[][..], |segment| {
                &segment.payload[segment.start + state.offset..]
            });
        let finish = delivery.is_some_and(|segment| segment.kind == Kind::Fin);
        tokio::select! {
            () = cancel.cancelled() => break,
            packet = packets.recv() => {
                let Some(packet) = packet else { break; };
                if !state.receive(packet, &router, &peer) { break; }
            },
            result = async {
                if finish {
                    write.shutdown().await.map(|()| DeliveryProgress::Finished)
                } else {
                    write.write(data).await.map(DeliveryProgress::Written)
                }
            }, if finish || !data.is_empty() => {
                match result {
                    Ok(DeliveryProgress::Finished) => {
                        state.delivery.pop_front();
                        state.control(&router, &peer, Kind::Ack);
                    }
                    Ok(DeliveryProgress::Written(0)) | Err(_) => break,
                    Ok(DeliveryProgress::Written(written)) => {
                        state.offset += written;
                        if state.delivery.front().is_some_and(|segment| segment.start + state.offset == segment.payload.len()) {
                            state.delivery.pop_front();
                            state.offset = 0;
                            state.control(&router, &peer, Kind::Ack);
                        }
                    }
                }
            },
            result = read.read(&mut buffer), if can_read => {
                let Ok(length) = result else { break; };
                if length == 0 {
                    state.local_fin = true;
                    state.transmit(&router, &peer, Kind::Fin, Vec::new());
                } else {
                    let mut payload = std::mem::replace(&mut buffer, vec![0; PAYLOAD]);
                    payload.truncate(length);
                    state.transmit(&router, &peer, Kind::Data, payload);
                }
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
