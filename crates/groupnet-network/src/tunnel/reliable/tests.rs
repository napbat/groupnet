//! Congestion, credit, memory and timeout transitions on a paused clock.

use std::{
    num::{NonZeroU32, NonZeroUsize},
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;
use groupnet_core::NodeId;
use tokio::time::advance;

use super::{
    Reliability, Response, Role,
    receive::{CREDIT_GROWTH, Receiver},
};
use crate::{
    PacketBuffer, Router, RouterConfig,
    tunnel::{
        TunnelLimits,
        budget::MemoryBudget,
        wire::{Fields, HEADER, Kind, Packet, SessionId, cost},
    },
};

const ID: SessionId = SessionId([7; 16]);
const FLOOR: usize = 64 * 1024;
const WINDOW: usize = 1 << 20;

fn limits() -> TunnelLimits {
    TunnelLimits {
        max_sessions: NonZeroUsize::new(2).unwrap(),
        sessions_per_peer: NonZeroUsize::new(2).unwrap(),
        min_window: NonZeroU32::new(u32::try_from(FLOOR).unwrap()).unwrap(),
        max_window: NonZeroU32::new(u32::try_from(WINDOW).unwrap()).unwrap(),
        memory_budget: NonZeroUsize::new(4 << 20).unwrap(),
        ..TunnelLimits::default()
    }
}

fn data(sequence: u64, length: usize) -> Packet {
    let mut bytes = vec![1; cost(length)];
    Packet::stamp(
        Fields {
            id: ID,
            kind: Kind::Data,
            sequence,
            ack: 0,
            credit: 0,
        },
        &mut bytes,
    );
    Packet::decode(Bytes::from(bytes)).unwrap()
}

fn control(kind: Kind, sequence: u64, ack: u64, credit: usize) -> Packet {
    Packet {
        id: ID,
        kind,
        sequence,
        ack,
        credit: u32::try_from(credit).unwrap(),
        encoded: Bytes::from_static(&[0; HEADER]),
    }
}

/// One session's reliability state over an unrouted router: transmitted
/// segments stay outstanding and acknowledgements are fed in directly.
struct Harness {
    router: Router,
    peer: NodeId,
    budget: Arc<MemoryBudget>,
    state: Reliability,
    sent: u64,
}

impl Harness {
    fn new() -> Self {
        let limits = limits();
        limits.validate().unwrap();
        let budget = MemoryBudget::new(&limits);
        let (send, receive) = (budget.reserve(FLOOR), budget.reserve(FLOOR));
        Self {
            router: Router::new(NodeId::new("local"), RouterConfig::default()).unwrap(),
            peer: NodeId::new("peer"),
            budget,
            state: Reliability::new(ID, Role::Responder, limits, send, receive),
            sent: 0,
        }
    }

    fn packet(&self, length: usize) -> PacketBuffer {
        let mut packet = self
            .router
            .tunnel_packet_buffer(&self.peer, cost(length))
            .unwrap();
        packet.extend_from_slice(&vec![0; cost(length)]);
        packet
    }

    fn send(&mut self, segments: usize) {
        for _ in 0..segments {
            let packet = self.packet(1);
            self.state
                .transmit(&self.router, &self.peer, Kind::Data, packet);
            self.sent += 1;
        }
    }

    fn feed(&mut self, packet: Packet) -> Response {
        let response = self.state.receive(packet, &self.router, &self.peer);
        assert_ne!(response, Response::Close);
        response
    }

    fn ack(&mut self, ack: u64, credit: usize) {
        let _ = self.feed(control(Kind::Ack, 0, ack, credit));
    }

    /// Sends a full congestion window and acknowledges all of it.
    fn round_trip(&mut self) {
        self.send(self.state.congestion.window());
        self.ack(self.sent, WINDOW);
    }
}

#[tokio::test(start_paused = true)]
async fn only_in_order_data_coalesces_its_acknowledgement() {
    let mut harness = Harness::new();
    assert_eq!(harness.feed(data(0, 100)), Response::Coalesce);
    assert_eq!(harness.feed(data(1, 100)), Response::Coalesce);
    // A duplicate, a gap and the repair that fills it are acknowledged at once.
    assert_eq!(harness.feed(data(1, 100)), Response::Acknowledge);
    assert_eq!(harness.feed(data(3, 100)), Response::Acknowledge);
    assert_eq!(harness.feed(data(2, 100)), Response::Acknowledge);
    assert_eq!(harness.state.receiver.next(), 4);
    assert_eq!(
        harness.feed(control(Kind::Open, 0, 0, 0)),
        Response::Acknowledge
    );
    assert_eq!(harness.feed(control(Kind::Ack, 0, 0, 0)), Response::Quiet);
    harness.router.close().await;
}

#[tokio::test(start_paused = true)]
async fn delivery_announces_only_material_window_growth() {
    let mut harness = Harness::new();
    let segment = 16 * 1024;
    for sequence in 0..4 {
        let _ = harness.feed(data(sequence, segment));
    }
    // The batch acknowledgement advertises ample credit: reopening one
    // segment's worth is not worth a packet.
    harness
        .state
        .control(&harness.router, &harness.peer, Kind::Ack);
    assert!(harness.state.receiver.delivered(segment));
    assert!(!harness.state.window_update_due());
    // Fill the receive window until the advertised credit cannot carry a
    // maximal segment: the sender may be stalled, so any reopening counts.
    let mut sequence = 4;
    while harness.state.receiver.credit() as usize >= cost(segment) {
        let _ = harness.feed(data(sequence, segment));
        sequence += 1;
    }
    harness
        .state
        .control(&harness.router, &harness.peer, Kind::Ack);
    assert!(!harness.state.window_update_due());
    assert!(harness.state.receiver.delivered(segment));
    assert!(harness.state.window_update_due());
    harness.router.close().await;
}

#[tokio::test(start_paused = true)]
async fn partial_acknowledgements_walk_the_window_without_further_timeouts() {
    let mut harness = Harness::new();
    for _ in 0..2 {
        harness.round_trip();
    }
    assert_eq!(harness.state.congestion.window(), 36);
    let first = harness.sent;
    harness.send(32);
    advance(harness.state.rto).await;
    assert!(harness.state.tick(&harness.router, &harness.peer));
    // The timeout halves the window and retransmits its first eighteen.
    let recovery = harness.state.recovery.unwrap();
    assert_eq!((recovery.until, recovery.cursor), (first + 32, first + 18));
    // Each partial acknowledgement retransmits as many more as it frees.
    harness.ack(first + 4, WINDOW);
    assert_eq!(harness.state.recovery.unwrap().cursor, first + 22);
    // Duplicates of retransmitted data never restart recovery.
    for _ in 0..3 {
        harness.ack(first + 4, WINDOW);
    }
    assert_eq!(harness.state.congestion.window(), 18);
    harness.ack(first + 32, WINDOW);
    assert!(harness.state.recovery.is_none());
    harness.router.close().await;
}

#[tokio::test(start_paused = true)]
async fn slow_start_triples_per_round_trip_up_to_the_window() {
    let mut harness = Harness::new();
    assert_eq!(harness.state.congestion.window(), 4);
    for expected in [12, 36, 64, 64] {
        harness.round_trip();
        assert_eq!(harness.state.congestion.window(), expected);
    }
    harness.router.close().await;
}

#[tokio::test(start_paused = true)]
async fn three_duplicate_acknowledgements_halve_into_the_threshold() {
    let mut harness = Harness::new();
    for _ in 0..3 {
        harness.round_trip();
    }
    assert_eq!(harness.state.congestion.window(), 64);
    let first = harness.sent;
    harness.send(32);
    for _ in 0..3 {
        harness.ack(first, WINDOW);
    }
    assert_eq!(
        (
            harness.state.congestion.window(),
            harness.state.congestion.threshold()
        ),
        (32, 32)
    );
    // The acknowledgement ending recovery leaves the halved window.
    harness.ack(harness.sent, WINDOW);
    assert_eq!(harness.state.congestion.window(), 32);
    // Without queueing delay, avoidance grows a used window by a quarter.
    harness.round_trip();
    assert_eq!(harness.state.congestion.window(), 40);
    harness.round_trip();
    assert_eq!(harness.state.congestion.window(), 50);
    harness.router.close().await;
}

#[tokio::test(start_paused = true)]
async fn window_updates_are_not_duplicate_acknowledgements() {
    let mut harness = Harness::new();
    harness.ack(0, 1000);
    harness.send(4);
    // Deliveries reopen credit at the same cumulative ACK: window updates.
    for credit in 1001..=1008 {
        harness.ack(0, credit);
    }
    assert_eq!(harness.state.duplicate_acks, 0);
    // Out-of-order arrivals leave credit unchanged: loss evidence.
    for _ in 0..3 {
        harness.ack(0, 1008);
    }
    assert_eq!(
        (
            harness.state.congestion.window(),
            harness.state.congestion.threshold()
        ),
        (2, 2)
    );
    harness.router.close().await;
}

#[tokio::test(start_paused = true)]
async fn credit_comes_only_from_the_newest_acknowledgement() {
    let mut harness = Harness::new();
    harness.ack(0, 500_000);
    harness.send(2);
    harness.ack(2, 300_000);
    assert_eq!(harness.state.credit, 300_000);
    // A reordered older acknowledgement never extends credit.
    harness.ack(1, 900_000);
    harness.ack(0, 900_000);
    assert_eq!(
        (harness.state.credit, harness.state.credit_ack),
        (300_000, 2)
    );
    // At the newest acknowledgement only a larger credit applies.
    harness.ack(2, 200_000);
    assert_eq!(harness.state.credit, 300_000);
    harness.ack(2, 400_000);
    assert_eq!(harness.state.credit, 400_000);
    harness.router.close().await;
}

#[tokio::test(start_paused = true)]
async fn sends_stay_within_credit_congestion_and_reserved_memory() {
    let mut harness = Harness::new();
    let payload = 16 * 1024;
    assert_eq!(harness.state.read_limit(), 0, "no credit before the peer's");
    harness.ack(0, cost(payload) + cost(100));
    assert_eq!(harness.state.read_limit(), payload);
    let packet = harness.packet(payload);
    harness
        .state
        .transmit(&harness.router, &harness.peer, Kind::Data, packet);
    // 100 bytes of credit remain: no short segment while one is unacknowledged.
    assert_eq!(harness.state.read_limit(), 0);
    harness.ack(1, 100 + HEADER);
    // Nothing unacknowledged: the remaining credit is sent rather than stalled.
    assert_eq!(harness.state.read_limit(), 100);
    harness.ack(1, WINDOW);
    // Slow start: the acknowledged segment opened the window by two, to six.
    for _ in 0..6 {
        assert_eq!(harness.state.read_limit(), payload);
        let packet = harness.packet(payload);
        harness
            .state
            .transmit(&harness.router, &harness.peer, Kind::Data, packet);
    }
    assert_eq!(harness.state.read_limit(), 0, "congestion window of six");
    assert_eq!(harness.state.send_memory.bytes(), 6 * cost(payload));
    harness.ack(7, WINDOW);
    assert_eq!(harness.state.send_memory.bytes(), FLOOR);
    assert_eq!(harness.budget.in_use(), 2 * FLOOR);
    harness.router.close().await;
}

#[tokio::test(start_paused = true)]
async fn idle_sender_relinquishes_credit_until_its_idle_segment_is_acknowledged() {
    let mut harness = Harness::new();
    harness.ack(0, WINDOW);
    harness.send(1);
    harness.ack(1, WINDOW);
    advance(Duration::from_secs(1)).await;
    assert!(harness.state.tick(&harness.router, &harness.peer));
    assert_eq!(harness.state.outstanding.len(), 1, "idle segment sequenced");
    assert_eq!((harness.state.credit, harness.state.credit_ack), (0, 2));
    assert_eq!(harness.state.read_limit(), 0);
    // Credit advertised before the receiver processed `Idle` is ignored.
    harness.ack(1, WINDOW);
    assert_eq!(harness.state.credit, 0);
    harness.ack(2, FLOOR);
    assert_eq!(harness.state.credit, FLOOR);
    assert!(harness.state.outstanding.is_empty());
    // Relinquished once per active period.
    advance(Duration::from_secs(1)).await;
    assert!(harness.state.tick(&harness.router, &harness.peer));
    assert!(harness.state.outstanding.is_empty());
    harness.router.close().await;
}

#[test]
fn receiver_holds_in_credit_data_grows_with_progress_and_releases_on_idle() {
    let limits = limits();
    let budget = MemoryBudget::new(&limits);
    let mut receiver = Receiver::new(budget.reserve(FLOOR), WINDOW);
    assert_eq!(receiver.credit() as usize, FLOOR);
    let segment = 16 * 1024;
    // Out of order within credit: held, credit unchanged, nothing acknowledged.
    receiver.accept(data(1, segment));
    assert_eq!((receiver.next(), receiver.credit() as usize), (0, FLOOR));
    // Beyond credit: discarded.
    assert!(!receiver.accept(data(9, 60_000)));
    receiver.accept(data(0, segment));
    assert_eq!(receiver.next(), 2);
    // Two in-order segments grew the reservation by CREDIT_GROWTH times their
    // cost, then their delivery reopened it all as credit.
    let grown = FLOOR + 2 * CREDIT_GROWTH * cost(segment);
    assert_eq!(receiver.reserved(), grown);
    assert_eq!(receiver.credit() as usize, grown - 2 * cost(segment));
    assert!(receiver.delivered(segment));
    assert!(receiver.delivered(segment));
    assert_eq!(receiver.credit() as usize, grown);
    let mut idle = vec![0; HEADER];
    Packet::stamp(
        Fields {
            id: ID,
            kind: Kind::Idle,
            sequence: 2,
            ack: 0,
            credit: 0,
        },
        &mut idle,
    );
    receiver.accept(Packet::decode(Bytes::from(idle)).unwrap());
    assert_eq!(receiver.next(), 3);
    assert_eq!(receiver.reserved(), FLOOR);
    assert_eq!(receiver.credit() as usize, FLOOR);
    assert_eq!(budget.in_use(), FLOOR);
}

#[tokio::test(start_paused = true)]
async fn retransmission_timeout_halves_into_the_threshold_and_backs_off() {
    let mut harness = Harness::new();
    for _ in 0..2 {
        harness.round_trip();
    }
    assert_eq!(harness.state.congestion.window(), 36);
    harness.send(16);
    let timeout = harness.state.rto;
    assert_eq!(timeout, Duration::from_millis(200));
    advance(timeout).await;
    assert!(harness.state.tick(&harness.router, &harness.peer));
    assert_eq!(
        (
            harness.state.congestion.window(),
            harness.state.congestion.threshold()
        ),
        (18, 18)
    );
    assert_eq!(harness.state.rto, Duration::from_millis(400));
    harness.router.close().await;
}

#[tokio::test(start_paused = true)]
async fn first_sample_initializes_the_timeout_within_configured_bounds() {
    let mut harness = Harness::new();
    assert_eq!(harness.state.rto, Duration::from_secs(1));
    harness.send(1);
    advance(Duration::from_millis(300)).await;
    harness.ack(1, WINDOW);
    // RFC 6298: SRTT = R and RTTVAR = R / 2, so RTO = R + 4 * R / 2.
    assert_eq!(harness.state.rto, Duration::from_millis(900));
    let mut fast = Harness::new();
    fast.send(1);
    advance(Duration::from_millis(10)).await;
    fast.ack(1, WINDOW);
    assert_eq!(fast.state.rto, Duration::from_millis(200));
    harness.router.close().await;
    fast.router.close().await;
}

#[tokio::test(start_paused = true)]
async fn segments_taken_from_tls_go_out_one_batch_per_wake_and_none_is_lost() {
    use tokio::io::AsyncWriteExt;
    let mut harness = Harness::new();
    let segment = 1024;
    let (mut tls, pipe) = crate::tunnel::pipe::pair(
        harness.router.tunnel_buffers(&harness.peer),
        64 * 1024,
        segment,
    );
    tls.write_all(&vec![9; (super::BATCH + 2) * segment])
        .await
        .unwrap();
    harness.state.credit = WINDOW;
    harness.state.congestion = super::congestion::Congestion::new(4 * super::BATCH, 1024);
    let limit = harness.state.read_limit();
    let taken = std::future::poll_fn(|cx| pipe.poll_take(cx, limit))
        .await
        .unwrap();
    assert!(
        harness
            .state
            .send_taken(&harness.router, &harness.peer, &pipe, taken)
    );
    assert_eq!(harness.state.next_send, super::BATCH as u64);
    let limit = harness.state.read_limit();
    let taken = pipe
        .try_take(limit)
        .unwrap()
        .expect("the rest stays queued");
    assert!(
        harness
            .state
            .send_taken(&harness.router, &harness.peer, &pipe, taken)
    );
    assert_eq!(harness.state.next_send, super::BATCH as u64 + 2);
    assert_eq!(
        harness.state.outstanding_cost,
        (super::BATCH + 2) * cost(segment),
        "every taken segment is retained for retransmission"
    );
    harness.router.close().await;
}
