//! Congestion-window and retransmission-timeout transitions on a paused clock.

use std::time::Duration;

use bytes::Bytes;
use groupnet_core::NodeId;
use tokio::time::advance;

use super::{Reliability, Role};
use crate::{
    Router, RouterConfig,
    tunnel::{
        TunnelLimits,
        wire::{HEADER, Kind, Packet, SessionId},
    },
};

const ID: SessionId = SessionId([7; 16]);

/// One session's reliability state over an unrouted router: transmitted
/// segments stay outstanding and acknowledgements are fed in directly.
struct Harness {
    router: Router,
    peer: NodeId,
    state: Reliability,
    sent: u64,
}

impl Harness {
    fn new() -> Self {
        Self {
            router: Router::new(NodeId::new("local"), RouterConfig::default()).unwrap(),
            peer: NodeId::new("peer"),
            state: Reliability::new(ID, Role::Responder, TunnelLimits::default()),
            sent: 0,
        }
    }

    fn send(&mut self, segments: usize) {
        for _ in 0..segments {
            let mut packet = self
                .router
                .tunnel_packet_buffer(&self.peer, HEADER + 1)
                .unwrap();
            packet.resize_payload(HEADER + 1);
            self.state
                .transmit(&self.router, &self.peer, Kind::Data, packet);
            self.sent += 1;
        }
    }

    fn ack(&mut self, ack: u64, window: u16) {
        let packet = Packet {
            id: ID,
            kind: Kind::Ack,
            sequence: 0,
            ack,
            window,
            encoded: Bytes::new(),
        };
        assert!(self.state.receive(packet, &self.router, &self.peer));
    }

    /// Sends a full congestion window and acknowledges all of it.
    fn round_trip(&mut self) {
        self.send(self.state.congestion);
        self.ack(self.sent, 64);
    }
}

#[tokio::test(start_paused = true)]
async fn slow_start_doubles_per_round_trip_up_to_the_window() {
    let mut harness = Harness::new();
    assert_eq!(harness.state.congestion, 4);
    for expected in [8, 16, 32, 64, 64] {
        harness.round_trip();
        assert_eq!(harness.state.congestion, expected);
    }
    harness.router.close().await;
}

#[tokio::test(start_paused = true)]
async fn three_duplicate_acknowledgements_halve_into_an_additive_threshold() {
    let mut harness = Harness::new();
    for _ in 0..3 {
        harness.round_trip();
    }
    assert_eq!(harness.state.congestion, 32);
    let first = harness.sent;
    harness.send(32);
    for _ in 0..3 {
        harness.ack(first, 64);
    }
    assert_eq!(
        (harness.state.congestion, harness.state.threshold),
        (16, 16)
    );
    harness.ack(harness.sent, 64);
    assert_eq!(harness.state.congestion, 17);
    harness.round_trip();
    assert_eq!(harness.state.congestion, 18);
    harness.router.close().await;
}

#[tokio::test(start_paused = true)]
async fn window_updates_are_not_duplicate_acknowledgements() {
    let mut harness = Harness::new();
    harness.send(4);
    // Out-of-order arrivals shrink the receiver's credit: loss evidence.
    harness.ack(0, 32);
    // Deliveries reopen credit at the same cumulative ACK: window updates.
    for window in 33..=40 {
        harness.ack(0, window);
    }
    assert_eq!(harness.state.duplicate_acks, 1);
    assert_eq!(harness.state.congestion, 4);
    harness.ack(0, 40);
    harness.ack(0, 39);
    assert_eq!((harness.state.congestion, harness.state.threshold), (2, 2));
    harness.router.close().await;
}

#[tokio::test(start_paused = true)]
async fn retransmission_timeout_halves_into_the_threshold_and_backs_off() {
    let mut harness = Harness::new();
    for _ in 0..2 {
        harness.round_trip();
    }
    assert_eq!(harness.state.congestion, 16);
    harness.send(16);
    let timeout = harness.state.rto;
    assert_eq!(timeout, Duration::from_millis(200));
    advance(timeout).await;
    assert!(harness.state.tick(&harness.router, &harness.peer));
    assert_eq!((harness.state.congestion, harness.state.threshold), (8, 8));
    assert_eq!(harness.state.rto, Duration::from_millis(400));
    harness.router.close().await;
}

#[tokio::test(start_paused = true)]
async fn first_sample_initializes_the_timeout_within_configured_bounds() {
    let mut harness = Harness::new();
    assert_eq!(harness.state.rto, Duration::from_secs(1));
    harness.send(1);
    advance(Duration::from_millis(300)).await;
    harness.ack(1, 64);
    // RFC 6298: SRTT = R and RTTVAR = R / 2, so RTO = R + 4 * R / 2.
    assert_eq!(harness.state.rto, Duration::from_millis(900));
    let mut fast = Harness::new();
    fast.send(1);
    advance(Duration::from_millis(10)).await;
    fast.ack(1, 64);
    assert_eq!(fast.state.rto, Duration::from_millis(200));
    harness.router.close().await;
    fast.router.close().await;
}
