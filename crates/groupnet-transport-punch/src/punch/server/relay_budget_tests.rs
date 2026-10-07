//! Admitted relay data is exempt from the fixed control budget; pacing is by bytes.

use std::num::NonZeroU64;

use tokio::time::timeout;

use super::*;

const SETTLE: Duration = Duration::from_secs(1);

fn loopback() -> SocketAddr {
    ([127, 0, 0, 1], 0).into()
}

fn sender(now: Instant, address: SocketAddr, pacing: RelayPacing) -> Entry {
    let mut entry = Entry::new(now, pacing);
    entry.active = Some(Registration {
        address,
        session: [7; 16],
        proof: [9; 16],
        sequence: 1,
        seen: now,
        relay_only: true,
        candidates: Candidates::default(),
    });
    entry
}

fn recipient(now: Instant, address: SocketAddr) -> Entry {
    let mut entry = Entry::new(now, RelayPacing::Backpressure);
    entry.active = Some(Registration {
        address,
        session: [8; 16],
        proof: [10; 16],
        sequence: 1,
        seen: now,
        relay_only: true,
        candidates: Candidates::default(),
    });
    entry
}

fn relay(sequence: u64, target: Session, message: &[u8]) -> Packet<'_> {
    Packet {
        sender: "sender",
        session: [7; 16],
        sequence,
        body: Body::Relay {
            proof: [9; 16],
            peer: "recipient",
            target,
            message,
        },
    }
}

fn heartbeat(sequence: u64) -> Packet<'static> {
    Packet {
        sender: "sender",
        session: [7; 16],
        sequence,
        body: Body::Heartbeat { proof: [9; 16] },
    }
}

async fn delivered(socket: &UdpSocket) -> Vec<u8> {
    let mut bytes = [0; MAX_PACKET];
    let (length, _) = timeout(SETTLE, socket.recv_from(&mut bytes))
        .await
        .expect("relay datagram delivered")
        .unwrap();
    let Body::Delivered { proof, message, .. } =
        wire::decode_mode(&bytes[..length], None).unwrap().body
    else {
        panic!("expected a delivered relay datagram");
    };
    assert_eq!(proof, [10; 16]);
    message.to_vec()
}

#[tokio::test]
async fn admitted_relay_data_never_spends_the_fixed_control_budget() {
    let socket = UdpSocket::bind(loopback()).await.unwrap();
    let source = UdpSocket::bind(loopback()).await.unwrap();
    let target = UdpSocket::bind(loopback()).await.unwrap();
    let address = source.local_addr().unwrap();
    let now = Instant::now();
    let mut entries = HashMap::from([
        (
            "sender".to_owned(),
            sender(now, address, RelayPacing::Backpressure),
        ),
        (
            "recipient".to_owned(),
            recipient(now, target.local_addr().unwrap()),
        ),
    ]);
    // Twice the per-window control budget, all within one window.
    let admitted = 2 * u64::from(PACKETS_PER_INTERVAL);
    for sequence in 2..2 + admitted {
        let payload = sequence.to_be_bytes();
        handle_established(
            &socket,
            None,
            &mut entries,
            relay(sequence, [8; 16], &payload),
            address,
            now,
        )
        .await;
        assert_eq!(delivered(&target).await, payload);
    }
    let mut sequence = 2 + admitted;
    assert_eq!(entries["sender"].rate_count, 0);
    assert_eq!(entries["sender"].active.unwrap().sequence, sequence - 1);
    // Relay toward a stale recipient session is rejected traffic: it spends
    // the control budget and is never delivered.
    handle_established(
        &socket,
        None,
        &mut entries,
        relay(sequence, [99; 16], b"stale"),
        address,
        now,
    )
    .await;
    assert_eq!(entries["sender"].rate_count, 1);
    sequence += 1;
    // Exhaust the control budget; further control traffic is refused.
    while entries["sender"].rate_count < PACKETS_PER_INTERVAL {
        handle_established(
            &socket,
            None,
            &mut entries,
            heartbeat(sequence),
            address,
            now,
        )
        .await;
        sequence += 1;
    }
    handle_established(
        &socket,
        None,
        &mut entries,
        heartbeat(sequence),
        address,
        now,
    )
    .await;
    assert_eq!(entries["sender"].active.unwrap().sequence, sequence - 1);
    // Admitted relay data still flows with the control budget exhausted, and
    // the first datagram the recipient sees after the stale one is this one.
    handle_established(
        &socket,
        None,
        &mut entries,
        relay(sequence, [8; 16], b"after control exhaustion"),
        address,
        now,
    )
    .await;
    assert_eq!(delivered(&target).await, b"after control exhaustion");
    assert_eq!(entries["sender"].active.unwrap().sequence, sequence);
    assert_eq!(entries["sender"].rate_count, PACKETS_PER_INTERVAL);
}

#[test]
fn byte_pacing_drops_over_budget_relay_without_spending_control() {
    let now = Instant::now();
    let address = SocketAddr::from(([127, 0, 0, 1], 1234));
    let pacing = RelayPacing::Bytes {
        bytes_per_second: NonZeroU64::new(1000).unwrap(),
        burst_bytes: NonZeroU64::new(1000).unwrap(),
    };
    let mut entry = sender(now, address, pacing);
    let verified = |entry: &Entry, sequence| {
        entry
            .verify(relay(sequence, [8; 16], b""), address, now)
            .unwrap()
    };
    let registration = verified(&entry, 2);
    assert!(entry.forward(registration, 1000, now));
    assert_eq!(entry.active.unwrap().sequence, 2);
    // Over budget: dropped, and the sequence is not committed.
    let registration = verified(&entry, 3);
    assert!(!entry.forward(registration, 1, now));
    assert_eq!(entry.active.unwrap().sequence, 2);
    assert!(entry.forward(registration, 1, now + Duration::from_millis(1)));
    assert_eq!(entry.active.unwrap().sequence, 3);
    assert_eq!(entry.rate_count, 0);
    // Unpaced relay data is limited by neither packets nor bytes.
    let mut unpaced = sender(now, address, RelayPacing::Backpressure);
    for sequence in 2..2 + 4 * u64::from(PACKETS_PER_INTERVAL) {
        let registration = verified(&unpaced, sequence);
        assert!(unpaced.forward(registration, MAX_PACKET, now));
    }
    assert_eq!(unpaced.rate_count, 0);
}

#[test]
fn relay_pacing_burst_must_hold_one_maximum_relay_message() {
    let mut config = RendezvousConfig::open(loopback());
    config.validate().unwrap();
    let paced = |burst| RelayPacing::Bytes {
        bytes_per_second: NonZeroU64::new(1).unwrap(),
        burst_bytes: NonZeroU64::new(burst).unwrap(),
    };
    let max = u64::try_from(super::super::MAX_MESSAGE).unwrap();
    config.relay_pacing = paced(max - 1);
    assert!(config.validate().is_err());
    config.relay_pacing = paced(max);
    config.validate().unwrap();
}
