use std::{io, sync::atomic::Ordering, time::Duration};

use bytes::Bytes;
use futures_util::io::{AsyncReadExt, AsyncWriteExt};
use groupnet_testkit::cluster::eventually;
use groupnet_transport::QueueCapacity;
use tokio::time::timeout;

use super::{
    BOUND, Reliable,
    fabric::{Fabric, config},
};
use crate::unordered::wire::{self, Kind, WINDOW};

#[tokio::test(start_paused = true)]
async fn stalled_authenticated_controls_do_not_serialize_other_peers() {
    for waiting_for_ready in [false, true] {
        let mut remote = config();
        remote.max_sessions = QueueCapacity::of(2);
        remote.sessions_per_peer = QueueCapacity::MIN;
        let setup_timeout = remote.setup_timeout;
        let (fabric, malicious) = Fabric::with_stalling_peer(config(), remote).await;
        let started = tokio::time::Instant::now();
        let mut stalled = malicious
            .connect_control(fabric.br.local_id())
            .await
            .unwrap();
        if waiting_for_ready {
            let mut request = [0; 29];
            request[..8].copy_from_slice(b"GNUORD01");
            request[8] = wire::policy(Reliable);
            request[9..25].fill(9);
            request[25..].copy_from_slice(&1024_u32.to_be_bytes());
            stalled.write_all(&request).await.unwrap();
            stalled.flush().await.unwrap();
            let mut reply = [0; 30];
            stalled.read_exact(&mut reply).await.unwrap();
            assert_eq!(reply[29], 0);
        }
        // The first authenticated accept already holds this peer's only slot,
        // even when it has not supplied a single unordered request byte.
        assert_control_rejected(malicious.connect_control(fabric.br.local_id()).await).await;
        let (a, b) = fabric.pair(Reliable).await;
        assert!(started.elapsed() < setup_timeout);
        a.send(Bytes::from_static(b"unrelated peer")).await.unwrap();
        assert_eq!(b.recv().await.unwrap(), b"unrelated peer"[..]);
        // Setup and established sessions consume the same global quota. The
        // successful handshake must transfer, not duplicate or release, its slot.
        assert_control_rejected(fabric.at.connect_control(fabric.br.local_id()).await).await;
        // Expiration is local; allow its reset packet to traverse the routed
        // fabric rather than racing the exact same timer deadline remotely.
        tokio::time::sleep(setup_timeout).await;
        let expired = timeout(BOUND, stalled.read(&mut [0])).await.unwrap();
        assert!(matches!(expired, Ok(0) | Err(_)));
        assert!(started.elapsed() >= setup_timeout);
        a.send(Bytes::from_static(b"after setup expiry"))
            .await
            .unwrap();
        assert_eq!(b.recv().await.unwrap(), b"after setup expiry"[..]);
        drop(stalled);
        malicious.close().await;
        fabric.close().await;
    }
}

async fn assert_control_rejected(result: io::Result<groupnet_network::tunnel::TunneledStream>) {
    match result {
        Ok(mut stream) => {
            assert!(
                timeout(BOUND, stream.read_exact(&mut [0]))
                    .await
                    .unwrap()
                    .is_err()
            );
        }
        // Capacity rejection can reset the authenticated control stream before
        // its initiator has finished the final tunnel preamble exchange.
        Err(error) => assert!(matches!(
            error.kind(),
            io::ErrorKind::ConnectionAborted | io::ErrorKind::UnexpectedEof
        )),
    }
}

#[derive(Clone, Copy)]
enum Resolution {
    Recover,
    Cancel,
    Timeout,
}

async fn exercise_horizon(kind: Kind, resolution: Resolution) {
    let mut bounds = config();
    bounds.pending_sends = QueueCapacity::of(3);
    if matches!(resolution, Resolution::Timeout) {
        bounds.max_attempts = 1;
    }
    let fabric = Fabric::new(bounds).await;
    let (a, b) = fabric.pair(Reliable).await;
    fabric.faults.block(kind as u8, 1);
    let first = tokio::spawn({
        let a = a.clone();
        async move { a.send(Bytes::from_static(b"oldest")).await }
    });
    eventually("selected logical data or ACK withheld", || {
        fabric.faults.blocked.load(Ordering::SeqCst) != 0
    })
    .await;
    if kind == Kind::Ack {
        assert_eq!(b.recv().await.unwrap(), b"oldest"[..]);
    }
    for _ in 2..WINDOW {
        a.send(Bytes::from_static(b"later")).await.unwrap();
        assert_eq!(b.recv().await.unwrap(), b"later"[..]);
    }
    // Two concurrent callers contend for the final ID inside the horizon.
    // Registering that send and advancing the ID must be one atomic operation.
    let (last, overflow) = tokio::join!(
        a.send(Bytes::from_static(b"last in window")),
        a.send(Bytes::from_static(b"beyond window")),
    );
    last.unwrap();
    assert_eq!(overflow.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    assert_eq!(b.recv().await.unwrap(), b"last in window"[..]);
    assert!(!first.is_finished());
    for _ in 0..3 {
        assert_eq!(
            a.send(Bytes::from_static(b"still beyond window"))
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock,
        );
    }
    assert_eq!(last_data_id(&fabric), WINDOW as u64);
    match resolution {
        Resolution::Recover => {
            fabric.faults.block(0, 0);
            first.await.unwrap().unwrap();
            if kind == Kind::Data {
                assert_eq!(b.recv().await.unwrap(), b"oldest"[..]);
            }
            assert!(timeout(Duration::from_millis(1), b.recv()).await.is_err());
        }
        Resolution::Cancel => {
            first.abort();
            assert!(first.await.unwrap_err().is_cancelled());
            fabric.faults.block(0, 0);
        }
        Resolution::Timeout => {
            assert_eq!(
                first.await.unwrap().unwrap_err().kind(),
                io::ErrorKind::TimedOut
            );
            fabric.faults.block(0, 0);
        }
    }
    a.send(Bytes::from_static(b"horizon released"))
        .await
        .unwrap();
    assert_eq!(b.recv().await.unwrap(), b"horizon released"[..]);
    assert_eq!(last_data_id(&fabric), WINDOW as u64 + 1);
    fabric.close().await;
}

fn last_data_id(fabric: &Fabric) -> u64 {
    let captured = fabric
        .faults
        .captured
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    u64::from_be_bytes(captured.as_ref().unwrap()[29..37].try_into().unwrap())
}

#[tokio::test(start_paused = true)]
async fn lost_oldest_data_recovers_at_the_deduplication_horizon() {
    exercise_horizon(Kind::Data, Resolution::Recover).await;
}

#[tokio::test(start_paused = true)]
async fn lost_oldest_ack_is_reacknowledged_at_the_deduplication_horizon() {
    exercise_horizon(Kind::Ack, Resolution::Recover).await;
}

#[tokio::test(start_paused = true)]
async fn cancelled_oldest_send_releases_the_horizon_without_consuming_blocked_ids() {
    exercise_horizon(Kind::Data, Resolution::Cancel).await;
}

#[tokio::test(start_paused = true)]
async fn timed_out_oldest_send_releases_the_horizon_without_consuming_blocked_ids() {
    exercise_horizon(Kind::Data, Resolution::Timeout).await;
}
