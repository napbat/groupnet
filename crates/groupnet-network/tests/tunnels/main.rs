//! End-to-end TLS tunnels over real heterogeneous and deliberately impaired routers.

mod failover;
mod fixtures;
mod native;
mod protocols;

use std::{io, sync::atomic::Ordering, time::Duration};

use fixtures::{Fabric, credentials};
use futures_util::io::{AsyncReadExt, AsyncWriteExt};
use groupnet_core::NodeId;
use groupnet_network as network;
use groupnet_network::{
    Router, RouterConfig,
    tunnel::{PeerIdentity, TunnelLimits, TunnelTransport},
};
use groupnet_transport::bulk::BulkTransport;
use tokio::time::{interval, timeout};

const DEADLINE: Duration = Duration::from_secs(30);

fn binary(length: usize) -> Vec<u8> {
    (0..length)
        .map(|offset| u8::try_from((offset * 73 + offset / 17) % 256).unwrap())
        .collect()
}

async fn transfer(lossy: bool, tcp: bool) {
    let fabric = Fabric::new(lossy, tcp).await;
    let (a, c, _) = credentials();
    let (sender, receiver) = fabric.tunnels(a, c);
    let route = fabric.a.route_to(fabric.c.local_id()).unwrap();
    assert_eq!(route.next_hop, *fabric.bridge.local_id());
    let payload = binary(1024 * 1024 + 31);
    let reply = binary(128 * 1024 + 7);
    timeout(DEADLINE, async {
        let (client, server) = tokio::join!(sender.connect(fabric.c.local_id()), receiver.accept());
        let mut client = client.unwrap();
        let (peer, mut server) = server.unwrap();
        assert_eq!(peer, *fabric.a.local_id());
        let send = async {
            client.write_all(&payload).await.unwrap();
            client.close().await.unwrap();
            // Half-close must not truncate the opposite direction.
            let mut response = Vec::new();
            client.read_to_end(&mut response).await.unwrap();
            assert_eq!(response, reply);
        };
        let receive = async {
            let mut uploaded = Vec::new();
            server.read_to_end(&mut uploaded).await.unwrap();
            assert_eq!(uploaded, payload);
            server.write_all(&reply).await.unwrap();
            server.close().await.unwrap();
        };
        tokio::join!(send, receive);
    })
    .await
    .unwrap();
    if lossy {
        assert!(fabric.faults.dropped.load(Ordering::Relaxed) > 0);
        assert!(fabric.faults.reordered.load(Ordering::Relaxed) > 0);
        assert!(fabric.faults.duplicated.load(Ordering::Relaxed) > 0);
    }
    sender.close().await;
    receiver.close().await;
    fabric.close().await;
}

#[tokio::test]
async fn binary_half_close_across_memory_and_tcp_bridge() {
    transfer(false, true).await;
}

#[tokio::test]
async fn retransmission_reordering_and_deduplication_across_multihop_router() {
    transfer(true, false).await;
}

#[tokio::test]
async fn different_receive_windows_exchange_streams_in_both_directions() {
    let fabric = Fabric::new(false, false).await;
    let (a, c, _) = credentials();
    let ap = PeerIdentity::new(fabric.c.local_id().clone(), &c.leaf).unwrap();
    let cp = PeerIdentity::new(fabric.a.local_id().clone(), &a.leaf).unwrap();
    let small = TunnelLimits {
        window: 8,
        setup_timeout: Duration::from_secs(2),
        ..TunnelLimits::default()
    };
    let large = TunnelLimits {
        window: 64,
        ..small.clone()
    };
    let a = TunnelTransport::with_limits(fabric.a.clone(), a.identity, vec![ap], small).unwrap();
    let c = TunnelTransport::with_limits(fabric.c.clone(), c.identity, vec![cp], large).unwrap();
    let payload = binary(16 * 1024);
    for (sender, receiver, target, origin) in [
        (&a, &c, fabric.c.local_id(), fabric.a.local_id()),
        (&c, &a, fabric.a.local_id(), fabric.c.local_id()),
    ] {
        timeout(DEADLINE, async {
            let (outgoing, incoming) = tokio::join!(sender.connect(target), receiver.accept());
            let mut outgoing = outgoing.unwrap();
            let (peer, mut incoming) = incoming.unwrap();
            assert_eq!(&peer, origin);
            let sending = async {
                outgoing.write_all(&payload).await.unwrap();
                outgoing.close().await.unwrap();
            };
            let receiving = async {
                let mut bytes = Vec::new();
                incoming.read_to_end(&mut bytes).await.unwrap();
                assert_eq!(bytes, payload);
                incoming.close().await.unwrap();
            };
            tokio::join!(sending, receiving);
        })
        .await
        .unwrap();
    }
    a.close().await;
    c.close().await;
    fabric.close().await;
}

#[tokio::test]
async fn simultaneous_full_duplex_binary_streams() {
    let fabric = Fabric::new(false, false).await;
    let (a, c, _) = credentials();
    let (sender, receiver) = fabric.tunnels(a, c);
    timeout(DEADLINE, async {
        let (client, server) = tokio::join!(sender.connect(fabric.c.local_id()), receiver.accept());
        let (mut cr, mut cw) = client.unwrap().split();
        let (mut sr, mut sw) = server.unwrap().1.split();
        let first = binary(512 * 1024 + 13);
        let second = binary(768 * 1024 + 9);
        let client_write = async {
            cw.write_all(&first).await.unwrap();
            cw.close().await.unwrap();
        };
        let server_write = async {
            sw.write_all(&second).await.unwrap();
            sw.close().await.unwrap();
        };
        let client_read = async {
            let mut bytes = Vec::new();
            cr.read_to_end(&mut bytes).await.unwrap();
            assert_eq!(bytes, second);
        };
        let server_read = async {
            let mut bytes = Vec::new();
            sr.read_to_end(&mut bytes).await.unwrap();
            assert_eq!(bytes, first);
        };
        tokio::join!(client_write, server_write, client_read, server_read);
    })
    .await
    .unwrap();
    sender.close().await;
    receiver.close().await;
    fabric.close().await;
}

#[tokio::test]
async fn valid_ca_certificate_cannot_impersonate_routing_alias() {
    for wrong_client_pin in [false, true] {
        let fabric = Fabric::new(false, false).await;
        let (a, c, other) = credentials();
        let ap = PeerIdentity::new(
            fabric.c.local_id().clone(),
            if wrong_client_pin {
                &c.leaf
            } else {
                &other.leaf
            },
        )
        .unwrap();
        let cp = PeerIdentity::new(
            fabric.a.local_id().clone(),
            if wrong_client_pin {
                &other.leaf
            } else {
                &a.leaf
            },
        )
        .unwrap();
        let sender = TunnelTransport::new(fabric.a.clone(), a.identity, vec![ap]).unwrap();
        let receiver = TunnelTransport::new(fabric.c.clone(), c.identity, vec![cp]).unwrap();
        assert!(
            timeout(Duration::from_secs(12), sender.connect(fabric.c.local_id()))
                .await
                .unwrap()
                .is_err()
        );
        assert!(
            timeout(Duration::from_millis(100), receiver.accept())
                .await
                .is_err()
        );
        sender.close().await;
        receiver.close().await;
        fabric.close().await;
    }
}

#[tokio::test]
async fn revocation_discards_queued_accepts_and_readmission_never_revives_old_streams() {
    let fabric = Fabric::new(false, false).await;
    let (a, c, _) = credentials();
    let client_pin = PeerIdentity::new(fabric.a.local_id().clone(), &a.leaf).unwrap();
    let (sender, receiver) = fabric.tunnels(a, c);
    let mut old = timeout(DEADLINE, sender.connect(fabric.c.local_id()))
        .await
        .unwrap()
        .unwrap();
    assert!(receiver.revoke_peer(fabric.a.local_id()));
    receiver.admit_peer(client_pin).unwrap();
    let (new, accepted) = timeout(DEADLINE, async {
        tokio::join!(sender.connect(fabric.c.local_id()), receiver.accept())
    })
    .await
    .unwrap();
    let mut new = new.unwrap();
    let mut accepted = accepted.unwrap().1;
    // The queued stream from the previous admission must have been skipped.
    timeout(DEADLINE, async {
        new.write_all(b"new generation").await.unwrap();
        let mut marker = [0; 14];
        accepted.read_exact(&mut marker).await.unwrap();
        assert_eq!(&marker, b"new generation");
    })
    .await
    .unwrap();
    let mut byte = [0];
    assert!(
        timeout(Duration::from_secs(2), old.read(&mut byte))
            .await
            .unwrap()
            .is_err()
    );
    assert!(receiver.revoke_peer(fabric.a.local_id()));
    assert!(accepted.read(&mut byte).await.is_err());
    sender.close().await;
    receiver.close().await;
    fabric.close().await;
}

#[tokio::test]
async fn backpressure_and_close_cancel_blocked_io_and_accept() {
    let fabric = Fabric::new(false, false).await;
    let (a, c, _) = credentials();
    let (sender, receiver) = fabric.tunnels(a, c);
    let (client, server) = timeout(DEADLINE, async {
        tokio::join!(sender.connect(fabric.c.local_id()), receiver.accept())
    })
    .await
    .unwrap();
    let mut client = client.unwrap();
    let mut server = server.unwrap().1;
    let large = vec![0xA5; 16 * 1024 * 1024];
    assert!(
        timeout(Duration::from_millis(150), client.write_all(&large))
            .await
            .is_err()
    );
    sender.close().await;
    assert!(client.write_all(b"cancelled").await.is_err());
    assert!(
        timeout(Duration::from_secs(2), server.read_to_end(&mut Vec::new()))
            .await
            .unwrap()
            .is_err()
    );
    let accept = receiver.accept();
    let shutdown = receiver.close();
    let (accepted, ()) = tokio::join!(accept, shutdown);
    assert!(accepted.is_err());
    fabric.close().await;
}

#[tokio::test]
async fn healthy_idle_session_survives_peer_failure_deadline() {
    let fabric = Fabric::new(false, false).await;
    let (a, c, _) = credentials();
    let (sender, receiver) = fabric.tunnels(a, c);
    let (client, server) = timeout(DEADLINE, async {
        tokio::join!(sender.connect(fabric.c.local_id()), receiver.accept())
    })
    .await
    .unwrap();
    let mut client = client.unwrap();
    let mut server = server.unwrap().1;
    let mut clock = interval(Duration::from_secs(1));
    for _ in 0..23 {
        clock.tick().await;
    }
    timeout(DEADLINE, async {
        client.write_all(b"alive").await.unwrap();
        let mut bytes = [0; 5];
        server.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"alive");
    })
    .await
    .unwrap();
    sender.close().await;
    receiver.close().await;
    fabric.close().await;
}

#[tokio::test]
async fn setup_deadline_and_cancelled_connect_release_native_sessions() {
    let (a, c, _) = credentials();
    let router = Router::new(NodeId::new("isolated"), RouterConfig::default()).unwrap();
    let peer = NodeId::new("unreachable");
    let transport = TunnelTransport::new(
        router.clone(),
        a.identity,
        vec![PeerIdentity::new(peer.clone(), &c.leaf).unwrap()],
    )
    .unwrap();
    for _ in 0..16 {
        assert!(
            timeout(Duration::from_millis(25), transport.connect(&peer))
                .await
                .is_err()
        );
        // Let cancellation and registry cleanup run before the next admission.
        tokio::task::yield_now().await;
    }
    let error = timeout(Duration::from_secs(12), transport.connect(&peer))
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    transport.close().await;
    router.close().await;
}
