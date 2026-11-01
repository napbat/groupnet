//! Raw message-plane endpoint tests.

use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::time::Duration;

use groupnet_core::NodeId;
use groupnet_transport::framing::MAX_FRAME_BYTES;
use groupnet_transport::{Inbound, QueueCapacity, Transport};

use super::{TcpMsgConfig, TcpMsgConfigError, TcpMsgTransport};

/// Bind a loopback endpoint on an ephemeral port under the given id.
async fn bind_as(id: &str) -> TcpMsgTransport {
    TcpMsgTransport::bind(NodeId::new(id), "127.0.0.1:0")
        .await
        .expect("bind")
}

async fn recv_one(t: &TcpMsgTransport) -> Inbound {
    tokio::time::timeout(Duration::from_secs(5), t.recv())
        .await
        .expect("recv timed out")
        .expect("recv")
}

/// Polls until `cond` holds or a 5s deadline passes.
async fn eventually(mut cond: impl FnMut() -> bool, what: &str) {
    for _ in 0..500 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for: {what}");
}

/// An address that refuses connections: bind a listener, note the port,
/// drop it.
async fn dead_addr() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    listener.local_addr().expect("addr")
}

/// Two frames to the same peer arrive attributed — over ONE pooled
/// connection, including a frame far larger than any datagram.
#[tokio::test]
async fn frames_flow_over_a_single_reused_connection() {
    let a = bind_as("msg-a").await;
    let b = bind_as("msg-b").await;
    a.register_peer(NodeId::new("msg-b"), b.local_addr())
        .unwrap();

    let big = vec![0xCDu8; 100 * 1024];
    a.send(&NodeId::new("msg-b"), b"hello").await.expect("send");
    a.send(&NodeId::new("msg-b"), &big).await.expect("send");

    let first = recv_one(&b).await;
    assert_eq!(first.from, NodeId::new("msg-a"));
    assert_eq!(first.msg, b"hello".to_vec());
    let second = recv_one(&b).await;
    assert_eq!(second.msg, big, "TCP framing carries what UDP could not");
    assert_eq!(
        a.outbound_connections(),
        1,
        "both frames rode one persistent connection"
    );
}

/// Sending to a peer with no address book entry is a silent drop and
/// pools nothing — the best-effort contract.
#[tokio::test]
async fn unknown_peer_is_a_silent_drop() {
    let a = bind_as("drop-a").await;
    a.send(&NodeId::new("nobody"), b"lost").await.expect("send");
    assert_eq!(a.outbound_connections(), 0);
}

/// An idle connection closes itself: the pool follows active exchange,
/// so idle peers cost nothing.
#[tokio::test]
async fn idle_connection_closes_itself() {
    let a = TcpMsgTransport::bind_with(
        NodeId::new("idle-a"),
        "127.0.0.1:0",
        TcpMsgConfig {
            idle_timeout: Duration::from_millis(100),
            ..TcpMsgConfig::default()
        },
    )
    .await
    .expect("bind");
    let b = bind_as("idle-b").await;
    a.register_peer(NodeId::new("idle-b"), b.local_addr())
        .unwrap();

    a.send(&NodeId::new("idle-b"), b"ping").await.expect("send");
    assert_eq!(recv_one(&b).await.msg, b"ping".to_vec());
    eventually(|| a.outbound_connections() == 0, "idle close").await;
}

/// A dead peer never errors a send; the failed dial cleans the pool and
/// a later send (after re-registration) dials fresh and delivers.
#[tokio::test]
async fn failed_dial_recovers_and_redials() {
    let a = bind_as("redial-a").await;
    let peer = NodeId::new("redial-b");
    a.register_peer(peer.clone(), dead_addr().await).unwrap();

    a.send(&peer, b"void").await.expect("send is best-effort");
    eventually(|| a.outbound_connections() == 0, "failed dial cleanup").await;

    let b = bind_as("redial-b").await;
    a.register_peer(peer.clone(), b.local_addr()).unwrap();
    a.send(&peer, b"back").await.expect("send");
    assert_eq!(recv_one(&b).await.msg, b"back".to_vec());
}

/// The dial intro offers the accepting side a dial-back path: the
/// joiner's listener arrives as the frames' unverified source, and once
/// the consumer confirms it a seed can answer a joiner nobody ever
/// registered on it.
#[tokio::test]
async fn a_confirmed_intro_teaches_the_reverse_path() {
    let joiner = bind_as("intro-joiner").await;
    let seed = bind_as("intro-seed").await;
    joiner
        .register_peer(NodeId::new("intro-seed"), seed.local_addr())
        .unwrap();

    joiner
        .send(&NodeId::new("intro-seed"), b"hi")
        .await
        .expect("send");
    let inbound = recv_one(&seed).await;
    assert_eq!(inbound.msg, b"hi".to_vec());
    assert_eq!(inbound.source, Some(joiner.local_addr()));
    assert_eq!(
        seed.peer_addr(&NodeId::new("intro-joiner")),
        None,
        "an unverified intro records nothing"
    );
    seed.confirm_peer(&inbound.from, joiner.local_addr());
    assert_eq!(
        seed.peer_addr(&NodeId::new("intro-joiner")),
        Some(joiner.local_addr()),
        "the confirmed intro registered the joiner's listener"
    );

    seed.send(&NodeId::new("intro-joiner"), b"welcome")
        .await
        .expect("send");
    let back = recv_one(&joiner).await;
    assert_eq!(back.from, NodeId::new("intro-seed"));
    assert_eq!(back.msg, b"welcome".to_vec());

    // Once the book holds the intro, later frames carry no source.
    joiner
        .send(&NodeId::new("intro-seed"), b"again")
        .await
        .expect("send");
    assert_eq!(recv_one(&seed).await.source, None);
}

/// A dialer that claims a registered id, introducing its own listener,
/// does not move that id's address: the intro is only reported.
#[tokio::test]
async fn a_claimed_id_does_not_rebind_its_address() {
    let seed = bind_as("claim-seed").await;
    let victim = NodeId::new("claim-victim");
    let genuine = dead_addr().await;
    seed.register_peer(victim.clone(), genuine).unwrap();
    let impostor = bind_as("claim-victim").await;
    impostor
        .register_peer(NodeId::new("claim-seed"), seed.local_addr())
        .unwrap();

    impostor
        .send(&NodeId::new("claim-seed"), b"redirect")
        .await
        .expect("send");
    let inbound = recv_one(&seed).await;
    assert_eq!(inbound.from, victim);
    assert_eq!(inbound.source, Some(impostor.local_addr()));
    assert_eq!(seed.peer_addr(&victim), Some(genuine));
}

/// Gossiped advertisements teach the book like registration; garbage is
/// ignored (an advertisement is a hint, never an error).
#[tokio::test]
async fn learn_peer_parses_and_ignores_garbage() {
    let t = bind_as("learn-a").await;
    t.learn_peer(&NodeId::new("good"), "127.0.0.1:9999");
    assert_eq!(
        t.peer_addr(&NodeId::new("good")),
        Some("127.0.0.1:9999".parse().expect("addr"))
    );
    t.learn_peer(&NodeId::new("bad"), "not-an-address");
    assert_eq!(t.peer_addr(&NodeId::new("bad")), None);
}

/// The pool never exceeds its cap: dialing a new peer at the cap closes
/// the oldest connection.
#[tokio::test]
async fn pool_cap_evicts_the_oldest_connection() {
    let a = TcpMsgTransport::bind_with(
        NodeId::new("cap-a"),
        "127.0.0.1:0",
        TcpMsgConfig {
            max_outbound: NonZeroUsize::MIN,
            ..TcpMsgConfig::default()
        },
    )
    .await
    .expect("bind");
    let b = bind_as("cap-b").await;
    let c = bind_as("cap-c").await;
    a.register_peer(NodeId::new("cap-b"), b.local_addr())
        .unwrap();
    a.register_peer(NodeId::new("cap-c"), c.local_addr())
        .unwrap();

    a.send(&NodeId::new("cap-b"), b"one").await.expect("send");
    assert_eq!(recv_one(&b).await.msg, b"one".to_vec());
    a.send(&NodeId::new("cap-c"), b"two").await.expect("send");
    assert_eq!(recv_one(&c).await.msg, b"two".to_vec());
    assert_eq!(a.outbound_connections(), 1, "cap held: oldest was closed");
}

#[tokio::test]
async fn close_drains_listener_incomplete_handshakes_and_sessions() {
    use tokio::io::AsyncReadExt;

    let a = bind_as("close-a").await;
    let b = bind_as("close-b").await;
    let address = a.local_addr();
    a.register_peer(NodeId::new("close-b"), b.local_addr())
        .unwrap();
    a.send(&NodeId::new("close-b"), b"outbound")
        .await
        .expect("send");
    let outbound = recv_one(&b).await;
    assert_eq!(outbound.msg.as_ref(), b"outbound");
    // The consumer confirms a's introduced listener, as the router would.
    b.confirm_peer(&outbound.from, outbound.source.expect("a's intro"));
    b.send(&NodeId::new("close-a"), b"inbound")
        .await
        .expect("send");
    assert_eq!(recv_one(&a).await.msg.as_ref(), b"inbound");

    // A client that never finishes the introduction must not keep close
    // waiting for the handshake's idle timeout.
    let mut stalled = tokio::net::TcpStream::connect(address)
        .await
        .expect("connect");
    tokio::time::timeout(Duration::from_secs(5), a.close())
        .await
        .expect("close must drain promptly");
    assert_eq!(a.outbound_connections(), 0);
    let _replacement = tokio::net::TcpListener::bind(address)
        .await
        .expect("close released listener");
    let mut byte = [0];
    let read = tokio::time::timeout(Duration::from_secs(5), stalled.read(&mut byte))
        .await
        .expect("session socket released");
    assert!(matches!(read, Ok(0) | Err(_)), "session remained open");
    assert!(a.send(&NodeId::new("close-b"), b"late").await.is_err());
    assert!(a.recv().await.is_err());
    a.close().await;
    b.close().await;
}

/// `closed` waits for shutdown through any clone, then stays resolved.
#[tokio::test]
async fn closed_observes_shutdown_from_any_clone_and_is_sticky() {
    let a = bind_as("closed-a").await;
    let waiter = tokio::spawn({
        let a = a.clone();
        async move { a.closed().await }
    });
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished(), "a running endpoint is not closed");
    a.clone().shutdown();
    tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("shutdown observed")
        .expect("waiter");
    tokio::time::timeout(Duration::from_secs(5), a.closed())
        .await
        .expect("closed is sticky");
    a.close().await;
}

#[tokio::test]
async fn configured_inbox_backpressure_preserves_frames_in_order() {
    let receiver = TcpMsgTransport::bind_with(
        NodeId::new("bounded-receiver"),
        "127.0.0.1:0",
        TcpMsgConfig {
            inbound_queue: QueueCapacity::MIN,
            max_frame_bytes: NonZeroUsize::new(4).unwrap(),
            ..TcpMsgConfig::default()
        },
    )
    .await
    .expect("bind");
    let sender = bind_as("bounded-sender").await;
    sender
        .register_peer(receiver.local_id().clone(), receiver.local_addr())
        .unwrap();
    for body in [b"one".as_slice(), b"two".as_slice(), b"last".as_slice()] {
        sender.send(receiver.local_id(), body).await.expect("send");
    }
    let inner = receiver.direct().expect("direct");
    groupnet_testkit::cluster::eventually("inbound queue fills", || {
        inner.inbox.try_lock().is_ok_and(|inbox| inbox.len() == 1)
    })
    .await;
    for body in [b"one".as_slice(), b"two".as_slice(), b"last".as_slice()] {
        assert_eq!(recv_one(&receiver).await.msg, body);
    }
    sender.close().await;
    receiver.close().await;
}

#[tokio::test]
async fn message_bounds_are_validated_before_binding() {
    let invalid = [
        TcpMsgConfig {
            idle_timeout: Duration::ZERO,
            ..TcpMsgConfig::default()
        },
        TcpMsgConfig {
            max_frame_bytes: NonZeroUsize::new(MAX_FRAME_BYTES + 1).unwrap(),
            ..TcpMsgConfig::default()
        },
        TcpMsgConfig {
            max_frame_bytes: NonZeroUsize::MAX,
            ..TcpMsgConfig::default()
        },
    ];
    let expected = [
        TcpMsgConfigError::ZeroIdleTimeout,
        TcpMsgConfigError::MaxFrameBytesAboveCeiling {
            max_frame_bytes: NonZeroUsize::new(MAX_FRAME_BYTES + 1).unwrap(),
        },
        TcpMsgConfigError::MaxFrameBytesAboveCeiling {
            max_frame_bytes: NonZeroUsize::MAX,
        },
    ];
    for (config, expected) in invalid.into_iter().zip(expected) {
        assert_eq!(config.validate(), Err(expected));
        let error = TcpMsgTransport::bind_with(NodeId::new("invalid"), "127.0.0.1:0", config)
            .await
            .expect_err("invalid bounds");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(
            error
                .get_ref()
                .and_then(|inner| inner.downcast_ref::<TcpMsgConfigError>()),
            Some(&expected),
            "binding surfaces the field-specific error"
        );
    }
    for id in [
        String::new(),
        "x".repeat(groupnet_transport::MAX_NODE_ID_BYTES + 1),
    ] {
        let error = TcpMsgTransport::bind(NodeId::new(id), "127.0.0.1:0")
            .await
            .expect_err("local id outside the handshake bound");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    }
    let ceiling = TcpMsgConfig {
        inbound_queue: QueueCapacity::of(2048),
        max_frame_bytes: NonZeroUsize::new(MAX_FRAME_BYTES).unwrap(),
        ..TcpMsgConfig::default()
    };
    assert_eq!(ceiling.validate(), Ok(()), "the ceiling itself is valid");
    let endpoint = TcpMsgTransport::bind_with(NodeId::new("larger"), "127.0.0.1:0", ceiling)
        .await
        .expect("operational limits may exceed defaults");
    endpoint.close().await;
}

/// A direct endpoint records the registered address.
#[tokio::test]
async fn direct_register_peer_is_observable() {
    let a = bind_as("register-a").await;
    let peer = NodeId::new("register-b");
    let address = dead_addr().await;
    assert_eq!(a.peer_addr(&peer), None);
    assert_eq!(a.register_peer(peer.clone(), address), Ok(()));
    assert_eq!(a.peer_addr(&peer), Some(address));
    a.close().await;
}

#[cfg(feature = "link")]
#[tokio::test]
async fn raw_owned_queue_keeps_storage_and_enforces_frame_cap() {
    let sender = TcpMsgTransport::bind_with(
        NodeId::new("owned-raw"),
        "127.0.0.1:0",
        TcpMsgConfig {
            max_frame_bytes: NonZeroUsize::new(4).unwrap(),
            outbound_queue: QueueCapacity::MIN,
            ..TcpMsgConfig::default()
        },
    )
    .await
    .expect("bind");
    let peer = NodeId::new("queued-peer");
    let (frames, mut queue) = tokio::sync::mpsc::channel(1);
    {
        let inner = sender.direct().expect("direct");
        let mut pool = inner.pool.lock().expect("pool");
        pool.conns.insert(
            peer.clone(),
            super::Conn {
                generation: 0,
                frames,
            },
        );
        pool.order.push_back((0, peer.clone()));
    }
    sender
        .send_owned_admitted(&peer, bytes::Bytes::from_static(b"too large"), None)
        .await
        .expect("oversized is a best-effort drop");
    assert!(matches!(
        queue.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    let payload = bytes::Bytes::from(vec![0xAB; 4]);
    let storage = payload.as_ptr();
    sender
        .send_owned_admitted(&peer, payload, None)
        .await
        .expect("owned send");
    sender
        .send_owned_admitted(&peer, bytes::Bytes::from_static(b"full"), None)
        .await
        .expect("full queue drops");
    let received = queue.try_recv().expect("queued");
    assert_eq!(received.as_ptr(), storage);
    assert_eq!(received.as_ref(), &[0xAB; 4]);
    assert!(matches!(
        queue.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    sender.close().await;
}
