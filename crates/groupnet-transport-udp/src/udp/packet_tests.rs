//! Regressions for raw datagram rejection and shared sending-buffer ownership.

use std::time::Duration;

use groupnet_core::NodeId;
use groupnet_transport::Transport;

use super::UdpTransport;
use super::framing::{MAX_DATAGRAM, SendBuffer};

async fn bind(id: &str) -> UdpTransport {
    UdpTransport::bind(NodeId::new(id), "127.0.0.1:0")
        .await
        .expect("raw endpoint")
}

#[tokio::test]
async fn registered_source_cannot_bypass_malformed_identity_rejection() {
    let receiver = bind("receiver").await;
    let sender = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("sender socket");
    let sender_id = NodeId::new("sender");
    receiver.register_peer(
        sender_id.clone(),
        sender.local_addr().expect("sender address"),
    );
    let addr = receiver.local_addr().expect("receiver address");
    for malformed in [b"bad".as_slice(), &[1, 0, 0, 0, 0xff], &[1, 4, 0, 0]] {
        sender.send_to(malformed, addr).await.expect("noise send");
    }
    let mut buffer = SendBuffer::new(&sender_id).expect("sender prefix");
    sender
        .send_to(buffer.frame(b"valid").expect("valid frame"), addr)
        .await
        .expect("valid send");
    let inbound = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await
        .expect("receive deadline")
        .expect("receive");
    assert_eq!(inbound.from, sender_id);
    assert_eq!(inbound.msg.as_ref(), b"valid");
    assert_eq!(receiver.known_peers(), vec![sender_id]);
}

#[tokio::test]
async fn concurrent_clones_keep_datagrams_separate() {
    let sender = bind("sender").await;
    let receiver = bind("receiver").await;
    let receiver_id = NodeId::new("receiver");
    sender.register_peer(
        receiver_id.clone(),
        receiver.local_addr().expect("receiver address"),
    );
    let clone = sender.clone();
    let first = [1; 1024];
    let second = [2; 257];
    let (first_send, second_send) = tokio::join!(
        sender.send(&receiver_id, &first),
        clone.send(&receiver_id, &second)
    );
    first_send.expect("first send");
    second_send.expect("second send");
    let arrivals = tokio::time::timeout(Duration::from_secs(2), async {
        let first = receiver.recv().await.expect("first receive");
        let second = receiver.recv().await.expect("second receive");
        [first, second]
    })
    .await
    .expect("receive deadline");
    for inbound in &arrivals {
        assert_eq!(inbound.from, NodeId::new("sender"));
    }
    assert!(arrivals.iter().any(|packet| packet.msg.as_ref() == first));
    assert!(arrivals.iter().any(|packet| packet.msg.as_ref() == second));
}

#[tokio::test]
async fn oversized_outbound_frame_is_rejected_without_poisoning_buffer() {
    let sender = bind("sender").await;
    let receiver = bind("receiver").await;
    let receiver_id = NodeId::new("receiver");
    sender.register_peer(
        receiver_id.clone(),
        receiver.local_addr().expect("receiver address"),
    );
    assert_eq!(
        sender
            .send(&receiver_id, &vec![0; MAX_DATAGRAM])
            .await
            .expect_err("oversized datagram")
            .kind(),
        std::io::ErrorKind::InvalidInput
    );
    sender.send(&receiver_id, b"next").await.expect("next send");
    let arrivals = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await
        .expect("receive deadline")
        .expect("receive");
    assert_eq!(arrivals.msg.as_ref(), b"next");
}

#[tokio::test]
async fn oversized_local_identity_is_rejected_at_bind() {
    assert_eq!(
        UdpTransport::bind(NodeId::new("x".repeat(1025)), "127.0.0.1:0")
            .await
            .expect_err("oversized identity")
            .kind(),
        std::io::ErrorKind::InvalidInput
    );
}

#[cfg(feature = "link")]
#[tokio::test]
async fn raw_owned_admitted_send_roundtrips_bytes() {
    let sender = bind("sender").await;
    let receiver = bind("receiver").await;
    let receiver_id = NodeId::new("receiver");
    sender.register_peer(
        receiver_id.clone(),
        receiver.local_addr().expect("receiver address"),
    );
    sender
        .send_owned_admitted(&receiver_id, bytes::Bytes::from_static(b"owned"), None)
        .await
        .expect("owned send");
    let arrivals = tokio::time::timeout(Duration::from_secs(2), receiver.recv_admitted())
        .await
        .expect("receive deadline")
        .expect("receive");
    assert_eq!(arrivals.packet.from, NodeId::new("sender"));
    assert_eq!(arrivals.packet.msg.as_ref(), b"owned");
    assert_eq!(arrivals.session, None);
}

#[tokio::test]
async fn receive_reuses_scratch_and_retains_independent_owned_payloads() {
    let sender = bind("sender").await;
    let receiver = bind("receiver").await;
    let inner = match &receiver.backend {
        super::Backend::Direct(inner) => inner,
        #[cfg(feature = "connectivity")]
        super::Backend::Connectivity(_) => panic!("raw endpoint"),
    };
    let pointer = inner.receive_buffer.lock().await.as_ptr();
    sender.register_peer(
        NodeId::new("receiver"),
        receiver.local_addr().expect("address"),
    );
    // Cancelling an idle receive releases the scratch owner for the next call.
    tokio::select! {
        biased;
        _ = receiver.recv() => panic!("idle receive unexpectedly completed"),
        () = std::future::ready(()) => {},
    }
    sender
        .send(&NodeId::new("receiver"), b"first")
        .await
        .expect("send");
    let first = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await
        .expect("receive bound")
        .expect("receive");
    sender
        .send(&NodeId::new("receiver"), b"second")
        .await
        .expect("send");
    let second = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await
        .expect("receive bound")
        .expect("receive");
    assert_eq!(inner.receive_buffer.lock().await.as_ptr(), pointer);
    assert_eq!(first.msg.as_ref(), b"first");
    assert_eq!(second.msg.as_ref(), b"second");
}
