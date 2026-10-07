//! The in-process fabric's public contract, exercised through the
//! [`Transport`] trait the rest of the workspace talks to: attributed
//! delivery, best-effort sends that never error, and one routing table shared
//! across every [`Network`] clone.

use groupnet_core::NodeId;
use groupnet_transport::{QueueCapacity, Transport};
use groupnet_transport_mem::{Network, NetworkConfig};

/// One slot per endpoint: the second send waits for receive capacity.
fn single_slot() -> Network {
    Network::with_config(NetworkConfig {
        inbound_queue: QueueCapacity::MIN,
    })
}

/// A frame reaches the addressed endpoint carrying the *sender's* id — the
/// attribution every layer above (gossip, membership) is keyed on.
#[tokio::test]
async fn round_trip_attributes_the_sender() {
    let net = Network::new();
    let a = net.endpoint(NodeId::new("mem-a"));
    let b = net.endpoint(NodeId::new("mem-b"));

    a.send(&NodeId::new("mem-b"), b"hello").await.expect("send");

    let got = b.recv().await.expect("recv");
    assert_eq!(got.from, NodeId::new("mem-a"), "receiver learns the sender");
    assert_eq!(got.msg, b"hello".to_vec());

    // Order is preserved per link: the channel is a queue, not a set.
    a.send(&NodeId::new("mem-b"), b"one").await.expect("send");
    a.send(&NodeId::new("mem-b"), b"two").await.expect("send");
    assert_eq!(b.recv().await.expect("recv").msg, b"one".to_vec());
    assert_eq!(b.recv().await.expect("recv").msg, b"two".to_vec());
}

/// Sending to an id nobody registered is a **silent drop**: `Ok(())`, nothing
/// misrouted, and the endpoint keeps working afterwards. That is the
/// best-effort contract every binding owes the engine.
///
/// The negative is proven by ordering, not by waiting: delivery is FIFO per
/// sender, so had the unroutable frame been misdelivered to `b`, it would have
/// to arrive ahead of the probe sent after it.
#[tokio::test]
async fn unknown_peer_is_a_silent_drop() {
    let net = Network::new();
    let a = net.endpoint(NodeId::new("drop-a"));
    let b = net.endpoint(NodeId::new("drop-b"));

    a.send(&NodeId::new("nobody"), b"lost")
        .await
        .expect("an unroutable send reports success, not an error");

    a.send(&NodeId::new("drop-b"), b"probe")
        .await
        .expect("send");
    assert_eq!(
        b.recv().await.expect("recv").msg,
        b"probe".to_vec(),
        "the unroutable frame was dropped, not delivered ahead of the probe"
    );
}

/// A peer whose endpoint has been dropped is likewise a drop, not an error:
/// dropping the endpoint unregisters it from the fabric.
#[tokio::test]
async fn send_to_a_dropped_endpoint_still_succeeds() {
    let net = Network::new();
    let a = net.endpoint(NodeId::new("dead-a"));
    let b = net.endpoint(NodeId::new("dead-b"));
    drop(b);

    a.send(&NodeId::new("dead-b"), b"void")
        .await
        .expect("a dead peer is a drop, never an error");
}

/// `Network` clones share one routing table: an endpoint created from a clone
/// is reachable from the original, and vice versa. Fixtures rely on this to
/// hand a cloned fabric to each node.
#[tokio::test]
async fn clones_share_one_routing_table() {
    let net = Network::new();
    let a = net.endpoint(NodeId::new("clone-a"));
    let b = net.clone().endpoint(NodeId::new("clone-b"));

    a.send(&NodeId::new("clone-b"), b"across")
        .await
        .expect("send");
    let got = b.recv().await.expect("recv");
    assert_eq!(got.from, NodeId::new("clone-a"));
    assert_eq!(got.msg, b"across".to_vec());

    b.send(&NodeId::new("clone-a"), b"back")
        .await
        .expect("send");
    let back = a.recv().await.expect("recv");
    assert_eq!(back.from, NodeId::new("clone-b"));
    assert_eq!(back.msg, b"back".to_vec());
}

/// An old endpoint's cleanup must not remove a new endpoint at the same id.
#[tokio::test]
async fn dropping_replaced_endpoint_keeps_replacement_reachable() {
    let net = Network::new();
    let sender = net.endpoint(NodeId::new("sender"));
    let old = net.endpoint(NodeId::new("replaced"));
    let replacement = net.endpoint(NodeId::new("replaced"));
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(1), old.recv())
            .await
            .expect("replacement must close the displaced receiver")
            .is_err()
    );
    drop(old);

    sender
        .send(&NodeId::new("replaced"), b"replacement")
        .await
        .expect("send");
    assert_eq!(
        replacement.recv().await.expect("recv").msg.as_ref(),
        b"replacement"
    );
}

#[tokio::test]
async fn full_message_queue_waits_for_receive_capacity() {
    use std::future::{Future, poll_fn};
    use std::task::Poll;

    let net = single_slot();
    let a = net.endpoint(NodeId::new("sender"));
    let b = net.endpoint(NodeId::new("receiver"));
    let target = b.local_id().clone();
    a.send(&target, b"first").await.unwrap();
    let mut pending = std::pin::pin!(a.send(&target, b"second"));
    poll_fn(|cx| {
        assert!(pending.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(b.recv().await.unwrap().msg.as_ref(), b"first");
    pending.await.unwrap();
    assert_eq!(b.recv().await.unwrap().msg.as_ref(), b"second");
}

#[tokio::test]
async fn dropping_full_target_unblocks_a_waiting_sender() {
    use std::future::{Future, poll_fn};
    use std::task::Poll;

    let net = single_slot();
    let a = net.endpoint(NodeId::new("sender"));
    let b = net.endpoint(NodeId::new("receiver"));
    let target = b.local_id().clone();
    a.send(&target, b"first").await.unwrap();
    let mut pending = std::pin::pin!(a.send(&target, b"second"));
    poll_fn(|cx| {
        assert!(pending.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(b);
    pending
        .await
        .expect("closed targets remain best-effort drops");
}

#[tokio::test]
async fn replacement_does_not_redirect_an_already_waiting_send() {
    use std::future::{Future, poll_fn};
    use std::task::Poll;

    let net = single_slot();
    let a = net.endpoint(NodeId::new("sender"));
    let old = net.endpoint(NodeId::new("receiver"));
    let target = old.local_id().clone();
    a.send(&target, b"first").await.unwrap();
    let mut pending = std::pin::pin!(a.send(&target, b"old-generation"));
    poll_fn(|cx| {
        assert!(pending.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let replacement = net.endpoint(target.clone());
    assert_eq!(old.recv().await.unwrap().msg.as_ref(), b"first");
    pending.await.unwrap();
    assert_eq!(old.recv().await.unwrap().msg.as_ref(), b"old-generation");
    assert!(old.recv().await.is_err());
    drop(old);
    a.send(&target, b"new-generation").await.unwrap();
    assert_eq!(
        replacement.recv().await.unwrap().msg.as_ref(),
        b"new-generation"
    );
}

#[cfg(feature = "link")]
#[tokio::test]
async fn owned_send_preserves_packet_storage() {
    let net = Network::new();
    let a = net.endpoint(NodeId::new("sender"));
    let b = net.endpoint(NodeId::new("receiver"));
    let packet = bytes::Bytes::from(vec![0x5a; 4096]);
    let pointer = packet.as_ptr();
    a.send_owned_admitted(b.local_id(), packet, None)
        .await
        .unwrap();
    let inbound = b.recv().await.unwrap();
    assert_eq!(inbound.msg.as_ptr(), pointer);
    assert_eq!(inbound.msg.len(), 4096);
}

#[cfg(feature = "link")]
#[tokio::test]
async fn static_owned_send_drops_admitted_session_packets() {
    use bytes::Bytes;
    use groupnet_transport::admission::{AcceptedPeer, SessionRegistry};

    let net = Network::new();
    let a = net.endpoint(NodeId::new("sender"));
    let b = net.endpoint(NodeId::new("receiver"));
    let registry = SessionRegistry::new(1).unwrap();
    let lease = registry
        .try_admit(AcceptedPeer::new(b.local_id().clone()))
        .unwrap();
    a.send_owned_admitted(
        b.local_id(),
        Bytes::from_static(b"wrong-lifetime"),
        Some(lease.id()),
    )
    .await
    .unwrap();
    a.send_owned_admitted(b.local_id(), Bytes::from_static(b"static"), None)
        .await
        .unwrap();
    assert_eq!(b.recv().await.unwrap().msg.as_ref(), b"static");
}
