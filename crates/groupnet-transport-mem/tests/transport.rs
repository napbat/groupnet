//! The in-process fabric's public contract, exercised through the
//! [`Transport`] trait the rest of the workspace talks to: attributed
//! delivery, best-effort sends that never error, and one routing table shared
//! across every [`Network`] clone.

use std::time::Duration;

use groupnet_core::NodeId;
use groupnet_transport::Transport;
use groupnet_transport_mem::Network;

/// How long a "nothing arrives" assertion waits before calling an inbox empty.
const QUIET: Duration = Duration::from_millis(50);

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
#[tokio::test]
async fn unknown_peer_is_a_silent_drop() {
    let net = Network::new();
    let a = net.endpoint(NodeId::new("drop-a"));
    let b = net.endpoint(NodeId::new("drop-b"));

    a.send(&NodeId::new("nobody"), b"lost")
        .await
        .expect("an unroutable send reports success, not an error");

    assert!(
        tokio::time::timeout(QUIET, b.recv()).await.is_err(),
        "the unroutable frame was not delivered to some other endpoint"
    );

    a.send(&NodeId::new("drop-b"), b"live").await.expect("send");
    assert_eq!(b.recv().await.expect("recv").msg, b"live".to_vec());
}

/// A registered peer whose endpoint has been dropped is likewise a drop, not
/// an error: the send half outlives the receiver and the failure is swallowed.
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
