//! The in-process data plane's public contract, exercised through the traits
//! the rest of the workspace talks to ([`BulkTransport`] under a [`DataPlane`],
//! framed by `DataStream`): attributed connections, ordered multi-frame
//! round trips, a clean end of stream when the writer goes away, and streams
//! that stay independent even between the same pair of nodes.

#![cfg(feature = "bulk")]

use std::io;

use bytes::Bytes;
use groupnet_core::NodeId;
use groupnet_transport::bulk::{BulkTransport, DataPlane};
use groupnet_transport_mem::MemBulkNet;

/// A pair of connected data planes on one fabric, `(a, b)`.
fn pair() -> (
    DataPlane<impl BulkTransport<Error = io::Error>>,
    DataPlane<impl BulkTransport<Error = io::Error>>,
) {
    let net = MemBulkNet::new();
    let a = net.endpoint(NodeId::new("bulk-a"));
    // Cloned fabric: endpoints from a clone share one table of accept queues.
    let b = net.clone().endpoint(NodeId::new("bulk-b"));
    (DataPlane::new(a), DataPlane::new(b))
}

/// The acceptor learns who opened the stream — the *connector's* id, not its
/// own. That attribution is what a data-plane handler keys replication and
/// snapshot transfer on, and it arrives with the stream, with no handshake to
/// wait for.
#[tokio::test]
async fn accept_attributes_the_connector() {
    let (a, b) = pair();

    let _out = a.connect(&NodeId::new("bulk-b")).await.expect("connect");

    let (from, _in) = b.accept().await.expect("accept");
    assert_eq!(
        from,
        NodeId::new("bulk-a"),
        "the acceptor learns the connector"
    );
}

/// Connecting to an id nobody registered is an **error**, deliberately unlike
/// the control plane's silent drop: a stream plane is connection-oriented, so
/// a caller holding a stream may assume there is a peer on the far end.
#[tokio::test]
async fn connecting_to_an_unknown_peer_is_an_error() {
    let (a, _b) = pair();

    let err = a
        .connect(&NodeId::new("nobody"))
        .await
        .expect_err("an unroutable connect must not hand back a stream");
    assert_eq!(err.kind(), io::ErrorKind::NotFound);
}

/// Frames survive the round trip whole and in order. Reliability and ordering
/// are the data plane's whole point, so the in-process binding must not be
/// weaker than the TCP one it stands in for.
#[tokio::test]
async fn frames_round_trip_in_order() {
    let (a, b) = pair();

    let mut out = a.connect(&NodeId::new("bulk-b")).await.expect("connect");
    let (_from, mut inbound) = b.accept().await.expect("accept");

    for i in 0..8u8 {
        out.send(Bytes::from(vec![i; 100 + usize::from(i)]))
            .await
            .expect("send");
    }

    for i in 0..8u8 {
        let got = inbound.recv().await.expect("recv").expect("a frame");
        assert_eq!(
            got.len(),
            100 + usize::from(i),
            "frame {i} arrived in order"
        );
        assert!(got.iter().all(|&b| b == i), "frame {i} arrived intact");
    }
}

/// A writer that goes away mid-stream surfaces to the reader as `None` — a
/// *clean* end of stream at the frame boundary, indistinguishable from an
/// orderly close. The already-written frame is still delivered first: dropping
/// the writer does not discard what it flushed.
///
/// This pins the framing layer's documented behaviour, and it is precisely why
/// a handoff protocol over this plane needs its own in-band "done" marker: EOF
/// alone cannot tell "the peer finished" from "the peer died between frames".
#[tokio::test]
async fn a_dropped_writer_is_a_clean_eof_at_the_frame_boundary() {
    let (a, b) = pair();

    let mut out = a.connect(&NodeId::new("bulk-b")).await.expect("connect");
    let (_from, mut inbound) = b.accept().await.expect("accept");

    out.send(Bytes::from_static(b"only-frame"))
        .await
        .expect("send");
    drop(out);

    let got = inbound.recv().await.expect("recv").expect("a frame");
    assert_eq!(got, &b"only-frame"[..], "the flushed frame still arrives");
    assert!(
        inbound.recv().await.expect("recv").is_none(),
        "the writer's disappearance reads as a clean EOF, not an error"
    );
}

/// Two streams between the *same* pair are independent pipes: frames written
/// on one never surface on the other, and each keeps its own order. Bulk
/// transfers run concurrently (a snapshot alongside a replication stream), so
/// they must not share a byte lane.
#[tokio::test]
async fn two_streams_between_one_pair_do_not_interleave() {
    let (a, b) = pair();

    // Both connects queue on b's accept queue in the order they were made.
    let mut first_out = a.connect(&NodeId::new("bulk-b")).await.expect("connect 1");
    let mut second_out = a.connect(&NodeId::new("bulk-b")).await.expect("connect 2");
    let (_, mut first_in) = b.accept().await.expect("accept 1");
    let (_, mut second_in) = b.accept().await.expect("accept 2");

    // Interleave the writes across the two streams.
    first_out.send(Bytes::from_static(b"one-a")).await.unwrap();
    second_out.send(Bytes::from_static(b"two-a")).await.unwrap();
    first_out.send(Bytes::from_static(b"one-b")).await.unwrap();
    second_out.send(Bytes::from_static(b"two-b")).await.unwrap();

    for expected in [&b"one-a"[..], &b"one-b"[..]] {
        let got = first_in.recv().await.expect("recv").expect("a frame");
        assert_eq!(got, expected, "stream one carries only its own frames");
    }
    for expected in [&b"two-a"[..], &b"two-b"[..]] {
        let got = second_in.recv().await.expect("recv").expect("a frame");
        assert_eq!(got, expected, "stream two carries only its own frames");
    }
}
