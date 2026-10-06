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

/// The bootstrap request/reply exchange closes each write half to prove
/// there are no trailing frames, yet must continue reading the peer's half.
#[tokio::test]
async fn write_half_shutdown_preserves_the_reply_half() {
    let (a, b) = pair();
    let mut outbound = a.connect(&NodeId::new("bulk-b")).await.expect("connect");
    let (_, mut inbound) = b.accept().await.expect("accept");

    outbound
        .send_bounded(Bytes::from_static(b"request"), 7)
        .await
        .expect("request");
    outbound
        .finish_write()
        .await
        .expect("request write shutdown");
    assert_eq!(
        inbound.recv_bounded(7).await.unwrap().unwrap(),
        &b"request"[..]
    );
    assert!(inbound.recv_bounded(7).await.unwrap().is_none());

    inbound
        .send_bounded(Bytes::from_static(b"reply"), 5)
        .await
        .expect("reply");
    inbound.finish_write().await.expect("reply write shutdown");
    assert_eq!(
        outbound.recv_bounded(5).await.unwrap().unwrap(),
        &b"reply"[..]
    );
    assert!(outbound.recv_bounded(5).await.unwrap().is_none());
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

#[test]
fn bulk_configuration_rejects_invalid_queue_and_pipe_capacities() {
    use groupnet_transport_mem::MemBulkConfig;

    for config in [
        MemBulkConfig {
            accept_queue: 0,
            ..MemBulkConfig::default()
        },
        MemBulkConfig {
            accept_queue: usize::MAX,
            ..MemBulkConfig::default()
        },
        MemBulkConfig {
            pipe_buffer: 0,
            ..MemBulkConfig::default()
        },
        MemBulkConfig {
            pipe_buffer: usize::MAX,
            ..MemBulkConfig::default()
        },
    ] {
        assert_eq!(
            MemBulkNet::with_config(config).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }
}

#[tokio::test]
async fn full_accept_queue_backpressures_connectors() {
    use groupnet_transport_mem::MemBulkConfig;
    use std::future::{Future, poll_fn};
    use std::task::Poll;

    let net = MemBulkNet::with_config(MemBulkConfig {
        accept_queue: 1,
        pipe_buffer: 8,
    })
    .unwrap();
    let a = net.endpoint(NodeId::new("sender"));
    let b = net.endpoint(NodeId::new("receiver"));
    let target = b.local_id().clone();
    let _first = a.connect(&target).await.unwrap();
    let mut pending = std::pin::pin!(a.connect(&target));
    poll_fn(|cx| {
        assert!(pending.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let _first_inbound = b.accept().await.unwrap();
    let _second = pending.await.unwrap();
    let (from, _second_inbound) = b.accept().await.unwrap();
    assert_eq!(from, *a.local_id());
}

#[tokio::test]
async fn dropped_full_acceptor_unblocks_connect_with_refusal() {
    use groupnet_transport_mem::MemBulkConfig;
    use std::future::{Future, poll_fn};
    use std::task::Poll;

    let net = MemBulkNet::with_config(MemBulkConfig {
        accept_queue: 1,
        ..MemBulkConfig::default()
    })
    .unwrap();
    let a = net.endpoint(NodeId::new("sender"));
    let b = net.endpoint(NodeId::new("receiver"));
    let target = b.local_id().clone();
    let _first = a.connect(&target).await.unwrap();
    let mut pending = std::pin::pin!(a.connect(&target));
    poll_fn(|cx| {
        assert!(pending.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(b);
    assert_eq!(
        pending.await.unwrap_err().kind(),
        io::ErrorKind::ConnectionRefused
    );
}

#[tokio::test]
async fn replacement_preserves_waiting_connection_generation_and_open_streams() {
    use groupnet_transport_mem::MemBulkConfig;
    use std::future::{Future, poll_fn};
    use std::task::Poll;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_util::compat::FuturesAsyncReadCompatExt;

    let net = MemBulkNet::with_config(MemBulkConfig {
        accept_queue: 1,
        pipe_buffer: 8,
    })
    .unwrap();
    let a = net.endpoint(NodeId::new("sender"));
    let old = net.endpoint(NodeId::new("receiver"));
    let target = old.local_id().clone();
    let first = a.connect(&target).await.unwrap();
    let mut pending = std::pin::pin!(a.connect(&target));
    poll_fn(|cx| {
        assert!(pending.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let replacement = net.endpoint(target.clone());
    let (_, first_inbound) = old.accept().await.unwrap();
    let _old_second = pending.await.unwrap();
    let _old_second_inbound = old.accept().await.unwrap();
    assert_eq!(
        old.accept().await.unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    drop(old);
    let _new = a.connect(&target).await.unwrap();
    let _new_inbound = replacement.accept().await.unwrap();

    // Endpoint replacement and drop do not disturb pipes already connected.
    let mut first = first.compat();
    let mut first_inbound = first_inbound.compat();
    first.write_all(b"survives").await.unwrap();
    let mut data = [0; 8];
    first_inbound.read_exact(&mut data).await.unwrap();
    assert_eq!(&data, b"survives");
}

#[tokio::test]
async fn configured_pipe_buffer_backpressures_writes() {
    use groupnet_transport_mem::MemBulkConfig;
    use std::future::{Future, poll_fn};
    use std::task::Poll;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_util::compat::FuturesAsyncReadCompatExt;

    let net = MemBulkNet::with_config(MemBulkConfig {
        accept_queue: 1,
        pipe_buffer: 1,
    })
    .unwrap();
    let a = net.endpoint(NodeId::new("sender"));
    let b = net.endpoint(NodeId::new("receiver"));
    let mut out = a.connect(b.local_id()).await.unwrap().compat();
    let (_, inbound) = b.accept().await.unwrap();
    let mut inbound = inbound.compat();
    out.write_all(b"a").await.unwrap();
    let mut pending = std::pin::pin!(out.write_all(b"b"));
    poll_fn(|cx| {
        assert!(pending.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(inbound.read_u8().await.unwrap(), b'a');
    pending.await.unwrap();
    assert_eq!(inbound.read_u8().await.unwrap(), b'b');
}
