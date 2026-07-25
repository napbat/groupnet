//! Integration test over *real* TCP: stream a multi-megabyte payload from one
//! node to another on loopback, through the full data-plane stack
//! (TcpBulkTransport → futures-io compat → DataStream framing → zerocopy
//! header → zero-copy `Bytes`).

#![cfg(feature = "bulk")]

use bytes::Bytes;
use groupnet_core::NodeId;
use groupnet_transport::bulk::DataPlane;
use groupnet_transport_tcp::TcpBulkTransport;

#[tokio::test]
async fn streams_a_multi_megabyte_blob_over_loopback() {
    let a = TcpBulkTransport::bind(NodeId::new("node-a"), "127.0.0.1:0")
        .await
        .expect("bind a");
    let b = TcpBulkTransport::bind(NodeId::new("node-b"), "127.0.0.1:0")
        .await
        .expect("bind b");

    // a needs to know where b listens.
    a.register_peer(NodeId::new("node-b"), b.local_addr().unwrap());

    let data_a = DataPlane::new(a);
    let data_b = DataPlane::new(b);

    let payload = Bytes::from(vec![0xABu8; 4_000_000]);
    let expected = payload.clone();

    // Receiver: accept the inbound stream and read one framed message.
    let receiver = tokio::spawn(async move {
        let (from, mut stream) = data_b.accept().await.expect("accept");
        let got = stream.recv().await.expect("recv").expect("a frame");
        (from, got)
    });

    // Sender: connect and stream the blob.
    let mut stream = data_a
        .connect(&NodeId::new("node-b"))
        .await
        .expect("connect");
    stream.send(payload).await.expect("send");

    let (from, got) = receiver.await.expect("join");
    assert_eq!(from, NodeId::new("node-a"), "peer identified via handshake");
    assert_eq!(got.len(), 4_000_000);
    assert_eq!(got, expected, "payload survived the round trip intact");
}
