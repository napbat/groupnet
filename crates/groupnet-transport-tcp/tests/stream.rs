//! Integration test over *real* TCP: stream a multi-megabyte payload from one
//! node to another on loopback, through the full data-plane stack
//! (`TcpBulkTransport` → futures-io compat → `DataStream` framing → zerocopy
//! header → zero-copy `Bytes`).

#![cfg(feature = "bulk")]

use bytes::Bytes;
use groupnet_core::NodeId;
use groupnet_transport::MAX_NODE_ID_BYTES;
use groupnet_transport::bulk::DataPlane;
use groupnet_transport_tcp::{NodeIdError, TcpBulkTransport, TcpHostEndpointError};
use std::io;
use std::time::Duration;

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

#[tokio::test]
async fn hostname_book_resolves_each_connect_under_caller_deadline() {
    let a = TcpBulkTransport::bind(NodeId::new("host-a"), "127.0.0.1:0")
        .await
        .unwrap();
    let b = TcpBulkTransport::bind(NodeId::new("host-b"), "127.0.0.1:0")
        .await
        .unwrap();
    let port = b.local_addr().unwrap().port();
    assert_eq!(
        a.register_peer_host(NodeId::new("invalid"), "localhost".into()),
        Err(TcpHostEndpointError::MissingPort)
    );
    assert_eq!(
        a.register_peer_host(NodeId::new("invalid"), format!("localhost:{port}/../bad")),
        Err(TcpHostEndpointError::ForbiddenByte {
            byte: b'/',
            index: "localhost:".len() + port.to_string().len(),
        })
    );
    a.register_peer_host(NodeId::new("host-b"), format!("localhost:{port}"))
        .unwrap();
    let a = DataPlane::new(a);
    let b = DataPlane::new(b);
    let receiver = tokio::spawn(async move { b.accept().await.unwrap().0 });
    let _stream = tokio::time::timeout(Duration::from_secs(10), a.connect(&NodeId::new("host-b")))
        .await
        .expect("bounded connect")
        .expect("resolved host");
    assert_eq!(receiver.await.unwrap(), NodeId::new("host-a"));
}

/// The host-endpoint bounds are inclusive and each rejection names its rule,
/// kept as the inner error of the `InvalidInput` it converts into.
#[test]
fn host_endpoints_are_rejected_with_their_typed_rule() {
    let endpoint = TcpBulkTransport::dial_only(NodeId::new("hosts")).expect("valid id");
    let node = || NodeId::new("peer");
    let longest = format!("{}:65535", "h".repeat(320 - ":65535".len()));
    assert_eq!(endpoint.register_peer_host(node(), longest.clone()), Ok(()));
    assert_eq!(endpoint.register_peer_host(node(), "h:1".into()), Ok(()));
    for (host, expected) in [
        (
            format!("h{longest}"),
            TcpHostEndpointError::TooLong { len: 321, max: 320 },
        ),
        (
            "user@h:1".into(),
            TcpHostEndpointError::ForbiddenByte {
                byte: b'@',
                index: 4,
            },
        ),
        (
            "h :1".into(),
            TcpHostEndpointError::ForbiddenByte {
                byte: b' ',
                index: 1,
            },
        ),
        (String::new(), TcpHostEndpointError::MissingPort),
        (":1".into(), TcpHostEndpointError::EmptyHost),
        ("h:0".into(), TcpHostEndpointError::InvalidPort),
        ("h:65536".into(), TcpHostEndpointError::InvalidPort),
        ("h:".into(), TcpHostEndpointError::InvalidPort),
    ] {
        let error = endpoint
            .register_peer_host(node(), host)
            .expect_err("invalid host endpoint");
        assert_eq!(error, expected);
        assert_ne!(error.to_string(), "");
        let io = io::Error::from(error);
        assert_eq!(io.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            io.get_ref()
                .and_then(|inner| inner.downcast_ref::<TcpHostEndpointError>()),
            Some(&expected)
        );
    }
}

#[tokio::test]
async fn a_dial_only_endpoint_calls_out_and_never_accepts() {
    let client = TcpBulkTransport::dial_only(NodeId::new("client-a")).expect("valid id");
    assert_eq!(
        client.local_addr(),
        None,
        "a dial-only endpoint has no listener"
    );
    let server = TcpBulkTransport::bind(NodeId::new("server-b"), "127.0.0.1:0")
        .await
        .expect("bind server");
    client.register_peer(NodeId::new("server-b"), server.local_addr().unwrap());
    let client = DataPlane::new(client);
    let server = DataPlane::new(server);

    // The server answers on the stream that the client opened.
    let echo = tokio::spawn(async move {
        let (from, mut stream) = server.accept().await.expect("accept");
        let request = stream.recv().await.expect("recv").expect("a frame");
        stream.send(request).await.expect("reply");
        from
    });
    let mut stream = client
        .connect(&NodeId::new("server-b"))
        .await
        .expect("connect");
    stream
        .send(Bytes::from_static(b"ping"))
        .await
        .expect("send");
    let reply = stream.recv().await.expect("recv").expect("a reply");
    assert_eq!(reply, Bytes::from_static(b"ping"));
    assert_eq!(echo.await.unwrap(), NodeId::new("client-a"));

    // Nothing can reach the client, so accept stays pending.
    let accepted = tokio::time::timeout(Duration::from_millis(50), client.accept()).await;
    assert!(accepted.is_err(), "a dial-only endpoint accepted a stream");
}

#[test]
fn a_dial_only_endpoint_rejects_identities_its_handshake_would_refuse() {
    assert_eq!(
        TcpBulkTransport::dial_only(NodeId::new("")).err(),
        Some(NodeIdError::Empty)
    );
    assert_eq!(
        TcpBulkTransport::dial_only(NodeId::new("x".repeat(MAX_NODE_ID_BYTES + 1))).err(),
        Some(NodeIdError::TooLong {
            len: MAX_NODE_ID_BYTES + 1,
            max: MAX_NODE_ID_BYTES,
        })
    );
    assert!(TcpBulkTransport::dial_only(NodeId::new("x".repeat(MAX_NODE_ID_BYTES))).is_ok());
}
