//! Accept-side contracts of `TcpBulkTransport` over real loopback TCP: Nagle is
//! disabled on both ends, and identity handshakes run per connection under a
//! deadline, so a stalled or malformed client cannot block other accepts.

#![cfg(feature = "bulk")]

use std::io;
use std::time::Duration;

use groupnet_core::NodeId;
use groupnet_transport::QueueCapacity;
use groupnet_transport::bulk::BulkTransport;
use groupnet_transport_tcp::{TcpBulkConfig, TcpBulkTransport};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

/// Failure-report bound for socket progress; no test waits on it to pass.
const SETTLE: Duration = Duration::from_secs(5);

async fn server(config: TcpBulkConfig) -> TcpBulkTransport {
    TcpBulkTransport::bind_with(NodeId::new("server"), "127.0.0.1:0", config)
        .await
        .expect("bind server")
}

fn client(server: &TcpBulkTransport, id: &str) -> TcpBulkTransport {
    let client = TcpBulkTransport::dial_only(NodeId::new(id));
    client.register_peer(
        server.local_id().clone(),
        server.local_addr().expect("server address"),
    );
    client
}

/// Connects `client` and accepts on `server`, attributing the stream.
async fn connect_and_accept(client: &TcpBulkTransport, server: &TcpBulkTransport) {
    let (dialed, accepted) = timeout(SETTLE, async {
        tokio::join!(client.connect(server.local_id()), server.accept())
    })
    .await
    .expect("accept is not blocked");
    let dialed = dialed.expect("connect");
    let (from, accepted) = accepted.expect("accept");
    assert_eq!(&from, client.local_id());
    assert!(dialed.get_ref().nodelay().expect("dialed nodelay"));
    assert!(accepted.get_ref().nodelay().expect("accepted nodelay"));
}

/// A closed socket reads EOF or a reset.
async fn assert_closed(socket: &mut TcpStream, why: &str) {
    let mut byte = [0];
    let read = timeout(SETTLE, socket.read(&mut byte)).await.expect(why);
    assert!(matches!(read, Ok(0) | Err(_)), "{why}");
}

#[tokio::test]
async fn both_ends_disable_nagle() {
    let server = server(TcpBulkConfig::default()).await;
    connect_and_accept(&client(&server, "client"), &server).await;
}

#[tokio::test]
async fn stalled_and_malformed_handshakes_never_block_or_fail_accepts() {
    let server = server(TcpBulkConfig::default()).await;
    let address = server.local_addr().expect("address");
    // Accepted ahead of the real client: one never speaks, one overstates
    // its id. Neither may delay the next accept or surface as its error.
    let _stalled = TcpStream::connect(address).await.expect("stalled");
    let mut malformed = TcpStream::connect(address).await.expect("malformed");
    malformed
        .write_all(&u32::MAX.to_be_bytes())
        .await
        .expect("oversized id length");
    connect_and_accept(&client(&server, "client"), &server).await;
    assert_closed(&mut malformed, "a malformed handshake is dropped").await;
}

#[tokio::test]
async fn handshake_deadline_frees_the_only_slot() {
    let server = server(TcpBulkConfig {
        handshake_timeout: Duration::from_millis(100),
        max_handshakes: QueueCapacity::MIN,
        ..TcpBulkConfig::default()
    })
    .await;
    let mut stalled = TcpStream::connect(server.local_addr().expect("address"))
        .await
        .expect("stalled");
    assert_closed(&mut stalled, "the deadline drops a stalled handshake").await;
    connect_and_accept(&client(&server, "client"), &server).await;
}

#[tokio::test]
async fn dropping_the_endpoint_cancels_pending_handshakes() {
    let server = server(TcpBulkConfig::default()).await;
    let mut stalled = TcpStream::connect(server.local_addr().expect("address"))
        .await
        .expect("stalled");
    drop(server);
    assert_closed(&mut stalled, "drop releases pending handshakes").await;
}

#[tokio::test]
async fn invalid_bounds_and_local_ids_are_rejected_before_binding() {
    let zero_deadline = TcpBulkConfig {
        handshake_timeout: Duration::ZERO,
        ..TcpBulkConfig::default()
    };
    assert_eq!(
        zero_deadline.validate().expect_err("zero deadline").kind(),
        io::ErrorKind::InvalidInput
    );
    let error = TcpBulkTransport::bind_with(NodeId::new("server"), "127.0.0.1:0", zero_deadline)
        .await
        .expect_err("zero deadline");
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    for id in [
        String::new(),
        "x".repeat(groupnet_transport::MAX_NODE_ID_BYTES + 1),
    ] {
        let error = TcpBulkTransport::bind(NodeId::new(id), "127.0.0.1:0")
            .await
            .expect_err("local id outside the handshake bound");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }
}
