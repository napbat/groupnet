//! `TcpBulkTransport` under a cluster secret, over real loopback TCP: peers
//! holding the same secret connect and stream; a peer with another secret,
//! with none, or speaking the old unversioned handshake is refused before a
//! stream byte flows, and every refusal is counted.

#![cfg(feature = "bulk")]

use std::time::Duration;

use groupnet_core::NodeId;
use groupnet_testkit::cluster::eventually;
use groupnet_transport::bulk::BulkTransport;
use groupnet_transport_tcp::{ClusterSecret, TcpBulkConfig, TcpBulkTransport};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

/// Failure-report bound for socket progress; no test waits on it to pass.
const SETTLE: Duration = Duration::from_secs(5);

fn secret(byte: u8) -> ClusterSecret {
    ClusterSecret::new([byte; ClusterSecret::MIN_LEN]).expect("valid secret")
}

async fn server(secret: Option<ClusterSecret>) -> TcpBulkTransport {
    TcpBulkTransport::bind_with(
        NodeId::new("server"),
        "127.0.0.1:0",
        TcpBulkConfig {
            secret,
            ..TcpBulkConfig::default()
        },
    )
    .await
    .expect("bind server")
}

fn client(server: &TcpBulkTransport, id: &str, secret: Option<ClusterSecret>) -> TcpBulkTransport {
    let id = NodeId::new(id);
    let client = match secret {
        Some(secret) => TcpBulkTransport::dial_only_with(id, secret),
        None => TcpBulkTransport::dial_only(id),
    }
    .expect("valid id");
    client.register_peer(
        server.local_id().clone(),
        server.local_addr().expect("server address"),
    );
    client
}

/// A genuine connection after the refused ones is the next accepted stream,
/// so no refused connection was ever queued.
async fn accept_genuine(server: &TcpBulkTransport) {
    let genuine = client(server, "genuine", Some(secret(1)));
    let (dialed, accepted) = timeout(SETTLE, async {
        tokio::join!(genuine.connect(server.local_id()), server.accept())
    })
    .await
    .expect("accept is not blocked");
    dialed.expect("genuine connect");
    assert_eq!(accepted.expect("genuine accept").0, NodeId::new("genuine"));
}

#[tokio::test]
async fn peers_with_the_same_secret_connect_and_stream() {
    let server = server(Some(secret(1))).await;
    let client = client(&server, "client", Some(secret(1)));
    let (dialed, accepted) = timeout(SETTLE, async {
        tokio::join!(client.connect(server.local_id()), server.accept())
    })
    .await
    .expect("accept is not blocked");
    let mut dialed = dialed.expect("connect");
    let (from, mut accepted) = accepted.expect("accept");
    assert_eq!(from, NodeId::new("client"));

    dialed.get_mut().write_all(b"frame").await.expect("write");
    let mut frame = [0; 5];
    timeout(SETTLE, accepted.get_mut().read_exact(&mut frame))
        .await
        .expect("bytes arrive")
        .expect("read");
    assert_eq!(&frame, b"frame");
    assert_eq!(server.auth_failures(), 0);
    assert_eq!(client.auth_failures(), 0);
}

#[tokio::test]
async fn a_peer_with_another_secret_is_refused() {
    let server = server(Some(secret(1))).await;
    let intruder = client(&server, "intruder", Some(secret(2)));
    let error = timeout(SETTLE, intruder.connect(server.local_id()))
        .await
        .expect("handshake ends")
        .expect_err("server tag refused");
    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    assert_eq!(intruder.auth_failures(), 1);
    eventually("the server counts the missing answer", || {
        server.auth_failures() == 1
    })
    .await;
    accept_genuine(&server).await;
}

#[tokio::test]
async fn a_peer_without_a_secret_is_refused() {
    let server = server(Some(secret(1))).await;
    let open = client(&server, "open", None);
    // An open hello expects no answer; the keyed server closes it unread.
    let mut dialed = timeout(SETTLE, open.connect(server.local_id()))
        .await
        .expect("open dial ends")
        .expect("open hello written");
    let mut byte = [0];
    let read = timeout(SETTLE, dialed.get_mut().read(&mut byte))
        .await
        .expect("server closes");
    assert!(matches!(read, Ok(0) | Err(_)));
    assert_eq!(server.auth_failures(), 1);
    accept_genuine(&server).await;
}

#[tokio::test]
async fn a_keyed_peer_is_refused_by_an_open_endpoint() {
    let server = server(None).await;
    let keyed = client(&server, "keyed", Some(secret(1)));
    timeout(SETTLE, keyed.connect(server.local_id()))
        .await
        .expect("handshake ends")
        .expect_err("no challenge comes back");
    assert_eq!(keyed.auth_failures(), 1);
    eventually("the open endpoint counts the keyed hello", || {
        server.auth_failures() == 1
    })
    .await;
}

/// The unversioned handshake, a bare id field claiming any identity, is
/// refused by a keyed endpoint.
#[tokio::test]
async fn the_unversioned_handshake_is_refused() {
    let server = server(Some(secret(1))).await;
    let mut raw = TcpStream::connect(server.local_addr().expect("address"))
        .await
        .expect("connect");
    raw.write_all(&[0, 0, 0, 7, b'g', b'e', b'n', b'u', b'i', b'n', b'e'])
        .await
        .expect("old handshake");
    let mut byte = [0];
    let read = timeout(SETTLE, raw.read(&mut byte))
        .await
        .expect("server closes");
    assert!(matches!(read, Ok(0) | Err(_)));
    assert_eq!(server.auth_failures(), 1);
    accept_genuine(&server).await;
}
