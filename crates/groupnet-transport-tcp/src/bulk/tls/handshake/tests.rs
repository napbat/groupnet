//! The admission exchange alone, over in-memory pipes (TLS is exercised in
//! `tls::tests`).

use groupnet_transport::MAX_NODE_ID_BYTES;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, duplex};

use super::super::tests::{REMOTE, Rename, TOKEN, Token, node};
use super::*;

async fn dialing(
    mut sock: DuplexStream,
    local: NodeId,
    credential: &[u8],
    peer: NodeId,
) -> io::Result<DuplexStream> {
    dial(&mut sock, &local, credential, &peer).await?;
    Ok(sock)
}

async fn accepting(
    mut sock: DuplexStream,
    local: NodeId,
    admission: &dyn Admission,
) -> io::Result<(NodeId, DuplexStream)> {
    let from = accept(&mut sock, &local, admission, Some(REMOTE)).await?;
    Ok((from, sock))
}

async fn handshake(
    credential: &[u8],
    admission: &dyn Admission,
) -> (io::Result<DuplexStream>, io::Result<(NodeId, DuplexStream)>) {
    let (near, far) = duplex(4096);
    tokio::join!(
        dialing(near, node("a"), credential, node("b")),
        accepting(far, node("b"), admission),
    )
}

/// Feeds `bytes` to a hub and returns its verdict, keeping the client end
/// open so a reader that waited for more bytes would hang, not fail.
async fn accept_raw(bytes: &[u8]) -> io::Error {
    let (mut near, far) = duplex(4096);
    near.write_all(bytes).await.expect("write");
    let policy = Token::default();
    let error = accepting(far, node("b"), &policy)
        .await
        .expect_err("malformed hello refused");
    assert_eq!(*policy.seen.lock().expect("seen"), None, "policy never ran");
    drop(near);
    error
}

/// A hello with a valid id `a`, then `rest`.
fn hello_then(rest: &[u8]) -> Vec<u8> {
    [&MAGIC[..], &[VERSION], &1u32.to_be_bytes(), b"a", rest].concat()
}

#[tokio::test]
async fn a_policy_admits_the_right_credential_and_bytes_flow() {
    let policy = Token::default();
    let (dialer, acceptor) = handshake(TOKEN, &policy).await;
    let mut dialer = dialer.expect("admitted dial");
    let (from, mut acceptor) = acceptor.expect("admitted accept");
    assert_eq!(from, node("a"));
    assert_eq!(*policy.seen.lock().expect("seen"), Some(REMOTE));

    dialer.write_all(b"frame").await.expect("write");
    let mut frame = [0; 5];
    acceptor.read_exact(&mut frame).await.expect("read");
    assert_eq!(&frame, b"frame");
}

#[tokio::test]
async fn a_refused_credential_fails_both_sides_with_permission_denied() {
    let (dialer, acceptor) = handshake(b"wrong", &Token::default()).await;
    assert_eq!(
        dialer.expect_err("refusal reported").kind(),
        io::ErrorKind::PermissionDenied
    );
    assert_eq!(
        acceptor.expect_err("refused").kind(),
        io::ErrorKind::PermissionDenied
    );
}

#[tokio::test]
async fn a_policy_that_renames_the_claim_is_refused() {
    let (dialer, acceptor) = handshake(TOKEN, &Rename).await;
    assert_eq!(
        acceptor.expect_err("renamed").kind(),
        io::ErrorKind::PermissionDenied
    );
    assert_eq!(
        dialer.expect_err("refusal reported").kind(),
        io::ErrorKind::PermissionDenied
    );
}

#[tokio::test]
async fn a_claim_of_the_hubs_own_id_is_refused() {
    let (near, far) = duplex(4096);
    let policy = Token::default();
    let (dialer, acceptor) = tokio::join!(
        dialing(near, node("b"), TOKEN, node("b")),
        accepting(far, node("b"), &policy),
    );
    assert_eq!(
        acceptor.expect_err("own id").kind(),
        io::ErrorKind::PermissionDenied
    );
    assert_eq!(
        dialer.expect_err("refused").kind(),
        io::ErrorKind::PermissionDenied
    );
}

#[tokio::test]
async fn a_client_refuses_a_hub_answering_for_another_id() {
    let (near, far) = duplex(4096);
    let policy = Token::default();
    let (dialer, acceptor) = tokio::join!(
        dialing(near, node("a"), TOKEN, node("elsewhere")),
        accepting(far, node("b"), &policy),
    );
    acceptor.expect("the hub admitted the claim");
    assert_eq!(
        dialer.expect_err("wrong hub").kind(),
        io::ErrorKind::PermissionDenied
    );
}

#[tokio::test]
async fn another_magic_or_version_is_invalid_data() {
    for hello in [
        [&b"GNBH"[..], &[VERSION]].concat(),
        [&MAGIC[..], &[VERSION + 1]].concat(),
    ] {
        assert_eq!(accept_raw(&hello).await.kind(), io::ErrorKind::InvalidData);
    }
}

#[tokio::test]
async fn oversized_fields_fail_before_their_body_is_read() {
    // An id length past the bound, with no body behind it.
    let length = u32::try_from(MAX_NODE_ID_BYTES + 1).expect("fits");
    let id = [&MAGIC[..], &[VERSION], &length.to_be_bytes()].concat();
    assert_eq!(accept_raw(&id).await.kind(), io::ErrorKind::InvalidData);
    // A credential length past the bound, with no body behind it.
    let length = u32::try_from(MAX_CREDENTIAL_BYTES + 1).expect("fits");
    let credential = hello_then(&length.to_be_bytes());
    assert_eq!(
        accept_raw(&credential).await.kind(),
        io::ErrorKind::InvalidData
    );
    // The largest announced length the header can carry.
    assert_eq!(
        accept_raw(&hello_then(&u32::MAX.to_be_bytes()))
            .await
            .kind(),
        io::ErrorKind::InvalidData
    );
}

#[tokio::test]
async fn an_unknown_status_is_invalid_data() {
    let (near, mut far) = duplex(4096);
    let (dialed, ()) = tokio::join!(dialing(near, node("a"), TOKEN, node("b")), async {
        let mut hello = [0; 5 + 4 + 1 + 4 + TOKEN.len()];
        far.read_exact(&mut hello).await.expect("hello");
        far.write_all(&[7]).await.expect("status");
    });
    assert_eq!(
        dialed.expect_err("unknown status").kind(),
        io::ErrorKind::InvalidData
    );
}
