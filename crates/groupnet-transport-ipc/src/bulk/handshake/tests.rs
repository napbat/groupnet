use groupnet_transport::admission::{AcceptedPeer, AdmissionError};
use groupnet_transport::link::LinkFuture;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, duplex};

use super::*;

fn node(id: &str) -> NodeId {
    NodeId::new(id)
}

/// Admits a claim only with the expected credential.
#[derive(Debug)]
struct Token(&'static [u8]);

impl Admission for Token {
    fn admit<'a>(
        &'a self,
        request: JoinRequest<'a>,
    ) -> LinkFuture<'a, Result<AcceptedPeer, AdmissionError>> {
        Box::pin(async move {
            request.validate()?;
            if request.credential == self.0 {
                Ok(AcceptedPeer::new(request.claimed.clone()))
            } else {
                Err(AdmissionError::CredentialRejected {
                    claimed: request.claimed.clone(),
                })
            }
        })
    }
}

/// Accepts every claim under another identity.
#[derive(Debug)]
struct Rename;

impl Admission for Rename {
    fn admit<'a>(
        &'a self,
        _request: JoinRequest<'a>,
    ) -> LinkFuture<'a, Result<AcceptedPeer, AdmissionError>> {
        Box::pin(async { Ok(AcceptedPeer::new(NodeId::new("someone-else"))) })
    }
}

/// Runs one side of a handshake and then drops its end of the pipe, as a
/// failed endpoint closes its connection.
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
    admission: Option<&dyn Admission>,
) -> io::Result<(NodeId, DuplexStream)> {
    let from = accept(&mut sock, &local, admission).await?;
    Ok((from, sock))
}

async fn handshake(
    credential: &[u8],
    admission: Option<&dyn Admission>,
) -> (io::Result<DuplexStream>, io::Result<(NodeId, DuplexStream)>) {
    let (near, far) = duplex(4096);
    tokio::join!(
        dialing(near, node("a"), credential, node("b")),
        accepting(far, node("b"), admission),
    )
}

/// Feeds `bytes` to an acceptor and returns its verdict, keeping the dialing
/// end open so a reader that waits for more bytes would hang, not fail.
async fn accept_raw(bytes: &[u8]) -> io::Error {
    let (mut near, far) = duplex(4096);
    near.write_all(bytes).await.expect("write");
    let error = accepting(far, node("b"), None)
        .await
        .expect_err("malformed hello refused");
    drop(near);
    error
}

#[tokio::test]
async fn an_open_acceptor_admits_any_claim_and_bytes_flow() {
    let (dialer, acceptor) = handshake(b"anything", None).await;
    let mut dialer = dialer.expect("dial");
    let (from, mut acceptor) = acceptor.expect("accept");
    assert_eq!(from, node("a"));

    dialer.write_all(b"frame").await.expect("write");
    let mut frame = [0; 5];
    acceptor.read_exact(&mut frame).await.expect("read");
    assert_eq!(&frame, b"frame");
    acceptor.write_all(b"back").await.expect("write");
    let mut back = [0; 4];
    dialer.read_exact(&mut back).await.expect("read");
    assert_eq!(&back, b"back");
}

#[tokio::test]
async fn a_policy_admits_the_right_credential() {
    let (dialer, acceptor) = handshake(b"letmein", Some(&Token(b"letmein"))).await;
    dialer.expect("admitted dial");
    assert_eq!(acceptor.expect("admitted accept").0, node("a"));
}

#[tokio::test]
async fn a_refused_credential_fails_both_sides_with_permission_denied() {
    let (dialer, acceptor) = handshake(b"wrong", Some(&Token(b"letmein"))).await;
    assert_eq!(
        dialer.expect_err("refusal reported").kind(),
        io::ErrorKind::PermissionDenied
    );
    let error = acceptor.expect_err("claim refused");
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    assert!(matches!(
        error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<AdmissionError>()),
        Some(AdmissionError::CredentialRejected { .. })
    ));
}

#[tokio::test]
async fn a_policy_that_renames_the_claim_is_refused() {
    let (dialer, acceptor) = handshake(b"", Some(&Rename)).await;
    assert_eq!(
        dialer.expect_err("refusal reported").kind(),
        io::ErrorKind::PermissionDenied
    );
    assert_eq!(
        acceptor.expect_err("renamed claim refused").kind(),
        io::ErrorKind::PermissionDenied
    );
}

#[tokio::test]
async fn a_dialer_claiming_the_acceptors_id_is_refused() {
    let (near, far) = duplex(4096);
    let (dialer, acceptor) = tokio::join!(
        dialing(near, node("b"), b"", node("b")),
        accepting(far, node("b"), None),
    );
    assert_eq!(
        dialer.expect_err("refusal reported").kind(),
        io::ErrorKind::PermissionDenied
    );
    assert_eq!(
        acceptor.expect_err("own id refused").kind(),
        io::ErrorKind::PermissionDenied
    );
}

#[tokio::test]
async fn an_acceptor_answering_for_another_id_is_refused() {
    let (near, far) = duplex(4096);
    let (dialer, acceptor) = tokio::join!(
        dialing(near, node("a"), b"", node("b")),
        accepting(far, node("c"), None),
    );
    assert_eq!(
        dialer.expect_err("wrong acceptor id").kind(),
        io::ErrorKind::PermissionDenied
    );
    // The acceptor finished its side; the dialer then closed the stream.
    acceptor.expect("acceptor answered");
}

#[tokio::test]
async fn another_magic_or_version_is_invalid_data() {
    let field = [0, 0, 0, 1, b'a', 0, 0, 0, 0];
    for hello in [
        [b'G', b'N', b'I', b'X', VERSION],
        [b'G', b'N', b'I', b'B', VERSION + 1],
        [b'G', b'N', b'I', b'1', VERSION],
    ] {
        let error = accept_raw(&[&hello[..], &field].concat()).await;
        assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{hello:?}");
    }
}

/// Every length prefix is checked against its bound before the body is read
/// or allocated: the acceptor fails at once although the dialer never sends
/// the announced bytes.
#[tokio::test]
async fn oversized_or_empty_fields_fail_before_allocation() {
    let hello = [b'G', b'N', b'I', b'B', VERSION];
    let over_id = u32::try_from(MAX_ID + 1).expect("fits");
    let over_credential = u32::try_from(MAX_CREDENTIAL_BYTES + 1).expect("fits");
    let cases: [Vec<u8>; 4] = [
        [&hello[..], &over_id.to_be_bytes()].concat(),
        [&hello[..], &u32::MAX.to_be_bytes()].concat(),
        [&hello[..], &0_u32.to_be_bytes()].concat(),
        [
            &hello[..],
            &[0, 0, 0, 1, b'a'],
            &over_credential.to_be_bytes(),
        ]
        .concat(),
    ];
    for bytes in cases {
        let error = accept_raw(&bytes).await;
        assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{bytes:?}");
    }
}

#[tokio::test]
async fn ids_and_credentials_at_their_bounds_are_admitted() {
    let long = "x".repeat(MAX_ID);
    let credential = vec![9; MAX_CREDENTIAL_BYTES];
    let (near, far) = duplex(4096);
    let (dialer, acceptor) = tokio::join!(
        dialing(near, node(&long), &credential, node("b")),
        accepting(far, node("b"), None),
    );
    dialer.expect("dial at the bounds");
    assert_eq!(acceptor.expect("accept at the bounds").0, node(&long));
}

#[tokio::test]
async fn an_unknown_status_is_invalid_data() {
    let (near, mut far) = duplex(4096);
    let answer = async {
        let mut hello = [0; 5 + 4 + 1 + 4];
        far.read_exact(&mut hello).await.expect("hello");
        far.write_all(&[7]).await.expect("status");
        far
    };
    let (dialer, _far) = tokio::join!(dialing(near, node("a"), b"", node("b")), answer);
    assert_eq!(
        dialer.expect_err("unknown status").kind(),
        io::ErrorKind::InvalidData
    );
}

#[tokio::test]
async fn the_hello_and_answer_keep_their_wire_layout() {
    let (near, mut far) = duplex(4096);
    let peer = async {
        let mut hello = [0; 5 + 4 + 2 + 4 + 3];
        far.read_exact(&mut hello).await.expect("hello");
        far.write_all(&[ACCEPTED, 0, 0, 0, 1, b'b'])
            .await
            .expect("answer");
        hello
    };
    let (dialer, hello) = tokio::join!(dialing(near, node("ab"), b"key", node("b")), peer);
    dialer.expect("dial");
    assert_eq!(
        hello,
        [
            b'G', b'N', b'I', b'B', VERSION, 0, 0, 0, 2, b'a', b'b', 0, 0, 0, 3, b'k', b'e', b'y'
        ]
    );

    let (mut near, far) = duplex(4096);
    near.write_all(&[
        b'G', b'N', b'I', b'B', VERSION, 0, 0, 0, 1, b'a', 0, 0, 0, 0,
    ])
    .await
    .expect("hello");
    let (from, _far) = accepting(far, node("b"), None).await.expect("accept");
    assert_eq!(from, node("a"));
    let mut answer = [0; 6];
    near.read_exact(&mut answer).await.expect("answer");
    assert_eq!(answer, [ACCEPTED, 0, 0, 0, 1, b'b']);

    let (mut near, far) = duplex(4096);
    near.write_all(&[
        b'G', b'N', b'I', b'B', VERSION, 0, 0, 0, 1, b'a', 0, 0, 0, 0,
    ])
    .await
    .expect("hello");
    accepting(far, node("b"), Some(&Token(b"key")))
        .await
        .expect_err("refused");
    let mut refusal = Vec::new();
    near.read_to_end(&mut refusal).await.expect("refusal");
    assert_eq!(refusal, [REFUSED]);
}
