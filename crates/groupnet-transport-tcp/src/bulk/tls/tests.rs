//! The TLS layer and the admission exchange together, over in-memory pipes:
//! pins, per-connection client keys and the refusal of cleartext hellos in
//! both directions.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Mutex;

use groupnet_transport::admission::{AcceptedPeer, Admission, AdmissionError, JoinRequest};
use groupnet_transport::link::LinkFuture;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, duplex};

use super::*;

/// The credential [`Token`] admits.
pub(super) const TOKEN: &[u8] = b"letmein";

/// The client address the tests hand to the hub.
pub(super) const REMOTE: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 40_000);

pub(super) fn node(id: &str) -> NodeId {
    NodeId::new(id)
}

/// Admits a claim only with [`TOKEN`], recording the remote address it saw.
#[derive(Debug, Default)]
pub(super) struct Token {
    pub(super) seen: Mutex<Option<SocketAddr>>,
}

impl Admission for Token {
    fn admit<'a>(
        &'a self,
        request: JoinRequest<'a>,
    ) -> LinkFuture<'a, Result<AcceptedPeer, AdmissionError>> {
        Box::pin(async move {
            request.validate()?;
            *self.seen.lock().expect("seen") = request.remote;
            if request.credential == TOKEN {
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
pub(super) struct Rename;

impl Admission for Rename {
    fn admit<'a>(
        &'a self,
        _request: JoinRequest<'a>,
    ) -> LinkFuture<'a, Result<AcceptedPeer, AdmissionError>> {
        Box::pin(async { Ok(AcceptedPeer::new(NodeId::new("someone-else"))) })
    }
}

/// A client stream past TLS and admission, or the error that stopped it.
type Dialed = io::Result<tokio_rustls::client::TlsStream<DuplexStream>>;

/// A hub stream past TLS and admission, with the client's key and id.
type Admitted = io::Result<(
    NodeId,
    TcpTlsPublicKey,
    tokio_rustls::server::TlsStream<DuplexStream>,
)>;

async fn client(io: DuplexStream, key: &TcpTlsKey, pin: &TcpTlsPublicKey) -> Dialed {
    let provider = session::provider();
    let mut stream = session::connect(io, &provider, key, pin, REMOTE.ip()).await?;
    handshake::dial(&mut stream, &node("client"), TOKEN, &node("hub")).await?;
    Ok(stream)
}

async fn hub(io: DuplexStream, key: &TcpTlsKey, admission: &dyn Admission) -> Admitted {
    let acceptor = session::acceptor(&session::provider(), key)?;
    let (mut stream, client_key) = session::accept(io, &acceptor).await?;
    let from = handshake::accept(&mut stream, &node("hub"), admission, Some(REMOTE)).await?;
    Ok((from, client_key, stream))
}

/// Runs one client against one hub whose key is `hub_key`, the client
/// pinning `pin`.
async fn connect(
    hub_key: &TcpTlsKey,
    pin: &TcpTlsPublicKey,
    session_key: &TcpTlsKey,
) -> (Dialed, Admitted) {
    let (near, far) = duplex(16 * 1024);
    let policy = Token::default();
    let (dialed, admitted) =
        tokio::join!(client(near, session_key, pin), hub(far, hub_key, &policy));
    if admitted.is_ok() {
        assert_eq!(*policy.seen.lock().expect("seen"), Some(REMOTE));
    }
    (dialed, admitted)
}

#[tokio::test]
async fn a_pinned_hub_admits_a_client_and_bytes_flow_both_ways() {
    let hub_key = TcpTlsKey::generate().expect("hub key");
    let session_key = TcpTlsKey::generate().expect("session key");
    let (dialed, admitted) = connect(&hub_key, hub_key.public_key(), &session_key).await;
    let mut dialed = dialed.expect("client admitted");
    let (from, client_key, mut accepted) = admitted.expect("hub admits");
    assert_eq!(from, node("client"));
    assert_eq!(
        &client_key,
        session_key.public_key(),
        "the hub sees the session key"
    );
    assert_eq!(dialed.get_ref().1.alpn_protocol(), Some(session::ALPN));

    dialed.write_all(b"request").await.expect("write");
    dialed.flush().await.expect("flush");
    let mut request = [0; 7];
    accepted.read_exact(&mut request).await.expect("read");
    assert_eq!(&request, b"request");
    accepted.write_all(b"reply").await.expect("write");
    accepted.flush().await.expect("flush");
    let mut reply = [0; 5];
    dialed.read_exact(&mut reply).await.expect("read");
    assert_eq!(&reply, b"reply");
}

#[tokio::test]
async fn a_client_refuses_a_hub_whose_key_is_not_its_pin() {
    let hub_key = TcpTlsKey::generate().expect("hub key");
    let other = TcpTlsKey::generate().expect("other key");
    let session_key = TcpTlsKey::generate().expect("session key");
    let (dialed, admitted) = connect(&hub_key, other.public_key(), &session_key).await;
    let error = dialed.expect_err("wrong hub refused");
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    assert!(
        admitted.is_err(),
        "the hub never admits the refused session"
    );
}

#[tokio::test]
async fn a_hub_refuses_a_plain_hello_and_a_plain_acceptor_refuses_tls() {
    let hub_key = TcpTlsKey::generate().expect("hub key");
    let policy = Token::default();

    // A cleartext data-plane hello to a TLS hub.
    let (mut near, far) = duplex(16 * 1024);
    let (client_id, hub_id) = (node("client"), node("hub"));
    let (plain, admitted) = tokio::join!(
        crate::bulk::handshake::dial(&mut near, &client_id, &hub_id, None),
        hub(far, &hub_key, &policy),
    );
    plain.expect("the open hello is written");
    let Err(error) = admitted else {
        panic!("a plain hello must be refused");
    };
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(*policy.seen.lock().expect("seen"), None, "policy never ran");

    // A TLS client to a cleartext acceptor.
    let session_key = TcpTlsKey::generate().expect("session key");
    let (near, mut far) = duplex(16 * 1024);
    let (dialed, plain) = tokio::join!(client(near, &session_key, hub_key.public_key()), async {
        let accepted = crate::bulk::handshake::accept(&mut far, &node("hub"), None).await;
        drop(far);
        accepted
    },);
    assert_eq!(
        plain.expect_err("TLS refused").kind(),
        io::ErrorKind::InvalidData
    );
    assert!(dialed.is_err(), "the client gets no stream");
}

#[tokio::test]
async fn admission_refusals_reach_the_client_over_tls() {
    let hub_key = TcpTlsKey::generate().expect("hub key");
    let policy = Token::default();
    let (near, far) = duplex(16 * 1024);
    let (dialed, admitted) = tokio::join!(
        async {
            let provider = session::provider();
            let session_key = TcpTlsKey::generate().expect("session key");
            let mut stream = session::connect(
                near,
                &provider,
                &session_key,
                hub_key.public_key(),
                REMOTE.ip(),
            )
            .await?;
            handshake::dial(&mut stream, &node("client"), b"guess", &node("hub")).await
        },
        hub(far, &hub_key, &policy),
    );
    assert_eq!(
        dialed.expect_err("refused").kind(),
        io::ErrorKind::PermissionDenied
    );
    assert!(admitted.is_err());
}
