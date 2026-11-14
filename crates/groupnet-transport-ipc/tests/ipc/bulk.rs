//! Real local-IPC data-plane tests: a hub admits clients it was never
//! configured with through its admission policy, clients only dial, and
//! handshakes are bounded in time and number. No mocked streams or fixed
//! names.

use std::io;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use groupnet_core::NodeId;
use groupnet_rpc::{RpcClient, RpcError, RpcServer};
use groupnet_testkit::cluster::eventually_within;
use groupnet_transport::Bytes;
use groupnet_transport::admission::{AcceptedPeer, AdmissionError, JoinRequest};
use groupnet_transport::bulk::{BulkTransport, DataPlane};
use groupnet_transport::link::LinkFuture;
// The adapter re-exports the shared types its own signatures name.
use groupnet_transport_ipc::{
    Admission, IpcAddress, IpcAddressError, IpcBulkConfig, IpcBulkConfigError, IpcBulkDialConfig,
    IpcBulkTransport, IpcPeerError, NodeIdError, QueueCapacity,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;

use super::address::Address;

/// Failure-report bound for IPC progress; no test waits on it to pass.
const SETTLE: Duration = Duration::from_secs(5);

/// How long a test watches for something that must not happen.
const QUIET: Duration = Duration::from_millis(200);

/// The credential the hub's policy admits.
const TOKEN: &[u8] = b"letmein";

/// Recognizes the expected typed address rejection.
type AddressCheck = fn(&IpcAddressError) -> bool;

/// A raw connection that speaks no handshake at all. A pipe whose listener is
/// replacing its pending instance answers busy; polling for a free instance
/// is bounded by [`SETTLE`].
#[cfg(windows)]
async fn raw(address: &Address) -> tokio::net::windows::named_pipe::NamedPipeClient {
    /// `ERROR_PIPE_BUSY`.
    const BUSY: i32 = 231;
    let IpcAddress::NamedPipe(name) = &address.ipc;
    let mut opened = None;
    eventually_within("a free pipe instance", SETTLE, || {
        match tokio::net::windows::named_pipe::ClientOptions::new().open(name) {
            Ok(pipe) => opened = Some(pipe),
            Err(error) if error.raw_os_error() == Some(BUSY) => {}
            Err(error) => panic!("raw connect: {error}"),
        }
        opened.is_some()
    })
    .await;
    opened.expect("opened")
}

/// A raw connection that speaks no handshake at all.
#[cfg(unix)]
async fn raw(address: &Address) -> tokio::net::UnixStream {
    let IpcAddress::Unix(path) = &address.ipc;
    tokio::net::UnixStream::connect(path)
        .await
        .expect("raw connect")
}

/// Admits a claim only with [`TOKEN`].
#[derive(Debug)]
struct Token;

impl Admission for Token {
    fn admit<'a>(
        &'a self,
        request: JoinRequest<'a>,
    ) -> LinkFuture<'a, Result<AcceptedPeer, AdmissionError>> {
        Box::pin(async move {
            request.validate()?;
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

/// The hub's identity.
static HUB: LazyLock<NodeId> = LazyLock::new(|| NodeId::new("hub"));

/// A hub whose policy admits only [`TOKEN`].
fn hub(address: &Address, config: IpcBulkConfig) -> IpcBulkTransport {
    let admission: Arc<dyn Admission> = Arc::new(Token);
    IpcBulkTransport::bind_with(
        HUB.clone(),
        &address.ipc,
        IpcBulkConfig {
            admission: Some(admission),
            ..config
        },
    )
    .expect("bind hub")
}

/// A dial-only client presenting `credential`, knowing only the hub.
fn client(id: &str, credential: &[u8], hub: &Address) -> IpcBulkTransport {
    let client = IpcBulkTransport::dial_only(
        NodeId::new(id),
        IpcBulkDialConfig {
            credential: credential.to_vec(),
            ..IpcBulkDialConfig::default()
        },
    )
    .expect("valid client");
    client
        .register_peer(HUB.clone(), hub.ipc.clone())
        .expect("hub address");
    client
}

fn kind_of<E: std::error::Error + 'static>(error: &io::Error) -> Option<&E> {
    error.get_ref().and_then(|inner| inner.downcast_ref::<E>())
}

#[tokio::test]
async fn a_hub_admits_only_the_right_credential_and_frames_flow_both_ways() {
    let address = Address::new();
    let hub = DataPlane::new(hub(&address, IpcBulkConfig::default()));
    let good = DataPlane::new(client("good", TOKEN, &address));
    let bad = DataPlane::new(client("bad", b"guess", &address));

    let (dialed, accepted) = timeout(SETTLE, async {
        tokio::join!(good.connect(&HUB), hub.accept())
    })
    .await
    .expect("admitted promptly");
    let mut dialed = dialed.expect("good client admitted");
    let (from, mut accepted) = accepted.expect("hub accepts");
    assert_eq!(from, NodeId::new("good"));

    for payload in [Vec::new(), b"request".to_vec(), vec![0x5a; 200_000]] {
        dialed
            .send(Bytes::from(payload.clone()))
            .await
            .expect("client sends");
        let received = timeout(SETTLE, accepted.recv())
            .await
            .expect("hub receives promptly")
            .expect("hub reads")
            .expect("frame, not end of stream");
        assert_eq!(received.as_ref(), payload);
        accepted.send(received).await.expect("hub answers");
        let echoed = timeout(SETTLE, dialed.recv())
            .await
            .expect("client receives promptly")
            .expect("client reads")
            .expect("frame, not end of stream");
        assert_eq!(echoed.as_ref(), payload);
    }

    let refused = timeout(SETTLE, bad.connect(&HUB))
        .await
        .expect("refusal is prompt")
        .expect_err("wrong credential refused");
    assert_eq!(refused.kind(), io::ErrorKind::PermissionDenied);
    assert_eq!(bad.transport().auth_failures(), 1);
    eventually_within("hub counts the refusal", SETTLE, || {
        hub.transport().auth_failures() == 1
    })
    .await;
    assert_eq!(good.transport().auth_failures(), 0);

    // The refused client never reached the accept queue: the next stream the
    // hub hands out is the good client's second one.
    let (second, next) = timeout(SETTLE, async {
        tokio::join!(good.connect(&HUB), hub.accept())
    })
    .await
    .expect("second admission is prompt");
    second.expect("second connection");
    assert_eq!(next.expect("hub accepts").0, NodeId::new("good"));
}

#[tokio::test]
async fn a_dial_only_client_calls_the_hub_over_rpc() {
    let address = Address::new();
    let server = RpcServer::bind(
        DataPlane::new(hub(&address, IpcBulkConfig::default())),
        |from: NodeId, request: Bytes| async move {
            let mut reply = format!("{from}:").into_bytes();
            reply.extend_from_slice(&request);
            Ok(Bytes::from(reply))
        },
    );
    let caller = RpcClient::new(DataPlane::new(client("caller", TOKEN, &address)));
    for request in [&b"ping"[..], b"again"] {
        let reply = caller
            .call(&HUB, Bytes::copy_from_slice(request), SETTLE)
            .await
            .expect("hub answers");
        assert_eq!(reply.as_ref(), [b"caller:", request].concat());
    }

    let intruder = RpcClient::new(DataPlane::new(client("intruder", b"guess", &address)));
    assert_eq!(
        intruder
            .call(&HUB, Bytes::from_static(b"ping"), SETTLE)
            .await,
        Err(RpcError::Unreachable)
    );
    caller.shutdown();
    intruder.shutdown();
    server.shutdown();
}

#[tokio::test]
async fn an_open_hub_admits_every_local_peer_and_checks_its_own_identity() {
    let address = Address::new();
    let hub = IpcBulkTransport::bind(HUB.clone(), &address.ipc).expect("open hub");
    let anyone = client("anyone", b"whatever", &address);
    let (dialed, accepted) = timeout(SETTLE, async {
        tokio::join!(anyone.connect(&HUB), hub.accept())
    })
    .await
    .expect("prompt");
    dialed.expect("open hub admits");
    assert_eq!(accepted.expect("accepted").0, NodeId::new("anyone"));

    // A client that believes another node listens there is refused by its
    // own check of the acceptor's id.
    let confused =
        IpcBulkTransport::dial_only(NodeId::new("confused"), IpcBulkDialConfig::default())
            .expect("valid client");
    let elsewhere = NodeId::new("elsewhere");
    confused
        .register_peer(elsewhere.clone(), address.ipc.clone())
        .expect("address");
    let (dialed, _) = tokio::join!(
        timeout(SETTLE, confused.connect(&elsewhere)),
        timeout(QUIET, hub.accept()),
    );
    assert_eq!(
        dialed.expect("prompt").expect_err("wrong acceptor").kind(),
        io::ErrorKind::PermissionDenied
    );
    assert_eq!(confused.auth_failures(), 1);
}

#[tokio::test]
async fn a_stalled_or_malformed_handshake_is_dropped_without_blocking_others() {
    let address = Address::new();
    let hub = hub(
        &address,
        IpcBulkConfig {
            handshake_timeout: Duration::from_millis(300),
            ..IpcBulkConfig::default()
        },
    );
    // Says nothing: its handshake can only time out.
    let _stalled = raw(&address).await;
    // Speaks another protocol: refused at once.
    let mut wrong = raw(&address).await;
    wrong
        .write_all(b"GNI1\x01a")
        .await
        .expect("write a foreign hello");

    let good = client("good", TOKEN, &address);
    let (dialed, accepted) = timeout(SETTLE, async {
        tokio::join!(good.connect(&HUB), hub.accept())
    })
    .await
    .expect("the stalled peer does not block accepts");
    dialed.expect("good client admitted");
    assert_eq!(accepted.expect("accepted").0, NodeId::new("good"));

    let mut byte = [0];
    let read = timeout(SETTLE, wrong.read(&mut byte))
        .await
        .expect("the malformed peer is closed");
    assert!(matches!(read, Ok(0) | Err(_)), "{read:?}");
    eventually_within("both failed handshakes counted", SETTLE, || {
        hub.auth_failures() == 2
    })
    .await;
}

#[tokio::test]
async fn max_connections_holds_accepts_until_a_stream_is_dropped() {
    let address = Address::new();
    let hub = hub(
        &address,
        IpcBulkConfig {
            max_connections: QueueCapacity::of(1),
            ..IpcBulkConfig::default()
        },
    );
    let first = client("first", TOKEN, &address);
    let (dialed, accepted) = timeout(SETTLE, async {
        tokio::join!(first.connect(&HUB), hub.accept())
    })
    .await
    .expect("first admission is prompt");
    let first_stream = dialed.expect("first admitted");
    let (_, held) = accepted.expect("accepted");

    let second = client("second", TOKEN, &address);
    let pending = tokio::spawn(async move {
        let stream = second.connect(&HUB).await;
        (second, stream)
    });
    assert!(
        timeout(QUIET, hub.accept()).await.is_err(),
        "a second connection is accepted only once the first ends"
    );

    drop(held);
    drop(first_stream);
    let (from, _stream) = timeout(SETTLE, hub.accept())
        .await
        .expect("the freed slot admits the waiting client")
        .expect("accepted");
    assert_eq!(from, NodeId::new("second"));
    let (second, stream) = timeout(SETTLE, pending)
        .await
        .expect("the waiting client completes")
        .expect("task");
    stream.expect("second admitted");
    assert_eq!(second.auth_failures(), 0);
}

#[tokio::test]
async fn a_dial_only_endpoint_never_accepts_and_dials_only_registered_peers() {
    let client = IpcBulkTransport::dial_only(NodeId::new("client"), IpcBulkDialConfig::default())
        .expect("valid client");
    assert!(timeout(QUIET, client.accept()).await.is_err());
    let error = client
        .connect(&NodeId::new("nobody"))
        .await
        .expect_err("unregistered peer");
    assert_eq!(error.kind(), io::ErrorKind::NotFound);
    assert_eq!(client.auth_failures(), 0);
}

#[tokio::test]
async fn binders_validate_everything_before_binding() {
    let address = Address::new();
    let error = IpcBulkTransport::bind_with(
        HUB.clone(),
        &address.ipc,
        IpcBulkConfig {
            handshake_timeout: Duration::ZERO,
            ..IpcBulkConfig::default()
        },
    )
    .expect_err("zero deadline");
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        kind_of::<IpcBulkConfigError>(&error),
        Some(&IpcBulkConfigError::ZeroHandshakeTimeout)
    );
    for id in [String::new(), "x".repeat(65)] {
        let error = IpcBulkTransport::bind(NodeId::new(id), &address.ipc).expect_err("bad id");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(kind_of::<NodeIdError>(&error).is_some(), "{error}");
    }
    // Nothing was bound by the refused attempts.
    let hub = IpcBulkTransport::bind(HUB.clone(), &address.ipc).expect("address still free");
    // A second listener cannot take the address over.
    IpcBulkTransport::bind(NodeId::new("other"), &address.ipc).expect_err("address in use");
    drop(hub);
}

#[test]
fn dial_only_validates_and_binding_needs_a_runtime() {
    let error = IpcBulkTransport::dial_only(
        NodeId::new("client"),
        IpcBulkDialConfig {
            credential: vec![0; groupnet_transport::admission::MAX_CREDENTIAL_BYTES + 1],
            ..IpcBulkDialConfig::default()
        },
    )
    .expect_err("credential above the bound");
    assert!(matches!(
        kind_of::<IpcBulkConfigError>(&error),
        Some(IpcBulkConfigError::CredentialTooLong { .. })
    ));
    let error = IpcBulkTransport::dial_only(NodeId::new(""), IpcBulkDialConfig::default())
        .expect_err("empty id");
    assert!(kind_of::<NodeIdError>(&error).is_some(), "{error}");

    let address = Address::new();
    IpcBulkTransport::bind(HUB.clone(), &address.ipc).expect_err("no Tokio runtime");
}

#[test]
fn register_peer_names_each_rejection() {
    let client = IpcBulkTransport::dial_only(NodeId::new("client"), IpcBulkDialConfig::default())
        .expect("valid client");
    let address = Address::new();
    let error = client
        .register_peer(NodeId::new("x".repeat(65)), address.ipc.clone())
        .expect_err("long id");
    assert!(matches!(
        kind_of::<IpcPeerError>(&error),
        Some(IpcPeerError::Identity(NodeIdError::TooLong { .. }))
    ));
    let error = client
        .register_peer(NodeId::new("client"), address.ipc.clone())
        .expect_err("own id");
    assert!(matches!(
        kind_of::<IpcPeerError>(&error),
        Some(IpcPeerError::Local)
    ));
    #[cfg(windows)]
    let (foreign, expected): (_, AddressCheck) = (
        IpcAddress::NamedPipe(r"\\server\pipe\hub".to_owned()),
        |error| matches!(error, IpcAddressError::NotLocalPipe),
    );
    #[cfg(unix)]
    let (foreign, expected): (_, AddressCheck) =
        (IpcAddress::Unix("relative/socket".into()), |error| {
            matches!(error, IpcAddressError::RelativePath)
        });
    let error = client
        .register_peer(HUB.clone(), foreign)
        .expect_err("non-local address");
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(
        matches!(kind_of::<IpcPeerError>(&error), Some(IpcPeerError::Address(address)) if expected(address)),
        "{error}"
    );
    client
        .register_peer(HUB.clone(), address.ipc.clone())
        .expect("valid peer");
}
