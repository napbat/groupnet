//! `TcpBulkTransport<TcpTls>` over real loopback TCP: a hub admits clients it
//! was never configured with through its admission policy, each client dials
//! with a fresh key per connection and pins the hub's key, frames, RPC calls
//! and a subscription flow over the admitted streams, and every refusal (a
//! wrong credential, a wrong pin, a cleartext peer on either side) closes the
//! connection before a stream byte flows.

#![cfg(feature = "tls")]

use std::io;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use groupnet_core::NodeId;
use groupnet_rpc::{RpcClient, RpcError, RpcServer, RpcStatus, SubscriptionSink};
use groupnet_testkit::cluster::eventually_within;
use groupnet_transport::Bytes;
use groupnet_transport::admission::{AcceptedPeer, AdmissionError, JoinRequest};
use groupnet_transport::bulk::{BulkTransport, DataPlane};
use groupnet_transport::link::LinkFuture;
// The adapter re-exports the shared types its own signatures name.
use groupnet_transport_tcp::{
    Admission, QueueCapacity, TcpBulkTransport, TcpTls, TcpTlsConfig, TcpTlsConfigError,
    TcpTlsDialConfig, TcpTlsKey, TcpTlsPublicKey,
};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::timeout;

/// Failure-report bound for socket progress; no test waits on it to pass.
const SETTLE: Duration = Duration::from_secs(5);

/// How long a test watches for something that must not happen.
const QUIET: Duration = Duration::from_millis(200);

/// The credential the hub's policy admits.
const TOKEN: &[u8] = b"letmein";

/// The hub's identity.
static HUB: LazyLock<NodeId> = LazyLock::new(|| NodeId::new("hub"));

/// Admits any identity that presents [`TOKEN`] from a loopback address.
#[derive(Debug)]
struct Token;

impl Admission for Token {
    fn admit<'a>(
        &'a self,
        request: JoinRequest<'a>,
    ) -> LinkFuture<'a, Result<AcceptedPeer, AdmissionError>> {
        Box::pin(async move {
            request.validate()?;
            let local = request.remote.is_some_and(|addr| addr.ip().is_loopback());
            if local && request.credential == TOKEN {
                Ok(AcceptedPeer::new(request.claimed.clone()))
            } else {
                Err(AdmissionError::CredentialRejected {
                    claimed: request.claimed.clone(),
                })
            }
        })
    }
}

/// A dial-only client presenting `credential` that knows only the hub's
/// address and pinned key.
fn client(
    id: &str,
    credential: &[u8],
    hub: &TcpBulkTransport<TcpTls>,
    pin: TcpTlsPublicKey,
) -> TcpBulkTransport<TcpTls> {
    let client = TcpBulkTransport::dial_only_tls(
        NodeId::new(id),
        TcpTlsDialConfig {
            credential: credential.to_vec(),
            ..TcpTlsDialConfig::default()
        },
    )
    .expect("valid client");
    client.register_peer(HUB.clone(), hub.local_addr().expect("hub address"), pin);
    client
}

/// A hub with a fresh long-lived key whose policy admits only [`TOKEN`], and
/// the key a client pins for it, as read from the published encoding.
async fn pinned_hub(
    refine: impl FnOnce(TcpTlsConfig) -> TcpTlsConfig,
) -> (TcpBulkTransport<TcpTls>, TcpTlsPublicKey) {
    let config = hub_config(refine);
    let pin = TcpTlsPublicKey::from_der(config.key.public_key().as_der()).expect("pin");
    let hub = TcpBulkTransport::bind_tls(HUB.clone(), "127.0.0.1:0", config)
        .await
        .expect("bind hub");
    (hub, pin)
}

#[tokio::test]
async fn a_hub_admits_two_unknown_clients_and_frames_flow_both_ways() {
    let (hub, pin) = pinned_hub(|config| config).await;
    let hub = DataPlane::new(hub);
    let alpha = DataPlane::new(client("alpha", TOKEN, hub.transport(), pin.clone()));
    let beta = DataPlane::new(client("beta", TOKEN, hub.transport(), pin.clone()));

    for (name, client) in [("alpha", &alpha), ("beta", &beta)] {
        let (dialed, accepted) = timeout(SETTLE, async {
            tokio::join!(client.connect(&HUB), hub.accept())
        })
        .await
        .expect("admitted promptly");
        let mut dialed = dialed.expect("client admitted");
        let (from, mut accepted) = accepted.expect("hub accepts");
        assert_eq!(from, NodeId::new(name));
        for payload in [Vec::new(), b"request".to_vec(), vec![0x5a; 300_000]] {
            // Each side writes while the other reads: a frame larger than
            // the socket buffers completes only then.
            let (sent, received) = timeout(SETTLE, async {
                tokio::join!(dialed.send(Bytes::from(payload.clone())), accepted.recv())
            })
            .await
            .expect("hub receives promptly");
            sent.expect("client sends");
            let received = received.expect("hub reads").expect("a frame");
            assert_eq!(received.as_ref(), payload);
            let (answered, echoed) = timeout(SETTLE, async {
                tokio::join!(accepted.send(received), dialed.recv())
            })
            .await
            .expect("client receives promptly");
            answered.expect("hub answers");
            let echoed = echoed.expect("client reads").expect("a frame");
            assert_eq!(echoed.as_ref(), payload);
        }
    }
    assert_eq!(hub.transport().auth_failures(), 0);
    assert_eq!(alpha.transport().auth_failures(), 0);
}

#[tokio::test]
async fn every_connection_has_its_own_client_key_and_the_hub_its_pinned_one() {
    let (hub, pin) = pinned_hub(|config| config).await;
    let alpha = client("alpha", TOKEN, &hub, pin.clone());
    let mut keys = Vec::new();
    for _ in 0..2 {
        let (dialed, accepted) = timeout(SETTLE, async {
            tokio::join!(alpha.connect(&HUB), hub.accept())
        })
        .await
        .expect("prompt");
        let dialed = dialed.expect("admitted");
        let (_, accepted) = accepted.expect("accepted");
        assert_eq!(dialed.get_ref().peer_key(), &pin);
        assert!(
            accepted
                .get_ref()
                .peer_addr()
                .expect("address")
                .ip()
                .is_loopback()
        );
        assert_eq!(
            dialed.get_ref().peer_addr().expect("address"),
            hub.local_addr().expect("hub address")
        );
        keys.push(accepted.get_ref().peer_key().clone());
    }
    assert_ne!(keys[0], keys[1], "a fresh key per connection");
    assert_ne!(keys[0], pin);
}

#[tokio::test]
async fn a_wrong_credential_or_a_wrong_pin_is_refused_and_counted() {
    let (hub, pin) = pinned_hub(|config| config).await;
    let intruder = client("intruder", b"guess", &hub, pin.clone());
    let (refused, _) = tokio::join!(
        timeout(SETTLE, intruder.connect(&HUB)),
        timeout(QUIET, hub.accept()),
    );
    assert_eq!(
        refused
            .expect("refusal is prompt")
            .expect_err("wrong credential")
            .kind(),
        io::ErrorKind::PermissionDenied
    );
    assert_eq!(intruder.auth_failures(), 1);
    eventually_within("hub counts the refused credential", SETTLE, || {
        hub.auth_failures() == 1
    })
    .await;

    // A client that pinned another key refuses this hub before it sends its
    // claim or credential.
    let other = TcpTlsKey::generate().expect("other key");
    let misled = client("misled", TOKEN, &hub, other.public_key().clone());
    let (refused, _) = tokio::join!(
        timeout(SETTLE, misled.connect(&HUB)),
        timeout(QUIET, hub.accept()),
    );
    assert_eq!(
        refused
            .expect("refusal is prompt")
            .expect_err("wrong pin")
            .kind(),
        io::ErrorKind::PermissionDenied
    );
    assert_eq!(misled.auth_failures(), 1);
    eventually_within("hub counts the aborted TLS handshake", SETTLE, || {
        hub.auth_failures() == 2
    })
    .await;

    // Neither refusal was queued: the next stream is the genuine client's.
    let genuine = client("genuine", TOKEN, &hub, pin);
    let (dialed, accepted) = timeout(SETTLE, async {
        tokio::join!(genuine.connect(&HUB), hub.accept())
    })
    .await
    .expect("prompt");
    dialed.expect("genuine client admitted");
    assert_eq!(accepted.expect("accepted").0, NodeId::new("genuine"));
}

#[tokio::test]
async fn tls_and_plain_endpoints_refuse_each_other() {
    let (hub, pin) = pinned_hub(|config| config).await;
    // A cleartext client claims an id at the TLS hub.
    let plain = TcpBulkTransport::dial_only(NodeId::new("plain")).expect("plain client");
    plain.register_peer(HUB.clone(), hub.local_addr().expect("hub address"));
    let (dialed, accepted) = tokio::join!(
        timeout(SETTLE, plain.connect(&HUB)),
        timeout(QUIET, hub.accept()),
    );
    // The open hello needs no answer, so only the hub can refuse it: it
    // never hands the stream on, and the dialer's stream ends.
    assert!(accepted.is_err(), "the TLS hub queued a cleartext peer");
    if let Ok(Ok(stream)) = dialed {
        let mut stream = stream.into_inner();
        let _ = stream.write_all(b"after the refusal").await;
    }
    eventually_within("hub counts the cleartext hello", SETTLE, || {
        hub.auth_failures() == 1
    })
    .await;

    // A TLS client dials a cleartext endpoint.
    let open = TcpBulkTransport::bind(HUB.clone(), "127.0.0.1:0")
        .await
        .expect("open endpoint");
    let tls = TcpBulkTransport::dial_only_tls(NodeId::new("tls"), TcpTlsDialConfig::default())
        .expect("TLS client");
    tls.register_peer(HUB.clone(), open.local_addr().expect("address"), pin);
    let (dialed, accepted) = tokio::join!(
        timeout(SETTLE, tls.connect(&HUB)),
        timeout(QUIET, open.accept()),
    );
    assert!(accepted.is_err(), "the cleartext endpoint queued TLS");
    dialed
        .expect("refusal is prompt")
        .expect_err("cleartext endpoint refused");
    assert_eq!(tls.auth_failures(), 1);
    eventually_within("open endpoint counts the TLS hello", SETTLE, || {
        open.auth_failures() == 1
    })
    .await;
}

#[tokio::test]
async fn dial_only_clients_call_and_subscribe_over_rpc() {
    let (hub, pin) = pinned_hub(|config| config).await;
    let address = hub.local_addr().expect("hub address");
    let server = RpcServer::with_subscriptions(
        |from: NodeId, request: Bytes| async move {
            let mut reply = format!("{from}:").into_bytes();
            reply.extend_from_slice(&request);
            Ok::<_, RpcStatus>(Bytes::from(reply))
        },
        |from: NodeId, _request: Bytes, mut events: SubscriptionSink| async move {
            for n in 1..=3 {
                if events
                    .send(Bytes::from(format!("event {n} for {from}")))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            Ok::<(), RpcStatus>(())
        },
    )
    .accept_on(DataPlane::new(hub));

    let dial = |id: &str, credential: &[u8]| {
        let transport = TcpBulkTransport::dial_only_tls(
            NodeId::new(id),
            TcpTlsDialConfig {
                credential: credential.to_vec(),
                ..TcpTlsDialConfig::default()
            },
        )
        .expect("valid client");
        transport.register_peer(HUB.clone(), address, pin.clone());
        RpcClient::new(DataPlane::new(transport))
    };
    for name in ["alpha", "beta"] {
        let caller = dial(name, TOKEN);
        let reply = caller
            .call(&HUB, Bytes::from_static(b"ping"), SETTLE)
            .await
            .expect("hub answers");
        assert_eq!(reply.as_ref(), format!("{name}:ping").as_bytes());
        let mut events = caller
            .subscribe(&HUB, Bytes::new(), SETTLE)
            .await
            .expect("subscribed");
        for n in 1..=3 {
            let event = timeout(SETTLE, events.next())
                .await
                .expect("event promptly")
                .expect("an event, not the end")
                .expect("event, not an error");
            assert_eq!(event.as_ref(), format!("event {n} for {name}").as_bytes());
        }
        assert!(
            timeout(SETTLE, events.next())
                .await
                .expect("end promptly")
                .is_none()
        );
        caller.shutdown();
    }

    let intruder = dial("intruder", b"guess");
    assert_eq!(
        intruder
            .call(&HUB, Bytes::from_static(b"ping"), SETTLE)
            .await,
        Err(RpcError::Unreachable)
    );
    intruder.shutdown();
    server.shutdown();
}

#[tokio::test]
async fn max_connections_holds_accepts_until_a_stream_is_dropped() {
    let (hub, pin) = pinned_hub(|config| TcpTlsConfig {
        max_connections: QueueCapacity::of(1),
        ..config
    })
    .await;
    let alpha = client("alpha", TOKEN, &hub, pin);
    let (dialed, accepted) = timeout(SETTLE, async {
        tokio::join!(alpha.connect(&HUB), hub.accept())
    })
    .await
    .expect("prompt");
    let _first = dialed.expect("first admitted");
    let (_, first) = accepted.expect("first accepted");

    // The second connection waits in the kernel backlog: its TLS handshake
    // cannot start while the only connection slot is held.
    let second = alpha.connect(&HUB);
    tokio::pin!(second);
    assert!(
        timeout(QUIET, &mut second).await.is_err(),
        "a second connection was admitted at the bound"
    );
    drop(first);
    let (dialed, accepted) = timeout(SETTLE, async { tokio::join!(second, hub.accept()) })
        .await
        .expect("admitted once the slot frees");
    dialed.expect("second admitted");
    assert_eq!(accepted.expect("accepted").0, NodeId::new("alpha"));
}

#[tokio::test]
async fn a_stalled_handshake_times_out_without_blocking_others() {
    let (hub, pin) = pinned_hub(|config| TcpTlsConfig {
        handshake_timeout: Duration::from_millis(300),
        ..config
    })
    .await;
    // Says nothing: its handshake can only time out.
    let _stalled = TcpStream::connect(hub.local_addr().expect("address"))
        .await
        .expect("raw connect");
    let alpha = client("alpha", TOKEN, &hub, pin);
    let (dialed, accepted) = timeout(SETTLE, async {
        tokio::join!(alpha.connect(&HUB), hub.accept())
    })
    .await
    .expect("not blocked by the stalled peer");
    dialed.expect("admitted");
    assert_eq!(accepted.expect("accepted").0, NodeId::new("alpha"));
    eventually_within("the stalled handshake is counted", SETTLE, || {
        hub.auth_failures() == 1
    })
    .await;
}

#[tokio::test]
async fn binders_validate_before_any_socket_and_dialers_need_no_runtime() {
    let zero = hub_config(|config| TcpTlsConfig {
        handshake_timeout: Duration::ZERO,
        ..config
    });
    let error = TcpBulkTransport::bind_tls(HUB.clone(), "127.0.0.1:0", zero)
        .await
        .expect_err("zero deadline");
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<TcpTlsConfigError>()),
        Some(&TcpTlsConfigError::ZeroHandshakeTimeout)
    );
    let long = vec![0; groupnet_transport::admission::MAX_CREDENTIAL_BYTES + 1];
    let error = TcpBulkTransport::bind_tls(
        HUB.clone(),
        "127.0.0.1:0",
        hub_config(|config| TcpTlsConfig {
            credential: long.clone(),
            ..config
        }),
    )
    .await
    .expect_err("long credential");
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    let error = TcpBulkTransport::bind_tls(NodeId::new(""), "127.0.0.1:0", hub_config(|c| c))
        .await
        .expect_err("empty id");
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);

    // Made outside any runtime; its accept never completes.
    let dialer = std::thread::spawn(|| {
        TcpBulkTransport::dial_only_tls(NodeId::new("client"), TcpTlsDialConfig::default())
    })
    .join()
    .expect("thread")
    .expect("no runtime needed");
    assert!(dialer.local_addr().is_none());
    assert!(timeout(QUIET, dialer.accept()).await.is_err());
    assert_eq!(
        dialer.connect(&HUB).await.expect_err("unknown hub").kind(),
        io::ErrorKind::NotFound
    );
    assert_eq!(
        TcpBulkTransport::dial_only_tls(
            NodeId::new("client"),
            TcpTlsDialConfig {
                credential: long,
                ..TcpTlsDialConfig::default()
            },
        )
        .expect_err("long credential")
        .kind(),
        io::ErrorKind::InvalidInput
    );
}

fn hub_config(refine: impl FnOnce(TcpTlsConfig) -> TcpTlsConfig) -> TcpTlsConfig {
    refine(TcpTlsConfig::new(
        TcpTlsKey::generate().expect("key"),
        Arc::new(Token),
    ))
}
