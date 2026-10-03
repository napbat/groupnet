//! Integration test: **RPC calls over the in-memory data plane** — what a
//! healthy connection carries.
//!
//! * an echo round trips, empty payload included;
//! * many concurrent calls from several clients to one server each get their
//!   *own* answer, over exactly one connection per client;
//! * a handler's error reaches the caller as `Remote`, a panicking handler
//!   is answered rather than left hanging, and the connection survives both;
//! * a request over the client's frame limit is refused unsent, one at the
//!   limit goes through, and a response over the server's limit comes back as
//!   `RESPONSE_TOO_LARGE`;
//! * a server runs at most `max_concurrent_per_connection` handlers for one
//!   connection.
//!
//! All waiting is a bounded poll (`eventually`), never a bare sleep.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use groupnet_core::NodeId;
use groupnet_rpc::{RpcClient, RpcConfig, RpcError, RpcServer, RpcServerConfig, RpcStatus};
use groupnet_testkit::cluster::eventually;
use groupnet_transport::bulk::DataPlane;
use groupnet_transport_mem::bulk::{MemBulkNet, MemBulkTransport};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

/// A call budget no healthy in-process call comes near.
const CALL: Duration = Duration::from_secs(5);

fn plane(net: &MemBulkNet, id: &str) -> DataPlane<MemBulkTransport> {
    DataPlane::new(net.endpoint(NodeId::new(id)))
}

fn client(net: &MemBulkNet, id: &str) -> RpcClient<MemBulkTransport> {
    RpcClient::new(plane(net, id), RpcConfig::default())
}

#[tokio::test]
async fn an_echo_round_trips() {
    let net = MemBulkNet::new();
    let _server = RpcServer::spawn(
        plane(&net, "s"),
        |_from: NodeId, request: Bytes| async move { Ok::<_, RpcStatus>(request) },
    );
    let client = client(&net, "c");
    let server = NodeId::new("s");

    let reply = client
        .call(&server, Bytes::from_static(b"ping"), CALL)
        .await;
    assert_eq!(reply, Ok(Bytes::from_static(b"ping")));
    assert_eq!(
        client.call(&server, Bytes::new(), CALL).await,
        Ok(Bytes::new())
    );
}

/// Three clients fire 100 calls each, all at once, at a handler whose delay
/// varies per request so answers come back out of order. Every call gets the
/// answer to *its* request — attributed to the client that sent it — and the
/// server saw one connection per client.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_calls_from_several_clients_pair_with_their_answers() {
    const CLIENTS: [&str; 3] = ["c1", "c2", "c3"];
    const CALLS: usize = 100;

    let net = MemBulkNet::new();
    let server = RpcServer::spawn(
        plane(&net, "s"),
        |from: NodeId, request: Bytes| async move {
            let n: u64 = std::str::from_utf8(&request)
                .expect("utf-8 request")
                .parse()
                .expect("numeric request");
            tokio::time::sleep(Duration::from_millis(n % 7)).await;
            Ok::<_, RpcStatus>(Bytes::from(format!("{from}:{n}")))
        },
    );
    let clients: Vec<_> = CLIENTS.iter().map(|id| (*id, client(&net, id))).collect();
    let mut calls = JoinSet::new();
    for (id, client) in &clients {
        let id = *id;
        for n in 0..CALLS {
            let client = client.clone();
            calls.spawn(async move {
                let reply = client
                    .call(&NodeId::new("s"), Bytes::from(n.to_string()), CALL)
                    .await;
                (id, n, reply)
            });
        }
    }
    let mut answered = 0;
    while let Some(joined) = calls.join_next().await {
        let (id, n, reply) = joined.expect("call task");
        assert_eq!(reply, Ok(Bytes::from(format!("{id}:{n}"))), "{id} call {n}");
        answered += 1;
    }
    assert_eq!(answered, CLIENTS.len() * CALLS);
    assert_eq!(
        server.connections(),
        CLIENTS.len(),
        "each client multiplexes every call onto one connection"
    );
}

#[tokio::test]
async fn handler_errors_and_panics_are_answered_and_leave_the_connection_up() {
    let net = MemBulkNet::new();
    let server = RpcServer::spawn(
        plane(&net, "s"),
        |_from: NodeId, request: Bytes| async move {
            match &request[..] {
                b"missing" => Err(RpcStatus::new(404, "no such key")),
                b"panic" => panic!("handler bug"),
                _ => Ok(request),
            }
        },
    );
    let client = client(&net, "c");
    let to = NodeId::new("s");

    assert_eq!(
        client.call(&to, Bytes::from_static(b"missing"), CALL).await,
        Err(RpcError::Remote(RpcStatus::new(404, "no such key")))
    );
    let panicked = client.call(&to, Bytes::from_static(b"panic"), CALL).await;
    assert!(
        matches!(&panicked, Err(RpcError::Remote(status)) if status.code == RpcStatus::HANDLER_PANICKED),
        "{panicked:?}"
    );
    assert_eq!(
        client.call(&to, Bytes::from_static(b"ok"), CALL).await,
        Ok(Bytes::from_static(b"ok"))
    );
    assert_eq!(
        server.connections(),
        1,
        "neither failure cost the connection"
    );
}

#[tokio::test]
async fn frame_limits_refuse_oversize_requests_unsent_and_oversize_responses_remotely() {
    /// Room for a 14-byte request head plus a 50-byte payload.
    const LIMIT: usize = 64;
    const MAX_REQUEST: usize = LIMIT - 14;

    let net = MemBulkNet::new();
    let server = RpcServer::spawn_with(
        plane(&net, "s"),
        |_from: NodeId, request: Bytes| async move {
            if &request[..] == b"big" {
                return Ok(Bytes::from(vec![0u8; LIMIT]));
            }
            Ok::<_, RpcStatus>(request)
        },
        RpcServerConfig {
            max_frame_bytes: LIMIT,
            ..RpcServerConfig::default()
        },
    );
    let client = RpcClient::new(
        plane(&net, "c"),
        RpcConfig {
            max_frame_bytes: LIMIT,
            ..RpcConfig::default()
        },
    );
    let to = NodeId::new("s");

    assert_eq!(
        client
            .call(&to, Bytes::from(vec![1u8; MAX_REQUEST + 1]), CALL)
            .await,
        Err(RpcError::TooLarge)
    );
    assert_eq!(server.connections(), 0, "a refused request opens nothing");

    let at_limit = Bytes::from(vec![1u8; MAX_REQUEST]);
    assert_eq!(client.call(&to, at_limit.clone(), CALL).await, Ok(at_limit));

    let oversize = client.call(&to, Bytes::from_static(b"big"), CALL).await;
    assert!(
        matches!(&oversize, Err(RpcError::Remote(status)) if status.code == RpcStatus::RESPONSE_TOO_LARGE),
        "{oversize:?}"
    );
    assert_eq!(
        server.connections(),
        1,
        "the refusal left the connection up"
    );
}

/// Six calls race at a server allowed two handlers per connection: two run,
/// the rest wait — and all six finish once the handlers are released.
#[tokio::test]
async fn a_connection_runs_at_most_its_concurrency_limit_of_handlers() {
    const LIMIT: usize = 2;
    const CALLS: usize = 6;

    let net = MemBulkNet::new();
    let running = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Semaphore::new(0));
    let handler = {
        let (running, peak, gate) = (running.clone(), peak.clone(), gate.clone());
        move |_from: NodeId, request: Bytes| {
            let (running, peak, gate) = (running.clone(), peak.clone(), gate.clone());
            async move {
                let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                gate.acquire().await.expect("gate open").forget();
                running.fetch_sub(1, Ordering::SeqCst);
                Ok::<_, RpcStatus>(request)
            }
        }
    };
    let _server = RpcServer::spawn_with(
        plane(&net, "s"),
        handler,
        RpcServerConfig {
            max_concurrent_per_connection: LIMIT,
            ..RpcServerConfig::default()
        },
    );
    let client = client(&net, "c");

    let mut calls = JoinSet::new();
    for n in 0..CALLS {
        let client = client.clone();
        calls.spawn(async move {
            client
                .call(
                    &NodeId::new("s"),
                    Bytes::from(vec![u8::try_from(n).unwrap()]),
                    CALL,
                )
                .await
        });
    }
    eventually("the limit's worth of handlers to start", || {
        running.load(Ordering::SeqCst) == LIMIT
    })
    .await;

    gate.add_permits(CALLS);
    let mut done = 0;
    while let Some(joined) = calls.join_next().await {
        assert!(joined.expect("call task").is_ok());
        done += 1;
    }
    assert_eq!(done, CALLS);
    assert_eq!(
        peak.load(Ordering::SeqCst),
        LIMIT,
        "never more than {LIMIT} handlers ran at once for one connection"
    );
}
