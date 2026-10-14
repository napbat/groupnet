//! Integration test: **the connection-level serving API** over the in-memory
//! data plane. The test owns every `accept` and hands streams to an
//! `RpcServer` value, which serves each under the server's limits.
//!
//! * a handed-over stream is served with its peer's id, later calls reuse
//!   it, and every clone of the server counts the same connections;
//! * at `max_connections`, `serve` refuses `AtCapacity` and closes the
//!   stream, while `reserve` waits until a connection ends and its slot then
//!   serves the next stream;
//! * a served connection runs at most `max_concurrent_per_connection`
//!   handlers, idles out under the server's `idle_timeout`, and is dropped
//!   for a frame over the server's `max_frame_bytes`;
//! * `shutdown` ends served connections, wakes a waiting `reserve` with
//!   `Shutdown`, and refuses every later stream; dropping the last clone ends
//!   them too.
//!
//! All waiting is virtual time or a bounded poll (`eventually`), never a bare
//! sleep.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures_util::FutureExt;
use groupnet_core::NodeId;
use groupnet_rpc::{
    Bytes, DataPlane, FrameLimit, QueueCapacity, RpcClient, RpcConfig, RpcError, RpcServeError,
    RpcServer, RpcServerConfig, RpcStatus,
};
use groupnet_testkit::cluster::eventually;
use groupnet_transport_mem::{MemBulkNet, MemBulkTransport};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time::advance;

/// A call budget no healthy in-process call comes near.
const CALL: Duration = Duration::from_secs(5);

/// The idle timeout under test.
const IDLE: Duration = Duration::from_secs(30);

/// One second short of [`IDLE`].
const BEFORE_IDLE: Duration = Duration::from_secs(29);

fn plane(net: &MemBulkNet, id: &str) -> DataPlane<MemBulkTransport> {
    DataPlane::new(net.endpoint(NodeId::new(id)))
}

fn ping() -> Bytes {
    Bytes::from_static(b"ping")
}

/// Answers `<caller>/<request>`, so a reply proves which peer the server was
/// told it served.
#[expect(
    clippy::needless_pass_by_value,
    reason = "the handler signature RpcServer calls takes both by value"
)]
fn whoami(from: NodeId, request: Bytes) -> std::future::Ready<Result<Bytes, RpcStatus>> {
    std::future::ready(Ok(Bytes::from(format!(
        "{from}/{}",
        String::from_utf8_lossy(&request)
    ))))
}

/// Accepts the next stream on `inbound` and hands it to `server`.
async fn accept_into<H, F>(
    inbound: &DataPlane<MemBulkTransport>,
    server: &RpcServer<H>,
) -> Result<(), RpcServeError>
where
    H: Fn(NodeId, Bytes) -> F + Clone + Send + 'static,
    F: Future<Output = Result<Bytes, RpcStatus>> + Send + 'static,
{
    let (from, stream) = inbound.accept().await.expect("in-memory accept");
    server.serve(from, stream)
}

#[tokio::test]
async fn a_handed_over_stream_is_served_and_counted_by_every_clone() {
    let net = MemBulkNet::new();
    let inbound = plane(&net, "s");
    let server = RpcServer::new(whoami);
    let client = RpcClient::new(plane(&net, "c"));
    let to = NodeId::new("s");

    let first = tokio::spawn({
        let (client, to) = (client.clone(), to.clone());
        async move { client.call(&to, ping(), CALL).await }
    });
    accept_into(&inbound, &server).await.expect("a free slot");
    assert_eq!(
        first.await.expect("call task"),
        Ok(Bytes::from_static(b"c/ping"))
    );
    // No second accept: the client reuses the connection it was served on.
    assert_eq!(
        client.call(&to, Bytes::from_static(b"again"), CALL).await,
        Ok(Bytes::from_static(b"c/again"))
    );
    let clone = server.clone();
    assert_eq!(server.connections(), 1);
    assert_eq!(clone.connections(), 1, "clones share one connection count");
}

#[tokio::test]
async fn at_max_connections_serve_refuses_and_reserve_waits_for_a_slot() {
    let net = MemBulkNet::new();
    let inbound = plane(&net, "s");
    let server = RpcServer::with_config(
        whoami,
        RpcServerConfig {
            max_connections: QueueCapacity::MIN,
            ..RpcServerConfig::default()
        },
    )
    .expect("valid limits");
    let to = NodeId::new("s");

    let first = RpcClient::new(plane(&net, "c1"));
    let call = tokio::spawn({
        let (first, to) = (first.clone(), to.clone());
        async move { first.call(&to, ping(), CALL).await }
    });
    accept_into(&inbound, &server).await.expect("a free slot");
    assert_eq!(
        call.await.expect("call task"),
        Ok(Bytes::from_static(b"c1/ping"))
    );

    // Full: a second stream is refused, and the refusal closes it.
    let dialer = plane(&net, "c2");
    let mut refused = dialer.connect(&to).await.expect("in-memory connect");
    assert_eq!(
        accept_into(&inbound, &server).await,
        Err(RpcServeError::AtCapacity)
    );
    assert_eq!(
        refused.recv().await.ok().flatten(),
        None,
        "the refused stream was closed"
    );
    assert_eq!(server.connections(), 1);

    // `reserve` waits instead, and gets the slot the first connection frees.
    assert!(
        server.reserve().now_or_never().is_none(),
        "no slot while the server is full"
    );
    let waiting = tokio::spawn(server.reserve());
    first.shutdown();
    let slot = waiting
        .await
        .expect("reserve task")
        .expect("a slot once the first connection ended");
    assert_eq!(server.connections(), 0);

    let third = RpcClient::new(plane(&net, "c3"));
    let call = tokio::spawn({
        let (third, to) = (third.clone(), to.clone());
        async move { third.call(&to, ping(), CALL).await }
    });
    let (from, stream) = inbound.accept().await.expect("in-memory accept");
    slot.serve(from, stream).expect("the reserved slot serves");
    assert_eq!(
        call.await.expect("call task"),
        Ok(Bytes::from_static(b"c3/ping"))
    );
    assert_eq!(server.connections(), 1);
}

/// Six calls race on one served connection allowed two handlers: two run,
/// the rest wait, and all six finish once the handlers are released.
#[tokio::test]
async fn a_served_connection_runs_at_most_its_handler_limit() {
    const LIMIT: usize = 2;
    const CALLS: usize = 6;

    let net = MemBulkNet::new();
    let inbound = plane(&net, "s");
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
    let server = RpcServer::with_config(
        handler,
        RpcServerConfig {
            max_concurrent_per_connection: QueueCapacity::of(LIMIT),
            ..RpcServerConfig::default()
        },
    )
    .expect("valid limits");
    let client = RpcClient::new(plane(&net, "c"));

    let mut calls = JoinSet::new();
    for n in 0..CALLS {
        let client = client.clone();
        calls.spawn(async move {
            let payload = Bytes::from(vec![u8::try_from(n).expect("small n")]);
            client.call(&NodeId::new("s"), payload, CALL).await
        });
    }
    accept_into(&inbound, &server).await.expect("a free slot");
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
    assert_eq!(server.connections(), 1, "every call shared one connection");
    assert_eq!(
        peak.load(Ordering::SeqCst),
        LIMIT,
        "never more than {LIMIT} handlers ran at once for one connection"
    );
}

#[tokio::test(start_paused = true)]
async fn a_served_connection_idles_out_under_the_server_timeout() {
    let net = MemBulkNet::new();
    let inbound = plane(&net, "s");
    let server = RpcServer::with_config(
        whoami,
        RpcServerConfig {
            idle_timeout: IDLE,
            ..RpcServerConfig::default()
        },
    )
    .expect("valid limits");
    let client = RpcClient::with_config(
        plane(&net, "c"),
        RpcConfig {
            idle_timeout: 4 * IDLE,
            ..RpcConfig::default()
        },
    )
    .expect("valid limits");

    // The client stays alive, so only the server's idle timer can end it.
    let call = tokio::spawn({
        let client = client.clone();
        async move { client.call(&NodeId::new("s"), ping(), CALL).await }
    });
    accept_into(&inbound, &server).await.expect("a free slot");
    assert_eq!(
        call.await.expect("call task"),
        Ok(Bytes::from_static(b"c/ping"))
    );
    advance(BEFORE_IDLE).await;
    assert_eq!(server.connections(), 1, "not yet idle long enough");
    advance(Duration::from_secs(2)).await;
    eventually("the server to close the idle served connection", || {
        server.connections() == 0
    })
    .await;
}

#[tokio::test]
async fn a_frame_over_the_server_limit_drops_the_served_connection() {
    /// Room for a 14-byte request head plus a 50-byte payload.
    const LIMIT: usize = 64;

    let net = MemBulkNet::new();
    let inbound = plane(&net, "s");
    let server = RpcServer::with_config(
        whoami,
        RpcServerConfig {
            max_frame_bytes: FrameLimit::new(LIMIT).expect("a valid limit"),
            ..RpcServerConfig::default()
        },
    )
    .expect("valid limits");
    // The client's own limit is the default, so it sends the oversize frame.
    let client = RpcClient::new(plane(&net, "c"));

    let call = tokio::spawn(async move {
        client
            .call(&NodeId::new("s"), Bytes::from(vec![7u8; LIMIT]), CALL)
            .await
    });
    accept_into(&inbound, &server).await.expect("a free slot");
    assert_eq!(
        call.await.expect("call task"),
        Err(RpcError::ConnectionLost)
    );
    eventually("the oversize frame to end the served connection", || {
        server.connections() == 0
    })
    .await;
}

#[tokio::test]
async fn shutdown_or_the_last_drop_ends_served_connections() {
    let net = MemBulkNet::new();
    let inbound = plane(&net, "s");
    let started = Arc::new(Semaphore::new(0));
    let handler = {
        let started = started.clone();
        move |_from: NodeId, _request: Bytes| {
            let started = started.clone();
            async move {
                started.add_permits(1);
                std::future::pending::<Result<Bytes, RpcStatus>>().await
            }
        }
    };
    let server = RpcServer::with_config(
        handler.clone(),
        RpcServerConfig {
            max_connections: QueueCapacity::MIN,
            ..RpcServerConfig::default()
        },
    )
    .expect("valid limits");
    let client = RpcClient::new(plane(&net, "c"));
    let to = NodeId::new("s");

    let blocked = tokio::spawn({
        let (client, to) = (client.clone(), to.clone());
        async move { client.call(&to, ping(), CALL).await }
    });
    accept_into(&inbound, &server).await.expect("a free slot");
    started.acquire().await.expect("handler started").forget();
    let waiting = tokio::spawn(server.reserve());

    server.shutdown();
    assert_eq!(
        blocked.await.expect("call task"),
        Err(RpcError::ConnectionLost)
    );
    assert_eq!(
        waiting.await.expect("reserve task").unwrap_err(),
        RpcServeError::Shutdown,
        "a waiting reserve wakes with the shutdown"
    );
    eventually("the aborted connection to be released", || {
        server.connections() == 0
    })
    .await;
    let dialer = plane(&net, "late");
    let _late = dialer.connect(&to).await.expect("in-memory connect");
    assert_eq!(
        accept_into(&inbound, &server).await,
        Err(RpcServeError::Shutdown)
    );
    assert_eq!(server.reserve().await.unwrap_err(), RpcServeError::Shutdown);

    // Dropping every clone instead of shutting down ends them the same way.
    let server = RpcServer::new(handler);
    let blocked = tokio::spawn(async move { client.call(&to, ping(), CALL).await });
    accept_into(&inbound, &server).await.expect("a free slot");
    started.acquire().await.expect("handler started").forget();
    drop(server);
    assert_eq!(
        blocked.await.expect("call task"),
        Err(RpcError::ConnectionLost)
    );
}
