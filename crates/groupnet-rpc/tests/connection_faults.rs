//! Integration test: **RPC under connection faults** over the in-memory data
//! plane — timeouts, broken and dropped streams, restarts and shutdown.
//!
//! * a slow handler under a short timeout fails the call `Timeout`; the
//!   server drops the expired work, and its late answer is discarded without
//!   costing the connection later calls;
//! * a client that goes away ends its server-side connection task;
//! * a server killed mid-call fails that call `ConnectionLost`, and a
//!   restarted server is reached by the next call over a fresh connection;
//! * an unregistered peer is `Unreachable`;
//! * a shut-down client fails in-flight and later calls `Shutdown`; a
//!   shut-down server drops its connections and is no longer reachable.
//!
//! All waiting is a bounded poll (`eventually`), never a bare sleep.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use groupnet_core::NodeId;
use groupnet_rpc::{RpcClient, RpcConfig, RpcError, RpcServer, RpcServerHandle, RpcStatus};
use groupnet_testkit::cluster::eventually;
use groupnet_transport::bulk::{BulkTransport, DataPlane};
use groupnet_transport_mem::bulk::{MemBulkNet, MemBulkTransport};
use tokio::sync::mpsc;

/// A call budget no healthy in-process call comes near.
const CALL: Duration = Duration::from_secs(5);

/// A client endpoint that counts the connections it opens, so a test can
/// tell a reused connection from a replaced one.
#[derive(Debug)]
struct Counting {
    inner: MemBulkTransport,
    connects: Arc<AtomicUsize>,
}

impl BulkTransport for Counting {
    type Error = io::Error;
    type Stream = <MemBulkTransport as BulkTransport>::Stream;

    fn connect(&self, to: &NodeId) -> impl Future<Output = io::Result<Self::Stream>> + Send {
        self.connects.fetch_add(1, Ordering::SeqCst);
        self.inner.connect(to)
    }

    fn accept(&self) -> impl Future<Output = io::Result<(NodeId, Self::Stream)>> + Send {
        self.inner.accept()
    }
}

/// A client on `net` as `id`, and its connection counter.
fn counting_client(net: &MemBulkNet, id: &str) -> (RpcClient<Counting>, Arc<AtomicUsize>) {
    let connects = Arc::new(AtomicUsize::new(0));
    let plane = DataPlane::new(Counting {
        inner: net.endpoint(NodeId::new(id)),
        connects: connects.clone(),
    });
    (
        RpcClient::new(plane, RpcConfig::default()).expect("default limits are valid"),
        connects,
    )
}

/// A server on `net` as `id` (re-registering `id` evicts any earlier one):
/// it echoes, and for `b"block"` reports on `started` and never answers.
fn blocking_echo(
    net: &MemBulkNet,
    id: &str,
    started: mpsc::UnboundedSender<()>,
) -> RpcServerHandle {
    RpcServer::spawn(
        DataPlane::new(net.endpoint(NodeId::new(id))),
        move |_from: NodeId, request: Bytes| {
            let started = started.clone();
            async move {
                if &request[..] == b"block" {
                    let _ = started.send(());
                    std::future::pending::<()>().await;
                }
                Ok::<_, RpcStatus>(request)
            }
        },
    )
}

/// Sets its flag when dropped: proof that the server abandoned a handler.
struct DroppedFlag(Arc<AtomicBool>);

impl Drop for DroppedFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn a_timed_out_call_is_dropped_server_side_and_its_late_answer_discarded() {
    let net = MemBulkNet::new();
    let abandoned = Arc::new(AtomicBool::new(false));
    let handler = {
        let abandoned = abandoned.clone();
        move |_from: NodeId, request: Bytes| {
            let abandoned = abandoned.clone();
            async move {
                if &request[..] == b"slow" {
                    let _flag = DroppedFlag(abandoned);
                    std::future::pending::<()>().await;
                }
                Ok::<_, RpcStatus>(request)
            }
        }
    };
    let server = RpcServer::spawn(DataPlane::new(net.endpoint(NodeId::new("s"))), handler);
    let (client, connects) = counting_client(&net, "c");
    let to = NodeId::new("s");

    assert_eq!(
        client
            .call(&to, Bytes::from_static(b"slow"), Duration::from_millis(50))
            .await,
        Err(RpcError::Timeout)
    );
    // The deadline travelled with the request: the server drops the work
    // (and answers an id the client has already forgotten).
    eventually("the server to abandon the expired handler", || {
        abandoned.load(Ordering::SeqCst)
    })
    .await;

    for _ in 0..3 {
        assert_eq!(
            client.call(&to, Bytes::from_static(b"fast"), CALL).await,
            Ok(Bytes::from_static(b"fast"))
        );
    }
    assert_eq!(
        connects.load(Ordering::SeqCst),
        1,
        "the timeout cost no reconnect"
    );
    assert_eq!(server.connections(), 1);
}

#[tokio::test]
async fn a_dropped_client_ends_its_server_connection() {
    let net = MemBulkNet::new();
    let (started, _) = mpsc::unbounded_channel();
    let server = blocking_echo(&net, "s", started);
    let (client, _) = counting_client(&net, "c");

    assert!(
        client
            .call(&NodeId::new("s"), Bytes::from_static(b"hi"), CALL)
            .await
            .is_ok()
    );
    assert_eq!(server.connections(), 1);

    drop(client);
    eventually("the server to retire the dropped peer's connection", || {
        server.connections() == 0
    })
    .await;
}

#[tokio::test]
async fn a_server_killed_mid_call_loses_the_call_and_a_restart_is_reached() {
    let net = MemBulkNet::new();
    let (started, mut on_start) = mpsc::unbounded_channel();
    let server = blocking_echo(&net, "s", started.clone());
    let (client, connects) = counting_client(&net, "c");

    let in_flight = tokio::spawn({
        let client = client.clone();
        async move {
            client
                .call(&NodeId::new("s"), Bytes::from_static(b"block"), CALL)
                .await
        }
    });
    on_start.recv().await.expect("the handler started");
    server.shutdown();
    assert_eq!(
        in_flight.await.expect("call task"),
        Err(RpcError::ConnectionLost),
        "the request was delivered and never answered: its outcome is unknown"
    );

    let _restarted = blocking_echo(&net, "s", started);
    assert_eq!(
        client
            .call(&NodeId::new("s"), Bytes::from_static(b"again"), CALL)
            .await,
        Ok(Bytes::from_static(b"again"))
    );
    assert_eq!(
        connects.load(Ordering::SeqCst),
        2,
        "the broken connection was replaced"
    );
}

#[tokio::test]
async fn an_unregistered_peer_is_unreachable() {
    let net = MemBulkNet::new();
    let (client, _) = counting_client(&net, "c");
    assert_eq!(
        client
            .call(&NodeId::new("nobody"), Bytes::from_static(b"hi"), CALL)
            .await,
        Err(RpcError::Unreachable)
    );
}

#[tokio::test]
async fn a_shut_down_client_fails_in_flight_and_later_calls() {
    let net = MemBulkNet::new();
    let (started, mut on_start) = mpsc::unbounded_channel();
    let server = blocking_echo(&net, "s", started);
    let (client, _) = counting_client(&net, "c");

    let in_flight = tokio::spawn({
        let client = client.clone();
        async move {
            client
                .call(&NodeId::new("s"), Bytes::from_static(b"block"), CALL)
                .await
        }
    });
    on_start.recv().await.expect("the handler started");
    client.shutdown();
    assert_eq!(in_flight.await.expect("call task"), Err(RpcError::Shutdown));
    assert_eq!(
        client
            .call(&NodeId::new("s"), Bytes::from_static(b"hi"), CALL)
            .await,
        Err(RpcError::Shutdown)
    );
    eventually("the server to see the client's connection close", || {
        server.connections() == 0
    })
    .await;
}

#[tokio::test]
async fn a_shut_down_server_drops_its_connections_and_stops_accepting() {
    let net = MemBulkNet::new();
    let (started, _) = mpsc::unbounded_channel();
    let server = blocking_echo(&net, "s", started);
    let (first, _) = counting_client(&net, "c1");
    assert!(
        first
            .call(&NodeId::new("s"), Bytes::from_static(b"hi"), CALL)
            .await
            .is_ok()
    );

    server.shutdown();
    eventually("the server's connections to close", || {
        server.connections() == 0
    })
    .await;

    // A client with no cached connection must dial — and nothing listens.
    let (second, _) = counting_client(&net, "c2");
    assert_eq!(
        second
            .call(&NodeId::new("s"), Bytes::from_static(b"hi"), CALL)
            .await,
        Err(RpcError::Unreachable)
    );
}
