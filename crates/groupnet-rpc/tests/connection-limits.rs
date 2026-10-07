//! Integration test: **RPC connection limits** over the in-memory data plane,
//! under paused Tokio time.
//!
//! * a client closes a connection no call has used for its idle timeout, and
//!   the next call reconnects;
//! * a server closes a connection with no request and no handler for its idle
//!   timeout, but never while a handler runs;
//! * a server serves at most `max_connections` connections and admits the
//!   next once one ends;
//! * a client tracks at most `max_peers` destinations: an unused one is
//!   evicted for a new one, and with every destination busy a new one is
//!   refused `Saturated`, unsent;
//! * invalid timers are refused when the client or server is built.
//!
//! All waiting is virtual time or a bounded poll (`eventually`).

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use groupnet_core::NodeId;
use groupnet_rpc::{
    RpcClient, RpcConfig, RpcError, RpcServer, RpcServerConfig, RpcServerHandle, RpcStatus,
};
use groupnet_testkit::cluster::eventually;
use groupnet_transport::QueueCapacity;
use groupnet_transport::bulk::DataPlane;
use groupnet_transport_mem::bulk::{MemBulkNet, MemBulkTransport};
use tokio::sync::Semaphore;
use tokio::time::advance;

/// A call budget no healthy in-process call comes near.
const CALL: Duration = Duration::from_secs(5);
/// The idle timeout under test.
const IDLE: Duration = Duration::from_secs(30);
/// One second short of [`IDLE`].
const BEFORE_IDLE: Duration = Duration::from_secs(29);
/// An idle timeout the test never reaches.
const NEVER: Duration = Duration::from_secs(3600);

fn plane(net: &MemBulkNet, id: &str) -> DataPlane<MemBulkTransport> {
    DataPlane::new(net.endpoint(NodeId::new(id)))
}

fn client(net: &MemBulkNet, id: &str, config: RpcConfig) -> RpcClient<MemBulkTransport> {
    RpcClient::new(plane(net, id), config).expect("valid client limits")
}

/// Holds `b"block"` handlers until released, counting the ones started.
struct Gate {
    permits: Semaphore,
    started: AtomicUsize,
}

impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            permits: Semaphore::new(0),
            started: AtomicUsize::new(0),
        })
    }

    fn started(&self) -> usize {
        self.started.load(Ordering::SeqCst)
    }

    fn release(&self) {
        self.permits.add_permits(1);
    }
}

/// A server that echoes, and for `b"block"` holds the handler on `gate`.
fn gated_echo(
    net: &MemBulkNet,
    id: &str,
    gate: &Arc<Gate>,
    config: RpcServerConfig,
) -> RpcServerHandle {
    let gate = gate.clone();
    RpcServer::spawn_with(
        plane(net, id),
        move |_from: NodeId, request: Bytes| {
            let gate = gate.clone();
            async move {
                if &request[..] == b"block" {
                    gate.started.fetch_add(1, Ordering::SeqCst);
                    gate.permits.acquire().await.expect("gate open").forget();
                }
                Ok::<_, RpcStatus>(request)
            }
        },
        config,
    )
    .expect("valid server limits")
}

fn ping() -> Bytes {
    Bytes::from_static(b"ping")
}

#[tokio::test(start_paused = true)]
async fn a_client_closes_an_idle_connection_and_the_next_call_reconnects() {
    let net = MemBulkNet::new();
    let gate = Gate::new();
    let server_config = RpcServerConfig {
        idle_timeout: NEVER,
        ..RpcServerConfig::default()
    };
    let server = gated_echo(&net, "s", &gate, server_config);
    let client_config = RpcConfig {
        idle_timeout: IDLE,
        ..RpcConfig::default()
    };
    let client = client(&net, "c", client_config);
    let to = NodeId::new("s");

    assert_eq!(client.call(&to, ping(), CALL).await, Ok(ping()));
    assert_eq!(server.connections(), 1);
    advance(BEFORE_IDLE).await;
    assert_eq!(server.connections(), 1, "not yet idle long enough");
    advance(Duration::from_secs(2)).await;
    eventually("the client to close its idle connection", || {
        server.connections() == 0
    })
    .await;
    assert_eq!(client.peers(), 1, "the destination itself stays admitted");

    assert_eq!(client.call(&to, ping(), CALL).await, Ok(ping()));
    assert_eq!(server.connections(), 1, "the next call reconnected");
}

#[tokio::test(start_paused = true)]
async fn a_server_closes_idle_connections_but_never_under_a_running_handler() {
    let net = MemBulkNet::new();
    let gate = Gate::new();
    let server_config = RpcServerConfig {
        idle_timeout: IDLE,
        ..RpcServerConfig::default()
    };
    let server = gated_echo(&net, "s", &gate, server_config);
    let client_config = RpcConfig {
        idle_timeout: NEVER,
        ..RpcConfig::default()
    };
    let client = client(&net, "c", client_config);
    let to = NodeId::new("s");

    let blocked = tokio::spawn({
        let (client, to) = (client.clone(), to.clone());
        async move {
            client
                .call(&to, Bytes::from_static(b"block"), 4 * IDLE)
                .await
        }
    });
    eventually("the blocking handler to start", || gate.started() == 1).await;
    advance(2 * IDLE).await;
    assert_eq!(
        server.connections(),
        1,
        "a running handler keeps its connection open"
    );
    gate.release();
    assert_eq!(
        blocked.await.expect("call task"),
        Ok(Bytes::from_static(b"block")),
        "the answer is delivered"
    );

    advance(BEFORE_IDLE).await;
    assert_eq!(server.connections(), 1, "not yet idle long enough");
    advance(Duration::from_secs(2)).await;
    eventually("the server to close its idle connection", || {
        server.connections() == 0
    })
    .await;
    let fresh = self::client(&net, "c2", client_config);
    assert_eq!(fresh.call(&to, ping(), CALL).await, Ok(ping()));
}

#[tokio::test(start_paused = true)]
async fn a_full_server_admits_the_next_connection_once_one_ends() {
    let net = MemBulkNet::new();
    let gate = Gate::new();
    let server_config = RpcServerConfig {
        max_connections: QueueCapacity::MIN,
        ..RpcServerConfig::default()
    };
    let server = gated_echo(&net, "s", &gate, server_config);
    let first = client(&net, "c1", RpcConfig::default());
    let second = client(&net, "c2", RpcConfig::default());
    let to = NodeId::new("s");

    assert_eq!(first.call(&to, ping(), CALL).await, Ok(ping()));
    // The second connection is opened but not admitted: nothing reads it.
    assert_eq!(
        second.call(&to, ping(), Duration::from_secs(1)).await,
        Err(RpcError::Timeout)
    );
    assert_eq!(server.connections(), 1);

    first.shutdown();
    assert_eq!(
        second.call(&to, ping(), CALL).await,
        Ok(ping()),
        "the ended connection's slot admitted the waiting one"
    );
    assert_eq!(server.connections(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_client_evicts_an_unused_destination_and_refuses_when_all_are_busy() {
    let net = MemBulkNet::new();
    let gate = Gate::new();
    let one = gated_echo(&net, "s1", &gate, RpcServerConfig::default());
    let two = gated_echo(&net, "s2", &gate, RpcServerConfig::default());
    let client_config = RpcConfig {
        max_peers: QueueCapacity::MIN,
        ..RpcConfig::default()
    };
    let client = client(&net, "c", client_config);
    let (s1, s2) = (NodeId::new("s1"), NodeId::new("s2"));

    assert_eq!(client.call(&s1, ping(), CALL).await, Ok(ping()));
    assert_eq!(client.call(&s2, ping(), CALL).await, Ok(ping()));
    assert_eq!(client.peers(), 1);
    eventually("the evicted destination's connection to close", || {
        one.connections() == 0
    })
    .await;

    let blocked = tokio::spawn({
        let (client, s2) = (client.clone(), s2.clone());
        async move { client.call(&s2, Bytes::from_static(b"block"), CALL).await }
    });
    eventually("the busy destination's handler to start", || {
        gate.started() == 1
    })
    .await;
    assert_eq!(two.connections(), 1);
    assert_eq!(
        client.call(&s1, ping(), CALL).await,
        Err(RpcError::Saturated)
    );
    assert_eq!(one.connections(), 0, "the refused call was not sent");

    gate.release();
    assert_eq!(
        blocked.await.expect("call task"),
        Ok(Bytes::from_static(b"block"))
    );
    assert_eq!(client.call(&s1, ping(), CALL).await, Ok(ping()));
    assert_eq!(client.peers(), 1);
}

#[tokio::test]
async fn invalid_timers_are_refused_when_built() {
    let net = MemBulkNet::new();
    let error = RpcClient::new(
        plane(&net, "c"),
        RpcConfig {
            connect_timeout: Duration::ZERO,
            ..RpcConfig::default()
        },
    )
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    let error = RpcServer::spawn_with(
        plane(&net, "s"),
        |_from: NodeId, request: Bytes| async move { Ok::<_, RpcStatus>(request) },
        RpcServerConfig {
            idle_timeout: groupnet_rpc::MAX_TIMEOUT + Duration::from_millis(1),
            ..RpcServerConfig::default()
        },
    )
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
}
