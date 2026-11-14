//! Integration test: **subscription lifecycle and limits** over the
//! in-memory data plane — what ends a subscription from outside, and what
//! bounds them.
//!
//! * a server that goes away ends an open subscription `ConnectionLost`;
//! * a shut-down client ends its subscriptions `Shutdown`, and the server's
//!   sink fails `Disconnected` once the connection is gone;
//! * a server runs at most `max_subscriptions_per_connection` per
//!   connection, refusing more with `TOO_MANY_SUBSCRIPTIONS`, and admits
//!   the next once one is cancelled;
//! * a client opens at most `max_subscriptions_per_peer` to one peer,
//!   refusing more `TooManySubscriptions` unsent; reading one to its end or
//!   dropping it frees its slot;
//! * an open subscription keeps its connection past both sides' idle
//!   timeouts and its destination from `max_peers` eviction.
//!
//! All waiting is virtual time or a bounded poll (`eventually`), never a bare
//! sleep.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use groupnet_core::NodeId;
use groupnet_rpc::{
    QueueCapacity, RpcClient, RpcConfig, RpcError, RpcServer, RpcServerConfig, RpcServerHandle,
    RpcStatus, SinkError, Subscription, SubscriptionSink,
};
use groupnet_testkit::cluster::eventually;
use groupnet_transport::bulk::DataPlane;
use groupnet_transport_mem::{MemBulkNet, MemBulkTransport};
use tokio::sync::mpsc;
use tokio::time::advance;

/// An opening (and call) budget no healthy in-process exchange comes near.
const CALL: Duration = Duration::from_secs(5);
/// The idle timeout under test.
const IDLE: Duration = Duration::from_secs(30);

fn plane(net: &MemBulkNet, id: &str) -> DataPlane<MemBulkTransport> {
    DataPlane::new(net.endpoint(NodeId::new(id)))
}

fn client(net: &MemBulkNet, id: &str, config: RpcConfig) -> RpcClient<MemBulkTransport> {
    RpcClient::with_config(plane(net, id), config).expect("valid client limits")
}

/// A hub whose subscriptions, by payload, end at once (`end`) or send
/// `ready`, lend their sink to the test and hold until dropped (anything
/// else). Calls echo.
struct Hub {
    server: RpcServerHandle,
    started: Arc<AtomicUsize>,
    lent: mpsc::UnboundedReceiver<SubscriptionSink>,
}

fn hub(net: &MemBulkNet, id: &str, config: RpcServerConfig) -> Hub {
    let started = Arc::new(AtomicUsize::new(0));
    let (sinks, lent) = mpsc::unbounded_channel();
    let subscribe = {
        let started = started.clone();
        move |_from: NodeId, payload: Bytes, mut sink: SubscriptionSink| {
            let (started, sinks) = (started.clone(), sinks.clone());
            async move {
                if &payload[..] == b"end" {
                    return Ok(());
                }
                started.fetch_add(1, Ordering::SeqCst);
                sink.send(Bytes::from_static(b"ready"))
                    .await
                    .map_err(|error| RpcStatus::new(500, error.to_string()))?;
                let _ = sinks.send(sink);
                std::future::pending().await
            }
        }
    };
    let server = RpcServer::with_subscriptions_config(
        |_from: NodeId, request: Bytes| async move { Ok::<_, RpcStatus>(request) },
        subscribe,
        config,
    )
    .expect("valid server limits")
    .accept_on(plane(net, id));
    Hub {
        server,
        started,
        lent,
    }
}

impl Hub {
    fn started(&self) -> usize {
        self.started.load(Ordering::SeqCst)
    }
}

async fn open(
    client: &RpcClient<MemBulkTransport>,
    to: &str,
    verb: &'static str,
) -> Result<Subscription, RpcError> {
    client
        .subscribe(&NodeId::new(to), Bytes::from_static(verb.as_bytes()), CALL)
        .await
}

/// Opens a holding subscription and waits for its `ready`.
async fn ready(client: &RpcClient<MemBulkTransport>, to: &str) -> Subscription {
    let mut subscription = open(client, to, "hold").await.expect("sent");
    assert_eq!(
        subscription.next().await,
        Some(Ok(Bytes::from_static(b"ready")))
    );
    subscription
}

fn ping() -> Bytes {
    Bytes::from_static(b"ping")
}

#[tokio::test]
async fn a_server_that_goes_away_ends_a_subscription_connection_lost() {
    let net = MemBulkNet::new();
    let hub = hub(&net, "s", RpcServerConfig::default());
    let client = client(&net, "c", RpcConfig::default());

    let mut subscription = ready(&client, "s").await;
    hub.server.shutdown();
    assert_eq!(
        subscription.next().await,
        Some(Err(RpcError::ConnectionLost))
    );
    assert_eq!(subscription.next().await, None);
}

#[tokio::test]
async fn a_shut_down_client_ends_its_subscriptions_and_the_sink_disconnects() {
    let net = MemBulkNet::new();
    let mut hub = hub(&net, "s", RpcServerConfig::default());
    let client = client(&net, "c", RpcConfig::default());

    let mut subscription = ready(&client, "s").await;
    let mut sink = hub.lent.recv().await.expect("lent sink");
    client.shutdown();
    assert_eq!(subscription.next().await, Some(Err(RpcError::Shutdown)));
    assert_eq!(subscription.next().await, None);
    assert_eq!(
        open(&client, "s", "hold").await.err(),
        Some(RpcError::Shutdown)
    );

    eventually("the server to see the connection close", || {
        hub.server.connections() == 0
    })
    .await;
    assert_eq!(sink.send(ping()).await, Err(SinkError::Disconnected));
}

#[tokio::test]
async fn a_server_bounds_live_subscriptions_per_connection() {
    let net = MemBulkNet::new();
    let hub = hub(
        &net,
        "s",
        RpcServerConfig {
            max_subscriptions_per_connection: QueueCapacity::of(2),
            ..RpcServerConfig::default()
        },
    );
    let client = client(&net, "c", RpcConfig::default());

    let first = ready(&client, "s").await;
    let _second = ready(&client, "s").await;
    let mut refused = open(&client, "s", "hold").await.expect("sent");
    match refused.next().await {
        Some(Err(RpcError::Remote(status))) => {
            assert_eq!(status.code, RpcStatus::TOO_MANY_SUBSCRIPTIONS);
        }
        other => panic!("expected the limit's refusal, got {other:?}"),
    }
    assert_eq!(refused.next().await, None);
    assert_eq!(hub.started(), 2, "the refused handler never ran");
    assert_eq!(
        client.call(&NodeId::new("s"), ping(), CALL).await,
        Ok(ping())
    );

    // The cancel travels ahead of the next subscribe on the same stream.
    drop(first);
    let _third = ready(&client, "s").await;
    assert_eq!(hub.started(), 3);
    assert_eq!(hub.server.connections(), 1);
}

#[tokio::test]
async fn a_client_bounds_open_subscriptions_per_peer() {
    let net = MemBulkNet::new();
    let hub = hub(&net, "s", RpcServerConfig::default());
    let client = client(
        &net,
        "c",
        RpcConfig {
            max_subscriptions_per_peer: QueueCapacity::MIN,
            ..RpcConfig::default()
        },
    );

    // Read to its end, a subscription frees its slot even while kept.
    let mut ended = open(&client, "s", "end").await.expect("sent");
    assert_eq!(ended.next().await, None);

    let held = ready(&client, "s").await;
    assert_eq!(
        open(&client, "s", "hold").await.err(),
        Some(RpcError::TooManySubscriptions)
    );
    assert_eq!(hub.started(), 1, "the refused subscribe was not sent");

    drop(held);
    let _next = ready(&client, "s").await;
    assert_eq!(hub.started(), 2);
    drop(ended);
}

#[tokio::test(start_paused = true)]
async fn an_open_subscription_keeps_its_connection_past_both_idle_timeouts() {
    let net = MemBulkNet::new();
    let mut hub = hub(
        &net,
        "s",
        RpcServerConfig {
            idle_timeout: IDLE,
            ..RpcServerConfig::default()
        },
    );
    let client = client(
        &net,
        "c",
        RpcConfig {
            idle_timeout: IDLE,
            ..RpcConfig::default()
        },
    );

    let mut subscription = ready(&client, "s").await;
    let mut sink = hub.lent.recv().await.expect("lent sink");
    advance(4 * IDLE).await;
    assert_eq!(hub.server.connections(), 1, "neither side idled it out");
    sink.send(Bytes::from_static(b"late")).await.expect("open");
    assert_eq!(
        subscription.next().await,
        Some(Ok(Bytes::from_static(b"late")))
    );

    drop(subscription);
    advance(2 * IDLE).await;
    eventually("the unused connection to idle out", || {
        hub.server.connections() == 0
    })
    .await;
}

#[tokio::test]
async fn a_destination_with_an_open_subscription_is_not_evicted() {
    let net = MemBulkNet::new();
    let one = hub(&net, "s1", RpcServerConfig::default());
    let _two = hub(&net, "s2", RpcServerConfig::default());
    let client = client(
        &net,
        "c",
        RpcConfig {
            max_peers: QueueCapacity::MIN,
            ..RpcConfig::default()
        },
    );
    let s2 = NodeId::new("s2");

    let subscription = ready(&client, "s1").await;
    assert_eq!(
        client.call(&s2, ping(), CALL).await,
        Err(RpcError::Saturated)
    );
    assert_eq!(one.server.connections(), 1);

    drop(subscription);
    assert_eq!(client.call(&s2, ping(), CALL).await, Ok(ping()));
    eventually("the evicted destination's connection to close", || {
        one.server.connections() == 0
    })
    .await;
}
