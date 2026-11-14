//! Integration test: **RPC subscriptions (server push)** over the in-memory
//! data plane — what one subscription carries and how it ends.
//!
//! * items arrive in order, across many credit grants, and End then yields
//!   `None` for good;
//! * a handler error ends the subscription after its items with `Remote`, a
//!   panicking handler with `HANDLER_PANICKED`, and an item over the frame
//!   limit with `RESPONSE_TOO_LARGE` (its sink fails `TooLarge`);
//! * dropping a subscription cancels it: the server drops the handler and a
//!   sink held elsewhere fails `Cancelled`, while the connection stays up;
//! * credit bounds the server: a subscription nobody reads holds the server
//!   to its window, and blocks neither a call nor another subscription on
//!   the same connection; consuming items grants more;
//! * a server built without a subscribe handler refuses with
//!   `SUBSCRIPTIONS_UNSUPPORTED` and keeps serving calls;
//! * an oversized, zero-budget or unroutable subscribe is refused unsent.
//!
//! All waiting is a bounded poll (`eventually`), never a bare sleep.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use groupnet_core::NodeId;
use groupnet_rpc::{
    FrameLimit, RpcClient, RpcConfig, RpcError, RpcServer, RpcServerConfig, RpcServerHandle,
    RpcStatus, SinkError, Subscription, SubscriptionSink,
};
use groupnet_testkit::cluster::eventually;
use groupnet_transport::bulk::DataPlane;
use groupnet_transport_mem::{MemBulkNet, MemBulkTransport};
use tokio::sync::mpsc;

/// An opening (and call) budget no healthy in-process exchange comes near.
const CALL: Duration = Duration::from_secs(5);

fn plane(net: &MemBulkNet, id: &str) -> DataPlane<MemBulkTransport> {
    DataPlane::new(net.endpoint(NodeId::new(id)))
}

fn client(net: &MemBulkNet, id: &str, config: RpcConfig) -> RpcClient<MemBulkTransport> {
    RpcClient::with_config(plane(net, id), config).expect("valid client limits")
}

/// Sets its flag when dropped: proof that the server dropped a handler.
struct DroppedFlag(Arc<AtomicBool>);

impl Drop for DroppedFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// What the test server's subscribe handler reports.
struct Probe {
    /// Items the `flood` subscription has sent.
    flooded: AtomicUsize,
    /// Set once a `hold` handler is dropped.
    dropped: Arc<AtomicBool>,
    /// Sinks a `lend` handler handed out.
    lent: mpsc::UnboundedSender<SubscriptionSink>,
    /// The sink errors a `big` handler saw.
    errors: mpsc::UnboundedSender<SinkError>,
}

impl Probe {
    /// Runs one subscription, by the payload's verb:
    ///
    /// * `count:N` — items `"<from>:0"` … `"<from>:N-1"`, then End;
    /// * `fail` — items `a`, `b`, then a 404 error;
    /// * `panic` — panics;
    /// * `big:N` — an item of `N` bytes, then one of `N + 1` (reporting both
    ///   sink outcomes), then `Ok(())`;
    /// * `lend` — hands its sink out and holds until dropped;
    /// * `flood` — sends `f0`, `f1`, … until the sink fails, counting them.
    async fn run(
        self: Arc<Self>,
        from: NodeId,
        payload: Bytes,
        mut sink: SubscriptionSink,
    ) -> Result<(), RpcStatus> {
        let verb = String::from_utf8(payload.to_vec()).expect("UTF-8 verb");
        let (verb, arg) = verb.split_once(':').unwrap_or((&verb, ""));
        match verb {
            "count" => {
                for i in 0..arg.parse::<usize>().expect("count") {
                    send(&mut sink, format!("{from}:{i}")).await?;
                }
                Ok(())
            }
            "fail" => {
                send(&mut sink, "a").await?;
                send(&mut sink, "b").await?;
                Err(RpcStatus::new(404, "gone"))
            }
            "panic" => panic!("subscribe handler bug"),
            "big" => {
                let len = arg.parse::<usize>().expect("length");
                for item in [vec![1u8; len], vec![2u8; len + 1]] {
                    if let Err(error) = sink.send(Bytes::from(item)).await {
                        let _ = self.errors.send(error);
                    }
                }
                if let Err(error) = sink.send(Bytes::from_static(b"after")).await {
                    let _ = self.errors.send(error);
                }
                Ok(())
            }
            "lend" => {
                let _flag = DroppedFlag(self.dropped.clone());
                let _ = self.lent.send(sink);
                std::future::pending().await
            }
            "flood" => {
                for i in 0.. {
                    send(&mut sink, format!("f{i}")).await?;
                    self.flooded.fetch_add(1, Ordering::SeqCst);
                }
                Ok(())
            }
            other => Err(RpcStatus::new(400, format!("unknown verb {other}"))),
        }
    }
}

/// Sends `item`, mapping a failed sink to an error status (which nobody
/// sees once the subscription is over).
async fn send(sink: &mut SubscriptionSink, item: impl Into<String>) -> Result<(), RpcStatus> {
    sink.send(Bytes::from(item.into()))
        .await
        .map_err(|error| RpcStatus::new(500, error.to_string()))
}

/// The test server, its probe, and the receivers the probe reports on.
struct Hub {
    server: RpcServerHandle,
    probe: Arc<Probe>,
    lent: mpsc::UnboundedReceiver<SubscriptionSink>,
    errors: mpsc::UnboundedReceiver<SinkError>,
}

/// Serves `id` on `net`: calls echo, subscriptions run [`Probe::run`].
fn hub(net: &MemBulkNet, id: &str, config: RpcServerConfig) -> Hub {
    let (lent, lent_rx) = mpsc::unbounded_channel();
    let (errors, errors_rx) = mpsc::unbounded_channel();
    let probe = Arc::new(Probe {
        flooded: AtomicUsize::new(0),
        dropped: Arc::new(AtomicBool::new(false)),
        lent,
        errors,
    });
    let subscribe = {
        let probe = probe.clone();
        move |from: NodeId, payload: Bytes, sink: SubscriptionSink| {
            probe.clone().run(from, payload, sink)
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
        probe,
        lent: lent_rx,
        errors: errors_rx,
    }
}

async fn open(client: &RpcClient<MemBulkTransport>, to: &str, verb: &'static str) -> Subscription {
    client
        .subscribe(&NodeId::new(to), Bytes::from_static(verb.as_bytes()), CALL)
        .await
        .expect("the subscribe is sent")
}

fn item(text: &str) -> Bytes {
    Bytes::copy_from_slice(text.as_bytes())
}

fn remote_code(next: Option<Result<Bytes, RpcError>>) -> u16 {
    match next {
        Some(Err(RpcError::Remote(status))) => status.code,
        other => panic!("expected a remote error, got {other:?}"),
    }
}

#[tokio::test]
async fn items_arrive_in_order_across_many_grants_then_the_end() {
    const ITEMS: usize = 100;

    let net = MemBulkNet::new();
    let hub = hub(&net, "s", RpcServerConfig::default());
    // A window of 4 needs dozens of grants for 100 items.
    let client = client(
        &net,
        "c",
        RpcConfig {
            subscription_window: NonZeroU32::new(4).unwrap(),
            ..RpcConfig::default()
        },
    );

    let mut subscription = open(&client, "s", "count:100").await;
    for i in 0..ITEMS {
        assert_eq!(
            subscription.next().await,
            Some(Ok(item(&format!("c:{i}")))),
            "item {i}"
        );
    }
    assert_eq!(subscription.next().await, None, "End");
    assert_eq!(subscription.next().await, None, "and nothing after it");
    assert_eq!(hub.server.connections(), 1);
}

#[tokio::test]
async fn a_handler_error_or_panic_ends_the_subscription_after_its_items() {
    let net = MemBulkNet::new();
    let hub = hub(&net, "s", RpcServerConfig::default());
    let client = client(&net, "c", RpcConfig::default());

    let mut failing = open(&client, "s", "fail").await;
    assert_eq!(failing.next().await, Some(Ok(item("a"))));
    assert_eq!(failing.next().await, Some(Ok(item("b"))));
    assert_eq!(
        failing.next().await,
        Some(Err(RpcError::Remote(RpcStatus::new(404, "gone"))))
    );
    assert_eq!(failing.next().await, None);

    let mut panicking = open(&client, "s", "panic").await;
    assert_eq!(
        remote_code(panicking.next().await),
        RpcStatus::HANDLER_PANICKED
    );
    assert_eq!(panicking.next().await, None);

    assert_eq!(
        client
            .call(&NodeId::new("s"), Bytes::from_static(b"ok"), CALL)
            .await,
        Ok(Bytes::from_static(b"ok"))
    );
    assert_eq!(
        hub.server.connections(),
        1,
        "neither failure cost the connection"
    );
}

#[tokio::test]
async fn an_oversized_item_ends_the_subscription_with_response_too_large() {
    /// Room for a 10-byte item head plus a 54-byte item.
    const LIMIT: usize = 64;
    const MAX_ITEM: usize = LIMIT - 10;

    let net = MemBulkNet::new();
    let limit = FrameLimit::new(LIMIT).unwrap();
    let mut hub = hub(
        &net,
        "s",
        RpcServerConfig {
            max_frame_bytes: limit,
            ..RpcServerConfig::default()
        },
    );
    let client = client(
        &net,
        "c",
        RpcConfig {
            max_frame_bytes: limit,
            ..RpcConfig::default()
        },
    );

    let mut subscription = open(&client, "s", "big:54").await;
    assert_eq!(
        subscription.next().await,
        Some(Ok(Bytes::from(vec![1u8; MAX_ITEM]))),
        "an item at the limit goes through"
    );
    assert_eq!(
        remote_code(subscription.next().await),
        RpcStatus::RESPONSE_TOO_LARGE
    );
    assert_eq!(subscription.next().await, None);
    assert_eq!(hub.errors.recv().await, Some(SinkError::TooLarge));
    assert_eq!(
        hub.errors.recv().await,
        Some(SinkError::TooLarge),
        "a later send fails the same way"
    );
    assert_eq!(hub.server.connections(), 1);
}

#[tokio::test]
async fn dropping_a_subscription_cancels_the_handler_and_fails_its_sink() {
    let net = MemBulkNet::new();
    let mut hub = hub(&net, "s", RpcServerConfig::default());
    let client = client(&net, "c", RpcConfig::default());

    let mut subscription = open(&client, "s", "lend").await;
    let mut sink = hub.lent.recv().await.expect("the handler lent its sink");
    sink.send(Bytes::from_static(b"pushed"))
        .await
        .expect("open");
    assert_eq!(subscription.next().await, Some(Ok(item("pushed"))));

    drop(subscription);
    eventually("the server to drop the cancelled handler", || {
        hub.probe.dropped.load(Ordering::SeqCst)
    })
    .await;
    assert_eq!(
        sink.send(Bytes::from_static(b"late")).await,
        Err(SinkError::Cancelled)
    );
    assert_eq!(
        client
            .call(&NodeId::new("s"), Bytes::from_static(b"ok"), CALL)
            .await,
        Ok(Bytes::from_static(b"ok"))
    );
    assert_eq!(hub.server.connections(), 1, "the cancel cost no connection");
}

#[tokio::test]
async fn credit_bounds_the_server_and_a_stalled_subscription_blocks_nothing_else() {
    const WINDOW: usize = 4;

    let net = MemBulkNet::new();
    let hub = hub(&net, "s", RpcServerConfig::default());
    let client = client(
        &net,
        "c",
        RpcConfig {
            subscription_window: NonZeroU32::new(4).unwrap(),
            ..RpcConfig::default()
        },
    );
    let to = NodeId::new("s");
    let flooded = || hub.probe.flooded.load(Ordering::SeqCst);

    let mut stalled = open(&client, "s", "flood").await;
    eventually("the flood to fill its window", || flooded() == WINDOW).await;

    // The same connection still carries a call and a second subscription,
    // which needs several grants of its own.
    assert_eq!(
        client.call(&to, Bytes::from_static(b"ping"), CALL).await,
        Ok(Bytes::from_static(b"ping"))
    );
    let mut other = open(&client, "s", "count:10").await;
    for i in 0..10 {
        assert_eq!(other.next().await, Some(Ok(item(&format!("c:{i}")))));
    }
    assert_eq!(other.next().await, None);
    assert_eq!(hub.server.connections(), 1, "all on one connection");
    assert_eq!(flooded(), WINDOW, "the unread flood never ran ahead");

    // Consuming grants credit half a window at a time, at the next `next`.
    for i in 0..WINDOW {
        assert_eq!(stalled.next().await, Some(Ok(item(&format!("f{i}")))));
    }
    eventually("the granted half window to be sent", || {
        flooded() == WINDOW + WINDOW / 2
    })
    .await;
    assert_eq!(
        client.call(&to, Bytes::from_static(b"ping"), CALL).await,
        Ok(Bytes::from_static(b"ping"))
    );
    assert_eq!(flooded(), WINDOW + WINDOW / 2, "and no further");
    assert_eq!(stalled.next().await, Some(Ok(item(&format!("f{WINDOW}")))));
}

#[tokio::test]
async fn a_server_without_a_subscribe_handler_refuses_and_keeps_serving_calls() {
    let net = MemBulkNet::new();
    let server = RpcServer::bind(
        plane(&net, "s"),
        |_from: NodeId, request: Bytes| async move { Ok::<_, RpcStatus>(request) },
    );
    let client = client(&net, "c", RpcConfig::default());

    let mut subscription = open(&client, "s", "count:1").await;
    assert_eq!(
        remote_code(subscription.next().await),
        RpcStatus::SUBSCRIPTIONS_UNSUPPORTED
    );
    assert_eq!(subscription.next().await, None);
    assert_eq!(
        client
            .call(&NodeId::new("s"), Bytes::from_static(b"ok"), CALL)
            .await,
        Ok(Bytes::from_static(b"ok"))
    );
    assert_eq!(server.connections(), 1);
}

#[tokio::test]
async fn an_oversized_zero_budget_or_unroutable_subscribe_is_refused_unsent() {
    /// Room for a 14-byte subscribe head plus a 50-byte payload.
    const LIMIT: usize = 64;

    let net = MemBulkNet::new();
    let hub = hub(&net, "s", RpcServerConfig::default());
    let client = client(
        &net,
        "c",
        RpcConfig {
            max_frame_bytes: FrameLimit::new(LIMIT).unwrap(),
            ..RpcConfig::default()
        },
    );
    let to = NodeId::new("s");

    assert_eq!(
        client
            .subscribe(&to, Bytes::from(vec![0u8; LIMIT - 14 + 1]), CALL)
            .await
            .err(),
        Some(RpcError::TooLarge)
    );
    assert_eq!(
        client
            .subscribe(&to, Bytes::from_static(b"count:1"), Duration::ZERO)
            .await
            .err(),
        Some(RpcError::NotSent)
    );
    assert_eq!(hub.server.connections(), 0, "nothing was sent");
    assert_eq!(
        client
            .subscribe(&NodeId::new("nobody"), Bytes::from_static(b"count:1"), CALL)
            .await
            .err(),
        Some(RpcError::Unreachable)
    );
    let mut fits = client
        .subscribe(&to, Bytes::from_static(b"count:1"), CALL)
        .await
        .expect("sent");
    assert_eq!(fits.next().await, Some(Ok(item("c:0"))));
    assert_eq!(fits.next().await, None);
}
