use super::*;
use groupnet_testkit::cluster::eventually_within;
use groupnet_transport::link::{LinkFuture, LinkLifecycle};

const WAIT: Duration = Duration::from_secs(10);

#[derive(Debug, Default)]
struct Probe {
    updates: AsyncMutex<Vec<(NodeId, String)>>,
    dropped: AtomicBool,
}
#[derive(Debug)]
struct Endpoint {
    probe: Arc<Probe>,
    incoming: AsyncMutex<mpsc::Receiver<Inbound>>,
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        self.probe.dropped.store(true, Ordering::SeqCst);
    }
}

impl Transport for Endpoint {
    type Error = io::Error;

    fn send(&self, _to: &NodeId, _msg: &[u8]) -> impl Future<Output = io::Result<()>> {
        std::future::ready(Ok(()))
    }

    async fn recv(&self) -> io::Result<Inbound> {
        self.incoming.lock().await.recv().await.ok_or_else(closed)
    }

    fn learn_peer(&self, peer: &NodeId, address: &str) {
        self.probe
            .updates
            .try_lock()
            .expect("uncontended update log")
            .push((peer.clone(), address.to_owned()));
    }
}

fn endpoint(peers: Vec<NodeId>) -> (BoundLink, Arc<Probe>, mpsc::Sender<Inbound>) {
    let probe = Arc::new(Probe::default());
    let (send, incoming) = mpsc::channel(8);
    let link = BoundLink::new(
        Endpoint {
            probe: probe.clone(),
            incoming: AsyncMutex::new(incoming),
        },
        LinkConfig::new(peers),
    );
    (link, probe, send)
}

#[tokio::test]
async fn address_updates_reach_only_pre_admitted_links_and_never_admit_a_source() -> io::Result<()>
{
    let router = Router::new(NodeId::new("local"), RouterConfig::default())?;
    let peer = NodeId::new("peer");
    let other = NodeId::new("other");
    let unknown = NodeId::new("unknown");
    let (first, first_probe, incoming) = endpoint(vec![peer.clone()]);
    let (second, second_probe, _second_incoming) = endpoint(vec![other.clone()]);
    let (third, third_probe, _third_incoming) = endpoint(vec![peer.clone(), other]);
    router.add_link(first).await?;
    router.add_link(second).await?;
    router.add_link(third).await?;
    router.learn_peer(&peer, "old address");
    Transport::learn_peer(&router, &peer, "new address");
    router.learn_peer(&unknown, "gossiped address");
    let expected = vec![
        (peer.clone(), "old address".to_owned()),
        (peer.clone(), "new address".to_owned()),
    ];
    assert_eq!(*first_probe.updates.try_lock().unwrap(), expected);
    assert_eq!(*third_probe.updates.try_lock().unwrap(), expected);
    assert!(second_probe.updates.try_lock().unwrap().is_empty());
    incoming
        .send(Inbound {
            from: unknown.clone(),
            msg: wire::advert(0, std::slice::from_ref(&unknown)),
        })
        .await
        .unwrap();
    incoming
        .send(Inbound {
            from: peer.clone(),
            msg: wire::advert(0, std::slice::from_ref(&peer)),
        })
        .await
        .unwrap();
    // The admitted announcement is a FIFO barrier after the unauthorized one.
    eventually_within("admitted route", WAIT, || router.route_to(&peer).is_some()).await;
    assert!(router.route_to(&unknown).is_none());
    router.close().await;
    Ok(())
}

#[tokio::test]
async fn close_releases_endpoints_while_router_and_controls_survive() -> io::Result<()> {
    let router = Router::new(NodeId::new("local"), RouterConfig::default())?;
    let peer = NodeId::new("peer");
    let (link, probe, _incoming) = endpoint(vec![peer.clone()]);
    let control = link.driver.control();
    router.add_link(link).await?;
    router.learn_peer(&peer, "before close");
    assert!(!probe.dropped.load(Ordering::SeqCst));
    router.close().await;
    assert!(probe.dropped.load(Ordering::SeqCst));
    control.learn_peer(&peer, "retained control");
    router.learn_peer(&peer, "closed router");
    assert_eq!(probe.updates.try_lock().unwrap().len(), 1);
    router.close().await;
    Ok(())
}

#[derive(Debug, Default)]
struct DrainGate {
    entered: CancellationToken,
    release: CancellationToken,
    shutdown: CancellationToken,
}

impl LinkLifecycle for DrainGate {
    fn shutdown(&self) {
        self.shutdown.cancel();
    }

    fn close(&self) -> LinkFuture<'_, ()> {
        Box::pin(async {
            self.entered.cancel();
            self.release.cancelled().await;
        })
    }
}

#[tokio::test]
async fn cancelled_signals_initiation_without_waiting_for_protocol_drain() -> io::Result<()> {
    let router = Router::new(NodeId::new("local"), RouterConfig::default())?;
    let gate = Arc::new(DrainGate::default());
    let (link, probe, _incoming) = endpoint(vec![NodeId::new("peer")]);
    router.add_link(link.with_lifecycle(gate.clone())).await?;
    let closing = tokio::spawn({
        let router = router.clone();
        async move { router.close().await }
    });
    tokio::time::timeout(WAIT, router.cancelled())
        .await
        .expect("shutdown initiated");
    tokio::time::timeout(WAIT, gate.entered.cancelled())
        .await
        .expect("protocol draining");
    assert!(gate.shutdown.is_cancelled());
    assert!(probe.dropped.load(Ordering::SeqCst));
    assert!(
        !closing.is_finished(),
        "close must wait for protocol cleanup"
    );
    gate.release.cancel();
    tokio::time::timeout(WAIT, closing)
        .await
        .expect("drained")
        .expect("close task");
    Ok(())
}

#[tokio::test]
async fn protocol_namespaces_are_exclusive_isolated_and_generation_safe() -> io::Result<()> {
    let router = Router::new(NodeId::new("local"), RouterConfig::default())?;
    let application = router.bind_protocol(1)?;
    let other = router.bind_protocol(2)?;
    assert_eq!(
        router.bind_protocol(1).unwrap_err().kind(),
        io::ErrorKind::AlreadyExists
    );
    application.send(router.local_id(), b"opaque packet")?;
    other.send(router.local_id(), b"independent")?;
    let packet = application.recv().await?;
    assert_eq!(packet.from, *router.local_id());
    assert_eq!(packet.payload.as_ref(), b"opaque packet");
    assert_eq!(other.recv().await?.payload.as_ref(), b"independent");
    assert!(
        router
            .inner
            .messages
            .try_lock()
            .unwrap()
            .try_recv()
            .is_err()
    );
    let stale = application.clone();
    application.shutdown();
    assert!(!router.is_closed());
    assert!(!other.cancellation().is_cancelled());
    let replacement = router.bind_protocol(1)?;
    drop(application);
    stale.shutdown();
    drop(stale);
    assert_eq!(
        router.bind_protocol(1).unwrap_err().kind(),
        io::ErrorKind::AlreadyExists
    );
    replacement.send(router.local_id(), b"replacement")?;
    assert_eq!(replacement.recv().await?.payload.as_ref(), b"replacement");
    drop(replacement);
    let rebound = router.bind_protocol(1)?;
    router.shutdown();
    assert!(rebound.cancellation().is_cancelled());
    assert_eq!(
        other.recv().await.unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    router.close().await;
    Ok(())
}

#[tokio::test]
async fn protocol_capacity_and_unknown_destination_fail_closed() -> io::Result<()> {
    let router = Router::new(NodeId::new("local"), RouterConfig::default())?;
    assert!(
        router
            .inner
            .shared
            .send(router.local_id(), b"unknown", PayloadKind::Application(500))
            .is_err()
    );
    let mut endpoints = Vec::new();
    for id in 0..32 {
        endpoints.push(router.bind_protocol(id)?);
    }
    assert_eq!(
        router.bind_protocol(32).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    for _ in 0..128 {
        endpoints[0].send(router.local_id(), b"bounded")?;
    }
    assert_eq!(
        endpoints[0]
            .send(router.local_id(), b"overflow")
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    endpoints[1].send(router.local_id(), b"other queue")?;
    assert_eq!(endpoints[1].recv().await?.payload.as_ref(), b"other queue");
    router.close().await;
    Ok(())
}

#[tokio::test]
async fn send_validation_keeps_ordinary_capacity_and_prices_only_application_namespace()
-> io::Result<()> {
    for length in [1, 255] {
        let router = Router::new(NodeId::new("n".repeat(length)), RouterConfig::default())?;
        let application = router.bind_protocol(42)?;
        for (kind, overhead) in [
            (PayloadKind::Message, 24),
            (PayloadKind::Tunnel, 24),
            (PayloadKind::Application(42), 26),
        ] {
            let mut payload = vec![7; wire::MAX_FRAME - 2 * length - overhead];
            router
                .inner
                .shared
                .send(router.local_id(), &payload, kind)?;
            let received = match kind {
                PayloadKind::Message => router.recv().await?.msg,
                PayloadKind::Tunnel => router.recv_tunnel().await?.msg,
                PayloadKind::Application(_) => application.recv().await?.payload.to_vec(),
            };
            assert_eq!(received, payload);
            payload.push(7);
            assert_eq!(
                router
                    .inner
                    .shared
                    .send(router.local_id(), &payload, kind)
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidData
            );
        }
        router.close().await;
    }
    Ok(())
}
