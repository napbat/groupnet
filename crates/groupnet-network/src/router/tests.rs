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
