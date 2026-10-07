use super::*;
use groupnet_testkit::cluster::eventually_within;
use groupnet_transport::link::{LinkFuture, LinkLifecycle};
use std::sync::atomic::{AtomicBool, Ordering};

const WAIT: Duration = Duration::from_secs(10);

/// Records tunnel packets that link workers deliver inline.
#[derive(Default)]
pub(super) struct TunnelCapture(pub(super) Mutex<Vec<(NodeId, Bytes)>>);

impl TunnelInbox for TunnelCapture {
    fn deliver(&self, from: NodeId, payload: Bytes) {
        self.0.lock().expect("capture lock").push((from, payload));
    }
}

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
            msg: wire::advert(0, std::slice::from_ref(&unknown)).into(),
        })
        .await
        .unwrap();
    incoming
        .send(Inbound {
            from: peer.clone(),
            msg: wire::advert(0, std::slice::from_ref(&peer)).into(),
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
        let tunnels = Arc::new(TunnelCapture::default());
        router.claim_tunnels(tunnels.clone())?;
        assert_eq!(
            router
                .claim_tunnels(Arc::new(TunnelCapture::default()))
                .unwrap_err()
                .kind(),
            io::ErrorKind::AlreadyExists
        );
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
                PayloadKind::Tunnel => {
                    let (from, payload) = tunnels.0.lock().expect("capture").pop().expect("inline");
                    assert_eq!(&from, router.local_id());
                    payload
                }
                PayloadKind::Application(_) => application.recv().await?.payload,
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

#[tokio::test]
async fn sent_packet_storage_returns_to_the_pool_when_its_last_view_drops() -> io::Result<()> {
    let router = Router::new(NodeId::new("local"), RouterConfig::default())?;
    let peer = NodeId::new("peer");
    let segment = 16 * 1024;
    let packet = router.tunnel_packet_buffer(&peer, segment)?;
    let storage = packet.payload().as_ptr();
    drop(packet);
    let mut packet = router.tunnel_packet_buffer(&peer, segment)?;
    assert_eq!(
        packet.payload().as_ptr(),
        storage,
        "unsent storage is reused"
    );
    packet.extend_from_slice(&vec![7; segment]);
    let sent = router.send_tunnel_retained(&peer, packet)?;
    assert_eq!(sent.as_ptr(), storage, "sent without copying");
    let other = router.tunnel_packet_buffer(&peer, segment)?;
    assert_ne!(
        other.payload().as_ptr(),
        storage,
        "in flight storage is not reused"
    );
    drop(other);
    let retransmission = sent.clone();
    drop(sent);
    assert_ne!(
        router
            .tunnel_packet_buffer(&peer, segment)?
            .payload()
            .as_ptr(),
        storage
    );
    drop(retransmission);
    assert_eq!(
        router
            .tunnel_packet_buffer(&peer, segment)?
            .payload()
            .as_ptr(),
        storage,
        "returned when the last view drops"
    );
    router.close().await;
    Ok(())
}

#[tokio::test]
async fn owned_protocol_send_and_receive_preserve_headroom_allocation() -> io::Result<()> {
    let router = Router::new(NodeId::new("local"), RouterConfig::default())?;
    let application = router.bind_protocol(42)?;
    let mut encoded = application.packet_buffer(router.local_id(), 8)?;
    encoded.extend_from_slice(b"original");
    encoded.payload_mut()[0] = b'O';
    let ptr = encoded.payload().as_ptr() as usize;
    application.send_packet(router.local_id(), encoded)?;
    let delivered = application.recv().await?;
    assert_eq!(delivered.payload.as_ptr() as usize, ptr);
    assert_eq!(delivered.payload, b"Original".as_slice());
    let owned = Bytes::from(vec![8; 128]);
    let ptr = owned.as_ptr() as usize;
    application.send_owned(router.local_id(), owned)?;
    assert_eq!(application.recv().await?.payload.as_ptr() as usize, ptr);

    let peer = NodeId::new("peer");
    let (link, _, incoming) = endpoint(vec![peer.clone()]);
    router.add_link(link).await?;
    let frame = Bytes::from(wire::data(
        PayloadKind::Application(42),
        16,
        [3; 16],
        &peer,
        router.local_id(),
        b"inbound",
    ));
    let offset = PayloadKind::Application(42).header_len(&peer, router.local_id());
    let payload_ptr = frame[offset..].as_ptr() as usize;
    incoming
        .send(Inbound {
            from: peer,
            msg: frame,
        })
        .await
        .unwrap();
    let received = application.recv().await?;
    assert_eq!(received.payload.as_ptr() as usize, payload_ptr);
    assert_eq!(received.payload, b"inbound".as_slice());
    let mismatch = application.packet_buffer(router.local_id(), 0)?;
    assert!(
        application
            .send_packet(&NodeId::new("other"), mismatch)
            .is_err()
    );
    router.close().await;
    Ok(())
}

#[tokio::test]
async fn configured_protocol_and_queue_capacities_replace_operational_constants() -> io::Result<()>
{
    let config = RouterConfig {
        max_protocols: 40,
        protocol_queue: QueueCapacity::of(2),
        max_frame: 70_000,
        ..RouterConfig::default()
    };
    let router = Router::new(NodeId::new("local"), config)?;
    let mut endpoints = Vec::new();
    for id in 0..40 {
        endpoints.push(router.bind_protocol(id)?);
    }
    assert_eq!(
        router.bind_protocol(40).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(
        endpoints[0].max_payload(router.local_id()),
        70_000 - 26 - 2 * "local".len()
    );
    for _ in 0..2 {
        endpoints[0].send(router.local_id(), b"bounded")?;
    }
    assert_eq!(
        endpoints[0]
            .send(router.local_id(), b"overflow")
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    router.close().await;
    Ok(())
}

#[test]
fn routing_policy_rejects_only_invalid_resource_or_wire_bounds() {
    assert!(
        RouterConfig {
            max_routes: 8192,
            max_transports: 4097,
            ..RouterConfig::default()
        }
        .validate()
        .is_ok()
    );
    assert!(
        RouterConfig {
            replay_capacity: 0,
            ..RouterConfig::default()
        }
        .validate()
        .is_err()
    );
    assert!(
        RouterConfig {
            max_hops: 256,
            ..RouterConfig::default()
        }
        .validate()
        .is_err()
    );
    assert!(
        RouterConfig {
            max_frame: wire::MAX_DATA_HEADER - 1,
            ..RouterConfig::default()
        }
        .validate()
        .is_err()
    );
    assert!(
        RouterConfig {
            reassembly: ReassemblyConfig {
                max_fragments: 65_536,
                ..ReassemblyConfig::default()
            },
            ..RouterConfig::default()
        }
        .validate()
        .is_err()
    );
    assert!(
        RouterConfig {
            send_timeout: Duration::MAX,
            ..RouterConfig::default()
        }
        .validate()
        .is_err()
    );
}

#[tokio::test]
async fn foreign_packet_headroom_is_rejected_for_longer_and_shorter_sources() -> io::Result<()> {
    for (creator, sender) in [("a", "longer-source"), ("longer-source", "a")] {
        let creator = Router::new(NodeId::new(creator), RouterConfig::default())?;
        let sender = Router::new(NodeId::new(sender), RouterConfig::default())?;
        let origin = creator.bind_protocol(42)?;
        let destination = sender.bind_protocol(42)?;
        let mut packet = origin.packet_buffer(sender.local_id(), 7)?;
        packet.extend_from_slice(b"payload");
        assert!(destination.send_packet(sender.local_id(), packet).is_err());
        creator.close().await;
        sender.close().await;
    }
    Ok(())
}
