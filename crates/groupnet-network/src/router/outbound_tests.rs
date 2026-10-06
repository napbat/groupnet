use super::*;
use futures_util::StreamExt;
use groupnet_testkit::cluster::eventually_within;
use groupnet_transport::admission::AcceptedPeer;

const WAIT: Duration = Duration::from_secs(10);

#[derive(Debug)]
struct BlockedEndpoint {
    incoming: AsyncMutex<mpsc::Receiver<AdmittedInbound>>,
    registry: SessionRegistry,
    entered: CancellationToken,
    release: CancellationToken,
    delivered: mpsc::Sender<Vec<u8>>,
}

impl Transport for BlockedEndpoint {
    type Error = io::Error;

    fn send(&self, _peer: &NodeId, _bytes: &[u8]) -> impl Future<Output = io::Result<()>> {
        std::future::ready(Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "dynamic sends require generation",
        )))
    }

    async fn send_admitted(
        &self,
        peer: &NodeId,
        bytes: &[u8],
        expected: Option<SessionId>,
    ) -> io::Result<()> {
        let Ok(wire::Frame::Data { payload, .. }) = wire::decode(bytes) else {
            return Ok(());
        };
        if payload == b"blocked" {
            self.entered.cancel();
            self.release.cancelled().await;
        }
        if expected.is_some_and(|id| self.registry.is_active(peer, id)) {
            self.delivered
                .send(payload.to_vec())
                .await
                .map_err(|_| closed())?;
        }
        Ok(())
    }

    async fn recv(&self) -> io::Result<Inbound> {
        self.recv_admitted().await.map(|frame| frame.packet)
    }

    async fn recv_admitted(&self) -> io::Result<AdmittedInbound> {
        self.incoming.lock().await.recv().await.ok_or_else(closed)
    }
}

async fn advertise(incoming: &mpsc::Sender<AdmittedInbound>, peer: &NodeId, id: SessionId) {
    incoming
        .send(AdmittedInbound {
            packet: Inbound {
                from: peer.clone(),
                msg: wire::advert(0, std::slice::from_ref(peer)).into(),
            },
            session: Some(id),
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn blocked_and_router_queued_frames_cannot_cross_into_replacement_session() -> io::Result<()>
{
    let router = Router::new(NodeId::new("local"), RouterConfig::default())?;
    let peer = NodeId::new("peer");
    let registry = SessionRegistry::new(1)?;
    let first = registry.try_admit(AcceptedPeer::new(peer.clone()))?;
    let (incoming, receive) = mpsc::channel(16);
    let (delivered, mut packets) = mpsc::channel(16);
    let entered = CancellationToken::new();
    let release = CancellationToken::new();
    router
        .add_link(
            BoundLink::new(
                BlockedEndpoint {
                    incoming: AsyncMutex::new(receive),
                    registry: registry.clone(),
                    entered: entered.clone(),
                    release: release.clone(),
                    delivered,
                },
                LinkConfig::new(Vec::new()),
            )
            .with_sessions(registry.clone()),
        )
        .await?;
    advertise(&incoming, &peer, first.id()).await;
    eventually_within("initial route", WAIT, || router.route_to(&peer).is_some()).await;
    router.send(&peer, b"blocked").await?;
    tokio::time::timeout(WAIT, entered.cancelled())
        .await
        .unwrap();
    router.send(&peer, b"queued").await?;
    drop(first);
    assert!(router.route_to(&peer).is_none());
    let replacement = registry.try_admit(AcceptedPeer::new(peer.clone()))?;
    advertise(&incoming, &peer, replacement.id()).await;
    eventually_within("replacement route", WAIT, || {
        router.route_to(&peer).is_some()
    })
    .await;
    router.send(&peer, b"fresh").await?;
    release.cancel();
    assert_eq!(
        tokio::time::timeout(WAIT, packets.recv())
            .await
            .unwrap()
            .unwrap(),
        b"fresh"
    );
    assert!(packets.try_recv().is_err());
    router.close().await;
    Ok(())
}

#[tokio::test]
async fn remaining_fragments_keep_original_generation_and_stop_after_revocation() -> io::Result<()>
{
    let router = Router::new(NodeId::new("local"), RouterConfig::default())?;
    let shared = router.inner.shared.clone();
    let peer = NodeId::new("peer");
    let registry = SessionRegistry::new(1)?;
    let first = registry.try_admit(AcceptedPeer::new(peer.clone()))?;
    let (queued, receive) = mpsc::channel(16);
    let mut io = adapters::io(
        shared,
        TransportId(0),
        Vec::new(),
        Some(&registry),
        128,
        receive,
        CancellationToken::new(),
    );
    queued
        .send(Queued {
            peer: peer.clone(),
            bytes: vec![9; 250].into(),
            session: Some(first.id()),
        })
        .await
        .unwrap();
    loop {
        let packet = tokio::time::timeout(WAIT, io.outgoing.next())
            .await
            .unwrap()
            .unwrap();
        if packet.bytes().starts_with(b"GNF1") {
            assert_eq!(packet.session, Some(first.id()));
            break;
        }
    }
    drop(first);
    let replacement = registry.try_admit(AcceptedPeer::new(peer.clone()))?;
    queued
        .send(Queued {
            peer: peer.clone(),
            bytes: Bytes::from_static(b"fresh"),
            session: Some(replacement.id()),
        })
        .await
        .unwrap();
    loop {
        let packet = tokio::time::timeout(WAIT, io.outgoing.next())
            .await
            .unwrap()
            .unwrap();
        assert!(
            !packet.bytes().starts_with(b"GNF1"),
            "revoked remaining fragments must not emit"
        );
        if packet.bytes() == b"fresh" {
            assert_eq!(packet.session, Some(replacement.id()));
            break;
        }
    }
    router.close().await;
    Ok(())
}
