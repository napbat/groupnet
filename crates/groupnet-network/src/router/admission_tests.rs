use super::*;
use groupnet_testkit::cluster::eventually_within;
use groupnet_transport::admission::{AcceptedPeer, SessionRegistry};
use groupnet_transport::link::AdmittedInbound;

const WAIT: Duration = Duration::from_secs(10);

#[derive(Debug)]
struct Endpoint {
    incoming: AsyncMutex<mpsc::Receiver<AdmittedInbound>>,
    sent: mpsc::Sender<NodeId>,
    registry: SessionRegistry,
}

impl Transport for Endpoint {
    type Error = io::Error;

    fn send(&self, to: &NodeId, _bytes: &[u8]) -> impl Future<Output = io::Result<()>> {
        let _ = self.sent.try_send(to.clone());
        std::future::ready(Ok(()))
    }

    async fn send_admitted(
        &self,
        to: &NodeId,
        bytes: &[u8],
        expected: Option<SessionId>,
    ) -> io::Result<()> {
        if expected.is_some_and(|id| self.registry.is_active(to, id)) {
            self.send(to, bytes).await?;
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

#[tokio::test]
async fn live_sessions_refresh_recipients_and_withdraw_dependent_routes() -> io::Result<()> {
    let local = NodeId::new("local");
    let bootstrap = NodeId::new("bootstrap-only");
    let peer = NodeId::new("previously-unknown");
    let beyond = NodeId::new("beyond");
    let router = Router::new(local, RouterConfig::default())?;
    let registry = SessionRegistry::new(2)?;
    let (incoming, receive) = mpsc::channel(16);
    let (sent, mut recipients) = mpsc::channel(32);
    router
        .add_link(
            BoundLink::new(
                Endpoint {
                    incoming: AsyncMutex::new(receive),
                    sent,
                    registry: registry.clone(),
                },
                LinkConfig::new(vec![bootstrap.clone()]),
            )
            .with_sessions(registry.clone()),
        )
        .await?;
    let lease = registry.try_admit(AcceptedPeer::new(peer.clone()))?;
    let recipient = tokio::time::timeout(WAIT, recipients.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recipient, peer);
    assert_ne!(recipient, bootstrap);
    incoming
        .send(AdmittedInbound {
            packet: Inbound {
                from: peer.clone(),
                msg: wire::advert(1, &[peer.clone(), beyond.clone()]),
            },
            session: Some(lease.id()),
        })
        .await
        .unwrap();
    eventually_within("dependent route learned", WAIT, || {
        router.route_to(&beyond).is_some()
    })
    .await;
    let stale = lease.id();
    drop(lease);
    assert!(
        router.route_to(&beyond).is_none(),
        "generation gate withdraws immediately"
    );
    let replacement = registry.try_admit(AcceptedPeer::new(peer.clone()))?;
    incoming
        .send(AdmittedInbound {
            packet: Inbound {
                from: peer.clone(),
                msg: wire::advert(1, &[peer.clone(), beyond.clone()]),
            },
            session: Some(stale),
        })
        .await
        .unwrap();
    // The current-generation advert is a FIFO barrier after the stale frame.
    incoming
        .send(AdmittedInbound {
            packet: Inbound {
                from: peer.clone(),
                msg: wire::advert(0, std::slice::from_ref(&peer)),
            },
            session: Some(replacement.id()),
        })
        .await
        .unwrap();
    eventually_within("replacement route learned", WAIT, || {
        router.route_to(&peer).is_some()
    })
    .await;
    assert!(router.route_to(&beyond).is_none());
    eventually_within("reachable discovery snapshot refreshed", WAIT, || {
        let reachable = router.reachable();
        let snapshot = reachable.borrow();
        snapshot.contains(&peer) && !snapshot.contains(&beyond)
    })
    .await;
    router.close().await;
    Ok(())
}

#[test]
fn fragment_assemblies_do_not_cross_session_generations() {
    let registry = SessionRegistry::new(1).unwrap();
    let peer = NodeId::new("peer");
    let old = registry.try_admit(AcceptedPeer::new(peer.clone())).unwrap();
    let old_id = old.id();
    drop(old);
    let new = registry.try_admit(AcceptedPeer::new(peer.clone())).unwrap();
    let bytes: Arc<[u8]> = vec![3; 200].into();
    let parts: Vec<_> = wire::fragment(bytes.clone(), 128, [4; 16]).collect();
    let mut fragments = wire::Reassembly::default();
    assert!(
        fragments
            .receive(0, Some(old_id), &peer, parts[0].clone())
            .is_none()
    );
    assert!(
        fragments
            .receive(0, Some(new.id()), &peer, parts[1].clone())
            .is_none()
    );
    assert_eq!(
        fragments
            .receive(0, Some(new.id()), &peer, parts[0].clone())
            .unwrap(),
        bytes.as_ref()
    );
}
