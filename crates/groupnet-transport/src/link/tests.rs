use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug)]
struct Endpoint {
    dropped: Arc<AtomicBool>,
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

impl Transport for Endpoint {
    type Error = io::Error;

    fn send(&self, _to: &NodeId, _msg: &[u8]) -> impl Future<Output = io::Result<()>> {
        std::future::ready(Ok(()))
    }

    async fn recv(&self) -> io::Result<Inbound> {
        std::future::pending().await
    }
}

#[test]
fn bound_link_controls_do_not_retain_closed_endpoint() {
    let dropped = Arc::new(AtomicBool::new(false));
    let link = BoundLink::new(
        Endpoint {
            dropped: dropped.clone(),
        },
        LinkConfig::new(Vec::new()),
    );
    let control = link.driver.control();
    let provider: Box<dyn LinkProvider> = Box::new(link);
    let bound = futures::executor::block_on(provider.bind(NodeId::new("local"))).unwrap();
    let retained_control = bound.driver.control();
    assert!(!dropped.load(Ordering::SeqCst));
    futures::executor::block_on(bound.driver.close());
    assert!(dropped.load(Ordering::SeqCst));
    drop((control, retained_control));
}

#[test]
fn outbound_transfers_owned_allocation() {
    let bytes = vec![8; 512];
    let ptr = bytes.as_ptr();
    let outbound = Outbound::owned(NodeId::new("peer"), bytes, Instant::now());
    assert_eq!(outbound.bytes().as_ptr(), ptr);
    let owned = outbound.into_bytes();
    assert_eq!(owned.as_ptr(), ptr);
    let shared = Outbound::shared(NodeId::new("peer"), owned.clone(), Instant::now());
    assert_eq!(shared.into_bytes().as_ptr(), ptr);
    assert_eq!(owned, vec![8; 512]);
}

#[derive(Debug)]
struct OwnedEndpoint {
    observed: Arc<std::sync::atomic::AtomicUsize>,
    expected: SessionId,
}

impl Transport for OwnedEndpoint {
    type Error = io::Error;

    fn send(&self, _to: &NodeId, _msg: &[u8]) -> impl Future<Output = io::Result<()>> + Send {
        std::future::ready(Err(io::Error::other("owned worker used borrowed send")))
    }

    fn send_owned_admitted(
        &self,
        _to: &NodeId,
        msg: bytes::Bytes,
        session: Option<SessionId>,
    ) -> impl Future<Output = io::Result<()>> + Send {
        assert_eq!(session, Some(self.expected));
        self.observed.store(msg.as_ptr() as usize, Ordering::SeqCst);
        std::future::ready(Ok(()))
    }

    async fn recv(&self) -> io::Result<Inbound> {
        std::future::pending().await
    }
}

#[tokio::test]
async fn link_worker_transfers_the_allocation_and_selected_generation() {
    let registry = SessionRegistry::new(1).unwrap();
    let peer = NodeId::new("peer");
    let lease = registry
        .try_admit(crate::admission::AcceptedPeer::new(peer.clone()))
        .unwrap();
    let observed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let bound = BoundLink::new(
        OwnedEndpoint {
            observed: observed.clone(),
            expected: lease.id(),
        },
        LinkConfig::new(vec![peer.clone()]),
    );
    let bytes = vec![3; 512];
    let ptr = bytes.as_ptr() as usize;
    let outgoing = futures_util::stream::iter([Outbound::owned(
        peer,
        bytes,
        Instant::now() + std::time::Duration::from_secs(1),
    )
    .with_session(Some(lease.id()))]);
    let incoming = futures_util::sink::unfold((), |(), _packet: Option<AdmittedInbound>| async {
        Ok::<_, io::Error>(())
    });
    bound
        .driver
        .run(LinkIo {
            outgoing: Box::pin(outgoing),
            incoming: Box::pin(incoming),
            direct: None,
            cancel: CancellationToken::new(),
            mtu: 65_000,
        })
        .await;
    assert_eq!(observed.load(Ordering::SeqCst), ptr);
}

#[derive(Debug, Default)]
struct Pushing {
    sink: std::sync::OnceLock<InboundSink>,
}

impl Transport for Arc<Pushing> {
    type Error = io::Error;

    fn send(&self, _to: &NodeId, _msg: &[u8]) -> impl Future<Output = io::Result<()>> + Send {
        std::future::ready(Ok(()))
    }

    async fn recv(&self) -> io::Result<Inbound> {
        std::future::pending().await
    }

    fn attach_inbound(&self, sink: InboundSink) {
        assert!(self.sink.set(sink).is_ok(), "offered once per worker");
    }
}

#[derive(Debug)]
struct Collected(std::sync::mpsc::Sender<(usize, Option<SessionId>)>);

impl InboundDelivery for Collected {
    fn deliver(&self, packet: AdmittedInbound) {
        let _ = self.0.send((packet.packet.msg.len(), packet.session));
    }
}

#[tokio::test]
async fn worker_offers_the_direct_sink_bounded_by_the_link_mtu() {
    let transport = Arc::new(Pushing::default());
    let bound = BoundLink::new(transport.clone(), LinkConfig::new(Vec::new()));
    let (collected, delivered) = std::sync::mpsc::channel();
    let cancel = CancellationToken::new();
    let incoming = futures_util::SinkExt::sink_map_err(
        futures_util::sink::drain::<Option<AdmittedInbound>>(),
        |never: std::convert::Infallible| match never {},
    );
    let worker = tokio::spawn(bound.driver.run(LinkIo {
        outgoing: Box::pin(futures_util::stream::pending()),
        incoming: Box::pin(incoming),
        direct: Some(InboundSink::new(Arc::new(Collected(collected)), 8)),
        cancel: cancel.clone(),
        mtu: 8,
    }));
    let sink = loop {
        if let Some(sink) = transport.sink.get() {
            break sink.clone();
        }
        tokio::task::yield_now().await;
    };
    let registry = SessionRegistry::new(1).unwrap();
    let session = registry
        .try_admit(crate::admission::AcceptedPeer::new(NodeId::new("peer")))
        .unwrap()
        .id();
    for length in [8, 9] {
        sink.deliver(AdmittedInbound {
            packet: Inbound {
                from: NodeId::new("peer"),
                msg: bytes::Bytes::from(vec![0; length]),
            },
            session: Some(session),
        });
    }
    assert_eq!(
        delivered.try_iter().collect::<Vec<_>>(),
        [(8, Some(session))],
        "delivered inline with its generation; the oversized frame dropped"
    );
    cancel.cancel();
    worker.await.unwrap();
}
