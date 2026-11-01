//! A sealing router (`RouterConfig::cluster_secret`) accepts only frames
//! sealed under its secret by the neighbor its link attributes them to, and
//! seals everything it sends, forwarded frames included.

use super::*;
use groupnet_testkit::cluster::eventually_within;
use groupnet_transport::auth::{ClusterSecret, TAG_LEN};

use seal::Sealing;

const WAIT: Duration = Duration::from_secs(10);

#[derive(Debug)]
struct Endpoint {
    incoming: AsyncMutex<mpsc::Receiver<Inbound>>,
    sent: mpsc::Sender<(NodeId, Vec<u8>)>,
}

impl Transport for Endpoint {
    type Error = io::Error;

    fn send(&self, to: &NodeId, bytes: &[u8]) -> impl Future<Output = io::Result<()>> {
        let _ = self.sent.try_send((to.clone(), bytes.to_vec()));
        std::future::ready(Ok(()))
    }

    async fn recv(&self) -> io::Result<Inbound> {
        self.incoming.lock().await.recv().await.ok_or_else(closed)
    }
}

fn secret(byte: u8) -> ClusterSecret {
    ClusterSecret::new([byte; ClusterSecret::MIN_LEN]).expect("valid secret")
}

struct Harness {
    router: Router,
    incoming: mpsc::Sender<Inbound>,
    sent: mpsc::Receiver<(NodeId, Vec<u8>)>,
}

impl Harness {
    async fn start(peers: &[&NodeId]) -> io::Result<Self> {
        let router = Router::with_config(
            NodeId::new("local"),
            RouterConfig {
                cluster_secret: Some(secret(1)),
                ..RouterConfig::default()
            },
        )?;
        let (incoming, receive) = mpsc::channel(16);
        let (sent, sent_rx) = mpsc::channel(256);
        router
            .add_link(BoundLink::new(
                Endpoint {
                    incoming: AsyncMutex::new(receive),
                    sent,
                },
                LinkConfig::new(peers.iter().map(|peer| (*peer).clone()).collect()),
            ))
            .await?;
        Ok(Self {
            router,
            incoming,
            sent: sent_rx,
        })
    }

    async fn inject(&self, from: &NodeId, frame: Vec<u8>) {
        self.incoming
            .send(Inbound {
                from: from.clone(),
                msg: frame.into(),
            })
            .await
            .expect("link worker running");
    }
}

/// `envelope` sealed under `secret` by `sender`.
fn sealed(secret: ClusterSecret, sender: &NodeId, mut envelope: Vec<u8>) -> Vec<u8> {
    Sealing::new(Some(secret)).seal(sender, &mut envelope);
    envelope
}

fn advert(from: &NodeId) -> Vec<u8> {
    wire::advert(0, std::slice::from_ref(from))
}

#[tokio::test]
async fn only_frames_sealed_under_the_secret_by_their_sender_are_learned() -> io::Result<()> {
    let peer = NodeId::new("peer");
    let intruder = NodeId::new("intruder");
    let harness = Harness::start(&[&peer]).await?;

    // Unsealed, sealed under another secret, and sealed by another node but
    // claimed by the admitted peer: all three are dropped and counted.
    harness.inject(&peer, advert(&peer)).await;
    harness
        .inject(&peer, sealed(secret(2), &peer, advert(&peer)))
        .await;
    harness
        .inject(&peer, sealed(secret(1), &intruder, advert(&peer)))
        .await;
    // The genuine advert is a FIFO barrier behind them.
    harness
        .inject(&peer, sealed(secret(1), &peer, advert(&peer)))
        .await;
    eventually_within("sealed route", WAIT, || {
        harness.router.route_to(&peer).is_some()
    })
    .await;
    assert_eq!(harness.router.auth_failures(), 3);
    harness.router.close().await;
    Ok(())
}

#[tokio::test]
async fn sealed_messages_are_delivered_without_their_seal() -> io::Result<()> {
    let peer = NodeId::new("peer");
    let local = NodeId::new("local");
    let harness = Harness::start(&[&peer]).await?;
    let gossip = |payload: &[u8], sequence: u8| {
        wire::data(
            PayloadKind::Message,
            16,
            [sequence; 16],
            &peer,
            &local,
            payload,
        )
    };

    harness.inject(&peer, gossip(b"forged entry", 1)).await;
    harness
        .inject(&peer, sealed(secret(1), &peer, gossip(b"entry", 2)))
        .await;
    let inbound = Transport::recv(&harness.router).await?;
    assert_eq!(inbound.from, peer);
    assert_eq!(inbound.msg.as_ref(), b"entry");
    assert_eq!(harness.router.auth_failures(), 1);
    harness.router.close().await;
    Ok(())
}

#[tokio::test]
async fn every_frame_sent_carries_the_routers_seal() -> io::Result<()> {
    let peer = NodeId::new("peer");
    let local = NodeId::new("local");
    let mut harness = Harness::start(&[&peer]).await?;
    let (to, frame) = tokio::time::timeout(WAIT, harness.sent.recv())
        .await
        .expect("an announcement")
        .expect("link open");
    assert_eq!(to, peer);
    assert_eq!(&frame[..4], b"GNS1");
    let opener = Sealing::new(Some(secret(1)));
    assert_eq!(opener.open(&local, &frame), Some(frame.len() - TAG_LEN));
    assert!(opener.open(&peer, &frame).is_none());
    harness.router.close().await;
    Ok(())
}

/// A forwarder decrements the hop count and seals the frame again under its
/// own id, so the next hop accepts it from the neighbor that sent it.
#[tokio::test]
async fn a_forwarded_frame_is_resealed_by_the_forwarder() -> io::Result<()> {
    let first = NodeId::new("first");
    let second = NodeId::new("second");
    let local = NodeId::new("local");
    let mut harness = Harness::start(&[&first, &second]).await?;
    harness
        .inject(&second, sealed(secret(1), &second, advert(&second)))
        .await;
    eventually_within("route to second", WAIT, || {
        harness.router.route_to(&second).is_some()
    })
    .await;
    let data = wire::data(PayloadKind::Message, 16, [9; 16], &first, &second, b"relay");
    harness
        .inject(&first, sealed(secret(1), &first, data))
        .await;

    let opener = Sealing::new(Some(secret(1)));
    loop {
        let (to, frame) = tokio::time::timeout(WAIT, harness.sent.recv())
            .await
            .expect("the forwarded frame")
            .expect("link open");
        let length = opener
            .open(&local, &frame)
            .expect("sealed by the forwarder");
        if let Ok(wire::Frame::Data {
            hops,
            from,
            payload,
            ..
        }) = wire::decode_bounded(&frame[..length], wire::SEALED, wire::MAX_FRAME, 16)
        {
            assert_eq!(to, second);
            assert_eq!((hops, from, payload), (15, first.clone(), &b"relay"[..]));
            break;
        }
    }
    assert_eq!(harness.router.auth_failures(), 0);
    harness.router.close().await;
    Ok(())
}
