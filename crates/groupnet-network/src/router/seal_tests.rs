//! A sealing router (`RouterConfig::cluster_secret`) accepts only frames
//! sealed under its secret by the neighbor its link attributes them to,
//! checks fragment tags before reassembly, refuses replayed adverts, confirms
//! a frame's source address only after all of that, and seals everything it
//! sends, forwarded frames included.

use std::net::SocketAddr;

use super::*;
use groupnet_testkit::cluster::eventually_within;
use groupnet_transport::auth::{ClusterSecret, TAG_LEN};

use seal::Sealing;

const WAIT: Duration = Duration::from_secs(10);

#[derive(Debug)]
struct Endpoint {
    incoming: AsyncMutex<mpsc::Receiver<Inbound>>,
    sent: mpsc::Sender<(NodeId, Vec<u8>)>,
    confirmed: Arc<Mutex<Vec<(NodeId, SocketAddr)>>>,
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

    fn confirm_peer(&self, node: &NodeId, source: SocketAddr) {
        self.confirmed
            .lock()
            .expect("confirmations")
            .push((node.clone(), source));
    }
}

fn secret(byte: u8) -> ClusterSecret {
    ClusterSecret::new([byte; ClusterSecret::MIN_LEN]).expect("valid secret")
}

struct Harness {
    router: Router,
    incoming: mpsc::Sender<Inbound>,
    sent: mpsc::Receiver<(NodeId, Vec<u8>)>,
    confirmed: Arc<Mutex<Vec<(NodeId, SocketAddr)>>>,
}

impl Harness {
    async fn start(peers: &[&NodeId]) -> io::Result<Self> {
        Self::with_config(peers, RouterConfig::default()).await
    }

    /// A router sealing under `secret(1)` with `config`'s other settings.
    async fn with_config(peers: &[&NodeId], config: RouterConfig) -> io::Result<Self> {
        let router = Router::with_config(
            NodeId::new("local"),
            RouterConfig {
                cluster_secret: Some(secret(1)),
                ..config
            },
        )?;
        let (incoming, receive) = mpsc::channel(16);
        let (sent, sent_rx) = mpsc::channel(256);
        let confirmed = Arc::new(Mutex::new(Vec::new()));
        router
            .add_link(BoundLink::new(
                Endpoint {
                    incoming: AsyncMutex::new(receive),
                    sent,
                    confirmed: confirmed.clone(),
                },
                LinkConfig::new(peers.iter().map(|peer| (*peer).clone()).collect()),
            ))
            .await?;
        Ok(Self {
            router,
            incoming,
            sent: sent_rx,
            confirmed,
        })
    }

    async fn inject(&self, from: &NodeId, frame: impl Into<Bytes>) {
        self.inject_from(from, frame, None).await;
    }

    /// Hands the router `frame` claiming `from`, reported from `source`.
    async fn inject_from(
        &self,
        from: &NodeId,
        frame: impl Into<Bytes>,
        source: Option<SocketAddr>,
    ) {
        self.incoming
            .send(Inbound {
                from: from.clone(),
                msg: frame.into(),
                source,
            })
            .await
            .expect("link worker running");
    }

    fn confirmed(&self) -> Vec<(NodeId, SocketAddr)> {
        self.confirmed.lock().expect("confirmations").clone()
    }
}

/// `envelope` sealed under `secret` by `sender`.
fn sealed(secret: ClusterSecret, sender: &NodeId, mut envelope: Vec<u8>) -> Vec<u8> {
    Sealing::new(Some(secret)).seal(sender, &mut envelope);
    envelope
}

/// `from`'s own advert, ordered by `sequence`.
fn advert(from: &NodeId, sequence: u64) -> Vec<u8> {
    wire::advert(wire::test_advert(sequence), 0, std::slice::from_ref(from))
}

#[tokio::test]
async fn only_frames_sealed_under_the_secret_by_their_sender_are_learned() -> io::Result<()> {
    let peer = NodeId::new("peer");
    let intruder = NodeId::new("intruder");
    let harness = Harness::start(&[&peer]).await?;

    // Unsealed, sealed under another secret, and sealed by another node but
    // claimed by the admitted peer: all three are dropped and counted.
    harness.inject(&peer, advert(&peer, 1)).await;
    harness
        .inject(&peer, sealed(secret(2), &peer, advert(&peer, 2)))
        .await;
    harness
        .inject(&peer, sealed(secret(1), &intruder, advert(&peer, 3)))
        .await;
    // The genuine advert is a FIFO barrier behind them.
    harness
        .inject(&peer, sealed(secret(1), &peer, advert(&peer, 4)))
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
    assert_eq!(&frame[..4], b"GNS2");
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
        .inject(&second, sealed(secret(1), &second, advert(&second, 1)))
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

/// The blocker this guards: a datagram claiming a member's id must not move
/// that member's address. Only a frame whose seal verifies and that is not a
/// replay confirms the address it came from.
#[tokio::test]
async fn only_a_verified_fresh_frame_confirms_its_source() -> io::Result<()> {
    let peer = NodeId::new("peer");
    let local = NodeId::new("local");
    let intruder = NodeId::new("intruder");
    let spoofer: SocketAddr = "127.0.0.1:1".parse().expect("address");
    let genuine: SocketAddr = "127.0.0.1:2".parse().expect("address");
    let moved: SocketAddr = "127.0.0.1:3".parse().expect("address");
    let harness = Harness::start(&[&peer]).await?;
    let forger = Some(spoofer);

    // Unsealed, sealed under another secret, sealed by another node.
    harness.inject_from(&peer, advert(&peer, 1), forger).await;
    let foreign = sealed(secret(2), &peer, advert(&peer, 2));
    harness.inject_from(&peer, foreign, forger).await;
    let impersonated = sealed(secret(1), &intruder, advert(&peer, 3));
    harness.inject_from(&peer, impersonated, forger).await;
    // A genuine advert and a genuine data frame, each replayed from the
    // attacker's address.
    let genuine_advert = sealed(secret(1), &peer, advert(&peer, 4));
    harness
        .inject_from(&peer, genuine_advert.clone(), Some(genuine))
        .await;
    harness.inject_from(&peer, genuine_advert, forger).await;
    let data = wire::data(PayloadKind::Message, 16, [5; 16], &peer, &local, b"entry");
    let genuine_data = sealed(secret(1), &peer, data);
    harness
        .inject_from(&peer, genuine_data.clone(), Some(genuine))
        .await;
    harness.inject_from(&peer, genuine_data, forger).await;
    // A newer genuine advert from a new address is the FIFO barrier.
    let newer = sealed(secret(1), &peer, advert(&peer, 6));
    harness.inject_from(&peer, newer, Some(moved)).await;

    eventually_within("the barrier's source confirmed", WAIT, || {
        harness.confirmed().len() >= 3
    })
    .await;
    assert_eq!(
        harness.confirmed(),
        [
            (peer.clone(), genuine),
            (peer.clone(), genuine),
            (peer.clone(), moved)
        ]
    );
    assert_eq!(harness.router.auth_failures(), 3);
    harness.router.close().await;
    Ok(())
}

/// A captured advert replayed after its route expired does not bring the
/// route back: the receiver already accepted a newer one from that sender.
#[tokio::test]
async fn a_replayed_advert_does_not_revive_an_expired_route() -> io::Result<()> {
    let peer = NodeId::new("peer");
    let other = NodeId::new("other");
    let harness = Harness::with_config(
        &[&peer, &other],
        RouterConfig {
            announce_interval: Duration::from_millis(20),
            route_ttl: Duration::from_millis(300),
            ..RouterConfig::default()
        },
    )
    .await?;
    let captured = sealed(secret(1), &peer, advert(&peer, 1));
    harness.inject(&peer, captured.clone()).await;
    eventually_within("route learned", WAIT, || {
        harness.router.route_to(&peer).is_some()
    })
    .await;
    eventually_within("route expired", WAIT, || {
        harness.router.route_to(&peer).is_none()
    })
    .await;

    harness.inject(&peer, captured).await;
    // Another neighbor's advert is the FIFO barrier behind the replay.
    harness
        .inject(&other, sealed(secret(1), &other, advert(&other, 1)))
        .await;
    eventually_within("barrier route learned", WAIT, || {
        harness.router.route_to(&other).is_some()
    })
    .await;
    assert!(
        harness.router.route_to(&peer).is_none(),
        "the replay revived the route"
    );

    // The sender's next advert is newer and is learned again.
    harness
        .inject(&peer, sealed(secret(1), &peer, advert(&peer, 2)))
        .await;
    eventually_within("newer advert learned", WAIT, || {
        harness.router.route_to(&peer).is_some()
    })
    .await;
    assert_eq!(harness.router.auth_failures(), 0);
    harness.router.close().await;
    Ok(())
}

/// The re-review's lockout: a replayed sealed advert from an earlier boot of
/// the neighbor, one this router never tracked, must not stop the live
/// boot's adverts from being learned.
#[tokio::test]
async fn a_replayed_older_boot_does_not_lock_out_the_live_neighbor() -> io::Result<()> {
    let peer = NodeId::new("peer");
    let harness = Harness::with_config(
        &[&peer],
        RouterConfig {
            announce_interval: Duration::from_millis(20),
            route_ttl: Duration::from_millis(300),
            ..RouterConfig::default()
        },
    )
    .await?;
    let live = |sequence| wire::AdvertId {
        epoch: 20,
        nonce: [7; 8],
        sequence,
    };
    let older_boot = wire::AdvertId {
        epoch: 10,
        nonce: [3; 8],
        sequence: 50,
    };
    let sealed_advert = |id| {
        sealed(
            secret(1),
            &peer,
            wire::advert(id, 0, std::slice::from_ref(&peer)),
        )
    };
    harness.inject(&peer, sealed_advert(live(1))).await;
    eventually_within("route learned", WAIT, || {
        harness.router.route_to(&peer).is_some()
    })
    .await;
    eventually_within("route expired", WAIT, || {
        harness.router.route_to(&peer).is_none()
    })
    .await;

    harness.inject(&peer, sealed_advert(older_boot)).await;
    harness.inject(&peer, sealed_advert(live(2))).await;
    eventually_within("the live boot's next advert is learned", WAIT, || {
        harness.router.route_to(&peer).is_some()
    })
    .await;
    harness.router.close().await;
    Ok(())
}

/// `envelope`'s fragments at `mtu`, tagged as `sealing` tags them for `sender`.
fn fragments(sealing: &Sealing, sender: &NodeId, envelope: Vec<u8>, mtu: usize) -> Vec<Bytes> {
    wire::fragment(envelope.into(), mtu, wire::test_id(1), sealing.fragments())
        .map(|mut fragment| {
            sealing.seal_fragment(sender, &mut fragment);
            fragment.freeze()
        })
        .collect()
}

/// Fragments that claim a member but carry no valid tag are refused before
/// reassembly, so they cannot occupy its pending slots: a genuine fragmented
/// frame still completes with room for only one pending frame.
#[tokio::test]
async fn forged_fragments_do_not_fill_reassembly() -> io::Result<()> {
    let peer = NodeId::new("peer");
    let local = NodeId::new("local");
    let harness = Harness::with_config(
        &[&peer],
        RouterConfig {
            reassembly: ReassemblyConfig {
                max_pending: std::num::NonZeroUsize::MIN,
                ..ReassemblyConfig::default()
            },
            ..RouterConfig::default()
        },
    )
    .await?;
    let payload = vec![7; 300];
    let gossip = |id: u8| wire::data(PayloadKind::Message, 16, [id; 16], &peer, &local, &payload);

    // An open fragment and one tagged under another secret, both the first
    // of several, both claiming the admitted peer.
    let open = fragments(&Sealing::new(None), &peer, gossip(1), 128);
    let foreign_sealing = Sealing::new(Some(secret(2)));
    let foreign = fragments(
        &foreign_sealing,
        &peer,
        sealed(secret(2), &peer, gossip(2)),
        128,
    );
    assert!(open.len() > 1 && foreign.len() > 1);
    harness.inject(&peer, open[0].clone()).await;
    harness.inject(&peer, foreign[0].clone()).await;

    let sealing = Sealing::new(Some(secret(1)));
    for fragment in fragments(&sealing, &peer, sealed(secret(1), &peer, gossip(3)), 128) {
        harness.inject(&peer, fragment).await;
    }
    let inbound = tokio::time::timeout(WAIT, Transport::recv(&harness.router))
        .await
        .expect("the genuine fragmented frame completes")?;
    assert_eq!(
        (inbound.from, inbound.msg.as_ref()),
        (peer, payload.as_slice())
    );
    assert_eq!(harness.router.auth_failures(), 2);
    harness.router.close().await;
    Ok(())
}
