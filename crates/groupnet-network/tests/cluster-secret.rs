//! Keyed routers over real links: a UDP address book changes only for frames
//! whose seal verifies, and sealed frames larger than a link's MTU cross it as
//! tagged fragments.

use std::io;
use std::num::{NonZeroU32, NonZeroUsize};
use std::time::Duration;

use groupnet_core::NodeId;
// The transport contract types the router takes come from this crate's re-exports.
use groupnet_network::{BoundLink, ClusterSecret, LinkConfig, Router, RouterConfig, Transport};
use groupnet_testkit::cluster::eventually_within;
use groupnet_transport_mem::Network;
use groupnet_transport_udp::UdpTransport;

const WAIT: Duration = Duration::from_secs(10);

fn keyed(name: &str) -> io::Result<Router> {
    Router::with_config(
        NodeId::new(name),
        RouterConfig {
            announce_interval: Duration::from_millis(30),
            cluster_secret: Some(
                ClusterSecret::new([9; ClusterSecret::MIN_LEN]).expect("valid secret"),
            ),
            ..RouterConfig::default()
        },
    )
}

async fn udp(name: &str) -> io::Result<UdpTransport> {
    UdpTransport::bind(NodeId::new(name), "127.0.0.1:0").await
}

/// A node without the secret sends datagrams that claim `member` to
/// `target`: an unsealed router envelope and one with a forged seal.
async fn impersonate(member: &NodeId, target: &UdpTransport, target_id: &NodeId) -> io::Result<()> {
    let impostor = UdpTransport::bind(member.clone(), "127.0.0.1:0").await?;
    impostor.register_peer(target_id.clone(), target.local_addr())?;
    impostor.send(target_id, b"GNR4 unsealed frame").await?;
    impostor
        .send(target_id, &[b"GNS2".as_slice(), &[0; 64]].concat())
        .await
}

/// The review's attack: a non-member's datagrams claiming node-b used to
/// rebind node-a's address for node-b to the attacker before the router saw
/// them, redirecting node-a's sealed traffic for node-b.
#[tokio::test]
async fn a_claimed_member_id_does_not_move_its_udp_address() -> io::Result<()> {
    let (a_id, b_id) = (NodeId::new("node-a"), NodeId::new("node-b"));
    let (a_udp, b_udp) = (udp("node-a").await?, udp("node-b").await?);
    let b_addr = b_udp.local_addr();
    a_udp.register_peer(b_id.clone(), b_addr)?;
    b_udp.register_peer(a_id.clone(), a_udp.local_addr())?;
    let book = a_udp.clone();
    let a = keyed("node-a")?;
    a.add_link(BoundLink::new(a_udp, LinkConfig::new(vec![b_id.clone()])))
        .await?;
    let b = keyed("node-b")?;
    b.add_link(BoundLink::new(b_udp, LinkConfig::new(vec![a_id.clone()])))
        .await?;
    eventually_within("node-a reaches node-b", WAIT, || {
        a.route_to(&b_id).is_some()
    })
    .await;
    // node-b goes quiet, so no genuine frame could repair a moved address.
    b.close().await;

    impersonate(&b_id, &book, &a_id).await?;
    eventually_within("both forged datagrams refused", WAIT, || {
        a.auth_failures() >= 2
    })
    .await;
    assert_eq!(book.direct_addr_to(&b_id), Some(b_addr));
    a.close().await;
    Ok(())
}

/// A member nobody registered becomes reachable once its first sealed frame
/// verifies, and not through a non-member that claimed its id first.
#[tokio::test]
async fn a_verified_frame_teaches_an_unregistered_udp_address() -> io::Result<()> {
    let (a_id, b_id) = (NodeId::new("node-a"), NodeId::new("node-b"));
    let a_udp = udp("node-a").await?;
    let book = a_udp.clone();
    let a = keyed("node-a")?;
    a.add_link(BoundLink::new(a_udp, LinkConfig::new(vec![b_id.clone()])))
        .await?;

    impersonate(&b_id, &book, &a_id).await?;
    eventually_within("both forged datagrams refused", WAIT, || {
        a.auth_failures() >= 2
    })
    .await;
    assert_eq!(book.direct_addr_to(&b_id), None);

    let b_udp = udp("node-b").await?;
    let b_addr = b_udp.local_addr();
    b_udp.register_peer(a_id.clone(), book.local_addr())?;
    let b = keyed("node-b")?;
    b.add_link(BoundLink::new(b_udp, LinkConfig::new(vec![a_id.clone()])))
        .await?;
    eventually_within("node-b's sealed frames confirm its address", WAIT, || {
        book.direct_addr_to(&b_id) == Some(b_addr)
    })
    .await;
    eventually_within("routes both ways", WAIT, || {
        a.route_to(&b_id).is_some() && b.route_to(&a_id).is_some()
    })
    .await;
    a.close().await;
    b.close().await;
    Ok(())
}

/// A sealed message several times a link's MTU crosses it as tagged
/// fragments and arrives whole.
#[tokio::test]
async fn sealed_frames_cross_a_small_mtu_as_tagged_fragments() -> io::Result<()> {
    let (a, b) = (keyed("frag-a")?, keyed("frag-b")?);
    let net = Network::new();
    let mtu = NonZeroUsize::new(256).expect("nonzero mtu");
    for (router, peer) in [(&a, &b), (&b, &a)] {
        router.add_transport(
            net.endpoint(router.local_id().clone()),
            LinkConfig {
                cost: NonZeroU32::MIN,
                mtu,
                ..LinkConfig::new(vec![peer.local_id().clone()])
            },
        )?;
    }
    eventually_within("frag-a reaches frag-b", WAIT, || {
        a.route_to(b.local_id()).is_some()
    })
    .await;
    let payload: Vec<u8> = (0..4000_u32).map(|byte| byte.to_le_bytes()[0]).collect();
    Transport::send(&a, b.local_id(), &payload).await?;
    let inbound = tokio::time::timeout(WAIT, Transport::recv(&b))
        .await
        .expect("the fragmented message arrives")?;
    assert_eq!(inbound.from, *a.local_id());
    assert_eq!(inbound.msg.as_ref(), payload.as_slice());
    assert_eq!((a.auth_failures(), b.auth_failures()), (0, 0));
    a.close().await;
    b.close().await;
    Ok(())
}
