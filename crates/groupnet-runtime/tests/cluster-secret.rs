//! A cluster secret (`RouterConfig::cluster_secret`) admits only nodes that
//! hold it: their gossip flows, while every frame of a node without it, or
//! with another secret, is dropped and counted, so its entries and its
//! membership never reach the cluster.

use std::io;
use std::time::Duration;

use groupnet_core::NodeId;
use groupnet_network::{ClusterSecret, RouterConfig};
use groupnet_runtime::{Group, Node};
use groupnet_testkit::cluster::eventually;
use groupnet_transport_mem::{MemLink, Network};

const GOSSIP: Duration = Duration::from_millis(20);

fn secret(byte: u8) -> ClusterSecret {
    ClusterSecret::new([byte; ClusterSecret::MIN_LEN]).expect("valid secret")
}

/// Starts `id` on `net`, admitting and seeding every other id, and joins it
/// to group `g` with an entry naming itself.
async fn start(
    net: &Network,
    id: &str,
    everyone: &[&str],
    cluster_secret: Option<ClusterSecret>,
) -> io::Result<(Node, Group)> {
    let me = NodeId::new(id);
    let peers: Vec<_> = everyone
        .iter()
        .filter(|peer| **peer != id)
        .map(|peer| NodeId::new(*peer))
        .collect();
    let mut builder = Node::builder(me.clone())
        .link(MemLink::new(net.endpoint(me), peers.clone()))
        .routing(RouterConfig {
            announce_interval: GOSSIP,
            cluster_secret,
            ..RouterConfig::default()
        })
        .gossip_interval(GOSSIP);
    for peer in peers {
        builder = builder.seed(peer);
    }
    let node = builder.start().await?;
    let group = node.join_group("g");
    group
        .set_entry("rpc", id.as_bytes(), None)
        .expect("entry accepted");
    Ok((node, group))
}

#[tokio::test]
async fn nodes_sharing_a_secret_gossip_entries() -> io::Result<()> {
    let net = Network::new();
    let ids = ["a", "b"];
    let (node_a, a) = start(&net, "a", &ids, Some(secret(1))).await?;
    let (node_b, b) = start(&net, "b", &ids, Some(secret(1))).await?;
    eventually("entries cross the sealed link", || {
        a.node_entry(&NodeId::new("b"), "rpc").as_deref() == Some(&b"b"[..])
            && b.node_entry(&NodeId::new("a"), "rpc").as_deref() == Some(&b"a"[..])
    })
    .await;
    assert_eq!(node_a.router().auth_failures(), 0);
    assert_eq!(node_b.router().auth_failures(), 0);
    Ok(())
}

/// `outsider` gossips its entry without a secret or under another one; the
/// two holders of the cluster secret exchange theirs and drop every frame of
/// the outsider.
async fn outsider_is_dropped(outsider_secret: Option<ClusterSecret>) -> io::Result<()> {
    let net = Network::new();
    let ids = ["a", "c", "outsider"];
    let (node_a, a) = start(&net, "a", &ids, Some(secret(1))).await?;
    let (node_c, c) = start(&net, "c", &ids, Some(secret(1))).await?;
    let (node_outsider, outsider) = start(&net, "outsider", &ids, outsider_secret).await?;
    let outsider_id = NodeId::new("outsider");
    eventually(
        "members gossip while the outsider's frames are dropped",
        || {
            a.node_entry(&NodeId::new("c"), "rpc").is_some()
                && c.node_entry(&NodeId::new("a"), "rpc").is_some()
                && node_a.router().auth_failures() > 0
                && node_c.router().auth_failures() > 0
        },
    )
    .await;
    // No frame of the outsider was ever accepted, so nothing it gossiped,
    // its entry and its membership included, exists on either member.
    for group in [&a, &c] {
        assert_eq!(group.node_entry(&outsider_id, "rpc"), None);
        assert!(!group.members().contains(&outsider_id));
    }
    assert_eq!(outsider.node_entry(&NodeId::new("a"), "rpc"), None);
    assert!(node_a.router().route_to(&outsider_id).is_none());
    drop(node_outsider);
    Ok(())
}

#[tokio::test]
async fn unsealed_gossip_from_a_node_without_the_secret_is_dropped() -> io::Result<()> {
    outsider_is_dropped(None).await
}

#[tokio::test]
async fn gossip_sealed_under_another_secret_is_dropped() -> io::Result<()> {
    outsider_is_dropped(Some(secret(2))).await
}
