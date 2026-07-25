//! The strong-coherence tier (feature `acks`), over real nodes on the
//! in-memory transport.

#![cfg(feature = "acks")]

use std::num::NonZeroUsize;
use std::time::Duration;

use groupnet_consistency::{
    AckLedger, PeerWrite, PeerWrites, WriteFeed, WriteToken, applied_by, applied_cluster_wide,
};
use groupnet_core::NodeId;
use groupnet_runtime::{Group, Node};
use groupnet_transport_mem::{MemTransport, Network};

const fn cap(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).expect("nonzero")
}

fn spawn_node(net: &Network, id: &str, peer: &str) -> (NodeId, Node<MemTransport>, Group) {
    let me = NodeId::new(id);
    let node = Node::builder(me.clone(), net.endpoint(me.clone()))
        .seed(NodeId::new(peer))
        .gossip_interval_ms(10)
        .anti_entropy_interval_ms(25)
        .spawn();
    let group = node.join_group("stores");
    (me, node, group)
}

async fn converged(groups: &[&Group]) {
    for _ in 0..300 {
        if groups.iter().all(|g| g.members().len() == groups.len()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("membership did not converge");
}

fn decode(bytes: &[u8]) -> Option<String> {
    String::from_utf8(bytes.to_vec()).ok()
}

/// The write-side coherence half: a subscriber's ledger advertises what it
/// applied, the writer's cluster-wide wait resolves only then — and it
/// resolves within the timeout while the apply loop runs.
#[tokio::test]
async fn applied_acknowledgements_round_trip() {
    let net = Network::new();
    let (a_id, _a_node, a_group) = spawn_node(&net, "ack-a", "ack-b");
    let (b_id, _b_node, b_group) = spawn_node(&net, "ack-b", "ack-a");
    converged(&[&a_group, &b_group]).await;

    // B: apply loop that records into a ledger after each application.
    let mut peers = PeerWrites::new(b_group.clone(), b_id.clone(), decode);
    let ledger = AckLedger::new(b_group);
    tokio::spawn(async move {
        while let Some(event) = peers.next().await {
            match event {
                PeerWrite::Wrote { peer, token, .. } => ledger.record(&peer, token).await,
                PeerWrite::Gap {
                    peer,
                    missed_through,
                } => ledger.record(&peer, missed_through).await,
            }
        }
    });

    let feed = WriteFeed::new(a_group.clone(), cap(8), |key: &String| {
        key.clone().into_bytes()
    })
    .with_epoch(4);
    let token = feed.publish(&"w1".to_owned()).await;

    assert!(
        applied_cluster_wide(&a_group, &a_id, token, Duration::from_secs(5)).await,
        "the wait resolves once every alive member acknowledged"
    );
    assert_eq!(
        applied_by(&a_group, &b_id, &a_id),
        Some(token),
        "B's ledger advertises exactly the applied token"
    );
    // A token nobody has applied yet must time out, not lie.
    let future = WriteToken { epoch: 4, seq: 99 };
    assert!(
        !applied_cluster_wide(&a_group, &a_id, future, Duration::from_millis(200)).await,
        "an unapplied token must not be acknowledged"
    );
}
