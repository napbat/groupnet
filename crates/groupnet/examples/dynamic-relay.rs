//! Three previously unknown nodes discover one another through a keyless relay.
//!
//! Run with `cargo run -p groupnet --example dynamic-relay --features udp,connectivity`.
//! Add `-- --direct` to prefer verified direct UDP paths with relay fallback.
//! Open admission deliberately provides no cryptographic endpoint identity or
//! confidentiality. The rendezvous is not itself a coordination-group member.

use std::io;
use std::time::Duration;

use groupnet::connectivity::{PathPolicy, PeerPath, PunchConfig, Rendezvous};
use groupnet::core::NodeId;
use groupnet::runtime::Node;
use groupnet::transport::udp::UdpTransport;

#[tokio::main]
async fn main() -> io::Result<()> {
    let direct = std::env::args().any(|argument| argument == "--direct");
    let policy = if direct {
        PathPolicy::DirectPreferred
    } else {
        PathPolicy::RelayOnly
    };
    let expected_path = if direct {
        PeerPath::Direct
    } else {
        PeerPath::Relay
    };
    let relay =
        Rendezvous::bind_open("127.0.0.1:0".parse().expect("literal loopback address")).await?;
    let address = relay.local_addr()?;
    let ids = [
        NodeId::new("peer-a"),
        NodeId::new("peer-b"),
        NodeId::new("peer-c"),
    ];
    let mut nodes = Vec::with_capacity(ids.len());
    let mut transports = Vec::with_capacity(ids.len());
    for id in &ids {
        let mut config = PunchConfig::open(id.clone(), address);
        config.policy = policy;
        let transport = UdpTransport::bind_connectivity(config).await?;
        nodes.push(
            Node::builder(id.clone())
                .link(transport.clone().into_bound_link(1))
                .gossip_interval_ms(50)
                .start()
                .await?,
        );
        transports.push(transport);
    }
    let groups: Vec<_> = nodes
        .iter()
        .map(|node| node.join_group("example-network"))
        .collect();
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if groups.iter().all(|group| {
                let members = group.members();
                ids.iter().all(|id| members.contains(id))
            }) && transports.iter().enumerate().all(|(index, transport)| {
                ids.iter().enumerate().all(|(peer_index, peer)| {
                    index == peer_index || transport.path_to(peer) == Some(expected_path)
                })
            }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            "membership or selected paths did not converge",
        )
    })?;
    for (id, group) in ids.iter().zip(&groups) {
        println!("{} sees {:?}", id.as_str(), group.members());
    }
    println!(
        "PASS: {expected_path:?} paths and membership without initial peer lists or configured transport keys"
    );
    nodes[2].close().await;
    tokio::time::timeout(Duration::from_secs(15), async {
        while nodes[..2]
            .iter()
            .any(|node| node.router().route_to(&ids[2]).is_some())
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "departed peer route was retained"))?;
    println!("PASS: routes to departed peer withdrawn");
    for node in &nodes[..2] {
        node.close().await;
    }
    relay.close().await;
    Ok(())
}
