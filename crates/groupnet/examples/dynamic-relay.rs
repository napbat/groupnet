//! Three previously unknown nodes discover one another through a keyless relay.
//!
//! Run with `cargo run -p groupnet --example dynamic-relay --features punch`.
//! Open admission deliberately provides no cryptographic endpoint identity or
//! confidentiality. The rendezvous is not itself a coordination-group member.

use std::io;
use std::time::Duration;

use groupnet::core::NodeId;
use groupnet::runtime::Node;
use groupnet::transport::punch::{PunchConfig, PunchLink, Rendezvous};

#[tokio::main]
async fn main() -> io::Result<()> {
    let relay =
        Rendezvous::bind_open("127.0.0.1:0".parse().expect("literal loopback address")).await?;
    let address = relay.local_addr()?;
    let ids = [
        NodeId::new("player-a"),
        NodeId::new("player-b"),
        NodeId::new("player-c"),
    ];
    let mut nodes = Vec::with_capacity(ids.len());
    for id in &ids {
        nodes.push(
            Node::builder(id.clone())
                .link(PunchLink::new(PunchConfig::open(id.clone(), address)))
                .gossip_interval_ms(50)
                .start()
                .await?,
        );
    }
    let groups: Vec<_> = nodes
        .iter()
        .map(|node| node.join_group("game-room"))
        .collect();
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if groups.iter().all(|group| {
                let members = group.members();
                ids.iter().all(|id| members.contains(id))
            }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "relay membership did not converge"))?;
    for (id, group) in ids.iter().zip(&groups) {
        println!("{} sees {:?}", id.as_str(), group.members());
    }
    println!("PASS: dynamic relay membership without initial peer lists or shared keys");
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
