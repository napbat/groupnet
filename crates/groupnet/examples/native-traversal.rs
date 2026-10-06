//! Native multi-candidate traversal over UDP or TCP, without ICE/STUN/TURN.
//!
//! Run with `cargo run --example native-traversal --features udp,tcp-msg,connectivity`.
//! Add `-- --tcp`, `-- --ipv6`, or `-- --relay-only` (flags may be combined).
//! Loopback evidence does not establish compatibility with real Internet NATs.

use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use groupnet::connectivity::{
    PathPolicy, PeerPath, PunchConfig, Rendezvous, TcpPunchConfig, TcpRendezvous,
};
use groupnet::core::NodeId;
use groupnet::runtime::Node;
use groupnet::transport::Transport;
use groupnet::transport::tcp::TcpMsgTransport;
use groupnet::transport::udp::UdpTransport;

const DEADLINE: Duration = Duration::from_secs(20);

#[tokio::main]
async fn main() -> io::Result<()> {
    let arguments: Vec<_> = std::env::args().skip(1).collect();
    if arguments
        .iter()
        .any(|arg| !matches!(arg.as_str(), "--tcp" | "--ipv6" | "--relay-only"))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "expected --tcp, --ipv6, or --relay-only",
        ));
    }
    let bind = SocketAddr::new(
        if arguments.iter().any(|arg| arg == "--ipv6") {
            IpAddr::V6(Ipv6Addr::LOCALHOST)
        } else {
            IpAddr::V4(Ipv4Addr::LOCALHOST)
        },
        0,
    );
    let policy = if arguments.iter().any(|arg| arg == "--relay-only") {
        PathPolicy::RelayOnly
    } else {
        PathPolicy::DirectPreferred
    };
    if arguments.iter().any(|arg| arg == "--tcp") {
        tcp(bind, policy).await
    } else {
        udp(bind, policy).await
    }
}

async fn settle(mut ready: impl FnMut() -> bool) -> io::Result<()> {
    tokio::time::timeout(DEADLINE, async {
        while !ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "native traversal did not converge"))
}

fn expected(policy: PathPolicy) -> PeerPath {
    match policy {
        PathPolicy::DirectPreferred => PeerPath::Direct,
        PathPolicy::RelayOnly => PeerPath::Relay,
    }
}

async fn exchange<T: Transport<Error = io::Error>>(left: &T, right: &T) -> io::Result<()> {
    for (sender, receiver, from, to, payload) in [
        (
            left,
            right,
            "native-a",
            "native-b",
            b"native forward payload".as_slice(),
        ),
        (
            right,
            left,
            "native-b",
            "native-a",
            b"native reverse payload".as_slice(),
        ),
    ] {
        sender.send(&NodeId::new(to), payload).await?;
        let packet = tokio::time::timeout(DEADLINE, receiver.recv())
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "native message timed out"))??;
        if packet.from != NodeId::new(from) || packet.msg != payload {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "native message identity or bytes changed",
            ));
        }
    }
    println!("PASS: bidirectional native payload and sender identity");
    Ok(())
}

async fn membership(left: Node, right: Node) -> io::Result<()> {
    let groups = [
        left.join_group("native-example"),
        right.join_group("native-example"),
    ];
    settle(|| {
        groups.iter().all(|group| {
            let members = group.members();
            members.contains(&NodeId::new("native-a")) && members.contains(&NodeId::new("native-b"))
        })
    })
    .await?;
    println!("PASS: native LinkProvider membership convergence");
    right.close().await;
    settle(|| left.router().route_to(&NodeId::new("native-b")).is_none()).await?;
    left.close().await;
    println!("PASS: departed native peer route withdrawn");
    Ok(())
}

async fn udp(bind: SocketAddr, policy: PathPolicy) -> io::Result<()> {
    let relay = Rendezvous::bind_open(bind).await?;
    let address = relay.local_addr()?;
    let mut left = PunchConfig::open(NodeId::new("native-a"), address);
    left.candidate_binds = vec![bind];
    left.policy = policy;
    let mut right = left.clone();
    right.local = NodeId::new("native-b");
    let left = UdpTransport::bind_connectivity(left).await?;
    let right = UdpTransport::bind_connectivity(right).await?;
    settle(|| {
        left.path_to(&NodeId::new("native-b")) == Some(expected(policy))
            && right.path_to(&NodeId::new("native-a")) == Some(expected(policy))
    })
    .await?;
    println!(
        "UDP candidates: {:?}; selected {:?} at {:?}",
        left.local_candidates()?,
        left.path_to(&NodeId::new("native-b")),
        left.direct_addr_to(&NodeId::new("native-b"))
    );
    exchange(&left, &right).await?;
    let first = Node::builder(NodeId::new("native-a"))
        .link(left.into_bound_link(1))
        .gossip_interval_ms(50)
        .start()
        .await?;
    let second = Node::builder(NodeId::new("native-b"))
        .link(right.into_bound_link(1))
        .gossip_interval_ms(50)
        .start()
        .await?;
    let result = membership(first, second).await;
    relay.close().await;
    result
}

async fn tcp(bind: SocketAddr, policy: PathPolicy) -> io::Result<()> {
    let relay = TcpRendezvous::bind_open(bind).await?;
    let address = relay.local_addr()?;
    let mut left = TcpPunchConfig::open(NodeId::new("native-a"), address);
    left.candidate_binds = vec![bind];
    left.policy = policy;
    let mut right = left.clone();
    right.local = NodeId::new("native-b");
    let left = TcpMsgTransport::bind_connectivity(left).await?;
    let right = TcpMsgTransport::bind_connectivity(right).await?;
    settle(|| {
        left.path_to(&NodeId::new("native-b")) == Some(expected(policy))
            && right.path_to(&NodeId::new("native-a")) == Some(expected(policy))
    })
    .await?;
    println!(
        "TCP candidates: {:?}; selected {:?} at {:?}",
        left.local_candidates()?,
        left.path_to(&NodeId::new("native-b")),
        left.direct_addr_to(&NodeId::new("native-b"))
    );
    exchange(&left, &right).await?;
    let first = Node::builder(NodeId::new("native-a"))
        .link(left.into_bound_link(1))
        .gossip_interval_ms(50)
        .start()
        .await?;
    let second = Node::builder(NodeId::new("native-b"))
        .link(right.into_bound_link(1))
        .gossip_interval_ms(50)
        .start()
        .await?;
    let result = membership(first, second).await;
    relay.close().await;
    result
}
