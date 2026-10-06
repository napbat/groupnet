//! Memory-only A reaches TCP-only C through edge B, without application forwarding.
//!
//! Run `cargo run --example connectivity-bridge --features tcp-msg,connectivity`.
//! Add `-- --relay-only` to keep B-C traffic on the TCP rendezvous relay.
//! Add `-- --upgrade` to give A TCP connectivity and observe route promotion/fallback.
//! Local OS-socket evidence is not cross-machine or Internet NAT qualification.
//! Application streams use disposable, pinned TLS identities; group messages
//! remain on the managed node's separate coordination inbox.

use std::{io, net::SocketAddr, time::Duration};

use futures_util::io::{AsyncReadExt, AsyncWriteExt};
use groupnet::connectivity::{PathPolicy, PeerPath, TcpPunchConfig, TcpRendezvous};
use groupnet::core::NodeId;
use groupnet::network::TunnelConfig;
use groupnet::network::tunnel::{PeerIdentity, TlsIdentity};
use groupnet::runtime::Node;
use groupnet::transport::bulk::BulkTransport;
use groupnet::transport::mem::{MemLink, Network};
use groupnet::transport::tcp::TcpMsgTransport;
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};

const WAIT: Duration = Duration::from_secs(20);

#[tokio::main]
async fn main() -> io::Result<()> {
    let arguments: Vec<_> = std::env::args().skip(1).collect();
    if arguments
        .iter()
        .any(|arg| !matches!(arg.as_str(), "--relay-only" | "--upgrade"))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "expected --relay-only or --upgrade",
        ));
    }
    let policy = if arguments.iter().any(|arg| arg == "--relay-only") {
        PathPolicy::RelayOnly
    } else {
        PathPolicy::DirectPreferred
    };
    let relay = TcpRendezvous::bind_open("127.0.0.1:0".parse().map_err(io::Error::other)?).await?;
    let address = relay.local_addr()?;
    let b_tcp = TcpMsgTransport::bind_connectivity(config("edge-b", address, policy)).await?;
    let c_tcp = TcpMsgTransport::bind_connectivity(config("remote-c", address, policy)).await?;
    let expected = if policy == PathPolicy::DirectPreferred {
        PeerPath::Direct
    } else {
        PeerPath::Relay
    };
    settle(|| {
        b_tcp.path_to(&NodeId::new("remote-c")) == Some(expected)
            && c_tcp.path_to(&NodeId::new("edge-b")) == Some(expected)
    })
    .await?;
    println!("B-C TCP path: {expected:?}");

    let memory = Network::new();
    let [a_tls, b_tls, c_tls] = credentials()?;
    let a = Node::builder(NodeId::new("memory-a"))
        .link(MemLink::new(
            memory.endpoint(NodeId::new("memory-a")),
            vec![NodeId::new("edge-b")],
        ))
        .gossip_interval_ms(50)
        .tunnels(a_tls)
        .start()
        .await?;
    let b = Node::builder(NodeId::new("edge-b"))
        .link(MemLink::new(
            memory.endpoint(NodeId::new("edge-b")),
            vec![NodeId::new("memory-a")],
        ))
        .link(b_tcp.into_bound_link(1))
        .gossip_interval_ms(50)
        .tunnels(b_tls)
        .start()
        .await?;
    let c = Node::builder(NodeId::new("remote-c"))
        .link(c_tcp.into_bound_link(1))
        .gossip_interval_ms(50)
        .tunnels(c_tls)
        .start()
        .await?;
    let upgrade = arguments
        .iter()
        .any(|arg| arg == "--upgrade")
        .then_some((address, policy));
    let result = demonstrate(&a, &b, &c, upgrade).await;
    for node in [&c, &b, &a] {
        node.close().await;
    }
    relay.close().await;
    result
}

fn config(local: &str, address: SocketAddr, policy: PathPolicy) -> TcpPunchConfig {
    let mut config = TcpPunchConfig::open(NodeId::new(local), address);
    config.policy = policy;
    config
        .candidate_binds
        .push("127.0.0.1:0".parse().expect("literal address"));
    config
}

async fn settle(mut ready: impl FnMut() -> bool) -> io::Result<()> {
    tokio::time::timeout(WAIT, async {
        while !ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "bridge did not converge"))
}

async fn demonstrate(
    a: &Node,
    b: &Node,
    c: &Node,
    upgrade: Option<(SocketAddr, PathPolicy)>,
) -> io::Result<()> {
    let ids = [
        NodeId::new("memory-a"),
        NodeId::new("edge-b"),
        NodeId::new("remote-c"),
    ];
    let groups = [
        a.join_group("application-group"),
        b.join_group("application-group"),
        c.join_group("application-group"),
    ];
    settle(|| {
        groups
            .iter()
            .all(|group| ids.iter().all(|id| group.members().contains(id)))
    })
    .await?;
    settle(|| {
        a.router()
            .route_to(&ids[2])
            .is_some_and(|route| route.path == ids)
            && c.router()
                .route_to(&ids[0])
                .is_some_and(|route| route.path == [ids[2].clone(), ids[1].clone(), ids[0].clone()])
    })
    .await?;
    println!("PASS: application group converged; route memory-a -> edge-b -> remote-c");
    exchange(a, c, b"request from memory-only application").await?;
    println!("PASS: bidirectional A-C bytes preserve origin; no application forwarding on B");
    if let Some((address, policy)) = upgrade {
        promote(a, c, address, policy).await?;
    }
    c.close().await;
    settle(|| a.router().route_to(&ids[2]).is_none() && b.router().route_to(&ids[2]).is_none())
        .await?;
    exchange(a, b, b"memory link remains usable").await?;
    println!("PASS: C departure withdraws A-C route without breaking A-B memory traffic");
    Ok(())
}

async fn exchange(a: &Node, c: &Node, payload: &[u8]) -> io::Result<()> {
    tokio::time::timeout(WAIT, async {
        let (outgoing, incoming) = tokio::join!(a.connect(c.router().local_id()), c.accept());
        let mut outgoing = outgoing?;
        let (peer, mut incoming) = incoming?;
        if &peer != a.router().local_id() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected TLS peer",
            ));
        }
        let upload = async {
            outgoing.write_all(payload).await?;
            outgoing.close().await?;
            let mut echoed = Vec::new();
            outgoing.read_to_end(&mut echoed).await?;
            if echoed != payload {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "bridge changed payload",
                ));
            }
            Ok(())
        };
        let echo = async {
            let mut received = Vec::new();
            incoming.read_to_end(&mut received).await?;
            if received != payload {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "bridge changed payload",
                ));
            }
            incoming.write_all(&received).await?;
            incoming.close().await
        };
        tokio::try_join!(upload, echo)?;
        Ok(())
    })
    .await
    .map_err(io::Error::other)?
}

async fn promote(a: &Node, c: &Node, address: SocketAddr, policy: PathPolicy) -> io::Result<()> {
    let tcp = TcpMsgTransport::bind_connectivity(config("memory-a", address, policy)).await?;
    let expected = if policy == PathPolicy::DirectPreferred {
        PeerPath::Direct
    } else {
        PeerPath::Relay
    };
    settle(|| tcp.path_to(c.router().local_id()) == Some(expected)).await?;
    a.router().add_link(tcp.clone().into_bound_link(1)).await?;
    let direct = [NodeId::new("memory-a"), NodeId::new("remote-c")];
    settle(|| {
        a.router()
            .route_to(c.router().local_id())
            .is_some_and(|route| route.path == direct)
            && c.router()
                .route_to(a.router().local_id())
                .is_some_and(|route| route.path == [direct[1].clone(), direct[0].clone()])
    })
    .await?;
    exchange(a, c, b"one-hop route after compatible TCP connectivity").await?;
    println!("PASS: A gains TCP; router promotes A-C to one hop ({expected:?} physical path)");
    tcp.close().await;
    let transit = [
        NodeId::new("memory-a"),
        NodeId::new("edge-b"),
        NodeId::new("remote-c"),
    ];
    settle(|| {
        a.router()
            .route_to(c.router().local_id())
            .is_some_and(|route| route.path == transit)
            && c.router()
                .route_to(a.router().local_id())
                .is_some_and(|route| {
                    route.path == [transit[2].clone(), transit[1].clone(), transit[0].clone()]
                })
    })
    .await?;
    exchange(a, c, b"transit fallback after A TCP loss").await?;
    println!("PASS: A TCP loss restores route through B without application forwarding");
    Ok(())
}

fn credentials() -> io::Result<[TunnelConfig; 3]> {
    let mut params = CertificateParams::new(Vec::<String>::new()).map_err(io::Error::other)?;
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let ca = CertifiedIssuer::self_signed(params, KeyPair::generate().map_err(io::Error::other)?)
        .map_err(io::Error::other)?;
    let mut identities = Vec::new();
    let mut pins = Vec::new();
    for name in ["memory-a", "edge-b", "remote-c"] {
        let mut params =
            CertificateParams::new(vec!["groupnet.peer".to_owned()]).map_err(io::Error::other)?;
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        let key = KeyPair::generate().map_err(io::Error::other)?;
        let leaf = params
            .signed_by(&key, &ca)
            .map_err(io::Error::other)?
            .der()
            .to_vec();
        pins.push(PeerIdentity::new(NodeId::new(name), &leaf)?);
        identities.push(TlsIdentity::from_der(
            vec![ca.der().to_vec()],
            vec![leaf],
            key.serialize_der(),
        )?);
    }
    let mut configs = identities
        .into_iter()
        .map(|identity| TunnelConfig::new(identity, pins.clone()));
    Ok(std::array::from_fn(|_| {
        configs.next().expect("three identities")
    }))
}
