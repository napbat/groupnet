//! Loopback fixtures using the same pinned TLS setup as connectivity-bridge.

use std::{fmt, io, net::SocketAddr, time::Duration};

use groupnet::connectivity::{
    PathPolicy, PeerPath, PunchConfig, Rendezvous, TcpPunchConfig, TcpRendezvous,
};
use groupnet::core::NodeId;
use groupnet::network::{
    TunnelConfig,
    tunnel::{PeerIdentity, TlsIdentity},
};
use groupnet::runtime::Node;
use groupnet::transport::mem::{MemLink, Network};
use groupnet::transport::tcp::TcpMsgTransport;
use groupnet::transport::udp::UdpTransport;
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};

pub(super) const SETUP_TIMEOUT: Duration = Duration::from_secs(20);
const LEFT: &str = "packet-a";
const RIGHT: &str = "packet-b";

#[derive(Clone, Copy, Debug)]
pub(super) enum Path {
    Memory,
    Udp,
    Tcp,
    Relay,
}

impl Path {
    pub(super) const ALL: [Self; 4] = [Self::Memory, Self::Udp, Self::Tcp, Self::Relay];
}

impl fmt::Display for Path {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Memory => "memory",
            Self::Udp => "udp-direct",
            Self::Tcp => "tcp-direct",
            Self::Relay => "tcp-relay",
        })
    }
}

enum RelayService {
    None,
    Udp(Rendezvous),
    Tcp(TcpRendezvous),
}

enum Monitor {
    Memory,
    Udp(UdpTransport, UdpTransport),
    Tcp(TcpMsgTransport, TcpMsgTransport, PeerPath),
}

pub(super) struct Pair {
    pub(super) left: Node,
    pub(super) right: Node,
    relay: RelayService,
    monitor: Monitor,
}

impl Pair {
    pub(super) async fn new(path: Path) -> io::Result<Self> {
        let [left_tls, right_tls] = credentials()?;
        let left = Node::builder(NodeId::new(LEFT))
            .gossip_interval_ms(50)
            .tunnels(left_tls);
        let right = Node::builder(NodeId::new(RIGHT))
            .gossip_interval_ms(50)
            .tunnels(right_tls);
        let bind: SocketAddr = "127.0.0.1:0".parse().map_err(io::Error::other)?;
        let (left, right, relay, monitor) = match path {
            Path::Memory => {
                let memory = Network::new();
                (
                    left.link(MemLink::new(
                        memory.endpoint(NodeId::new(LEFT)),
                        vec![NodeId::new(RIGHT)],
                    )),
                    right.link(MemLink::new(
                        memory.endpoint(NodeId::new(RIGHT)),
                        vec![NodeId::new(LEFT)],
                    )),
                    RelayService::None,
                    Monitor::Memory,
                )
            }
            Path::Udp => {
                let relay = Rendezvous::bind_open(bind).await?;
                let mut config = PunchConfig::open(NodeId::new(LEFT), relay.local_addr()?);
                config.candidate_binds = vec![bind];
                config.policy = PathPolicy::DirectPreferred;
                let left_udp = UdpTransport::bind_connectivity(config.clone()).await?;
                config.local = NodeId::new(RIGHT);
                let right_udp = UdpTransport::bind_connectivity(config).await?;
                settle(|| {
                    left_udp.path_to(&NodeId::new(RIGHT)) == Some(PeerPath::Direct)
                        && right_udp.path_to(&NodeId::new(LEFT)) == Some(PeerPath::Direct)
                })
                .await?;
                (
                    left.link(left_udp.clone().into_bound_link(1)),
                    right.link(right_udp.clone().into_bound_link(1)),
                    RelayService::Udp(relay),
                    Monitor::Udp(left_udp, right_udp),
                )
            }
            Path::Tcp | Path::Relay => {
                let relay = TcpRendezvous::bind_open(bind).await?;
                let (policy, expected) = match path {
                    Path::Relay => (PathPolicy::RelayOnly, PeerPath::Relay),
                    _ => (PathPolicy::DirectPreferred, PeerPath::Direct),
                };
                let mut config = TcpPunchConfig::open(NodeId::new(LEFT), relay.local_addr()?);
                config.candidate_binds = vec![bind];
                config.policy = policy;
                let left_tcp = TcpMsgTransport::bind_connectivity(config.clone()).await?;
                config.local = NodeId::new(RIGHT);
                let right_tcp = TcpMsgTransport::bind_connectivity(config).await?;
                settle(|| {
                    left_tcp.path_to(&NodeId::new(RIGHT)) == Some(expected)
                        && right_tcp.path_to(&NodeId::new(LEFT)) == Some(expected)
                })
                .await?;
                (
                    left.link(left_tcp.clone().into_bound_link(1)),
                    right.link(right_tcp.clone().into_bound_link(1)),
                    RelayService::Tcp(relay),
                    Monitor::Tcp(left_tcp, right_tcp, expected),
                )
            }
        };
        let (left, right) = tokio::try_join!(left.start(), right.start())?;
        let pair = Self {
            left,
            right,
            relay,
            monitor,
        };
        settle(|| {
            pair.left.router().route_to(pair.right.id()).is_some()
                && pair.right.router().route_to(pair.left.id()).is_some()
        })
        .await?;
        pair.check_path()?;
        Ok(pair)
    }

    pub(super) fn check_path(&self) -> io::Result<()> {
        let valid = match &self.monitor {
            Monitor::Memory => true,
            Monitor::Udp(left, right) => {
                left.path_to(self.right.id()) == Some(PeerPath::Direct)
                    && right.path_to(self.left.id()) == Some(PeerPath::Direct)
            }
            Monitor::Tcp(left, right, expected) => {
                left.path_to(self.right.id()) == Some(*expected)
                    && right.path_to(self.left.id()) == Some(*expected)
            }
        };
        if valid {
            Ok(())
        } else {
            Err(io::Error::other(
                "physical path changed; refusing a mislabeled measurement",
            ))
        }
    }

    pub(super) async fn close(self) {
        self.right.close().await;
        self.left.close().await;
        match self.relay {
            RelayService::None => {}
            RelayService::Udp(relay) => relay.close().await,
            RelayService::Tcp(relay) => relay.close().await,
        }
    }
}

async fn settle(mut ready: impl FnMut() -> bool) -> io::Result<()> {
    tokio::time::timeout(SETUP_TIMEOUT, async {
        while !ready() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "packet path did not converge"))
}

fn credentials() -> io::Result<[TunnelConfig; 2]> {
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
    for name in [LEFT, RIGHT] {
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
        configs.next().expect("two identities")
    }))
}
