//! Loopback stream pairs: plain TCP, TLS 1.3 over TCP and a pinned Groupnet
//! tunnel over admitted TCP links, all authenticated by the same generated
//! credentials and opened on ephemeral loopback ports.

use std::{fmt, io, net::SocketAddr, num::NonZeroUsize, sync::Arc, time::Duration};

use groupnet::core::NodeId;
use groupnet::network::{
    Network, NetworkConfig, RouterConfig, TunnelConfig,
    tunnel::{PeerIdentity, SegmentSize, TlsIdentity, TunnelLimits},
};
use groupnet::transport::admission::OpenAdmission;
use groupnet::transport::bulk::BulkTransport;
use groupnet::transport::tcp::{TcpAdmissionConfig, TcpMsgConfig, TcpMsgTransport};
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tokio_util::compat::FuturesAsyncReadCompatExt;

const SETUP_TIMEOUT: Duration = Duration::from_secs(20);
const CLIENT: &str = "throughput-client";
const SERVER: &str = "throughput-server";
const SERVER_NAME: &str = "groupnet.peer";

/// One measured byte stream, erased so every path runs the same workload code.
pub(super) trait Duplex: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T: AsyncRead + AsyncWrite + Unpin + Send> Duplex for T {}

pub(super) type Stream = Box<dyn Duplex>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Path {
    Tcp,
    Tls,
    Tunnel,
}

impl Path {
    pub(super) const ALL: [Self; 3] = [Self::Tcp, Self::Tls, Self::Tunnel];
}

impl fmt::Display for Path {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Tcp => "tcp",
            Self::Tls => "tls-tcp",
            Self::Tunnel => "groupnet-tunnel",
        })
    }
}

/// DER credentials shared by the TLS baseline and the tunnel.
struct Credentials {
    ca: Vec<u8>,
    /// Leaf certificate and PKCS#8 key for the client, then the server.
    leaves: [(Vec<u8>, Vec<u8>); 2],
}

impl Credentials {
    fn generate() -> io::Result<Self> {
        let mut params = CertificateParams::new(Vec::<String>::new()).map_err(io::Error::other)?;
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let ca =
            CertifiedIssuer::self_signed(params, KeyPair::generate().map_err(io::Error::other)?)
                .map_err(io::Error::other)?;
        let leaf = || -> io::Result<(Vec<u8>, Vec<u8>)> {
            let mut params =
                CertificateParams::new(vec![SERVER_NAME.to_owned()]).map_err(io::Error::other)?;
            params.extended_key_usages = vec![
                ExtendedKeyUsagePurpose::ServerAuth,
                ExtendedKeyUsagePurpose::ClientAuth,
            ];
            params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
            let key = KeyPair::generate().map_err(io::Error::other)?;
            let certificate = params.signed_by(&key, &ca).map_err(io::Error::other)?;
            Ok((certificate.der().to_vec(), key.serialize_der()))
        };
        let leaves = [leaf()?, leaf()?];
        Ok(Self {
            ca: ca.der().to_vec(),
            leaves,
        })
    }

    fn tunnel(&self, index: usize, limits: &TunnelLimits) -> io::Result<TunnelConfig> {
        let (certificate, key) = &self.leaves[index];
        let identity = TlsIdentity::from_der(
            vec![self.ca.clone()],
            vec![certificate.clone()],
            key.clone(),
        )?;
        let pins = [CLIENT, SERVER]
            .into_iter()
            .zip(&self.leaves)
            .map(|(name, (leaf, _))| PeerIdentity::new(NodeId::new(name), leaf))
            .collect::<io::Result<Vec<_>>>()?;
        Ok(TunnelConfig::new(identity, pins).with_limits(limits.clone()))
    }

    /// The same mutually authenticated TLS 1.3 policy the tunnel enforces,
    /// minus routing-alias pins, which are not on the byte path.
    fn tls(&self) -> io::Result<(TlsConnector, TlsAcceptor)> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(CertificateDer::from(self.ca.clone()))
            .map_err(io::Error::other)?;
        let chain = |index: usize| vec![CertificateDer::from(self.leaves[index].0.clone())];
        let key = |index: usize| {
            PrivateKeyDer::try_from(self.leaves[index].1.clone()).map_err(io::Error::other)
        };
        let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::new(roots.clone()),
            provider.clone(),
        )
        .build()
        .map_err(io::Error::other)?;
        let server = rustls::ServerConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(io::Error::other)?
            .with_client_cert_verifier(verifier)
            .with_single_cert(chain(1), key(1)?)
            .map_err(io::Error::other)?;
        let client = rustls::ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(io::Error::other)?
            .with_root_certificates(roots)
            .with_client_auth_cert(chain(0), key(0)?)
            .map_err(io::Error::other)?;
        Ok((
            TlsConnector::from(Arc::new(client)),
            TlsAcceptor::from(Arc::new(server)),
        ))
    }
}

/// Two Groupnet nodes joined by one admitted loopback TCP link.
struct Tunnel {
    client: Network,
    server: Network,
    links: [TcpMsgTransport; 2],
}

impl Tunnel {
    async fn start(credentials: &Credentials, segment: SegmentSize) -> io::Result<Self> {
        let limits = TunnelLimits {
            payload: segment,
            ..TunnelLimits::default()
        };
        let frames = limits.packet_queue();
        let bind: SocketAddr = "127.0.0.1:0".parse().map_err(io::Error::other)?;
        let ids = [NodeId::new(CLIENT), NodeId::new(SERVER)];
        let mut links = Vec::with_capacity(2);
        for id in &ids {
            links.push(
                TcpMsgTransport::bind_admitted(
                    id.clone(),
                    bind,
                    TcpMsgConfig {
                        max_outbound: NonZeroUsize::MIN,
                        outbound_queue: frames,
                        ..TcpMsgConfig::default()
                    },
                    Arc::new(OpenAdmission),
                    Vec::new(),
                    TcpAdmissionConfig {
                        max_peers: 1,
                        ..TcpAdmissionConfig::default()
                    },
                )
                .await?,
            );
        }
        let links: [TcpMsgTransport; 2] = links.try_into().expect("two links");
        links[0].register_peer(ids[1].clone(), links[1].local_addr());
        links[0].connect_peer(&ids[1])?;
        let mut networks = Vec::with_capacity(2);
        for (index, id) in ids.iter().enumerate() {
            networks.push(
                NetworkConfig::default()
                    .with_router(RouterConfig {
                        forwarding: false,
                        link_queue: frames,
                        ..RouterConfig::default()
                    })
                    .with_link(links[index].clone().into_bound_link(1))
                    .with_tunnels(credentials.tunnel(index, &limits)?)
                    .bind(id.clone())
                    .await?,
            );
        }
        let [client, server]: [Network; 2] = networks.try_into().expect("two networks");
        let tunnel = Self {
            client,
            server,
            links,
        };
        tokio::time::timeout(SETUP_TIMEOUT, async {
            while tunnel.client.router().route_to(&ids[1]).is_none()
                || tunnel.server.router().route_to(&ids[0]).is_none()
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "tunnel route did not converge"))?;
        Ok(tunnel)
    }

    async fn close(self) {
        self.client.close().await;
        self.server.close().await;
        for link in &self.links {
            link.close().await;
        }
    }
}

enum Backend {
    Tcp,
    Tls(TlsConnector, TlsAcceptor),
    Tunnel(Tunnel),
}

/// Opens fresh connected stream pairs on one path.
pub(super) struct Fixture {
    backend: Backend,
}

impl Fixture {
    pub(super) async fn start(path: Path, segment: SegmentSize) -> io::Result<Self> {
        let credentials = Credentials::generate()?;
        let backend = match path {
            Path::Tcp => Backend::Tcp,
            Path::Tls => {
                let (connector, acceptor) = credentials.tls()?;
                Backend::Tls(connector, acceptor)
            }
            Path::Tunnel => Backend::Tunnel(Tunnel::start(&credentials, segment).await?),
        };
        Ok(Self { backend })
    }

    /// Connects a client stream and accepts its server stream.
    pub(super) async fn pair(&self) -> io::Result<(Stream, Stream)> {
        tokio::time::timeout(SETUP_TIMEOUT, async {
            match &self.backend {
                Backend::Tcp => {
                    let (client, server) = tcp_pair().await?;
                    Ok((Box::new(client) as Stream, Box::new(server) as Stream))
                }
                Backend::Tls(connector, acceptor) => {
                    let (client, server) = tcp_pair().await?;
                    let name = ServerName::try_from(SERVER_NAME).map_err(io::Error::other)?;
                    let (client, server) =
                        tokio::try_join!(connector.connect(name, client), acceptor.accept(server))?;
                    Ok((Box::new(client) as Stream, Box::new(server) as Stream))
                }
                Backend::Tunnel(tunnel) => {
                    let server = NodeId::new(SERVER);
                    let (client, (_, server)) =
                        tokio::try_join!(tunnel.client.connect(&server), tunnel.server.accept())?;
                    Ok((
                        Box::new(client.compat()) as Stream,
                        Box::new(server.compat()) as Stream,
                    ))
                }
            }
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "stream setup timed out"))?
    }

    pub(super) async fn close(self) {
        if let Backend::Tunnel(tunnel) = self.backend {
            tunnel.close().await;
        }
    }
}

async fn tcp_pair() -> io::Result<(TcpStream, TcpStream)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let (client, (server, _)) = tokio::try_join!(
        TcpStream::connect(listener.local_addr()?),
        listener.accept()
    )?;
    // Groupnet's TCP links disable Nagle too; keep the baselines comparable.
    client.set_nodelay(true)?;
    server.set_nodelay(true)?;
    Ok((client, server))
}
