//! Native UDP rendezvous, simultaneous punching and relay fallback.
//!
//! Keyed configurations authenticate a trusted fabric, not Byzantine identities.
//! Keyless configurations (`key: None`) require neither a pre-shared transport
//! key nor an identity keypair; internal random session challenges still prove
//! return-routability. Application admission may independently require credentials.
//! Open admission provides no cryptographic identity assurance. The rendezvous
//! is not a routing participant.

mod endpoint;
mod server;
mod wire;

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use groupnet_core::NodeId;
use groupnet_transport::admission::{SessionLease, SessionRegistry};
use groupnet_transport::link::AdmittedInbound;
use groupnet_transport::link::{LinkFuture, LinkLifecycle};
use groupnet_transport::{Inbound, Transport};
use ring::{
    hmac,
    rand::{SecureRandom, SystemRandom},
};
use tokio::net::UdpSocket;
use tokio::sync::{Mutex as AsyncMutex, mpsc};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

pub use server::Rendezvous;

/// Largest application message carried by one session-bound UDP datagram.
pub const MAX_MESSAGE: usize = 960;
const MAX_PEERS: usize = 128;
const QUEUE: usize = 128;
const HEARTBEAT: Duration = Duration::from_secs(1);
const LEASE: Duration = Duration::from_secs(6);
const DIRECT_LEASE: Duration = Duration::from_secs(3);

/// Explicitly provisioned trusted-fabric HMAC key; Debug never reveals key bytes.
#[derive(Clone)]
pub struct NetworkKey {
    bytes: [u8; 32],
    auth: hmac::Key,
}

impl fmt::Debug for NetworkKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("NetworkKey([REDACTED])")
    }
}

impl NetworkKey {
    /// Generates a key using the operating system's cryptographic random source.
    ///
    /// # Errors
    /// Returns an error if secure random bytes cannot be obtained.
    pub fn generate() -> io::Result<Self> {
        Ok(Self::from_bytes(random()?))
    }

    /// Imports a key provisioned through a secure out-of-band channel.
    #[must_use]
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self {
            auth: hmac::Key::new(hmac::HMAC_SHA256, &bytes),
            bytes,
        }
    }

    /// Exports secret key material for explicit secure provisioning.
    #[must_use]
    pub const fn to_bytes(&self) -> [u8; 32] {
        self.bytes
    }
}

/// Which paths may be established for this endpoint.
/// Independent of transport key presence; keyless direct paths still prove return-routability.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PathPolicy {
    /// Probe peers simultaneously, preferring a live direct path to the relay.
    #[default]
    DirectPreferred,
    /// Never probe or accept direct peer packets; use only the rendezvous relay.
    /// Pair discovery omits both physical endpoint addresses if either peer is relay-only.
    RelayOnly,
}

/// The currently usable path to a configured or dynamically admitted peer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeerPath {
    /// A session-bound return-routability probe recently succeeded.
    Direct,
    /// A live rendezvous registration is available for relay delivery.
    Relay,
}

/// Socket, trusted identities and rendezvous policy for one endpoint.
#[derive(Clone)]
pub struct PunchConfig {
    /// Stable routing identity (1–64 UTF-8 bytes).
    pub local: NodeId,
    /// Local UDP bind address; use the correct address family for the rendezvous.
    pub bind: SocketAddr,
    /// Explicit rendezvous address; no third-party discovery service is used.
    pub rendezvous: SocketAddr,
    /// Optional explicit trusted-fabric key; keyed peers never downgrade.
    /// `None` requires no provisioned transport key or identity keypair, but does
    /// not disable internal random challenges or application admission credentials.
    pub key: Option<NetworkKey>,
    /// Initial static allowlist, or no fixed identities in dynamic mode.
    pub peers: Vec<NodeId>,
    /// Whether admitted rendezvous peers are discovered dynamically.
    pub dynamic: bool,
    /// Opaque application admission credential (at most 1024 bytes).
    pub credential: Vec<u8>,
    /// Direct-path preference or relay-only operation.
    pub policy: PathPolicy,
}

impl PunchConfig {
    /// Creates a direct-preferred configuration with an ephemeral wildcard bind.
    #[must_use]
    pub fn new(local: NodeId, rendezvous: SocketAddr, key: NetworkKey, peers: Vec<NodeId>) -> Self {
        let bind = if rendezvous.is_ipv4() {
            SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0))
        } else {
            SocketAddr::from((std::net::Ipv6Addr::UNSPECIFIED, 0))
        };
        Self {
            local,
            bind,
            rendezvous,
            key: Some(key),
            peers,
            dynamic: false,
            credential: Vec::new(),
            policy: PathPolicy::DirectPreferred,
        }
    }

    /// Creates explicit keyless dynamic discovery, conservatively defaulting to relay-only.
    #[must_use]
    pub fn open(local: NodeId, rendezvous: SocketAddr) -> Self {
        Self::dynamic(local, rendezvous, None, Vec::new())
    }

    /// Creates dynamic discovery with an optional fabric key and opaque credential.
    /// Keyless configurations default to relay-only; policy may explicitly enable direct paths.
    #[must_use]
    pub fn dynamic(
        local: NodeId,
        rendezvous: SocketAddr,
        key: Option<NetworkKey>,
        credential: Vec<u8>,
    ) -> Self {
        Self {
            local,
            bind: SocketAddr::new(
                if rendezvous.is_ipv4() {
                    Ipv4Addr::UNSPECIFIED.into()
                } else {
                    std::net::Ipv6Addr::UNSPECIFIED.into()
                },
                0,
            ),
            rendezvous,
            policy: if key.is_some() {
                PathPolicy::DirectPreferred
            } else {
                PathPolicy::RelayOnly
            },
            key,
            peers: Vec::new(),
            dynamic: true,
            credential,
        }
    }
}

impl fmt::Debug for PunchConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PunchConfig")
            .field("local", &self.local)
            .field("bind", &self.bind)
            .field("rendezvous", &self.rendezvous)
            .field("key", &self.key)
            .field("peers", &self.peers)
            .field("dynamic", &self.dynamic)
            .field("credential", &"[REDACTED]")
            .field("policy", &self.policy)
            .finish()
    }
}

#[derive(Clone, Copy)]
struct Capability {
    token: wire::Session,
    issued: Instant,
}

impl fmt::Debug for Capability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Capability")
            .field("token", &"[REDACTED]")
            .field("issued", &self.issued)
            .finish()
    }
}

impl Capability {
    fn live(self, now: Instant) -> bool {
        now.duration_since(self.issued) < DIRECT_LEASE
    }
}

#[derive(Clone, Copy)]
struct PendingCapability {
    capability: Capability,
    nonce: wire::Session,
    sequence: u64,
}

impl fmt::Debug for PendingCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingCapability")
            .field("capability", &self.capability)
            .field("nonce", &"[REDACTED]")
            .field("sequence", &self.sequence)
            .finish()
    }
}

#[derive(Debug)]
struct Peer {
    node: NodeId,
    session: wire::Session,
    address: Option<SocketAddr>,
    relay_only: bool,
    offered: Instant,
    direct: Option<Capability>,
    probe: Option<Capability>,
    pending: Option<PendingCapability>,
    confirmed: Option<Capability>,
    confirmed_probe: u64,
    sequence: u64,
    lease: SessionLease,
}

impl Peer {
    fn path(&self, now: Instant) -> Option<PeerPath> {
        if self.direct.is_some_and(|capability| capability.live(now)) {
            Some(PeerPath::Direct)
        } else if now.duration_since(self.offered) < LEASE {
            Some(PeerPath::Relay)
        } else {
            None
        }
    }
}

#[derive(Debug)]
struct Outbound {
    to: NodeId,
    session: groupnet_transport::admission::SessionId,
    target: wire::Session,
    message: Vec<u8>,
}

#[derive(Debug)]
struct Inner {
    address: SocketAddr,
    configured: Vec<NodeId>,
    peers: Arc<Mutex<HashMap<String, Peer>>>,
    outbound: mpsc::Sender<Outbound>,
    inbound: AsyncMutex<mpsc::Receiver<AdmittedInbound>>,
    sessions: SessionRegistry,
    dynamic: bool,
    cancel: CancellationToken,
    task: AsyncMutex<Option<JoinHandle<()>>>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.sessions.close();
    }
}

/// A bounded best-effort UDP transport with session-bound native hole punching.
///
/// Clones share queues, path state and one runtime. Closing any clone closes all
/// clones; dropping the last clone cancels the runtime and releases its socket.
#[derive(Clone, Debug)]
pub struct PunchTransport {
    inner: Arc<Inner>,
}

impl PunchTransport {
    /// Binds the socket and starts registration, discovery and keepalive tasks.
    /// Dynamic configurations wait for actual server admission, failing within
    /// five seconds; static configurations retain background registration.
    ///
    /// # Errors
    /// Rejects invalid configuration, dynamic policy denial or registration
    /// timeout, and socket/random-source failures.
    pub async fn bind(config: PunchConfig) -> io::Result<Self> {
        validate_names(&config.peers)?;
        validate_name(&config.local)?;
        if config.credential.len() > groupnet_transport::admission::MAX_CREDENTIAL_BYTES {
            return Err(invalid("invalid credential size"));
        }
        if config.peers.contains(&config.local) {
            return Err(invalid("local identity must not be a configured peer"));
        }
        if config.rendezvous.port() == 0
            || config.rendezvous.ip().is_unspecified()
            || config.rendezvous.ip().is_multicast()
            || config.bind.is_ipv4() != config.rendezvous.is_ipv4()
        {
            return Err(invalid(
                "invalid rendezvous address or socket address family",
            ));
        }
        let socket = UdpSocket::bind(config.bind).await?;
        let address = socket.local_addr()?;
        let session = random()?;
        let nonce = random()?;
        let peers = Arc::new(Mutex::new(HashMap::new()));
        let cancel = CancellationToken::new();
        let (outbound, outgoing) = mpsc::channel(QUEUE);
        let (incoming, inbound) = mpsc::channel(QUEUE);
        let configured = config.peers.clone();
        let sessions = SessionRegistry::new(MAX_PEERS)?;
        let dynamic = config.dynamic;
        let (ready, admitted) = tokio::sync::oneshot::channel();
        let mut task = tokio::spawn(endpoint::run(
            config,
            socket,
            (session, nonce),
            peers.clone(),
            (outgoing, incoming, ready),
            sessions.clone(),
            cancel.clone(),
        ));
        if dynamic {
            let result = tokio::time::timeout(Duration::from_secs(5), admitted).await;
            let error = match result {
                Ok(Ok(Ok(()))) => None,
                Ok(Ok(Err(error))) => Some(error),
                Ok(Err(_)) => Some(closed()),
                Err(_) => Some(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "rendezvous admission timed out",
                )),
            };
            if let Some(error) = error {
                cancel.cancel();
                let _ = (&mut task).await;
                return Err(error);
            }
        }
        Ok(Self {
            inner: Arc::new(Inner {
                address,
                configured,
                peers,
                outbound,
                inbound: AsyncMutex::new(inbound),
                cancel,
                sessions,
                dynamic,
                task: AsyncMutex::new(Some(task)),
            }),
        })
    }

    /// Returns the socket's actual bind address, including its assigned port.
    ///
    /// # Errors
    /// Returns `NotConnected` after shutdown.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.ensure_open()?;
        Ok(self.inner.address)
    }

    /// Returns configured peers, or the currently live discovered peers.
    #[must_use]
    pub fn known_peers(&self) -> Vec<NodeId> {
        if self.inner.dynamic {
            lock(&self.inner.peers)
                .values()
                .filter(|peer| peer.path(Instant::now()).is_some() && peer.lease.is_active())
                .map(|peer| peer.node.clone())
                .collect()
        } else {
            self.inner.configured.clone()
        }
    }

    /// Returns the current direct or relay path, or `None` before discovery/after expiry.
    #[must_use]
    pub fn path_to(&self, node: &NodeId) -> Option<PeerPath> {
        if self.inner.cancel.is_cancelled() {
            return None;
        }
        let peers = lock(&self.inner.peers);
        let peer = peers.get(node.as_str())?;
        if !peer.lease.is_active() {
            return None;
        }
        peer.path(Instant::now())
    }

    pub(crate) fn sessions(&self) -> SessionRegistry {
        self.inner.sessions.clone()
    }

    /// Cancels all I/O and waits for the socket-owning task to finish.
    pub async fn close(&self) {
        self.inner.cancel.cancel();
        self.inner.sessions.close();
        let mut task = self.inner.task.lock().await;
        if let Some(running) = task.as_mut() {
            let _ = running.await;
        }
        task.take();
    }

    fn ensure_open(&self) -> io::Result<()> {
        if self.inner.cancel.is_cancelled() {
            Err(closed())
        } else {
            Ok(())
        }
    }

    fn enqueue(
        &self,
        to: &NodeId,
        message: &[u8],
        expected: Option<groupnet_transport::admission::SessionId>,
    ) -> io::Result<()> {
        self.ensure_open()?;
        if message.len() > MAX_MESSAGE {
            return Err(invalid("UDP message exceeds MAX_MESSAGE"));
        }
        if (!self.inner.dynamic && !self.inner.configured.contains(to))
            || (self.inner.dynamic && self.path_to(to).is_none())
        {
            return Ok(());
        }
        let (session, target) = {
            let peers = lock(&self.inner.peers);
            let Some(peer) = peers.get(to.as_str()) else {
                return Ok(());
            };
            if peer.path(Instant::now()).is_none()
                || !peer.lease.is_active()
                || expected.is_some_and(|id| id != peer.lease.id())
            {
                return Ok(());
            }
            (peer.lease.id(), peer.session)
        };
        if let Ok(permit) = self.inner.outbound.try_reserve() {
            permit.send(Outbound {
                to: to.clone(),
                session,
                target,
                message: message.to_vec(),
            });
        }
        Ok(())
    }
}

impl LinkLifecycle for PunchTransport {
    fn shutdown(&self) {
        self.inner.cancel.cancel();
        self.inner.sessions.close();
    }

    fn close(&self) -> LinkFuture<'_, ()> {
        Box::pin(Self::close(self))
    }
}

impl Transport for PunchTransport {
    type Error = io::Error;

    fn send(&self, to: &NodeId, message: &[u8]) -> impl Future<Output = io::Result<()>> {
        std::future::ready(self.enqueue(to, message, None))
    }

    fn send_admitted(
        &self,
        to: &NodeId,
        message: &[u8],
        session: Option<groupnet_transport::admission::SessionId>,
    ) -> impl Future<Output = io::Result<()>> {
        std::future::ready(if session.is_some() {
            self.enqueue(to, message, session)
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "punch session required",
            ))
        })
    }

    async fn recv(&self) -> io::Result<Inbound> {
        Ok(self.recv_admitted().await?.packet)
    }

    async fn recv_admitted(&self) -> io::Result<AdmittedInbound> {
        loop {
            let received = tokio::select! {
                biased;
                () = self.inner.cancel.cancelled() => return Err(closed()),
                message = async { self.inner.inbound.lock().await.recv().await } => message.ok_or_else(closed)?,
            };
            if received
                .session
                .is_some_and(|id| self.inner.sessions.is_active(&received.packet.from, id))
            {
                return Ok(received);
            }
        }
    }
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn closed() -> io::Error {
    io::Error::new(io::ErrorKind::NotConnected, "UDP transport closed")
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn random<const N: usize>() -> io::Result<[u8; N]> {
    let mut bytes = [0; N];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| io::Error::other("secure random source failed"))?;
    Ok(bytes)
}

fn validate_name(node: &NodeId) -> io::Result<()> {
    if node.as_str().is_empty() || node.as_str().len() > 64 {
        return Err(invalid("UDP identities must contain 1–64 UTF-8 bytes"));
    }
    Ok(())
}

fn validate_names(nodes: &[NodeId]) -> io::Result<()> {
    if nodes.len() > MAX_PEERS {
        return Err(invalid("UDP peer limit is 128"));
    }
    let mut seen = HashSet::with_capacity(nodes.len());
    for node in nodes {
        validate_name(node)?;
        if !seen.insert(node) {
            return Err(invalid("duplicate UDP identity"));
        }
    }
    Ok(())
}

fn transient(error: &io::Error) -> bool {
    matches!(error.kind(), io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionRefused)
        // Winsock consumes an oversized UDP datagram with WSAEMSGSIZE rather
        // than returning a truncated prefix; hostile input must not shut down I/O.
        || error.raw_os_error() == Some(10040)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod admission_tests;
