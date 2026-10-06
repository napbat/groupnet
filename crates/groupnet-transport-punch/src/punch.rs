//! Native UDP rendezvous, simultaneous punching and relay fallback.
//!
//! Keyed configurations authenticate a trusted fabric, not Byzantine identities.
//! Keyless configurations (`key: None`) require neither a pre-shared transport
//! key nor an identity keypair; internal random session challenges still prove
//! return-routability. Application admission may independently require credentials.
//! Open admission provides no cryptographic identity assurance. The rendezvous
//! is not a routing participant.

mod candidates;
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
use groupnet_transport::Inbound;
use groupnet_transport::admission::{SessionLease, SessionRegistry};
use groupnet_transport::link::AdmittedInbound;
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
    /// Primary UDP bind address; its family must match the rendezvous.
    /// This socket owns the observed mapping and remains the relay/control path.
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
    /// Additional direct-path sockets (at most three), including another IP family.
    /// Their lifetimes are owned by this endpoint; the primary `bind` owns relay registration.
    pub candidate_binds: Vec<SocketAddr>,
    /// Operator-supplied direct addresses (at most eight), checked independently.
    /// Addresses are untrusted hints, never proof of a usable path.
    pub advertised_candidates: Vec<SocketAddr>,
    /// Gather usable local interface addresses for wildcard-bound sockets.
    /// Link-local addresses are excluded because remote scope identifiers are not portable.
    pub gather_interfaces: bool,
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
            candidate_binds: Vec::new(),
            advertised_candidates: Vec::new(),
            gather_interfaces: true,
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
            candidate_binds: Vec::new(),
            advertised_candidates: Vec::new(),
            gather_interfaces: true,
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
            .field("candidate_binds", &self.candidate_binds)
            .field("advertised_candidates", &self.advertised_candidates)
            .field("gather_interfaces", &self.gather_interfaces)
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

struct Peer {
    node: NodeId,
    session: wire::Session,
    addresses: candidates::PeerCandidates,
    secret: wire::Session,
    relay_only: bool,
    offered: Instant,
    checks: Vec<PathCheck>,
    selected: Option<usize>,
    candidate_cursor: usize,
    sequence: u64,
    lease: SessionLease,
}

impl fmt::Debug for Peer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Peer")
            .field("node", &self.node)
            .field("session", &self.session)
            .field("addresses", &self.addresses)
            .field("secret", &"[REDACTED]")
            .field("relay_only", &self.relay_only)
            .field("offered", &self.offered)
            .field("checks", &self.checks)
            .field("selected", &self.selected)
            .field("candidate_cursor", &self.candidate_cursor)
            .field("sequence", &self.sequence)
            .field("lease", &self.lease)
            .finish()
    }
}

#[derive(Debug)]
struct PathCheck {
    address: SocketAddr,
    socket: usize,
    direct: Option<Capability>,
    probe: Option<Capability>,
    pending: Option<PendingCapability>,
    confirmed: Option<Capability>,
    confirmed_probe: u64,
    attempts: u8,
    next_probe: Instant,
}

impl PathCheck {
    fn new(address: SocketAddr, socket: usize) -> Self {
        Self {
            address,
            socket,
            direct: None,
            probe: None,
            pending: None,
            confirmed: None,
            confirmed_probe: 0,
            attempts: 0,
            next_probe: Instant::now(),
        }
    }
}

impl Peer {
    fn path(&self, now: Instant) -> Option<PeerPath> {
        if self.direct_path(now).is_some() {
            Some(PeerPath::Direct)
        } else if now.duration_since(self.offered) < LEASE {
            Some(PeerPath::Relay)
        } else {
            None
        }
    }

    fn direct_path(&self, now: Instant) -> Option<&PathCheck> {
        self.selected
            .and_then(|index| self.checks.get(index))
            .filter(|check| check.direct.is_some_and(|capability| capability.live(now)))
            .or_else(|| {
                self.checks
                    .iter()
                    .find(|check| check.direct.is_some_and(|capability| capability.live(now)))
            })
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
    local: NodeId,
    address: SocketAddr,
    addresses: Vec<SocketAddr>,
    candidates: candidates::Candidates,
    observed: Arc<Mutex<Option<SocketAddr>>>,
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

/// A bounded best-effort UDP connection with session-bound native hole punching.
///
/// Clones share queues, path state and one runtime. Closing any clone closes all
/// clones; dropping the last clone cancels the runtime and releases every socket.
/// At most four leased sockets, eight advertised addresses plus their observed
/// mapping, and 32 active independent checks per peer are retained. Remaining
/// configured pairs rotate through expired unvalidated slots.
/// Checks have three-second deadlines; failed
/// pairs pause for ten seconds after three attempts. A healthy selection is
/// sticky while alternatives remain eligible for expiry-driven failover.
#[derive(Clone, Debug)]
pub struct UdpConnection {
    inner: Arc<Inner>,
}

impl UdpConnection {
    /// Binds the primary and candidate sockets, then starts registration and checks.
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
        if !candidates::valid(config.rendezvous)
            || config.bind.is_ipv4() != config.rendezvous.is_ipv4()
        {
            return Err(invalid(
                "invalid rendezvous address or socket address family",
            ));
        }
        let (sockets, local_candidates) = candidates::bind(&config).await?;
        let addresses = sockets
            .iter()
            .map(UdpSocket::local_addr)
            .collect::<io::Result<Vec<_>>>()?;
        let address = addresses[0];
        let observed = Arc::new(Mutex::new(None));
        let session = random()?;
        let nonce = random()?;
        let peers = Arc::new(Mutex::new(HashMap::new()));
        let cancel = CancellationToken::new();
        let (outbound, outgoing) = mpsc::channel(QUEUE);
        let (incoming, inbound) = mpsc::channel(QUEUE);
        let configured = config.peers.clone();
        let sessions = SessionRegistry::new(MAX_PEERS)?;
        let dynamic = config.dynamic;
        let local = config.local.clone();
        let (ready, admitted) = tokio::sync::oneshot::channel();
        let mut task = tokio::spawn(endpoint::run(
            config,
            (sockets, local_candidates, observed.clone()),
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
                local,
                address,
                addresses,
                candidates: local_candidates,
                observed,
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

    /// Returns every leased socket's actual bind address.
    ///
    /// # Errors
    /// Returns `NotConnected` after shutdown.
    pub fn local_addrs(&self) -> io::Result<Vec<SocketAddr>> {
        self.ensure_open()?;
        Ok(self.inner.addresses.clone())
    }

    /// Returns the bounded advertised local candidates (empty under relay-only policy).
    ///
    /// # Errors
    /// Returns `NotConnected` after shutdown.
    pub fn local_candidates(&self) -> io::Result<Vec<SocketAddr>> {
        self.ensure_open()?;
        Ok(self.inner.candidates.iter().collect())
    }

    /// Returns the primary address observed by the current rendezvous registration.
    /// Relay-only endpoints do not retain or disclose this address.
    #[must_use]
    pub fn observed_addr(&self) -> Option<SocketAddr> {
        if self.inner.cancel.is_cancelled() {
            return None;
        }
        *lock(&self.inner.observed)
    }

    /// Returns the selected live direct peer address, or `None` while relaying.
    #[must_use]
    pub fn direct_addr_to(&self, node: &NodeId) -> Option<SocketAddr> {
        if self.inner.cancel.is_cancelled() {
            return None;
        }
        let peers = lock(&self.inner.peers);
        let peer = peers.get(node.as_str())?;
        if !peer.lease.is_active() {
            return None;
        }
        Some(peer.direct_path(Instant::now())?.address)
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

    /// Returns the application identity retained by this connection.
    #[must_use]
    pub fn local_id(&self) -> &NodeId {
        &self.inner.local
    }

    /// Returns a clone of the same live admission and generation registry.
    #[must_use]
    pub fn sessions(&self) -> SessionRegistry {
        self.inner.sessions.clone()
    }

    /// Cancels owned I/O and invalidates all admitted sessions on every clone.
    pub fn shutdown(&self) {
        self.inner.cancel.cancel();
        self.inner.sessions.close();
    }

    /// Cancels all I/O and waits for the socket-owning task to finish.
    pub async fn close(&self) {
        self.shutdown();
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

impl UdpConnection {
    /// Queues a bounded best-effort packet for a live admitted neighbor.
    /// Unknown peers and full queues are dropped without blocking.
    ///
    /// # Errors
    /// Returns `NotConnected` after shutdown or `InvalidInput` for oversized data.
    pub fn send(&self, to: &NodeId, message: &[u8]) -> impl Future<Output = io::Result<()>> + Send {
        std::future::ready(self.enqueue(to, message, None))
    }

    /// Queues data only when the supplied admission generation is still current.
    /// Stale generations and full queues are dropped without blocking.
    ///
    /// # Errors
    /// Returns `NotConnected` after shutdown or when no generation is supplied,
    /// and `InvalidInput` for oversized data.
    pub fn send_admitted(
        &self,
        to: &NodeId,
        message: &[u8],
        session: Option<groupnet_transport::admission::SessionId>,
    ) -> impl Future<Output = io::Result<()>> + Send {
        std::future::ready(if session.is_some() {
            self.enqueue(to, message, session)
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "punch session required",
            ))
        })
    }

    /// Receives a packet from a currently admitted neighbor.
    ///
    /// # Errors
    /// Returns `NotConnected` when this connection closes.
    pub async fn recv(&self) -> io::Result<Inbound> {
        Ok(self.recv_admitted().await?.packet)
    }

    /// Receives a packet with its current admission generation.
    /// Queued packets from revoked or replaced generations are discarded.
    ///
    /// # Errors
    /// Returns `NotConnected` when this connection closes.
    pub async fn recv_admitted(&self) -> io::Result<AdmittedInbound> {
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
    io::Error::new(io::ErrorKind::NotConnected, "UDP connection closed")
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
