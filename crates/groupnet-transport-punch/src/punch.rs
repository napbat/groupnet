//! Native UDP rendezvous, simultaneous punching and relay fallback.
//!
//! Keyed configurations authenticate a trusted fabric, not Byzantine identities.
//! Keyless configurations (`key: None`) require neither a pre-shared transport
//! key nor an identity keypair; internal random session challenges still prove
//! return-routability. Application admission may independently require credentials.
//! Open admission provides no cryptographic identity assurance. The rendezvous
//! is not a routing participant.

mod candidates;
mod config;
mod endpoint;
mod server;
mod wire;

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use groupnet_core::NodeId;
use groupnet_transport::admission::{SessionLease, SessionRegistry};
use groupnet_transport::link::AdmittedInbound;
use groupnet_transport::{Inbound, QueueCapacity};
use ring::hmac;
use tokio::net::UdpSocket;
use tokio::sync::{Mutex as AsyncMutex, mpsc};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::common::{closed, invalid, lock, random};

pub use config::{UdpPunchConfig, UdpPunchConfigError};
pub use server::{
    UdpRendezvous, UdpRendezvousConfig, UdpRendezvousConfigError, UdpRendezvousLimits,
};

/// Largest application message carried by one session-bound UDP datagram.
pub const MAX_UDP_MESSAGE: usize = 960;
const DEFAULT_MAX_PEERS: NonZeroUsize = NonZeroUsize::new(128).unwrap();
const DEFAULT_QUEUE_CAPACITY: QueueCapacity = QueueCapacity::of(128);
const HEARTBEAT: Duration = Duration::from_secs(1);
const LEASE: Duration = Duration::from_secs(6);
const DIRECT_LEASE: Duration = Duration::from_secs(3);
const MAX_CHECKS: usize = 32;

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

    fn tuple(&self) -> (SocketAddr, usize) {
        (self.address, self.socket)
    }

    /// Holds a live transmit or receive capability.
    fn validated(&self, now: Instant) -> bool {
        self.direct.is_some_and(|capability| capability.live(now))
            || self
                .confirmed
                .is_some_and(|capability| capability.live(now))
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
    message: Bytes,
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
    /// Rejects invalid configuration as `InvalidInput` wrapping its
    /// [`UdpPunchConfigError`], then dynamic policy denial or registration
    /// timeout, and socket/random-source failures.
    pub async fn bind(config: UdpPunchConfig) -> io::Result<Self> {
        config.validate()?;
        let sessions = SessionRegistry::new(config.max_peers);
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
        let (outbound, outgoing) = mpsc::channel(config.queue_capacity.get());
        let (incoming, inbound) = mpsc::channel(config.queue_capacity.get());
        let configured = config.peers.static_peers().to_vec();
        let dynamic = config.peers.is_dynamic();
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

    /// Returns the primary socket's bind address, including its assigned port.
    /// Cached at bind and retained after shutdown.
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.inner.address
    }

    /// Returns every leased socket's bind address, primary first.
    /// Cached at bind and retained after shutdown.
    #[must_use]
    pub fn local_addrs(&self) -> &[SocketAddr] {
        &self.inner.addresses
    }

    /// Returns the bounded advertised local candidates (empty under relay-only policy).
    /// Gathered at bind and retained after shutdown.
    #[must_use]
    pub fn local_candidates(&self) -> Vec<SocketAddr> {
        self.inner.candidates.iter().collect()
    }

    /// Whether this connection has shut down through any clone. A live query.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.inner.cancel.is_cancelled()
    }

    /// Returns the primary address observed by the current rendezvous registration.
    /// Relay-only endpoints do not retain or disclose this address. A live
    /// query: `None` before registration and after shutdown.
    #[must_use]
    pub fn observed_addr(&self) -> Option<SocketAddr> {
        if self.inner.cancel.is_cancelled() {
            return None;
        }
        *lock(&self.inner.observed)
    }

    /// Returns the selected live direct peer address, or `None` while relaying.
    /// A live query: `None` after shutdown.
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
    /// A live query: `None` after shutdown.
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
        crate::common::join(&self.inner.task).await;
    }

    fn ensure_open(&self) -> io::Result<()> {
        if self.is_closed() {
            Err(closed())
        } else {
            Ok(())
        }
    }

    fn enqueue(
        &self,
        to: &NodeId,
        length: usize,
        expected: Option<groupnet_transport::admission::SessionId>,
        message: impl FnOnce() -> Bytes,
    ) -> io::Result<()> {
        self.ensure_open()?;
        if length > MAX_UDP_MESSAGE {
            return Err(invalid("UDP message exceeds MAX_UDP_MESSAGE"));
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
                message: message(),
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
        std::future::ready(
            self.enqueue(to, message.len(), None, || Bytes::copy_from_slice(message)),
        )
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
            self.enqueue(to, message.len(), session, || {
                Bytes::copy_from_slice(message)
            })
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "punch session required",
            ))
        })
    }

    /// Queues an owned best-effort packet without copying its payload.
    /// Unknown peers and full queues are dropped without blocking.
    ///
    /// # Errors
    /// Returns `NotConnected` after shutdown or `InvalidInput` for oversized data.
    pub fn send_owned(
        &self,
        to: &NodeId,
        message: Bytes,
    ) -> impl Future<Output = io::Result<()>> + Send {
        std::future::ready(self.enqueue(to, message.len(), None, || message))
    }

    /// Queues an owned packet only for the supplied current admission generation.
    /// Stale generations and full queues are dropped without blocking; accepted
    /// payloads are moved into the queue without copying.
    ///
    /// # Errors
    /// Returns `NotConnected` after shutdown or when no generation is supplied,
    /// and `InvalidInput` for oversized data.
    pub fn send_owned_admitted(
        &self,
        to: &NodeId,
        message: Bytes,
        session: Option<groupnet_transport::admission::SessionId>,
    ) -> impl Future<Output = io::Result<()>> + Send {
        std::future::ready(if session.is_some() {
            self.enqueue(to, message.len(), session, || message)
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

#[cfg(test)]
mod queue_tests;
