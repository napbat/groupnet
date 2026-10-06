//! Native UDP rendezvous, simultaneous punching and relay fallback.
//!
//! Keyed configurations authenticate a trusted fabric, not Byzantine identities.
//! Explicit open configurations require no secret and provide no cryptographic
//! identity assurance. Both modes prove address return-routability before
//! admission, discovery, or relay. The rendezvous is not a routing participant.

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

/// Largest application message carried by one authenticated UDP datagram.
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
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PathPolicy {
    /// Probe peers simultaneously, preferring a live direct path to the relay.
    #[default]
    DirectPreferred,
    /// Never probe or accept direct peer packets; use only the rendezvous relay.
    RelayOnly,
}

/// The currently usable path to a configured or dynamically admitted peer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeerPath {
    /// An authenticated round-trip probe recently succeeded.
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

    /// Creates explicit keyless, unauthenticated, dynamically discovered relay.
    #[must_use]
    pub fn open(local: NodeId, rendezvous: SocketAddr) -> Self {
        Self::dynamic(local, rendezvous, None, Vec::new())
    }

    /// Creates dynamic discovery with an optional fabric key and opaque credential.
    /// Keyless operation is relay-only; no unauthenticated direct packets are accepted.
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

#[derive(Debug)]
struct Peer {
    node: NodeId,
    session: wire::Session,
    address: SocketAddr,
    relay_only: bool,
    offered: Instant,
    direct: Option<Instant>,
    probe: Option<wire::Session>,
    sequence: u64,
    lease: SessionLease,
}

impl Peer {
    fn path(&self, now: Instant) -> Option<PeerPath> {
        if self
            .direct
            .is_some_and(|time| now.duration_since(time) < DIRECT_LEASE)
        {
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

/// A bounded best-effort UDP transport with authenticated native hole punching.
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
        if config.credential.len() > groupnet_transport::admission::MAX_CREDENTIAL_BYTES
            || (config.key.is_none() && config.policy != PathPolicy::RelayOnly)
        {
            return Err(invalid("invalid credential size or keyless direct policy"));
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
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use groupnet_testkit::cluster::eventually_within;
    use tokio::net::UdpSocket;
    use tokio::time::timeout;

    use super::wire::{Body, MAX_PACKET, Packet, Session};
    use super::*;

    const SETTLE: Duration = Duration::from_secs(5);
    const SILENCE: Duration = Duration::from_millis(150);

    fn loopback() -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], 0))
    }

    fn key() -> NetworkKey {
        NetworkKey::from_bytes([23; 32])
    }

    struct RawPeer {
        name: &'static str,
        session: Session,
        sequence: u64,
        socket: UdpSocket,
    }

    impl RawPeer {
        async fn new(name: &'static str, session: Session) -> Self {
            Self {
                name,
                session,
                sequence: 0,
                socket: UdpSocket::bind(loopback()).await.unwrap(),
            }
        }

        async fn send(&mut self, address: SocketAddr, body: Body<'_>) -> Vec<u8> {
            self.sequence += 1;
            let mut bytes = [0; MAX_PACKET];
            let length = wire::encode(
                Packet {
                    sender: self.name,
                    session: self.session,
                    sequence: self.sequence,
                    body,
                },
                &key(),
                &mut bytes,
            )
            .unwrap();
            self.socket
                .send_to(&bytes[..length], address)
                .await
                .unwrap();
            bytes[..length].to_vec()
        }

        async fn receive(&self) -> Vec<u8> {
            let mut bytes = [0; MAX_PACKET + 1];
            let (length, _) = timeout(SETTLE, self.socket.recv_from(&mut bytes))
                .await
                .unwrap()
                .unwrap();
            bytes[..length].to_vec()
        }

        async fn silent(&self) {
            let mut bytes = [0; MAX_PACKET + 1];
            assert!(
                timeout(SILENCE, self.socket.recv_from(&mut bytes))
                    .await
                    .is_err()
            );
        }

        async fn challenge(&mut self, server: SocketAddr) -> (Session, Session) {
            let nonce = random().unwrap();
            self.send(server, Body::Hello { nonce }).await;
            let bytes = self.receive().await;
            let packet = wire::decode(&bytes, &key()).unwrap();
            match packet.body {
                Body::Challenge {
                    nonce: echoed,
                    cookie,
                } => {
                    assert_eq!(echoed, nonce);
                    (nonce, cookie)
                }
                other => panic!("expected challenge, got {other:?}"),
            }
        }

        async fn register(&mut self, server: SocketAddr, policy: PathPolicy) -> Vec<u8> {
            let (nonce, cookie) = self.challenge(server).await;
            let registration = self
                .send(
                    server,
                    Body::Register {
                        nonce,
                        cookie,
                        relay_only: policy == PathPolicy::RelayOnly,
                        credential: &[],
                    },
                )
                .await;
            let bytes = self.receive().await;
            assert!(matches!(
                wire::decode(&bytes, &key()).unwrap().body,
                Body::Registered
            ));
            registration
        }

        async fn observed_peer(&mut self, server: SocketAddr, name: &str) -> (SocketAddr, Session) {
            self.send(server, Body::Query { peer: name }).await;
            let bytes = self.receive().await;
            match wire::decode(&bytes, &key()).unwrap().body {
                Body::Offer {
                    peer,
                    address,
                    session,
                    ..
                } => {
                    assert_eq!(peer, name);
                    (address, session)
                }
                other => panic!("expected offer, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn captured_registration_cannot_redirect_from_a_different_udp_address() {
        let relay = Rendezvous::bind(loopback(), key(), vec!["a".into(), "b".into()])
            .await
            .unwrap();
        let address = relay.local_addr().unwrap();
        let mut a = RawPeer::new("a", [1; 16]).await;
        let mut b = RawPeer::new("b", [2; 16]).await;
        let mut attacker = RawPeer::new("a", [1; 16]).await;
        b.register(address, PathPolicy::RelayOnly).await;
        let (nonce, cookie) = a.challenge(address).await;
        // Even an authentic registration needs the challenge's observed address.
        let captured = a
            .send(
                address,
                Body::Register {
                    nonce,
                    cookie,
                    relay_only: true,
                    credential: &[],
                },
            )
            .await;
        attacker.socket.send_to(&captured, address).await.unwrap();
        attacker.silent().await;
        assert!(matches!(
            wire::decode(&a.receive().await, &key()).unwrap().body,
            Body::Registered
        ));
        assert_eq!(
            b.observed_peer(address, "a").await.0,
            a.socket.local_addr().unwrap()
        );
        // Even a fresh duplicate Hello from another address cannot challenge an incumbent.
        attacker.send(address, Body::Hello { nonce: [7; 16] }).await;
        attacker.silent().await; // Sequence one is older than the accepted registration.
        attacker.sequence = 100;
        attacker.send(address, Body::Hello { nonce }).await;
        attacker.silent().await;
        attacker.socket.send_to(&captured, address).await.unwrap();
        attacker.silent().await;
        assert_eq!(
            b.observed_peer(address, "a").await.0,
            a.socket.local_addr().unwrap()
        );
        relay.close().await;
    }

    #[tokio::test]
    async fn consumed_and_old_session_registration_replays_cannot_replace_a_new_session() {
        let relay = Rendezvous::bind(loopback(), key(), vec!["a".into(), "b".into()])
            .await
            .unwrap();
        let address = relay.local_addr().unwrap();
        let mut a = RawPeer::new("a", [1; 16]).await;
        let mut b = RawPeer::new("b", [2; 16]).await;
        b.register(address, PathPolicy::RelayOnly).await;
        let captured = a.register(address, PathPolicy::RelayOnly).await;
        a.socket.send_to(&captured, address).await.unwrap();
        a.silent().await;
        a.send(address, Body::Depart).await;
        a.session = [3; 16];
        a.sequence = 0;
        a.register(address, PathPolicy::RelayOnly).await;
        a.socket.send_to(&captured, address).await.unwrap();
        a.silent().await;
        assert_eq!(b.observed_peer(address, "a").await.1, [3; 16]);
        // Replaying an old session's Hello cannot displace the current session.
        let current = a.session;
        a.session = [1; 16];
        a.send(address, Body::Hello { nonce: [9; 16] }).await;
        a.silent().await;
        a.socket.send_to(&captured, address).await.unwrap();
        a.silent().await;
        assert_eq!(b.observed_peer(address, "a").await.1, current);
        relay.close().await;
    }

    #[tokio::test]
    async fn expired_challenge_and_unregistered_relay_are_rejected() {
        let relay = Rendezvous::bind(loopback(), key(), vec!["a".into(), "b".into()])
            .await
            .unwrap();
        let address = relay.local_addr().unwrap();
        let mut a = RawPeer::new("a", [1; 16]).await;
        let mut b = RawPeer::new("b", [2; 16]).await;
        b.register(address, PathPolicy::RelayOnly).await;
        a.send(
            address,
            Body::Relay {
                peer: "b",
                target: b.session,
                message: b"not registered",
            },
        )
        .await;
        b.silent().await;
        let (nonce, cookie) = a.challenge(address).await;
        let issued = Instant::now();
        eventually_within("challenge expires", SETTLE, || {
            issued.elapsed() >= Duration::from_millis(3100)
        })
        .await;
        a.send(
            address,
            Body::Register {
                nonce,
                cookie,
                relay_only: true,
                credential: &[],
            },
        )
        .await;
        a.silent().await;
        b.send(address, Body::Query { peer: "a" }).await;
        b.silent().await;
        a.register(address, PathPolicy::RelayOnly).await;
        assert_eq!(b.observed_peer(address, "a").await.1, a.session);
        relay.close().await;
    }

    #[tokio::test]
    async fn replayed_relay_datagrams_and_wrong_recipient_sessions_are_not_forwarded() {
        let relay = Rendezvous::bind(loopback(), key(), vec!["a".into(), "b".into()])
            .await
            .unwrap();
        let address = relay.local_addr().unwrap();
        let mut a = RawPeer::new("a", [1; 16]).await;
        let mut b = RawPeer::new("b", [2; 16]).await;
        a.register(address, PathPolicy::RelayOnly).await;
        b.register(address, PathPolicy::RelayOnly).await;
        a.send(
            address,
            Body::Relay {
                peer: "b",
                target: [7; 16],
                message: b"stale session",
            },
        )
        .await;
        b.silent().await;
        let captured = a
            .send(
                address,
                Body::Relay {
                    peer: "b",
                    target: b.session,
                    message: b"once",
                },
            )
            .await;
        let delivered = b.receive().await;
        match wire::decode(&delivered, &key()).unwrap().body {
            Body::Delivered {
                peer,
                session,
                message,
            } => {
                assert_eq!(peer, "a");
                assert_eq!(session, a.session);
                assert_eq!(message, b"once");
            }
            other => panic!("expected relay delivery, got {other:?}"),
        }
        a.socket.send_to(&captured, address).await.unwrap();
        b.silent().await;
        relay.close().await;
    }

    #[tokio::test]
    async fn unknown_names_and_bad_authentication_cannot_obtain_reflections() {
        let relay = Rendezvous::bind(loopback(), key(), vec!["a".into()])
            .await
            .unwrap();
        let address = relay.local_addr().unwrap();
        let mut unknown = RawPeer::new("unknown", [1; 16]).await;
        unknown.send(address, Body::Hello { nonce: [1; 16] }).await;
        unknown.silent().await;
        let a = RawPeer::new("a", [2; 16]).await;
        let mut bytes = [0; MAX_PACKET];
        let length = wire::encode(
            Packet {
                sender: "a",
                session: a.session,
                sequence: 1,
                body: Body::Hello { nonce: [1; 16] },
            },
            &NetworkKey::from_bytes([9; 32]),
            &mut bytes,
        )
        .unwrap();
        a.socket.send_to(&bytes[..length], address).await.unwrap();
        a.silent().await;
        a.socket
            .send_to(&[0; MAX_PACKET + 100], address)
            .await
            .unwrap();
        a.silent().await;
        let mut a = a;
        a.register(address, PathPolicy::RelayOnly).await;
        relay.close().await;
    }

    #[tokio::test]
    async fn direct_packet_replays_bad_keys_and_unknown_sources_do_not_enter_receive_queue() {
        let relay = Rendezvous::bind(
            loopback(),
            key(),
            vec!["a".into(), "b".into(), "unknown".into()],
        )
        .await
        .unwrap();
        let address = relay.local_addr().unwrap();
        let mut config = PunchConfig::new("a".into(), address, key(), vec!["b".into()]);
        config.bind = loopback();
        let a = PunchTransport::bind(config).await.unwrap();
        let mut b = RawPeer::new("b", [2; 16]).await;
        b.register(address, PathPolicy::DirectPreferred).await;
        eventually_within("raw peer discovered", SETTLE, || {
            a.path_to(&"b".into()).is_some()
        })
        .await;
        let a_session = b.observed_peer(address, "a").await.1;
        let destination = a.local_addr().unwrap();
        b.socket
            .send_to(&[0; MAX_PACKET + 100], destination)
            .await
            .unwrap();
        assert!(timeout(SILENCE, a.recv()).await.is_err());
        let packet = Packet {
            sender: "b",
            session: b.session,
            sequence: 100,
            body: Body::Direct {
                peer: "a",
                target: a_session,
                message: b"authenticated",
            },
        };
        let mut bytes = [0; MAX_PACKET];
        let length = wire::encode(packet, &NetworkKey::from_bytes([9; 32]), &mut bytes).unwrap();
        b.socket
            .send_to(&bytes[..length], destination)
            .await
            .unwrap();
        assert!(timeout(SILENCE, a.recv()).await.is_err());
        let length = wire::encode(
            Packet {
                sender: "unknown",
                ..packet
            },
            &key(),
            &mut bytes,
        )
        .unwrap();
        b.socket
            .send_to(&bytes[..length], destination)
            .await
            .unwrap();
        assert!(timeout(SILENCE, a.recv()).await.is_err());
        let length = wire::encode(packet, &key(), &mut bytes).unwrap();
        b.socket
            .send_to(&bytes[..length], destination)
            .await
            .unwrap();
        assert_eq!(
            timeout(SETTLE, a.recv()).await.unwrap().unwrap().msg,
            b"authenticated"
        );
        b.socket
            .send_to(&bytes[..length], destination)
            .await
            .unwrap();
        assert!(timeout(SILENCE, a.recv()).await.is_err());
        a.close().await;
        relay.close().await;
    }
}

#[cfg(test)]
mod admission_tests;
