//! Native TCP-only rendezvous, relay, and session-authenticated multi-candidate traversal.
//!
//! Reusable sockets attempt simultaneous open from the registration source port;
//! platform or NAT restrictions leave the independently running relay usable.
//! Open admission proves no identity ownership. Neither mode encrypts payloads;
//! applications requiring confidentiality must layer an encrypted protocol above it.
//!
//! Control admission is connection-bound. There is no implicit control reconnect:
//! established direct peers survive control loss, but losing the last usable
//! direct peer terminates the endpoint so callers can explicitly rebind it.
//! Keyed control and all established direct streams use direction-separated MACs
//! and fresh monotonic per-direction frame sequences.
//!
//! Relay data uses bounded TCP backpressure, optionally paced by payload bytes.
//! Control abuse has an independent configurable budget.

mod endpoint;
mod policy;
mod server;
mod sockets;
mod wire;

use crate::{NetworkKey, PathPolicy, PeerPath};
use bytes::Bytes;
use groupnet_core::NodeId;
use groupnet_transport::{
    Inbound,
    admission::{SessionId, SessionLease, SessionRegistry},
    link::AdmittedInbound,
};
use ring::rand::{SecureRandom, SystemRandom};
use std::{
    collections::HashMap,
    fmt, io,
    net::SocketAddr,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};
use tokio::{
    sync::{Mutex as AsyncMutex, mpsc},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

pub use policy::{ControlRateLimit, RelayPacing, TcpRendezvousConfig};
pub use server::TcpRendezvous;

/// Maximum application payload in one native TCP frame.
pub const MAX_TCP_MESSAGE: usize = 65_000;
// A static peer list is encoded with a u8 count. This is a wire invariant,
// not the operational limit on dynamically admitted sessions.
const MAX_PEERS: usize = u8::MAX as usize;
const MAX_CANDIDATES: usize = 8;
const DEADLINE: Duration = Duration::from_secs(3);
const IDLE: Duration = Duration::from_secs(6);

/// Configuration for one TCP-only traversal endpoint.
#[derive(Clone)]
pub struct TcpPunchConfig {
    /// Application-chosen routing identity (1–64 UTF-8 bytes).
    pub local: NodeId,
    /// Registration and primary reusable listening/source socket bind.
    pub bind: SocketAddr,
    /// Explicit TCP rendezvous address.
    pub rendezvous: SocketAddr,
    /// Optional trusted-fabric key. Keyed endpoints never downgrade.
    pub key: Option<NetworkKey>,
    /// Static peer allowlist; empty when discovering dynamically.
    pub peers: Vec<NodeId>,
    /// Whether rendezvous-admitted identities may be discovered dynamically.
    pub dynamic: bool,
    /// Opaque application admission credential, limited to 1024 bytes.
    pub credential: Vec<u8>,
    /// Direct preference or privacy-preserving relay-only operation.
    pub policy: PathPolicy,
    /// Additional listener/source binds, at most three, including other families.
    /// Each socket is retained for the lifetime of the endpoint.
    pub candidate_binds: Vec<SocketAddr>,
    /// Explicit externally mapped TCP candidates, at most eight total candidates.
    /// These are hints, never authorization. Invalid addresses are rejected.
    /// Each local/remote pair permits one outstanding check, with three-attempt
    /// bursts separated by a ten-second cooldown. A rotating peer scheduler caps
    /// the endpoint at sixteen outstanding dials and sixteen new checks/second.
    pub advertised_candidates: Vec<SocketAddr>,
    /// Expand wildcard listeners to local interface addresses when gathering.
    /// IPv6 link-local addresses without a scope are not advertised.
    pub gather_interfaces: bool,
    /// Maximum concurrently admitted neighbors (an operational memory bound).
    pub max_peers: usize,
    /// Capacity of the endpoint's bounded packet, event and writer queues.
    pub queue_capacity: usize,
}

impl TcpPunchConfig {
    /// Creates a keyed, static, direct-preferred endpoint.
    #[must_use]
    pub fn new(local: NodeId, rendezvous: SocketAddr, key: NetworkKey, peers: Vec<NodeId>) -> Self {
        let mut config = Self::dynamic(local, rendezvous, Some(key), Vec::new());
        config.peers = peers;
        config.dynamic = false;
        config
    }

    /// Creates explicit keyless dynamic discovery, defaulting to relay-only.
    #[must_use]
    pub fn open(local: NodeId, rendezvous: SocketAddr) -> Self {
        Self::dynamic(local, rendezvous, None, Vec::new())
    }

    /// Creates dynamic discovery with application credentials and an optional key.
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
                    std::net::Ipv4Addr::UNSPECIFIED.into()
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
            max_peers: 128,
            queue_capacity: 128,
        }
    }
}

impl fmt::Debug for TcpPunchConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TcpPunchConfig")
            .field("local", &self.local)
            .field("bind", &self.bind)
            .field("rendezvous", &self.rendezvous)
            .field("key", &self.key)
            .field("peers", &self.peers)
            .field("dynamic", &self.dynamic)
            .field("policy", &self.policy)
            .field("candidate_binds", &self.candidate_binds)
            .field("advertised_candidates", &self.advertised_candidates)
            .field("gather_interfaces", &self.gather_interfaces)
            .field("max_peers", &self.max_peers)
            .field("queue_capacity", &self.queue_capacity)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct View {
    session: wire::Token,
    lease: SessionLease,
    direct: Option<SocketAddr>,
}

#[derive(Debug)]
struct Outgoing {
    to: NodeId,
    generation: SessionId,
    target: wire::Token,
    message: Bytes,
}

#[derive(Debug)]
struct Inner {
    local: NodeId,
    address: SocketAddr,
    addresses: Vec<SocketAddr>,
    observed: SocketAddr,
    candidates: Vec<SocketAddr>,
    peers: Arc<Mutex<HashMap<NodeId, View>>>,
    outbound: mpsc::Sender<Outgoing>,
    inbound: AsyncMutex<mpsc::Receiver<AdmittedInbound>>,
    sessions: SessionRegistry,
    cancel: CancellationToken,
    task: AsyncMutex<Option<JoinHandle<()>>>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.sessions.close();
    }
}

/// Cloneable packet connection whose relay and candidate checks use only TCP.
/// Closing any clone closes the endpoint; dropping the last clone cancels it.
/// Rendezvous loss is terminal once no validated direct sessions remain.
#[derive(Clone, Debug)]
pub struct TcpConnection {
    inner: Arc<Inner>,
}

impl TcpConnection {
    /// Binds reusable listeners and completes bounded rendezvous admission.
    ///
    /// # Errors
    /// Returns invalid configuration, socket, authentication, admission, or timeout errors.
    pub async fn bind(config: TcpPunchConfig) -> io::Result<Self> {
        endpoint::bind(config).await
    }

    /// Returns the primary registration source bind, including its assigned port.
    ///
    /// # Errors
    /// Returns `NotConnected` after shutdown.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.ensure_open()?;
        Ok(self.inner.address)
    }

    /// Returns the registration source address observed by rendezvous.
    #[must_use]
    pub fn observed_addr(&self) -> Option<SocketAddr> {
        (!self.inner.cancel.is_cancelled()).then_some(self.inner.observed)
    }

    /// Returns locally gathered/explicit candidates (empty for relay-only).
    ///
    /// # Errors
    /// Returns `NotConnected` after shutdown.
    pub fn local_candidates(&self) -> io::Result<Vec<SocketAddr>> {
        self.ensure_open()?;
        Ok(self.inner.candidates.clone())
    }

    /// Returns every retained reusable source/listener bind.
    ///
    /// # Errors
    /// Returns `NotConnected` after shutdown.
    pub fn local_addrs(&self) -> io::Result<Vec<SocketAddr>> {
        self.ensure_open()?;
        Ok(self.inner.addresses.clone())
    }

    /// Returns the physical peer endpoint of the validated direct connection.
    #[must_use]
    pub fn direct_addr_to(&self, node: &NodeId) -> Option<SocketAddr> {
        if self.inner.cancel.is_cancelled() {
            return None;
        }
        lock(&self.inner.peers)
            .get(node)
            .and_then(|peer| peer.direct)
    }

    /// Returns the currently available direct or relay path.
    #[must_use]
    pub fn path_to(&self, node: &NodeId) -> Option<PeerPath> {
        if self.inner.cancel.is_cancelled() {
            return None;
        }
        lock(&self.inner.peers)
            .get(node)
            .filter(|peer| peer.lease.is_active())
            .map(|peer| {
                if peer.direct.is_some() {
                    PeerPath::Direct
                } else {
                    PeerPath::Relay
                }
            })
    }

    /// Returns live, rendezvous-admitted neighbors only.
    #[must_use]
    pub fn known_peers(&self) -> Vec<NodeId> {
        lock(&self.inner.peers)
            .iter()
            .filter(|(_, peer)| peer.lease.is_active())
            .map(|(node, _)| node.clone())
            .collect()
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

    /// Cancels and drains all control, dial, listener, and established stream tasks.
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
        length: usize,
        message: impl FnOnce() -> Bytes,
        expected: Option<SessionId>,
    ) -> io::Result<()> {
        self.ensure_open()?;
        if length > MAX_TCP_MESSAGE {
            return Err(invalid("TCP message exceeds MAX_TCP_MESSAGE"));
        }
        let peers = lock(&self.inner.peers);
        let Some(peer) = peers.get(to) else {
            return Ok(());
        };
        if !peer.lease.is_active() || expected.is_some_and(|id| id != peer.lease.id()) {
            return Ok(());
        }
        if let Ok(permit) = self.inner.outbound.try_reserve() {
            permit.send(Outgoing {
                to: to.clone(),
                generation: peer.lease.id(),
                target: peer.session,
                message: message(),
            });
        }
        Ok(())
    }
}

impl TcpConnection {
    /// Queues a bounded best-effort packet for a live admitted neighbor.
    /// Unknown peers and full queues are dropped without blocking.
    ///
    /// # Errors
    /// Returns `NotConnected` after shutdown or `InvalidInput` for oversized data.
    pub fn send(&self, to: &NodeId, message: &[u8]) -> impl Future<Output = io::Result<()>> + Send {
        std::future::ready(self.enqueue(
            to,
            message.len(),
            || Bytes::copy_from_slice(message),
            None,
        ))
    }

    /// Queues data only when the supplied admission generation is still current.
    /// Missing or stale generations and full queues are dropped without blocking.
    ///
    /// # Errors
    /// Returns `NotConnected` after shutdown or `InvalidInput` for oversized data.
    pub fn send_admitted(
        &self,
        to: &NodeId,
        message: &[u8],
        session: Option<SessionId>,
    ) -> impl Future<Output = io::Result<()>> + Send {
        std::future::ready(if session.is_some() {
            self.enqueue(
                to,
                message.len(),
                || Bytes::copy_from_slice(message),
                session,
            )
        } else {
            Ok(())
        })
    }

    /// Transfers an owned packet to the bounded queue without copying its payload.
    ///
    /// # Errors
    /// Returns `NotConnected` after shutdown or `InvalidInput` for oversized data.
    pub fn send_owned(
        &self,
        to: &NodeId,
        message: Bytes,
    ) -> impl Future<Output = io::Result<()>> + Send {
        std::future::ready(self.enqueue(to, message.len(), || message, None))
    }

    /// Transfers owned data only for the still-current admission generation.
    /// Missing/stale generations and full queues are dropped without blocking.
    ///
    /// # Errors
    /// Returns `NotConnected` after shutdown or `InvalidInput` for oversized data.
    pub fn send_owned_admitted(
        &self,
        to: &NodeId,
        message: Bytes,
        session: Option<SessionId>,
    ) -> impl Future<Output = io::Result<()>> + Send {
        std::future::ready(if session.is_some() {
            self.enqueue(to, message.len(), || message, session)
        } else {
            Ok(())
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
            let packet = tokio::select! {
                biased;
                () = self.inner.cancel.cancelled() => return Err(closed()),
                packet = async { self.inner.inbound.lock().await.recv().await } => packet.ok_or_else(closed)?,
            };
            if packet
                .session
                .is_some_and(|id| self.inner.sessions.is_active(&packet.packet.from, id))
            {
                return Ok(packet);
            }
        }
    }
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn validate_capacity(capacity: usize) -> io::Result<()> {
    if capacity == 0 || capacity > tokio::sync::Semaphore::MAX_PERMITS {
        return Err(invalid(
            "TCP capacity must be nonzero and fit the channel semaphore",
        ));
    }
    Ok(())
}

fn closed() -> io::Error {
    io::Error::new(
        io::ErrorKind::NotConnected,
        "TCP traversal transport closed",
    )
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn random() -> io::Result<wire::Token> {
    let mut token = [0; 32];
    SystemRandom::new()
        .fill(&mut token)
        .map_err(|_| io::Error::other("secure random source failed"))?;
    Ok(token)
}
