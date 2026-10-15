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

mod config;
mod endpoint;
mod policy;
mod server;
mod sockets;
mod wire;

use crate::PeerPath;
use crate::common::{PunchSendError, closed, invalid, lock, random};
use bytes::Bytes;
use groupnet_core::NodeId;
use groupnet_transport::{
    Inbound,
    admission::{SessionId, SessionLease, SessionRegistry},
    link::AdmittedInbound,
};
use std::{
    collections::HashMap,
    io,
    net::SocketAddr,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};
use tokio::{
    sync::{Mutex as AsyncMutex, mpsc, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

pub use config::{TcpPunchConfig, TcpPunchConfigError};
pub use policy::{
    ControlRateLimit, TcpRendezvousConfig, TcpRendezvousConfigError, TcpRendezvousLimits,
};
pub use server::TcpRendezvous;

/// Maximum application payload in one native TCP frame.
pub const MAX_TCP_MESSAGE: usize = 65_000;
// A static peer list is encoded with a u8 count. This is a wire invariant,
// not the operational limit on dynamically admitted sessions.
const MAX_PEERS: usize = u8::MAX as usize;
const MAX_CANDIDATES: usize = 8;
const MAX_CANDIDATE_BINDS: usize = 3;
const DEADLINE: Duration = Duration::from_secs(3);
const IDLE: Duration = Duration::from_secs(6);

#[derive(Debug)]
struct View {
    session: wire::Token,
    lease: SessionLease,
    direct: Option<SocketAddr>,
}

/// Published neighbor views; every path mutation notifies [`TcpConnection::path_changes`].
#[derive(Debug)]
struct Views {
    peers: Mutex<HashMap<NodeId, View>>,
    changes: watch::Sender<()>,
}

impl Views {
    fn new() -> Self {
        Self {
            peers: Mutex::new(HashMap::new()),
            changes: watch::Sender::new(()),
        }
    }

    fn read(&self) -> MutexGuard<'_, HashMap<NodeId, View>> {
        lock(&self.peers)
    }

    fn insert(&self, node: NodeId, view: View) {
        lock(&self.peers).insert(node, view);
        self.changes.send_replace(());
    }

    fn remove(&self, node: &NodeId) {
        if lock(&self.peers).remove(node).is_some() {
            self.changes.send_replace(());
        }
    }

    fn set_direct(&self, node: &NodeId, direct: Option<SocketAddr>) {
        let changed = lock(&self.peers)
            .get_mut(node)
            .is_some_and(|view| std::mem::replace(&mut view.direct, direct) != direct);
        if changed {
            self.changes.send_replace(());
        }
    }

    fn clear(&self) {
        let mut peers = lock(&self.peers);
        if !peers.is_empty() {
            peers.clear();
            drop(peers);
            self.changes.send_replace(());
        }
    }
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
    peers: Arc<Views>,
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
    /// Rejects invalid configuration as `InvalidInput` wrapping its
    /// [`TcpPunchConfigError`], then returns socket, authentication, admission,
    /// or timeout errors.
    pub async fn bind(config: TcpPunchConfig) -> io::Result<Self> {
        endpoint::bind(config).await
    }

    /// Returns the primary registration source bind, including its assigned port.
    /// Cached at bind and retained after shutdown.
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.inner.address
    }

    /// Returns the registration source address observed by rendezvous.
    /// A live query: `None` after shutdown.
    #[must_use]
    pub fn observed_addr(&self) -> Option<SocketAddr> {
        (!self.is_closed()).then_some(self.inner.observed)
    }

    /// Returns locally gathered/explicit candidates (empty for relay-only).
    /// Gathered at bind and retained after shutdown.
    #[must_use]
    pub fn local_candidates(&self) -> Vec<SocketAddr> {
        self.inner.candidates.clone()
    }

    /// Returns every retained reusable source/listener bind.
    /// Cached at bind and retained after shutdown.
    #[must_use]
    pub fn local_addrs(&self) -> &[SocketAddr] {
        &self.inner.addresses
    }

    /// Whether this connection has shut down through any clone. A live query.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.inner.cancel.is_cancelled()
    }

    /// Returns the physical peer endpoint of the validated direct connection.
    /// A live query: `None` while relaying and after shutdown.
    #[must_use]
    pub fn direct_addr_to(&self, node: &NodeId) -> Option<SocketAddr> {
        if self.inner.cancel.is_cancelled() {
            return None;
        }
        self.inner
            .peers
            .read()
            .get(node)
            .and_then(|peer| peer.direct)
    }

    /// Returns the currently available direct or relay path.
    /// A live query: `None` after shutdown.
    #[must_use]
    pub fn path_to(&self, node: &NodeId) -> Option<PeerPath> {
        if self.inner.cancel.is_cancelled() {
            return None;
        }
        self.inner
            .peers
            .read()
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
        self.inner
            .peers
            .read()
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

    /// Notifies whenever a neighbor view is admitted or removed, or its direct
    /// path is established or lost. Read the new state through [`Self::path_to`],
    /// [`Self::direct_addr_to`] and [`Self::known_peers`]; the sender outlives
    /// shutdown, so observe closure through [`Self::sessions`].
    #[must_use]
    pub fn path_changes(&self) -> watch::Receiver<()> {
        self.inner.peers.changes.subscribe()
    }

    /// Cancels owned I/O and invalidates all admitted sessions on every clone.
    pub fn shutdown(&self) {
        self.inner.cancel.cancel();
        self.inner.sessions.close();
    }

    /// Cancels and drains all control, dial, listener, and established stream tasks.
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
        message: impl FnOnce() -> Bytes,
        expected: Option<SessionId>,
    ) -> io::Result<()> {
        self.ensure_open()?;
        if length > MAX_TCP_MESSAGE {
            return Err(PunchSendError::PayloadTooLarge {
                len: length,
                max: MAX_TCP_MESSAGE,
            }
            .into());
        }
        let peers = self.inner.peers.read();
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
    /// Returns `NotConnected` after shutdown, or `InvalidInput` carrying
    /// [`PunchSendError`] for oversized data.
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
    /// Returns `NotConnected` after shutdown, or `InvalidInput` carrying
    /// [`PunchSendError`] for oversized data.
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
    /// Returns `NotConnected` after shutdown, or `InvalidInput` carrying
    /// [`PunchSendError`] for oversized data.
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
    /// Returns `NotConnected` after shutdown, or `InvalidInput` carrying
    /// [`PunchSendError`] for oversized data.
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
