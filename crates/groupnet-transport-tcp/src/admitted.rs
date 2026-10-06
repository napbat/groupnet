//! Policy-controlled, full-duplex TCP sessions for managed links.

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use groupnet_core::NodeId;
use groupnet_transport::admission::{
    Admission, JoinRequest, MAX_CREDENTIAL_BYTES, SessionLease, SessionRegistry,
};
use groupnet_transport::link::{BoundLink, LinkConfig, LinkFuture, LinkLifecycle};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
use tokio::time::timeout;

use super::{Inner, MAX_FRAME, QueuedInbound, TcpMsgConfig, TcpMsgTransport};
use crate::handshake::{read_id, read_str, write_id, write_str};

const MAGIC: &[u8; 8] = b"GNJOIN01";

/// Bounds for policy work and established managed TCP sessions.
#[derive(Clone, Debug)]
pub struct TcpAdmissionConfig {
    /// Maximum handshakes executing concurrently, including custom policy work.
    pub max_pending: usize,
    /// Maximum live admitted identities. Duplicate live identities are rejected.
    pub max_peers: usize,
    /// Deadline covering connection establishment, wire exchange, and policy work.
    pub handshake_timeout: Duration,
}

impl Default for TcpAdmissionConfig {
    fn default() -> Self {
        Self {
            max_pending: 64,
            max_peers: 1024,
            handshake_timeout: Duration::from_secs(5),
        }
    }
}

#[derive(Debug)]
struct Connection {
    generation: u64,
    session: Option<groupnet_transport::admission::SessionId>,
    pending: bool,
    outbound: bool,
    frames: mpsc::Sender<Vec<u8>>,
    cancel: watch::Sender<bool>,
}

#[derive(Debug, Default)]
struct Connections {
    next_generation: u64,
    peers: HashMap<NodeId, Connection>,
}

pub(super) struct Managed {
    pub(super) sessions: SessionRegistry,
    policy: Arc<dyn Admission>,
    credential: Vec<u8>,
    config: TcpAdmissionConfig,
    pending: Arc<Semaphore>,
    connections: Mutex<Connections>,
    inbound: Mutex<Option<mpsc::Sender<QueuedInbound>>>,
}

impl fmt::Debug for Managed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Application policies can contain secrets too: neither policy nor credential is printed.
        f.debug_struct("Managed")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl TcpMsgTransport {
    /// Binds a policy-controlled endpoint with bounded, full-duplex sessions.
    ///
    /// Credentials are opaque application bytes, never cryptographic identity by
    /// themselves. Use explicit `OpenAdmission` for unauthenticated membership.
    /// Managed sessions stay connected until disconnect, revocation, or shutdown;
    /// the raw transport's idle/oldest-first eviction does not revoke membership.
    ///
    /// # Errors
    /// Returns invalid-input errors for invalid bounds or oversized credentials,
    /// and propagates listener bind failures.
    pub async fn bind_admitted(
        local: NodeId,
        addr: impl ToSocketAddrs,
        config: TcpMsgConfig,
        policy: Arc<dyn Admission>,
        credential: Vec<u8>,
        admission: TcpAdmissionConfig,
    ) -> io::Result<Self> {
        if credential.len() > MAX_CREDENTIAL_BYTES
            || !(1..=4096).contains(&admission.max_pending)
            || admission.max_peers == 0
            || admission.handshake_timeout.is_zero()
            || local.as_str().is_empty()
            || local.as_str().len() > 255
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid TCP admission configuration",
            ));
        }
        let managed = Arc::new(Managed {
            sessions: SessionRegistry::new(admission.max_peers)?,
            pending: Arc::new(Semaphore::new(admission.max_pending)),
            policy,
            credential,
            config: admission,
            connections: Mutex::new(Connections::default()),
            inbound: Mutex::new(None),
        });
        Self::bind_inner(local, addr, config, Some(managed)).await
    }

    /// Initiates admission to a registered bootstrap endpoint without a frame.
    /// Unknown addresses are ignored, following the best-effort send contract.
    ///
    /// # Errors
    /// Returns an error if this endpoint is shut down or is not admission-enabled.
    ///
    /// # Panics
    /// If a connection or address-book lock was poisoned.
    pub fn connect_peer(&self, node: &NodeId) -> io::Result<()> {
        let Some(managed) = &self.inner.admission else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TCP admission is not configured",
            ));
        };
        managed.send(&self.inner, node, None)
    }

    /// Packages this endpoint with its owned workers and live-session routing.
    /// An admitted endpoint routes only through live admitted peers; address-book
    /// entries are bootstrap hints and never bypass admission.
    #[must_use]
    pub fn into_bound_link(self, cost: u32) -> BoundLink {
        let mut config = LinkConfig::new(Vec::new());
        config.cost = cost;
        let lifecycle = self.lifecycle();
        let sessions = self.sessions();
        let bound = BoundLink::new(self, config).with_lifecycle(lifecycle);
        if let Some(sessions) = sessions {
            bound.with_sessions(sessions)
        } else {
            bound
        }
    }
}

impl TcpMsgTransport {
    pub(crate) fn bootstrap(&self, peers: Vec<NodeId>) {
        if peers.is_empty() {
            return;
        }
        let inner = Arc::downgrade(&self.inner);
        self.inner.tasks.spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut first = 0;
            loop {
                tick.tick().await;
                let Some(inner) = inner.upgrade() else {
                    return;
                };
                let Some(managed) = &inner.admission else {
                    return;
                };
                for offset in 0..peers.len() {
                    let peer = &peers[(first + offset) % peers.len()];
                    let _ = managed.send(&inner, peer, None);
                }
                // An unavailable prefix must not monopolize bounded dial slots.
                first = (first + managed.config.max_pending.min(peers.len())) % peers.len();
            }
        });
    }
}

impl LinkLifecycle for TcpMsgTransport {
    fn shutdown(&self) {
        Self::shutdown(self);
    }

    fn close(&self) -> LinkFuture<'_, ()> {
        Box::pin(Self::close(self))
    }
}

impl Managed {
    pub(super) fn outbound_connections(&self) -> usize {
        self.connections
            .lock()
            .expect("connections lock poisoned")
            .peers
            .values()
            .filter(|connection| connection.outbound)
            .count()
    }

    pub(super) fn set_inbound(&self, inbound: mpsc::Sender<QueuedInbound>) {
        *self.inbound.lock().expect("inbound lock poisoned") = Some(inbound);
    }

    pub(super) fn shutdown(&self) {
        self.pending.close();
        self.sessions.close();
        self.inbound.lock().expect("inbound lock poisoned").take();
        self.connections
            .lock()
            .expect("connections lock poisoned")
            .peers
            .clear();
    }

    pub(super) fn send_admitted(
        &self,
        inner: &Arc<Inner>,
        peer: &NodeId,
        msg: &[u8],
        expected: Option<groupnet_transport::admission::SessionId>,
    ) -> io::Result<()> {
        if inner.tasks.stopped() {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "tcp msg transport shut down",
            ));
        }
        let Some(expected) = expected else {
            return Ok(());
        };
        if msg.len() > MAX_FRAME {
            return Ok(());
        }
        let connections = self.connections.lock().expect("connections lock poisoned");
        let Some(connection) = connections.peers.get(peer) else {
            return Ok(());
        };
        if connection.session != Some(expected) || !self.sessions.is_active(peer, expected) {
            return Ok(());
        }
        // This queue belongs permanently to this socket owner. No capacity
        // await or later NodeId lookup can migrate its frames to a replacement.
        let _ = connection.frames.try_send(frame(msg));
        Ok(())
    }

    pub(super) fn send(
        self: &Arc<Self>,
        inner: &Arc<Inner>,
        peer: &NodeId,
        msg: Option<&[u8]>,
    ) -> io::Result<()> {
        if inner.tasks.stopped() {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "tcp msg transport shut down",
            ));
        }
        if peer == &inner.local || msg.is_some_and(|msg| msg.len() > MAX_FRAME) {
            return Ok(());
        }
        let mut connections = self.connections.lock().expect("connections lock poisoned");
        if let Some(connection) = connections.peers.get(peer) {
            if connection
                .session
                .is_some_and(|session| !self.sessions.is_active(peer, session))
            {
                return Ok(());
            }
            if let Some(msg) = msg {
                let _ = connection.frames.try_send(frame(msg));
            }
            return Ok(());
        }
        let Some(addr) = inner
            .peers
            .read()
            .expect("peers lock poisoned")
            .get(peer)
            .copied()
        else {
            return Ok(());
        };
        let Ok(permit) = self.pending.clone().try_acquire_owned() else {
            return Ok(());
        };
        let (frames, receiver) = mpsc::channel(inner.config.outbound_queue);
        if let Some(msg) = msg {
            let _ = frames.try_send(frame(msg));
        }
        let (cancel, cancelled) = watch::channel(false);
        let generation = connections.next_generation;
        connections.next_generation = generation
            .checked_add(1)
            .ok_or_else(|| io::Error::other("TCP session generations exhausted"))?;
        connections.peers.insert(
            peer.clone(),
            Connection {
                generation,
                session: None,
                pending: true,
                outbound: true,
                frames,
                cancel,
            },
        );
        drop(connections);
        let owned = Owner {
            managed: self.clone(),
            peer: peer.clone(),
            generation,
        };
        let weak = Arc::downgrade(inner);
        let local = inner.local.clone();
        let intro = inner.intro.clone();
        let managed = self.clone();
        let peer = peer.clone();
        inner.tasks.spawn(async move {
            let work = async move {
                let mut socket = TcpStream::connect(addr).await?;
                let _ = socket.set_nodelay(true);
                let lease = managed
                    .dial_handshake(&mut socket, &local, &intro, &peer, generation)
                    .await?;
                Ok::<_, io::Error>((socket, lease))
            };
            run_pending(owned, weak, work, receiver, cancelled, permit).await;
        });
        Ok(())
    }

    pub(super) fn accept(self: &Arc<Self>, inner: &Arc<Inner>, socket: TcpStream) {
        let Ok(permit) = self.pending.clone().try_acquire_owned() else {
            return;
        };
        let managed = self.clone();
        let weak = Arc::downgrade(inner);
        let local = inner.local.clone();
        let intro = inner.intro.clone();
        let queue = inner.config.outbound_queue;
        inner.tasks.spawn(async move {
            let result = timeout(managed.config.handshake_timeout, async {
                let mut socket = socket;
                let hello = read_hello(&mut socket).await?;
                if hello.node == local {
                    return Err(denied());
                }
                let accepted = managed.authorize(&hello, socket.peer_addr().ok()).await?;
                let (owner, receiver, cancelled) =
                    managed.claim_inbound(&local, &hello.node, queue)?;
                let lease = owner.admit(accepted)?;
                write_hello(&mut socket, &local, &intro, &managed.credential).await?;
                write_id(&mut socket, lease.node()).await?;
                let acknowledged = read_id(&mut socket).await?;
                if acknowledged != local {
                    return Err(denied());
                }
                learn_intro(&weak, &hello.node, &hello.intro);
                Ok::<_, io::Error>((socket, lease, owner, receiver, cancelled))
            })
            .await;
            drop(permit);
            if let Ok(Ok((socket, lease, owner, receiver, cancelled))) = result {
                run_session(socket, lease, owner, receiver, cancelled).await;
            }
        });
    }

    async fn authorize(
        &self,
        hello: &Hello,
        remote: Option<std::net::SocketAddr>,
    ) -> io::Result<groupnet_transport::admission::AcceptedPeer> {
        let accepted = self
            .policy
            .admit(JoinRequest {
                claimed: &hello.node,
                credential: &hello.credential,
                remote,
            })
            .await?;
        if accepted.node != hello.node {
            return Err(denied());
        }
        Ok(accepted)
    }

    async fn dial_handshake(
        &self,
        socket: &mut TcpStream,
        local: &NodeId,
        intro: &str,
        peer: &NodeId,
        generation: u64,
    ) -> io::Result<SessionLease> {
        write_hello(socket, local, intro, &self.credential).await?;
        let hello = read_hello(socket).await?;
        let accepted_local = read_id(socket).await?;
        if accepted_local != *local || hello.node != *peer {
            return Err(denied());
        }
        let accepted = self.authorize(&hello, socket.peer_addr().ok()).await?;
        // A simultaneous inbound winner may have cancelled this provisional dial.
        let lease = {
            let mut connections = self.connections.lock().expect("connections lock poisoned");
            let Some(connection) = connections
                .peers
                .get_mut(peer)
                .filter(|connection| connection.generation == generation)
            else {
                return Err(denied());
            };
            let lease = self.sessions.try_admit(accepted)?;
            connection.session = Some(lease.id());
            connection.pending = false;
            lease
        };
        write_id(socket, lease.node()).await?;
        Ok(lease)
    }

    fn claim_inbound(
        self: &Arc<Self>,
        local: &NodeId,
        peer: &NodeId,
        queue: usize,
    ) -> io::Result<(Owner, mpsc::Receiver<Vec<u8>>, watch::Receiver<bool>)> {
        let mut connections = self.connections.lock().expect("connections lock poisoned");
        if let Some(existing) = connections.peers.get(peer) {
            // Only a provisional simultaneous outbound dial is replaceable. The
            // lexically lower identity's outbound socket wins at both endpoints.
            if !existing.pending || !existing.outbound || local.as_str() < peer.as_str() {
                return Err(denied());
            }
            let _ = existing.cancel.send(true);
        }
        let (frames, receiver) = mpsc::channel(queue);
        let (cancel, cancelled) = watch::channel(false);
        let generation = connections.next_generation;
        connections.next_generation = generation
            .checked_add(1)
            .ok_or_else(|| io::Error::other("TCP session generations exhausted"))?;
        connections.peers.insert(
            peer.clone(),
            Connection {
                generation,
                session: None,
                pending: true,
                outbound: false,
                frames,
                cancel,
            },
        );
        Ok((
            Owner {
                managed: self.clone(),
                peer: peer.clone(),
                generation,
            },
            receiver,
            cancelled,
        ))
    }
}

struct Owner {
    managed: Arc<Managed>,
    peer: NodeId,
    generation: u64,
}

impl Owner {
    fn admit(
        &self,
        accepted: groupnet_transport::admission::AcceptedPeer,
    ) -> io::Result<SessionLease> {
        let mut connections = self
            .managed
            .connections
            .lock()
            .expect("connections lock poisoned");
        let Some(connection) = connections
            .peers
            .get_mut(&self.peer)
            .filter(|connection| connection.generation == self.generation)
        else {
            return Err(denied());
        };
        let lease = self.managed.sessions.try_admit(accepted)?;
        connection.session = Some(lease.id());
        connection.pending = false;
        Ok(lease)
    }
}

impl Drop for Owner {
    fn drop(&mut self) {
        let mut connections = self
            .managed
            .connections
            .lock()
            .expect("connections lock poisoned");
        if connections
            .peers
            .get(&self.peer)
            .is_some_and(|c| c.generation == self.generation)
        {
            connections.peers.remove(&self.peer);
        }
    }
}

async fn run_pending(
    owner: Owner,
    inner: Weak<Inner>,
    handshake: impl Future<Output = io::Result<(TcpStream, SessionLease)>>,
    receiver: mpsc::Receiver<Vec<u8>>,
    mut cancelled: watch::Receiver<bool>,
    permit: OwnedSemaphorePermit,
) {
    let result = tokio::select! {
        result = timeout(owner.managed.config.handshake_timeout, handshake) => result,
        _ = cancelled.changed() => return,
    };
    drop(permit);
    if let Ok(Ok((socket, lease))) = result {
        if inner.upgrade().is_none() {
            return;
        }
        run_session(socket, lease, owner, receiver, cancelled).await;
    }
}

async fn run_session(
    socket: TcpStream,
    lease: SessionLease,
    owner: Owner,
    mut frames: mpsc::Receiver<Vec<u8>>,
    mut cancelled: watch::Receiver<bool>,
) {
    let Some(inbound) = owner
        .managed
        .inbound
        .lock()
        .expect("inbound lock poisoned")
        .clone()
    else {
        return;
    };
    let mut sessions = owner.managed.sessions.subscribe();
    let (mut reader, mut writer) = socket.into_split();
    let reading = async {
        while let Ok(Some(msg)) = super::read_frame(&mut reader).await {
            if !lease.is_active() {
                break;
            }
            let queued = QueuedInbound {
                packet: groupnet_transport::Inbound {
                    from: lease.node().clone(),
                    msg,
                },
                session: Some(lease.id()),
            };
            if inbound.send(queued).await.is_err() {
                break;
            }
        }
    };
    let writing = async {
        while let Some(frame) = frames.recv().await {
            if !lease.is_active() || writer.write_all(&frame).await.is_err() {
                break;
            }
        }
    };
    let revoked = async {
        loop {
            if !lease.is_active() || sessions.changed().await.is_err() {
                break;
            }
        }
    };
    tokio::select! {
        () = reading => {},
        () = writing => {},
        () = revoked => {},
        _ = cancelled.changed() => {},
    }
    // Owner and lease drop together on every exit/cancellation path. Queued
    // frames keep the producing generation, never a fresh identity-only lookup.
}

struct Hello {
    node: NodeId,
    intro: String,
    credential: Vec<u8>,
}

async fn write_hello(
    socket: &mut TcpStream,
    local: &NodeId,
    intro: &str,
    credential: &[u8],
) -> io::Result<()> {
    socket.write_all(MAGIC).await?;
    write_id(socket, local).await?;
    write_str(socket, intro).await?;
    let len = u32::try_from(credential.len()).map_err(|_| denied())?;
    socket.write_all(&len.to_be_bytes()).await?;
    socket.write_all(credential).await
}

async fn read_hello(socket: &mut TcpStream) -> io::Result<Hello> {
    let mut magic = [0; 8];
    socket.read_exact(&mut magic).await?;
    if &magic != MAGIC {
        return Err(denied());
    }
    let node = read_id(socket).await?;
    if node.as_str().is_empty() {
        return Err(denied());
    }
    let intro = read_str(socket).await?;
    let len = socket.read_u32().await? as usize;
    if len > MAX_CREDENTIAL_BYTES {
        return Err(denied());
    }
    let mut credential = vec![0; len];
    socket.read_exact(&mut credential).await?;
    Ok(Hello {
        node,
        intro,
        credential,
    })
}

fn learn_intro(inner: &Weak<Inner>, node: &NodeId, intro: &str) {
    if let Ok(address) = intro.parse::<std::net::SocketAddr>()
        && let Some(inner) = inner.upgrade()
    {
        inner
            .peers
            .write()
            .expect("peers lock poisoned")
            .insert(node.clone(), address);
    }
}

fn frame(msg: &[u8]) -> Vec<u8> {
    let mut framed = Vec::with_capacity(4 + msg.len());
    framed.extend_from_slice(
        &u32::try_from(msg.len())
            .expect("bounded frame")
            .to_be_bytes(),
    );
    framed.extend_from_slice(msg);
    framed
}

fn denied() -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, "TCP admission rejected")
}

#[cfg(test)]
#[path = "admission_tests.rs"]
mod tests;
