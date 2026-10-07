//! End-to-end mutually authenticated TLS 1.3 streams over routed datagrams.
//!
//! Each stream has a configured bounded ciphertext sliding window, cumulative
//! acknowledgements, deduplication, reordering, reserved byte credit and
//! retransmission with slow-start congestion control. Routing changes may
//! retransmit the same ciphertext through another adapter without changing the
//! TLS identity. Node-wide [`TunnelLimits`](crate::tunnel::TunnelLimits) bound
//! admission, setup, sessions, queues, retained ciphertext memory and idle
//! expiry. Each endpoint chooses its own send segment and windows; every
//! endpoint accepts segments up to the protocol-wide
//! [`SegmentSize::MAX`](crate::tunnel::SegmentSize::MAX).
//! Revocation invalidates active and queued streams. Re-admission creates a new
//! admission generation and cannot restore an old stream's credentials.
//!
//! Pinned TLS authenticates the protocol-tagged setup preamble before delivery:
//! bulk ordered streams and secure datagram session controls have independent
//! bounded accept queues. Unknown namespaces fail closed. Exported session keys
//! remain tied to the retained control stream's admission and cancellation.

mod budget;
mod config;
mod pipe;
mod reliable;
mod stream;
mod tls;
mod wire;

pub(crate) use config::DEFAULT_STREAM_FRAMES;
pub use config::{RetransmitTimeouts, SegmentSize, TunnelLimits};

#[cfg(test)]
mod tests;

use std::{
    collections::HashMap,
    io,
    sync::{Arc, Mutex, Weak},
};

use groupnet_core::NodeId;
use groupnet_transport::bulk::BulkTransport;
use ring::rand::{SecureRandom, SystemRandom};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{Mutex as AsyncMutex, mpsc},
    time::timeout,
};
use tokio_rustls::{TlsAcceptor, TlsConnector, TlsStream};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::Router;
use bytes::Bytes;
use wire::{Kind, Packet, SessionId};

pub use stream::TunneledStream;
pub use tls::{PeerIdentity, TlsIdentity};

/// Authenticated preamble; its digit is the tunnel protocol [`wire::VERSION`].
const PREAMBLE: &[u8; 11] = b"GN-TUNNEL-3";

const _: () = assert!(PREAMBLE[10] == b'0' + wire::VERSION);

const ORDERED: u16 = 1;
const CONTROL: u16 = 2;

#[derive(Debug)]
struct Admission {
    identity: PeerIdentity,
    cancel: CancellationToken,
}

#[derive(Debug)]
struct Session {
    packets: mpsc::Sender<Packet>,
}

#[derive(Debug, Default)]
struct State {
    peers: HashMap<NodeId, Arc<Admission>>,
    sessions: HashMap<(NodeId, SessionId), Session>,
}

#[derive(Debug)]
struct Accepted {
    admission: Arc<Admission>,
    stream: TunneledStream,
}

#[derive(Debug)]
struct Inner {
    router: Router,
    identity: TlsIdentity,
    limits: TunnelLimits,
    budget: Arc<budget::MemoryBudget>,
    state: Mutex<State>,
    incoming: mpsc::Sender<Accepted>,
    accepts: AsyncMutex<mpsc::Receiver<Accepted>>,
    control_incoming: mpsc::Sender<Accepted>,
    control_accepts: AsyncMutex<mpsc::Receiver<Accepted>>,
    cancel: CancellationToken,
    tasks: TaskTracker,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// A bounded reliable bulk transport whose streams are authenticated end to end.
/// Clones share admission, queues and session ownership. The last handle's drop
/// cancels all tasks; [`close`](Self::close) additionally waits for task shutdown.
#[derive(Clone, Debug)]
pub struct TunnelTransport {
    inner: Arc<Inner>,
}

impl TunnelTransport {
    /// Claims this router's tunnel plane and starts its bounded dispatcher.
    ///
    /// # Errors
    /// Rejects duplicate peers, oversized admission or an already-claimed router.
    /// Requires a running Tokio executor.
    pub fn new(
        router: Router,
        identity: TlsIdentity,
        peers: Vec<PeerIdentity>,
    ) -> io::Result<Self> {
        Self::with_limits(router, identity, peers, TunnelLimits::default())
    }

    /// Claims the tunnel plane using explicit bounded resource/reliability policy.
    /// # Errors
    /// Rejects invalid limits, admission, or an already-claimed router.
    pub fn with_limits(
        router: Router,
        identity: TlsIdentity,
        peers: Vec<PeerIdentity>,
        limits: TunnelLimits,
    ) -> io::Result<Self> {
        limits.validate()?;
        tokio::runtime::Handle::try_current()
            .map_err(|_| error(io::ErrorKind::NotConnected, "Tokio executor required"))?;
        if peers.len() > limits.max_peers.get() {
            return Err(error(io::ErrorKind::InvalidInput, "too many TLS peers"));
        }
        let cancel = router.cancellation();
        let mut state = State::default();
        for peer in peers {
            router.validate_tunnel_payload(&peer.node, limits.payload.get() + wire::HEADER)?;
            let node = peer.node.clone();
            if state
                .peers
                .insert(
                    node,
                    Arc::new(Admission {
                        identity: peer,
                        cancel: cancel.child_token(),
                    }),
                )
                .is_some()
            {
                return Err(error(io::ErrorKind::InvalidInput, "duplicate TLS peer"));
            }
        }
        let (incoming, accepts) = mpsc::channel(limits.accept_queue.get());
        let (control_incoming, control_accepts) = mpsc::channel(limits.accept_queue.get());
        let inner = Arc::new(Inner {
            router,
            identity,
            budget: budget::MemoryBudget::new(&limits),
            limits,
            state: Mutex::new(state),
            incoming,
            accepts: AsyncMutex::new(accepts),
            control_incoming,
            control_accepts: AsyncMutex::new(control_accepts),
            cancel,
            tasks: TaskTracker::new(),
        });
        inner
            .router
            .claim_tunnels(Arc::new(Dispatcher(Arc::downgrade(&inner))))?;
        Ok(Self { inner })
    }

    /// Admits a peer or replaces its pin, cancelling sessions under the old pin.
    /// Re-registering an unchanged admitted pin preserves that generation.
    ///
    /// # Errors
    /// Returns an error if closed, the state lock was poisoned, or admission is full.
    pub fn admit_peer(&self, peer: PeerIdentity) -> io::Result<()> {
        self.inner
            .router
            .validate_tunnel_payload(&peer.node, self.inner.limits.payload.get() + wire::HEADER)?;
        let mut state = self.inner.state.lock().map_err(|_| poisoned())?;
        if self.inner.cancel.is_cancelled() {
            return Err(closed());
        }
        if let Some(old) = state.peers.get(&peer.node) {
            if old.identity.pin == peer.pin {
                return Ok(());
            }
        } else if state.peers.len() >= self.inner.limits.max_peers.get() {
            return Err(error(io::ErrorKind::WouldBlock, "TLS admission full"));
        }
        let node = peer.node.clone();
        if let Some(old) = state.peers.insert(
            node,
            Arc::new(Admission {
                identity: peer,
                cancel: self.inner.cancel.child_token(),
            }),
        ) {
            old.cancel.cancel();
        }
        Ok(())
    }

    /// Revokes the current admission and immediately invalidates active and queued streams.
    #[must_use]
    pub fn revoke_peer(&self, node: &NodeId) -> bool {
        let Ok(mut state) = self.inner.state.lock() else {
            return false;
        };
        let Some(peer) = state.peers.remove(node) else {
            return false;
        };
        peer.cancel.cancel();
        true
    }

    /// Connects an authenticated session-setup channel, isolated from bulk streams.
    ///
    /// This channel is for secure datagram session negotiation and exporter key
    /// derivation, not the unordered payload plane. The namespace is exchanged
    /// inside pinned TLS before either endpoint receives the stream.
    ///
    /// # Errors
    /// Returns an error for missing admission, capacity, failed authentication,
    /// revocation, shutdown, or the bounded setup deadline.
    pub async fn connect_control(&self, to: &NodeId) -> io::Result<TunneledStream> {
        self.connect_namespace(to, CONTROL).await
    }

    /// Accepts only authenticated session-setup channels, never bulk streams.
    ///
    /// # Errors
    /// Returns an error when the transport closes or its state lock is poisoned.
    pub async fn accept_control(&self) -> io::Result<(NodeId, TunneledStream)> {
        self.accept_namespace(&self.inner.control_accepts).await
    }

    /// Returns a child token notified by transport or node shutdown.
    ///
    /// Cancelling the returned token does not close the shared transport.
    #[must_use]
    pub fn cancellation(&self) -> CancellationToken {
        self.inner.cancel.child_token()
    }

    /// Tunnel ciphertext bytes currently reserved against
    /// [`TunnelLimits::memory_budget`]: every live session's floors plus growth
    /// lent above them.
    #[must_use]
    pub fn reserved_memory(&self) -> usize {
        self.inner.budget.in_use()
    }

    /// Cancels sessions and queued accepts, then waits for all owned tasks to stop.
    pub async fn close(&self) {
        {
            let _state = self.inner.state.lock();
            self.inner.cancel.cancel();
            self.inner.tasks.close();
        }
        self.inner.tasks.wait().await;
        self.inner.accepts.lock().await.close();
        let mut queue = self.inner.accepts.lock().await;
        while queue.try_recv().is_ok() {}
        drop(queue);
        let mut queue = self.inner.control_accepts.lock().await;
        queue.close();
        while queue.try_recv().is_ok() {}
    }
}

impl BulkTransport for TunnelTransport {
    type Error = io::Error;
    type Stream = TunneledStream;

    async fn connect(&self, to: &NodeId) -> io::Result<Self::Stream> {
        self.connect_namespace(to, ORDERED).await
    }

    async fn accept(&self) -> io::Result<(NodeId, Self::Stream)> {
        self.accept_namespace(&self.inner.accepts).await
    }
}

impl TunnelTransport {
    async fn connect_namespace(&self, to: &NodeId, namespace: u16) -> io::Result<TunneledStream> {
        let admission = self
            .inner
            .state
            .lock()
            .map_err(|_| poisoned())?
            .peers
            .get(to)
            .cloned()
            .ok_or_else(|| error(io::ErrorKind::PermissionDenied, "TLS peer not admitted"))?;
        let mut id = [0; 16];
        SystemRandom::new()
            .fill(&mut id)
            .map_err(|_| io::Error::other("session randomness unavailable"))?;
        let (raw, cancel, sent) = start_session(
            &self.inner,
            &admission,
            SessionId(id),
            reliable::Role::Initiator,
        )?;
        let mut guard = CancelOnDrop(Some(cancel.clone()));
        let operation = async {
            let name = rustls::pki_types::ServerName::try_from("groupnet.peer")
                .map_err(|_| error(io::ErrorKind::InvalidInput, "invalid TLS server name"))?;
            let mut tls = TlsConnector::from(self.inner.identity.client.clone())
                .connect(name, raw)
                .await?;
            let (_, connection) = tls.get_ref();
            tls::verify(
                &admission.identity,
                connection.peer_certificates(),
                connection.alpn_protocol(),
            )?;
            tls.write_all(PREAMBLE).await?;
            tls.write_all(&namespace.to_be_bytes()).await?;
            tls.flush().await?;
            let mut reply = [0; PREAMBLE.len()];
            tls.read_exact(&mut reply).await?;
            if &reply != PREAMBLE {
                return Err(error(
                    io::ErrorKind::InvalidData,
                    "invalid authenticated preamble",
                ));
            }
            let mut reply_namespace = [0; 2];
            tls.read_exact(&mut reply_namespace).await?;
            if u16::from_be_bytes(reply_namespace) != namespace {
                return Err(error(
                    io::ErrorKind::InvalidData,
                    "invalid tunnel namespace",
                ));
            }
            ensure_admitted(&self.inner, &admission)?;
            Ok(TunneledStream::new(
                TlsStream::Client(tls),
                cancel.clone(),
                sent,
            ))
        };
        let result = tokio::select! {
            () = cancel.cancelled() => Err(closed()),
            result = timeout(self.inner.limits.setup_timeout, operation) => result.map_err(|_| error(io::ErrorKind::TimedOut, "TLS tunnel setup deadline"))?,
        };
        if result.is_ok() {
            guard.0 = None;
        }
        result
    }

    async fn accept_namespace(
        &self,
        accepts: &AsyncMutex<mpsc::Receiver<Accepted>>,
    ) -> io::Result<(NodeId, TunneledStream)> {
        let mut queue = tokio::select! {
            () = self.inner.cancel.cancelled() => return Err(closed()),
            queue = accepts.lock() => queue,
        };
        loop {
            let accepted = tokio::select! {
                () = self.inner.cancel.cancelled() => return Err(closed()),
                accepted = queue.recv() => accepted.ok_or_else(closed)?,
            };
            if ensure_admitted(&self.inner, &accepted.admission).is_ok() {
                return Ok((accepted.admission.identity.node.clone(), accepted.stream));
            }
        }
    }
}

#[derive(Debug)]
struct CancelOnDrop(Option<CancellationToken>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if let Some(cancel) = &self.0 {
            cancel.cancel();
        }
    }
}

fn ensure_admitted(inner: &Inner, admission: &Arc<Admission>) -> io::Result<()> {
    let state = inner.state.lock().map_err(|_| poisoned())?;
    if admission.cancel.is_cancelled()
        || !state
            .peers
            .get(&admission.identity.node)
            .is_some_and(|current| Arc::ptr_eq(current, admission))
    {
        return Err(error(
            io::ErrorKind::PermissionDenied,
            "TLS admission revoked",
        ));
    }
    Ok(())
}

fn start_session(
    inner: &Arc<Inner>,
    admission: &Arc<Admission>,
    id: SessionId,
    role: reliable::Role,
) -> io::Result<(pipe::TlsEnd, CancellationToken, CancellationToken)> {
    let mut state = inner.state.lock().map_err(|_| poisoned())?;
    let peer = admission.identity.node.clone();
    if inner.cancel.is_cancelled()
        || admission.cancel.is_cancelled()
        || !state
            .peers
            .get(&peer)
            .is_some_and(|current| Arc::ptr_eq(current, admission))
    {
        return Err(closed());
    }
    if state.sessions.len() >= inner.limits.max_sessions.get()
        || state
            .sessions
            .keys()
            .filter(|(node, _)| node == &peer)
            .count()
            >= inner.limits.sessions_per_peer.get()
        || state.sessions.contains_key(&(peer.clone(), id))
    {
        return Err(error(
            io::ErrorKind::WouldBlock,
            "TLS session capacity exceeded",
        ));
    }
    let (packets, receiver) = mpsc::channel(inner.limits.packet_queue().get());
    // The session count bound above keeps every floor within the set-aside budget.
    let floor = inner.limits.min_window.get() as usize;
    let send_memory = inner.budget.reserve(floor);
    let receive_memory = inner.budget.reserve(floor);
    let cancel = admission.cancel.child_token();
    let sent = CancellationToken::new();
    let (stream, segments) = pipe::pair(
        inner.router.tunnel_buffers(&peer),
        inner.limits.stream_buffer.get(),
        inner.limits.payload.get(),
    );
    state
        .sessions
        .insert((peer.clone(), id), Session { packets });
    let weak = Arc::downgrade(inner);
    let router = inner.router.clone();
    let task_cancel = cancel.clone();
    let task_sent = sent.clone();
    let limits = inner.limits.clone();
    inner.tasks.spawn(async move {
        reliable::run(
            router,
            peer.clone(),
            id,
            role,
            limits,
            reliable::SessionIo {
                pipe: segments,
                packets: receiver,
                cancel: task_cancel,
                sent: task_sent,
                send_memory,
                receive_memory,
            },
        )
        .await;
        if let Some(inner) = weak.upgrade()
            && let Ok(mut state) = inner.state.lock()
        {
            state.sessions.remove(&(peer, id));
        }
    });
    Ok((stream, cancel, sent))
}

/// Routes each arriving tunnel packet to its session's bounded queue, or opens
/// a responder session for an admitted peer's `Open`. Runs inline on the link
/// worker that received the packet; a full session queue drops the packet as loss.
#[derive(Debug)]
struct Dispatcher(Weak<Inner>);

impl crate::router::TunnelInbox for Dispatcher {
    fn deliver(&self, from: NodeId, payload: Bytes) {
        let Some(inner) = self.0.upgrade() else {
            return;
        };
        if inner.cancel.is_cancelled() {
            return;
        }
        let Some(packet) = Packet::decode(payload) else {
            return;
        };
        let admission = {
            let Ok(state) = inner.state.lock() else {
                inner.cancel.cancel();
                return;
            };
            if let Some(session) = state.sessions.get(&(from.clone(), packet.id)) {
                let _ = session.packets.try_send(packet);
                return;
            }
            if packet.kind != Kind::Open {
                return;
            }
            let Some(admission) = state.peers.get(&from).cloned() else {
                return;
            };
            admission
        };
        if let Ok((raw, session_cancel, sent)) =
            start_session(&inner, &admission, packet.id, reliable::Role::Responder)
        {
            let server = inner.identity.server.clone();
            let weak = Arc::downgrade(&inner);
            inner.tasks.spawn(authenticate_inbound(
                weak,
                server,
                admission,
                raw,
                session_cancel,
                sent,
            ));
        }
    }
}

async fn authenticate_inbound(
    weak: Weak<Inner>,
    server: Arc<rustls::ServerConfig>,
    admission: Arc<Admission>,
    raw: pipe::TlsEnd,
    cancel: CancellationToken,
    sent: CancellationToken,
) {
    let mut guard = CancelOnDrop(Some(cancel.clone()));
    let Some(inner) = weak.upgrade() else {
        return;
    };
    let setup_timeout = inner.limits.setup_timeout;
    drop(inner);
    let operation = async {
        let mut tls = TlsAcceptor::from(server).accept(raw).await?;
        let (_, connection) = tls.get_ref();
        tls::verify(
            &admission.identity,
            connection.peer_certificates(),
            connection.alpn_protocol(),
        )?;
        let mut preamble = [0; PREAMBLE.len()];
        tls.read_exact(&mut preamble).await?;
        if &preamble != PREAMBLE {
            return Err(error(
                io::ErrorKind::InvalidData,
                "invalid authenticated preamble",
            ));
        }
        let mut namespace = [0; 2];
        tls.read_exact(&mut namespace).await?;
        let inner = weak.upgrade().ok_or_else(closed)?;
        let incoming = match u16::from_be_bytes(namespace) {
            ORDERED => &inner.incoming,
            CONTROL => &inner.control_incoming,
            _ => {
                return Err(error(
                    io::ErrorKind::InvalidData,
                    "unknown tunnel namespace",
                ));
            }
        };
        ensure_admitted(&inner, &admission)?;
        tls.write_all(PREAMBLE).await?;
        tls.write_all(&namespace).await?;
        tls.flush().await?;
        let stream = TunneledStream::new(TlsStream::Server(tls), cancel.clone(), sent);
        incoming
            .try_send(Accepted { admission, stream })
            .map_err(|_| error(io::ErrorKind::WouldBlock, "TLS accept queue full"))
    };
    let result = tokio::select! {
        () = cancel.cancelled() => return,
        result = timeout(setup_timeout, operation) => result,
    };
    if matches!(result, Ok(Ok(()))) {
        guard.0 = None;
    }
}

fn error(kind: io::ErrorKind, message: &'static str) -> io::Error {
    io::Error::new(kind, message)
}

fn closed() -> io::Error {
    error(io::ErrorKind::ConnectionAborted, "tunnel transport closed")
}

fn poisoned() -> io::Error {
    io::Error::other("tunnel state lock poisoned")
}
