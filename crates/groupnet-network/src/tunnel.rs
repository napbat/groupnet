//! End-to-end mutually authenticated TLS 1.3 streams over routed datagrams.
//!
//! Each stream has a configured bounded ciphertext sliding window, cumulative
//! acknowledgements, deduplication, reordering, receive credit and retransmission
//! with congestion backoff. Routing changes may retransmit the same ciphertext
//! through another adapter without changing the TLS identity. Node-wide
//! [`TunnelLimits`](crate::tunnel::TunnelLimits) bound admission, setup, sessions, queues and idle expiry.
//! Revocation invalidates active and queued streams. Re-admission creates a new
//! admission generation and cannot restore an old stream's credentials.
//!
//! Pinned TLS authenticates the protocol-tagged setup preamble before delivery:
//! bulk ordered streams and secure datagram session controls have independent
//! bounded accept queues. Unknown namespaces fail closed. Exported session keys
//! remain tied to the retained control stream's admission and cancellation.

mod config;
mod reliable;
mod stream;
mod tls;
mod wire;

pub use config::TunnelLimits;

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
    io::{AsyncReadExt, AsyncWriteExt, DuplexStream},
    sync::{Mutex as AsyncMutex, mpsc},
    time::timeout,
};
use tokio_rustls::{TlsAcceptor, TlsConnector, TlsStream};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::Router;
use wire::{Kind, Packet, SessionId};

pub use stream::TunneledStream;
pub use tls::{PeerIdentity, TlsIdentity};

const PREAMBLE: &[u8; 11] = b"GN-TUNNEL-2";
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
        if peers.len() > limits.max_peers {
            return Err(error(io::ErrorKind::InvalidInput, "too many TLS peers"));
        }
        let cancel = router.cancellation();
        let mut state = State::default();
        for peer in peers {
            router.validate_tunnel_payload(&peer.node, limits.payload + wire::HEADER)?;
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
        router.claim_tunnels()?;
        let (incoming, accepts) = mpsc::channel(limits.accept_queue);
        let (control_incoming, control_accepts) = mpsc::channel(limits.accept_queue);
        let inner = Arc::new(Inner {
            router: router.clone(),
            identity,
            limits,
            state: Mutex::new(state),
            incoming,
            accepts: AsyncMutex::new(accepts),
            control_incoming,
            control_accepts: AsyncMutex::new(control_accepts),
            cancel: cancel.clone(),
            tasks: TaskTracker::new(),
        });
        inner
            .tasks
            .spawn(dispatch(Arc::downgrade(&inner), router, cancel));
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
            .validate_tunnel_payload(&peer.node, self.inner.limits.payload + wire::HEADER)?;
        let mut state = self.inner.state.lock().map_err(|_| poisoned())?;
        if self.inner.cancel.is_cancelled() {
            return Err(closed());
        }
        if let Some(old) = state.peers.get(&peer.node) {
            if old.identity.pin == peer.pin {
                return Ok(());
            }
        } else if state.peers.len() >= self.inner.limits.max_peers {
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
) -> io::Result<(DuplexStream, CancellationToken, CancellationToken)> {
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
    if state.sessions.len() >= inner.limits.max_sessions
        || state
            .sessions
            .keys()
            .filter(|(node, _)| node == &peer)
            .count()
            >= inner.limits.sessions_per_peer
        || state.sessions.contains_key(&(peer.clone(), id))
    {
        return Err(error(
            io::ErrorKind::WouldBlock,
            "TLS session capacity exceeded",
        ));
    }
    let (packets, receiver) = mpsc::channel(inner.limits.packet_queue);
    let cancel = admission.cancel.child_token();
    let sent = CancellationToken::new();
    let (stream, raw) = tokio::io::duplex(inner.limits.stream_buffer);
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
                raw,
                packets: receiver,
                cancel: task_cancel,
                sent: task_sent,
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

async fn dispatch(weak: Weak<Inner>, router: Router, cancel: CancellationToken) {
    loop {
        let inbound = tokio::select! {
            () = cancel.cancelled() => break,
            inbound = router.recv_tunnel() => match inbound { Ok(inbound) => inbound, Err(_) => break },
        };
        let Some(inner) = weak.upgrade() else {
            break;
        };
        let Some(packet) = Packet::decode(inbound.msg, &inner.limits) else {
            continue;
        };
        let (existing, admission) = {
            let Ok(state) = inner.state.lock() else {
                break;
            };
            (
                state
                    .sessions
                    .get(&(inbound.from.clone(), packet.id))
                    .map(|session| session.packets.clone()),
                state.peers.get(&inbound.from).cloned(),
            )
        };
        if let Some(existing) = existing {
            let _ = existing.try_send(packet);
        } else if packet.kind == Kind::Open {
            let Some(admission) = admission else {
                continue;
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
    cancel.cancel();
}

async fn authenticate_inbound(
    weak: Weak<Inner>,
    server: Arc<rustls::ServerConfig>,
    admission: Arc<Admission>,
    raw: DuplexStream,
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
