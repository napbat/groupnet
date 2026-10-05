//! End-to-end mutually authenticated TLS 1.3 streams over routed datagrams.
//!
//! Each stream has a native bounded sliding window (32 ciphertext packets),
//! cumulative acknowledgements, deduplication, reordering, receive credit and
//! retransmission with congestion backoff. Routing changes may retransmit the
//! same ciphertext through another adapter without changing the TLS identity.
//! There are at most 128 admitted peers, 64 sessions and 8 sessions per peer;
//! setup expires after ten seconds, while healthy idle sessions use heartbeats.
//! Revocation invalidates active and queued streams. Re-admission creates a new
//! admission generation and cannot restore an old stream's credentials.

mod reliable;
mod stream;
mod tls;
mod wire;

use std::{
    collections::HashMap,
    io,
    sync::{Arc, Mutex, Weak},
    time::Duration,
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

const MAX_PEERS: usize = 128;
const MAX_SESSIONS: usize = 64;
const PER_PEER: usize = 8;
const SETUP: Duration = Duration::from_secs(10);
const PREAMBLE: &[u8; 11] = b"GN-TUNNEL-1";

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
    state: Mutex<State>,
    incoming: mpsc::Sender<Accepted>,
    accepts: AsyncMutex<mpsc::Receiver<Accepted>>,
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
        tokio::runtime::Handle::try_current()
            .map_err(|_| error(io::ErrorKind::NotConnected, "Tokio executor required"))?;
        if peers.len() > MAX_PEERS {
            return Err(error(io::ErrorKind::InvalidInput, "too many TLS peers"));
        }
        let cancel = router.cancellation().child_token();
        let mut state = State::default();
        for peer in peers {
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
        let (incoming, accepts) = mpsc::channel(32);
        let inner = Arc::new(Inner {
            router: router.clone(),
            identity,
            state: Mutex::new(state),
            incoming,
            accepts: AsyncMutex::new(accepts),
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
    pub fn trust_peer(&self, peer: PeerIdentity) -> io::Result<()> {
        let mut state = self.inner.state.lock().map_err(|_| poisoned())?;
        if self.inner.cancel.is_cancelled() {
            return Err(closed());
        }
        if let Some(old) = state.peers.get(&peer.node) {
            if old.identity.pin == peer.pin {
                return Ok(());
            }
        } else if state.peers.len() >= MAX_PEERS {
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
    pub fn remove_peer(&self, node: &NodeId) -> bool {
        let Ok(mut state) = self.inner.state.lock() else {
            return false;
        };
        let Some(peer) = state.peers.remove(node) else {
            return false;
        };
        peer.cancel.cancel();
        true
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
    }
}

impl BulkTransport for TunnelTransport {
    type Error = io::Error;
    type Stream = TunneledStream;

    async fn connect(&self, to: &NodeId) -> io::Result<Self::Stream> {
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
            tls.flush().await?;
            let mut reply = [0; PREAMBLE.len()];
            tls.read_exact(&mut reply).await?;
            if &reply != PREAMBLE {
                return Err(error(
                    io::ErrorKind::InvalidData,
                    "invalid authenticated preamble",
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
            result = timeout(SETUP, operation) => result.map_err(|_| error(io::ErrorKind::TimedOut, "TLS tunnel setup deadline"))?,
        };
        if result.is_ok() {
            guard.0 = None;
        }
        result
    }

    async fn accept(&self) -> io::Result<(NodeId, Self::Stream)> {
        let mut queue = tokio::select! {
            () = self.inner.cancel.cancelled() => return Err(closed()),
            queue = self.inner.accepts.lock() => queue,
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
    if state.sessions.len() >= MAX_SESSIONS
        || state
            .sessions
            .keys()
            .filter(|(node, _)| node == &peer)
            .count()
            >= PER_PEER
        || state.sessions.contains_key(&(peer.clone(), id))
    {
        return Err(error(
            io::ErrorKind::WouldBlock,
            "TLS session capacity exceeded",
        ));
    }
    let (packets, receiver) = mpsc::channel(64);
    let cancel = admission.cancel.child_token();
    let sent = CancellationToken::new();
    let (stream, raw) = tokio::io::duplex(32 * 1024);
    state
        .sessions
        .insert((peer.clone(), id), Session { packets });
    let weak = Arc::downgrade(inner);
    let router = inner.router.clone();
    let task_cancel = cancel.clone();
    let task_sent = sent.clone();
    inner.tasks.spawn(async move {
        reliable::run(
            router,
            peer.clone(),
            id,
            role,
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
        let Some(packet) = Packet::decode(inbound.msg) else {
            continue;
        };
        let Some(inner) = weak.upgrade() else {
            break;
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
        tls.write_all(PREAMBLE).await?;
        tls.flush().await?;
        let inner = weak.upgrade().ok_or_else(closed)?;
        ensure_admitted(&inner, &admission)?;
        let stream = TunneledStream::new(TlsStream::Server(tls), cancel.clone(), sent);
        inner
            .incoming
            .try_send(Accepted { admission, stream })
            .map_err(|_| error(io::ErrorKind::WouldBlock, "TLS accept queue full"))
    };
    let result = tokio::select! {
        () = cancel.cancelled() => return,
        result = timeout(SETUP, operation) => result,
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
