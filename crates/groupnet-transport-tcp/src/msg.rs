//! Control-plane messaging over persistent TCP connections; the public
//! contract is documented on [`TcpMsgTransport`].

use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::Duration;

use bytes::Bytes;
use groupnet_core::NodeId;
use groupnet_transport::Inbound;
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::mpsc;
use tokio::time::timeout;

use crate::handshake::{check_id, read_intro, write_intro};
use crate::tasks::Tasks;

mod addresses;
#[cfg(feature = "link")]
mod admitted;
mod config;
#[cfg(feature = "connectivity")]
mod connectivity;
mod endpoint;
mod framing;

use addresses::AddressBook;
#[cfg(feature = "link")]
pub use admitted::{TcpAdmission, TcpAdmissionConfig, TcpAdmissionError, TcpConnectPeerError};
pub use config::{TcpMsgConfig, TcpMsgConfigError};
#[cfg(feature = "connectivity")]
pub use connectivity::TcpPathChanges;
pub use endpoint::TcpRegisterPeerError;
use framing::{FrameReader, extend_ready, next_batch, write_frames};

#[derive(Debug)]
struct QueuedInbound {
    packet: Inbound,
    #[cfg(feature = "link")]
    session: Option<groupnet_transport::admission::SessionId>,
}

/// Where reader tasks hand inbound frames: the network's synchronous sink once
/// a link worker attached one, otherwise the bounded inbox. Either way a reader
/// reads no further input until its frame is handed over.
#[derive(Clone, Debug)]
struct InboundPath {
    queue: mpsc::Sender<QueuedInbound>,
    #[cfg(feature = "link")]
    direct: Arc<std::sync::OnceLock<groupnet_transport::link::InboundSink>>,
}

impl InboundPath {
    /// Hands over one frame; false once the transport's inbox is gone.
    async fn deliver(&self, packet: QueuedInbound) -> bool {
        #[cfg(feature = "link")]
        if let Some(sink) = self.direct.get() {
            if self.queue.is_closed() {
                return false;
            }
            sink.deliver(groupnet_transport::link::AdmittedInbound {
                packet: packet.packet,
                session: packet.session,
            });
            // Charge the scheduler's cooperative budget as the queue send it
            // replaces did: a reader on a busy socket otherwise runs on while
            // the task its delivery woke waits, unstealable, behind it.
            tokio::task::consume_budget().await;
            return true;
        }
        self.queue.send(packet).await.is_ok()
    }
}

/// How long a dial may take before the connection attempt is abandoned.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// An outbound connection's handle in the pool: the frame queue plus a
/// generation stamp so a writer task only ever removes *its own* entry.
#[derive(Debug)]
struct Conn {
    generation: u64,
    frames: mpsc::Sender<Bytes>,
}

/// The outbound connection pool. Invariant: `order` holds exactly one
/// `(generation, node)` pair per live entry in `conns`.
#[derive(Debug, Default)]
struct Pool {
    next_generation: u64,
    conns: HashMap<NodeId, Conn>,
    /// Dial order, for oldest-first eviction at the cap.
    order: VecDeque<(u64, NodeId)>,
}

impl Pool {
    /// Removes `node`'s entry regardless of generation.
    fn remove(&mut self, node: &NodeId) {
        if let Some(conn) = self.conns.remove(node) {
            let generation = conn.generation;
            self.order.retain(|(g, _)| *g != generation);
        }
    }

    /// Removes `node`'s entry only if it still belongs to `generation` — a
    /// writer task must not remove the fresher connection that replaced it.
    fn remove_generation(&mut self, node: &NodeId, generation: u64) {
        if self
            .conns
            .get(node)
            .is_some_and(|c| c.generation == generation)
        {
            self.conns.remove(node);
            self.order.retain(|(g, _)| *g != generation);
        }
    }
}

#[derive(Debug)]
struct Inner {
    local: NodeId,
    local_addr: SocketAddr,
    /// What we introduce ourselves with when dialing (empty: nothing
    /// dialable — bound to an unspecified address with no `advertise`).
    intro: String,
    config: TcpMsgConfig,
    /// Explicit contacts and, for managed endpoints, a bounded learned book.
    /// One lock covers registration, learning and lookup without lock inversion.
    peers: RwLock<AddressBook>,
    pool: Mutex<Pool>,
    inbox: AsyncMutex<mpsc::Receiver<QueuedInbound>>,
    /// The network's synchronous sink, attached once by a link worker.
    #[cfg(feature = "link")]
    direct: Arc<std::sync::OnceLock<groupnet_transport::link::InboundSink>>,
    #[cfg(feature = "link")]
    admission: Option<Arc<admitted::Managed>>,
    tasks: Arc<Tasks>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.tasks.shutdown();
    }
}

/// A persistent-connection TCP endpoint for the control plane.
///
/// It implements the best-effort, message-oriented
/// [`Transport`](groupnet_transport::Transport) contract on pooled,
/// long-lived connections instead of datagrams — the constant-connection
/// option for clusters that want frames (gossip, eager delta push) delivered
/// at network latency over reliable links, without changing the engine or
/// the protocol:
///
/// * **Lazy.** A connection is dialed the first time a frame is addressed to
///   a peer, and reused for every frame after that.
/// * **Bounded.** At most [`TcpMsgConfig::max_outbound`] outbound connections
///   exist at once (dialing past the cap closes the oldest), and a connection
///   with nothing to send for [`TcpMsgConfig::idle_timeout`] closes itself.
///   The pool therefore follows the peers this node is *actively* exchanging
///   with — on a large cluster that is the rotating gossip/anti-entropy
///   fanout, a handful of warm sockets, never one per member. Persistent
///   connections are a per-deployment choice made here at the transport
///   layer; nothing forces them on a deployment that prefers datagrams.
/// * **Still best-effort.** TCP orders bytes *within* one connection, but the
///   transport keeps the datagram contract: frames to an unknown or dead peer
///   are dropped, a full per-peer queue drops the frame, and a connection
///   failure drops whatever was queued behind it. The engine's anti-entropy
///   repairs all of it — do not add reliability on top.
///
/// # Address learning
///
/// Only seed addresses need registering up front
/// ([`register_peer`](Self::register_peer)). The rest of the book fills
/// itself in two ways: the dial handshake carries the dialer's own listener
/// address, which a raw endpoint reports with that connection's frames as
/// [`Inbound::source`](groupnet_transport::Inbound::source) and records once
/// the consumer confirms it through
/// [`Transport::confirm_peer`](groupnet_transport::Transport::confirm_peer)
/// (the router does after the frame authenticates), so the accepting side can
/// dial back a peer nobody registered on it (a joiner reaching a seed); and
/// gossiped `advertise_addr` values arrive through
/// [`Transport::learn_peer`](groupnet_transport::Transport::learn_peer)
/// (the runtime feeds them automatically), which resolves third parties.
/// Between them, a cluster bootstraps from seed addresses alone.
///
/// # Trusted low-level endpoint
///
/// A connection's claimed id and listener address are trusted as-is — the
/// same trust model as UDP source-address attribution. Inbound and outbound
/// are separate sockets — two mutually chatty nodes hold two connections,
/// not one full-duplex one. A failed dial drops the frames queued behind it
/// and the next send re-dials, which self-limits to one connect attempt per
/// burst of sends.
///
/// With `link`, `bind_admitted` (configured by a `TcpAdmission`) instead uses
/// bounded application admission and a full-duplex socket for each live
/// identity. Managed sockets are not subject to raw idle/oldest-first
/// eviction: dropping an idle writer must not withdraw an admitted neighbor
/// or prevent a server from reaching a joiner behind NAT. Its handshake and
/// gossip-learned addresses share a bounded book capped by
/// `TcpAdmissionConfig::max_peers`; learning refreshes contact age and evicts
/// the least recently learned contact. Explicitly registered bootstrap
/// contacts are retained separately and cannot be overwritten by learned
/// addresses.
///
/// With `connectivity`, `bind_connectivity` owns a native TCP connection with
/// admitted candidate traversal and relay fallback instead of a raw
/// connection pool. Address hints cannot bypass native admission.
///
/// # Handles
///
/// Cheap to [`Clone`]: clones share the listener, the address book, and the
/// connection pool, so a [`register_peer`](Self::register_peer) through any
/// handle is visible to all.
/// Dropping the last handle cancels every owned task. Call [`close`](Self::close)
/// when the listener and session sockets must be fully released before continuing.
#[derive(Clone, Debug)]
pub struct TcpMsgTransport {
    backend: Backend,
}

#[derive(Clone, Debug)]
enum Backend {
    Direct(Arc<Inner>),
    #[cfg(feature = "connectivity")]
    Connectivity {
        connection: groupnet_transport_punch::TcpConnection,
    },
}

impl TcpMsgTransport {
    /// Binds a listening socket for `local` with the default
    /// [`TcpMsgConfig`]. Register peers with
    /// [`register_peer`](Self::register_peer) before sending.
    ///
    /// # Errors
    /// Returns `InvalidInput` for a local id that is empty or longer than
    /// [`MAX_NODE_ID_BYTES`](groupnet_transport::MAX_NODE_ID_BYTES), and
    /// propagates any socket bind error.
    pub async fn bind(local: NodeId, addr: impl ToSocketAddrs) -> io::Result<Self> {
        Self::bind_with(local, addr, TcpMsgConfig::default()).await
    }

    /// Binds with explicit tuning. Must be called within a Tokio runtime —
    /// the accept loop and per-connection workers run as spawned tasks.
    ///
    /// # Errors
    /// Returns `InvalidInput` for invalid bounds or a local id outside the
    /// handshake bound, and propagates socket bind errors.
    pub async fn bind_with(
        local: NodeId,
        addr: impl ToSocketAddrs,
        config: TcpMsgConfig,
    ) -> io::Result<Self> {
        Self::bind_inner(
            local,
            addr,
            config,
            #[cfg(feature = "link")]
            None,
        )
        .await
    }

    async fn bind_inner(
        local: NodeId,
        addr: impl ToSocketAddrs,
        config: TcpMsgConfig,
        #[cfg(feature = "link")] admission: Option<Arc<admitted::Managed>>,
    ) -> io::Result<Self> {
        config.validate()?;
        check_id(&local)?;
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        let (inbound_tx, inbound_rx) = mpsc::channel(config.inbound_queue.get());
        let inbound = InboundPath {
            queue: inbound_tx,
            #[cfg(feature = "link")]
            direct: Arc::default(),
        };
        let read_idle = config.idle_timeout.saturating_mul(2);
        let intro = config
            .advertise
            .or_else(|| (!local_addr.ip().is_unspecified()).then_some(local_addr))
            .map(|a| a.to_string())
            .unwrap_or_default();
        #[cfg(feature = "link")]
        let learned_capacity = admission
            .as_ref()
            .map(|managed| managed.config.max_peers.get());
        #[cfg(not(feature = "link"))]
        let learned_capacity = None;
        let inner = Arc::new(Inner {
            local,
            local_addr,
            intro,
            config,
            peers: RwLock::new(AddressBook::new(learned_capacity)),
            pool: Mutex::new(Pool::default()),
            inbox: AsyncMutex::new(inbound_rx),
            tasks: Arc::new(Tasks::default()),
            #[cfg(feature = "link")]
            direct: inbound.direct.clone(),
            #[cfg(feature = "link")]
            admission,
        });
        #[cfg(feature = "link")]
        if let Some(managed) = &inner.admission {
            managed.set_inbound(inbound.clone());
        }
        inner.tasks.spawn(accept_loop(
            listener,
            inbound,
            read_idle,
            Arc::downgrade(&inner),
        ));
        Ok(Self {
            backend: Backend::Direct(inner),
        })
    }
}

/// Everything an outbound writer task owns.
#[derive(Debug)]
struct Outbound {
    /// Weak so a parked writer never keeps a dropped transport alive.
    inner: Weak<Inner>,
    peer: NodeId,
    generation: u64,
    addr: SocketAddr,
    frames: mpsc::Receiver<Bytes>,
    idle: Duration,
    local: NodeId,
    /// Our own listener address, introduced so the peer can dial back.
    intro: String,
}

impl Outbound {
    /// Removes this connection's pool entry (generation-checked).
    fn leave_pool(&self) {
        if let Some(inner) = self.inner.upgrade() {
            inner
                .pool
                .lock()
                .expect("pool lock poisoned")
                .remove_generation(&self.peer, self.generation);
        }
    }
}

/// Dials, handshakes, then writes queued frames until idle, eviction, or a
/// socket error. Every exit path leaves the pool entry cleaned up so the next
/// send re-dials.
async fn write_loop(mut out: Outbound) {
    if let Ok(Ok(mut sock)) = timeout(CONNECT_TIMEOUT, TcpStream::connect(out.addr)).await {
        let _ = sock.set_nodelay(true); // latency is the point of eager frames
        if write_intro(&mut sock, &out.local, &out.intro).await.is_ok() {
            let mut batch = Vec::with_capacity(framing::WRITE_BATCH);
            loop {
                match timeout(out.idle, next_batch(&mut out.frames, &mut batch)).await {
                    // Idle: leave the pool first so a racing send re-dials,
                    // then flush the few frames that may have just landed.
                    Err(_elapsed) => {
                        out.leave_pool();
                        loop {
                            extend_ready(&mut out.frames, &mut batch);
                            if batch.is_empty() || write_frames(&mut sock, &batch).await.is_err() {
                                return;
                            }
                            batch.clear();
                        }
                    }
                    // Sender gone: evicted from the pool or the transport was
                    // dropped — the entry is already out either way.
                    Ok(false) => return,
                    Ok(true) => {
                        if write_frames(&mut sock, &batch).await.is_err() {
                            break; // connection failed mid-write
                        }
                    }
                }
            }
        }
    }
    // Dial, handshake, or write failure: whatever was queued is dropped
    // (best-effort) and the pool entry goes away so the next send re-dials.
    out.leave_pool();
}

/// Accepts inbound connections and spawns a reader per peer. Exits when the
/// listener fails or every transport handle is dropped.
async fn accept_loop(
    listener: TcpListener,
    inbound: InboundPath,
    read_idle: Duration,
    inner: Weak<Inner>,
) {
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let Ok((sock, _addr)) = accepted else {
                    return; // listener failure ends intake; readers drain on their own
                };
                let _ = sock.set_nodelay(true);
                let Some(endpoint) = inner.upgrade() else {
                    return;
                };
                #[cfg(feature = "link")]
                if let Some(managed) = &endpoint.admission {
                    managed.accept(&endpoint, sock);
                    continue;
                }
                endpoint.tasks.spawn(read_loop(
                    sock,
                    inbound.clone(),
                    read_idle,
                    inner.clone(),
                ));
            }
            () = inbound.queue.closed() => return, // transport dropped
        }
    }
}

/// Reads the intro + frames off one inbound connection, attributing each
/// frame to the introduced peer id.
async fn read_loop(
    mut sock: TcpStream,
    inbound: InboundPath,
    read_idle: Duration,
    inner: Weak<Inner>,
) {
    let Ok(Ok((from, intro))) = timeout(read_idle, read_intro(&mut sock)).await else {
        return;
    };
    // The dial-back path: the listen address a dialer introduced goes up with
    // its frames as an unverified `Inbound::source` until the book holds it.
    // Only a consumer that authenticated a frame records it, through
    // `Transport::confirm_peer`, so a dialer that merely claims an id cannot
    // redirect that id's traffic.
    let mut unconfirmed = intro.parse::<SocketAddr>().ok();
    let Some(endpoint) = inner.upgrade() else {
        return;
    };
    let mut reader = FrameReader::new(endpoint.config.max_frame_bytes.get());
    drop(endpoint);
    loop {
        let Ok(read) = timeout(read_idle, reader.next(&mut sock)).await else {
            return; // silent past the reaper deadline: presumed half-open
        };
        let Ok(Some(msg)) = read else {
            return; // clean close or a broken frame — either way, done
        };
        unconfirmed = unconfirmed.filter(|addr| {
            inner
                .upgrade()
                .is_some_and(|inner| inner.peer_addr(&from) != Some(*addr))
        });
        let event = Inbound {
            from: from.clone(),
            msg,
            source: unconfirmed,
        };
        // recv() backpressure propagates here, and from here to the socket.
        if !inbound
            .deliver(QueuedInbound {
                packet: event,
                #[cfg(feature = "link")]
                session: None,
            })
            .await
        {
            return;
        }
    }
}

#[cfg(test)]
mod tests;
