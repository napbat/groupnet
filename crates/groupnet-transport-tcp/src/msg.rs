//! Control-plane messaging over **persistent** TCP connections.
//!
//! [`TcpMsgTransport`] implements the best-effort, message-oriented
//! [`Transport`](groupnet_transport::Transport) contract on pooled, long-lived connections instead of
//! datagrams — the constant-connection option for clusters that want frames
//! (gossip, eager delta push) delivered at network latency over reliable
//! links, without changing the engine or the protocol:
//!
//! * **Lazy.** A connection is dialed the first time a frame is addressed to
//!   a peer, and reused for every frame after that.
//! * **Bounded.** At most [`TcpMsgConfig::max_outbound`] outbound connections
//!   exist at once (dialing past the cap closes the oldest), and a connection
//!   with nothing to send for [`TcpMsgConfig::idle_timeout`] closes itself.
//!   The pool therefore follows the peers this node is *actively* exchanging
//!   with — on a large cluster that is the rotating gossip/anti-entropy
//!   fanout, a handful of warm sockets, never one per member. Persistent
//!   connections are a per-deployment choice made here at the transport
//!   layer; nothing forces them on a deployment that prefers datagrams.
//! * **Still best-effort.** TCP orders bytes *within* one connection, but the
//!   transport keeps the datagram contract: frames to an unknown or dead peer
//!   are dropped, a full per-peer queue drops the frame, and a connection
//!   failure drops whatever was queued behind it. The engine's anti-entropy
//!   repairs all of it — do not add reliability on top.
//!
//! ## Address learning
//!
//! Only seed addresses need registering up front
//! ([`register_peer`](TcpMsgTransport::register_peer)). The rest of the book
//! fills itself in two ways: the dial handshake carries the dialer's own
//! listener address, so the accepting side can dial back a peer nobody
//! registered on it (a joiner reaching a seed); and gossiped `advertise_addr`
//! values arrive through [`groupnet_transport::Transport::learn_peer`] (the runtime feeds them
//! automatically), which resolves third parties. Between them, a cluster
//! bootstraps from seed addresses alone.
//!
//! ## Trusted low-level endpoint
//!
//! A connection's claimed id and listener address are trusted as-is — the
//! same trust model as UDP source-address attribution. Inbound and outbound
//! are separate sockets — two mutually chatty nodes hold two connections,
//! not one full-duplex one. A failed dial drops the frames queued behind it
//! and the next send re-dials, which self-limits to one connect attempt per
//! burst of sends.
//!
//! With `link`, `bind_admitted` instead uses bounded application admission and
//! a full-duplex socket for each live identity. Managed sockets are not subject
//! to raw idle/oldest-first eviction: dropping an idle writer must not withdraw
//! an admitted neighbor or prevent a server from reaching a joiner behind NAT.
//!
//! With `connectivity`, `TcpMsgTransport::bind_connectivity` owns a native
//! TCP connection with admitted candidate traversal and relay fallback instead
//! of a raw connection pool. Address hints cannot bypass native admission.

use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::Duration;

use bytes::Bytes;
use groupnet_core::NodeId;
use groupnet_transport::{Inbound, QueueCapacity};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::mpsc;
use tokio::time::timeout;

use crate::handshake::{check_id, read_intro, write_intro};
use crate::tasks::Tasks;

#[cfg(feature = "link")]
#[path = "admitted.rs"]
mod admitted;
#[cfg(feature = "link")]
pub use admitted::TcpAdmissionConfig;

#[cfg(feature = "connectivity")]
#[path = "connectivity.rs"]
mod connectivity;
#[cfg(feature = "connectivity")]
pub use connectivity::TcpPathChanges;
#[path = "msg_endpoint.rs"]
mod endpoint;
#[path = "msg_framing.rs"]
mod framing;
use framing::{read_frame, write_frame};

#[derive(Debug)]
struct QueuedInbound {
    packet: Inbound,
    #[cfg(feature = "link")]
    session: Option<groupnet_transport::admission::SessionId>,
}

/// Default allocation guard for an inbound payload; deployments may tune it
/// up to the shared stream frame ceiling.
const DEFAULT_MAX_FRAME: usize = 16 * 1024 * 1024;

/// How long a dial may take before the connection attempt is abandoned.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Tuning for [`TcpMsgTransport`] and [`TcpLink`](crate::TcpLink). The default
/// values suit a gossip control plane. Capacities are valid by type; the
/// remaining bounds are checked once by [`validate`](Self::validate), which
/// every binding and link builder calls.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TcpMsgConfig {
    /// Close an outbound connection after this long without a frame to send.
    /// The read side of an inbound connection allows twice this before
    /// presuming the peer gone, so a clean close normally comes from the
    /// sender and the reader timeout only reaps half-open sockets left by a
    /// peer that died without a FIN. Must be nonzero. Default: 30s.
    pub idle_timeout: Duration,
    /// Most outbound connections pooled at once; dialing past the cap closes
    /// the oldest. Size it to cover the gossip/anti-entropy fanout — the pool
    /// follows who this node is currently talking to, not the cluster.
    /// Default: 64.
    pub max_outbound: NonZeroUsize,
    /// Frames buffered per outbound connection while it dials or drains; a
    /// full queue drops the frame (best-effort). Default: 256.
    pub outbound_queue: QueueCapacity,
    /// Frames buffered between socket readers and the consumer. Readers wait
    /// when full, propagating TCP backpressure. Default: 1024.
    pub inbound_queue: QueueCapacity,
    /// Maximum payload bytes per frame, checked before inbound allocation and
    /// outbound queueing. Must be nonzero and at most the shared stream ceiling
    /// [`MAX_FRAME_BYTES`](groupnet_transport::framing::MAX_FRAME_BYTES).
    /// Default: 16 MiB.
    pub max_frame_bytes: usize,
    /// The listener address to introduce ourselves with when dialing, so the
    /// accepting side can dial back without prior registration. `None`
    /// introduces the bound address unless it is unspecified (`0.0.0.0`);
    /// set it when peers must reach this node somewhere else (NAT, container
    /// networking). Default: `None`.
    pub advertise: Option<SocketAddr>,
}

impl Default for TcpMsgConfig {
    fn default() -> Self {
        Self {
            idle_timeout: Duration::from_secs(30),
            max_outbound: NonZeroUsize::new(64).expect("nonzero literal"),
            outbound_queue: QueueCapacity::of(256),
            inbound_queue: QueueCapacity::of(1024),
            max_frame_bytes: DEFAULT_MAX_FRAME,
            advertise: None,
        }
    }
}

impl TcpMsgConfig {
    /// Checks the bounds the field types cannot express.
    ///
    /// # Errors
    /// Returns `InvalidInput` for a zero idle timeout, or a frame limit that
    /// is zero or above the shared stream frame ceiling.
    pub fn validate(&self) -> io::Result<()> {
        if self.idle_timeout.is_zero()
            || self.max_frame_bytes == 0
            || self.max_frame_bytes > groupnet_transport::framing::MAX_FRAME_BYTES
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid TCP message bounds",
            ));
        }
        Ok(())
    }
}

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
    /// `NodeId` -> where to dial. Interior mutability so peers can be
    /// registered after binding (e.g. once ephemeral ports are known).
    peers: RwLock<HashMap<NodeId, SocketAddr>>,
    pool: Mutex<Pool>,
    inbox: AsyncMutex<mpsc::Receiver<QueuedInbound>>,
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
        address: SocketAddr,
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
        let read_idle = config.idle_timeout.saturating_mul(2);
        let intro = config
            .advertise
            .or_else(|| (!local_addr.ip().is_unspecified()).then_some(local_addr))
            .map(|a| a.to_string())
            .unwrap_or_default();
        let inner = Arc::new(Inner {
            local,
            local_addr,
            intro,
            config,
            peers: RwLock::new(HashMap::new()),
            pool: Mutex::new(Pool::default()),
            inbox: AsyncMutex::new(inbound_rx),
            tasks: Arc::new(Tasks::default()),
            #[cfg(feature = "link")]
            admission,
        });
        #[cfg(feature = "link")]
        if let Some(managed) = &inner.admission {
            managed.set_inbound(inbound_tx.clone());
        }
        inner.tasks.spawn(accept_loop(
            listener,
            inbound_tx,
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
            loop {
                match timeout(out.idle, out.frames.recv()).await {
                    // Idle: leave the pool first so a racing send re-dials,
                    // then flush the few frames that may have just landed.
                    Err(_elapsed) => {
                        out.leave_pool();
                        while let Ok(frame) = out.frames.try_recv() {
                            if write_frame(&mut sock, &frame).await.is_err() {
                                break;
                            }
                        }
                        return;
                    }
                    // Sender gone: evicted from the pool or the transport was
                    // dropped — the entry is already out either way.
                    Ok(None) => return,
                    Ok(Some(frame)) => {
                        if write_frame(&mut sock, &frame).await.is_err() {
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
    inbound: mpsc::Sender<QueuedInbound>,
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
            () = inbound.closed() => return, // transport dropped
        }
    }
}

/// Reads the intro + frames off one inbound connection, attributing each
/// frame to the introduced peer id.
async fn read_loop(
    mut sock: TcpStream,
    inbound: mpsc::Sender<QueuedInbound>,
    read_idle: Duration,
    inner: Weak<Inner>,
) {
    let Ok(Ok((from, intro))) = timeout(read_idle, read_intro(&mut sock)).await else {
        return;
    };
    // The dial-back path: a dialer that told us where it listens is in the
    // book before its first frame surfaces, so replies can flow even to a
    // peer nobody registered here (a joiner reaching a seed).
    if !intro.is_empty()
        && let Ok(addr) = intro.parse::<SocketAddr>()
        && let Some(inner) = inner.upgrade()
    {
        inner
            .peers
            .write()
            .expect("peers lock poisoned")
            .insert(from.clone(), addr);
    }
    let Some(endpoint) = inner.upgrade() else {
        return;
    };
    let max_frame_bytes = endpoint.config.max_frame_bytes;
    drop(endpoint);
    loop {
        let Ok(read) = timeout(read_idle, read_frame(&mut sock, max_frame_bytes)).await else {
            return; // silent past the reaper deadline: presumed half-open
        };
        let Ok(Some(msg)) = read else {
            return; // clean close or a broken frame — either way, done
        };
        let event = Inbound {
            from: from.clone(),
            msg,
        };
        // recv() backpressure propagates here, and from here to the socket.
        if inbound
            .send(QueuedInbound {
                packet: event,
                #[cfg(feature = "link")]
                session: None,
            })
            .await
            .is_err()
        {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::num::NonZeroUsize;
    use std::time::Duration;

    use groupnet_core::NodeId;
    use groupnet_transport::framing::MAX_FRAME_BYTES;
    use groupnet_transport::{Inbound, Transport};

    use super::{QueueCapacity, TcpMsgConfig, TcpMsgTransport};

    /// Bind a loopback endpoint on an ephemeral port under the given id.
    async fn bind_as(id: &str) -> TcpMsgTransport {
        TcpMsgTransport::bind(NodeId::new(id), "127.0.0.1:0")
            .await
            .expect("bind")
    }

    async fn recv_one(t: &TcpMsgTransport) -> Inbound {
        tokio::time::timeout(Duration::from_secs(5), t.recv())
            .await
            .expect("recv timed out")
            .expect("recv")
    }

    /// Polls until `cond` holds or a 5s deadline passes.
    async fn eventually(mut cond: impl FnMut() -> bool, what: &str) {
        for _ in 0..500 {
            if cond() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("timed out waiting for: {what}");
    }

    /// An address that refuses connections: bind a listener, note the port,
    /// drop it.
    async fn dead_addr() -> SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        listener.local_addr().expect("addr")
    }

    /// Two frames to the same peer arrive attributed — over ONE pooled
    /// connection, including a frame far larger than any datagram.
    #[tokio::test]
    async fn frames_flow_over_a_single_reused_connection() {
        let a = bind_as("msg-a").await;
        let b = bind_as("msg-b").await;
        a.register_peer(NodeId::new("msg-b"), b.local_addr());

        let big = vec![0xCDu8; 100 * 1024];
        a.send(&NodeId::new("msg-b"), b"hello").await.expect("send");
        a.send(&NodeId::new("msg-b"), &big).await.expect("send");

        let first = recv_one(&b).await;
        assert_eq!(first.from, NodeId::new("msg-a"));
        assert_eq!(first.msg, b"hello".to_vec());
        let second = recv_one(&b).await;
        assert_eq!(second.msg, big, "TCP framing carries what UDP could not");
        assert_eq!(
            a.outbound_connections(),
            1,
            "both frames rode one persistent connection"
        );
    }

    /// Sending to a peer with no address book entry is a silent drop and
    /// pools nothing — the best-effort contract.
    #[tokio::test]
    async fn unknown_peer_is_a_silent_drop() {
        let a = bind_as("drop-a").await;
        a.send(&NodeId::new("nobody"), b"lost").await.expect("send");
        assert_eq!(a.outbound_connections(), 0);
    }

    /// An idle connection closes itself: the pool follows active exchange,
    /// so idle peers cost nothing.
    #[tokio::test]
    async fn idle_connection_closes_itself() {
        let a = TcpMsgTransport::bind_with(
            NodeId::new("idle-a"),
            "127.0.0.1:0",
            TcpMsgConfig {
                idle_timeout: Duration::from_millis(100),
                ..TcpMsgConfig::default()
            },
        )
        .await
        .expect("bind");
        let b = bind_as("idle-b").await;
        a.register_peer(NodeId::new("idle-b"), b.local_addr());

        a.send(&NodeId::new("idle-b"), b"ping").await.expect("send");
        assert_eq!(recv_one(&b).await.msg, b"ping".to_vec());
        eventually(|| a.outbound_connections() == 0, "idle close").await;
    }

    /// A dead peer never errors a send; the failed dial cleans the pool and
    /// a later send (after re-registration) dials fresh and delivers.
    #[tokio::test]
    async fn failed_dial_recovers_and_redials() {
        let a = bind_as("redial-a").await;
        let peer = NodeId::new("redial-b");
        a.register_peer(peer.clone(), dead_addr().await);

        a.send(&peer, b"void").await.expect("send is best-effort");
        eventually(|| a.outbound_connections() == 0, "failed dial cleanup").await;

        let b = bind_as("redial-b").await;
        a.register_peer(peer.clone(), b.local_addr());
        a.send(&peer, b"back").await.expect("send");
        assert_eq!(recv_one(&b).await.msg, b"back".to_vec());
    }

    /// The dial intro teaches the accepting side a dial-back path: a seed
    /// can answer a joiner nobody ever registered on it.
    #[tokio::test]
    async fn inbound_intro_teaches_the_reverse_path() {
        let joiner = bind_as("intro-joiner").await;
        let seed = bind_as("intro-seed").await;
        joiner.register_peer(NodeId::new("intro-seed"), seed.local_addr());

        joiner
            .send(&NodeId::new("intro-seed"), b"hi")
            .await
            .expect("send");
        assert_eq!(recv_one(&seed).await.msg, b"hi".to_vec());
        assert_eq!(
            seed.peer_addr(&NodeId::new("intro-joiner")),
            Some(joiner.local_addr()),
            "the intro registered the joiner's listener"
        );

        seed.send(&NodeId::new("intro-joiner"), b"welcome")
            .await
            .expect("send");
        let back = recv_one(&joiner).await;
        assert_eq!(back.from, NodeId::new("intro-seed"));
        assert_eq!(back.msg, b"welcome".to_vec());
    }

    /// Gossiped advertisements teach the book like registration; garbage is
    /// ignored (an advertisement is a hint, never an error).
    #[tokio::test]
    async fn learn_peer_parses_and_ignores_garbage() {
        let t = bind_as("learn-a").await;
        t.learn_peer(&NodeId::new("good"), "127.0.0.1:9999");
        assert_eq!(
            t.peer_addr(&NodeId::new("good")),
            Some("127.0.0.1:9999".parse().expect("addr"))
        );
        t.learn_peer(&NodeId::new("bad"), "not-an-address");
        assert_eq!(t.peer_addr(&NodeId::new("bad")), None);
    }

    /// The pool never exceeds its cap: dialing a new peer at the cap closes
    /// the oldest connection.
    #[tokio::test]
    async fn pool_cap_evicts_the_oldest_connection() {
        let a = TcpMsgTransport::bind_with(
            NodeId::new("cap-a"),
            "127.0.0.1:0",
            TcpMsgConfig {
                max_outbound: NonZeroUsize::MIN,
                ..TcpMsgConfig::default()
            },
        )
        .await
        .expect("bind");
        let b = bind_as("cap-b").await;
        let c = bind_as("cap-c").await;
        a.register_peer(NodeId::new("cap-b"), b.local_addr());
        a.register_peer(NodeId::new("cap-c"), c.local_addr());

        a.send(&NodeId::new("cap-b"), b"one").await.expect("send");
        assert_eq!(recv_one(&b).await.msg, b"one".to_vec());
        a.send(&NodeId::new("cap-c"), b"two").await.expect("send");
        assert_eq!(recv_one(&c).await.msg, b"two".to_vec());
        assert_eq!(a.outbound_connections(), 1, "cap held: oldest was closed");
    }

    #[tokio::test]
    async fn close_drains_listener_incomplete_handshakes_and_sessions() {
        use tokio::io::AsyncReadExt;

        let a = bind_as("close-a").await;
        let b = bind_as("close-b").await;
        let address = a.local_addr();
        a.register_peer(NodeId::new("close-b"), b.local_addr());
        a.send(&NodeId::new("close-b"), b"outbound")
            .await
            .expect("send");
        assert_eq!(recv_one(&b).await.msg.as_ref(), b"outbound");
        b.send(&NodeId::new("close-a"), b"inbound")
            .await
            .expect("send");
        assert_eq!(recv_one(&a).await.msg.as_ref(), b"inbound");

        // A client that never finishes the introduction must not keep close
        // waiting for the handshake's idle timeout.
        let mut stalled = tokio::net::TcpStream::connect(address)
            .await
            .expect("connect");
        tokio::time::timeout(Duration::from_secs(5), a.close())
            .await
            .expect("close must drain promptly");
        assert_eq!(a.outbound_connections(), 0);
        let _replacement = tokio::net::TcpListener::bind(address)
            .await
            .expect("close released listener");
        let mut byte = [0];
        let read = tokio::time::timeout(Duration::from_secs(5), stalled.read(&mut byte))
            .await
            .expect("session socket released");
        assert!(matches!(read, Ok(0) | Err(_)), "session remained open");
        assert!(a.send(&NodeId::new("close-b"), b"late").await.is_err());
        assert!(a.recv().await.is_err());
        a.close().await;
        b.close().await;
    }

    /// `closed` waits for shutdown through any clone, then stays resolved.
    #[tokio::test]
    async fn closed_observes_shutdown_from_any_clone_and_is_sticky() {
        let a = bind_as("closed-a").await;
        let waiter = tokio::spawn({
            let a = a.clone();
            async move { a.closed().await }
        });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished(), "a running endpoint is not closed");
        a.clone().shutdown();
        tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("shutdown observed")
            .expect("waiter");
        tokio::time::timeout(Duration::from_secs(5), a.closed())
            .await
            .expect("closed is sticky");
        a.close().await;
    }

    #[tokio::test]
    async fn configured_inbox_backpressure_preserves_frames_in_order() {
        let receiver = TcpMsgTransport::bind_with(
            NodeId::new("bounded-receiver"),
            "127.0.0.1:0",
            TcpMsgConfig {
                inbound_queue: QueueCapacity::MIN,
                max_frame_bytes: 4,
                ..TcpMsgConfig::default()
            },
        )
        .await
        .expect("bind");
        let sender = bind_as("bounded-sender").await;
        sender.register_peer(receiver.local_id().clone(), receiver.local_addr());
        for body in [b"one".as_slice(), b"two".as_slice(), b"last".as_slice()] {
            sender.send(receiver.local_id(), body).await.expect("send");
        }
        let inner = receiver.direct().expect("direct");
        groupnet_testkit::cluster::eventually("inbound queue fills", || {
            inner.inbox.try_lock().is_ok_and(|inbox| inbox.len() == 1)
        })
        .await;
        for body in [b"one".as_slice(), b"two".as_slice(), b"last".as_slice()] {
            assert_eq!(recv_one(&receiver).await.msg, body);
        }
        sender.close().await;
        receiver.close().await;
    }

    #[tokio::test]
    async fn message_bounds_are_validated_before_binding() {
        let invalid = [
            TcpMsgConfig {
                idle_timeout: Duration::ZERO,
                ..TcpMsgConfig::default()
            },
            TcpMsgConfig {
                max_frame_bytes: 0,
                ..TcpMsgConfig::default()
            },
            TcpMsgConfig {
                max_frame_bytes: MAX_FRAME_BYTES + 1,
                ..TcpMsgConfig::default()
            },
            TcpMsgConfig {
                max_frame_bytes: usize::MAX,
                ..TcpMsgConfig::default()
            },
        ];
        for config in invalid {
            assert_eq!(
                config.validate().expect_err("invalid bounds").kind(),
                std::io::ErrorKind::InvalidInput
            );
            let error = TcpMsgTransport::bind_with(NodeId::new("invalid"), "127.0.0.1:0", config)
                .await
                .expect_err("invalid bounds");
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        }
        for id in [
            String::new(),
            "x".repeat(groupnet_transport::MAX_NODE_ID_BYTES + 1),
        ] {
            let error = TcpMsgTransport::bind(NodeId::new(id), "127.0.0.1:0")
                .await
                .expect_err("local id outside the handshake bound");
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        }
        let endpoint = TcpMsgTransport::bind_with(
            NodeId::new("larger"),
            "127.0.0.1:0",
            TcpMsgConfig {
                inbound_queue: QueueCapacity::of(2048),
                max_frame_bytes: MAX_FRAME_BYTES,
                ..TcpMsgConfig::default()
            },
        )
        .await
        .expect("operational limits may exceed defaults");
        endpoint.close().await;
    }

    #[cfg(feature = "link")]
    #[tokio::test]
    async fn raw_owned_queue_keeps_storage_and_enforces_frame_cap() {
        let sender = TcpMsgTransport::bind_with(
            NodeId::new("owned-raw"),
            "127.0.0.1:0",
            TcpMsgConfig {
                max_frame_bytes: 4,
                outbound_queue: QueueCapacity::MIN,
                ..TcpMsgConfig::default()
            },
        )
        .await
        .expect("bind");
        let peer = NodeId::new("queued-peer");
        let (frames, mut queue) = tokio::sync::mpsc::channel(1);
        {
            let inner = sender.direct().expect("direct");
            let mut pool = inner.pool.lock().expect("pool");
            pool.conns.insert(
                peer.clone(),
                super::Conn {
                    generation: 0,
                    frames,
                },
            );
            pool.order.push_back((0, peer.clone()));
        }
        sender
            .send_owned_admitted(&peer, bytes::Bytes::from_static(b"too large"), None)
            .await
            .expect("oversized is a best-effort drop");
        assert!(matches!(
            queue.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        let payload = bytes::Bytes::from(vec![0xAB; 4]);
        let storage = payload.as_ptr();
        sender
            .send_owned_admitted(&peer, payload, None)
            .await
            .expect("owned send");
        sender
            .send_owned_admitted(&peer, bytes::Bytes::from_static(b"full"), None)
            .await
            .expect("full queue drops");
        let received = queue.try_recv().expect("queued");
        assert_eq!(received.as_ptr(), storage);
        assert_eq!(received.as_ref(), &[0xAB; 4]);
        assert!(matches!(
            queue.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        sender.close().await;
    }
}
