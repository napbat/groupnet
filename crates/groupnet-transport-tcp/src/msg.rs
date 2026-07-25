//! Control-plane messaging over **persistent** TCP connections.
//!
//! [`TcpMsgTransport`] implements the best-effort, message-oriented
//! [`Transport`] contract on top of pooled, long-lived connections instead of
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
//! values arrive through [`Transport::learn_peer`] (the runtime feeds them
//! automatically), which resolves third parties. Between them, a cluster
//! bootstraps from seed addresses alone.
//!
//! ## Scaffold simplifications
//!
//! A connection's claimed id and listener address are trusted as-is — the
//! same trust model as UDP source-address attribution. Inbound and outbound
//! are separate sockets — two mutually chatty nodes hold two connections,
//! not one full-duplex one. A failed dial drops the frames queued behind it
//! and the next send re-dials, which self-limits to one connect attempt per
//! burst of sends.

use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::Duration;

use groupnet_core::NodeId;
use groupnet_transport::{Inbound, Transport};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::mpsc;
use tokio::time::timeout;

use crate::handshake::{read_id, read_str, write_id, write_str};

/// Hard upper bound on a single frame. Engine frames are soft-capped far
/// below this (`Config::max_delta_frame_bytes`); the guard only stops a
/// garbage length prefix from allocating gigabytes on the read side, and an
/// oversized outbound frame is dropped rather than poisoning the link.
const MAX_FRAME: usize = 16 * 1024 * 1024;

/// Inbound frames buffered between the reader tasks and
/// [`Transport::recv`]. When the consumer lags, readers stop pulling from
/// their sockets and TCP backpressure does the rest.
const INBOUND_QUEUE: usize = 1024;

/// How long a dial may take before the connection attempt is abandoned.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Tuning for [`TcpMsgTransport`]. The `Default` values suit a gossip
/// control plane; zero values are lifted to one.
#[derive(Clone, Debug)]
pub struct TcpMsgConfig {
    /// Close an outbound connection after this long without a frame to send.
    /// The read side of an inbound connection allows twice this before
    /// presuming the peer gone, so a clean close normally comes from the
    /// sender and the reader timeout only reaps half-open sockets left by a
    /// peer that died without a FIN. Default: 30s.
    pub idle_timeout: Duration,
    /// Most outbound connections pooled at once; dialing past the cap closes
    /// the oldest. Size it to cover the gossip/anti-entropy fanout — the pool
    /// follows who this node is currently talking to, not the cluster.
    /// Default: 64.
    pub max_outbound: usize,
    /// Frames buffered per outbound connection while it dials or drains; a
    /// full queue drops the frame (best-effort). Default: 256.
    pub outbound_queue: usize,
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
            max_outbound: 64,
            outbound_queue: 256,
            advertise: None,
        }
    }
}

/// An outbound connection's handle in the pool: the frame queue plus a
/// generation stamp so a writer task only ever removes *its own* entry.
#[derive(Debug)]
struct Conn {
    generation: u64,
    frames: mpsc::Sender<Vec<u8>>,
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
    /// NodeId -> where to dial. Interior mutability so peers can be
    /// registered after binding (e.g. once ephemeral ports are known).
    peers: RwLock<HashMap<NodeId, SocketAddr>>,
    pool: Mutex<Pool>,
    inbox: AsyncMutex<mpsc::Receiver<Inbound>>,
}

/// A persistent-connection TCP endpoint for the control plane.
///
/// Cheap to [`Clone`]: clones share the listener, the address book, and the
/// connection pool, so a [`register_peer`](Self::register_peer) through any
/// handle is visible to all.
#[derive(Clone, Debug)]
pub struct TcpMsgTransport {
    inner: Arc<Inner>,
}

impl TcpMsgTransport {
    /// Binds a listening socket for `local` with the default
    /// [`TcpMsgConfig`]. Register peers with
    /// [`register_peer`](Self::register_peer) before sending.
    ///
    /// # Errors
    /// Propagates any socket bind error.
    pub async fn bind(local: NodeId, addr: impl ToSocketAddrs) -> io::Result<Self> {
        Self::bind_with(local, addr, TcpMsgConfig::default()).await
    }

    /// Binds with explicit tuning. Must be called within a Tokio runtime —
    /// the accept loop and per-connection workers run as spawned tasks.
    ///
    /// # Errors
    /// Propagates any socket bind error.
    pub async fn bind_with(
        local: NodeId,
        addr: impl ToSocketAddrs,
        mut config: TcpMsgConfig,
    ) -> io::Result<Self> {
        config.max_outbound = config.max_outbound.max(1);
        config.outbound_queue = config.outbound_queue.max(1);
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        let (inbound_tx, inbound_rx) = mpsc::channel(INBOUND_QUEUE);
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
        });
        tokio::spawn(accept_loop(
            listener,
            inbound_tx,
            read_idle,
            Arc::downgrade(&inner),
        ));
        Ok(Self { inner })
    }

    /// This endpoint's local node id.
    #[must_use]
    pub fn local_id(&self) -> &NodeId {
        &self.inner.local
    }

    /// The address the listener is bound to (useful with an ephemeral `:0`).
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.inner.local_addr
    }

    /// Teaches this endpoint that `node` listens at `addr`, replacing any
    /// previous binding. An existing connection to `node` is left alone; it
    /// dials the new address only after it next closes (idle or error).
    pub fn register_peer(&self, node: NodeId, addr: SocketAddr) {
        self.inner
            .peers
            .write()
            .expect("peers lock poisoned")
            .insert(node, addr);
    }

    /// The address this endpoint would dial for `node`, however it was
    /// learned — registration, a gossiped advertisement, or a dial-back
    /// intro. `None` if unknown.
    #[must_use]
    pub fn peer_addr(&self, node: &NodeId) -> Option<SocketAddr> {
        self.inner
            .peers
            .read()
            .expect("peers lock poisoned")
            .get(node)
            .copied()
    }

    /// Outbound connections currently pooled (established or still dialing).
    /// Observability for the bounded-pool promise: on a large cluster this
    /// tracks the active fanout, not the membership size.
    #[must_use]
    pub fn outbound_connections(&self) -> usize {
        self.inner
            .pool
            .lock()
            .expect("pool lock poisoned")
            .conns
            .len()
    }
}

impl Transport for TcpMsgTransport {
    type Error = io::Error;

    fn learn_peer(&self, node: &NodeId, addr: &str) {
        // An advertisement is a hint: register what parses, ignore the rest.
        if let Ok(addr) = addr.parse::<SocketAddr>() {
            self.register_peer(node.clone(), addr);
        }
    }

    async fn send(&self, to: &NodeId, msg: &[u8]) -> io::Result<()> {
        if msg.len() > MAX_FRAME {
            return Ok(()); // oversized frame = drop, never poison the link
        }
        // Pre-frame (length prefix + payload) so the writer hands the socket
        // one buffer per frame.
        let mut framed = Vec::with_capacity(4 + msg.len());
        framed.extend_from_slice(&(msg.len() as u32).to_be_bytes());
        framed.extend_from_slice(msg);

        // Fast path: an existing (possibly still dialing) connection.
        {
            let mut pool = self.inner.pool.lock().expect("pool lock poisoned");
            if let Some(conn) = pool.conns.get(to) {
                match conn.frames.try_send(framed) {
                    Ok(()) => return Ok(()),
                    // Full queue: best-effort drop; anti-entropy repairs.
                    Err(mpsc::error::TrySendError::Full(_)) => return Ok(()),
                    // The writer is exiting (idle close or error): remove the
                    // husk and fall through to a fresh dial.
                    Err(mpsc::error::TrySendError::Closed(frame)) => {
                        framed = frame;
                        pool.remove(to);
                    }
                }
            }
        }

        // Resolve the address without holding any lock across an await.
        let addr = self
            .inner
            .peers
            .read()
            .expect("peers lock poisoned")
            .get(to)
            .copied();
        let Some(addr) = addr else {
            return Ok(()); // unknown peer = drop, per the trait contract
        };

        let (tx, rx) = mpsc::channel(self.inner.config.outbound_queue);
        tx.try_send(framed).expect("fresh queue has capacity");
        let generation;
        {
            let mut pool = self.inner.pool.lock().expect("pool lock poisoned");
            // At the cap: close the oldest connection(s) first. Dropping the
            // sender ends that writer task; if its peer is still active, the
            // next frame to it simply re-dials.
            while pool.conns.len() >= self.inner.config.max_outbound {
                let Some((g, node)) = pool.order.pop_front() else {
                    break;
                };
                if pool.conns.get(&node).is_some_and(|c| c.generation == g) {
                    pool.conns.remove(&node);
                }
            }
            generation = pool.next_generation;
            pool.next_generation += 1;
            pool.conns.insert(
                to.clone(),
                Conn {
                    generation,
                    frames: tx,
                },
            );
            pool.order.push_back((generation, to.clone()));
        }
        // Concurrent sends to the same peer can race past the fast path; the
        // loser's insert replaces the winner's entry and the winner's task
        // exits once its now-unreferenced queue drains. Rare and harmless.
        tokio::spawn(write_loop(Outbound {
            inner: Arc::downgrade(&self.inner),
            peer: to.clone(),
            generation,
            addr,
            frames: rx,
            idle: self.inner.config.idle_timeout,
            local: self.inner.local.clone(),
            intro: self.inner.intro.clone(),
        }));
        Ok(())
    }

    async fn recv(&self) -> io::Result<Inbound> {
        // The tokio mutex is held across the await intentionally; only the
        // single receive loop ever calls this.
        let mut inbox = self.inner.inbox.lock().await;
        inbox
            .recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "tcp msg transport shut down"))
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
    frames: mpsc::Receiver<Vec<u8>>,
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
        if write_id(&mut sock, &out.local).await.is_ok()
            && write_str(&mut sock, &out.intro).await.is_ok()
        {
            loop {
                match timeout(out.idle, out.frames.recv()).await {
                    // Idle: leave the pool first so a racing send re-dials,
                    // then flush the few frames that may have just landed.
                    Err(_elapsed) => {
                        out.leave_pool();
                        while let Ok(frame) = out.frames.try_recv() {
                            if sock.write_all(&frame).await.is_err() {
                                break;
                            }
                        }
                        return;
                    }
                    // Sender gone: evicted from the pool or the transport was
                    // dropped — the entry is already out either way.
                    Ok(None) => return,
                    Ok(Some(frame)) => {
                        if sock.write_all(&frame).await.is_err() {
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
    inbound: mpsc::Sender<Inbound>,
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
                tokio::spawn(read_loop(sock, inbound.clone(), read_idle, inner.clone()));
            }
            () = inbound.closed() => return, // transport dropped
        }
    }
}

/// Reads the intro + frames off one inbound connection, attributing each
/// frame to the introduced peer id.
async fn read_loop(
    mut sock: TcpStream,
    inbound: mpsc::Sender<Inbound>,
    read_idle: Duration,
    inner: Weak<Inner>,
) {
    let Ok(Ok((from, intro))) = timeout(read_idle, read_intro(&mut sock)).await else {
        return;
    };
    // The dial-back path: a dialer that told us where it listens is in the
    // book before its first frame surfaces, so replies can flow even to a
    // peer nobody registered here (a joiner reaching a seed).
    if !intro.is_empty() {
        if let Ok(addr) = intro.parse::<SocketAddr>() {
            if let Some(inner) = inner.upgrade() {
                inner
                    .peers
                    .write()
                    .expect("peers lock poisoned")
                    .insert(from.clone(), addr);
            }
        }
    }
    loop {
        let Ok(read) = timeout(read_idle, read_frame(&mut sock)).await else {
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
        if inbound.send(event).await.is_err() {
            return;
        }
    }
}

/// Reads the dialer's intro: its node id and its (possibly empty) listener
/// address.
async fn read_intro(sock: &mut TcpStream) -> io::Result<(NodeId, String)> {
    let from = read_id(sock).await?;
    let intro = read_str(sock).await?;
    Ok((from, intro))
}

/// Reads one length-prefixed frame. `Ok(None)` is a clean close between
/// frames; an oversized length is an error (the connection is torn down).
async fn read_frame(sock: &mut TcpStream) -> io::Result<Option<Vec<u8>>> {
    let mut len_bytes = [0u8; 4];
    if let Err(err) = sock.read_exact(&mut len_bytes).await {
        return if err.kind() == io::ErrorKind::UnexpectedEof {
            Ok(None)
        } else {
            Err(err)
        };
    }
    let len = u32::from_be_bytes(len_bytes) as usize;
    if len > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame length exceeds the transport maximum",
        ));
    }
    let mut msg = vec![0u8; len];
    sock.read_exact(&mut msg).await?;
    Ok(Some(msg))
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use groupnet_core::NodeId;
    use groupnet_transport::{Inbound, Transport};

    use super::{TcpMsgConfig, TcpMsgTransport};

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
                max_outbound: 1,
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
}
