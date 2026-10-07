//! Data-plane streams over TCP: [`TcpBulkTransport`], a [`BulkTransport`] for
//! reliable, ordered byte streams (replication, snapshot transfer).
//!
//! A [`tokio::net::TcpStream`] is already `AsyncRead + AsyncWrite`; the only
//! glue is `tokio_util::compat` to present it as the runtime-agnostic
//! `futures-io` stream the trait asks for, plus a one-line node-id handshake so
//! the accepting side can attribute the connection. Both ends disable Nagle's
//! algorithm: framed request/response traffic must not wait for delayed ACKs.
//!
//! Accepting is owned by a listener task: every accepted connection runs its
//! identity handshake in its own task under [`TcpBulkConfig::handshake_timeout`],
//! at most [`TcpBulkConfig::max_handshakes`] at once, and completed handshakes
//! queue for [`accept`](BulkTransport::accept). A connection that stalls or
//! fails its handshake is dropped without delaying or failing other accepts.
//!
//! Peer endpoints may be fixed socket addresses or bounded host:port names.
//! Hostnames are resolved afresh on each connection so pod address churn does
//! not change the exact `NodeId` used by the bulk protocol.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use groupnet_core::NodeId;
use groupnet_transport::QueueCapacity;
use groupnet_transport::bulk::BulkTransport;
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs, lookup_host};
use tokio::sync::{Mutex as AsyncMutex, OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::timeout;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

use crate::handshake::{check_id, read_id, write_id};

/// Accept-side bounds for [`TcpBulkTransport`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TcpBulkConfig {
    /// Deadline for one accepted connection to deliver its identity handshake.
    /// Must be nonzero. Default: 5s.
    pub handshake_timeout: Duration,
    /// Accepted connections held at once between `accept(2)` and
    /// [`accept`](BulkTransport::accept): handshaking or waiting in the queue.
    /// At the bound, new connections wait in the kernel backlog. Default: 64.
    pub max_handshakes: QueueCapacity,
    /// Handshaken connections buffered until [`accept`](BulkTransport::accept)
    /// takes them. Default: 64.
    pub accept_queue: QueueCapacity,
}

impl Default for TcpBulkConfig {
    fn default() -> Self {
        Self {
            handshake_timeout: Duration::from_secs(5),
            max_handshakes: QueueCapacity::of(64),
            accept_queue: QueueCapacity::of(64),
        }
    }
}

impl TcpBulkConfig {
    /// Checks the bounds the types cannot express.
    ///
    /// # Errors
    /// Returns `InvalidInput` for a zero handshake timeout.
    pub fn validate(&self) -> io::Result<()> {
        if self.handshake_timeout.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TCP bulk handshake timeout must be nonzero",
            ));
        }
        Ok(())
    }
}

/// A TCP-backed data-plane transport endpoint.
///
/// [`bind`](Self::bind) makes an endpoint that listens and dials.
/// [`dial_only`](Self::dial_only) makes one that only dials: a client that
/// calls the nodes of a cluster without being reachable itself. Dropping the
/// endpoint cancels its listener task and every pending handshake.
#[derive(Debug)]
pub struct TcpBulkTransport {
    local: NodeId,
    listener: Option<Listener>,
    peers: RwLock<HashMap<NodeId, PeerEndpoint>>,
}

const MAX_HOST_ENDPOINT_BYTES: usize = 320;
const MAX_RESOLVED_ADDRESSES: usize = 8;

#[derive(Clone, Debug)]
enum PeerEndpoint {
    Address(SocketAddr),
    Host(String),
}

type Accepted = io::Result<(NodeId, TcpStream)>;

/// The listener task and the queue of connections it has attributed.
#[derive(Debug)]
struct Listener {
    addr: SocketAddr,
    accepted: AsyncMutex<mpsc::Receiver<Accepted>>,
    task: JoinHandle<()>,
}

impl Drop for Listener {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl TcpBulkTransport {
    /// Binds a listening TCP socket for `local` with the default
    /// [`TcpBulkConfig`]. Register peers with
    /// [`register_peer`](Self::register_peer) before connecting out.
    ///
    /// # Errors
    /// Returns `InvalidInput` for a local id that is empty or longer than
    /// [`MAX_NODE_ID_BYTES`](groupnet_transport::MAX_NODE_ID_BYTES), and
    /// propagates any socket bind error.
    pub async fn bind(local: NodeId, addr: impl ToSocketAddrs) -> io::Result<Self> {
        Self::bind_with(local, addr, TcpBulkConfig::default()).await
    }

    /// Binds with explicit accept bounds. Must be called within a Tokio
    /// runtime: the listener and its handshakes run as spawned tasks.
    ///
    /// # Errors
    /// Returns `InvalidInput` for an invalid configuration or local id, and
    /// propagates any socket bind error.
    pub async fn bind_with(
        local: NodeId,
        addr: impl ToSocketAddrs,
        config: TcpBulkConfig,
    ) -> io::Result<Self> {
        config.validate()?;
        check_id(&local)?;
        let listener = TcpListener::bind(addr).await?;
        let addr = listener.local_addr()?;
        let (queue, accepted) = mpsc::channel(config.accept_queue.get());
        let task = tokio::spawn(accept_loop(listener, queue, config));
        Ok(Self {
            local,
            listener: Some(Listener {
                addr,
                accepted: AsyncMutex::new(accepted),
                task,
            }),
            peers: RwLock::new(HashMap::new()),
        })
    }

    /// An endpoint for `local` that opens no socket until it dials: it
    /// connects to registered peers, and its
    /// [`accept`](BulkTransport::accept) never completes. A client process
    /// uses it to call servers (for example a `groupnet-rpc` server, which
    /// answers on the stream the client opened) without a listening port.
    #[must_use]
    pub fn dial_only(local: NodeId) -> Self {
        Self {
            local,
            listener: None,
            peers: RwLock::new(HashMap::new()),
        }
    }

    /// This endpoint's local node id.
    #[must_use]
    pub fn local_id(&self) -> &NodeId {
        &self.local
    }

    /// The address the listener is bound to (useful with an ephemeral `:0`).
    ///
    /// # Errors
    /// A [`dial_only`](Self::dial_only) endpoint has no listener and returns
    /// [`io::ErrorKind::NotConnected`].
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener
            .as_ref()
            .map(|listener| listener.addr)
            .ok_or_else(no_listener)
    }

    /// Teaches this endpoint that `node` listens at `addr`.
    ///
    /// # Panics
    /// If the address book was poisoned by a panic in another thread.
    pub fn register_peer(&self, node: NodeId, addr: SocketAddr) {
        self.peers
            .write()
            .expect("peers lock poisoned")
            .insert(node, PeerEndpoint::Address(addr));
    }

    /// Registers a bounded host:port endpoint, resolved anew on every connect.
    /// The address book is liveness routing only; the given `NodeId` remains the
    /// bulk protocol's exact peer identity.
    ///
    /// # Errors
    /// Rejects an empty, malformed, or overlong host:port endpoint.
    ///
    /// # Panics
    /// If the address book was poisoned by a panic in another thread.
    pub fn register_peer_host(&self, node: NodeId, host: String) -> io::Result<()> {
        let Some((name, port)) = host.rsplit_once(':') else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "host requires port",
            ));
        };
        if host.is_empty()
            || host.len() > MAX_HOST_ENDPOINT_BYTES
            || name.is_empty()
            || port.parse::<u16>().ok().is_none_or(|port| port == 0)
            || host.bytes().any(|byte| {
                byte.is_ascii_whitespace() || matches!(byte, b'/' | b'\\' | b'@' | b'#')
            })
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid host endpoint",
            ));
        }
        self.peers
            .write()
            .expect("peers lock poisoned")
            .insert(node, PeerEndpoint::Host(host));
        Ok(())
    }
}

impl BulkTransport for TcpBulkTransport {
    type Error = io::Error;
    type Stream = Compat<TcpStream>;

    async fn connect(&self, to: &NodeId) -> io::Result<Self::Stream> {
        // Resolve without holding the lock across the await.
        let endpoint = self
            .peers
            .read()
            .expect("peers lock poisoned")
            .get(to)
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "unknown peer"))?;
        let mut sock = match endpoint {
            PeerEndpoint::Address(addr) => TcpStream::connect(addr).await?,
            PeerEndpoint::Host(host) => {
                let addresses = lookup_host(&host)
                    .await?
                    .take(MAX_RESOLVED_ADDRESSES + 1)
                    .collect::<Vec<_>>();
                if addresses.is_empty() || addresses.len() > MAX_RESOLVED_ADDRESSES {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "resolved address count outside bound",
                    ));
                }
                let mut last = None;
                let mut connected = None;
                for address in addresses {
                    match TcpStream::connect(address).await {
                        Ok(stream) => {
                            connected = Some(stream);
                            break;
                        }
                        Err(error) => last = Some(error),
                    }
                }
                connected.ok_or_else(|| {
                    last.unwrap_or_else(|| {
                        io::Error::new(io::ErrorKind::NotFound, "no reachable peer address")
                    })
                })?
            }
        };
        sock.set_nodelay(true)?;
        write_id(&mut sock, &self.local).await?;
        Ok(sock.compat())
    }

    async fn accept(&self) -> io::Result<(NodeId, Self::Stream)> {
        let Some(listener) = &self.listener else {
            // A dial-only endpoint has nothing to accept.
            return std::future::pending().await;
        };
        let (from, sock) = listener
            .accepted
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::BrokenPipe, "TCP bulk listener stopped")
            })??;
        Ok((from, sock.compat()))
    }
}

/// Accepts connections while handshake slots are free, giving each its own
/// deadline-bounded handshake task. Listener errors are forwarded to
/// [`BulkTransport::accept`] as before; handshake failures only drop their own
/// connection. Pending handshakes are owned here and cancelled with the loop.
async fn accept_loop(listener: TcpListener, queue: mpsc::Sender<Accepted>, config: TcpBulkConfig) {
    let slots = Arc::new(Semaphore::new(config.max_handshakes.get()));
    let mut handshakes = JoinSet::new();
    loop {
        while handshakes.try_join_next().is_some() {}
        let Ok(slot) = slots.clone().acquire_owned().await else {
            return;
        };
        match listener.accept().await {
            Ok((sock, _addr)) => {
                handshakes.spawn(handshake(
                    sock,
                    queue.clone(),
                    config.handshake_timeout,
                    slot,
                ));
            }
            Err(error) => {
                if queue.send(Err(error)).await.is_err() {
                    return;
                }
            }
        }
    }
}

/// Attributes one accepted connection. The slot is held until the connection
/// is queued, so stalled and unconsumed connections share one bound.
async fn handshake(
    mut sock: TcpStream,
    queue: mpsc::Sender<Accepted>,
    deadline: Duration,
    _slot: OwnedSemaphorePermit,
) {
    if sock.set_nodelay(true).is_err() {
        return;
    }
    let Ok(Ok(from)) = timeout(deadline, read_id(&mut sock)).await else {
        return;
    };
    let _ = queue.send(Ok((from, sock))).await;
}

fn no_listener() -> io::Error {
    io::Error::new(
        io::ErrorKind::NotConnected,
        "a dial-only endpoint has no listener",
    )
}
