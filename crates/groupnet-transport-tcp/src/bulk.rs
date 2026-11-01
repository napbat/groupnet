//! Data-plane streams over TCP: [`TcpBulkTransport`], a [`BulkTransport`] for
//! reliable, ordered byte streams (replication, snapshot transfer).
//!
//! A [`tokio::net::TcpStream`] is already `AsyncRead + AsyncWrite`; the only
//! glue is `tokio_util::compat` to present it as the runtime-agnostic
//! `futures-io` stream the trait asks for, plus a versioned identity
//! handshake so the accepting side can attribute the connection. Both ends
//! disable Nagle's algorithm: framed request/response traffic must not wait
//! for delayed ACKs.
//!
//! Without a [`ClusterSecret`] the handshake only claims the dialer's id: any
//! party that reaches the listener can claim any id, so such an endpoint
//! belongs on a private network that only cluster nodes reach. With
//! [`TcpBulkConfig::secret`] (or [`TcpBulkTransport::dial_only_with`]) both
//! sides prove they hold the secret through a mutual challenge before any
//! stream byte flows, and refuse a peer that holds another secret or none
//! (see `handshake` for the wire). Refusals close the connection and count
//! in [`TcpBulkTransport::auth_failures`].
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

mod handshake;

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use groupnet_core::NodeId;
use groupnet_transport::auth::ClusterSecret;
use groupnet_transport::bulk::BulkTransport;
use groupnet_transport::{MAX_NODE_ID_BYTES, NodeIdError, QueueCapacity, validate_node_id};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs, lookup_host};
use tokio::sync::{Mutex as AsyncMutex, OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::timeout;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

use crate::handshake::check_id;

/// Accept-side bounds and the cluster secret of a [`TcpBulkTransport`].
#[derive(Clone, Debug)]
pub struct TcpBulkConfig {
    /// Deadline for one accepted connection to complete its identity
    /// handshake. Must be nonzero. Default: 5s.
    pub handshake_timeout: Duration,
    /// Accepted connections held at once between `accept(2)` and
    /// [`accept`](BulkTransport::accept): handshaking or waiting in the queue.
    /// At the bound, new connections wait in the kernel backlog. Default: 64.
    pub max_handshakes: QueueCapacity,
    /// Handshaken connections buffered until [`accept`](BulkTransport::accept)
    /// takes them. Default: 64.
    pub accept_queue: QueueCapacity,
    /// The cluster secret both directions authenticate with. `None` (the
    /// default) keeps the unauthenticated handshake, for a private network
    /// only; with a secret, peers without it, or with another, are refused.
    pub secret: Option<ClusterSecret>,
}

impl Default for TcpBulkConfig {
    fn default() -> Self {
        Self {
            handshake_timeout: Duration::from_secs(5),
            max_handshakes: QueueCapacity::of(64),
            accept_queue: QueueCapacity::of(64),
            secret: None,
        }
    }
}

impl TcpBulkConfig {
    /// Checks the bound the types cannot express.
    ///
    /// # Errors
    /// Returns [`TcpBulkConfigError::ZeroHandshakeTimeout`].
    pub const fn validate(&self) -> Result<(), TcpBulkConfigError> {
        if self.handshake_timeout.is_zero() {
            return Err(TcpBulkConfigError::ZeroHandshakeTimeout);
        }
        Ok(())
    }
}

/// A violated [`TcpBulkConfig`] bound, from [`TcpBulkConfig::validate`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TcpBulkConfigError {
    /// `handshake_timeout` is zero.
    ZeroHandshakeTimeout,
}

impl std::fmt::Display for TcpBulkConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroHandshakeTimeout => f.write_str("handshake_timeout must be nonzero"),
        }
    }
}

impl std::error::Error for TcpBulkConfigError {}

impl From<TcpBulkConfigError> for io::Error {
    fn from(error: TcpBulkConfigError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}

/// Why [`TcpBulkTransport::register_peer_host`] rejected a `host:port`
/// endpoint. Converts into an `InvalidInput` `io::Error` that keeps this
/// error as its inner error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TcpHostEndpointError {
    /// The endpoint is longer than the host-endpoint bound.
    TooLong {
        /// The endpoint's length in bytes.
        len: usize,
        /// The longest endpoint accepted, in bytes.
        max: usize,
    },
    /// The endpoint contains ASCII whitespace or one of `/`, `\`, `@`, `#`.
    ForbiddenByte {
        /// The offending byte.
        byte: u8,
        /// Its offset in the endpoint.
        index: usize,
    },
    /// The endpoint has no `:port` suffix.
    MissingPort,
    /// The host before the final `:` is empty.
    EmptyHost,
    /// The port after the final `:` is not a decimal number from 1 to 65535.
    InvalidPort,
}

impl std::fmt::Display for TcpHostEndpointError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooLong { len, max } => {
                write!(
                    f,
                    "host endpoint is {len} bytes, above the {max}-byte bound"
                )
            }
            Self::ForbiddenByte { byte, index } => write!(
                f,
                "host endpoint byte {byte:#04x} at offset {index} is not allowed"
            ),
            Self::MissingPort => f.write_str("host endpoint requires a :port suffix"),
            Self::EmptyHost => f.write_str("host endpoint has an empty host"),
            Self::InvalidPort => f.write_str("host endpoint port must be 1 to 65535"),
        }
    }
}

impl std::error::Error for TcpHostEndpointError {}

impl From<TcpHostEndpointError> for io::Error {
    fn from(error: TcpHostEndpointError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
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
    identity: Arc<Identity>,
    listener: Option<Listener>,
    peers: RwLock<HashMap<NodeId, PeerEndpoint>>,
}

/// What the dialing side and every accepted handshake of one endpoint share.
#[derive(Debug)]
struct Identity {
    local: NodeId,
    secret: Option<ClusterSecret>,
    auth_failures: AtomicU64,
}

impl Identity {
    fn new(local: NodeId, secret: Option<ClusterSecret>) -> Arc<Self> {
        Arc::new(Self {
            local,
            secret,
            auth_failures: AtomicU64::new(0),
        })
    }

    /// Counts a handshake that did not complete, before its socket closes.
    fn refused(&self) {
        self.auth_failures.fetch_add(1, Ordering::SeqCst);
    }
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

    /// Binds with explicit accept bounds and an optional cluster secret. Must
    /// be called within a Tokio runtime: the listener and its handshakes run
    /// as spawned tasks.
    ///
    /// # Errors
    /// Returns `InvalidInput` carrying the [`TcpBulkConfigError`] or
    /// [`NodeIdError`](groupnet_transport::NodeIdError) for an invalid
    /// configuration or local id, and propagates any socket bind error.
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
        let identity = Identity::new(local, config.secret);
        let task = tokio::spawn(accept_loop(
            listener,
            queue,
            config.max_handshakes,
            config.handshake_timeout,
            identity.clone(),
        ));
        Ok(Self {
            identity,
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
    /// It holds no cluster secret, so only open endpoints accept it; use
    /// [`dial_only_with`](Self::dial_only_with) for a keyed cluster.
    ///
    /// # Errors
    /// Returns the [`NodeIdError`] for a `local` id that is empty or longer
    /// than [`MAX_NODE_ID_BYTES`], before any endpoint exists, so a dial
    /// never opens a socket for an identity its handshake would refuse.
    pub fn dial_only(local: NodeId) -> Result<Self, NodeIdError> {
        Self::dialer(local, None)
    }

    /// A [`dial_only`](Self::dial_only) endpoint that authenticates every
    /// connection with `secret`: it reaches only endpoints bound with the
    /// same secret, and refuses a peer that fails the challenge.
    ///
    /// # Errors
    /// Returns the [`NodeIdError`] for a `local` id that is empty or longer
    /// than [`MAX_NODE_ID_BYTES`], before any endpoint exists.
    pub fn dial_only_with(local: NodeId, secret: ClusterSecret) -> Result<Self, NodeIdError> {
        Self::dialer(local, Some(secret))
    }

    fn dialer(local: NodeId, secret: Option<ClusterSecret>) -> Result<Self, NodeIdError> {
        validate_node_id(&local, MAX_NODE_ID_BYTES)?;
        Ok(Self {
            identity: Identity::new(local, secret),
            listener: None,
            peers: RwLock::new(HashMap::new()),
        })
    }

    /// Handshakes this endpoint refused or that failed since it was made:
    /// accepted connections whose hello had another version or mode, whose
    /// answer was wrong, or that closed or stalled before answering, and
    /// dialed connections whose peer did not authenticate or refused this
    /// endpoint. Each one closed its connection.
    #[must_use]
    pub fn auth_failures(&self) -> u64 {
        self.identity.auth_failures.load(Ordering::SeqCst)
    }

    /// This endpoint's local node id.
    #[must_use]
    pub fn local_id(&self) -> &NodeId {
        &self.identity.local
    }

    /// The address the listener was bound to (useful with an ephemeral `:0`),
    /// cached at bind; `None` for a [`dial_only`](Self::dial_only) endpoint,
    /// which has no listener.
    #[must_use]
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.listener.as_ref().map(|listener| listener.addr)
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
    /// Returns the [`TcpHostEndpointError`] for an overlong endpoint, one with
    /// whitespace or a `/`, `\`, `@` or `#` byte, or one without a nonempty
    /// host and a port from 1 to 65535, and records nothing.
    ///
    /// # Panics
    /// If the address book was poisoned by a panic in another thread.
    pub fn register_peer_host(
        &self,
        node: NodeId,
        host: String,
    ) -> Result<(), TcpHostEndpointError> {
        check_host_endpoint(&host)?;
        self.peers
            .write()
            .expect("peers lock poisoned")
            .insert(node, PeerEndpoint::Host(host));
        Ok(())
    }
}

/// Checks a `host:port` endpoint for [`TcpBulkTransport::register_peer_host`].
fn check_host_endpoint(host: &str) -> Result<(), TcpHostEndpointError> {
    if host.len() > MAX_HOST_ENDPOINT_BYTES {
        return Err(TcpHostEndpointError::TooLong {
            len: host.len(),
            max: MAX_HOST_ENDPOINT_BYTES,
        });
    }
    let forbidden =
        |byte: u8| byte.is_ascii_whitespace() || matches!(byte, b'/' | b'\\' | b'@' | b'#');
    if let Some(index) = host.bytes().position(forbidden) {
        return Err(TcpHostEndpointError::ForbiddenByte {
            byte: host.as_bytes()[index],
            index,
        });
    }
    let Some((name, port)) = host.rsplit_once(':') else {
        return Err(TcpHostEndpointError::MissingPort);
    };
    if name.is_empty() {
        return Err(TcpHostEndpointError::EmptyHost);
    }
    if port.parse::<u16>().ok().is_none_or(|port| port == 0) {
        return Err(TcpHostEndpointError::InvalidPort);
    }
    Ok(())
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
        let identity = &self.identity;
        if let Err(error) =
            handshake::dial(&mut sock, &identity.local, to, identity.secret.as_ref()).await
        {
            identity.refused();
            return Err(error);
        }
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
async fn accept_loop(
    listener: TcpListener,
    queue: mpsc::Sender<Accepted>,
    max_handshakes: QueueCapacity,
    deadline: Duration,
    identity: Arc<Identity>,
) {
    let slots = Arc::new(Semaphore::new(max_handshakes.get()));
    let mut handshakes = JoinSet::new();
    loop {
        while handshakes.try_join_next().is_some() {}
        let Ok(slot) = slots.clone().acquire_owned().await else {
            return;
        };
        match listener.accept().await {
            Ok((sock, _addr)) => {
                handshakes.spawn(attribute(
                    sock,
                    queue.clone(),
                    deadline,
                    identity.clone(),
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
/// is queued, so stalled and unconsumed connections share one bound. A
/// handshake that fails or misses its deadline is counted before its socket
/// closes.
async fn attribute(
    mut sock: TcpStream,
    queue: mpsc::Sender<Accepted>,
    deadline: Duration,
    identity: Arc<Identity>,
    _slot: OwnedSemaphorePermit,
) {
    if sock.set_nodelay(true).is_err() {
        return;
    }
    let accepted = timeout(
        deadline,
        handshake::accept(&mut sock, &identity.local, identity.secret.as_ref()),
    )
    .await;
    let Ok(Ok(from)) = accepted else {
        identity.refused();
        return;
    };
    let _ = queue.send(Ok((from, sock))).await;
}
