//! Data-plane streams over TCP: [`TcpBulkTransport`], a [`BulkTransport`] for
//! reliable, ordered byte streams (replication, snapshot transfer, RPC).
//!
//! The endpoint has two modes, chosen by its type parameter:
//!
//! * [`TcpPlain`] (the default): cleartext streams after a versioned identity
//!   handshake (`GNBH`), for cluster nodes on a private network;
//! * [`TcpTls`] (feature `tls`): mutually authenticated TLS 1.3 streams for
//!   clients a hub was never configured with, across a trust boundary (see
//!   the mode's docs).
//!
//! Neither mode falls back to the other: a plain endpoint refuses a TLS
//! handshake and a TLS endpoint refuses a plain hello.
//!
//! In plain mode, a [`tokio::net::TcpStream`] is already
//! `AsyncRead + AsyncWrite`; the only glue is `tokio_util::compat` to present
//! it as the runtime-agnostic `futures-io` stream the trait asks for. Both
//! ends of both modes disable Nagle's algorithm: framed request/response
//! traffic must not wait for delayed ACKs.
//!
//! Without a [`ClusterSecret`] the plain handshake only claims the dialer's
//! id: any party that reaches the listener can claim any id, so such an
//! endpoint belongs on a private network that only cluster nodes reach. With
//! [`TcpBulkConfig::secret`] (or [`TcpBulkTransport::dial_only_with`]) both
//! sides prove they hold the secret through a mutual challenge before any
//! stream byte flows, and refuse a peer that holds another secret or none
//! (see `handshake` for the wire). Refusals close the connection and count
//! in [`TcpBulkTransport::auth_failures`]. The plain handshake authenticates
//! connection setup only: stream bytes after it carry no integrity protection
//! and are not bound to the handshake, so an on-path attacker can inject into
//! or alter RPC traffic. Keep a plain data plane on a private network, and use
//! the TLS mode where the network crosses a trust boundary.
//!
//! Accepting is owned by a listener task: every accepted connection runs its
//! handshake in its own task under the handshake deadline, a bounded number at
//! once, and completed handshakes queue for [`accept`](BulkTransport::accept).
//! A connection that stalls or fails its handshake is dropped without
//! delaying or failing other accepts.
//!
//! Peer endpoints may be fixed socket addresses or bounded host:port names.
//! Hostnames are resolved afresh on each connection so pod address churn does
//! not change the exact `NodeId` used by the bulk protocol.

mod accept;
mod endpoint;
mod handshake;
#[cfg(feature = "tls")]
mod tls;

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use groupnet_core::NodeId;
use groupnet_transport::auth::ClusterSecret;
use groupnet_transport::bulk::BulkTransport;
use groupnet_transport::{MAX_NODE_ID_BYTES, NodeIdError, QueueCapacity, validate_node_id};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::time::timeout;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

pub use endpoint::TcpHostEndpointError;
#[cfg(feature = "tls")]
pub use tls::{
    TcpTls, TcpTlsConfig, TcpTlsConfigError, TcpTlsDialConfig, TcpTlsKey, TcpTlsKeyError,
    TcpTlsPublicKey, TcpTlsPublicKeyError, TcpTlsStream,
};

use crate::handshake::check_id;
use accept::{Bounds, Listener};
use endpoint::{PeerEndpoint, Peers};

/// Default deadline for one handshake.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// Accept-side bounds and the cluster secret of a plain
/// [`TcpBulkTransport`].
#[derive(Clone, Debug)]
pub struct TcpBulkConfig {
    /// Deadline for one identity handshake to complete, both on accepted
    /// connections and on connections this endpoint dials (a dialed
    /// handshake that misses it fails with `TimedOut`). Dial-only endpoints
    /// use the default. Must be nonzero. Default: 5s.
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
    /// The secret authenticates connection setup only: bytes after the
    /// handshake carry no integrity protection and are not bound to it, so an
    /// on-path attacker can inject into or alter RPC traffic. Keep the data
    /// plane on a private network, or use the `TcpTls` mode (feature `tls`)
    /// where it crosses a trust boundary.
    pub secret: Option<ClusterSecret>,
}

impl Default for TcpBulkConfig {
    fn default() -> Self {
        Self {
            handshake_timeout: HANDSHAKE_TIMEOUT,
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

/// A TCP-backed data-plane transport endpoint.
///
/// The mode parameter `M` fixes how connections are secured, and each mode
/// has only its own settings:
///
/// * `TcpBulkTransport<TcpPlain>` (the default, written `TcpBulkTransport`):
///   [`bind`](Self::bind) makes an endpoint that listens and dials;
///   [`dial_only`](Self::dial_only) makes one that only dials: a client that
///   calls the nodes of a cluster without being reachable itself.
/// * `TcpBulkTransport<TcpTls>` (feature `tls`): `bind_tls` makes a hub that
///   admits clients it was never configured with; `dial_only_tls` makes a
///   client. Every connection is mutually authenticated TLS 1.3.
///
/// Dropping the endpoint cancels its listener task and every pending
/// handshake.
#[derive(Debug)]
pub struct TcpBulkTransport<M = TcpPlain> {
    identity: Arc<Identity>,
    addr: Option<SocketAddr>,
    mode: M,
}

/// The cleartext mode of a [`TcpBulkTransport`]: the `GNBH` identity
/// handshake, keyed by an optional [`ClusterSecret`]. Built by
/// [`bind`](TcpBulkTransport::bind), [`bind_with`](TcpBulkTransport::bind_with),
/// [`dial_only`](TcpBulkTransport::dial_only) and
/// [`dial_only_with`](TcpBulkTransport::dial_only_with).
#[derive(Debug)]
pub struct TcpPlain {
    secret: Option<Arc<ClusterSecret>>,
    listener: Option<Listener<TcpStream>>,
    peers: Peers<PeerEndpoint>,
}

/// What the dialing side and every accepted handshake of one endpoint share.
#[derive(Debug)]
struct Identity {
    local: NodeId,
    handshake_timeout: Duration,
    auth_failures: AtomicU64,
}

impl Identity {
    fn new(local: NodeId, handshake_timeout: Duration) -> Arc<Self> {
        Arc::new(Self {
            local,
            handshake_timeout,
            auth_failures: AtomicU64::new(0),
        })
    }

    /// Counts a handshake that did not complete, before its socket closes.
    fn refused(&self) {
        self.auth_failures.fetch_add(1, Ordering::SeqCst);
    }

    /// Runs a dialed handshake under the deadline, counting a failure.
    async fn dialed<T>(&self, handshake: impl Future<Output = io::Result<T>>) -> io::Result<T> {
        let dialed = timeout(self.handshake_timeout, handshake)
            .await
            .unwrap_or_else(|_| {
                Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "peer missed the handshake deadline",
                ))
            });
        if dialed.is_err() {
            self.refused();
        }
        dialed
    }
}

impl<M> TcpBulkTransport<M> {
    /// Handshakes this endpoint refused or that failed since it was made:
    /// accepted connections whose hello had another version or mode, whose
    /// answer was wrong, whose claim was refused, or that closed or stalled
    /// before answering, and dialed connections whose peer did not
    /// authenticate, refused this endpoint, or missed the handshake deadline.
    /// Each one closed its connection.
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
    /// cached at bind; `None` for a dial-only endpoint, which has no listener.
    #[must_use]
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.addr
    }
}

impl TcpBulkTransport<TcpPlain> {
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
        let identity = Identity::new(local, config.handshake_timeout);
        let secret = config.secret.map(Arc::new);
        let bounds = Bounds {
            max_handshakes: config.max_handshakes,
            accept_queue: config.accept_queue,
            max_connections: None,
        };
        let accepting = identity.clone();
        let keyed = secret.clone();
        let listener = Listener::spawn(listener, bounds, identity.clone(), move |sock, _, _| {
            let identity = accepting.clone();
            let secret = keyed.clone();
            async move {
                let mut sock = sock;
                let from = handshake::accept(&mut sock, &identity.local, secret.as_deref()).await?;
                Ok((from, sock))
            }
        });
        Ok(Self {
            identity,
            addr: Some(addr),
            mode: TcpPlain {
                secret,
                listener: Some(listener),
                peers: Peers::new(),
            },
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
    /// same secret, and refuses a peer that fails the challenge. Each dialed
    /// handshake is bounded by the default
    /// [`TcpBulkConfig::handshake_timeout`] (5s).
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
            identity: Identity::new(local, HANDSHAKE_TIMEOUT),
            addr: None,
            mode: TcpPlain {
                secret: secret.map(Arc::new),
                listener: None,
                peers: Peers::new(),
            },
        })
    }

    /// Teaches this endpoint that `node` listens at `addr`.
    ///
    /// # Panics
    /// If the address book was poisoned by a panic in another thread.
    pub fn register_peer(&self, node: NodeId, addr: SocketAddr) {
        self.mode.peers.insert(node, PeerEndpoint::Address(addr));
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
        self.mode.peers.insert(node, PeerEndpoint::host(host)?);
        Ok(())
    }
}

impl BulkTransport for TcpBulkTransport<TcpPlain> {
    type Error = io::Error;
    type Stream = Compat<TcpStream>;

    async fn connect(&self, to: &NodeId) -> io::Result<Self::Stream> {
        let mut sock = self.mode.peers.get(to)?.open().await?;
        let identity = &self.identity;
        identity
            .dialed(handshake::dial(
                &mut sock,
                &identity.local,
                to,
                self.mode.secret.as_deref(),
            ))
            .await?;
        Ok(sock.compat())
    }

    async fn accept(&self) -> io::Result<(NodeId, Self::Stream)> {
        let Some(listener) = &self.mode.listener else {
            // A dial-only endpoint has nothing to accept.
            return std::future::pending().await;
        };
        let (from, sock) = listener.accept().await?;
        Ok((from, sock.compat()))
    }
}
