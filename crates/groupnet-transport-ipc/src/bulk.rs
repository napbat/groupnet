//! Data-plane streams over local IPC (feature `bulk`): [`IpcBulkTransport`], a
//! [`BulkTransport`] for reliable, ordered byte streams between processes of
//! one host, and [`IpcBulkStream`], the native connection it carries.
//!
//! [`bind`](IpcBulkTransport::bind) makes a hub: it listens on a local
//! address and admits clients it was never configured with, through an
//! optional [`Admission`] policy ([`IpcBulkConfig::admission`]).
//! [`dial_only`](IpcBulkTransport::dial_only) makes a client that registers hub
//! addresses and never listens. Every connection opens with a versioned
//! identity handshake (`GNIB`, version 1; see `handshake` for the wire): the
//! dialer claims its id and presents an opaque credential, the acceptor's
//! policy judges the claim, and the acceptor answers with its own id, which
//! the dialer checks against the peer it dialed. Without a policy every local
//! peer is accepted: as for [`IpcTransport`](crate::IpcTransport), the
//! operating system boundary is the trust boundary (owner-only Unix
//! directories, local-only named pipes), and a credential is an application
//! claim, not a cryptographic identity.
//!
//! Accepting is owned by a listener task: every accepted connection runs its
//! handshake in its own task under [`IpcBulkConfig::handshake_timeout`], at
//! most [`IpcBulkConfig::max_handshakes`] at once, and completed handshakes
//! queue for [`accept`](BulkTransport::accept). A connection that stalls, is
//! malformed or is refused is dropped without delaying or failing other
//! accepts, and counts in [`IpcBulkTransport::auth_failures`].

mod config;
mod handshake;

use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};
use std::time::Duration;

use groupnet_core::NodeId;
use groupnet_transport::QueueCapacity;
use groupnet_transport::admission::Admission;
use groupnet_transport::bulk::BulkTransport;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{Mutex as AsyncMutex, OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{Instant, timeout, timeout_at};
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

pub use config::{IpcBulkConfig, IpcBulkConfigError, IpcBulkDialConfig};

use crate::ipc::{IpcAddress, IpcPeerError, frame, platform};

/// One established data-plane connection of an [`IpcBulkTransport`]: a Unix
/// socket or a Windows named pipe, past its identity handshake.
///
/// The [`BulkTransport`] stream is this connection behind
/// `tokio_util::compat`; [`Compat::get_ref`] and [`Compat::into_inner`] reach
/// it. An accepted connection holds one of the listener's
/// [`IpcBulkConfig::max_connections`] until it is dropped.
#[derive(Debug)]
pub struct IpcBulkStream {
    // Declared first so the native handle closes before the slot is released.
    stream: platform::Stream,
    _slot: Option<OwnedSemaphorePermit>,
}

impl AsyncRead for IpcBulkStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for IpcBulkStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().stream).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }
}

/// A local-IPC data-plane transport endpoint.
///
/// [`bind`](Self::bind) makes an endpoint that listens and dials;
/// [`dial_only`](Self::dial_only) makes one that only dials: a client that
/// calls a hub (for example a `groupnet-rpc` server, which answers on the
/// stream the client opened) without being reachable itself. Dropping the
/// endpoint cancels its listener task and every pending handshake.
#[derive(Debug)]
pub struct IpcBulkTransport {
    identity: Arc<Identity>,
    listener: Option<Listener>,
    peers: RwLock<HashMap<NodeId, IpcAddress>>,
}

/// What the dialing side and every accepted handshake of one endpoint share.
struct Identity {
    local: NodeId,
    credential: Vec<u8>,
    admission: Option<Arc<dyn Admission>>,
    handshake_timeout: Duration,
    auth_failures: AtomicU64,
}

impl Identity {
    /// Counts a handshake that did not complete, before its connection closes.
    fn refused(&self) {
        self.auth_failures.fetch_add(1, Ordering::SeqCst);
    }
}

impl std::fmt::Debug for Identity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Neither the policy nor the credential is printed.
        f.debug_struct("Identity")
            .field("local", &self.local)
            .field("handshake_timeout", &self.handshake_timeout)
            .field("auth_failures", &self.auth_failures)
            .finish_non_exhaustive()
    }
}

type Accepted = io::Result<(NodeId, IpcBulkStream)>;

/// The listener task and the queue of connections it has attributed.
#[derive(Debug)]
struct Listener {
    accepted: AsyncMutex<mpsc::Receiver<Accepted>>,
    task: JoinHandle<()>,
}

impl Drop for Listener {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// The listener-side bounds the accept loop applies.
#[derive(Clone, Copy, Debug)]
struct Bounds {
    max_handshakes: QueueCapacity,
    max_connections: QueueCapacity,
}

impl IpcBulkTransport {
    /// Binds a native listener for `local` at `address` with the default
    /// [`IpcBulkConfig`]: no admission policy, so every local peer is
    /// accepted. Register peers with [`register_peer`](Self::register_peer)
    /// before connecting out.
    ///
    /// Synchronous like [`IpcTransport::bind`](crate::IpcTransport::bind), but
    /// it must run inside a Tokio runtime: the listener and its handshakes run
    /// as spawned tasks. Unix callers create their own private directory
    /// before binding.
    ///
    /// # Errors
    /// Returns `InvalidInput` carrying the
    /// [`NodeIdError`](crate::NodeIdError) for an empty or over-64-byte id, an
    /// address rejected with its [`IpcAddressError`](crate::IpcAddressError),
    /// an error for a missing Tokio runtime, and any native listener error
    /// (an occupied address among them).
    pub fn bind(local: NodeId, address: &IpcAddress) -> io::Result<Self> {
        Self::bind_with(local, address, IpcBulkConfig::default())
    }

    /// Binds with explicit bounds, an optional admission policy and the
    /// credential this endpoint presents when it dials.
    ///
    /// # Errors
    /// Returns `InvalidInput` carrying the [`IpcBulkConfigError`] before
    /// anything is bound, in addition to [`Self::bind`] errors.
    pub fn bind_with(
        local: NodeId,
        address: &IpcAddress,
        config: IpcBulkConfig,
    ) -> io::Result<Self> {
        config.validate()?;
        frame::validate_id(&local)?;
        tokio::runtime::Handle::try_current()
            .map_err(|_| io::Error::other("IPC bulk requires a Tokio runtime"))?;
        let listener = platform::Listener::bind(address, config.max_connections.get())?;
        let (queue, accepted) = mpsc::channel(config.accept_queue.get());
        let identity = Arc::new(Identity {
            local,
            credential: config.credential,
            admission: config.admission,
            handshake_timeout: config.handshake_timeout,
            auth_failures: AtomicU64::new(0),
        });
        let bounds = Bounds {
            max_handshakes: config.max_handshakes,
            max_connections: config.max_connections,
        };
        let task = tokio::spawn(accept_loop(listener, queue, bounds, identity.clone()));
        Ok(Self {
            identity,
            listener: Some(Listener {
                accepted: AsyncMutex::new(accepted),
                task,
            }),
            peers: RwLock::new(HashMap::new()),
        })
    }

    /// An endpoint for `local` that never listens: it connects to registered
    /// peers presenting [`IpcBulkDialConfig::credential`], and its
    /// [`accept`](BulkTransport::accept) never completes. It opens nothing
    /// until it dials and needs no Tokio runtime to be made.
    ///
    /// # Errors
    /// Returns `InvalidInput` carrying the [`IpcBulkConfigError`] or the
    /// [`NodeIdError`](crate::NodeIdError) for an empty or over-64-byte id,
    /// before any endpoint exists.
    pub fn dial_only(local: NodeId, config: IpcBulkDialConfig) -> io::Result<Self> {
        config.validate()?;
        frame::validate_id(&local)?;
        Ok(Self {
            identity: Arc::new(Identity {
                local,
                credential: config.credential,
                admission: None,
                handshake_timeout: config.handshake_timeout,
                auth_failures: AtomicU64::new(0),
            }),
            listener: None,
            peers: RwLock::new(HashMap::new()),
        })
    }

    /// Handshakes this endpoint refused or that failed since it was made:
    /// accepted connections whose hello had another magic or version, whose
    /// fields were malformed or oversized, whose claim the policy refused, or
    /// that closed or stalled before finishing; and dialed connections whose
    /// peer refused this endpoint, answered for another id, or missed the
    /// handshake deadline. Each one closed its connection.
    #[must_use]
    pub fn auth_failures(&self) -> u64 {
        self.identity.auth_failures.load(Ordering::SeqCst)
    }

    /// This endpoint's local node id.
    #[must_use]
    pub fn local_id(&self) -> &NodeId {
        &self.identity.local
    }

    /// Teaches this endpoint that `node` listens at `address`, replacing any
    /// address registered for it before. Connections already open are not
    /// affected.
    ///
    /// # Errors
    /// Returns `InvalidInput` carrying the [`IpcPeerError`] for an empty or
    /// over-64-byte id, an address that breaks the platform's local-only rules
    /// or this endpoint's own id, and records nothing.
    ///
    /// # Panics
    /// If the address book was poisoned by a panic in another thread.
    pub fn register_peer(&self, node: NodeId, address: IpcAddress) -> io::Result<()> {
        frame::validate_id(&node).map_err(IpcPeerError::Identity)?;
        platform::validate_address(&address).map_err(IpcPeerError::Address)?;
        if node == self.identity.local {
            return Err(IpcPeerError::Local.into());
        }
        self.peers
            .write()
            .expect("peers lock poisoned")
            .insert(node, address);
        Ok(())
    }
}

impl BulkTransport for IpcBulkTransport {
    type Error = io::Error;
    type Stream = Compat<IpcBulkStream>;

    async fn connect(&self, to: &NodeId) -> io::Result<Self::Stream> {
        // Resolve without holding the lock across the await.
        let address = self
            .peers
            .read()
            .expect("peers lock poisoned")
            .get(to)
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "unknown peer"))?;
        let identity = &self.identity;
        // One deadline covers connecting (a busy Windows pipe is retried) and
        // the handshake.
        let deadline = Instant::now() + identity.handshake_timeout;
        let mut stream = timeout_at(deadline, platform::connect(&address))
            .await
            .unwrap_or_else(|_| Err(missed_deadline()))?;
        let dialed = timeout_at(
            deadline,
            handshake::dial(&mut stream, &identity.local, &identity.credential, to),
        )
        .await
        .unwrap_or_else(|_| Err(missed_deadline()));
        if let Err(error) = dialed {
            identity.refused();
            return Err(error);
        }
        Ok(IpcBulkStream {
            stream,
            _slot: None,
        }
        .compat())
    }

    async fn accept(&self) -> io::Result<(NodeId, Self::Stream)> {
        let Some(listener) = &self.listener else {
            // A dial-only endpoint has nothing to accept.
            return std::future::pending().await;
        };
        let (from, stream) = listener
            .accepted
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::BrokenPipe, "IPC bulk listener stopped")
            })??;
        Ok((from, stream.compat()))
    }
}

fn missed_deadline() -> io::Error {
    io::Error::new(
        io::ErrorKind::TimedOut,
        "peer missed the handshake deadline",
    )
}

/// Accepts connections while a handshake slot and a connection slot are free,
/// giving each its own deadline-bounded handshake task. Listener errors are
/// forwarded to [`BulkTransport::accept`]; handshake failures only drop their
/// own connection. Pending handshakes are owned here and cancelled with the
/// loop.
async fn accept_loop(
    mut listener: platform::Listener,
    queue: mpsc::Sender<Accepted>,
    bounds: Bounds,
    identity: Arc<Identity>,
) {
    let handshakes_free = Arc::new(Semaphore::new(bounds.max_handshakes.get()));
    let connections_free = Arc::new(Semaphore::new(bounds.max_connections.get()));
    let mut handshakes = JoinSet::new();
    loop {
        while handshakes.try_join_next().is_some() {}
        let Ok(handshake) = handshakes_free.clone().acquire_owned().await else {
            return;
        };
        let Ok(connection) = connections_free.clone().acquire_owned().await else {
            return;
        };
        match listener.accept().await {
            Ok(stream) => {
                let stream = IpcBulkStream {
                    stream,
                    _slot: Some(connection),
                };
                handshakes.spawn(attribute(
                    stream,
                    queue.clone(),
                    identity.clone(),
                    handshake,
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

/// Attributes one accepted connection. The handshake slot is held until the
/// connection is queued, so stalled and unconsumed connections share one
/// bound. A handshake that fails or misses its deadline is counted before its
/// connection closes.
async fn attribute(
    mut stream: IpcBulkStream,
    queue: mpsc::Sender<Accepted>,
    identity: Arc<Identity>,
    _handshake: OwnedSemaphorePermit,
) {
    let accepted = timeout(
        identity.handshake_timeout,
        handshake::accept(
            &mut stream.stream,
            &identity.local,
            identity.admission.as_deref(),
        ),
    )
    .await;
    let Ok(Ok(from)) = accepted else {
        identity.refused();
        return;
    };
    let _ = queue.send(Ok((from, stream))).await;
}
