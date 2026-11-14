//! [`RpcServer`]: the answering side. The server value serves connections
//! the application hands it ([`RpcServer::serve`]) under bounded admission;
//! [`RpcServer::bind`] layers an accept loop over a plane on top of that.

mod connection;
mod subscription;

use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures_util::io::{AsyncRead, AsyncWrite};
use groupnet_core::NodeId;
use groupnet_transport::bulk::{BulkTransport, DataPlane, DataStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio::task::{AbortHandle, JoinSet};

use crate::config::{RpcServerConfig, RpcServerConfigError};
use crate::{RpcStatus, lock};

use connection::Live;

pub use subscription::{NoSubscriptions, SinkError, SubscribeHandler, SubscriptionSink};

/// The first pause after a failed `accept`, doubled per consecutive failure.
const ACCEPT_BACKOFF_MIN: Duration = Duration::from_millis(10);

/// The longest pause between `accept` retries.
const ACCEPT_BACKOFF_MAX: Duration = Duration::from_secs(1);

/// Serves RPC connections with one async call handler — and, if built with
/// [`with_subscriptions`](Self::with_subscriptions), one subscribe handler —
/// under one set of [`RpcServerConfig`] limits.
///
/// `S` is the subscribe handler: [`NoSubscriptions`] for a server built with
/// [`new`](Self::new) or [`with_config`](Self::with_config), which answers
/// every subscribe [`RpcStatus::SUBSCRIPTIONS_UNSUPPORTED`].
///
/// The server does not own any `accept`: the application accepts a stream
/// wherever it likes (a [`DataPlane`] it shares with other consumers, a
/// tunnel, a test pipe) and hands it over with [`serve`](Self::serve), or
/// [`reserve`](Self::reserve)s a slot first to wait for capacity. Every
/// connection handed over counts against `max_connections` and is served
/// under `max_concurrent_per_connection`, `idle_timeout` and
/// `max_frame_bytes`. [`bind`](Self::bind) is the convenience that runs the
/// accept loop of a plane dedicated to RPC on top of this.
///
/// Cheap to clone; clones share the limits and the connections. Dropping the
/// last clone (and every outstanding [`RpcSlot`]) shuts the server down, as
/// does [`shutdown`](Self::shutdown).
pub struct RpcServer<H, S = NoSubscriptions> {
    handler: H,
    subscribe: S,
    shared: Arc<Shared>,
}

/// What every clone of one server shares.
struct Shared {
    config: RpcServerConfig,
    /// One permit per admissible connection; closed by `shutdown`.
    admission: Arc<Semaphore>,
    connections: Arc<AtomicUsize>,
    /// Every connection task; dropping the set aborts them all.
    tasks: Mutex<JoinSet<()>>,
}

impl Shared {
    fn shutdown(&self) {
        // Under the task lock, so a slot admitted before the close either
        // spawned already (and is aborted here) or sees the close and refuses.
        let mut tasks = lock(&self.tasks);
        self.admission.close();
        tasks.abort_all();
    }
}

impl fmt::Debug for Shared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shared")
            .field("config", &self.config)
            .field("connections", &self.connections.load(Ordering::SeqCst))
            .finish_non_exhaustive()
    }
}

impl<H: Clone, S: Clone> Clone for RpcServer<H, S> {
    fn clone(&self) -> Self {
        Self {
            handler: self.handler.clone(),
            subscribe: self.subscribe.clone(),
            shared: self.shared.clone(),
        }
    }
}

impl<H, S> fmt::Debug for RpcServer<H, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RpcServer")
            .field("config", &self.shared.config)
            .field("connections", &self.connections())
            .finish_non_exhaustive()
    }
}

impl<H> RpcServer<H> {
    /// A server answering with `handler` under [`RpcServerConfig::default`].
    ///
    /// `handler` is called with the peer that opened the connection and the
    /// request payload. Each request runs as its own task and is abandoned
    /// at its deadline (answered [`RpcStatus::DEADLINE_EXCEEDED`]); a handler
    /// that panics is answered [`RpcStatus::HANDLER_PANICKED`].
    #[must_use]
    pub fn new<F>(handler: H) -> Self
    where
        H: Fn(NodeId, Bytes) -> F + Clone + Send + 'static,
        F: Future<Output = Result<Bytes, RpcStatus>> + Send + 'static,
    {
        Self::validated(handler, RpcServerConfig::default())
    }

    /// [`new`](Self::new) with explicit limits.
    ///
    /// # Errors
    /// The [`RpcServerConfigError`] naming the field `config` violates;
    /// nothing is clamped.
    pub fn with_config<F>(handler: H, config: RpcServerConfig) -> Result<Self, RpcServerConfigError>
    where
        H: Fn(NodeId, Bytes) -> F + Clone + Send + 'static,
        F: Future<Output = Result<Bytes, RpcStatus>> + Send + 'static,
    {
        config.validate()?;
        Ok(Self::validated(handler, config))
    }

    fn validated(handler: H, config: RpcServerConfig) -> Self {
        RpcServer::validated_with(handler, NoSubscriptions, config)
    }

    /// Serves `plane` with `handler` under [`RpcServerConfig::default`],
    /// owning the plane's [`DataPlane::accept`]: every inbound stream on it
    /// is an RPC connection from the peer that opened it. Must be called
    /// within a Tokio runtime. Shorthand for
    /// `RpcServer::new(handler).accept_on(plane)`.
    ///
    /// The accept loop [`reserve`](Self::reserve)s a slot before each
    /// `accept` and [`serve`](RpcSlot::serve)s the stream through it, so at
    /// `max_connections` it stops accepting until a connection ends — which
    /// pushes back on dialers through the transport. A failed `accept` is
    /// retried with a bounded backoff. To share a plane with other stream
    /// consumers, run the accept loop yourself and call
    /// [`serve`](Self::serve) instead.
    #[must_use = "dropping the handle shuts the server down"]
    pub fn bind<B, F>(plane: DataPlane<B>, handler: H) -> RpcServerHandle
    where
        B: BulkTransport,
        H: Fn(NodeId, Bytes) -> F + Clone + Send + 'static,
        F: Future<Output = Result<Bytes, RpcStatus>> + Send + 'static,
    {
        Self::new(handler).accept_on(plane)
    }

    /// [`bind`](Self::bind) with explicit limits.
    ///
    /// # Errors
    /// The [`RpcServerConfigError`] naming the field `config` violates;
    /// nothing is clamped and no task is started.
    pub fn bind_with<B, F>(
        plane: DataPlane<B>,
        handler: H,
        config: RpcServerConfig,
    ) -> Result<RpcServerHandle, RpcServerConfigError>
    where
        B: BulkTransport,
        H: Fn(NodeId, Bytes) -> F + Clone + Send + 'static,
        F: Future<Output = Result<Bytes, RpcStatus>> + Send + 'static,
    {
        Ok(Self::with_config(handler, config)?.accept_on(plane))
    }
}

impl<H, S> RpcServer<H, S> {
    /// A server answering calls with `handler` and subscriptions with
    /// `subscribe`, under [`RpcServerConfig::default`].
    ///
    /// `handler` runs as for [`new`](Self::new). `subscribe` is called with
    /// the peer that opened the connection, the subscribe payload and the
    /// [`SubscriptionSink`] its items go through; it runs as its own task
    /// for as long as the subscription lives, without a deadline. Returning
    /// `Ok(())` ends the subscription (the client's
    /// [`Subscription::next`](crate::Subscription::next) yields `None`),
    /// `Err(status)` ends it with that error, and a panic with
    /// [`RpcStatus::HANDLER_PANICKED`]. A client that cancels drops the
    /// handler's future; so does the connection ending. At most
    /// [`max_subscriptions_per_connection`](RpcServerConfig::max_subscriptions_per_connection)
    /// run per connection, apart from the call handlers' limit.
    #[must_use]
    pub fn with_subscriptions<F, G>(handler: H, subscribe: S) -> Self
    where
        H: Fn(NodeId, Bytes) -> F + Clone + Send + 'static,
        F: Future<Output = Result<Bytes, RpcStatus>> + Send + 'static,
        S: Fn(NodeId, Bytes, SubscriptionSink) -> G + Clone + Send + 'static,
        G: Future<Output = Result<(), RpcStatus>> + Send + 'static,
    {
        Self::validated_with(handler, subscribe, RpcServerConfig::default())
    }

    /// [`with_subscriptions`](Self::with_subscriptions) with explicit
    /// limits.
    ///
    /// # Errors
    /// The [`RpcServerConfigError`] naming the field `config` violates;
    /// nothing is clamped.
    pub fn with_subscriptions_config<F, G>(
        handler: H,
        subscribe: S,
        config: RpcServerConfig,
    ) -> Result<Self, RpcServerConfigError>
    where
        H: Fn(NodeId, Bytes) -> F + Clone + Send + 'static,
        F: Future<Output = Result<Bytes, RpcStatus>> + Send + 'static,
        S: Fn(NodeId, Bytes, SubscriptionSink) -> G + Clone + Send + 'static,
        G: Future<Output = Result<(), RpcStatus>> + Send + 'static,
    {
        config.validate()?;
        Ok(Self::validated_with(handler, subscribe, config))
    }

    fn validated_with(handler: H, subscribe: S, config: RpcServerConfig) -> Self {
        Self {
            handler,
            subscribe,
            shared: Arc::new(Shared {
                config,
                admission: Arc::new(Semaphore::new(config.max_connections.get())),
                connections: Arc::new(AtomicUsize::new(0)),
                tasks: Mutex::new(JoinSet::new()),
            }),
        }
    }

    /// Serves `stream`, already accepted from `from`, as one RPC connection,
    /// if a connection slot is free. Returns at once; the connection runs on
    /// its own task until the peer closes it, it breaks, it sends anything
    /// but a well-formed request, or it idles out. Must be called within a
    /// Tokio runtime.
    ///
    /// The stream is the server's from here on: answers are written by one
    /// writer per connection, so frames never interleave, and at most
    /// `max_concurrent_per_connection` handlers run for it — past that the
    /// server stops reading it, which pushes back on the caller through the
    /// transport. Whatever chose the stream (a protocol tag the application
    /// read first, say) must already be consumed: the next byte on it is the
    /// first RPC frame.
    ///
    /// # Errors
    /// [`RpcServeError::AtCapacity`] if `max_connections` connections are
    /// being served, [`RpcServeError::Shutdown`] after
    /// [`shutdown`](Self::shutdown). Either way `stream` is dropped, which
    /// closes it: the caller's calls on it fail and its client reconnects on
    /// the next call. To wait for capacity instead of refusing, take a slot
    /// with [`reserve`](Self::reserve) and serve through it.
    pub fn serve<T, F>(&self, from: NodeId, stream: DataStream<T>) -> Result<(), RpcServeError>
    where
        T: AsyncRead + AsyncWrite + Send + Unpin + 'static,
        H: Fn(NodeId, Bytes) -> F + Clone + Send + 'static,
        F: Future<Output = Result<Bytes, RpcStatus>> + Send + 'static,
        S: SubscribeHandler,
    {
        let permit = match self.shared.admission.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(TryAcquireError::NoPermits) => return Err(RpcServeError::AtCapacity),
            Err(TryAcquireError::Closed) => return Err(RpcServeError::Shutdown),
        };
        self.spawn(permit, from, stream)
    }

    /// Waits for a free connection slot and holds it until it is used
    /// ([`RpcSlot::serve`]) or dropped. The returned future owns what it
    /// needs, so it may be awaited on any task.
    ///
    /// Reserving *before* accepting is how an accept loop pushes back on
    /// dialers when the server is full; reserving after reading which
    /// consumer a stream is for makes just that connection wait.
    ///
    /// # Errors
    /// [`RpcServeError::Shutdown`] once the server is shut down, including
    /// while waiting.
    pub fn reserve(
        &self,
    ) -> impl Future<Output = Result<RpcSlot<H, S>, RpcServeError>> + Send + 'static
    where
        H: Clone + Send + 'static,
        S: SubscribeHandler,
    {
        let server = self.clone();
        async move {
            let permit = server
                .shared
                .admission
                .clone()
                .acquire_owned()
                .await
                .map_err(|_closed| RpcServeError::Shutdown)?;
            Ok(RpcSlot { server, permit })
        }
    }

    /// Connections currently being served.
    #[must_use]
    pub fn connections(&self) -> usize {
        self.shared.connections.load(Ordering::SeqCst)
    }

    /// Refuses every later connection and aborts every connection with its
    /// running handlers, for all clones. Callers in flight see their
    /// connection drop. The abort completes asynchronously;
    /// [`connections`](Self::connections) reaches zero once it has.
    /// Idempotent.
    pub fn shutdown(&self) {
        self.shared.shutdown();
    }

    /// Starts serving one admitted connection on the shared task set.
    fn spawn<T, F>(
        &self,
        permit: OwnedSemaphorePermit,
        from: NodeId,
        stream: DataStream<T>,
    ) -> Result<(), RpcServeError>
    where
        T: AsyncRead + AsyncWrite + Send + Unpin + 'static,
        H: Fn(NodeId, Bytes) -> F + Clone + Send + 'static,
        F: Future<Output = Result<Bytes, RpcStatus>> + Send + 'static,
        S: SubscribeHandler,
    {
        let shared = &*self.shared;
        let mut tasks = lock(&shared.tasks);
        if shared.admission.is_closed() {
            return Err(RpcServeError::Shutdown);
        }
        // Reap finished connections; the set stays bounded by the live ones.
        while tasks.try_join_next().is_some() {}
        let live = Live::enter(&shared.connections, permit);
        tasks.spawn(connection::serve(
            from,
            stream,
            (self.handler.clone(), self.subscribe.clone()),
            shared.config,
            live,
        ));
        Ok(())
    }

    /// Runs this server's accept loop on `plane`, owning the plane's
    /// [`DataPlane::accept`] exactly as [`bind`](Self::bind) does: every
    /// inbound stream on it is an RPC connection from the peer that opened
    /// it. Must be called within a Tokio runtime. The server's clones keep
    /// serving connections handed to them; the returned handle's
    /// [`shutdown`](RpcServerHandle::shutdown) (or drop) shuts them all down.
    #[must_use = "dropping the handle shuts the server down"]
    pub fn accept_on<B, F>(self, plane: DataPlane<B>) -> RpcServerHandle
    where
        B: BulkTransport,
        H: Fn(NodeId, Bytes) -> F + Clone + Send + 'static,
        F: Future<Output = Result<Bytes, RpcStatus>> + Send + 'static,
        S: SubscribeHandler,
    {
        let shared = self.shared.clone();
        let task = tokio::spawn(accept_loop(plane, self)).abort_handle();
        RpcServerHandle { task, shared }
    }
}

/// One reserved connection slot of an [`RpcServer`]
/// ([`RpcServer::reserve`]). Dropping it unused frees the slot.
pub struct RpcSlot<H, S = NoSubscriptions> {
    server: RpcServer<H, S>,
    permit: OwnedSemaphorePermit,
}

impl<H, S> fmt::Debug for RpcSlot<H, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RpcSlot")
            .field("server", &self.server)
            .finish_non_exhaustive()
    }
}

impl<H, S> RpcSlot<H, S> {
    /// Serves `stream`, already accepted from `from`, in this slot, exactly
    /// as [`RpcServer::serve`] does once a slot is free. Must be called
    /// within a Tokio runtime.
    ///
    /// # Errors
    /// [`RpcServeError::Shutdown`] if the server was shut down after the
    /// slot was reserved; `stream` is dropped, which closes it.
    pub fn serve<T, F>(self, from: NodeId, stream: DataStream<T>) -> Result<(), RpcServeError>
    where
        T: AsyncRead + AsyncWrite + Send + Unpin + 'static,
        H: Fn(NodeId, Bytes) -> F + Clone + Send + 'static,
        F: Future<Output = Result<Bytes, RpcStatus>> + Send + 'static,
        S: SubscribeHandler,
    {
        self.server.spawn(self.permit, from, stream)
    }
}

/// Why an [`RpcServer`] did not take a connection handed to it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RpcServeError {
    /// [`RpcServerConfig::max_connections`] connections are already being
    /// served. Back-pressure, not a fault: retry once one ends, or
    /// [`reserve`](RpcServer::reserve) to wait for it.
    AtCapacity,
    /// The server was shut down.
    Shutdown,
}

impl fmt::Display for RpcServeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::AtCapacity => "rpc server is serving max_connections connections",
            Self::Shutdown => "rpc server shut down",
        })
    }
}

impl Error for RpcServeError {}

/// An [`RpcServer`] running its own accept loop ([`RpcServer::bind`]).
/// Dropping it shuts the server down.
#[derive(Debug)]
pub struct RpcServerHandle {
    task: AbortHandle,
    shared: Arc<Shared>,
}

impl RpcServerHandle {
    /// Stops accepting and aborts every connection with its running
    /// handlers. Callers in flight see their connection drop. The abort
    /// completes asynchronously; [`connections`](Self::connections) reaches
    /// zero once it has. Idempotent.
    pub fn shutdown(&self) {
        self.task.abort();
        self.shared.shutdown();
    }

    /// Connections currently being served.
    #[must_use]
    pub fn connections(&self) -> usize {
        self.shared.connections.load(Ordering::SeqCst)
    }
}

impl Drop for RpcServerHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Accepts connections until the server shuts down, admitting at most
/// `max_connections` at once: a slot is reserved before `accept`, so a full
/// server pushes back on dialers through the transport.
async fn accept_loop<B, H, F, S>(plane: DataPlane<B>, server: RpcServer<H, S>)
where
    B: BulkTransport,
    H: Fn(NodeId, Bytes) -> F + Clone + Send + 'static,
    F: Future<Output = Result<Bytes, RpcStatus>> + Send + 'static,
    S: SubscribeHandler,
{
    let mut backoff = ACCEPT_BACKOFF_MIN;
    loop {
        let Ok(slot) = server.reserve().await else {
            return;
        };
        if let Ok((from, stream)) = plane.accept().await {
            backoff = ACCEPT_BACKOFF_MIN;
            if slot.serve(from, stream).is_err() {
                return;
            }
        } else {
            drop(slot);
            // A failed handshake from one peer must not end the server; a
            // transport that keeps failing is retried at a bounded rate.
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(ACCEPT_BACKOFF_MAX);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serve_errors_describe_their_cause() {
        assert!(
            RpcServeError::AtCapacity
                .to_string()
                .contains("max_connections")
        );
        assert!(RpcServeError::Shutdown.to_string().contains("shut down"));
    }
}
