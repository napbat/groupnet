//! [`RpcServer`]: the answering side — owns a plane's `accept`, runs one
//! bounded task set per connection.

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures_util::FutureExt;
use futures_util::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use groupnet_core::NodeId;
use groupnet_transport::bulk::{BulkTransport, DataPlane, DataStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::{AbortHandle, JoinSet};
use tokio::time::{Instant, timeout_at};

use crate::codec::{ERROR_HEAD, Frame, RESPONSE_HEAD};
use crate::{DEFAULT_MAX_FRAME_BYTES, RpcStatus, check_frame_limit, write_frames};

/// The first pause after a failed `accept`, doubled per consecutive failure.
const ACCEPT_BACKOFF_MIN: Duration = Duration::from_millis(10);
/// The longest pause between `accept` retries.
const ACCEPT_BACKOFF_MAX: Duration = Duration::from_secs(1);

/// Server limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RpcServerConfig {
    /// The largest RPC frame (head included) this server accepts or sends.
    /// Match the clients' [`RpcConfig::max_frame_bytes`](crate::RpcConfig::max_frame_bytes).
    pub max_frame_bytes: usize,
    /// Handlers run concurrently for one connection; at the limit the server
    /// stops reading that connection until one finishes.
    pub max_concurrent_per_connection: usize,
}

impl Default for RpcServerConfig {
    /// 16 MiB frames, 64 concurrent handlers per connection.
    fn default() -> Self {
        Self {
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            max_concurrent_per_connection: 64,
        }
    }
}

/// Spawns RPC servers; see [`spawn`](Self::spawn).
#[derive(Debug)]
pub struct RpcServer;

impl RpcServer {
    /// Serves `plane` with `handler` under [`RpcServerConfig::default`].
    ///
    /// The server takes over [`DataPlane::accept`]: every inbound stream is
    /// an RPC connection from the peer that opened it, and `handler` is
    /// called with that peer's id and the request payload. Must be called
    /// within a Tokio runtime.
    #[must_use = "dropping the handle shuts the server down"]
    pub fn spawn<B, H, F>(plane: DataPlane<B>, handler: H) -> RpcServerHandle
    where
        B: BulkTransport,
        H: Fn(NodeId, Bytes) -> F + Clone + Send + 'static,
        F: Future<Output = Result<Bytes, RpcStatus>> + Send + 'static,
    {
        Self::spawn_with(plane, handler, RpcServerConfig::default())
    }

    /// [`spawn`](Self::spawn) with explicit limits.
    ///
    /// Each request runs as its own task, at most
    /// `max_concurrent_per_connection` per connection, and is abandoned at
    /// its deadline (answered [`RpcStatus::DEADLINE_EXCEEDED`]). A handler
    /// that panics is answered [`RpcStatus::HANDLER_PANICKED`]. Answers are
    /// written by one writer per connection, so frames never interleave.
    ///
    /// # Panics
    /// If `config.max_frame_bytes` cannot hold a request head or exceeds
    /// [`MAX_FRAME_LIMIT`](crate::MAX_FRAME_LIMIT), or
    /// `config.max_concurrent_per_connection` is zero or above
    /// [`Semaphore::MAX_PERMITS`].
    #[must_use = "dropping the handle shuts the server down"]
    pub fn spawn_with<B, H, F>(
        plane: DataPlane<B>,
        handler: H,
        config: RpcServerConfig,
    ) -> RpcServerHandle
    where
        B: BulkTransport,
        H: Fn(NodeId, Bytes) -> F + Clone + Send + 'static,
        F: Future<Output = Result<Bytes, RpcStatus>> + Send + 'static,
    {
        check_frame_limit(config.max_frame_bytes);
        assert!(
            (1..=Semaphore::MAX_PERMITS).contains(&config.max_concurrent_per_connection),
            "max_concurrent_per_connection must be within 1..={}",
            Semaphore::MAX_PERMITS
        );
        let connections = Arc::new(AtomicUsize::new(0));
        let task = tokio::spawn(accept_loop(plane, handler, config, connections.clone()));
        RpcServerHandle {
            task: task.abort_handle(),
            connections,
        }
    }
}

/// A running [`RpcServer`]. Dropping it shuts the server down.
#[derive(Debug)]
pub struct RpcServerHandle {
    task: AbortHandle,
    connections: Arc<AtomicUsize>,
}

impl RpcServerHandle {
    /// Stops accepting and aborts every connection with its running
    /// handlers. Callers in flight see their connection drop. The abort
    /// completes asynchronously; [`connections`](Self::connections) reaches
    /// zero once it has. Idempotent.
    pub fn shutdown(&self) {
        self.task.abort();
    }

    /// Connections currently being served.
    #[must_use]
    pub fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }
}

impl Drop for RpcServerHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Counts one live connection for as long as it exists — including a task
/// aborted before its first poll, since the guard is moved into the future.
struct Live(Arc<AtomicUsize>);

impl Live {
    fn enter(count: &Arc<AtomicUsize>) -> Self {
        count.fetch_add(1, Ordering::SeqCst);
        Self(count.clone())
    }
}

impl Drop for Live {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Accepts connections until aborted. Every connection task lives in the
/// set, so aborting this task (dropping the set) aborts them all.
async fn accept_loop<B, H, F>(
    plane: DataPlane<B>,
    handler: H,
    config: RpcServerConfig,
    connections: Arc<AtomicUsize>,
) where
    B: BulkTransport,
    H: Fn(NodeId, Bytes) -> F + Clone + Send + 'static,
    F: Future<Output = Result<Bytes, RpcStatus>> + Send + 'static,
{
    let mut served = JoinSet::new();
    let mut backoff = ACCEPT_BACKOFF_MIN;
    loop {
        // Reap finished connections; the set stays bounded by the live ones.
        while served.try_join_next().is_some() {}
        if let Ok((from, stream)) = plane.accept().await {
            backoff = ACCEPT_BACKOFF_MIN;
            let live = Live::enter(&connections);
            served.spawn(serve(from, stream, handler.clone(), config, live));
        } else {
            // A failed handshake from one peer must not end the server; a
            // transport that keeps failing is retried at a bounded rate.
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(ACCEPT_BACKOFF_MAX);
        }
    }
}

/// Serves one connection: requests are read and dispatched, answers flow
/// through a single writer. Either side ending ends the connection, and with
/// it every handler still running for it.
async fn serve<S, H, F>(
    from: NodeId,
    stream: DataStream<S>,
    handler: H,
    config: RpcServerConfig,
    _live: Live,
) where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    H: Fn(NodeId, Bytes) -> F + Clone + Send + 'static,
    F: Future<Output = Result<Bytes, RpcStatus>> + Send + 'static,
{
    let (read_half, write_half) = stream.into_inner().split();
    let mut reader = DataStream::new(read_half);
    let mut writer = DataStream::new(write_half);
    let (answers, queue) = mpsc::channel(config.max_concurrent_per_connection);
    tokio::select! {
        () = read_requests(&mut reader, from, handler, config, answers) => {}
        () = write_frames(&mut writer, queue, config.max_frame_bytes) => {}
    }
}

/// Reads requests and spawns one bounded task per request. Returns on a
/// clean end, a read error, or any frame that is not a well-formed request.
async fn read_requests<R, H, F>(
    reader: &mut DataStream<R>,
    from: NodeId,
    handler: H,
    config: RpcServerConfig,
    answers: mpsc::Sender<crate::codec::Encoded>,
) where
    R: AsyncRead + Unpin,
    H: Fn(NodeId, Bytes) -> F + Clone + Send + 'static,
    F: Future<Output = Result<Bytes, RpcStatus>> + Send + 'static,
{
    let permits = Arc::new(Semaphore::new(config.max_concurrent_per_connection));
    // Dropped with this future, which aborts every handler still running.
    let mut work = JoinSet::new();
    loop {
        while work.try_join_next().is_some() {}
        // Take a slot before reading, so a saturated connection stops being
        // read and the caller feels the back-pressure.
        let Ok(permit) = permits.clone().acquire_owned().await else {
            return;
        };
        let Ok(Some(bytes)) = reader.recv_bounded(config.max_frame_bytes).await else {
            return;
        };
        let received = Instant::now();
        let Ok(Frame::Request {
            id,
            deadline_ms,
            payload,
        }) = Frame::decode(&bytes)
        else {
            return;
        };
        let deadline = received + Duration::from_millis(u64::from(deadline_ms));
        work.spawn(answer(
            Request {
                from: from.clone(),
                id,
                deadline,
                payload,
            },
            handler.clone(),
            answers.clone(),
            permit,
            config.max_frame_bytes,
        ));
    }
}

/// One decoded request, stamped with its server-side deadline.
struct Request {
    from: NodeId,
    id: u64,
    deadline: Instant,
    payload: Bytes,
}

/// Runs one handler within its deadline and queues its answer.
async fn answer<H, F>(
    request: Request,
    handler: H,
    answers: mpsc::Sender<crate::codec::Encoded>,
    _permit: OwnedSemaphorePermit,
    max_frame_bytes: usize,
) where
    H: Fn(NodeId, Bytes) -> F + Send + 'static,
    F: Future<Output = Result<Bytes, RpcStatus>> + Send + 'static,
{
    let Request {
        from,
        id,
        deadline,
        payload,
    } = request;
    let deadline_exceeded = || RpcStatus::new(RpcStatus::DEADLINE_EXCEEDED, "deadline exceeded");
    let outcome = if Instant::now() >= deadline {
        Err(deadline_exceeded())
    } else {
        // The call itself sits inside the guarded future, so a handler that
        // panics before returning its future is caught too.
        let run = AssertUnwindSafe(async move { handler(from, payload).await }).catch_unwind();
        match timeout_at(deadline, run).await {
            Err(_) => Err(deadline_exceeded()),
            Ok(Err(_panic)) => Err(RpcStatus::new(
                RpcStatus::HANDLER_PANICKED,
                "handler panicked",
            )),
            Ok(Ok(outcome)) => outcome,
        }
    };
    let frame = match outcome {
        Ok(body) if body.len() <= max_frame_bytes - RESPONSE_HEAD => {
            Frame::Response { id, payload: body }
        }
        Ok(body) => Frame::Error {
            id,
            status: RpcStatus::new(
                RpcStatus::RESPONSE_TOO_LARGE,
                format!(
                    "response of {} bytes exceeds the {max_frame_bytes}-byte frame limit",
                    body.len()
                ),
            )
            .truncated(max_frame_bytes - ERROR_HEAD),
        },
        Err(status) => Frame::Error {
            id,
            status: status.truncated(max_frame_bytes - ERROR_HEAD),
        },
    };
    // The writer is gone only when the connection is; nobody to tell.
    let _ = answers.send(frame.encode()).await;
}
