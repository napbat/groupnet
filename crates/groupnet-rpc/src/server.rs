//! [`RpcServer`]: the answering side — owns a plane's `accept`, admits a
//! bounded number of connections and runs one bounded task set per
//! connection.

use std::io;
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

use crate::codec::{ERROR_HEAD, Encoded, Frame, RESPONSE_HEAD};
use crate::config::{FrameLimit, RpcServerConfig};
use crate::{RpcStatus, write_frames};

/// The first pause after a failed `accept`, doubled per consecutive failure.
const ACCEPT_BACKOFF_MIN: Duration = Duration::from_millis(10);
/// The longest pause between `accept` retries.
const ACCEPT_BACKOFF_MAX: Duration = Duration::from_secs(1);

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
        start(plane, handler, RpcServerConfig::default())
    }

    /// [`spawn`](Self::spawn) with explicit limits.
    ///
    /// At most `max_connections` connections are served at once; at the
    /// limit the server stops accepting until one ends. Each request runs as
    /// its own task, at most `max_concurrent_per_connection` per connection,
    /// and is abandoned at its deadline (answered
    /// [`RpcStatus::DEADLINE_EXCEEDED`]). A handler that panics is answered
    /// [`RpcStatus::HANDLER_PANICKED`]. Answers are written by one writer per
    /// connection, so frames never interleave. A connection with no request
    /// read and no handler running for `idle_timeout` is closed once its
    /// answers are written.
    ///
    /// # Errors
    /// `InvalidInput` if `config` fails [`RpcServerConfig::validate`].
    pub fn spawn_with<B, H, F>(
        plane: DataPlane<B>,
        handler: H,
        config: RpcServerConfig,
    ) -> io::Result<RpcServerHandle>
    where
        B: BulkTransport,
        H: Fn(NodeId, Bytes) -> F + Clone + Send + 'static,
        F: Future<Output = Result<Bytes, RpcStatus>> + Send + 'static,
    {
        config.validate()?;
        Ok(start(plane, handler, config))
    }
}

/// Spawns the accept loop for an already validated configuration.
fn start<B, H, F>(plane: DataPlane<B>, handler: H, config: RpcServerConfig) -> RpcServerHandle
where
    B: BulkTransport,
    H: Fn(NodeId, Bytes) -> F + Clone + Send + 'static,
    F: Future<Output = Result<Bytes, RpcStatus>> + Send + 'static,
{
    let connections = Arc::new(AtomicUsize::new(0));
    let task = tokio::spawn(accept_loop(plane, handler, config, connections.clone()));
    RpcServerHandle {
        task: task.abort_handle(),
        connections,
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

/// Counts one live connection and holds its admission slot for as long as
/// it exists — including a task aborted before its first poll, since the
/// guard is moved into the future.
struct Live {
    count: Arc<AtomicUsize>,
    _slot: OwnedSemaphorePermit,
}

impl Live {
    fn enter(count: &Arc<AtomicUsize>, slot: OwnedSemaphorePermit) -> Self {
        count.fetch_add(1, Ordering::SeqCst);
        Self {
            count: count.clone(),
            _slot: slot,
        }
    }
}

impl Drop for Live {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Accepts connections until aborted, admitting at most `max_connections`
/// at once: a slot is taken before `accept`, so a full server pushes back on
/// dialers through the transport. Every connection task lives in the set, so
/// aborting this task (dropping the set) aborts them all.
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
    let admission = Arc::new(Semaphore::new(config.max_connections.get()));
    let mut served = JoinSet::new();
    let mut backoff = ACCEPT_BACKOFF_MIN;
    loop {
        // Reap finished connections; the set stays bounded by the live ones.
        while served.try_join_next().is_some() {}
        let Ok(slot) = admission.clone().acquire_owned().await else {
            return;
        };
        if let Ok((from, stream)) = plane.accept().await {
            backoff = ACCEPT_BACKOFF_MIN;
            let live = Live::enter(&connections, slot);
            served.spawn(serve(from, stream, handler.clone(), config, live));
        } else {
            drop(slot);
            // A failed handshake from one peer must not end the server; a
            // transport that keeps failing is retried at a bounded rate.
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(ACCEPT_BACKOFF_MAX);
        }
    }
}

/// How a connection's request reader ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReadEnd {
    /// A clean end, a read error, or a frame that is not a request.
    Closed,
    /// No request and no running handler for the idle timeout.
    Idle,
}

/// Serves one connection: requests are read and dispatched, answers flow
/// through a single writer. Either side ending ends the connection, and with
/// it every handler still running for it; an idle connection first writes
/// the answers its finished handlers queued.
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
    let (answers, mut queue) = mpsc::channel(config.max_concurrent_per_connection.get());
    let end = tokio::select! {
        end = read_requests(&mut reader, from, handler, config, answers) => end,
        () = write_frames(&mut writer, &mut queue, config.max_frame_bytes) => ReadEnd::Closed,
    };
    if end == ReadEnd::Idle {
        // Every handler has finished and every answer sender is gone, so the
        // writer drains the queue and returns.
        write_frames(&mut writer, &mut queue, config.max_frame_bytes).await;
    }
}

/// Resolves once every running handler has finished and `idle` has then
/// passed with nothing new to do.
async fn idle_out(work: &mut JoinSet<()>, idle: Duration) {
    while work.join_next().await.is_some() {}
    tokio::time::sleep(idle).await;
}

/// Reads requests and spawns one bounded task per request. Returns on a
/// clean end, a read error, any frame that is not a well-formed request, or
/// idleness.
async fn read_requests<R, H, F>(
    reader: &mut DataStream<R>,
    from: NodeId,
    handler: H,
    config: RpcServerConfig,
    answers: mpsc::Sender<Encoded>,
) -> ReadEnd
where
    R: AsyncRead + Unpin,
    H: Fn(NodeId, Bytes) -> F + Clone + Send + 'static,
    F: Future<Output = Result<Bytes, RpcStatus>> + Send + 'static,
{
    let permits = Arc::new(Semaphore::new(config.max_concurrent_per_connection.get()));
    // Dropped with this future, which aborts every handler still running.
    let mut work = JoinSet::new();
    loop {
        while work.try_join_next().is_some() {}
        // Take a slot before reading, so a saturated connection stops being
        // read and the caller feels the back-pressure.
        let Ok(permit) = permits.clone().acquire_owned().await else {
            return ReadEnd::Closed;
        };
        let read = tokio::select! {
            read = reader.recv_bounded(config.max_frame_bytes.get()) => read,
            () = idle_out(&mut work, config.idle_timeout) => return ReadEnd::Idle,
        };
        let Ok(Some(bytes)) = read else {
            return ReadEnd::Closed;
        };
        let received = Instant::now();
        let Ok(Frame::Request {
            id,
            deadline_ms,
            payload,
        }) = Frame::decode(&bytes)
        else {
            return ReadEnd::Closed;
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
    answers: mpsc::Sender<Encoded>,
    _permit: OwnedSemaphorePermit,
    max_frame_bytes: FrameLimit,
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
    let max_message = max_frame_bytes.body(ERROR_HEAD);
    let frame = match outcome {
        Ok(body) if body.len() <= max_frame_bytes.body(RESPONSE_HEAD) => {
            Frame::Response { id, payload: body }
        }
        Ok(body) => Frame::Error {
            id,
            status: RpcStatus::new(
                RpcStatus::RESPONSE_TOO_LARGE,
                format!(
                    "response of {} bytes exceeds the {}-byte frame limit",
                    body.len(),
                    max_frame_bytes.get()
                ),
            )
            .truncated(max_message),
        },
        Err(status) => Frame::Error {
            id,
            status: status.truncated(max_message),
        },
    };
    // The writer is gone only when the connection is; nobody to tell.
    let _ = answers.send(frame.encode()).await;
}
