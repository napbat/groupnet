//! One served connection: admission bookkeeping, the bounded request reader,
//! the per-request handler tasks and the single answer writer.

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures_util::FutureExt;
use futures_util::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use groupnet_core::NodeId;
use groupnet_transport::bulk::DataStream;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::JoinSet;
use tokio::time::{Instant, timeout_at};

use crate::codec::{ERROR_HEAD, Encoded, Frame, RESPONSE_HEAD};
use crate::config::{FrameLimit, RpcServerConfig};
use crate::{RpcStatus, write_frames};

/// Counts one live connection and holds its admission slot for as long as
/// it exists — including a task aborted before its first poll, since the
/// guard is moved into the future.
pub(super) struct Live {
    count: Arc<AtomicUsize>,
    _slot: OwnedSemaphorePermit,
}

impl Live {
    pub(super) fn enter(count: &Arc<AtomicUsize>, slot: OwnedSemaphorePermit) -> Self {
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
pub(super) async fn serve<S, H, F>(
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
            read = reader.recv_bounded(config.max_frame_bytes.frame_bytes()) => read,
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
