//! [`RpcClient`]: concurrent calls multiplexed onto one stream per peer.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use futures_util::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use groupnet_core::NodeId;
use groupnet_transport::bulk::{BulkTransport, DataPlane, DataStream};
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::task::AbortHandle;
use tokio::time::{Instant, timeout_at};

use crate::codec::{Frame, REQUEST_HEAD};
use crate::{DEFAULT_MAX_FRAME_BYTES, RpcError, RpcStatus, check_frame_limit, write_frames};

/// Client limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RpcConfig {
    /// How long opening a connection to a peer may take before the call
    /// fails [`Unreachable`](RpcError::Unreachable). A call's own timeout
    /// still bounds it (and then fails [`Timeout`](RpcError::Timeout)).
    pub connect_timeout: Duration,
    /// The largest RPC frame (head included) this client sends or accepts.
    /// Match the server's [`RpcServerConfig::max_frame_bytes`](crate::RpcServerConfig::max_frame_bytes).
    pub max_frame_bytes: usize,
    /// Concurrent calls allowed to one peer; more wait for a slot within
    /// their own timeout.
    pub max_in_flight_per_peer: usize,
}

impl Default for RpcConfig {
    /// 3 s to connect, 16 MiB frames, 1024 calls in flight per peer.
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(3),
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            max_in_flight_per_peer: 1024,
        }
    }
}

/// The calling side of the RPC layer. Cheap to clone; every clone shares the
/// same connections.
///
/// Connections are opened lazily, one per peer, on the first call to it, and
/// reused by every later call. A connection that breaks fails each call in
/// flight on it with [`RpcError::ConnectionLost`] and is replaced by the next
/// call. Dropping the last clone closes every connection.
pub struct RpcClient<B: BulkTransport> {
    inner: Arc<Inner<B>>,
}

impl<B: BulkTransport> Clone for RpcClient<B> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<B: BulkTransport> fmt::Debug for RpcClient<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RpcClient")
            .field("config", &self.inner.config)
            .field("shut_down", &self.inner.is_shut())
            .finish_non_exhaustive()
    }
}

struct Inner<B: BulkTransport> {
    plane: DataPlane<B>,
    config: RpcConfig,
    peers: Mutex<HashMap<NodeId, Arc<Peer>>>,
    next_id: AtomicU64,
    shut: AtomicBool,
}

/// One peer's slot: its call budget and its (at most one) connection.
struct Peer {
    in_flight: Arc<Semaphore>,
    /// Serializes dialing, so concurrent first calls open one connection.
    dialing: tokio::sync::Mutex<()>,
    conn: Mutex<Option<Arc<Conn>>>,
}

/// One live connection: the writer's queue, the calls awaiting answers, and
/// the task that drives both halves of the stream.
struct Conn {
    frames: mpsc::Sender<crate::codec::Encoded>,
    calls: Arc<Calls>,
    task: AbortHandle,
}

/// The calls awaiting an answer on one connection.
#[derive(Default)]
struct Calls {
    state: Mutex<CallsState>,
}

#[derive(Default)]
struct CallsState {
    /// Set once the connection is gone; nothing registers after it.
    closed: bool,
    waiting: HashMap<u64, oneshot::Sender<Result<Bytes, RpcStatus>>>,
}

/// Locks a mutex whose data stays consistent across a panic: every critical
/// section here is a single map or flag update.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl<B: BulkTransport> RpcClient<B> {
    /// A client that opens its connections on `plane`.
    ///
    /// # Panics
    /// If `config.max_frame_bytes` cannot hold a request head or exceeds
    /// [`MAX_FRAME_LIMIT`](crate::MAX_FRAME_LIMIT), or
    /// `config.max_in_flight_per_peer` is zero or above
    /// [`Semaphore::MAX_PERMITS`].
    #[must_use]
    pub fn new(plane: DataPlane<B>, config: RpcConfig) -> Self {
        check_frame_limit(config.max_frame_bytes);
        assert!(
            (1..=Semaphore::MAX_PERMITS).contains(&config.max_in_flight_per_peer),
            "max_in_flight_per_peer must be within 1..={}",
            Semaphore::MAX_PERMITS
        );
        Self {
            inner: Arc::new(Inner {
                plane,
                config,
                peers: Mutex::new(HashMap::new()),
                next_id: AtomicU64::new(0),
                shut: AtomicBool::new(false),
            }),
        }
    }

    /// Calls `to` with `payload` and waits up to `timeout` for its answer.
    ///
    /// The server receives the remaining budget as the request's deadline
    /// and drops the work once it passes. Dropping the returned future
    /// abandons the call (a late answer is discarded).
    ///
    /// # Errors
    /// See [`RpcError`]: [`TooLarge`](RpcError::TooLarge) and
    /// [`Unreachable`](RpcError::Unreachable) mean the request was not sent;
    /// [`Timeout`](RpcError::Timeout) and
    /// [`ConnectionLost`](RpcError::ConnectionLost) mean its outcome is
    /// unknown; [`Remote`](RpcError::Remote) carries the handler's error.
    pub async fn call(
        &self,
        to: &NodeId,
        payload: Bytes,
        timeout: Duration,
    ) -> Result<Bytes, RpcError> {
        let inner = &*self.inner;
        if inner.is_shut() {
            return Err(RpcError::Shutdown);
        }
        if payload.len() > inner.config.max_frame_bytes - REQUEST_HEAD {
            return Err(RpcError::TooLarge);
        }
        // The deadline travels as u32 milliseconds; a longer timeout is
        // clamped to it (~49 days).
        let timeout = timeout.min(Duration::from_millis(u64::from(u32::MAX)));
        if timeout.is_zero() {
            return Err(RpcError::Timeout);
        }
        let deadline = Instant::now() + timeout;
        let peer = inner.peer(to);
        let _slot = match timeout_at(deadline, peer.in_flight.clone().acquire_owned()).await {
            Err(_) => return Err(RpcError::Timeout),
            Ok(Err(_closed)) => return Err(RpcError::Shutdown),
            Ok(Ok(slot)) => slot,
        };
        loop {
            let conn = inner.connection(to, &peer, deadline).await?;
            let id = inner.next_id.fetch_add(1, Ordering::Relaxed);
            // A connection that died since it was handed out has not seen
            // this request: dial again, within the same deadline.
            let Some(answer) = conn.calls.register(id) else {
                continue;
            };
            let _waiting = Waiting {
                calls: &conn.calls,
                id,
            };
            let remaining = deadline.saturating_duration_since(Instant::now());
            let deadline_ms = u32::try_from(remaining.as_millis())
                .unwrap_or(u32::MAX)
                .max(1);
            let frame = Frame::Request {
                id,
                deadline_ms,
                payload: payload.clone(),
            }
            .encode();
            match timeout_at(deadline, conn.frames.send(frame)).await {
                Err(_) => return Err(RpcError::Timeout),
                // The writer is gone and never took the frame: unsent.
                Ok(Err(_)) => continue,
                Ok(Ok(())) => {}
            }
            return match timeout_at(deadline, answer).await {
                Err(_) => Err(RpcError::Timeout),
                Ok(Err(_)) if inner.is_shut() => Err(RpcError::Shutdown),
                Ok(Err(_)) => Err(RpcError::ConnectionLost),
                Ok(Ok(Ok(body))) => Ok(body),
                Ok(Ok(Err(status))) if status.code == RpcStatus::DEADLINE_EXCEEDED => {
                    Err(RpcError::Timeout)
                }
                Ok(Ok(Err(status))) => Err(RpcError::Remote(status)),
            };
        }
    }

    /// Closes every connection and fails every call — in flight, waiting or
    /// future — with [`RpcError::Shutdown`]. Idempotent.
    pub fn shutdown(&self) {
        self.inner.shut.store(true, Ordering::SeqCst);
        let peers: Vec<Arc<Peer>> = lock(&self.inner.peers).values().cloned().collect();
        for peer in peers {
            peer.in_flight.close();
            if let Some(conn) = lock(&peer.conn).take() {
                conn.close();
            }
        }
    }
}

impl<B: BulkTransport> Inner<B> {
    fn is_shut(&self) -> bool {
        self.shut.load(Ordering::SeqCst)
    }

    fn peer(&self, to: &NodeId) -> Arc<Peer> {
        lock(&self.peers)
            .entry(to.clone())
            .or_insert_with(|| {
                Arc::new(Peer {
                    in_flight: Arc::new(Semaphore::new(self.config.max_in_flight_per_peer)),
                    dialing: tokio::sync::Mutex::new(()),
                    conn: Mutex::new(None),
                })
            })
            .clone()
    }

    /// The peer's live connection, dialing one if there is none.
    async fn connection(
        &self,
        to: &NodeId,
        peer: &Peer,
        deadline: Instant,
    ) -> Result<Arc<Conn>, RpcError> {
        if let Some(conn) = peer.live() {
            return Ok(conn);
        }
        let Ok(_dialing) = timeout_at(deadline, peer.dialing.lock()).await else {
            return Err(RpcError::Timeout);
        };
        if let Some(conn) = peer.live() {
            return Ok(conn);
        }
        if self.is_shut() {
            return Err(RpcError::Shutdown);
        }
        let connect_by = Instant::now() + self.config.connect_timeout;
        let stream = match timeout_at(connect_by.min(deadline), self.plane.connect(to)).await {
            Ok(Ok(stream)) => stream,
            Ok(Err(_)) => return Err(RpcError::Unreachable),
            Err(_) if connect_by <= deadline => return Err(RpcError::Unreachable),
            Err(_) => return Err(RpcError::Timeout),
        };
        let conn = Arc::new(Conn::spawn(stream, &self.config));
        let mut slot = lock(&peer.conn);
        // Checked under the slot lock that `shutdown` takes after setting the
        // flag, so a connection is either seen and closed there or refused here.
        if self.is_shut() {
            drop(slot);
            conn.close();
            return Err(RpcError::Shutdown);
        }
        *slot = Some(conn.clone());
        Ok(conn)
    }
}

impl Peer {
    /// The current connection, if it is still open.
    fn live(&self) -> Option<Arc<Conn>> {
        let mut slot = lock(&self.conn);
        match slot.as_ref() {
            Some(conn) if !conn.calls.is_closed() => Some(conn.clone()),
            Some(_) => {
                *slot = None;
                None
            }
            None => None,
        }
    }
}

impl Conn {
    fn spawn<S>(stream: DataStream<S>, config: &RpcConfig) -> Self
    where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        let (frames, queue) = mpsc::channel(config.max_in_flight_per_peer);
        let calls = Arc::new(Calls::default());
        let task = tokio::spawn(drive(stream, queue, calls.clone(), config.max_frame_bytes))
            .abort_handle();
        Self {
            frames,
            calls,
            task,
        }
    }

    /// Tears the connection down and fails its waiting calls.
    fn close(&self) {
        self.task.abort();
        self.calls.close();
    }
}

impl Drop for Conn {
    fn drop(&mut self) {
        self.close();
    }
}

impl Calls {
    /// Registers call `id`, unless the connection is already gone.
    fn register(&self, id: u64) -> Option<oneshot::Receiver<Result<Bytes, RpcStatus>>> {
        let mut state = lock(&self.state);
        if state.closed {
            return None;
        }
        let (tx, rx) = oneshot::channel();
        state.waiting.insert(id, tx);
        Some(rx)
    }

    /// Hands call `id` its answer; an answer nobody waits for is discarded.
    fn answer(&self, id: u64, answer: Result<Bytes, RpcStatus>) {
        let waiter = lock(&self.state).waiting.remove(&id);
        if let Some(waiter) = waiter {
            let _ = waiter.send(answer);
        }
    }

    fn forget(&self, id: u64) {
        lock(&self.state).waiting.remove(&id);
    }

    fn is_closed(&self) -> bool {
        lock(&self.state).closed
    }

    /// Marks the connection gone and drops every waiter, which wakes each
    /// with a closed channel.
    fn close(&self) {
        let waiting = {
            let mut state = lock(&self.state);
            state.closed = true;
            std::mem::take(&mut state.waiting)
        };
        drop(waiting);
    }
}

/// Removes a call's waiter however the call ends — answered, timed out, or
/// abandoned by its caller.
struct Waiting<'a> {
    calls: &'a Calls,
    id: u64,
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.calls.forget(self.id);
    }
}

/// Closes a connection's call table when its task ends, however it ends.
struct CloseOnExit(Arc<Calls>);

impl Drop for CloseOnExit {
    fn drop(&mut self) {
        self.0.close();
    }
}

/// Drives one client connection: the writer drains the request queue while
/// the reader routes answers to their calls. Either side failing ends both.
async fn drive<S>(
    stream: DataStream<S>,
    queue: mpsc::Receiver<crate::codec::Encoded>,
    calls: Arc<Calls>,
    max_frame_bytes: usize,
) where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let _close = CloseOnExit(calls.clone());
    let (read_half, write_half) = stream.into_inner().split();
    let mut reader = DataStream::new(read_half);
    let mut writer = DataStream::new(write_half);
    let read = async {
        // A clean end, a read error, a malformed frame or a request from
        // the server all end the connection.
        while let Ok(Some(bytes)) = reader.recv_bounded(max_frame_bytes).await {
            match Frame::decode(&bytes) {
                Ok(Frame::Response { id, payload }) => calls.answer(id, Ok(payload)),
                Ok(Frame::Error { id, status }) => calls.answer(id, Err(status)),
                Ok(Frame::Request { .. }) | Err(_) => return,
            }
        }
    };
    tokio::select! {
        () = read => {}
        () = write_frames(&mut writer, queue, max_frame_bytes) => {}
    }
}
