//! [`RpcClient`]: concurrent calls multiplexed onto one stream per peer.

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use futures_util::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use groupnet_core::NodeId;
use groupnet_transport::bulk::{BulkTransport, DataPlane, DataStream};
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::task::AbortHandle;
use tokio::time::{Instant, sleep_until, timeout_at};

use crate::codec::{Encoded, Frame, REQUEST_HEAD};
use crate::config::{FrameLimit, MAX_TIMEOUT, RpcConfig};
use crate::{RpcError, RpcStatus, write_frames};

/// The calling side of the RPC layer. Cheap to clone; every clone shares the
/// same connections.
///
/// Connections are opened lazily, one per peer, on the first call to it, and
/// reused by every later call. A connection that breaks fails each call in
/// flight on it with [`RpcError::ConnectionLost`] and is replaced by the next
/// call; one with no call waiting for [`RpcConfig::idle_timeout`] is closed.
/// At most [`RpcConfig::max_peers`] destinations are tracked at once.
/// Dropping the last clone closes every connection.
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
    /// Admitted destinations. A peer is in use while a call holds a clone of
    /// its `Arc`; clones are only handed out under this lock.
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
    frames: mpsc::Sender<Encoded>,
    calls: Arc<Calls>,
    task: AbortHandle,
}

/// The calls awaiting an answer on one connection.
struct Calls {
    state: Mutex<CallsState>,
}

struct CallsState {
    /// Set once the connection is gone; nothing registers after it.
    closed: bool,
    waiting: HashMap<u64, oneshot::Sender<Result<Bytes, RpcStatus>>>,
    /// When `waiting` last became empty (or the connection opened).
    idle_since: Instant,
}

/// Locks a mutex whose data stays consistent across a panic: every critical
/// section here is a single map or flag update.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl<B: BulkTransport> RpcClient<B> {
    /// A client that opens its connections on `plane`.
    ///
    /// # Errors
    /// `InvalidInput` if `config` fails [`RpcConfig::validate`].
    pub fn new(plane: DataPlane<B>, config: RpcConfig) -> io::Result<Self> {
        config.validate()?;
        Ok(Self {
            inner: Arc::new(Inner {
                plane,
                config,
                peers: Mutex::new(HashMap::new()),
                next_id: AtomicU64::new(0),
                shut: AtomicBool::new(false),
            }),
        })
    }

    /// Calls `to` with `payload` and waits up to `timeout` for its answer.
    ///
    /// The server receives the remaining budget as the request's deadline
    /// and drops the work once it passes. A timeout above
    /// [`MAX_TIMEOUT`](crate::MAX_TIMEOUT) is clamped to it. Dropping the
    /// returned future abandons the call (a late answer is discarded).
    ///
    /// # Errors
    /// See [`RpcError`]: [`TooLarge`](RpcError::TooLarge),
    /// [`Saturated`](RpcError::Saturated) and
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
        if payload.len() > inner.config.max_frame_bytes.body(REQUEST_HEAD) {
            return Err(RpcError::TooLarge);
        }
        let timeout = timeout.min(MAX_TIMEOUT);
        if timeout.is_zero() {
            return Err(RpcError::Timeout);
        }
        let deadline = Instant::now() + timeout;
        let peer = inner.peer(to)?;
        let _slot = match timeout_at(deadline, peer.in_flight.clone().acquire_owned()).await {
            Err(_) => return Err(RpcError::Timeout),
            Ok(Err(_closed)) => return Err(RpcError::Shutdown),
            Ok(Ok(slot)) => slot,
        };
        loop {
            let conn = inner.connection(to, &peer, deadline).await?;
            let id = inner.next_id.fetch_add(1, Ordering::Relaxed);
            // A connection that died (or idled out) since it was handed out
            // has not seen this request: dial again, within the same deadline.
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
            peer.close();
        }
    }

    /// Destinations currently admitted (each with at most one connection).
    #[must_use]
    pub fn peers(&self) -> usize {
        lock(&self.inner.peers).len()
    }
}

impl<B: BulkTransport> Inner<B> {
    fn is_shut(&self) -> bool {
        self.shut.load(Ordering::SeqCst)
    }

    /// The destination's slot, admitting it under the global `max_peers`
    /// bound. At the bound, one destination no call is using is evicted —
    /// preferably one without a live connection, else the one idle longest.
    fn peer(&self, to: &NodeId) -> Result<Arc<Peer>, RpcError> {
        let mut peers = lock(&self.peers);
        if let Some(peer) = peers.get(to) {
            return Ok(peer.clone());
        }
        if peers.len() >= self.config.max_peers.get() {
            let victim = peers
                .iter()
                // Only this map holds an unused peer: no call can reach it.
                .filter(|(_, peer)| Arc::strong_count(peer) == 1)
                .min_by_key(|(_, peer)| peer.idle_since())
                .map(|(id, _)| id.clone())
                .ok_or(RpcError::Saturated)?;
            if let Some(evicted) = peers.remove(&victim) {
                evicted.close();
            }
        }
        let peer = Arc::new(Peer {
            in_flight: Arc::new(Semaphore::new(self.config.max_in_flight_per_peer.get())),
            dialing: tokio::sync::Mutex::new(()),
            conn: Mutex::new(None),
        });
        peers.insert(to.clone(), peer.clone());
        Ok(peer)
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
        // Validated at construction to lie within `MAX_TIMEOUT`.
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

    /// Eviction order: `None` (no live connection) first, then oldest idle.
    fn idle_since(&self) -> Option<Instant> {
        self.live().map(|conn| conn.calls.idle_since())
    }

    /// Closes the peer's connection, if any.
    fn close(&self) {
        if let Some(conn) = lock(&self.conn).take() {
            conn.close();
        }
    }
}

impl Conn {
    fn spawn<S>(stream: DataStream<S>, config: &RpcConfig) -> Self
    where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        let (frames, queue) = mpsc::channel(config.max_in_flight_per_peer.get());
        let calls = Arc::new(Calls::new());
        let task = tokio::spawn(drive(
            stream,
            queue,
            calls.clone(),
            config.max_frame_bytes,
            config.idle_timeout,
        ))
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
    fn new() -> Self {
        Self {
            state: Mutex::new(CallsState {
                closed: false,
                waiting: HashMap::new(),
                idle_since: Instant::now(),
            }),
        }
    }

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
        let waiter = lock(&self.state).remove(id);
        if let Some(waiter) = waiter {
            let _ = waiter.send(answer);
        }
    }

    fn forget(&self, id: u64) {
        lock(&self.state).remove(id);
    }

    fn is_closed(&self) -> bool {
        lock(&self.state).closed
    }

    fn idle_since(&self) -> Instant {
        lock(&self.state).idle_since
    }

    /// Closes the table once no call has waited on it for `idle`; otherwise
    /// returns when to check again. Closing under the lock means no call can
    /// register on a connection that is about to end.
    fn close_if_idle(&self, idle: Duration) -> Option<Instant> {
        let mut state = lock(&self.state);
        let now = Instant::now();
        if !state.waiting.is_empty() {
            return Some(now + idle);
        }
        let due = state.idle_since + idle;
        if now < due {
            return Some(due);
        }
        state.closed = true;
        None
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

impl CallsState {
    /// Removes a waiter, starting the idle clock when it was the last one.
    fn remove(&mut self, id: u64) -> Option<oneshot::Sender<Result<Bytes, RpcStatus>>> {
        let waiter = self.waiting.remove(&id);
        if waiter.is_some() && self.waiting.is_empty() {
            self.idle_since = Instant::now();
        }
        waiter
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

/// Resolves once the connection has had no call waiting for `idle`, having
/// closed its call table.
async fn idle_out(calls: &Calls, idle: Duration) {
    let mut check = calls.idle_since() + idle;
    loop {
        sleep_until(check).await;
        match calls.close_if_idle(idle) {
            Some(next) => check = next,
            None => return,
        }
    }
}

/// Drives one client connection: the writer drains the request queue while
/// the reader routes answers to their calls. Either side failing, or the
/// connection idling out, ends both.
async fn drive<S>(
    stream: DataStream<S>,
    mut queue: mpsc::Receiver<Encoded>,
    calls: Arc<Calls>,
    max_frame_bytes: FrameLimit,
    idle: Duration,
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
        while let Ok(Some(bytes)) = reader.recv_bounded(max_frame_bytes.get()).await {
            match Frame::decode(&bytes) {
                Ok(Frame::Response { id, payload }) => calls.answer(id, Ok(payload)),
                Ok(Frame::Error { id, status }) => calls.answer(id, Err(status)),
                Ok(Frame::Request { .. }) | Err(_) => return,
            }
        }
    };
    tokio::select! {
        () = read => {}
        () = write_frames(&mut writer, &mut queue, max_frame_bytes) => {}
        () = idle_out(&calls, idle) => {}
    }
}
