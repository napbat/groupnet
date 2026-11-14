//! The listener task every [`TcpBulkTransport`](super::TcpBulkTransport) mode
//! shares: accepted connections run their mode's handshake in their own task
//! under the handshake deadline, bounded in number, and completed handshakes
//! queue for `accept`.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use groupnet_core::NodeId;
use groupnet_transport::QueueCapacity;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex as AsyncMutex, OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::timeout;

use super::Identity;

/// One attributed connection, or a listener error for `accept` to return.
type Accepted<S> = io::Result<(NodeId, S)>;

/// The accept-side bounds of one listener.
#[derive(Clone, Copy, Debug)]
pub(super) struct Bounds {
    /// Accepted connections held at once between `accept(2)` and `accept`.
    pub(super) max_handshakes: QueueCapacity,
    /// Handshaken connections buffered until `accept` takes them.
    pub(super) accept_queue: QueueCapacity,
    /// Accepted connections alive at once, when the mode bounds them: the
    /// permit travels into the handshake and stays with the stream.
    pub(super) max_connections: Option<QueueCapacity>,
}

/// The listener task and the queue of connections it has attributed.
#[derive(Debug)]
pub(super) struct Listener<S> {
    accepted: AsyncMutex<mpsc::Receiver<Accepted<S>>>,
    task: JoinHandle<()>,
}

impl<S> Drop for Listener<S> {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl<S: Send + 'static> Listener<S> {
    /// Starts the accept loop on `listener`. `handshake` attributes one
    /// accepted connection (its remote address and, when the mode bounds live
    /// connections, its connection permit given); it runs under
    /// `identity.handshake_timeout`.
    pub(super) fn spawn<H, F>(
        listener: TcpListener,
        bounds: Bounds,
        identity: Arc<Identity>,
        handshake: H,
    ) -> Self
    where
        H: Fn(TcpStream, SocketAddr, Option<OwnedSemaphorePermit>) -> F + Send + 'static,
        F: Future<Output = Accepted<S>> + Send + 'static,
    {
        let (queue, accepted) = mpsc::channel(bounds.accept_queue.get());
        let task = tokio::spawn(accept_loop(listener, queue, bounds, identity, handshake));
        Self {
            accepted: AsyncMutex::new(accepted),
            task,
        }
    }

    /// The next attributed connection, or the listener's error.
    pub(super) async fn accept(&self) -> io::Result<(NodeId, S)> {
        self.accepted
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "TCP bulk listener stopped"))?
    }
}

/// Accepts connections while handshake slots (and connection slots, when
/// bounded) are free, giving each its own deadline-bounded handshake task.
/// Listener errors are forwarded to `accept`; handshake failures only drop
/// their own connection. Pending handshakes are owned here and cancelled with
/// the loop.
async fn accept_loop<S, H, F>(
    listener: TcpListener,
    queue: mpsc::Sender<Accepted<S>>,
    bounds: Bounds,
    identity: Arc<Identity>,
    handshake: H,
) where
    S: Send + 'static,
    H: Fn(TcpStream, SocketAddr, Option<OwnedSemaphorePermit>) -> F,
    F: Future<Output = Accepted<S>> + Send + 'static,
{
    let handshakes_free = Arc::new(Semaphore::new(bounds.max_handshakes.get()));
    let connections_free = bounds
        .max_connections
        .map(|max| Arc::new(Semaphore::new(max.get())));
    let mut handshakes = JoinSet::new();
    loop {
        while handshakes.try_join_next().is_some() {}
        let Ok(slot) = handshakes_free.clone().acquire_owned().await else {
            return;
        };
        let connection = match &connections_free {
            Some(free) => {
                let Ok(connection) = free.clone().acquire_owned().await else {
                    return;
                };
                Some(connection)
            }
            None => None,
        };
        match listener.accept().await {
            Ok((sock, remote)) => {
                if sock.set_nodelay(true).is_err() {
                    continue;
                }
                handshakes.spawn(attribute(
                    handshake(sock, remote, connection),
                    queue.clone(),
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

/// Attributes one accepted connection. The handshake slot is held until the
/// connection is queued, so stalled and unconsumed connections share one
/// bound. A handshake that fails or misses its deadline is counted before its
/// socket closes.
async fn attribute<S>(
    handshake: impl Future<Output = Accepted<S>>,
    queue: mpsc::Sender<Accepted<S>>,
    identity: Arc<Identity>,
    _slot: OwnedSemaphorePermit,
) {
    let Ok(Ok(accepted)) = timeout(identity.handshake_timeout, handshake).await else {
        identity.refused();
        return;
    };
    let _ = queue.send(Ok(accepted)).await;
}
