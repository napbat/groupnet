//! Ordered byte streams backed by the node's existing pinned TLS tunnel plane.

use std::{
    io,
    sync::{Arc, Mutex},
};

use groupnet_core::NodeId;
use groupnet_network::tunnel::{TunnelTransport, TunneledStream};
use groupnet_transport::bulk::BulkTransport;
use tokio_util::sync::CancellationToken;

use crate::SessionProtocol;

#[derive(Debug)]
struct Inner {
    tunnels: TunnelTransport,
    cancel: CancellationToken,
    sessions: Mutex<Vec<CancellationToken>>,
}

impl Inner {
    fn shutdown(&self) {
        // Registration takes the same lock, so no session can escape a shutdown
        // racing completion of its authenticated handshake.
        let mut sessions = self
            .sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.cancel.cancel();
        for session in sessions.drain(..) {
            session.cancel();
        }
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// A cheap, clone-backed ordered session endpoint using pinned TLS tunnels.
///
/// Clones share endpoint shutdown and established-session ownership. The
/// default node bulk APIs and this endpoint share the ordered accept queue;
/// neither can receive an unordered session's authenticated setup channel.
/// Shutting down or dropping the last endpoint handle cancels its own sessions
/// and blocked calls, not the shared node, tunnel transport or other endpoints.
#[derive(Clone, Debug)]
pub struct OrderedProtocol {
    inner: Arc<Inner>,
}

impl OrderedProtocol {
    /// Wraps an existing tunnel transport without creating a dispatcher or socket.
    #[must_use]
    pub fn new(tunnels: TunnelTransport) -> Self {
        let cancel = tunnels.cancellation();
        Self {
            inner: Arc::new(Inner {
                tunnels,
                cancel,
                sessions: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Establishes an authenticated, reliable, ordered byte stream.
    ///
    /// # Errors
    /// Returns tunnel admission, capacity, authentication, setup or shutdown errors.
    pub async fn connect(&self, to: &NodeId, (): ()) -> io::Result<TunneledStream> {
        let stream = tokio::select! {
            biased;
            () = self.inner.cancel.cancelled() => return Err(closed()),
            stream = self.inner.tunnels.connect(to) => stream?,
        };
        self.register(&stream)?;
        Ok(stream)
    }

    /// Accepts an ordered stream and its authenticated logical peer identity.
    ///
    /// # Errors
    /// Returns tunnel admission, state-lock or shutdown errors.
    pub async fn accept(&self) -> io::Result<(NodeId, TunneledStream)> {
        let (peer, stream) = tokio::select! {
            biased;
            () = self.inner.cancel.cancelled() => return Err(closed()),
            accepted = self.inner.tunnels.accept() => accepted?,
        };
        self.register(&stream)?;
        Ok((peer, stream))
    }

    /// Cancels this endpoint's calls and streams without shutting down the node.
    pub fn shutdown(&self) {
        self.inner.shutdown();
    }

    /// Waits for endpoint or node shutdown.
    pub async fn closed(&self) {
        self.inner.cancel.cancelled().await;
    }

    fn register(&self, stream: &TunneledStream) -> io::Result<()> {
        let mut sessions = self
            .inner
            .sessions
            .lock()
            .map_err(|_| io::Error::other("ordered session lock poisoned"))?;
        if self.inner.cancel.is_cancelled() {
            return Err(closed());
        }
        // Native tunnel capacity bounds live entries; dropped/revoked streams
        // cancel their token and are removed before any new entry is appended.
        sessions.retain(|session| !session.is_cancelled());
        sessions.push(stream.cancellation());
        Ok(())
    }
}

impl SessionProtocol for OrderedProtocol {
    type Session = TunneledStream;
    type ConnectOptions = ();

    async fn connect(&self, to: &NodeId, options: ()) -> io::Result<Self::Session> {
        Self::connect(self, to, options).await
    }

    async fn accept(&self) -> io::Result<(NodeId, Self::Session)> {
        Self::accept(self).await
    }
}

fn closed() -> io::Error {
    io::Error::new(io::ErrorKind::ConnectionAborted, "ordered endpoint closed")
}
