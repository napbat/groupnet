//! An in-process [`Transport`] over Tokio channels.
//!
//! Useful for integration tests and examples: stand up several [`Node`]s on one
//! [`Network`] and they gossip as if over a real link — minus the sockets. It
//! honours the best-effort contract (sending to an unknown peer is a silent
//! drop, never an error).
//!
//! [`Node`]: crate::Node

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use groupnet_core::NodeId;
use groupnet_transport::{Inbound, Transport};
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::mpsc;

type Peers = Arc<Mutex<HashMap<NodeId, mpsc::UnboundedSender<Inbound>>>>;

/// A shared in-process network fabric. Clone it freely; every endpoint created
/// from clones shares one routing table.
#[derive(Clone, Default, Debug)]
pub struct Network {
    peers: Peers,
}

impl Network {
    /// Creates an empty network.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates and registers a transport endpoint for `id`.
    #[must_use]
    pub fn endpoint(&self, id: NodeId) -> MemTransport {
        let (tx, rx) = mpsc::unbounded_channel();
        self.peers
            .lock()
            .expect("network mutex poisoned")
            .insert(id.clone(), tx);
        MemTransport {
            id,
            peers: self.peers.clone(),
            inbox: AsyncMutex::new(rx),
        }
    }
}

/// One node's endpoint on a [`Network`].
#[derive(Debug)]
pub struct MemTransport {
    id: NodeId,
    peers: Peers,
    inbox: AsyncMutex<mpsc::UnboundedReceiver<Inbound>>,
}

/// The endpoint's receiver was closed.
#[derive(Debug)]
pub struct Closed;

impl fmt::Display for Closed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("transport endpoint closed")
    }
}

impl std::error::Error for Closed {}

impl Transport for MemTransport {
    type Error = Closed;

    // `async fn` here still satisfies the trait's `impl Future + Send` bound —
    // the compiler enforces `Send` on the returned future regardless.
    async fn send(&self, to: &NodeId, msg: &[u8]) -> Result<(), Closed> {
        // Resolve the target inside a scoped block so the std mutex guard is
        // dropped before any await point.
        let target = {
            let peers = self.peers.lock().expect("network mutex poisoned");
            peers.get(to).cloned()
        };
        if let Some(tx) = target {
            // Dead peer == drop; a best-effort transport never errors on send.
            let _ = tx.send(Inbound {
                from: self.id.clone(),
                msg: msg.to_vec(),
            });
        }
        Ok(())
    }

    async fn recv(&self) -> Result<Inbound, Closed> {
        // The tokio mutex is held across the await intentionally; only the
        // single receive loop ever calls this.
        let mut inbox = self.inbox.lock().await;
        inbox.recv().await.ok_or(Closed)
    }
}
