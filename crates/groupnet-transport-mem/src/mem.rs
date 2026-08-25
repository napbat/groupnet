//! The in-process fabric: a [`Network`] of per-node [`MemTransport`]
//! endpoints wired through tokio channels.

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
    ///
    /// # Panics
    /// If the fabric's routing table was poisoned by a panic in another thread.
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

    fn send(
        &self,
        to: &NodeId,
        msg: &[u8],
    ) -> impl std::future::Future<Output = Result<(), Closed>> + Send {
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
        std::future::ready(Ok(()))
    }

    async fn recv(&self) -> Result<Inbound, Closed> {
        // The tokio mutex is held across the await intentionally; only the
        // single receive loop ever calls this.
        let mut inbox = self.inbox.lock().await;
        inbox.recv().await.ok_or(Closed)
    }
}
