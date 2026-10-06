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
        let registration = tx.downgrade();
        self.peers
            .lock()
            .expect("network mutex poisoned")
            .insert(id.clone(), tx);
        MemTransport {
            id,
            peers: self.peers.clone(),
            inbox: AsyncMutex::new(rx),
            registration,
        }
    }
}

/// One node's endpoint on a [`Network`].
#[derive(Debug)]
pub struct MemTransport {
    id: NodeId,
    peers: Peers,
    inbox: AsyncMutex<mpsc::UnboundedReceiver<Inbound>>,
    // A strong sender here would prevent a displaced endpoint from observing EOF.
    registration: mpsc::WeakUnboundedSender<Inbound>,
}

impl MemTransport {
    /// The node identity registered for this endpoint.
    #[must_use]
    pub fn local_id(&self) -> &NodeId {
        &self.id
    }
}

impl Drop for MemTransport {
    fn drop(&mut self) {
        let mut peers = self.peers.lock().expect("network mutex poisoned");
        // An endpoint can be replaced at the same identity. An older handle
        // must never unregister its replacement when it is dropped later.
        if peers.get(&self.id).is_some_and(|sender| {
            self.registration
                .upgrade()
                .is_some_and(|registration| sender.same_channel(&registration))
        }) {
            peers.remove(&self.id);
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_drop_removes_its_channel_registration() {
        let net = Network::new();
        let endpoint = net.endpoint(NodeId::new("local"));
        assert_eq!(net.peers.lock().expect("peers").len(), 1);
        drop(endpoint);
        assert!(net.peers.lock().expect("peers").is_empty());
    }

    #[cfg(feature = "link")]
    #[tokio::test]
    async fn rejected_and_closed_links_release_endpoint_registrations() {
        use groupnet_transport::link::LinkProvider;

        let net = Network::new();
        let provider = crate::MemLink::new(net.endpoint(NodeId::new("actual")), Vec::new());
        assert!(Box::new(provider).bind(NodeId::new("wrong")).await.is_err());
        assert!(net.peers.lock().expect("peers").is_empty());

        let local = NodeId::new("local");
        let provider = crate::MemLink::new(net.endpoint(local.clone()), Vec::new());
        let bound = Box::new(provider).bind(local).await.expect("bind");
        assert_eq!(net.peers.lock().expect("peers").len(), 1);
        bound.driver.close().await;
        assert!(net.peers.lock().expect("peers").is_empty());
    }
}
