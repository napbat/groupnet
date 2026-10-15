//! The in-process fabric: a [`Network`] of per-node [`MemTransport`]
//! endpoints wired through tokio channels.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use groupnet_core::NodeId;
use groupnet_transport::{Inbound, Transport};
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::mpsc;

use crate::NetworkConfig;

type Peers = Arc<Mutex<HashMap<NodeId, mpsc::Sender<Inbound>>>>;

/// A shared in-process network fabric. Clone it freely; every endpoint created
/// from clones shares one routing table.
#[derive(Clone, Default, Debug)]
pub struct Network {
    peers: Peers,
    config: NetworkConfig,
}

impl Network {
    /// Creates an empty network.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a network with bounded endpoint queues.
    #[must_use]
    pub fn with_config(config: NetworkConfig) -> Self {
        Self {
            peers: Peers::default(),
            config,
        }
    }

    /// Creates and registers a transport endpoint for `id`.
    ///
    /// # Panics
    /// If the fabric's routing table was poisoned by a panic in another thread.
    #[must_use]
    pub fn endpoint(&self, id: NodeId) -> MemTransport {
        let (tx, rx) = mpsc::channel(self.config.inbound_queue.get());
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
    inbox: AsyncMutex<mpsc::Receiver<Inbound>>,
    // A strong sender here would prevent a displaced endpoint from observing EOF.
    registration: mpsc::WeakSender<Inbound>,
}

impl MemTransport {
    /// The node identity registered for this endpoint.
    #[must_use]
    pub fn local_id(&self) -> &NodeId {
        &self.id
    }

    fn target(&self, to: &NodeId) -> Option<mpsc::Sender<Inbound>> {
        self.peers
            .lock()
            .expect("network mutex poisoned")
            .get(to)
            .cloned()
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

/// Why a [`MemTransport`] can no longer receive.
///
/// Sending never fails: a missing or dead peer is a silent drop under the
/// best-effort [`Transport`] contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemTransportError {
    /// [`Network::endpoint`] registered a newer endpoint at this identity on
    /// the same fabric. Messages already queued are still delivered; then the
    /// displaced endpoint's inbox ends, because no sender can reach it.
    Displaced,
}

impl fmt::Display for MemTransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Displaced => {
                f.write_str("memory endpoint displaced by a newer endpoint at the same identity")
            }
        }
    }
}

impl std::error::Error for MemTransportError {}

impl Transport for MemTransport {
    type Error = MemTransportError;

    async fn send(&self, to: &NodeId, msg: &[u8]) -> Result<(), MemTransportError> {
        if let Some(tx) = self.target(to) {
            // Wait before allocating the borrowed packet. Dead peers still drop.
            if let Ok(permit) = tx.reserve().await {
                permit.send(Inbound {
                    from: self.id.clone(),
                    msg: Bytes::copy_from_slice(msg),
                });
            }
        }
        Ok(())
    }

    #[cfg(feature = "link")]
    async fn send_owned_admitted(
        &self,
        to: &NodeId,
        msg: Bytes,
        session: Option<groupnet_transport::admission::SessionId>,
    ) -> Result<(), MemTransportError> {
        if session.is_some() {
            return Ok(());
        }
        if let Some(tx) = self.target(to) {
            let _ = tx
                .send(Inbound {
                    from: self.id.clone(),
                    msg,
                })
                .await;
        }
        Ok(())
    }

    async fn recv(&self) -> Result<Inbound, MemTransportError> {
        // The tokio mutex is held across the await intentionally; only the
        // single receive loop ever calls this. Only the fabric's routing
        // table holds a strong sender, and only displacement drops it while
        // this endpoint is alive.
        let mut inbox = self.inbox.lock().await;
        inbox.recv().await.ok_or(MemTransportError::Displaced)
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
