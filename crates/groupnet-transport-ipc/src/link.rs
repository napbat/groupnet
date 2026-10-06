//! Router-independent native IPC preparation.

use std::io;
use std::sync::Arc;

use groupnet_core::NodeId;
use groupnet_transport::link::{BoundLink, LinkConfig, LinkFuture, LinkProvider, PeerEndpoint};

use crate::{IpcAddress, IpcTransport, MAX_FRAME};

/// A native IPC listener and explicitly configured peer addresses.
///
/// Binding registers every peer before returning a link. Failed peer
/// registration closes and drains the newly created listener before returning.
#[derive(Debug)]
pub struct IpcLink {
    bind: IpcAddress,
    peers: Vec<NodeId>,
    addresses: Vec<IpcAddress>,
    cost: u32,
}

impl IpcLink {
    /// Configures a listener and the native addresses of its routing peers.
    #[must_use]
    pub fn new(bind: IpcAddress, peers: Vec<PeerEndpoint<IpcAddress>>) -> Self {
        let (peers, addresses) = peers
            .into_iter()
            .map(|peer| (peer.node, peer.address))
            .unzip();
        Self {
            bind,
            peers,
            addresses,
            cost: 1,
        }
    }

    /// Sets the routing cost advertised for this link (one by default).
    #[must_use]
    pub const fn with_cost(mut self, cost: u32) -> Self {
        self.cost = cost;
        self
    }
}

impl LinkProvider for IpcLink {
    fn peers(&self) -> &[NodeId] {
        &self.peers
    }

    fn bind(self: Box<Self>, local: NodeId) -> LinkFuture<'static, io::Result<BoundLink>> {
        Box::pin(async move {
            let Self {
                bind,
                peers,
                addresses,
                cost,
            } = *self;
            let transport = IpcTransport::bind(local, &bind)?;
            for (peer, address) in peers.iter().zip(addresses) {
                if let Err(error) = transport.register_peer(peer.clone(), address) {
                    transport.close().await;
                    return Err(error);
                }
            }
            let lifecycle = Arc::new(transport.clone());
            Ok(BoundLink::new(
                transport,
                LinkConfig {
                    peers,
                    cost,
                    mtu: MAX_FRAME,
                },
            )
            .with_lifecycle(lifecycle))
        })
    }
}
