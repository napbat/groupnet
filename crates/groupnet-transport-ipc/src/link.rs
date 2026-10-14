//! Router-independent native IPC preparation.

use std::io;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;

use groupnet_core::NodeId;
use groupnet_transport::link::{BoundLink, LinkConfig, LinkFuture, LinkProvider, PeerEndpoint};

use crate::{IpcAddress, IpcConfig, IpcTransport, MAX_FRAME};

/// The link MTU: the IPC framing protocol's largest message.
const MTU: NonZeroUsize = NonZeroUsize::new(MAX_FRAME).unwrap();

/// A native IPC listener and explicitly configured peer addresses.
///
/// Binding preflights everything before it opens the listener: the
/// [`IpcConfig`], the local identity, the link's peers through
/// [`LinkConfig::validate_for`], and every peer exactly as
/// [`IpcTransport::register_peer`] would check it (the 64-byte IPC identity
/// bound, `max_peers` and the platform address). A failure is `InvalidInput`
/// carrying the typed cause ([`IpcConfigError`](crate::IpcConfigError),
/// [`LinkConfigError`](groupnet_transport::link::LinkConfigError),
/// [`IpcPeerError`](crate::IpcPeerError) or the local
/// [`NodeIdError`](crate::NodeIdError)), and no listener exists. Only a
/// registration racing the transport's own shutdown can still fail after
/// binding; that closes and drains the new listener before returning.
///
/// Setters only store their values. Preflight with [`IpcConfig::validate`] and
/// [`LinkConfig::validate_for`].
#[derive(Debug)]
pub struct IpcLink {
    bind: IpcAddress,
    peers: Vec<NodeId>,
    addresses: Vec<IpcAddress>,
    cost: NonZeroU32,
    config: IpcConfig,
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
            cost: NonZeroU32::MIN,
            config: IpcConfig::default(),
        }
    }

    /// Selects listener resource budgets, validated when the link binds.
    #[must_use]
    pub const fn with_config(mut self, config: IpcConfig) -> Self {
        self.config = config;
        self
    }

    /// Sets the routing cost advertised for this link (one by default).
    #[must_use]
    pub const fn with_cost(mut self, cost: NonZeroU32) -> Self {
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
                config,
            } = *self;
            let link = LinkConfig {
                peers,
                cost,
                mtu: MTU,
            };
            link.validate_for(&local)?;
            IpcTransport::preflight_peers(&local, &config, &link.peers, &addresses)?;
            let transport = IpcTransport::bind_with(local, &bind, config)?;
            for (peer, address) in link.peers.iter().zip(addresses) {
                if let Err(error) = transport.register_peer(peer.clone(), address) {
                    transport.close().await;
                    return Err(error);
                }
            }
            let lifecycle = Arc::new(transport.clone());
            Ok(BoundLink::new(transport, link).with_lifecycle(lifecycle))
        })
    }
}
