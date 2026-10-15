//! Router-independent native IPC preparation.

use std::io;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;

use groupnet_core::NodeId;
use groupnet_transport::link::{BoundLink, LinkConfig, LinkFuture, LinkProvider, PeerEndpoint};

use crate::ipc::Preflight;
use crate::{IpcAddress, IpcConfig, IpcTransport, MAX_FRAME};

/// The link MTU: the IPC framing protocol's largest message.
const MTU: NonZeroUsize = NonZeroUsize::new(MAX_FRAME).unwrap();

/// A native IPC listener and explicitly configured peer addresses.
///
/// Binding validates everything once, before it opens the listener: the
/// link's peers through [`LinkConfig::validate_for`], the [`IpcConfig`], the
/// local identity, and every peer as [`IpcTransport::register_peer`] would
/// check it (the 64-byte IPC identity bound, `max_peers` and the platform
/// address). A failure is `InvalidInput` carrying the typed cause
/// ([`LinkConfigError`](groupnet_transport::link::LinkConfigError),
/// [`IpcConfigError`](crate::IpcConfigError), the local
/// [`NodeIdError`](crate::NodeIdError), or an
/// [`IpcPeerError`](crate::IpcPeerError), whose `Address` variant wraps the
/// [`IpcAddressError`](crate::IpcAddressError)), and no listener exists. The
/// listener then opens with the validated peer book already in place, so
/// nothing is checked twice and no peer registration can fail afterwards;
/// only opening the listener itself (its own address, an occupied name, a
/// missing Tokio runtime) can still fail.
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
            let preflight = Preflight::for_link(local, config, &link, addresses)?;
            let transport = IpcTransport::start(&bind, preflight)?;
            let lifecycle = Arc::new(transport.clone());
            Ok(BoundLink::new(transport, link).with_lifecycle(lifecycle))
        })
    }
}
