//! The network side that a [`Node`](crate::Node) and a [`Client`](crate::Client)
//! share: one managed network, the node-addressed inbox and the built-in
//! protocol engines.

use std::fmt;
use std::io;
use std::sync::{Arc, Mutex};

use groupnet_core::NodeId;
use groupnet_messaging::ValidMessagingConfig;
use groupnet_network::tunnel::TunnelTransport;
use groupnet_network::{LinkProvider, Network, NetworkBuilder, Router, RouterConfig, TunnelConfig};
use groupnet_streams::{
    OrderedConfig, OrderedProtocol, UnorderedConfig, UnorderedProtocol, ValidOrderedConfig,
    ValidUnorderedConfig,
};

use crate::messaging::{Hub, MessagingConfig};

mod messaging;

pub(crate) use messaging::start_dispatcher;

/// The protocol state of one participant, without network ownership.
pub(crate) struct Shared {
    pub(crate) id: NodeId,
    pub(crate) messaging: Hub,
    /// Ordered-stream policy, validated at start and fixed for the participant's life.
    pub(crate) ordered_config: ValidOrderedConfig,
    pub(crate) ordered: Mutex<Option<OrderedProtocol>>,
    /// Unordered policy, validated at start and fixed for the participant's life.
    pub(crate) unordered_config: ValidUnorderedConfig,
    pub(crate) unordered: Mutex<Option<UnorderedProtocol>>,
}

/// One participant of a managed network: its identity, the network it owns,
/// its node-addressed inbox and its built-in protocol engines.
///
/// A [`Node`](crate::Node) and a [`Client`](crate::Client) each own one and
/// expose it with `participant()`. Every [`Endpoint`](crate::Endpoint) and
/// [`Peer`](crate::Peer) retains the participant it was bound to, and a custom
/// [`PeerImplementation`](crate::PeerImplementation) or
/// [`ReceiveBinding`](crate::ReceiveBinding) resolves shared resources through
/// it. Group coordination is not part of it: only a `Node` joins groups.
///
/// Cheap to clone; every clone retains the network's lifetime. The participant
/// has no close of its own: [`Node::close`](crate::Node::close) or
/// [`Client::close`](crate::Client::close) closes it.
#[derive(Clone)]
pub struct Participant {
    pub(crate) shared: Arc<Shared>,
    network: Network,
}

impl fmt::Debug for Participant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Participant")
            .field("id", &self.shared.id)
            .finish_non_exhaustive()
    }
}

impl Participant {
    /// This participant's id.
    #[must_use]
    pub fn id(&self) -> &NodeId {
        &self.shared.id
    }

    /// The participant's router, including route inspection and typed adapter
    /// registration. A router clone alone does not retain the managed
    /// network's lifetime.
    #[must_use]
    pub fn router(&self) -> &Router {
        self.network.router()
    }

    /// Accesses tunnel peer admission and revocation. Open ordered streams with
    /// `endpoint(Ordered::new())` (see [`Ordered`](crate::Ordered)).
    ///
    /// # Errors
    /// Returns `Unsupported` without configured TLS tunnels.
    pub fn tunnels(&self) -> io::Result<&TunnelTransport> {
        self.network.tunnels()
    }

    /// Stops the protocol engines and closes the network, then waits for its
    /// tasks. The first half of an owner's close.
    pub(crate) async fn close_network(&self) {
        self.shutdown_protocols();
        self.network.close().await;
    }

    /// Waits for the messaging worker, its dispatcher and the session engines
    /// to stop. The last half of an owner's close.
    pub(crate) async fn close_engines(&self) {
        self.shared.messaging.close().await;
        self.close_protocols().await;
    }
}

/// The settings a node and a client share: links, routing, tunnels and the
/// node-wide messaging and session policies.
#[derive(Debug, Default)]
pub(crate) struct Settings {
    network: NetworkBuilder,
    messaging: MessagingConfig,
    ordered: OrderedConfig,
    unordered: UnorderedConfig,
}

/// [`Settings`] whose protocol policies passed validation.
pub(crate) struct ValidSettings {
    network: NetworkBuilder,
    messaging: ValidMessagingConfig,
    ordered: ValidOrderedConfig,
    unordered: ValidUnorderedConfig,
}

impl Settings {
    pub(crate) fn link(&mut self, provider: impl LinkProvider) {
        self.network = std::mem::take(&mut self.network).link(provider);
    }

    pub(crate) fn links(&mut self, providers: impl IntoIterator<Item = Box<dyn LinkProvider>>) {
        self.network = std::mem::take(&mut self.network).links(providers);
    }

    pub(crate) fn routing(&mut self, config: RouterConfig) {
        self.network = std::mem::take(&mut self.network).routing(config);
    }

    pub(crate) fn tunnels(&mut self, config: TunnelConfig) {
        self.network = std::mem::take(&mut self.network).tunnels(config);
    }

    pub(crate) fn messaging(&mut self, config: MessagingConfig) {
        self.messaging = config;
    }

    pub(crate) fn ordered(&mut self, config: OrderedConfig) {
        self.ordered = config;
    }

    pub(crate) fn unordered(&mut self, config: UnorderedConfig) {
        self.unordered = config;
    }

    /// The configured adjacent peers of every link.
    pub(crate) fn peers(&self) -> Vec<NodeId> {
        self.network.peers()
    }

    /// Validates the messaging, ordered and unordered policies, binding nothing.
    ///
    /// # Errors
    /// `InvalidInput` wrapping the typed error that names the field.
    pub(crate) fn validate(self) -> io::Result<ValidSettings> {
        Ok(ValidSettings {
            messaging: ValidMessagingConfig::try_from(self.messaging)?,
            ordered: ValidOrderedConfig::try_from(self.ordered)?,
            unordered: ValidUnorderedConfig::try_from(self.unordered)?,
            network: self.network,
        })
    }
}

impl ValidSettings {
    /// Binds every link, starts routing and the messaging engine. The caller
    /// starts the dispatcher with [`start_dispatcher`].
    pub(crate) async fn bind(self, id: NodeId) -> io::Result<Participant> {
        let network = self.network.bind(id.clone()).await?;
        let messaging = Hub::new(network.router(), self.messaging)?;
        Ok(Participant {
            shared: Arc::new(Shared {
                id,
                messaging,
                ordered_config: self.ordered,
                ordered: Mutex::new(None),
                unordered_config: self.unordered,
                unordered: Mutex::new(None),
            }),
            network,
        })
    }
}
