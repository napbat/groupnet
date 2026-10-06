//! Static protocol bindings over managed logical peers, never physical sockets.

mod endpoint;
mod implementations;
pub use endpoint::Endpoint;
pub use implementations::{Messages, Ordered, Unordered};

use super::Node;
use groupnet_core::NodeId;
use groupnet_messaging::{Bytes, MessageId, MessageProtocol, Messaging};
use groupnet_streams::{OrderedProtocol, SessionProtocol, UnorderedConfig, UnorderedProtocol};
use std::{fmt, io};

/// Resolves a protocol implementation and its options against a managed node.
///
/// This trait is intentionally unsealed: custom selectors can resolve their own
/// concrete message or session implementations without boxing or dynamic dispatch.
/// Binding must not establish a connection or start per-peer workers. A built-in
/// may initialize the node's shared protocol worker on its first binding.
pub trait PeerImplementation {
    /// Concrete shared protocol used for this binding.
    type Protocol: Send + Sync;

    /// Immutable policy cloned into each send or connection attempt.
    type Options: Clone + Send + Sync;

    /// Resolves the shared protocol and fixes this binding's operation options.
    ///
    /// # Errors
    /// Returns configuration, protocol initialization or node shutdown errors.
    fn bind(self, node: &Node) -> io::Result<(Self::Protocol, Self::Options)>;
}

impl Node {
    fn cached_messages(&self) -> Messaging {
        self.inner.messaging.sender().clone()
    }

    fn cached_ordered(&self) -> io::Result<OrderedProtocol> {
        let mut protocol = self
            .inner
            .ordered
            .lock()
            .map_err(|_| io::Error::other("ordered protocol cache poisoned"))?;
        self.ensure_protocols_open()?;
        if let Some(protocol) = protocol.as_ref() {
            return Ok(protocol.clone());
        }
        let ordered = OrderedProtocol::new(self.tunnels()?.clone());
        *protocol = Some(ordered.clone());
        Ok(ordered)
    }

    /// Fixes node-wide unordered admission, capacity and timer settings.
    /// Does not initialize the protocol or open a connection. Repeating the same
    /// configuration is allowed; conflicting settings never mutate live policy.
    /// Call before the first unordered peer or endpoint binding.
    ///
    /// # Errors
    /// Returns `InvalidInput` for invalid or conflicting settings, or
    /// `NotConnected` after node shutdown.
    pub fn configure_unordered(&self, config: UnorderedConfig) -> io::Result<()> {
        config.validate()?;
        let mut state = self
            .inner
            .unordered
            .lock()
            .map_err(|_| io::Error::other("unordered protocol cache poisoned"))?;
        self.ensure_protocols_open()?;
        if let Some((existing, _)) = state.as_ref() {
            if existing != &config {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "unordered protocol configuration already fixed",
                ));
            }
        } else {
            *state = Some((config, None));
        }
        Ok(())
    }

    fn cached_unordered(&self) -> io::Result<UnorderedProtocol> {
        let mut state = self
            .inner
            .unordered
            .lock()
            .map_err(|_| io::Error::other("unordered protocol cache poisoned"))?;
        self.ensure_protocols_open()?;
        if let Some((_, Some(protocol))) = state.as_ref() {
            return Ok(protocol.clone());
        }
        let config = state
            .as_ref()
            .map_or_else(UnorderedConfig::default, |(config, _)| config.clone());
        let protocol =
            UnorderedProtocol::new(self.router(), self.tunnels()?.clone(), config.clone())?;
        *state = Some((config, Some(protocol.clone())));
        Ok(protocol)
    }

    /// Binds a typed endpoint and immutable options to this node's shared protocol.
    /// Does not open a connection, socket or per-peer worker. The first binding
    /// may initialize a node-shared protocol worker. Message receives use this
    /// node's existing inbox; unordered accepts match the selected delivery policy.
    ///
    /// # Errors
    /// Returns implementation binding errors or `NotConnected` after node shutdown.
    pub fn endpoint<I: PeerImplementation>(&self, implementation: I) -> io::Result<Endpoint<I>> {
        self.ensure_protocols_open()?;
        let (protocol, options) = implementation.bind(self)?;
        self.ensure_protocols_open()?;
        Ok(Endpoint::new(self.clone(), protocol, options))
    }

    /// Binds a logical destination using the same shared machinery as [`Self::endpoint`].
    /// Retains node lifetime without opening a connection, socket or per-peer worker.
    ///
    /// # Errors
    /// Returns implementation binding errors or `NotConnected` after node shutdown.
    pub fn peer<I: PeerImplementation>(
        &self,
        id: NodeId,
        implementation: I,
    ) -> io::Result<Peer<I>> {
        Ok(Peer {
            endpoint: self.endpoint(implementation)?,
            id,
        })
    }

    fn ensure_protocols_open(&self) -> io::Result<()> {
        if self.router().is_closed() {
            return Err(closed());
        }
        Ok(())
    }

    pub(crate) fn shutdown_protocols(&self) {
        self.cached_messages().shutdown();
        if let Ok(ordered) = self.inner.ordered.lock()
            && let Some(ordered) = ordered.as_ref()
        {
            ordered.shutdown();
        }
        if let Ok(unordered) = self.inner.unordered.lock()
            && let Some((_, Some(unordered))) = unordered.as_ref()
        {
            unordered.shutdown();
        }
    }

    pub(crate) async fn close_protocols(&self) {
        let ordered = self
            .inner
            .ordered
            .lock()
            .ok()
            .and_then(|protocol| protocol.clone());
        let unordered = self
            .inner
            .unordered
            .lock()
            .ok()
            .and_then(|state| state.as_ref().and_then(|(_, protocol)| protocol.clone()));
        if let Some(ordered) = ordered {
            ordered.closed().await;
        }
        if let Some(unordered) = unordered {
            unordered.closed().await;
        }
    }
}

/// Typed protocol binding to one logical destination, retaining managed node life.
/// Receives remain endpoint/node-owned, not a separate per-peer inbox. Options are
/// fixed at binding time and shared by every operation on this handle.
pub struct Peer<I: PeerImplementation> {
    endpoint: Endpoint<I>,
    id: NodeId,
}

impl<I: PeerImplementation> Clone for Peer<I>
where
    I::Protocol: Clone,
{
    fn clone(&self) -> Self {
        Self {
            endpoint: self.endpoint.clone(),
            id: self.id.clone(),
        }
    }
}

impl<I: PeerImplementation> fmt::Debug for Peer<I>
where
    I::Protocol: fmt::Debug,
    I::Options: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Peer")
            .field("endpoint", &self.endpoint)
            .field("id", &self.id)
            .finish()
    }
}

impl<I: PeerImplementation> Peer<I> {
    /// Logical remote destination, independent of its selected next hop.
    #[must_use]
    pub fn id(&self) -> &NodeId {
        &self.id
    }

    /// The concrete shared protocol implementation.
    #[must_use]
    pub fn protocol(&self) -> &I::Protocol {
        self.endpoint.protocol()
    }

    /// The immutable policy selected when this peer was bound.
    #[must_use]
    pub fn options(&self) -> &I::Options {
        self.endpoint.options()
    }

    /// The managed node retained by this binding.
    #[must_use]
    pub fn node(&self) -> &Node {
        self.endpoint.node()
    }
}

impl<I: PeerImplementation> Peer<I>
where
    I::Protocol: MessageProtocol<SendOptions = I::Options>,
{
    /// Sends owned bytes using the policy fixed at binding time.
    /// # Errors
    /// Returns protocol errors or `NotConnected` when the managed node closes.
    pub async fn send(&self, payload: Bytes) -> io::Result<MessageId> {
        self.endpoint.send(&self.id, payload).await
    }
}

impl<I: PeerImplementation> Peer<I>
where
    I::Protocol: SessionProtocol<ConnectOptions = I::Options>,
{
    /// Establishes this protocol's concrete session with its bound options.
    /// # Errors
    /// Returns protocol errors or `NotConnected` when the managed node closes.
    pub async fn connect(&self) -> io::Result<<I::Protocol as SessionProtocol>::Session> {
        self.endpoint.connect(&self.id).await
    }
}

fn closed() -> io::Error {
    io::Error::new(io::ErrorKind::NotConnected, "managed node network closed")
}
