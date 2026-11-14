//! Static protocol bindings over managed logical peers, never physical sockets.
//!
//! A [`Node`](crate::Node) and a [`Client`](crate::Client) bind the same
//! handles through their shared [`Participant`].

mod endpoint;
mod implementations;
mod managed;
pub use endpoint::Endpoint;
pub use implementations::{Messages, Ordered, Unordered};
pub use managed::ManagedProtocol;

use crate::Participant;
use crate::messaging::Frame;
use groupnet_core::NodeId;
use groupnet_messaging::{Bytes, MessageId, MessageProtocol};
use groupnet_streams::{OrderedProtocol, SessionProtocol, UnorderedProtocol};
use std::{fmt, io};

/// Resolves a protocol implementation and its options against a participant of
/// a managed network (a [`Node`](crate::Node) or a [`Client`](crate::Client)).
///
/// This trait is intentionally unsealed: custom selectors can resolve their own
/// concrete message or session implementations without boxing or dynamic dispatch.
/// Binding must not establish a connection or start per-peer workers. A built-in
/// may initialize the participant's shared protocol worker on its first binding.
/// Built-ins resolve to a [`ManagedProtocol`] view, which a custom
/// implementation may wrap.
pub trait PeerImplementation {
    /// Concrete shared protocol used for this binding.
    type Protocol: Send + Sync;

    /// Immutable policy cloned into each send or connection attempt.
    type Options: Clone + Send + Sync;

    /// Resolves the shared protocol and fixes this binding's operation options.
    ///
    /// # Errors
    /// Returns configuration, protocol initialization or shutdown errors.
    fn bind(self, participant: &Participant) -> io::Result<(Self::Protocol, Self::Options)>;
}

/// Managed receives for a [`MessageProtocol`], through one single-owner inbox.
///
/// [`Endpoint`] exposes `recv`, `recv_frame`, `on_recv` and `on_frame` for any
/// bound protocol implementing this trait, with the protocol's own
/// [`Receipt`](MessageProtocol::Receipt). A message protocol that does not
/// implement it is send-only at the handle layer.
///
/// Implementations must deliver from exactly one receive owner per participant,
/// so a manual receive and an installed callback can never both take frames:
/// the built-in [`Messages`] binding uses the node-addressed inbox
/// ([`Participant::recv_frame`], [`Participant::on_frame`]), which also refuses
/// competing owners with `WouldBlock`.
/// A protocol wrapping a built-in view delegates to it; one that owns a
/// separate router namespace owns that namespace's single receive queue.
/// Never read a raw messaging worker queue that the dispatcher owns.
pub trait ReceiveBinding: MessageProtocol {
    /// Exclusive callback ownership returned by [`Self::on_frame`].
    type Handle;

    /// Receives the next complete frame from the protocol's receive owner.
    ///
    /// # Errors
    /// Returns competing receive ownership or shutdown errors.
    fn recv_frame(
        &self,
        participant: &Participant,
    ) -> impl Future<Output = io::Result<Frame<Self::Receipt>>> + Send;

    /// Installs the exclusive serial frame receiver on the protocol's receive owner.
    ///
    /// # Errors
    /// Returns competing receive ownership or shutdown errors.
    fn on_frame<F, Fut>(&self, participant: &Participant, callback: F) -> io::Result<Self::Handle>
    where
        F: FnMut(Frame<Self::Receipt>) -> Fut + Send + 'static,
        Fut: Future<Output = io::Result<()>> + Send + 'static;
}

impl Participant {
    fn cached_messages(&self) -> ManagedProtocol<groupnet_messaging::Messaging> {
        ManagedProtocol::new(self.shared.messaging.sender().clone())
    }

    /// Resolves the shared ordered engine with the node-wide
    /// [`OrderedConfig`](groupnet_streams::OrderedConfig) validated at start.
    fn cached_ordered(&self) -> io::Result<OrderedProtocol> {
        let mut protocol = self
            .shared
            .ordered
            .lock()
            .map_err(|_| io::Error::other("ordered protocol cache poisoned"))?;
        self.ensure_protocols_open()?;
        if let Some(protocol) = protocol.as_ref() {
            return Ok(protocol.clone());
        }
        let ordered =
            OrderedProtocol::with_validated(self.tunnels()?.clone(), self.shared.ordered_config)?;
        *protocol = Some(ordered.clone());
        Ok(ordered)
    }

    /// Resolves the shared unordered engine with the node-wide
    /// [`UnorderedConfig`](groupnet_streams::UnorderedConfig) validated at start.
    fn cached_unordered(&self) -> io::Result<UnorderedProtocol> {
        let mut protocol = self
            .shared
            .unordered
            .lock()
            .map_err(|_| io::Error::other("unordered protocol cache poisoned"))?;
        self.ensure_protocols_open()?;
        if let Some(protocol) = protocol.as_ref() {
            return Ok(protocol.clone());
        }
        let unordered = UnorderedProtocol::with_validated(
            self.router(),
            self.tunnels()?.clone(),
            self.shared.unordered_config.clone(),
        )?;
        *protocol = Some(unordered.clone());
        Ok(unordered)
    }

    /// Binds a typed endpoint and immutable options to this participant's shared
    /// protocol. Does not open a connection, socket or per-peer worker. The
    /// first binding may initialize a shared protocol worker. Message receives
    /// use the existing node-addressed inbox; unordered accepts match the
    /// selected delivery policy.
    ///
    /// # Errors
    /// Returns implementation binding errors or `NotConnected` after shutdown.
    pub fn endpoint<I: PeerImplementation>(&self, implementation: I) -> io::Result<Endpoint<I>> {
        self.ensure_protocols_open()?;
        let (protocol, options) = implementation.bind(self)?;
        self.ensure_protocols_open()?;
        Ok(Endpoint::new(self.clone(), protocol, options))
    }

    /// Binds a logical destination using the same shared machinery as [`Self::endpoint`].
    /// Retains the network's lifetime without opening a connection, socket or
    /// per-peer worker.
    ///
    /// # Errors
    /// Returns implementation binding errors or `NotConnected` after shutdown.
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
        self.shared.messaging.sender().shutdown();
        if let Ok(ordered) = self.shared.ordered.lock()
            && let Some(ordered) = ordered.as_ref()
        {
            ordered.shutdown();
        }
        if let Ok(unordered) = self.shared.unordered.lock()
            && let Some(unordered) = unordered.as_ref()
        {
            unordered.shutdown();
        }
    }

    pub(crate) async fn close_protocols(&self) {
        let ordered = self
            .shared
            .ordered
            .lock()
            .ok()
            .and_then(|protocol| protocol.clone());
        let unordered = self
            .shared
            .unordered
            .lock()
            .ok()
            .and_then(|protocol| protocol.clone());
        if let Some(ordered) = ordered {
            ordered.closed().await;
        }
        if let Some(unordered) = unordered {
            unordered.closed().await;
        }
    }
}

/// Typed protocol binding to one logical destination, retaining the managed
/// network's life. Receives remain endpoint-owned, not a separate per-peer
/// inbox. Options are fixed at binding time and shared by every operation on
/// this handle.
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

    /// The shared protocol this peer binds: a restricted [`ManagedProtocol`]
    /// view for built-ins.
    #[must_use]
    pub fn protocol(&self) -> &I::Protocol {
        self.endpoint.protocol()
    }

    /// The immutable policy selected when this peer was bound.
    #[must_use]
    pub fn options(&self) -> &I::Options {
        self.endpoint.options()
    }

    /// The participant retained by this binding.
    #[must_use]
    pub fn participant(&self) -> &Participant {
        self.endpoint.participant()
    }
}

impl<I: PeerImplementation> Peer<I>
where
    I::Protocol: MessageProtocol<SendOptions = I::Options>,
{
    /// Sends owned bytes using the policy fixed at binding time.
    /// # Errors
    /// Returns protocol errors or `NotConnected` when the network closes.
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
    /// Returns protocol errors or `NotConnected` when the network closes.
    pub async fn connect(&self) -> io::Result<<I::Protocol as SessionProtocol>::Session> {
        self.endpoint.connect(&self.id).await
    }
}

fn closed() -> io::Error {
    io::Error::new(io::ErrorKind::NotConnected, "managed network closed")
}
