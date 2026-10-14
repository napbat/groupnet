//! Restricted views over node-owned built-in protocol engines.

use std::{fmt, io};

use groupnet_core::{GroupId, NodeId};
use groupnet_messaging::{Bytes, MessageId, MessageProtocol, Messaging, MessagingConfig};
use groupnet_streams::{OrderedProtocol, SessionProtocol, UnorderedOptions, UnorderedProtocol};

use super::{Node, ReceiveBinding};
use crate::messaging::{Frame, ReceiveHandle};

/// A managed view over one of a node's shared built-in protocol engines.
///
/// [`Endpoint::protocol`](super::Endpoint::protocol) and
/// [`Peer::protocol`](super::Peer::protocol) return this view for [`Messages`](super::Messages),
/// [`Ordered`](super::Ordered) and [`Unordered`](super::Unordered). It forwards
/// sends, connects and accepts through [`MessageProtocol`] and
/// [`SessionProtocol`], receives through the node's single-owner inbox
/// ([`ReceiveBinding`]) and observes engine shutdown with `closed`. It never
/// exposes the engine's raw receive queue, which the node dispatcher owns, nor
/// its shutdown, which would stop the engine for every handle on the node; the
/// engines stop only when the node closes.
///
/// Custom [`PeerImplementation`](super::PeerImplementation)s can wrap a clone
/// of this view to build on a built-in protocol.
///
/// ```no_run
/// # async fn demo(node: groupnet_runtime::Node) -> std::io::Result<()> {
/// let endpoint = node.endpoint(groupnet_runtime::Messages::best_effort())?;
/// endpoint.protocol().closed().await;
/// # Ok(())
/// # }
/// ```
///
/// The dispatcher-owned raw receive is unreachable through a managed handle:
///
/// ```compile_fail,E0599
/// # async fn demo(node: groupnet_runtime::Node) -> std::io::Result<()> {
/// let endpoint = node.endpoint(groupnet_runtime::Messages::best_effort())?;
/// let _frame = endpoint.protocol().recv().await;
/// # Ok(())
/// # }
/// ```
///
/// Neither is shutdown of the node-shared engine:
///
/// ```compile_fail,E0599
/// # fn demo(node: groupnet_runtime::Node) -> std::io::Result<()> {
/// node.endpoint(groupnet_runtime::Messages::best_effort())?.protocol().shutdown();
/// # Ok(())
/// # }
/// ```
///
/// ```compile_fail,E0599
/// # fn demo(node: groupnet_runtime::Node) -> std::io::Result<()> {
/// node.endpoint(groupnet_runtime::Ordered::new())?.protocol().shutdown();
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct ManagedProtocol<P> {
    engine: P,
}

impl<P> ManagedProtocol<P> {
    pub(super) fn new(engine: P) -> Self {
        Self { engine }
    }
}

impl<P: fmt::Debug> fmt::Debug for ManagedProtocol<P> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ManagedProtocol")
            .field("engine", &self.engine)
            .finish()
    }
}

impl ManagedProtocol<Messaging> {
    /// The node-wide messaging bounds fixed at [`NodeBuilder::messaging`](crate::NodeBuilder::messaging).
    #[must_use]
    pub fn config(&self) -> &MessagingConfig {
        self.engine.config()
    }

    /// Waits until the node's messaging engine shuts down.
    pub async fn closed(&self) {
        self.engine.cancellation().cancelled().await;
    }
}

impl ManagedProtocol<OrderedProtocol> {
    /// Waits until the node's ordered engine shuts down.
    pub async fn closed(&self) {
        self.engine.closed().await;
    }
}

impl ManagedProtocol<UnorderedProtocol> {
    /// Validates a selected policy and setup deadline against the node-wide
    /// [`UnorderedConfig`](groupnet_streams::UnorderedConfig).
    ///
    /// # Errors
    /// Returns the violated [`UnorderedOptionsError`](groupnet_streams::UnorderedOptionsError)
    /// rule: a disallowed policy, or a zero or over-long setup deadline.
    pub fn validate_options(
        &self,
        options: &UnorderedOptions,
    ) -> Result<(), groupnet_streams::UnorderedOptionsError> {
        self.engine.validate_options(options)
    }

    /// Waits until the node's unordered engine shuts down and drains.
    pub async fn closed(&self) {
        self.engine.closed().await;
    }
}

impl<P: MessageProtocol> MessageProtocol for ManagedProtocol<P> {
    const ID: groupnet_messaging::ProtocolId = P::ID;
    type SendOptions = P::SendOptions;
    type Receipt = P::Receipt;

    fn send(
        &self,
        to: &NodeId,
        group: Option<&GroupId>,
        payload: Bytes,
        options: Self::SendOptions,
    ) -> impl Future<Output = io::Result<MessageId>> + Send {
        self.engine.send(to, group, payload, options)
    }
}

impl ReceiveBinding for ManagedProtocol<Messaging> {
    type Handle = ReceiveHandle;

    fn recv_frame(&self, node: &Node) -> impl Future<Output = io::Result<Frame>> + Send {
        node.recv_frame()
    }

    fn on_frame<F, Fut>(&self, node: &Node, callback: F) -> io::Result<ReceiveHandle>
    where
        F: FnMut(Frame) -> Fut + Send + 'static,
        Fut: Future<Output = io::Result<()>> + Send + 'static,
    {
        node.on_frame(callback)
    }
}

impl<P: SessionProtocol> SessionProtocol for ManagedProtocol<P> {
    type Session = P::Session;
    type ConnectOptions = P::ConnectOptions;

    fn connect(
        &self,
        to: &NodeId,
        options: Self::ConnectOptions,
    ) -> impl Future<Output = io::Result<Self::Session>> + Send {
        self.engine.connect(to, options)
    }

    fn accept(&self) -> impl Future<Output = io::Result<(NodeId, Self::Session)>> + Send {
        self.engine.accept()
    }

    fn accept_with_options(
        &self,
        options: Self::ConnectOptions,
    ) -> impl Future<Output = io::Result<(NodeId, Self::Session)>> + Send {
        self.engine.accept_with_options(options)
    }
}
