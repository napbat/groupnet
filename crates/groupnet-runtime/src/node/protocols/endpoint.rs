//! A typed view over node-owned protocol state and receive ownership.

use super::{Messages, Node, PeerImplementation, closed};
use crate::messaging::{Frame, MessageContext, ReceiveHandle};
use groupnet_core::NodeId;
use groupnet_messaging::{Bytes, MessageId, MessageProtocol};
use groupnet_streams::SessionProtocol;
use std::{fmt, io};

/// A configured view over a node's shared protocol, not a new socket or inbox.
///
/// Every clone retains managed node lifetime and the options chosen when bound.
/// Message endpoints share the node's existing receive owner. Session endpoints
/// share node-wide acceptance and admission; unordered accepts select their policy.
pub struct Endpoint<I: PeerImplementation> {
    node: Node,
    protocol: I::Protocol,
    options: I::Options,
}

impl<I: PeerImplementation> Clone for Endpoint<I>
where
    I::Protocol: Clone,
{
    fn clone(&self) -> Self {
        Self {
            node: self.node.clone(),
            protocol: self.protocol.clone(),
            options: self.options.clone(),
        }
    }
}

impl<I: PeerImplementation> fmt::Debug for Endpoint<I>
where
    I::Protocol: fmt::Debug,
    I::Options: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Endpoint")
            .field("node", &self.node)
            .field("protocol", &self.protocol)
            .field("options", &self.options)
            .finish()
    }
}

impl<I: PeerImplementation> Endpoint<I> {
    pub(super) fn new(node: Node, protocol: I::Protocol, options: I::Options) -> Self {
        Self {
            node,
            protocol,
            options,
        }
    }

    /// The shared concrete implementation, including its low-level lifecycle API.
    #[must_use]
    pub fn protocol(&self) -> &I::Protocol {
        &self.protocol
    }

    /// The immutable options selected at binding time.
    #[must_use]
    pub fn options(&self) -> &I::Options {
        &self.options
    }

    /// The managed node retained by this endpoint.
    #[must_use]
    pub fn node(&self) -> &Node {
        &self.node
    }
}

impl<I: PeerImplementation> Endpoint<I>
where
    I::Protocol: MessageProtocol<SendOptions = I::Options>,
{
    /// Sends owned bytes to a logical node using this endpoint's bound policy.
    ///
    /// # Errors
    /// Returns protocol errors or `NotConnected` when the managed node closes.
    pub async fn send(&self, to: &NodeId, payload: Bytes) -> io::Result<MessageId> {
        tokio::select! {
            biased;
            () = self.node.router().cancelled() => Err(closed()),
            result = async {
                self.protocol.send(to, None, payload, self.options.clone()).await
            } => result,
        }
    }
}

impl<I: PeerImplementation> Endpoint<I>
where
    I::Protocol: SessionProtocol<ConnectOptions = I::Options>,
{
    /// Establishes a concrete session using this endpoint's bound options.
    ///
    /// # Errors
    /// Returns protocol errors or `NotConnected` when the managed node closes.
    pub async fn connect(
        &self,
        to: &NodeId,
    ) -> io::Result<<I::Protocol as SessionProtocol>::Session> {
        tokio::select! {
            biased;
            () = self.node.router().cancelled() => Err(closed()),
            result = async { self.protocol.connect(to, self.options.clone()).await } => result,
        }
    }

    /// Accepts a session matching the bound options and its authenticated identity.
    /// Built-in unordered protocols route reliable and unreliable sessions to
    /// distinct policy queues within the same node-wide admission bound.
    ///
    /// # Errors
    /// Returns policy, protocol or node shutdown errors.
    pub async fn accept(&self) -> io::Result<(NodeId, <I::Protocol as SessionProtocol>::Session)> {
        tokio::select! {
            biased;
            () = self.node.router().cancelled() => Err(closed()),
            result = async { self.protocol.accept_with_options(self.options.clone()).await } => result,
        }
    }
}

impl Endpoint<Messages> {
    /// Receives owned bytes and context from this node's existing inbox.
    /// Shares receive ownership with node receives and callbacks, not raw
    /// `Messaging::recv`, which is reserved for the node's dispatcher.
    ///
    /// # Errors
    /// Returns `WouldBlock` for a competing receive owner or shutdown errors.
    pub async fn recv(&self) -> io::Result<(MessageContext, Bytes)> {
        self.node.recv().await
    }

    /// Receives a complete frame from the same node-owned inbox as [`Self::recv`].
    ///
    /// # Errors
    /// Returns `WouldBlock` for a competing receive owner or shutdown errors.
    pub async fn recv_frame(&self) -> io::Result<Frame> {
        self.node.recv_frame().await
    }

    /// Installs the exclusive serial buffer receiver on this node's inbox.
    /// Successful callback completion acknowledges application.
    ///
    /// # Errors
    /// Returns competing receive ownership or shutdown errors.
    pub fn on_recv<F, Fut>(&self, callback: F) -> io::Result<ReceiveHandle>
    where
        F: FnMut(MessageContext, Bytes) -> Fut + Send + 'static,
        Fut: Future<Output = io::Result<()>> + Send + 'static,
    {
        self.node.on_recv(callback)
    }

    /// Installs the exclusive serial frame receiver on this node's inbox.
    ///
    /// # Errors
    /// Returns competing receive ownership or shutdown errors.
    pub fn on_frame<F, Fut>(&self, callback: F) -> io::Result<ReceiveHandle>
    where
        F: FnMut(Frame) -> Fut + Send + 'static,
        Fut: Future<Output = io::Result<()>> + Send + 'static,
    {
        self.node.on_frame(callback)
    }
}
