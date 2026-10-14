//! A typed view over node-owned protocol state and receive ownership.

use super::{Node, PeerImplementation, ReceiveBinding, closed};
use crate::messaging::{Frame, MessageContext};
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

    /// The shared protocol this endpoint binds. Built-ins return a restricted
    /// [`ManagedProtocol`](super::ManagedProtocol) view: protocol operations
    /// and shutdown observation, never the dispatcher-owned raw receive or
    /// engine shutdown.
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

impl<I: PeerImplementation> Endpoint<I>
where
    I::Protocol: ReceiveBinding,
{
    /// Receives owned bytes and their complete context from the protocol's
    /// single receive owner: this node's existing inbox for [`Messages`](super::Messages).
    /// Shares receive ownership with node receives and callbacks, never the
    /// raw messaging worker queue the node dispatcher owns.
    ///
    /// # Errors
    /// Returns `WouldBlock` for a competing receive owner or shutdown errors.
    pub async fn recv(
        &self,
    ) -> io::Result<(
        MessageContext<<I::Protocol as MessageProtocol>::Receipt>,
        Bytes,
    )> {
        Ok(self.recv_frame().await?.into_parts())
    }

    /// Receives a complete frame from the same receive owner as [`Self::recv`].
    ///
    /// # Errors
    /// Returns `WouldBlock` for a competing receive owner or shutdown errors.
    pub async fn recv_frame(&self) -> io::Result<Frame<<I::Protocol as MessageProtocol>::Receipt>> {
        self.protocol.recv_frame(&self.node).await
    }

    /// Installs the exclusive serial buffer receiver on the protocol's receive
    /// owner. For [`Messages`](super::Messages), successful callback completion
    /// acknowledges application.
    ///
    /// # Errors
    /// Returns competing receive ownership or shutdown errors.
    pub fn on_recv<F, Fut>(
        &self,
        mut callback: F,
    ) -> io::Result<<I::Protocol as ReceiveBinding>::Handle>
    where
        F: FnMut(MessageContext<<I::Protocol as MessageProtocol>::Receipt>, Bytes) -> Fut
            + Send
            + 'static,
        Fut: Future<Output = io::Result<()>> + Send + 'static,
    {
        self.protocol.on_frame(&self.node, move |frame| {
            let (context, payload) = frame.into_parts();
            callback(context, payload)
        })
    }

    /// Installs the exclusive serial frame receiver on the protocol's receive owner.
    ///
    /// # Errors
    /// Returns competing receive ownership or shutdown errors.
    pub fn on_frame<F, Fut>(
        &self,
        callback: F,
    ) -> io::Result<<I::Protocol as ReceiveBinding>::Handle>
    where
        F: FnMut(Frame<<I::Protocol as MessageProtocol>::Receipt>) -> Fut + Send + 'static,
        Fut: Future<Output = io::Result<()>> + Send + 'static,
    {
        self.protocol.on_frame(&self.node, callback)
    }
}
