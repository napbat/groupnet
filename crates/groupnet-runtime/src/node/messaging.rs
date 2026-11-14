//! Node-addressed application messages, sent and received outside any group.

use std::io;

use groupnet_core::NodeId;

use super::Node;
use crate::messaging::{Bytes, Frame, MessageContext, MessageId, ReceiveHandle, SendOptions};

impl Node {
    /// Sends an opaque borrowed buffer to one node, using best-effort delivery.
    /// See [`Participant::send`](crate::Participant::send).
    ///
    /// # Errors
    /// Rejects oversized payloads, shutdown, or local routing enqueue failures.
    pub async fn send(&self, to: &NodeId, payload: impl AsRef<[u8]>) -> io::Result<MessageId> {
        self.participant.send(to, payload).await
    }

    /// Sends an owned/shared buffer with explicit delivery options.
    /// See [`Participant::send_frame`](crate::Participant::send_frame).
    ///
    /// # Errors
    /// Rejects invalid size/options, shutdown, route/queue errors, receiver
    /// rejection, or an acknowledged delivery deadline expiring.
    pub async fn send_frame(
        &self,
        to: &NodeId,
        payload: Bytes,
        options: SendOptions,
    ) -> io::Result<MessageId> {
        self.participant.send_frame(to, payload, options).await
    }

    /// Receives the next node-addressed buffer without copying its payload.
    /// See [`Participant::recv`](crate::Participant::recv).
    ///
    /// # Errors
    /// Returns `WouldBlock` while another receive/callback owns this inbox,
    /// or `NotConnected` when messaging or the network closes.
    pub async fn recv(&self) -> io::Result<(MessageContext, Bytes)> {
        self.participant.recv().await
    }

    /// Receives the next node-addressed frame, independently of all groups.
    /// See [`Participant::recv_frame`](crate::Participant::recv_frame).
    ///
    /// # Errors
    /// Returns `WouldBlock` while another receive/callback owns this inbox,
    /// or `NotConnected` when messaging or the network closes.
    pub async fn recv_frame(&self) -> io::Result<Frame> {
        self.participant.recv_frame().await
    }

    /// Registers the exclusive, serial async receiver for node-addressed
    /// buffers. See [`Participant::on_recv`](crate::Participant::on_recv).
    ///
    /// # Errors
    /// Returns `WouldBlock` if the inbox already has a receive owner, or an
    /// error if messaging or the network is closed.
    pub fn on_recv<F, Fut>(&self, callback: F) -> io::Result<ReceiveHandle>
    where
        F: FnMut(MessageContext, Bytes) -> Fut + Send + 'static,
        Fut: Future<Output = io::Result<()>> + Send + 'static,
    {
        self.participant.on_recv(callback)
    }

    /// Registers the exclusive, serial async receiver for node-addressed
    /// frames. See [`Participant::on_frame`](crate::Participant::on_frame).
    ///
    /// # Errors
    /// Returns `WouldBlock` if the inbox already has a receive owner, or an
    /// error if messaging or the network is closed.
    pub fn on_frame<F, Fut>(&self, callback: F) -> io::Result<ReceiveHandle>
    where
        F: FnMut(Frame) -> Fut + Send + 'static,
        Fut: Future<Output = io::Result<()>> + Send + 'static,
    {
        self.participant.on_frame(callback)
    }
}
