use std::io;
use std::sync::Arc;

use groupnet_core::NodeId;

use super::{Inner, Node};
use crate::messaging::{Bytes, Frame, MessageContext, MessageId, ReceiveHandle, SendOptions};

impl Node {
    /// Sends an opaque borrowed buffer to one node, using best-effort delivery.
    /// Copies into owned bytes before routed packet encoding; `send_frame` accepts `Bytes`.
    ///
    /// # Errors
    /// Rejects oversized payloads, shutdown, or local routing enqueue failures.
    pub async fn send(&self, to: &NodeId, payload: impl AsRef<[u8]>) -> io::Result<MessageId> {
        let payload = payload.as_ref();
        let options = SendOptions::default();
        self.inner
            .messaging
            .sender()
            .validate_send(options, payload.len())?;
        self.send_frame(to, Bytes::copy_from_slice(payload), options)
            .await
    }

    /// Sends an owned/shared buffer, copying once into the routed packet allocation.
    /// `Delivered` means queue acceptance; `Applied` requires receiver completion.
    /// A timeout is an unknown outcome, not proof that the receiver did not act.
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
        self.inner
            .messaging
            .sender()
            .send(to, None, payload, options)
            .await
    }

    /// Receives the next node-addressed buffer without copying its payload.
    ///
    /// Shares the inbox with [`Self::recv_frame`] and both callback variants.
    /// Returns the complete context alongside the payload, including the receipt.
    /// Call [`MessageContext::applied`] after manual processing, or use
    /// [`Self::on_recv`] to acknowledge successful callback completion.
    ///
    /// # Errors
    /// Returns `WouldBlock` while another receive/callback owns this inbox,
    /// or `NotConnected` when messaging or the network closes.
    pub async fn recv(&self) -> io::Result<(MessageContext, Bytes)> {
        Ok(self.recv_frame().await?.into_parts())
    }

    /// Receives the next node-addressed frame, independently of all groups.
    /// Call [`Frame::applied`] after manually completing application work.
    ///
    /// # Errors
    /// Returns `WouldBlock` while another receive/callback owns this inbox,
    /// or `NotConnected` when messaging or the network closes.
    pub async fn recv_frame(&self) -> io::Result<Frame> {
        self.inner.messaging.recv().await
    }

    /// Registers the exclusive, serial async receiver for node-addressed buffers.
    ///
    /// Moves the complete context and payload into the callback without copying.
    /// The worker retains its receipt and acknowledges `Applied` after success.
    /// Errors reject processing and stop the worker; cancellation never
    /// acknowledges unfinished work. Keep the returned handle alive to receive.
    /// Shares the inbox with [`Self::on_frame`] and both manual receive variants.
    ///
    /// # Errors
    /// Returns `WouldBlock` if the inbox already has a receive owner, or an
    /// error if messaging or the network is closed.
    pub fn on_recv<F, Fut>(&self, mut callback: F) -> io::Result<ReceiveHandle>
    where
        F: FnMut(MessageContext, Bytes) -> Fut + Send + 'static,
        Fut: Future<Output = io::Result<()>> + Send + 'static,
    {
        self.on_frame(move |frame| {
            let (context, payload) = frame.into_parts();
            callback(context, payload)
        })
    }

    /// Registers the exclusive, serial async receiver for node-addressed frames.
    /// Successful callback completion acknowledges application; callback failure
    /// rejects the frame and is observable through the returned handle.
    ///
    /// # Errors
    /// Returns `WouldBlock` if the inbox already has a receive owner, or an
    /// error if messaging or the network is closed.
    pub fn on_frame<F, Fut>(&self, callback: F) -> io::Result<ReceiveHandle>
    where
        F: FnMut(Frame) -> Fut + Send + 'static,
        Fut: Future<Output = io::Result<()>> + Send + 'static,
    {
        self.inner.messaging.on_recv(callback)
    }

    pub(crate) async fn close_messaging(&self) {
        self.inner.messaging.close().await;
    }
}

/// A separate dispatcher keeps application traffic away from coordination/TLS.
/// Only a weak node-state reference is retained between packets; no task owns a
/// managed Network handle or a cycle through the group map.
pub(super) fn start_dispatcher(inner: &Arc<Inner>) {
    let state = Arc::downgrade(inner);
    let receiver = inner.messaging.sender().clone();
    let router = inner.transport.clone();
    let task = tokio::spawn(async move {
        loop {
            let frame = tokio::select! {
                biased;
                () = router.cancelled() => return,
                result = receiver.recv() => match result {
                    Ok(frame) => frame,
                    Err(_) => return,
                },
            };
            let Some(inner) = state.upgrade() else {
                let _ = frame.receipt().reject(io::ErrorKind::NotConnected);
                return;
            };
            if let Some(group_id) = &frame.group {
                let groups = inner.routes.lock().expect("routes mutex poisoned");
                if let Some(group) = groups.get(group_id) {
                    group.deliver_application(frame);
                } else {
                    let _ = frame.receipt().reject(io::ErrorKind::NotFound);
                }
            } else {
                inner.messaging.deliver(frame);
            }
        }
    });
    inner.messaging.set_dispatcher(task);
}
