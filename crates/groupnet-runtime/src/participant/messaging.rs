//! Node-addressed application messages and the dispatcher that feeds them.

use std::io;
use std::sync::Arc;

use groupnet_core::NodeId;

use super::{Participant, Shared};
use crate::messaging::{Bytes, Frame, MessageContext, MessageId, ReceiveHandle, SendOptions};

impl Participant {
    /// Sends an opaque borrowed buffer to one node, using best-effort delivery.
    /// Copies into owned bytes before routed packet encoding; `send_frame` accepts `Bytes`.
    ///
    /// # Errors
    /// Rejects oversized payloads, shutdown, or local routing enqueue failures.
    pub async fn send(&self, to: &NodeId, payload: impl AsRef<[u8]>) -> io::Result<MessageId> {
        let payload = payload.as_ref();
        let options = SendOptions::default();
        self.shared
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
        self.shared
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
        self.shared.messaging.recv().await
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
        self.shared.messaging.on_recv(callback)
    }
}

/// A separate dispatcher keeps application traffic away from coordination/TLS.
///
/// Node-addressed frames go to the participant's inbox; group-addressed frames
/// go to `groups`, which owns their delivery or rejection. Only a weak
/// reference to the protocol state is retained between packets, so no task
/// owns the managed network.
pub(crate) fn start_dispatcher<G>(participant: &Participant, mut groups: G)
where
    G: FnMut(Frame) + Send + 'static,
{
    let state = Arc::downgrade(&participant.shared);
    let receiver = participant.shared.messaging.sender().clone();
    let router = participant.router().clone();
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
            let Some(shared) = state.upgrade() else {
                let _ = frame.receipt().reject(io::ErrorKind::NotConnected);
                return;
            };
            deliver(&shared, frame, &mut groups);
        }
    });
    participant.shared.messaging.set_dispatcher(task);
}

fn deliver<G: FnMut(Frame)>(shared: &Shared, frame: Frame, groups: &mut G) {
    if frame.group.is_some() {
        groups(frame);
    } else {
        shared.messaging.deliver(frame);
    }
}
