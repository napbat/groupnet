use std::collections::HashMap;
use std::io;

use groupnet_core::{GroupId, NodeId};
use groupnet_messaging::Messaging;
use tokio::task::JoinSet;

use super::Group;
use crate::messaging::{
    Bytes, Frame, GroupSendReport, MessageContext, MessageId, ReceiveHandle, RecipientOutcome,
    SendOptions,
};

impl Group {
    /// Sends a borrowed opaque buffer to a snapshot of the live members,
    /// excluding this node, with best-effort delivery and shared fanout plaintext.
    /// This never becomes a Hosted/quorum commit or a metadata operation.
    ///
    /// # Errors
    /// Invalid payloads and shutdown before fanout return an error. Once fanout
    /// starts, each selected member's result is included in the report.
    pub async fn send(&self, payload: impl AsRef<[u8]>) -> io::Result<GroupSendReport> {
        let payload = payload.as_ref();
        let options = SendOptions::default();
        self.messaging
            .sender
            .validate_send(options, payload.len())?;
        self.send_frame(Bytes::copy_from_slice(payload), options)
            .await
    }

    /// Sends a shared buffer to a frozen local membership snapshot, excluding
    /// this node, within the configured recipient-send concurrency.
    ///
    /// Departures do not remove selected recipients; late joiners are not added.
    /// Every selected peer has a result, including rejection and timeout (whose
    /// outcome is unknown). An empty snapshot succeeds with an empty report.
    /// Receivers reject sources absent/dead in their own live membership view:
    /// views can lag, and this check is not authentication.
    ///
    /// # Errors
    /// Invalid size/options, a left group, or shutdown observed before fanout
    /// returns an error without sending to anyone, including for empty groups.
    pub async fn send_frame(
        &self,
        payload: Bytes,
        options: SendOptions,
    ) -> io::Result<GroupSendReport> {
        self.messaging
            .sender
            .validate_send(options, payload.len())?;
        self.messaging.ensure_open()?;
        let snapshot = self.members_rx.borrow().clone();
        if !snapshot.contains(&self.local) {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "local node has left the group",
            ));
        }
        let recipients = snapshot
            .iter()
            .filter(|node| *node != &self.local)
            .cloned()
            .collect();
        Ok(fanout(
            &self.messaging.sender,
            &self.id,
            recipients,
            payload,
            options,
        )
        .await)
    }

    /// Receives this group's next buffer without copying its payload.
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

    /// Receives this group's next application frame, independently of node
    /// messages, other groups, and coordination protocol traffic.
    /// Call [`Frame::applied`] after manually completing application work.
    ///
    /// # Errors
    /// Returns `WouldBlock` while another receive/callback owns this inbox,
    /// or `NotConnected` when messaging or the network closes.
    pub async fn recv_frame(&self) -> io::Result<Frame> {
        self.messaging.recv().await
    }

    /// Registers this group's exclusive serial async buffer receiver.
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

    /// Registers this group's exclusive serial async application receiver.
    /// Successful completion acknowledges application; an error rejects the
    /// frame and terminates the callback with its error observable on the handle.
    ///
    /// # Errors
    /// Returns `WouldBlock` if the inbox already has a receive owner, or an
    /// error if messaging or the network is closed.
    pub fn on_frame<F, Fut>(&self, callback: F) -> io::Result<ReceiveHandle>
    where
        F: FnMut(Frame) -> Fut + Send + 'static,
        Fut: Future<Output = io::Result<()>> + Send + 'static,
    {
        self.messaging.on_recv(callback)
    }

    pub(crate) fn deliver_application(&self, frame: Frame) {
        // One immutable snapshot prices both local participation and the source.
        // Only live members (Alive or Suspect) are eligible; lag fails closed.
        let members = self.members_rx.borrow().clone();
        if !members.contains(&self.local) {
            let _ = frame.receipt().reject(io::ErrorKind::NotConnected);
        } else if !members.contains(&frame.from) {
            let _ = frame.receipt().reject(io::ErrorKind::PermissionDenied);
        } else {
            self.messaging.deliver(frame);
        }
    }
}

async fn fanout(
    sender: &Messaging,
    group: &GroupId,
    recipients: Vec<NodeId>,
    payload: Bytes,
    options: SendOptions,
) -> GroupSendReport {
    let mut tasks = JoinSet::new();
    let concurrency = sender.config().fanout_concurrency.get();
    let mut active = HashMap::with_capacity(concurrency.min(recipients.len()));
    let mut results: Vec<Option<io::Result<MessageId>>> = std::iter::repeat_with(|| None)
        .take(recipients.len())
        .collect();
    let mut next = 0;
    loop {
        while next < recipients.len() && tasks.len() < concurrency {
            let sender = sender.clone();
            let to = recipients[next].clone();
            let group = group.clone();
            let payload = payload.clone();
            let task =
                tasks.spawn(async move { sender.send(&to, Some(&group), payload, options).await });
            active.insert(task.id(), next);
            next += 1;
        }
        let Some(completed) = tasks.join_next_with_id().await else {
            break;
        };
        let (id, result) = match completed {
            Ok((id, result)) => (id, result),
            Err(error) => (error.id(), Err(io::Error::other(error))),
        };
        if let Some(index) = active.remove(&id) {
            results[index] = Some(result);
        }
    }
    GroupSendReport {
        outcomes: recipients
            .into_iter()
            .zip(results)
            .map(|(node, result)| RecipientOutcome {
                node,
                result: result.unwrap_or_else(|| {
                    Err(io::Error::other("recipient send task lost its result"))
                }),
            })
            .collect(),
    }
}
