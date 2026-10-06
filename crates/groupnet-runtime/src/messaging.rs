//! Opaque node/group frames, delivery receipts, and exclusive receive callbacks.
//!
//! These messages do not use group metadata or any replicated commit path.
//! `Delivered` acknowledges a bounded application queue; `Applied` acknowledges
//! explicit application completion. A timeout is an unknown outcome. Sender
//! attribution and membership checks trust the fabric, not an authenticated channel.
//!
//! Buffer receives return ([`MessageContext`], [`Bytes`]); frame receives return
//! [`Frame`]. Both preserve the original sender, identity, group, delivery mode,
//! and receipt. [`MessageContext::applied`] acknowledges manual processing even
//! after the payload has moved elsewhere. Neither dropping the payload nor
//! dropping its context acknowledges application completion.
//!
//! `groupnet-messaging` owns the message codec, endpoint, and acknowledgement
//! state. This module owns runtime receive callbacks and group fanout reports.

use std::io;
use std::sync::Mutex;

use groupnet_core::NodeId;
use groupnet_network::Router;
use tokio::task::JoinHandle;

pub use groupnet_messaging::{
    Bytes, DEFAULT_MAX_MESSAGE_BYTES, Delivery, Frame, MessageContext, MessageId, MessageProtocol,
    Messaging, MessagingConfig, Receipt, SendOptions,
};
pub use inbox::ReceiveHandle;

mod inbox;

/// One attempt's result for every recipient selected from a membership snapshot.
///
/// An error for one recipient never hides successful deliveries to others.
/// Departures do not remove recipients and later joiners are not added.
#[derive(Debug)]
pub struct GroupSendReport {
    /// All selected peers, excluding the sender, with their individual outcomes.
    pub outcomes: Vec<RecipientOutcome>,
}

impl GroupSendReport {
    /// Whether every selected recipient succeeded (also true for an empty roster).
    #[must_use]
    pub fn all_succeeded(&self) -> bool {
        self.outcomes.iter().all(|outcome| outcome.result.is_ok())
    }
}

/// The outcome of sending one group frame to one selected member.
#[derive(Debug)]
pub struct RecipientOutcome {
    /// The selected member's logical identity.
    pub node: NodeId,
    /// Message identity on success, or delivery/timeout/rejection failure.
    pub result: io::Result<MessageId>,
}

/// The runtime owner of the network's dedicated application inbox.
#[derive(Debug)]
pub(crate) struct Hub {
    sender: Messaging,
    node: Inbox,
    dispatcher: Mutex<Option<JoinHandle<()>>>,
}

use inbox::Inbox;

impl Hub {
    pub(crate) fn new(router: &Router, config: MessagingConfig) -> io::Result<Self> {
        let sender = Messaging::with_config(router, config)?;
        Ok(Self {
            node: Inbox::new(sender.cancellation(), sender.config().inbox_capacity),
            sender,
            dispatcher: Mutex::new(None),
        })
    }

    pub(crate) fn sender(&self) -> &Messaging {
        &self.sender
    }

    pub(crate) fn set_dispatcher(&self, task: JoinHandle<()>) {
        *self.dispatcher.lock().expect("dispatcher mutex poisoned") = Some(task);
    }

    pub(crate) fn group(&self) -> GroupMessaging {
        GroupMessaging {
            sender: self.sender.clone(),
            inbox: Inbox::new(
                self.sender.cancellation(),
                self.sender.config().inbox_capacity,
            ),
        }
    }

    pub(crate) fn deliver(&self, frame: Frame) {
        self.node.deliver(frame);
    }

    pub(crate) async fn recv(&self) -> io::Result<Frame> {
        self.node.recv().await
    }

    pub(crate) fn on_recv<F, Fut>(&self, callback: F) -> io::Result<ReceiveHandle>
    where
        F: FnMut(Frame) -> Fut + Send + 'static,
        Fut: Future<Output = io::Result<()>> + Send + 'static,
    {
        self.node.on_recv(callback)
    }

    pub(crate) async fn close(&self) {
        self.sender.close().await;
        let dispatcher = self
            .dispatcher
            .lock()
            .expect("dispatcher mutex poisoned")
            .take();
        if let Some(dispatcher) = dispatcher {
            let _ = dispatcher.await;
        }
    }
}

/// Group handles own their queue and send plane, never the managed Network.
#[derive(Clone, Debug)]
pub(crate) struct GroupMessaging {
    pub(crate) sender: Messaging,
    inbox: Inbox,
}

impl GroupMessaging {
    pub(crate) fn ensure_open(&self) -> io::Result<()> {
        self.inbox.ensure_open()
    }

    pub(crate) fn deliver(&self, frame: Frame) {
        self.inbox.deliver(frame);
    }

    pub(crate) async fn recv(&self) -> io::Result<Frame> {
        self.inbox.recv().await
    }

    pub(crate) fn on_recv<F, Fut>(&self, callback: F) -> io::Result<ReceiveHandle>
    where
        F: FnMut(Frame) -> Fut + Send + 'static,
        Fut: Future<Output = io::Result<()>> + Send + 'static,
    {
        self.inbox.on_recv(callback)
    }
}
