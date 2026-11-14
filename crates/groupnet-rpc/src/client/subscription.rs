//! [`Subscription`]: the client's end of one server-push stream, granting
//! credit as the application consumes items.

use std::fmt;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{OwnedSemaphorePermit, mpsc};

use super::Peer;
use super::connection::{Conn, Event};
use crate::RpcError;
use crate::codec::Frame;

/// One open subscription, as [`RpcClient::subscribe`](crate::RpcClient::subscribe)
/// returns it.
///
/// [`next`](Self::next) yields the server's items in order. Each item the
/// application takes is consumed: the client grants the server credit for
/// consumed items, half a [`subscription_window`](crate::RpcConfig::subscription_window)
/// (rounded up) at a time, at the next call to `next`, so the server never
/// runs more than one window ahead and a subscription nobody reads only
/// holds back itself.
///
/// While open it keeps its connection from idling and its destination from
/// eviction, and holds one of the peer's
/// [`max_subscriptions_per_peer`](crate::RpcConfig::max_subscriptions_per_peer)
/// slots; reading it to its end or dropping it frees all three. Dropping an
/// open subscription cancels it: the client sends Cancel (best effort,
/// without blocking) and discards any item still in flight.
pub struct Subscription {
    id: u64,
    events: mpsc::UnboundedReceiver<Event>,
    window: NonZeroU32,
    /// Items consumed and not yet credited back.
    consumed: u32,
    shut: Arc<AtomicBool>,
    /// What the subscription holds while open; `None` once it ended.
    open: Option<Held>,
}

/// What an open subscription holds.
struct Held {
    conn: Arc<Conn>,
    _peer: Arc<Peer>,
    _slot: OwnedSemaphorePermit,
}

/// Everything [`RpcClient::subscribe`](crate::RpcClient::subscribe) hands
/// over once the subscribe is sent.
pub(super) struct Opened {
    pub(super) id: u64,
    pub(super) events: mpsc::UnboundedReceiver<Event>,
    pub(super) window: NonZeroU32,
    pub(super) conn: Arc<Conn>,
    pub(super) peer: Arc<Peer>,
    pub(super) slot: OwnedSemaphorePermit,
    pub(super) shut: Arc<AtomicBool>,
}

impl fmt::Debug for Subscription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Subscription")
            .field("id", &self.id)
            .field("window", &self.window)
            .field("open", &self.open.is_some())
            .finish_non_exhaustive()
    }
}

impl Subscription {
    pub(super) fn new(opened: Opened) -> Self {
        let Opened {
            id,
            events,
            window,
            conn,
            peer,
            slot,
            shut,
        } = opened;
        Self {
            id,
            events,
            window,
            consumed: 0,
            shut,
            open: Some(Held {
                conn,
                _peer: peer,
                _slot: slot,
            }),
        }
    }

    /// The next item, in the order the server sent them.
    ///
    /// * `Some(Ok(item))` — an item;
    /// * `None` — the server's handler ended the subscription (after its
    ///   last item), or the subscription already ended;
    /// * `Some(Err(`[`Remote`](RpcError::Remote)`(status)))` — the server
    ///   ended it with an error (the handler's, or a refusal such as
    ///   [`RpcStatus::TOO_MANY_SUBSCRIPTIONS`](crate::RpcStatus::TOO_MANY_SUBSCRIPTIONS));
    /// * `Some(Err(`[`ConnectionLost`](RpcError::ConnectionLost)`))` — the
    ///   connection broke with the subscription open;
    /// * `Some(Err(`[`Shutdown`](RpcError::Shutdown)`))` — the client was
    ///   shut down.
    ///
    /// After an error every later call returns `None`. Cancel-safe: dropping
    /// the future loses no item.
    pub async fn next(&mut self) -> Option<Result<Bytes, RpcError>> {
        self.open.as_ref()?;
        self.grant().await;
        let event = self.events.recv().await;
        match event {
            Some(Event::Item(item)) => {
                self.consumed += 1;
                Some(Ok(item))
            }
            Some(Event::End) => {
                self.open = None;
                None
            }
            Some(Event::Error(status)) => {
                self.open = None;
                Some(Err(RpcError::Remote(status)))
            }
            None => {
                self.open = None;
                Some(Err(if self.shut.load(Ordering::SeqCst) {
                    RpcError::Shutdown
                } else {
                    RpcError::ConnectionLost
                }))
            }
        }
    }

    /// Credits the server for the items consumed so far, once they reach
    /// half the window (rounded up). Waits for room in the connection's
    /// queue; dropped while waiting, it credits nothing and the next call
    /// tries again.
    async fn grant(&mut self) {
        let Some(grant) = NonZeroU32::new(self.consumed) else {
            return;
        };
        if grant.get() < self.window.get().div_ceil(2) {
            return;
        }
        let Some(open) = &self.open else {
            return;
        };
        // A gone writer means a gone connection: `recv` reports it.
        let Ok(slot) = open.conn.frames.reserve().await else {
            return;
        };
        // Raised before the frame is queued, so an item the grant allows
        // can never arrive ahead of its credit.
        if open.conn.calls.grant(self.id, grant) {
            slot.send(Frame::Credit { id: self.id, grant }.encode());
        }
        self.consumed = 0;
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        let Some(open) = self.open.take() else {
            return;
        };
        // Already ended by the server or the connection: nothing to cancel.
        if !open.conn.calls.forget(self.id) {
            return;
        }
        let cancel = Frame::Cancel { id: self.id }.encode();
        if let Err(TrySendError::Full(cancel)) = open.conn.frames.try_send(cancel) {
            // The queue is full: hand the cancel to a task rather than block.
            // Without a runtime it is lost, and the server's subscription
            // ends with the connection.
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                let frames = open.conn.frames.clone();
                runtime.spawn(async move {
                    let _ = frames.send(cancel).await;
                });
            }
        }
    }
}
