//! Server push: the subscribe handler contract, the [`SubscriptionSink`] a
//! handler sends items through, and one connection's table of live
//! subscriptions with their credit.
//!
//! Flow control is per subscription: the client's Subscribe carries a
//! `window`, every item spends one credit, and each Credit frame adds its
//! `grant`. A sink with no credit waits, so a slow subscription only ever
//! holds back itself; its items still go through the connection's single
//! writer, one bounded queue slot at a time.

use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::future::{Ready, ready};
use std::num::NonZeroU32;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures_util::FutureExt;
use groupnet_core::NodeId;
use tokio::sync::{Notify, mpsc};
use tokio::task::{AbortHandle, JoinSet};

use crate::codec::{ERROR_HEAD, Encoded, Frame, ITEM_HEAD};
use crate::config::FrameLimit;
use crate::{RpcStatus, lock};

mod sealed {
    /// Keeps [`SubscribeHandler`](super::SubscribeHandler) closed to the
    /// two shapes this crate runs.
    pub trait Sealed {}
}

/// The subscribe handler of an [`RpcServer`](crate::RpcServer): either
/// [`NoSubscriptions`] (a server built with
/// [`RpcServer::new`](crate::RpcServer::new)) or any
/// `Fn(NodeId, Bytes, SubscriptionSink) -> impl Future<Output = Result<(), RpcStatus>>`
/// that is `Clone + Send + 'static` with a `Send + 'static` future (one given
/// to [`RpcServer::with_subscriptions`](crate::RpcServer::with_subscriptions)).
///
/// Sealed: it names the two shapes, it is not an extension point.
pub trait SubscribeHandler: sealed::Sealed + Clone + Send + 'static {
    /// The future one subscription runs.
    type Future: Future<Output = Result<(), RpcStatus>> + Send + 'static;

    /// Whether this server takes subscriptions at all; without, every
    /// Subscribe is answered [`RpcStatus::SUBSCRIPTIONS_UNSUPPORTED`].
    const ENABLED: bool;

    /// Runs the handler for one subscription opened by `from` with
    /// `payload`, sending items through `sink`.
    fn open(&self, from: NodeId, payload: Bytes, sink: SubscriptionSink) -> Self::Future;
}

/// The subscribe handler of a server that takes no subscriptions: every
/// Subscribe is answered [`RpcStatus::SUBSCRIPTIONS_UNSUPPORTED`] and the
/// connection stays up.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NoSubscriptions;

impl sealed::Sealed for NoSubscriptions {}

impl SubscribeHandler for NoSubscriptions {
    type Future = Ready<Result<(), RpcStatus>>;

    const ENABLED: bool = false;

    fn open(&self, _from: NodeId, _payload: Bytes, _sink: SubscriptionSink) -> Self::Future {
        ready(Err(unsupported()))
    }
}

impl<S, F> sealed::Sealed for S
where
    S: Fn(NodeId, Bytes, SubscriptionSink) -> F,
    F: Future<Output = Result<(), RpcStatus>>,
{
}

impl<S, F> SubscribeHandler for S
where
    S: Fn(NodeId, Bytes, SubscriptionSink) -> F + Clone + Send + 'static,
    F: Future<Output = Result<(), RpcStatus>> + Send + 'static,
{
    type Future = F;

    const ENABLED: bool = true;

    fn open(&self, from: NodeId, payload: Bytes, sink: SubscriptionSink) -> F {
        self(from, payload, sink)
    }
}

fn unsupported() -> RpcStatus {
    RpcStatus::new(
        RpcStatus::SUBSCRIPTIONS_UNSUPPORTED,
        "this server takes no subscriptions",
    )
}

/// Why a [`SubscriptionSink::send`] did not send its item. Every variant is
/// final: the subscription is over and every later send fails the same way.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SinkError {
    /// The client cancelled the subscription (it dropped its
    /// [`Subscription`](crate::Subscription)). The server also drops the
    /// handler's future, so only a sink moved elsewhere sees this.
    Cancelled,
    /// The connection carrying the subscription ended, or the server shut
    /// down.
    Disconnected,
    /// The subscription already ended: its handler returned, or an earlier
    /// item was too large.
    Ended,
    /// This item exceeds the server's `max_frame_bytes`; it was not sent and
    /// the subscription ended with an [`RpcStatus::RESPONSE_TOO_LARGE`]
    /// error after the items before it.
    TooLarge,
}

impl fmt::Display for SinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Cancelled => "rpc subscription cancelled by the client",
            Self::Disconnected => "rpc subscription's connection ended",
            Self::Ended => "rpc subscription already ended",
            Self::TooLarge => "rpc subscription item exceeds the frame limit",
        })
    }
}

impl Error for SinkError {}

/// Where one subscription's handler sends its items, in order.
///
/// [`send`](Self::send) waits for the client's credit, so the server never
/// runs more than the client's window ahead of what the client consumed.
/// The handler returning ends the subscription: `Ok(())` sends End,
/// `Err(status)` an Error with that status. A sink outlives neither: once
/// the subscription is over (ended, cancelled, or its connection gone) every
/// send fails with a [`SinkError`].
pub struct SubscriptionSink {
    id: u64,
    state: Arc<State>,
    /// Weak, so a sink moved out of its handler never keeps the connection's
    /// writer (and thus the connection) alive.
    frames: mpsc::WeakSender<Encoded>,
    max_frame_bytes: FrameLimit,
}

impl fmt::Debug for SubscriptionSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubscriptionSink")
            .field("id", &self.id)
            .field("ended", &self.state.ended())
            .finish_non_exhaustive()
    }
}

impl SubscriptionSink {
    /// Sends `item` once the client has credit for it.
    ///
    /// Waits while the client has not consumed enough earlier items; the
    /// wait ends with an error if the subscription is cancelled or its
    /// connection ends meanwhile. Dropping the returned future sends
    /// nothing.
    ///
    /// # Errors
    /// [`SinkError::TooLarge`] if `item` exceeds the server's
    /// `max_frame_bytes` (the subscription then ends with
    /// [`RpcStatus::RESPONSE_TOO_LARGE`]); otherwise why the subscription is
    /// already over.
    pub async fn send(&mut self, item: Bytes) -> Result<(), SinkError> {
        if item.len() > self.max_frame_bytes.body(ITEM_HEAD) {
            let status = RpcStatus::new(
                RpcStatus::RESPONSE_TOO_LARGE,
                format!(
                    "subscription item of {} bytes exceeds the {}-byte frame limit",
                    item.len(),
                    self.max_frame_bytes.get()
                ),
            )
            .truncated(self.max_frame_bytes.body(ERROR_HEAD));
            let Some(frames) = self.frames.upgrade() else {
                return Err(self.state.end(SinkError::Disconnected));
            };
            let terminal = Frame::Error {
                id: self.id,
                status,
            };
            return Err(
                match self
                    .state
                    .finish(&frames, terminal, SinkError::TooLarge)
                    .await
                {
                    Ok(()) => SinkError::TooLarge,
                    Err(ended) => ended,
                },
            );
        }
        // Credit before a writer slot: a subscription waiting for credit
        // holds nothing the connection's calls or other subscriptions need.
        let credit = self.state.take_credit().await?;
        let Some(frames) = self.frames.upgrade() else {
            return Err(self.state.end(SinkError::Disconnected));
        };
        let Ok(slot) = frames.reserve().await else {
            return Err(self.state.end(SinkError::Disconnected));
        };
        let frame = Frame::Item {
            id: self.id,
            payload: item,
        }
        .encode();
        // Under the state lock, so no item is ever queued after the
        // subscription's terminal frame.
        let state = lock(&self.state.credit);
        if let Some(ended) = state.ended {
            return Err(ended);
        }
        slot.send(frame);
        credit.spend();
        Ok(())
    }
}

/// One credit taken for an item; returned unless the item is queued, so a
/// send dropped while it waits for a writer slot costs the window nothing.
struct Spent<'a> {
    state: Option<&'a State>,
}

impl Spent<'_> {
    /// The item was queued: the credit stays spent.
    fn spend(mut self) {
        self.state = None;
    }
}

impl Drop for Spent<'_> {
    fn drop(&mut self) {
        if let Some(state) = self.state {
            lock(&state.credit).available += 1;
            state.wake.notify_one();
        }
    }
}

/// One subscription's credit and fate, shared by its sink, its handler task
/// and the connection's table.
struct State {
    /// The subscribe's window: at no time may unspent credit exceed it.
    window: u64,
    credit: Mutex<Credit>,
    /// Wakes the sink waiting for credit (or for the end).
    wake: Notify,
}

struct Credit {
    /// Items the server may still send.
    available: u64,
    /// Set once the subscription is over; never cleared.
    ended: Option<SinkError>,
}

/// A client broke the subscription protocol: the connection is dropped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Violation;

impl State {
    fn new(window: NonZeroU32) -> Self {
        Self {
            window: u64::from(window.get()),
            credit: Mutex::new(Credit {
                available: u64::from(window.get()),
                ended: None,
            }),
            wake: Notify::new(),
        }
    }

    fn ended(&self) -> Option<SinkError> {
        lock(&self.credit).ended
    }

    /// Ends the subscription for `reason` unless it is already over, and
    /// returns why it is over.
    fn end(&self, reason: SinkError) -> SinkError {
        let ended = *lock(&self.credit).ended.get_or_insert(reason);
        self.wake.notify_one();
        ended
    }

    /// Takes one credit, waiting for a grant while there is none.
    async fn take_credit(&self) -> Result<Spent<'_>, SinkError> {
        loop {
            // Registered before the check: a grant between the check and the
            // wait stores its wake-up in the `Notify`.
            let granted = self.wake.notified();
            {
                let mut credit = lock(&self.credit);
                if let Some(ended) = credit.ended {
                    return Err(ended);
                }
                if credit.available > 0 {
                    credit.available -= 1;
                    return Ok(Spent { state: Some(self) });
                }
            }
            granted.await;
        }
    }

    /// Adds `grant` credit. A well-behaved client grants only for items it
    /// consumed, so unspent credit never exceeds the window; more is a
    /// protocol violation.
    fn grant(&self, grant: NonZeroU32) -> Result<(), Violation> {
        {
            let mut credit = lock(&self.credit);
            if credit.ended.is_some() {
                return Ok(());
            }
            let available = credit.available + u64::from(grant.get());
            if available > self.window {
                return Err(Violation);
            }
            credit.available = available;
        }
        self.wake.notify_one();
        Ok(())
    }

    /// Queues `terminal` and ends the subscription with `reason`, unless it
    /// is already over (then nothing is sent and the earlier reason is
    /// returned).
    async fn finish(
        &self,
        frames: &mpsc::Sender<Encoded>,
        terminal: Frame,
        reason: SinkError,
    ) -> Result<(), SinkError> {
        let Ok(slot) = frames.reserve().await else {
            return Err(self.end(SinkError::Disconnected));
        };
        {
            let mut credit = lock(&self.credit);
            if let Some(ended) = credit.ended {
                return Err(ended);
            }
            credit.ended = Some(reason);
            slot.send(terminal.encode());
        }
        self.wake.notify_one();
        Ok(())
    }
}

/// One live subscription in a connection's table.
struct Live {
    state: Arc<State>,
    task: AbortHandle,
}

/// A connection's live subscriptions. Dropping it — the connection ending —
/// ends every one of them, so a sink held anywhere fails
/// [`SinkError::Disconnected`]; their handler tasks are aborted with the
/// connection's task set.
pub(super) struct Subscriptions {
    live: Arc<Mutex<HashMap<u64, Live>>>,
    max: usize,
    max_frame_bytes: FrameLimit,
}

/// What a Subscribe frame asked for.
pub(super) struct Opening {
    pub(super) from: NodeId,
    pub(super) id: u64,
    pub(super) window: NonZeroU32,
    pub(super) payload: Bytes,
}

impl Subscriptions {
    pub(super) fn new(max: usize, max_frame_bytes: FrameLimit) -> Self {
        Self {
            live: Arc::new(Mutex::new(HashMap::new())),
            max,
            max_frame_bytes,
        }
    }

    /// Starts the subscription `opening` asks for on `work`, or returns the
    /// error frame that answers why not:
    /// [`RpcStatus::SUBSCRIPTIONS_UNSUPPORTED`] without a handler,
    /// [`RpcStatus::TOO_MANY_SUBSCRIPTIONS`] at the limit.
    ///
    /// # Errors
    /// [`Violation`] for an id that names a live subscription.
    pub(super) fn open<S: SubscribeHandler>(
        &self,
        opening: Opening,
        handler: &S,
        frames: &mpsc::Sender<Encoded>,
        work: &mut JoinSet<()>,
    ) -> Result<Option<Encoded>, Violation> {
        let Opening {
            from,
            id,
            window,
            payload,
        } = opening;
        let refusal = if S::ENABLED {
            let mut live = lock(&self.live);
            if live.contains_key(&id) {
                return Err(Violation);
            }
            if live.len() < self.max {
                let state = Arc::new(State::new(window));
                let sink = SubscriptionSink {
                    id,
                    state: state.clone(),
                    frames: frames.downgrade(),
                    max_frame_bytes: self.max_frame_bytes,
                };
                // Spawned under the table lock, so the task's own removal
                // always finds its entry in place.
                let task = work.spawn(run(
                    Running {
                        id,
                        state: state.clone(),
                        live: self.live.clone(),
                        frames: frames.clone(),
                        max_frame_bytes: self.max_frame_bytes,
                    },
                    guarded(handler.clone(), from, payload, sink),
                ));
                live.insert(id, Live { state, task });
                return Ok(None);
            }
            RpcStatus::new(
                RpcStatus::TOO_MANY_SUBSCRIPTIONS,
                format!("at most {} live subscriptions per connection", self.max),
            )
        } else {
            unsupported()
        };
        let status = refusal.truncated(self.max_frame_bytes.body(ERROR_HEAD));
        Ok(Some(Frame::Error { id, status }.encode()))
    }

    /// Adds credit to subscription `id`; credit for one already over is
    /// late, not wrong, and ignored.
    ///
    /// # Errors
    /// [`Violation`] for credit beyond the subscription's window.
    pub(super) fn credit(&self, id: u64, grant: NonZeroU32) -> Result<(), Violation> {
        let state = lock(&self.live).get(&id).map(|live| live.state.clone());
        state.map_or(Ok(()), |state| state.grant(grant))
    }

    /// Cancels subscription `id`: its handler is dropped and its sink fails
    /// [`SinkError::Cancelled`]; nothing more is sent for it. An id already
    /// over is ignored.
    pub(super) fn cancel(&self, id: u64) {
        let cancelled = lock(&self.live).remove(&id);
        if let Some(live) = cancelled {
            live.state.end(SinkError::Cancelled);
            live.task.abort();
        }
    }
}

impl Drop for Subscriptions {
    fn drop(&mut self) {
        let live = std::mem::take(&mut *lock(&self.live));
        for (_, live) in live {
            live.state.end(SinkError::Disconnected);
            live.task.abort();
        }
    }
}

/// The handler's run with the call itself inside the guarded future, so a
/// handler that panics before returning its future is caught too; a panic is
/// answered [`RpcStatus::HANDLER_PANICKED`].
async fn guarded<S: SubscribeHandler>(
    handler: S,
    from: NodeId,
    payload: Bytes,
    sink: SubscriptionSink,
) -> Result<(), RpcStatus> {
    AssertUnwindSafe(async move { handler.open(from, payload, sink).await })
        .catch_unwind()
        .await
        .unwrap_or_else(|_panic| {
            Err(RpcStatus::new(
                RpcStatus::HANDLER_PANICKED,
                "subscribe handler panicked",
            ))
        })
}

/// What a subscription's task needs besides the handler's future.
struct Running {
    id: u64,
    state: Arc<State>,
    live: Arc<Mutex<HashMap<u64, Live>>>,
    frames: mpsc::Sender<Encoded>,
    max_frame_bytes: FrameLimit,
}

/// Runs one subscription's handler and sends its terminal frame: End for
/// `Ok(())`, Error for `Err(status)` (a panic is
/// [`RpcStatus::HANDLER_PANICKED`]). Leaves the table first, so the client
/// may open another the moment it sees the end.
async fn run(running: Running, handler: impl Future<Output = Result<(), RpcStatus>>) {
    let Running {
        id,
        state,
        live,
        frames,
        max_frame_bytes,
    } = running;
    let outcome = handler.await;
    {
        let mut live = lock(&live);
        if live
            .get(&id)
            .is_some_and(|entry| Arc::ptr_eq(&entry.state, &state))
        {
            live.remove(&id);
        }
    }
    let terminal = match outcome {
        Ok(()) => Frame::End { id },
        Err(status) => Frame::Error {
            id,
            status: status.truncated(max_frame_bytes.body(ERROR_HEAD)),
        },
    };
    // Already over (an oversized item ended it) or the connection is gone:
    // nothing more to send.
    let _ = state.finish(&frames, terminal, SinkError::Ended).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sink_errors_describe_their_cause() {
        for (error, needle) in [
            (SinkError::Cancelled, "cancelled"),
            (SinkError::Disconnected, "connection"),
            (SinkError::Ended, "ended"),
            (SinkError::TooLarge, "frame limit"),
        ] {
            assert!(error.to_string().contains(needle), "{error:?}");
        }
    }

    #[test]
    fn credit_beyond_the_window_is_a_violation() {
        let state = State::new(NonZeroU32::new(4).unwrap());
        assert_eq!(state.grant(NonZeroU32::MIN), Err(Violation));
        lock(&state.credit).available = 1;
        assert_eq!(state.grant(NonZeroU32::new(3).unwrap()), Ok(()));
        assert_eq!(lock(&state.credit).available, 4);
        state.end(SinkError::Ended);
        assert_eq!(
            state.grant(NonZeroU32::MAX),
            Ok(()),
            "late credit for an ended subscription is ignored"
        );
    }

    #[tokio::test]
    async fn a_dropped_credit_is_returned_and_a_spent_one_is_not() {
        let state = State::new(NonZeroU32::MIN);
        drop(state.take_credit().await.expect("the window's one credit"));
        assert_eq!(lock(&state.credit).available, 1, "returned unspent");
        state.take_credit().await.expect("returned credit").spend();
        assert_eq!(lock(&state.credit).available, 0);
    }

    #[tokio::test]
    async fn a_sink_waits_for_credit_and_wakes_on_a_grant_or_the_end() {
        let state = Arc::new(State::new(NonZeroU32::MIN));
        state
            .take_credit()
            .await
            .expect("the window's one credit")
            .spend();
        let waiting = tokio::spawn({
            let state = state.clone();
            async move { state.take_credit().await.map(Spent::spend) }
        });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished(), "no credit left");
        state.grant(NonZeroU32::MIN).expect("within the window");
        assert_eq!(waiting.await.expect("task"), Ok(()));

        let waiting = tokio::spawn({
            let state = state.clone();
            async move { state.take_credit().await.map(Spent::spend) }
        });
        assert_eq!(state.end(SinkError::Cancelled), SinkError::Cancelled);
        assert_eq!(waiting.await.expect("task"), Err(SinkError::Cancelled));
        assert_eq!(
            state.end(SinkError::Disconnected),
            SinkError::Cancelled,
            "the first reason sticks"
        );
    }
}
