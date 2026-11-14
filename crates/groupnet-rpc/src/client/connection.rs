//! One client connection: the writer's queue, the table of calls and
//! subscriptions awaiting frames, and the task that drives both halves of
//! the stream.

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures_util::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use groupnet_transport::bulk::DataStream;
use tokio::sync::{mpsc, oneshot};
use tokio::task::AbortHandle;
use tokio::time::{Instant, sleep_until};

use crate::codec::{Encoded, Frame};
use crate::config::{FrameLimit, RpcConfig};
use crate::{RpcStatus, lock, write_frames};

/// One live connection: the writer's queue, the calls and subscriptions
/// awaiting frames, and the task that drives both halves of the stream.
pub(super) struct Conn {
    pub(super) frames: mpsc::Sender<Encoded>,
    pub(super) calls: Arc<Calls>,
    task: AbortHandle,
}

/// The calls awaiting an answer and the subscriptions awaiting items on one
/// connection. Ids are unique across both.
pub(super) struct Calls {
    state: Mutex<CallsState>,
}

struct CallsState {
    /// Set once the connection is gone; nothing registers after it.
    closed: bool,
    waiting: HashMap<u64, oneshot::Sender<Result<Bytes, RpcStatus>>>,
    subscriptions: HashMap<u64, Feed>,
    /// When `waiting` and `subscriptions` last both became empty (or the
    /// connection opened).
    idle_since: Instant,
}

/// What a subscription's reader-side end holds: where its frames go, and
/// the credit that bounds them.
struct Feed {
    events: mpsc::UnboundedSender<Event>,
    /// Items the server may have sent so far: the window plus every grant.
    allowed: u64,
    /// Items received so far; never above `allowed` from a well-behaved
    /// server, which bounds the unbounded channel by the window.
    received: u64,
}

/// One frame for a subscription, in arrival order.
#[derive(Debug)]
pub(super) enum Event {
    Item(Bytes),
    End,
    Error(RpcStatus),
}

/// The server broke the subscription protocol: the connection is dropped.
struct Violation;

impl Conn {
    pub(super) fn spawn<S>(stream: DataStream<S>, config: &RpcConfig) -> Self
    where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        let (frames, queue) = mpsc::channel(config.max_in_flight_per_peer.get());
        let calls = Arc::new(Calls::new());
        let task = tokio::spawn(drive(
            stream,
            queue,
            calls.clone(),
            config.max_frame_bytes,
            config.idle_timeout,
        ))
        .abort_handle();
        Self {
            frames,
            calls,
            task,
        }
    }

    /// Tears the connection down and fails its waiting calls and
    /// subscriptions.
    pub(super) fn close(&self) {
        self.task.abort();
        self.calls.close();
    }
}

impl Drop for Conn {
    fn drop(&mut self) {
        self.close();
    }
}

impl Calls {
    fn new() -> Self {
        Self {
            state: Mutex::new(CallsState {
                closed: false,
                waiting: HashMap::new(),
                subscriptions: HashMap::new(),
                idle_since: Instant::now(),
            }),
        }
    }

    /// Registers call `id`, unless the connection is already gone.
    pub(super) fn register(&self, id: u64) -> Option<oneshot::Receiver<Result<Bytes, RpcStatus>>> {
        let mut state = lock(&self.state);
        if state.closed {
            return None;
        }
        let (tx, rx) = oneshot::channel();
        state.waiting.insert(id, tx);
        Some(rx)
    }

    /// Registers subscription `id` with `window` items of credit, unless the
    /// connection is already gone.
    pub(super) fn subscribe(
        &self,
        id: u64,
        window: NonZeroU32,
    ) -> Option<mpsc::UnboundedReceiver<Event>> {
        let mut state = lock(&self.state);
        if state.closed {
            return None;
        }
        let (events, rx) = mpsc::unbounded_channel();
        state.subscriptions.insert(
            id,
            Feed {
                events,
                allowed: u64::from(window.get()),
                received: 0,
            },
        );
        Some(rx)
    }

    /// Raises subscription `id`'s credit by `grant`; `false` if it is over.
    pub(super) fn grant(&self, id: u64, grant: NonZeroU32) -> bool {
        let mut state = lock(&self.state);
        let Some(feed) = state.subscriptions.get_mut(&id) else {
            return false;
        };
        feed.allowed += u64::from(grant.get());
        true
    }

    /// Hands call `id` its answer — or, for an error naming a subscription,
    /// ends it; an answer nobody waits for is discarded.
    fn answer(&self, id: u64, answer: Result<Bytes, RpcStatus>) {
        let mut state = lock(&self.state);
        if let Some(waiter) = state.waiting.remove(&id) {
            state.touch();
            drop(state);
            let _ = waiter.send(answer);
        } else if let Err(status) = answer
            && let Some(feed) = state.subscriptions.remove(&id)
        {
            state.touch();
            let _ = feed.events.send(Event::Error(status));
        }
    }

    /// Routes one item to its subscription; an item for one that is over
    /// is late and discarded.
    fn item(&self, id: u64, item: Bytes) -> Result<(), Violation> {
        let mut state = lock(&self.state);
        let Some(feed) = state.subscriptions.get_mut(&id) else {
            return Ok(());
        };
        feed.received += 1;
        if feed.received > feed.allowed {
            return Err(Violation);
        }
        // A receiver dropped since is unsubscribing; nothing to deliver.
        let _ = feed.events.send(Event::Item(item));
        Ok(())
    }

    /// Ends subscription `id` after its items.
    fn end(&self, id: u64) {
        let mut state = lock(&self.state);
        if let Some(feed) = state.subscriptions.remove(&id) {
            state.touch();
            let _ = feed.events.send(Event::End);
        }
    }

    /// Removes call or subscription `id`; `true` if it was still there.
    pub(super) fn forget(&self, id: u64) -> bool {
        let mut state = lock(&self.state);
        let removed =
            state.waiting.remove(&id).is_some() || state.subscriptions.remove(&id).is_some();
        if removed {
            state.touch();
        }
        removed
    }

    pub(super) fn is_closed(&self) -> bool {
        lock(&self.state).closed
    }

    pub(super) fn idle_since(&self) -> Instant {
        lock(&self.state).idle_since
    }

    /// Closes the table once no call or subscription has used it for
    /// `idle`; otherwise returns when to check again. Closing under the lock
    /// means nothing can register on a connection that is about to end.
    fn close_if_idle(&self, idle: Duration) -> Option<Instant> {
        let mut state = lock(&self.state);
        let now = Instant::now();
        if !state.is_empty() {
            return Some(now + idle);
        }
        let due = state.idle_since + idle;
        if now < due {
            return Some(due);
        }
        state.closed = true;
        None
    }

    /// Marks the connection gone and drops every waiter and feed, which
    /// wakes each with a closed channel.
    fn close(&self) {
        let (waiting, subscriptions) = {
            let mut state = lock(&self.state);
            state.closed = true;
            (
                std::mem::take(&mut state.waiting),
                std::mem::take(&mut state.subscriptions),
            )
        };
        drop((waiting, subscriptions));
    }
}

impl CallsState {
    fn is_empty(&self) -> bool {
        self.waiting.is_empty() && self.subscriptions.is_empty()
    }

    /// Starts the idle clock once the last call or subscription is gone.
    fn touch(&mut self) {
        if self.is_empty() {
            self.idle_since = Instant::now();
        }
    }
}

/// Removes a call's waiter however the call ends — answered, timed out, or
/// abandoned by its caller.
pub(super) struct Waiting<'a> {
    pub(super) calls: &'a Calls,
    pub(super) id: u64,
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.calls.forget(self.id);
    }
}

/// Closes a connection's table when its task ends, however it ends.
struct CloseOnExit(Arc<Calls>);

impl Drop for CloseOnExit {
    fn drop(&mut self) {
        self.0.close();
    }
}

/// Resolves once the connection has had no call or subscription for
/// `idle`, having closed its table.
async fn idle_out(calls: &Calls, idle: Duration) {
    let mut check = calls.idle_since() + idle;
    loop {
        sleep_until(check).await;
        match calls.close_if_idle(idle) {
            Some(next) => check = next,
            None => return,
        }
    }
}

/// Drives one client connection: the writer drains the frame queue while
/// the reader routes answers to their calls and items to their
/// subscriptions. Either side failing, or the connection idling out, ends
/// both.
async fn drive<S>(
    stream: DataStream<S>,
    mut queue: mpsc::Receiver<Encoded>,
    calls: Arc<Calls>,
    max_frame_bytes: FrameLimit,
    idle: Duration,
) where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let _close = CloseOnExit(calls.clone());
    let (read_half, write_half) = stream.into_inner().split();
    let mut reader = DataStream::new(read_half);
    let mut writer = DataStream::new(write_half);
    let read = async {
        // A clean end, a read error, a malformed frame, a frame only a
        // client sends, or an item beyond its credit all end the connection.
        while let Ok(Some(bytes)) = reader.recv_bounded(max_frame_bytes.frame_bytes()).await {
            match Frame::decode(&bytes) {
                Ok(Frame::Response { id, payload }) => calls.answer(id, Ok(payload)),
                Ok(Frame::Error { id, status }) => calls.answer(id, Err(status)),
                Ok(Frame::Item { id, payload }) => {
                    if calls.item(id, payload).is_err() {
                        return;
                    }
                }
                Ok(Frame::End { id }) => calls.end(id),
                Ok(
                    Frame::Request { .. }
                    | Frame::Subscribe { .. }
                    | Frame::Credit { .. }
                    | Frame::Cancel { .. },
                )
                | Err(_) => return,
            }
        }
    };
    tokio::select! {
        () = read => {}
        () = write_frames(&mut writer, &mut queue, max_frame_bytes) => {}
        () = idle_out(&calls, idle) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_item_beyond_the_granted_credit_is_a_violation() {
        let calls = Calls::new();
        let mut events = calls.subscribe(1, NonZeroU32::MIN).expect("open");
        assert!(calls.item(1, Bytes::from_static(b"a")).is_ok());
        assert!(calls.item(1, Bytes::from_static(b"b")).is_err());
        assert!(calls.grant(1, NonZeroU32::MIN));
        assert!(
            calls.item(2, Bytes::from_static(b"late")).is_ok(),
            "unknown id"
        );
        assert!(matches!(events.try_recv(), Ok(Event::Item(item)) if item == "a"));
    }

    #[test]
    fn a_live_subscription_keeps_the_table_from_idling() {
        let calls = Calls::new();
        let _events = calls.subscribe(1, NonZeroU32::MIN).expect("open");
        assert!(calls.close_if_idle(Duration::ZERO).is_some());
        calls.end(1);
        assert!(!calls.grant(1, NonZeroU32::MIN), "ended");
        assert!(calls.close_if_idle(Duration::ZERO).is_none());
        assert!(calls.subscribe(2, NonZeroU32::MIN).is_none(), "closed");
    }
}
