//! # groupnet-rpc
//!
//! Request/response calls and server-push subscriptions over the data
//! plane: an [`RpcClient`] multiplexes any number of concurrent calls and
//! subscriptions onto **one** [`DataStream`] per peer, and an [`RpcServer`]
//! answers them with async handlers.
//!
//! ## Ownership of the data plane
//!
//! RPC composes with the other stream consumers of a node; it does not need
//! a bulk listener of its own. There are two ways to serve:
//!
//! * **Plane-owning** — [`RpcServer::bind`]`(plane, handler)` (or
//!   [`bind_with`](RpcServer::bind_with)) runs the accept loop itself, on top
//!   of the connection-level API: every inbound stream on that plane is an
//!   RPC connection, and a full server stops accepting until one ends. Use it
//!   for a plane that carries nothing but RPC: on a runtime `Node`, an
//!   ordered endpoint scoped to RPC's own stream protocol (below); for TCP,
//!   its own `TcpBulkTransport` bound to its own port.
//! * **Connection-level** — the application owns [`DataPlane::accept`] and
//!   hands each stream that is meant for RPC to
//!   [`RpcServer::serve`]`(from, stream)`. Every connection handed over is
//!   served under the same server limits; at `max_connections` `serve`
//!   refuses it with [`RpcServeError::AtCapacity`] (the stream is dropped),
//!   while [`RpcServer::reserve`] waits for a slot instead. Use it when the
//!   application itself decides which streams are RPC.
//!
//! On a runtime `Node`, every stream consumer binds its own ordered stream
//! protocol — `node.endpoint(Ordered::protocol(id))`, which is itself a
//! [`BulkTransport`] — and runs its own independent accept loop. The node
//! routes each inbound stream to the accept queue of the protocol its
//! connector named in the authenticated stream setup, so the RPC server, an
//! RPC client and any other consumer (blob transfer, say) share the node's
//! tunnels without a shared dispatcher, a tag byte, or ever receiving each
//! other's streams. Every node must agree on what each protocol id means.
//!
//! This crate supplies no managed protocol binding of its own, and accept
//! loops compete only within one scope: consumers that accept on the same
//! `StreamProtocolId` (two `node.endpoint(Ordered::protocol(id))` handles with
//! the same `id`, including `Ordered::new()`'s `StreamProtocolId::DEFAULT`, or
//! an RPC [`bind`](RpcServer::bind) beside another loop on RPC's protocol)
//! share one inbound queue and race for its streams. Give each consumer its
//! own protocol id, or make one application dispatcher the only accept loop of
//! a shared scope and hand RPC's streams to [`RpcServer::serve`]. Likewise, a
//! plain plane such as one `TcpBulkTransport` has a single inbound scope.
//!
//! ```no_run
//! use std::time::Duration;
//!
//! use groupnet_core::NodeId;
//! use groupnet_rpc::{Bytes, DataPlane, DataStream, RpcClient, RpcServer, RpcStatus};
//! use groupnet_runtime::{Node, Ordered, StreamProtocolId};
//!
//! /// The stream protocols every node in the cluster agrees on.
//! const RPC: StreamProtocolId = StreamProtocolId::new(1);
//! const BLOBS: StreamProtocolId = StreamProtocolId::new(2);
//!
//! # async fn blobs<S>(from: NodeId, stream: DataStream<S>) {}
//! # async fn demo(node: Node) -> std::io::Result<()> {
//! // Serving side: RPC owns the accept loop of its own protocol...
//! let server = RpcServer::bind(
//!     DataPlane::new(node.endpoint(Ordered::protocol(RPC))?),
//!     |from: NodeId, request: Bytes| async move {
//!         if request.is_empty() {
//!             return Err(RpcStatus::new(400, format!("{from} sent nothing")));
//!         }
//!         Ok(request)
//!     },
//! );
//! // ...while the blob consumer runs an independent one beside it.
//! let blob_plane = DataPlane::new(node.endpoint(Ordered::protocol(BLOBS))?);
//! tokio::spawn(async move {
//!     while let Ok((from, stream)) = blob_plane.accept().await {
//!         tokio::spawn(blobs(from, stream));
//!     }
//! });
//!
//! // Calling side: one client, cloned freely, dialing RPC's protocol.
//! let client = RpcClient::new(DataPlane::new(node.endpoint(Ordered::protocol(RPC))?));
//! let reply = client
//!     .call(&NodeId::new("node-b"), Bytes::from_static(b"ping"), Duration::from_secs(1))
//!     .await;
//! # drop((server, reply));
//! # Ok(())
//! # }
//! ```
//!
//! Peer addresses are the transport's business: register every peer a client
//! calls on the client's transport (`TcpBulkTransport::register_peer` /
//! `register_peer_host`), before building the plane or later through a kept
//! clone of it ([`DataPlane::transport`]). An unregistered peer is
//! [`RpcError::Unreachable`]; a re-registered address is dialed by the next
//! call that needs a new connection.
//!
//! A process that only calls (a client of a cluster, not a member) builds
//! its plane on a transport that does not listen, for TCP
//! `TcpBulkTransport::dial_only`, and runs no server: every response — and
//! every subscription item — comes back on the stream that the client
//! opened.
//!
//! ## Subscriptions (server push)
//!
//! A call answers once; a subscription answers many times. The client opens
//! one with [`RpcClient::subscribe`] and reads it with
//! [`Subscription::next`]; a server built with
//! [`RpcServer::with_subscriptions`] runs its subscribe handler with a
//! [`SubscriptionSink`] and sends items until the handler returns (`Ok(())`
//! ends it, `Err(status)` ends it with that error) or the client drops the
//! [`Subscription`], which cancels it and drops the handler. Subscriptions
//! share the connection, and its single writer, with calls.
//!
//! Flow control is per subscription: the server may send
//! [`RpcConfig::subscription_window`] items ahead of what the application
//! consumed, and [`SubscriptionSink::send`] waits for credit beyond that. The
//! client grants credit as [`next`](Subscription::next) consumes items, so a
//! subscription nobody reads holds back only itself — never the
//! connection's calls or its other subscriptions.
//!
//! ```
//! use std::time::Duration;
//!
//! use groupnet_core::NodeId;
//! use groupnet_rpc::{Bytes, DataPlane, RpcClient, RpcError, RpcServer, RpcStatus, SubscriptionSink};
//! use groupnet_transport_mem::MemBulkNet;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), RpcError> {
//! let net = MemBulkNet::new();
//! // The hub answers calls and counts down from the subscribe's first byte.
//! let _hub = RpcServer::with_subscriptions(
//!     |_from: NodeId, request: Bytes| async move { Ok::<_, RpcStatus>(request) },
//!     |_from: NodeId, start: Bytes, mut sink: SubscriptionSink| async move {
//!         let start = start.first().copied().unwrap_or(0);
//!         for n in (1..=start).rev() {
//!             // Waits for credit; fails once the client cancels or leaves.
//!             if sink.send(Bytes::from(vec![n])).await.is_err() {
//!                 break;
//!             }
//!         }
//!         Ok::<(), RpcStatus>(())
//!     },
//! )
//! .accept_on(DataPlane::new(net.endpoint(NodeId::new("hub"))));
//!
//! let client = RpcClient::new(DataPlane::new(net.endpoint(NodeId::new("client"))));
//! let mut countdown = client
//!     .subscribe(&NodeId::new("hub"), Bytes::from_static(&[3]), Duration::from_secs(1))
//!     .await?;
//! let mut seen = Vec::new();
//! while let Some(item) = countdown.next().await {
//!     seen.push(item?[0]);
//! }
//! assert_eq!(seen, [3, 2, 1]);
//! # Ok(())
//! # }
//! ```
//!
//! ## Wire format
//!
//! Each RPC frame is one data-plane frame (so it inherits the data plane's
//! length prefix); its payload is a typed big-endian head — version byte,
//! kind byte and fixed fields — followed by the body:
//!
//! | Kind | Direction | Fields after `version = 1`, `kind` |
//! |------|-----------|------------------------------------|
//! | `0` Request | client → server | `id: u64`, `deadline_ms: u32` (nonzero), payload |
//! | `1` Response | server → client | `id: u64`, payload |
//! | `2` Error | server → client | `id: u64`, `code: u16`, UTF-8 message |
//! | `3` Subscribe | client → server | `id: u64`, `window: u32` (nonzero), payload |
//! | `4` Item | server → client | `id: u64`, payload |
//! | `5` End | server → client | `id: u64` |
//! | `6` Credit | client → server | `id: u64`, `grant: u32` (nonzero) |
//! | `7` Cancel | client → server | `id: u64` |
//!
//! A request's `deadline_ms` is the caller's remaining budget when it was
//! sent, rounded up to whole milliseconds; the server measures it from
//! receipt, so no clocks are compared. Calls and subscriptions share one id
//! space per connection; an Error also ends a subscription. A server may
//! send `window` items of a subscription before its first Credit, and
//! `grant` more per Credit.
//! Malformed bytes (a short head, a zero deadline, window or grant, bytes
//! after an End, Credit or Cancel), an unknown version or kind, a frame only
//! the other side sends, a frame over the receiver's `max_frame_bytes`, an
//! item beyond its credit, credit beyond its window, or a Subscribe reusing
//! a live subscription's id drop the connection that carried them — and with
//! it, on the client, every call in flight and every subscription open on it
//! ([`RpcError::ConnectionLost`]).
//!
//! ## Limits
//!
//! Limits are typed: frame bounds are a [`FrameLimit`] (built by
//! [`FrameLimit::new`], failing with a [`FrameLimitError`]), queue and
//! admission bounds a [`QueueCapacity`], and the timers are checked once,
//! when the client or server is built ([`RpcConfig::validate`] →
//! [`RpcConfigError`], [`RpcServerConfig::validate`] →
//! [`RpcServerConfigError`], each variant naming its field). Nothing is
//! clamped; an invalid value is refused.
//!
//! * `max_frame_bytes` caps one whole RPC frame (head included) on both
//!   sides, at most the data plane's
//!   [`MAX_FRAME_BYTES`](groupnet_transport::framing::MAX_FRAME_BYTES); give
//!   the client's [`RpcConfig`] and the server's [`RpcServerConfig`] the same
//!   value. A request or subscribe over the client's limit is refused before
//!   it is sent ([`RpcError::TooLarge`]); a response over the server's limit
//!   is replaced by an [`RpcStatus::RESPONSE_TOO_LARGE`] error, and an item
//!   over it ends its subscription with that error
//!   ([`SinkError::TooLarge`] to the handler).
//! * A client allows `max_in_flight_per_peer` concurrent calls to each peer;
//!   further calls wait for a slot within their own timeout.
//! * A client tracks at most `max_peers` destinations. A call to a new one
//!   at the limit evicts a destination no call or open subscription is using
//!   (closing its connection), or fails [`RpcError::Saturated`] if every
//!   destination is busy.
//! * A client keeps at most `max_subscriptions_per_peer` subscriptions open
//!   to each peer; one more fails [`RpcError::TooManySubscriptions`], unsent.
//!   Each buffers at most `subscription_window` items.
//! * A server serves at most `max_connections` connections, however they
//!   reach it: at the limit [`RpcServer::serve`] refuses
//!   ([`RpcServeError::AtCapacity`]), [`RpcServer::reserve`] waits, and a
//!   [`bind`](RpcServer::bind) accept loop stops accepting until one ends.
//! * A server runs at most `max_concurrent_per_connection` call handlers per
//!   connection; past that it stops reading the connection, which pushes
//!   back on the caller through the transport.
//! * A server runs at most `max_subscriptions_per_connection` subscriptions
//!   per connection, apart from the call handlers; one more is answered
//!   [`RpcStatus::TOO_MANY_SUBSCRIPTIONS`] (and every subscribe to a server
//!   without a subscribe handler [`RpcStatus::SUBSCRIPTIONS_UNSUPPORTED`]).
//! * Idle connections are closed: by the client after its `idle_timeout`
//!   with no call waiting and no subscription open, by the server after its
//!   `idle_timeout` with no frame read and no handler or subscription
//!   running. Keep the server's longer than the
//!   clients', so the side that knows nothing is in flight closes first.
//!   Configured timers are bounded by [`MAX_TIMEOUT`], the wire deadline's
//!   `u32` milliseconds; a call's timeout saturates to it — the one
//!   permitted correction, unobservable at ~49.7 days.

mod client;
mod codec;
mod config;
mod server;

use std::error::Error;
use std::fmt;
use std::sync::{Mutex, MutexGuard, PoisonError};

use futures_util::io::AsyncWrite;
use tokio::sync::mpsc;

pub use bytes::Bytes;
pub use client::{RpcClient, Subscription};
pub use config::{
    FrameLimit, FrameLimitError, MAX_TIMEOUT, RpcConfig, RpcConfigError, RpcServerConfig,
    RpcServerConfigError,
};
pub use groupnet_transport::QueueCapacity;
pub use groupnet_transport::bulk::{BulkTransport, DataPlane, DataStream};
pub use server::{
    NoSubscriptions, RpcServeError, RpcServer, RpcServerHandle, RpcSlot, SinkError,
    SubscribeHandler, SubscriptionSink,
};

use codec::Encoded;

/// Locks a mutex whose data stays consistent across a panic: every critical
/// section here is a single map, flag or task-set update.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A failure a handler (or the server on its behalf) reports to the caller.
///
/// Codes `0xFF00..=0xFFFF` are reserved for this crate (the associated
/// constants); a handler is free to use any other code.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RpcStatus {
    /// Machine-readable reason.
    pub code: u16,
    /// Human-readable detail.
    pub message: String,
}

impl RpcStatus {
    /// The request's deadline passed before the handler answered; the server
    /// dropped the work. A client reports this as [`RpcError::Timeout`].
    pub const DEADLINE_EXCEEDED: u16 = 0xFF01;
    /// The call or subscribe handler panicked.
    pub const HANDLER_PANICKED: u16 = 0xFF02;
    /// The handler's response, or one subscription item, exceeded the
    /// server's `max_frame_bytes`. The handler did run; a subscription ends
    /// with this error at the oversized item, after the items before it.
    pub const RESPONSE_TOO_LARGE: u16 = 0xFF03;
    /// The server takes no subscriptions: it was built without a subscribe
    /// handler ([`RpcServer::new`] rather than
    /// [`RpcServer::with_subscriptions`]).
    pub const SUBSCRIPTIONS_UNSUPPORTED: u16 = 0xFF04;
    /// The connection already carries the server's
    /// [`max_subscriptions_per_connection`](RpcServerConfig::max_subscriptions_per_connection)
    /// live subscriptions; the subscribe handler did not run.
    pub const TOO_MANY_SUBSCRIPTIONS: u16 = 0xFF05;

    /// A status with `code` and `message`.
    #[must_use]
    pub fn new(code: u16, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    /// Cuts the message (at a character boundary) to at most `max` bytes, so
    /// the error frame fits the server's frame limit.
    fn truncated(mut self, max: usize) -> Self {
        if self.message.len() > max {
            let mut end = max;
            while !self.message.is_char_boundary(end) {
                end -= 1;
            }
            self.message.truncate(end);
        }
        self
    }
}

impl fmt::Display for RpcStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "rpc status {}: {}", self.code, self.message)
    }
}

impl Error for RpcStatus {}

/// Why an [`RpcClient::call`] produced no response, an
/// [`RpcClient::subscribe`] did not open, or a [`Subscription`] ended early.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RpcError {
    /// No connection to the peer could be opened within the connect timeout
    /// (unregistered, refused, or silent). The request was not sent.
    Unreachable,
    /// The timeout ended before the request (or subscribe) was sent: while
    /// it waited for a call slot of the peer, for another call's dial, for
    /// the connect, or for room in the connection's queue. The handler did
    /// not run.
    NotSent,
    /// The call's timeout elapsed with the request sent, on either side. The
    /// handler may have run; a late response is discarded.
    Timeout,
    /// The connection broke with this request sent and unanswered (or this
    /// subscription open): the outcome is **unknown** — the handler may or
    /// may not have run. The next call or subscribe reconnects.
    ConnectionLost,
    /// The handler (or the server on its behalf) answered with an error.
    Remote(RpcStatus),
    /// The request exceeds the client's `max_frame_bytes`; it was not sent.
    TooLarge,
    /// The client already tracks `max_peers` destinations, each with a call
    /// or subscription in progress, so a new destination cannot be admitted;
    /// the request was not sent.
    Saturated,
    /// The client already holds
    /// [`max_subscriptions_per_peer`](RpcConfig::max_subscriptions_per_peer)
    /// open subscriptions to this peer; the subscribe was not sent. Dropping
    /// a [`Subscription`] (or reading it to its end) frees its slot.
    TooManySubscriptions,
    /// The client was shut down.
    Shutdown,
}

impl fmt::Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unreachable => f.write_str("rpc peer unreachable"),
            Self::NotSent => f.write_str("rpc call timed out before the request was sent"),
            Self::Timeout => f.write_str("rpc call timed out"),
            Self::ConnectionLost => f.write_str("rpc connection lost with the call in flight"),
            Self::Remote(status) => write!(f, "remote error: {status}"),
            Self::TooLarge => f.write_str("rpc request exceeds the frame limit"),
            Self::Saturated => f.write_str("rpc client destination limit reached"),
            Self::TooManySubscriptions => {
                f.write_str("rpc client subscription limit for this peer reached")
            }
            Self::Shutdown => f.write_str("rpc client shut down"),
        }
    }
}

impl Error for RpcError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Remote(status) => Some(status),
            _ => None,
        }
    }
}

/// A connection's single writer: the only code that writes to its stream, so
/// frames never interleave. Returns when every sender is gone or a write
/// fails (the connection is then torn down by its owner).
async fn write_frames<W: AsyncWrite + Unpin>(
    writer: &mut DataStream<W>,
    frames: &mut mpsc::Receiver<Encoded>,
    max_frame_bytes: FrameLimit,
) {
    while let Some(frame) = frames.recv().await {
        if writer
            .send_bounded_parts(frame.head(), frame.body(), max_frame_bytes.frame_bytes())
            .await
            .is_err()
        {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_truncated_status_message_ends_on_a_character_boundary() {
        let status = RpcStatus::new(1, "ab\u{e9}cd");
        assert_eq!(status.clone().truncated(3).message, "ab");
        assert_eq!(status.clone().truncated(4).message, "ab\u{e9}");
        assert_eq!(status.clone().truncated(64).message, "ab\u{e9}cd");
        assert_eq!(status.truncated(0).message, "");
    }
}
