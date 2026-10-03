//! # groupnet-rpc
//!
//! Request/response calls over the data plane: an [`RpcClient`] multiplexes
//! any number of concurrent calls onto **one** [`DataStream`] per peer, and
//! an [`RpcServer`] answers them with an async handler.
//!
//! ```no_run
//! use std::time::Duration;
//!
//! use bytes::Bytes;
//! use groupnet_core::NodeId;
//! use groupnet_rpc::{RpcClient, RpcConfig, RpcServer, RpcStatus};
//! use groupnet_transport::bulk::{BulkTransport, DataPlane};
//!
//! # async fn demo<B: BulkTransport>(server_plane: DataPlane<B>, client_plane: DataPlane<B>) {
//! // On the serving node: the server owns this plane's `accept`.
//! let server = RpcServer::spawn(server_plane, |from: NodeId, request: Bytes| async move {
//!     if request.is_empty() {
//!         return Err(RpcStatus::new(400, format!("{from} sent nothing")));
//!     }
//!     Ok(request)
//! });
//!
//! // On a calling node: one client, cloned freely.
//! let client = RpcClient::new(client_plane, RpcConfig::default());
//! let reply = client
//!     .call(&NodeId::new("node-b"), Bytes::from_static(b"ping"), Duration::from_secs(1))
//!     .await;
//! # drop((server, reply));
//! # }
//! ```
//!
//! ## Ownership of the data plane
//!
//! The server **owns [`DataPlane::accept`]** of the plane it is given: every
//! inbound stream on it is taken to be an RPC connection. Give RPC its own
//! bulk transport (for TCP, its own `TcpBulkTransport` bound to its own port)
//! unless nothing else on the node accepts bulk streams. A client only
//! connects, so it may share a plane with other connecting users — but the
//! *remote* end of every stream it opens must be an [`RpcServer`].
//!
//! Peer addresses are the transport's business: register every peer a client
//! calls on the client's transport (`TcpBulkTransport::register_peer` /
//! `register_peer_host`), before building the plane or later through a kept
//! clone of it ([`DataPlane::transport`]). An unregistered peer is
//! [`RpcError::Unreachable`]; a re-registered address is dialed by the next
//! call that needs a new connection.
//!
//! ## Wire format
//!
//! Each RPC frame is one data-plane frame (so it inherits the data plane's
//! length prefix); its payload is version byte, kind byte and fixed fields,
//! big-endian:
//!
//! | Kind | Fields after `version = 1`, `kind` |
//! |------|------------------------------------|
//! | `0` Request | `id: u64`, `deadline_ms: u32` (nonzero), payload |
//! | `1` Response | `id: u64`, payload |
//! | `2` Error | `id: u64`, `code: u16`, UTF-8 message |
//!
//! A request's `deadline_ms` is the caller's remaining budget when it was
//! sent; the server measures it from receipt, so no clocks are compared.
//! Malformed bytes, an unknown version or kind, or a frame over the receiver's
//! `max_frame_bytes` drop the connection that carried them — and with it, on
//! the client, every call in flight on it ([`RpcError::ConnectionLost`]).
//!
//! ## Limits
//!
//! * `max_frame_bytes` caps one whole RPC frame (head included) on both
//!   sides; give the client's [`RpcConfig`] and the server's
//!   [`RpcServerConfig`] the same value. A request over the client's limit is
//!   refused before it is sent ([`RpcError::TooLarge`]); a response over the
//!   server's limit is replaced by an [`RpcStatus::RESPONSE_TOO_LARGE`] error.
//! * A client allows `max_in_flight_per_peer` concurrent calls to each peer;
//!   further calls wait for a slot within their own timeout.
//! * A server runs at most `max_concurrent_per_connection` handlers per
//!   connection; past that it stops reading the connection, which pushes
//!   back on the caller through the transport.
//!
//! [`DataPlane::accept`]: groupnet_transport::bulk::DataPlane::accept
//! [`DataPlane::transport`]: groupnet_transport::bulk::DataPlane::transport

mod client;
mod codec;
mod server;

use std::error::Error;
use std::fmt;

use futures_util::io::AsyncWrite;
use groupnet_transport::bulk::DataStream;
use tokio::sync::mpsc;

pub use client::{RpcClient, RpcConfig};
pub use server::{RpcServer, RpcServerConfig, RpcServerHandle};

use codec::Encoded;

/// The largest frame the data plane carries (256 MiB); the ceiling for every
/// `max_frame_bytes`.
pub const MAX_FRAME_LIMIT: usize = 256 << 20;

/// The default `max_frame_bytes` on both sides: 16 MiB.
pub const DEFAULT_MAX_FRAME_BYTES: usize = 16 << 20;

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
    /// The handler panicked.
    pub const HANDLER_PANICKED: u16 = 0xFF02;
    /// The handler's response exceeded the server's `max_frame_bytes`. The
    /// handler did run.
    pub const RESPONSE_TOO_LARGE: u16 = 0xFF03;

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

/// Why an [`RpcClient::call`] produced no response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RpcError {
    /// No connection to the peer could be opened within the connect timeout
    /// (unregistered, refused, or silent). The request was not sent.
    Unreachable,
    /// The call's timeout elapsed, on either side. If the request was sent
    /// the handler may have run; a late response is discarded.
    Timeout,
    /// The connection broke with this request sent and unanswered: the
    /// outcome is **unknown** — the handler may or may not have run. The next
    /// call reconnects.
    ConnectionLost,
    /// The handler (or the server on its behalf) answered with an error.
    Remote(RpcStatus),
    /// The request exceeds the client's `max_frame_bytes`; it was not sent.
    TooLarge,
    /// The client was shut down.
    Shutdown,
}

impl fmt::Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unreachable => f.write_str("rpc peer unreachable"),
            Self::Timeout => f.write_str("rpc call timed out"),
            Self::ConnectionLost => f.write_str("rpc connection lost with the call in flight"),
            Self::Remote(status) => write!(f, "remote error: {status}"),
            Self::TooLarge => f.write_str("rpc request exceeds the frame limit"),
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

/// Checks a configured frame limit.
///
/// # Panics
/// If `max_frame_bytes` cannot hold a request head or exceeds
/// [`MAX_FRAME_LIMIT`].
fn check_frame_limit(max_frame_bytes: usize) {
    assert!(
        (codec::REQUEST_HEAD..=MAX_FRAME_LIMIT).contains(&max_frame_bytes),
        "max_frame_bytes must be within {}..={MAX_FRAME_LIMIT}, got {max_frame_bytes}",
        codec::REQUEST_HEAD
    );
}

/// A connection's single writer: the only code that writes to its stream, so
/// frames never interleave. Returns when every sender is gone or a write
/// fails (the connection is then torn down by its owner).
async fn write_frames<W: AsyncWrite + Unpin>(
    writer: &mut DataStream<W>,
    mut frames: mpsc::Receiver<Encoded>,
    max_frame_bytes: usize,
) {
    while let Some(frame) = frames.recv().await {
        if writer
            .send_bounded_parts(frame.head(), frame.body(), max_frame_bytes)
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
