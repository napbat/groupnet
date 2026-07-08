//! Data-plane stream transport (feature `bulk`).
//!
//! Reliable, ordered byte *streams* for the high-throughput traffic a store
//! built on Groupnet needs — replicating writes to replicas, bootstrapping a
//! fresh replica from a snapshot, bulk state transfer on rebalance.
//!
//! This is deliberately separate from the control-plane [`Transport`](crate::Transport)
//! (small, best-effort *datagrams* for gossip). The two planes have opposite
//! requirements and are bound to their own physical connections — gossip over
//! UDP, data over TCP/QUIC.
//!
//! ## What's here
//!
//! * [`BulkTransport`](crate::bulk::BulkTransport) — the trait you bind:
//!   open/accept reliable streams. The stream type is `futures-io`'s
//!   runtime-agnostic [`AsyncRead`](futures_util::io::AsyncRead) +
//!   [`AsyncWrite`](futures_util::io::AsyncWrite), so a real `TcpStream` (or QUIC
//!   stream) adapts with zero overhead.
//! * [`DataStream`](crate::bulk::DataStream) — length-delimited framing over any
//!   such stream, moving [`Bytes`](bytes::Bytes) with no payload copies and a
//!   [`mod@zerocopy`]-parsed frame header.
//! * [`DataPlane`](crate::bulk::DataPlane) — a small handle that turns a bound
//!   [`BulkTransport`](crate::bulk::BulkTransport) into `connect` / `accept`
//!   returning framed [`DataStream`](crate::bulk::DataStream)s.

mod framing;

use std::fmt;
use std::future::Future;
use std::sync::Arc;

use futures_util::io::{AsyncRead, AsyncWrite};
use groupnet_core::NodeId;

pub use framing::DataStream;

/// A reliable, ordered, bidirectional byte-stream transport — the data plane.
///
/// Implement this over TCP, QUIC, or a Unix socket. Unlike the control-plane
/// [`Transport`](crate::Transport), delivery here is reliable and ordered (it's
/// a stream, not a datagram).
pub trait BulkTransport: Send + Sync + 'static {
    /// Transport-specific error type.
    type Error: std::error::Error + Send + Sync + 'static;
    /// The concrete stream type; a real `TcpStream` satisfies this via
    /// `tokio_util::compat`.
    type Stream: AsyncRead + AsyncWrite + Send + Unpin + 'static;

    /// Opens a stream to `to`.
    fn connect(
        &self,
        to: &NodeId,
    ) -> impl Future<Output = Result<Self::Stream, Self::Error>> + Send;

    /// Accepts the next inbound stream, and the id of the peer that opened it.
    fn accept(&self) -> impl Future<Output = Result<(NodeId, Self::Stream), Self::Error>> + Send;
}

/// A handle over a bound [`BulkTransport`] that yields framed [`DataStream`]s.
///
/// This is the data-plane counterpart to the control-plane `Node`. Keep it
/// alongside your `Node`: use the node's routing/membership to decide *which*
/// peer, then `connect` here to move bytes to it.
pub struct DataPlane<B: BulkTransport> {
    transport: Arc<B>,
}

impl<B: BulkTransport> Clone for DataPlane<B> {
    fn clone(&self) -> Self {
        Self {
            transport: self.transport.clone(),
        }
    }
}

impl<B: BulkTransport> fmt::Debug for DataPlane<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataPlane").finish_non_exhaustive()
    }
}

impl<B: BulkTransport> DataPlane<B> {
    /// Wraps a bound bulk transport.
    pub fn new(transport: B) -> Self {
        Self {
            transport: Arc::new(transport),
        }
    }

    /// Opens a framed stream to `to`.
    ///
    /// # Errors
    /// Propagates the transport's connect error.
    pub async fn connect(&self, to: &NodeId) -> Result<DataStream<B::Stream>, B::Error> {
        Ok(DataStream::new(self.transport.connect(to).await?))
    }

    /// Accepts the next inbound framed stream and the peer that opened it.
    ///
    /// # Errors
    /// Propagates the transport's accept error.
    pub async fn accept(&self) -> Result<(NodeId, DataStream<B::Stream>), B::Error> {
        let (from, stream) = self.transport.accept().await?;
        Ok((from, DataStream::new(stream)))
    }
}
