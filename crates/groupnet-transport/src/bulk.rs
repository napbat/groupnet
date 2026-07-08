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
//! * [`BulkTransport`] — the trait you bind: open/accept reliable streams. The
//!   stream type is `futures-io`'s runtime-agnostic [`AsyncRead`] + [`AsyncWrite`],
//!   so a real `TcpStream` (or QUIC stream) adapts with zero overhead.
//! * [`DataStream`] — length-delimited framing over any such stream, moving
//!   [`Bytes`] with no payload copies and a [`mod@zerocopy`]-parsed frame header.
//! * [`DataPlane`] — a small handle that turns a bound [`BulkTransport`] into
//!   `connect` / `accept` returning framed [`DataStream`]s.

use std::fmt;
use std::future::Future;
use std::io;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures_util::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use groupnet_core::NodeId;
use zerocopy::byteorder::big_endian::U32;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

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

/// A fixed-layout frame header, parsed and emitted with no copies and no
/// `unsafe` via [`mod@zerocopy`]. Length is network byte order so it's stable
/// across machines.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned, Clone, Copy, Debug)]
#[repr(C)]
struct FrameHeader {
    /// Payload length in bytes.
    len: U32,
    /// Frame kind (0 = data); reserved for future framing needs.
    kind: u8,
    /// Padding to a round 8 bytes.
    _reserved: [u8; 3],
}

const HEADER_SIZE: usize = core::mem::size_of::<FrameHeader>();

/// Rejects absurd frame lengths from a corrupt/hostile header (256 MiB cap).
const MAX_FRAME: usize = 256 << 20;

/// Length-delimited message framing over a raw byte stream.
///
/// Wraps any [`AsyncRead`] + [`AsyncWrite`] and moves whole [`Bytes`] payloads:
/// the header is typed (zerocopy), and the payload is read into one buffer and
/// handed out as `Bytes` with no extra copy.
pub struct DataStream<S> {
    inner: S,
}

impl<S> fmt::Debug for DataStream<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataStream").finish_non_exhaustive()
    }
}

impl<S> DataStream<S> {
    /// Wraps a raw stream in the framing protocol.
    pub fn new(inner: S) -> Self {
        Self { inner }
    }

    /// Unwraps back to the raw stream.
    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S: AsyncWrite + Unpin> DataStream<S> {
    /// Writes one framed message. The payload is not copied.
    ///
    /// # Errors
    /// Propagates any write error.
    pub async fn send(&mut self, payload: Bytes) -> io::Result<()> {
        let header = FrameHeader {
            len: U32::new(payload.len() as u32),
            kind: 0,
            _reserved: [0; 3],
        };
        self.inner.write_all(header.as_bytes()).await?;
        self.inner.write_all(&payload).await?;
        self.inner.flush().await?;
        Ok(())
    }
}

impl<S: AsyncRead + Unpin> DataStream<S> {
    /// Reads one framed message, or `None` at a clean end of stream.
    ///
    /// # Errors
    /// Propagates read errors, and rejects a header whose length exceeds the
    /// 256 MiB cap.
    pub async fn recv(&mut self) -> io::Result<Option<Bytes>> {
        let mut header_bytes = [0u8; HEADER_SIZE];
        match self.inner.read_exact(&mut header_bytes).await {
            Ok(()) => {}
            // No more frames: treat an EOF at a frame boundary as clean.
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }

        let header = FrameHeader::read_from_bytes(&header_bytes)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "malformed frame header"))?;
        let len = header.len.get() as usize;
        if len > MAX_FRAME {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "frame exceeds maximum size",
            ));
        }

        let mut payload = BytesMut::zeroed(len);
        self.inner.read_exact(&mut payload).await?;
        Ok(Some(payload.freeze()))
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_round_trip_including_a_large_payload() {
        futures::executor::block_on(async {
            // Write two frames into an in-memory buffer...
            let mut buf: Vec<u8> = Vec::new();
            {
                let mut w = DataStream::new(futures::io::Cursor::new(&mut buf));
                w.send(Bytes::from_static(b"hello")).await.unwrap();
                w.send(Bytes::from(vec![7u8; 1_000_000])).await.unwrap();
            }
            // ...and read them back out.
            let mut r = DataStream::new(futures::io::Cursor::new(&buf[..]));
            assert_eq!(r.recv().await.unwrap().unwrap(), &b"hello"[..]);
            let big = r.recv().await.unwrap().unwrap();
            assert_eq!(big.len(), 1_000_000);
            assert!(big.iter().all(|&b| b == 7));
            assert!(r.recv().await.unwrap().is_none(), "clean EOF");
        });
    }

    #[test]
    fn header_is_eight_bytes() {
        assert_eq!(HEADER_SIZE, 8);
    }
}
