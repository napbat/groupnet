//! Length-delimited framing for the data plane.
//!
//! The data-plane counterpart to the control plane's
//! [`wire`](groupnet_core::wire) codec: it turns a raw, reliable byte stream
//! into a sequence of discrete messages. Each frame is a fixed [`FrameHeader`] —
//! typed and copy-free via [`mod@zerocopy`] — followed by its payload, handed
//! out as [`Bytes`](bytes::Bytes) with no extra copy.

use std::fmt;
use std::io;

use bytes::{Bytes, BytesMut};
use futures_util::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use zerocopy::byteorder::big_endian::U32;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

/// A fixed-layout frame header, parsed and emitted with no copies and no
/// `unsafe` via [`mod@zerocopy`]. Length is network byte order so it's stable
/// across machines.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned, Clone, Copy, Debug)]
#[repr(C)]
struct FrameHeader {
    /// Payload length in bytes.
    len: U32,
    /// Frame kind; only [`FRAME_KIND_DATA`] today, reserved so future framing
    /// needs (control frames, end-of-stream markers) can be added compatibly.
    kind: u8,
    /// Padding to a round 8 bytes.
    _reserved: [u8; 3],
}

const HEADER_SIZE: usize = core::mem::size_of::<FrameHeader>();

/// The only [`FrameHeader::kind`] so far: an application data frame.
const FRAME_KIND_DATA: u8 = 0;

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
    #[expect(
        clippy::cast_possible_truncation,
        reason = "the header's length field is a u32 by definition of the framing, and \
                  a peer refuses anything past the 256 MiB `MAX_FRAME` cap — four \
                  orders of magnitude below where a length could truncate"
    )]
    pub async fn send(&mut self, payload: Bytes) -> io::Result<()> {
        let header = FrameHeader {
            len: U32::new(payload.len() as u32),
            kind: FRAME_KIND_DATA,
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
