//! Length-delimited framing for the data plane.
//!
//! The data-plane counterpart to the control plane's
//! [`wire`](groupnet_core::wire) codec: it turns a raw, reliable byte stream
//! into a sequence of discrete messages. Each frame is a fixed [`FrameHeader`] —
//! typed and copy-free via [`mod@zerocopy`] — followed by its payload, handed
//! out as [`Bytes`](bytes::Bytes) with no extra copy.

use std::fmt;
use std::io;

use bytes::Bytes;
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
    /// Frame kind; only [`FRAME_KIND_DATA`] today. Any other kind, like any
    /// nonzero reserved byte, is rejected as malformed.
    kind: u8,
    /// Padding to a round 8 bytes.
    reserved: [u8; 3],
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
/// handed out as `Bytes` with no extra copy. Cancelling a send or receive after
/// it starts may leave a partial frame; the caller must discard that stream.
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
        self.send_bounded(payload, MAX_FRAME).await
    }

    /// Sends a frame only if it fits the caller's finite operation budget.
    /// The limit is checked before any header or payload bytes are written.
    ///
    /// # Errors
    /// Rejects invalid limits or oversized payloads and propagates write errors.
    pub async fn send_bounded(&mut self, payload: Bytes, max_bytes: usize) -> io::Result<()> {
        self.send_bounded_ref(&payload, max_bytes).await
    }

    /// Sends from borrowed admitted storage without making a second payload
    /// allocation. The caller must retain that storage and its charge through
    /// this entire write. A cancelled write leaves the stream unusable.
    ///
    /// # Errors
    /// Rejects invalid limits or oversized payloads and propagates write errors.
    pub async fn send_bounded_ref(&mut self, payload: &[u8], max_bytes: usize) -> io::Result<()> {
        self.send_bounded_parts(&[], payload, max_bytes).await
    }

    /// Sends one frame whose payload is `head` followed by `body`, written
    /// from the two buffers without joining them — so a protocol header in
    /// front of a large body costs no copy. The receiver sees one ordinary
    /// frame. The limit applies to the combined length and is checked before
    /// anything is written. A cancelled write leaves the stream unusable.
    ///
    /// # Errors
    /// Rejects invalid limits or oversized payloads and propagates write errors.
    pub async fn send_bounded_parts(
        &mut self,
        head: &[u8],
        body: &[u8],
        max_bytes: usize,
    ) -> io::Result<()> {
        let total = head.len().checked_add(body.len());
        if max_bytes == 0 || max_bytes > MAX_FRAME || total.is_none_or(|total| total > max_bytes) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "frame exceeds caller's bounded send limit",
            ));
        }
        let len = total
            .and_then(|total| u32::try_from(total).ok())
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "frame length exceeds u32")
            })?;
        let header = FrameHeader {
            len: U32::new(len),
            kind: FRAME_KIND_DATA,
            reserved: [0; 3],
        };
        self.inner.write_all(header.as_bytes()).await?;
        self.inner.write_all(head).await?;
        self.inner.write_all(body).await?;
        self.inner.flush().await?;
        Ok(())
    }

    /// Requests write-half shutdown after a complete protocol terminator.
    /// Bindings used by the bootstrap exchange must preserve the read half
    /// until the peer's reply; a cancelled or failed close requires
    /// discarding this stream.
    ///
    /// # Errors
    /// Propagates the underlying stream's close error.
    pub async fn finish_write(&mut self) -> io::Result<()> {
        self.inner.close().await
    }
}

impl<S: AsyncRead + Unpin> DataStream<S> {
    /// Reads one framed message, or `None` at a clean end of stream.
    ///
    /// # Errors
    /// Propagates read errors, and rejects a header whose length exceeds the
    /// 256 MiB cap.
    pub async fn recv(&mut self) -> io::Result<Option<Bytes>> {
        self.recv_bounded(MAX_FRAME).await
    }

    /// Reads a frame only if its header fits the caller's operation budget.
    /// The length is rejected before allocating payload storage. A partial
    /// header is an error; only EOF at a frame boundary returns `None`.
    ///
    /// # Errors
    /// Rejects zero/oversized limits, malformed or excessive frames, and
    /// propagates read errors.
    pub async fn recv_bounded(&mut self, max_bytes: usize) -> io::Result<Option<Bytes>> {
        if max_bytes == 0 || max_bytes > MAX_FRAME {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid bounded receive limit",
            ));
        }
        let mut header_bytes = [0u8; HEADER_SIZE];
        // A one-byte read separates a clean boundary EOF from truncation.
        if self.inner.read(&mut header_bytes[..1]).await? == 0 {
            return Ok(None);
        }
        self.inner.read_exact(&mut header_bytes[1..]).await?;

        let header = FrameHeader::read_from_bytes(&header_bytes)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "malformed frame header"))?;
        if header.kind != FRAME_KIND_DATA || header.reserved != [0; 3] {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported frame header",
            ));
        }
        let len = usize::try_from(header.len.get()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "frame length is unrepresentable",
            )
        })?;
        if len > max_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "frame exceeds caller's bounded receive limit",
            ));
        }

        let mut payload = Vec::new();
        payload
            .try_reserve_exact(len)
            .map_err(|_| io::Error::new(io::ErrorKind::OutOfMemory, "frame allocation failed"))?;
        payload.resize(len, 0);
        self.inner.read_exact(&mut payload).await?;
        Ok(Some(Bytes::from(payload)))
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

    #[test]
    fn bounded_receive_rejects_header_before_payload_allocation() {
        futures::executor::block_on(async {
            let header = FrameHeader {
                len: U32::new(10_000),
                kind: FRAME_KIND_DATA,
                reserved: [0; 3],
            };
            // There is deliberately no payload; a reader that tried to read
            // it would report truncation instead of the declared cap.
            let mut stream = DataStream::new(futures::io::Cursor::new(header.as_bytes()));
            let error = stream.recv_bounded(16).await.unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        });
    }

    #[test]
    fn partial_header_is_not_a_clean_end_of_stream() {
        futures::executor::block_on(async {
            let mut partial = DataStream::new(futures::io::Cursor::new(&[1u8, 2, 3][..]));
            assert_eq!(
                partial.recv_bounded(16).await.unwrap_err().kind(),
                io::ErrorKind::UnexpectedEof
            );
            let mut clean = DataStream::new(futures::io::Cursor::new(&[][..]));
            assert!(clean.recv_bounded(16).await.unwrap().is_none());
        });
    }

    #[test]
    fn unsupported_header_is_rejected_before_body() {
        futures::executor::block_on(async {
            for (kind, reserved) in [(7, [0; 3]), (FRAME_KIND_DATA, [1, 0, 0])] {
                let header = FrameHeader {
                    len: U32::new(1),
                    kind,
                    reserved,
                };
                let mut stream = DataStream::new(futures::io::Cursor::new(header.as_bytes()));
                assert_eq!(
                    stream.recv_bounded(16).await.unwrap_err().kind(),
                    io::ErrorKind::InvalidData
                );
            }
        });
    }

    #[test]
    fn declared_payload_truncation_is_an_error() {
        futures::executor::block_on(async {
            let header = FrameHeader {
                len: U32::new(3),
                kind: FRAME_KIND_DATA,
                reserved: [0; 3],
            };
            let mut truncated = header.as_bytes().to_vec();
            truncated.extend_from_slice(b"ab");
            let mut stream = DataStream::new(futures::io::Cursor::new(truncated));
            assert_eq!(
                stream.recv_bounded(3).await.unwrap_err().kind(),
                io::ErrorKind::UnexpectedEof
            );
        });
    }

    #[test]
    fn bounded_send_refuses_without_writing_and_round_trips_at_limit() {
        futures::executor::block_on(async {
            let mut bytes = Vec::new();
            {
                let mut stream = DataStream::new(futures::io::Cursor::new(&mut bytes));
                assert_eq!(
                    stream
                        .send_bounded(Bytes::from_static(b"five!"), 4)
                        .await
                        .unwrap_err()
                        .kind(),
                    io::ErrorKind::InvalidInput
                );
                stream.send_bounded_ref(b"five!", 5).await.unwrap();
            }
            assert_eq!(bytes.len(), HEADER_SIZE + 5);
            let mut reader = DataStream::new(futures::io::Cursor::new(bytes));
            assert_eq!(
                reader.recv_bounded(5).await.unwrap().unwrap(),
                &b"five!"[..]
            );
        });
    }

    #[test]
    fn a_two_part_frame_is_one_frame_bounded_by_its_total() {
        futures::executor::block_on(async {
            let mut bytes = Vec::new();
            {
                let mut stream = DataStream::new(futures::io::Cursor::new(&mut bytes));
                assert_eq!(
                    stream
                        .send_bounded_parts(b"head", b"body", 7)
                        .await
                        .unwrap_err()
                        .kind(),
                    io::ErrorKind::InvalidInput
                );
                stream
                    .send_bounded_parts(b"head", b"body", 8)
                    .await
                    .unwrap();
                stream.send_bounded_parts(b"", b"", 1).await.unwrap();
            }
            assert_eq!(bytes.len(), 2 * HEADER_SIZE + 8);
            let mut reader = DataStream::new(futures::io::Cursor::new(bytes));
            assert_eq!(
                reader.recv_bounded(8).await.unwrap().unwrap(),
                &b"headbody"[..]
            );
            assert!(reader.recv_bounded(8).await.unwrap().unwrap().is_empty());
            assert!(reader.recv_bounded(8).await.unwrap().is_none());
        });
    }
}
