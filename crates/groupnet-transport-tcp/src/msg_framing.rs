//! Native message framing: the shared typed length prefix and a separately
//! owned payload. Writers gather queued frames into vectored writes; readers
//! split several frames out of one bounded read-ahead buffer, so a busy socket
//! costs one system call per batch instead of one or more per frame.

use std::io;

use bytes::{Buf, Bytes, BytesMut};
use groupnet_transport::framing::{LengthHeader, write_vectored};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use tokio::sync::mpsc;
use zerocopy::{FromBytes, FromZeros, IntoBytes};

/// Most queued frames gathered into one vectored write.
pub(super) const WRITE_BATCH: usize = 32;

/// Read-ahead each connection's reader requests per system call. Frames are
/// split from this storage without copying, so a retained frame keeps at most
/// one such allocation (or its own exact allocation when it is larger) alive.
const READ_AHEAD: usize = 64 * 1024;

/// Writes a header and body without allocating a concatenated frame.
#[cfg(test)]
pub(super) async fn write_frame(
    socket: &mut (impl AsyncWrite + Unpin),
    payload: &[u8],
) -> io::Result<()> {
    let header = LengthHeader::new(payload.len())?;
    write_vectored(socket, &[header.as_bytes(), payload]).await
}

/// Writes every frame in order with as few vectored writes as the shared
/// writer allows, without concatenating payloads.
pub(super) async fn write_frames(
    socket: &mut (impl AsyncWrite + Unpin),
    frames: &[Bytes],
) -> io::Result<()> {
    let mut headers = [LengthHeader::new_zeroed(); WRITE_BATCH];
    for batch in frames.chunks(WRITE_BATCH) {
        for (header, frame) in headers.iter_mut().zip(batch) {
            *header = LengthHeader::new(frame.len())?;
        }
        let mut parts: [&[u8]; 2 * WRITE_BATCH] = [&[]; 2 * WRITE_BATCH];
        let (pairs, _) = parts.as_chunks_mut::<2>();
        for ((pair, header), frame) in pairs.iter_mut().zip(&headers).zip(batch) {
            *pair = [header.as_bytes(), frame];
        }
        write_vectored(socket, &parts[..2 * batch.len()]).await?;
    }
    Ok(())
}

/// Waits for one queued frame, then takes whatever else is already queued, up
/// to [`WRITE_BATCH`]. A burst (two or more frames already waiting) yields once
/// first so producers scheduled behind this writer can extend the same system
/// call; a lone frame, such as a request or reply, is written without delay.
/// Returns `false` once the queue is closed and drained.
pub(super) async fn next_batch(queue: &mut mpsc::Receiver<Bytes>, batch: &mut Vec<Bytes>) -> bool {
    batch.clear();
    if queue.recv_many(batch, WRITE_BATCH).await == 0 {
        return false;
    }
    if (2..WRITE_BATCH).contains(&batch.len()) {
        tokio::task::yield_now().await;
        extend_ready(queue, batch);
    }
    true
}

/// Appends frames that are already queued until the batch holds
/// [`WRITE_BATCH`]. Keeps frames a cancelled [`next_batch`] already took.
pub(super) fn extend_ready(queue: &mut mpsc::Receiver<Bytes>, batch: &mut Vec<Bytes>) {
    while batch.len() < WRITE_BATCH {
        let Ok(frame) = queue.try_recv() else {
            break;
        };
        batch.push(frame);
    }
}

/// Splits length-prefixed frames out of a bounded read-ahead buffer.
#[derive(Debug)]
pub(super) struct FrameReader {
    buffer: BytesMut,
    max_frame_bytes: usize,
}

impl FrameReader {
    pub(super) fn new(max_frame_bytes: usize) -> Self {
        Self {
            buffer: BytesMut::new(),
            max_frame_bytes,
        }
    }

    /// The next frame. A clean close is only valid between frames. Oversized
    /// lengths are rejected before any storage for their payload is reserved.
    /// Cancel-safe: buffered bytes stay for the next call.
    pub(super) async fn next(
        &mut self,
        socket: &mut (impl AsyncRead + Unpin),
    ) -> io::Result<Option<Bytes>> {
        loop {
            let wanted = match LengthHeader::read_from_prefix(&self.buffer) {
                Ok((header, _)) => {
                    let frame = LengthHeader::SIZE + header.length_within(self.max_frame_bytes)?;
                    if self.buffer.len() >= frame {
                        let mut frame = self.buffer.split_to(frame);
                        frame.advance(LengthHeader::SIZE);
                        return Ok(Some(frame.freeze()));
                    }
                    frame - self.buffer.len()
                }
                Err(_) => LengthHeader::SIZE - self.buffer.len(),
            };
            self.buffer.reserve(wanted.max(READ_AHEAD));
            if socket.read_buf(&mut self.buffer).await? == 0 {
                return if self.buffer.is_empty() {
                    Ok(None)
                } else {
                    Err(io::ErrorKind::UnexpectedEof.into())
                };
            }
        }
    }
}

#[cfg(test)]
#[path = "msg_framing_tests.rs"]
mod tests;
