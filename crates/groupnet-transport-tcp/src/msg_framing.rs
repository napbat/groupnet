//! Native message framing: network-endian header and separately owned payload.

use std::io::{self, IoSlice};

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use zerocopy::byteorder::big_endian::U32;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned, Clone, Copy, Debug)]
#[repr(C)]
struct FrameHeader {
    len: U32,
}

/// Writes a header and body without allocating a concatenated frame. Partial
/// vectored writes may end inside either buffer and must resume at that byte.
pub(super) async fn write_frame(
    socket: &mut (impl AsyncWrite + Unpin),
    payload: &[u8],
) -> io::Result<()> {
    let len = u32::try_from(payload.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "frame length exceeds u32"))?;
    let header = FrameHeader { len: U32::new(len) };
    let mut head = header.as_bytes();
    let mut body = payload;
    if !socket.is_write_vectored() {
        socket.write_all(head).await?;
        return socket.write_all(body).await;
    }
    while !head.is_empty() {
        let slices = [IoSlice::new(head), IoSlice::new(body)];
        let written = socket.write_vectored(&slices).await?;
        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "TCP frame write stalled",
            ));
        }
        let head_written = written.min(head.len());
        head = &head[head_written..];
        body = &body[written - head_written..];
    }
    socket.write_all(body).await
}

/// A clean close is only valid before the first header byte. Reject partial
/// headers and oversized lengths before allocating payload storage.
pub(super) async fn read_frame(
    socket: &mut (impl AsyncRead + Unpin),
    max_frame_bytes: usize,
) -> io::Result<Option<Bytes>> {
    let mut header = FrameHeader { len: U32::new(0) };
    let bytes = header.as_mut_bytes();
    if socket.read(&mut bytes[..1]).await? == 0 {
        return Ok(None);
    }
    socket.read_exact(&mut bytes[1..]).await?;
    let len = header.len.get() as usize;
    if len > max_frame_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame length exceeds the configured transport maximum",
        ));
    }
    let mut payload = vec![0; len];
    socket.read_exact(&mut payload).await?;
    Ok(Some(payload.into()))
}

#[cfg(test)]
#[path = "msg_framing_tests.rs"]
mod tests;
