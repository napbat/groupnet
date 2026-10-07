//! Native message framing: the shared typed length prefix and a separately
//! owned payload, written together by the shared vectored writer.

use std::io;

use bytes::Bytes;
use groupnet_transport::framing::{LengthHeader, write_vectored};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use zerocopy::{FromZeros, IntoBytes};

/// Writes a header and body without allocating a concatenated frame.
pub(super) async fn write_frame(
    socket: &mut (impl AsyncWrite + Unpin),
    payload: &[u8],
) -> io::Result<()> {
    let header = LengthHeader::new(payload.len())?;
    write_vectored(socket, &[header.as_bytes(), payload]).await
}

/// A clean close is only valid before the first header byte. Reject partial
/// headers and oversized lengths before allocating payload storage.
pub(super) async fn read_frame(
    socket: &mut (impl AsyncRead + Unpin),
    max_frame_bytes: usize,
) -> io::Result<Option<Bytes>> {
    let mut header = LengthHeader::new_zeroed();
    let bytes = header.as_mut_bytes();
    if socket.read(&mut bytes[..1]).await? == 0 {
        return Ok(None);
    }
    socket.read_exact(&mut bytes[1..]).await?;
    let mut payload = vec![0; header.length_within(max_frame_bytes)?];
    socket.read_exact(&mut payload).await?;
    Ok(Some(payload.into()))
}

#[cfg(test)]
#[path = "msg_framing_tests.rs"]
mod tests;
