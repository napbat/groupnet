//! Shared byte-stream framing for Unix sockets and Windows pipes.

use std::io;

use groupnet_core::NodeId;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::MAX_FRAME;

const MAGIC: [u8; 4] = *b"GNI1";
const MAX_ID: usize = 64;

pub(super) fn validate_id(id: &NodeId) -> io::Result<()> {
    if id.as_str().is_empty() || id.as_str().len() > MAX_ID {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "IPC node ID must contain 1..=64 bytes",
        ));
    }
    Ok(())
}

pub(super) async fn introduce<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    local: &NodeId,
) -> io::Result<NodeId> {
    let len = u8::try_from(local.as_str().len()).map_err(|_| invalid("invalid local ID"))?;
    let mut intro = [0_u8; 5 + MAX_ID];
    intro[..4].copy_from_slice(&MAGIC);
    intro[4] = len;
    intro[5..5 + usize::from(len)].copy_from_slice(local.as_str().as_bytes());
    stream.write_all(&intro[..5 + usize::from(len)]).await?;
    let mut header = [0_u8; 5];
    stream.read_exact(&mut header).await?;
    let len = usize::from(header[4]);
    if header[..4] != MAGIC || len == 0 || len > MAX_ID {
        return Err(invalid("invalid IPC introduction"));
    }
    let mut id = [0_u8; MAX_ID];
    stream.read_exact(&mut id[..len]).await?;
    let id = std::str::from_utf8(&id[..len]).map_err(|_| invalid("IPC ID is not UTF-8"))?;
    Ok(NodeId::new(id))
}

pub(super) async fn read<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<Vec<u8>> {
    let length = reader.read_u32_le().await?;
    if length > u32::try_from(MAX_FRAME).expect("IPC frame bound fits u32") {
        return Err(invalid("IPC frame exceeds maximum length"));
    }
    let length = usize::try_from(length).map_err(|_| invalid("IPC length does not fit usize"))?;
    let mut frame = vec![0_u8; length];
    reader.read_exact(&mut frame).await?;
    Ok(frame)
}

pub(super) async fn write<W: AsyncWrite + Unpin>(writer: &mut W, frame: &[u8]) -> io::Result<()> {
    let length =
        u32::try_from(frame.len()).map_err(|_| invalid("IPC frame length does not fit u32"))?;
    if frame.len() > MAX_FRAME {
        return Err(invalid("IPC frame exceeds maximum length"));
    }
    writer.write_u32_le(length).await?;
    writer.write_all(frame).await
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
