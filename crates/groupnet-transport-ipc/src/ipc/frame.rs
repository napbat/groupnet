//! Shared byte-stream framing for Unix sockets and Windows pipes.

use std::io;

use bytes::Bytes;
use groupnet_core::NodeId;
use groupnet_transport::framing::write_vectored;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use zerocopy::byteorder::little_endian::U32;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use super::MAX_FRAME;

const MAGIC: [u8; 4] = *b"GNI1";
const MAX_ID: usize = 64;

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned, Clone, Copy, Debug)]
#[repr(C)]
struct IntroductionHeader {
    magic: [u8; 4],
    id_len: u8,
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned, Clone, Copy, Debug)]
#[repr(C)]
struct FrameHeader {
    length: U32,
}

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
    validate_id(local)?;
    let len = u8::try_from(local.as_str().len()).map_err(|_| invalid("invalid local ID"))?;
    let header = IntroductionHeader {
        magic: MAGIC,
        id_len: len,
    };
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(local.as_str().as_bytes()).await?;
    let mut header = IntroductionHeader {
        magic: [0; 4],
        id_len: 0,
    };
    stream.read_exact(header.as_mut_bytes()).await?;
    let len = usize::from(header.id_len);
    if header.magic != MAGIC || len == 0 || len > MAX_ID {
        return Err(invalid("invalid IPC introduction"));
    }
    let mut id = [0_u8; MAX_ID];
    stream.read_exact(&mut id[..len]).await?;
    let id = std::str::from_utf8(&id[..len]).map_err(|_| invalid("IPC ID is not UTF-8"))?;
    Ok(NodeId::new(id))
}

pub(super) async fn read<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<Bytes> {
    let mut header = FrameHeader {
        length: U32::new(0),
    };
    reader.read_exact(header.as_mut_bytes()).await?;
    let length = header.length.get();
    if length > u32::try_from(MAX_FRAME).expect("IPC frame bound fits u32") {
        return Err(invalid("IPC frame exceeds maximum length"));
    }
    let length = usize::try_from(length).map_err(|_| invalid("IPC length does not fit usize"))?;
    let mut frame = vec![0_u8; length];
    reader.read_exact(&mut frame).await?;
    Ok(frame.into())
}

pub(super) async fn write<W: AsyncWrite + Unpin>(writer: &mut W, frame: &[u8]) -> io::Result<()> {
    let length =
        u32::try_from(frame.len()).map_err(|_| invalid("IPC frame length does not fit u32"))?;
    if frame.len() > MAX_FRAME {
        return Err(invalid("IPC frame exceeds maximum length"));
    }
    let header = FrameHeader {
        length: U32::new(length),
    };
    // One vectored write for header and payload, without joining them.
    write_vectored(writer, &[header.as_bytes(), frame]).await
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests;
