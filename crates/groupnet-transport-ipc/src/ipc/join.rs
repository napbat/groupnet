//! The wire of an admitted IPC session: a join hello in each direction, the
//! acceptor's echo of the identity it admitted, and the dialer's
//! acknowledgement (see `docs/client-role.md`, section 2.1).
//!
//! Every field is the shared big-endian [`LengthHeader`] followed by its
//! bytes. Readers check the magic before they wait for more bytes and every
//! length before they allocate, so a static introduction or a foreign
//! protocol is refused at once and an oversized claim never allocates.

use std::io;

use groupnet_core::NodeId;
use groupnet_transport::admission::MAX_CREDENTIAL_BYTES;
use groupnet_transport::framing::{LengthHeader, write_vectored};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use zerocopy::{FromZeros, IntoBytes};

use super::frame::{MAX_ID, validate_id};

/// The first bytes of an admitted session's hello. A static endpoint's
/// introduction starts with `GNI1` and is refused here.
const MAGIC: [u8; 4] = *b"GNIJ";

/// One side's claim: its routing identity and opaque credential.
#[derive(Debug)]
pub(super) struct Hello {
    pub(super) node: NodeId,
    pub(super) credential: Vec<u8>,
}

/// Writes the hello of `local` in one vectored write.
pub(super) async fn write_hello(
    stream: &mut (impl AsyncWrite + Unpin),
    local: &NodeId,
    credential: &[u8],
) -> io::Result<()> {
    validate_id(local)?;
    let node = local.as_str().as_bytes();
    write_vectored(
        stream,
        &[
            &MAGIC,
            LengthHeader::new(node.len())?.as_bytes(),
            node,
            LengthHeader::new(credential.len())?.as_bytes(),
            credential,
        ],
    )
    .await
}

/// Reads a hello written by [`write_hello`].
pub(super) async fn read_hello(stream: &mut (impl AsyncRead + Unpin)) -> io::Result<Hello> {
    let mut magic = [0; 4];
    stream.read_exact(&mut magic).await?;
    if magic != MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "not an admitted IPC hello",
        ));
    }
    let node = read_id(stream).await?;
    let credential = read_field(stream, MAX_CREDENTIAL_BYTES).await?;
    Ok(Hello { node, credential })
}

/// Writes one identity as a length-prefixed field: the acceptor's echo of
/// the identity it admitted, and the dialer's acknowledgement.
pub(super) async fn write_id(
    stream: &mut (impl AsyncWrite + Unpin),
    id: &NodeId,
) -> io::Result<()> {
    validate_id(id)?;
    let id = id.as_str().as_bytes();
    write_vectored(stream, &[LengthHeader::new(id.len())?.as_bytes(), id]).await
}

/// Reads one identity field: nonempty UTF-8 within the IPC identity bound.
pub(super) async fn read_id(stream: &mut (impl AsyncRead + Unpin)) -> io::Result<NodeId> {
    let bytes = read_field(stream, MAX_ID).await?;
    if bytes.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "empty IPC identity",
        ));
    }
    String::from_utf8(bytes)
        .map(NodeId::new)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "IPC identity is not UTF-8"))
}

/// Reads one field of at most `max` bytes, refusing a longer one before it
/// allocates.
async fn read_field(stream: &mut (impl AsyncRead + Unpin), max: usize) -> io::Result<Vec<u8>> {
    let mut header = LengthHeader::new_zeroed();
    stream.read_exact(header.as_mut_bytes()).await?;
    let mut bytes = vec![0; header.length_within(max)?];
    stream.read_exact(&mut bytes).await?;
    Ok(bytes)
}

#[cfg(test)]
mod tests;
