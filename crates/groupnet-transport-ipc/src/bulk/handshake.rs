//! The data plane's versioned identity handshake, judged by an optional
//! admission policy.
//!
//! The dialer opens with a hello: the magic `GNIB` and the version byte
//! [`VERSION`], then its node id and its credential, each a field prefixed by
//! the shared big-endian [`LengthHeader`]. Ids are at most 64 bytes (the IPC
//! identity bound), credentials at most [`MAX_CREDENTIAL_BYTES`]; a reader
//! checks each length before it allocates.
//!
//! The acceptor runs its policy on the claim (`JoinRequest` with no remote
//! socket address) and requires the accepted identity to be the claimed one.
//! It answers one status byte: [`ACCEPTED`] followed by its own id as a
//! length-prefixed field, or [`REFUSED`] alone, after which it closes the
//! connection. The dialer checks the status and that the id is the peer it
//! dialed. Stream bytes follow the answer.
//!
//! A magic or version that does not match is refused as `InvalidData`. Like
//! every IPC identity, the claim is trusted within the operating system's
//! security boundary; the credential is an application claim, not a
//! cryptographic proof.

use std::io;

use groupnet_core::NodeId;
use groupnet_transport::admission::{Admission, JoinRequest, MAX_CREDENTIAL_BYTES};
use groupnet_transport::framing::{LengthHeader, write_vectored};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use zerocopy::{FromBytes, FromZeros, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::ipc::frame::MAX_ID;

/// The first bytes of every data-plane hello.
const MAGIC: [u8; 4] = *b"GNIB";

/// The handshake version. A hello with any other version is refused.
const VERSION: u8 = 1;

/// Status byte of an admitted dialer; the acceptor's id follows.
const ACCEPTED: u8 = 0;

/// Status byte of a refused dialer; the acceptor closes after it.
const REFUSED: u8 = 1;

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct Hello {
    magic: [u8; 4],
    version: u8,
}

fn refused(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, message)
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

/// The typed length prefix of one field.
fn field_header(bytes: &[u8]) -> io::Result<LengthHeader> {
    Ok(LengthHeader::new(bytes.len())?)
}

/// Reads one length prefix and checks it against `max` before any body is
/// read.
async fn read_length(sock: &mut (impl AsyncRead + Unpin), max: usize) -> io::Result<usize> {
    let mut header = LengthHeader::new_zeroed();
    sock.read_exact(header.as_mut_bytes()).await?;
    Ok(header.length_within(max)?)
}

/// Reads a node id field: nonempty UTF-8 of at most [`MAX_ID`] bytes, read
/// into a stack buffer.
async fn read_id(sock: &mut (impl AsyncRead + Unpin)) -> io::Result<NodeId> {
    let len = read_length(sock, MAX_ID).await?;
    if len == 0 {
        return Err(invalid("handshake id is empty"));
    }
    let mut id = [0; MAX_ID];
    sock.read_exact(&mut id[..len]).await?;
    std::str::from_utf8(&id[..len])
        .map(NodeId::new)
        .map_err(|_| invalid("handshake id is not UTF-8"))
}

/// Runs the dialing side as `local` toward `peer`, presenting `credential`.
/// Returns once the stream may carry data. `local` and `credential` were
/// checked when the endpoint was made.
///
/// # Errors
/// `PermissionDenied` when the acceptor refuses this endpoint or answers for
/// another id; `InvalidData` for an unknown status or a malformed id; any I/O
/// error, including the acceptor closing the connection without answering.
pub(super) async fn dial(
    sock: &mut (impl AsyncRead + AsyncWrite + Unpin),
    local: &NodeId,
    credential: &[u8],
    peer: &NodeId,
) -> io::Result<()> {
    let hello = Hello {
        magic: MAGIC,
        version: VERSION,
    };
    let id = local.as_str().as_bytes();
    write_vectored(
        sock,
        &[
            hello.as_bytes(),
            field_header(id)?.as_bytes(),
            id,
            field_header(credential)?.as_bytes(),
            credential,
        ],
    )
    .await?;
    match sock.read_u8().await? {
        ACCEPTED => {}
        REFUSED => return Err(refused("peer refused this endpoint")),
        _ => return Err(invalid("unknown bulk handshake status")),
    }
    if read_id(sock).await? != *peer {
        return Err(refused("peer answered for another node id"));
    }
    Ok(())
}

/// Runs the accepting side as `local`, returning the dialer's id once its
/// claim is admitted and the answer is written. A refused claim is answered
/// with [`REFUSED`] before the error returns.
///
/// # Errors
/// `InvalidData` for a hello of another magic or version, or a malformed or
/// oversized field (refused before the body is allocated); `PermissionDenied`
/// for a dialer claiming this endpoint's id or a policy that accepts another
/// identity than the claimed one; the policy's [`AdmissionError`] as an
/// `io::Error`; any I/O error.
///
/// [`AdmissionError`]: groupnet_transport::admission::AdmissionError
pub(super) async fn accept(
    sock: &mut (impl AsyncRead + AsyncWrite + Unpin),
    local: &NodeId,
    admission: Option<&dyn Admission>,
) -> io::Result<NodeId> {
    let mut hello = Hello::new_zeroed();
    sock.read_exact(hello.as_mut_bytes()).await?;
    if hello.magic != MAGIC || hello.version != VERSION {
        return Err(invalid("unsupported IPC bulk handshake version"));
    }
    let claimed = read_id(sock).await?;
    let mut credential = vec![0; read_length(sock, MAX_CREDENTIAL_BYTES).await?];
    sock.read_exact(&mut credential).await?;
    if let Err(error) = judge(local, &claimed, &credential, admission).await {
        // Best effort: the refusal stands whether or not the dialer reads it.
        let _ = write_vectored(sock, &[&[REFUSED]]).await;
        return Err(error);
    }
    let id = local.as_str().as_bytes();
    write_vectored(sock, &[&[ACCEPTED], field_header(id)?.as_bytes(), id]).await?;
    Ok(claimed)
}

/// Admits `claimed` unless it is this endpoint's own id or `admission`
/// refuses it or renames it.
async fn judge(
    local: &NodeId,
    claimed: &NodeId,
    credential: &[u8],
    admission: Option<&dyn Admission>,
) -> io::Result<()> {
    if claimed == local {
        return Err(refused("peer claimed this endpoint's own id"));
    }
    let Some(policy) = admission else {
        return Ok(());
    };
    let accepted = policy
        .admit(JoinRequest::new(claimed, credential, None))
        .await?;
    if accepted.node != *claimed {
        return Err(refused(
            "admission accepted another id than the claimed one",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
