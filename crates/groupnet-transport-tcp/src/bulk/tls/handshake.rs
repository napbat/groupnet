//! The admission exchange inside a TLS data-plane connection, after the TLS
//! handshake (see `session`) and before any stream byte.
//!
//! The client opens with a hello: the magic `GNBT` and the version byte
//! [`VERSION`], then its node id and its credential, each a field prefixed by
//! the shared big-endian [`LengthHeader`](groupnet_transport::framing::LengthHeader).
//! Ids are 1 to [`MAX_NODE_ID_BYTES`](groupnet_transport::MAX_NODE_ID_BYTES) bytes, credentials at most
//! [`MAX_CREDENTIAL_BYTES`]; a reader checks each length before it allocates.
//!
//! The hub runs its policy on the claim (`JoinRequest` with the client's
//! remote socket address) and requires the accepted identity to be the
//! claimed one. It answers one status byte: [`ACCEPTED`] followed by its own
//! id as a length-prefixed field, or [`REFUSED`] alone, after which it closes
//! the connection. The client checks the status and that the id is the peer
//! it dialed. Stream bytes follow the answer.
//!
//! A magic or version that does not match is refused as `InvalidData`. All of
//! it travels inside TLS, so neither the claim nor the credential is visible
//! on the network, and the credential reaches only the hub the client pinned.

use std::io;
use std::net::SocketAddr;

use groupnet_core::NodeId;
use groupnet_transport::admission::{Admission, JoinRequest, MAX_CREDENTIAL_BYTES};
use groupnet_transport::framing::write_vectored;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use zerocopy::{FromBytes, FromZeros, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::handshake::{field_header, read_field, read_id};

/// The first bytes of every admission hello.
const MAGIC: [u8; 4] = *b"GNBT";

/// The exchange's version. A hello with any other version is refused.
const VERSION: u8 = 1;

/// Status byte of an admitted client; the hub's id follows.
const ACCEPTED: u8 = 0;

/// Status byte of a refused client; the hub closes after it.
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

/// Runs the client side as `local` toward `peer`, presenting `credential`.
/// Returns once the stream may carry data. `local` and `credential` were
/// checked when the endpoint was made.
///
/// # Errors
/// `PermissionDenied` when the hub refuses this client or answers for another
/// id; `InvalidData` for an unknown status or a malformed id; any I/O error,
/// including the hub closing the connection without answering.
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
    sock.flush().await?;
    match sock.read_u8().await? {
        ACCEPTED => {}
        REFUSED => return Err(refused("peer refused this endpoint")),
        _ => return Err(invalid("unknown bulk admission status")),
    }
    if read_id(sock).await? != *peer {
        return Err(refused("peer answered for another node id"));
    }
    Ok(())
}

/// Runs the hub side as `local`, returning the client's id once its claim is
/// admitted and the answer is written. A refused claim is answered with
/// [`REFUSED`] before the error returns.
///
/// # Errors
/// `InvalidData` for a hello of another magic or version, or a malformed or
/// oversized field (refused before the body is allocated); `PermissionDenied`
/// for a client claiming this endpoint's id or a policy that accepts another
/// identity than the claimed one; the policy's [`AdmissionError`] as an
/// `io::Error`; any I/O error.
///
/// [`AdmissionError`]: groupnet_transport::admission::AdmissionError
pub(super) async fn accept(
    sock: &mut (impl AsyncRead + AsyncWrite + Unpin),
    local: &NodeId,
    admission: &dyn Admission,
    remote: Option<SocketAddr>,
) -> io::Result<NodeId> {
    let mut hello = Hello::new_zeroed();
    sock.read_exact(hello.as_mut_bytes()).await?;
    if hello.magic != MAGIC || hello.version != VERSION {
        return Err(invalid("unsupported bulk admission version"));
    }
    let claimed = read_id(sock).await?;
    let credential = read_field(sock, MAX_CREDENTIAL_BYTES).await?;
    if let Err(error) = judge(local, &claimed, &credential, admission, remote).await {
        // Best effort: the refusal stands whether or not the client reads it.
        let _ = write_vectored(sock, &[&[REFUSED]]).await;
        let _ = sock.flush().await;
        return Err(error);
    }
    let id = local.as_str().as_bytes();
    write_vectored(sock, &[&[ACCEPTED], field_header(id)?.as_bytes(), id]).await?;
    sock.flush().await?;
    Ok(claimed)
}

/// Admits `claimed` unless it is this endpoint's own id or `admission`
/// refuses it or renames it.
async fn judge(
    local: &NodeId,
    claimed: &NodeId,
    credential: &[u8],
    admission: &dyn Admission,
    remote: Option<SocketAddr>,
) -> io::Result<()> {
    if claimed == local {
        return Err(refused("peer claimed this endpoint's own id"));
    }
    let accepted = admission
        .admit(JoinRequest::new(claimed, credential, remote))
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
