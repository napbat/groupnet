//! The data plane's versioned identity handshake, and its mutual challenge
//! under a [`ClusterSecret`].
//!
//! The dialer opens with a hello: the magic `GNBH`, the version byte
//! [`VERSION`], a mode byte (open or keyed) and its node id as a
//! length-prefixed field. An open hello ends there and the stream starts; the
//! accepting side attributes it to the claimed id.
//!
//! A keyed hello carries a fresh 32-byte dialer nonce. The acceptor answers
//! with its own node id, a fresh acceptor nonce and its tag; the dialer checks
//! that the id is the peer it dialed and verifies the tag, then sends its own
//! tag, which the acceptor verifies before it hands the stream on. Each tag is
//! the HMAC over the protocol label, [`VERSION`], the signer's role, both
//! nonces and both claimed ids, so neither side's tag replays on another
//! connection or serves as the other role's answer.
//!
//! A version, magic or mode that does not match is refused: an open endpoint
//! refuses a keyed hello, a keyed endpoint an open one.
//!
//! The handshake authenticates connection setup only. Bytes after it carry no
//! integrity protection and are not bound to the handshake, so an on-path
//! attacker can inject into or alter RPC traffic: keep the data plane on a
//! private network, or use a mutually authenticated TLS mode (not yet
//! implemented) where it crosses a trust boundary.

use std::io;

use groupnet_core::NodeId;
use groupnet_transport::auth::{ClusterSecret, NONCE_LEN, TAG_LEN, fresh_nonce};
use groupnet_transport::framing::write_vectored;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use zerocopy::{FromBytes, FromZeros, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::handshake::{check_id, field_header, read_id};

/// The first bytes of every data-plane hello.
const MAGIC: [u8; 4] = *b"GNBH";

/// The handshake version. A hello with any other version is refused.
pub(crate) const VERSION: u8 = 1;

/// The domain label of every handshake tag.
const DOMAIN: &[u8] = b"groupnet tcp bulk handshake";

/// Mode byte of an unauthenticated hello.
const OPEN: u8 = 0;

/// Mode byte of a hello that starts the mutual challenge.
const KEYED: u8 = 1;

/// Role byte of the acceptor's tag.
const ACCEPTOR: u8 = b'A';

/// Role byte of the dialer's tag.
const DIALER: u8 = b'D';

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct Hello {
    magic: [u8; 4],
    version: u8,
    mode: u8,
}

fn refused(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, message)
}

/// Everything both tags of one keyed handshake cover, besides the role.
struct Transcript<'a> {
    dialer: &'a NodeId,
    acceptor: &'a NodeId,
    dialer_nonce: &'a [u8; NONCE_LEN],
    acceptor_nonce: &'a [u8; NONCE_LEN],
}

impl Transcript<'_> {
    fn tag(&self, secret: &ClusterSecret, role: u8) -> [u8; TAG_LEN] {
        secret.tag(DOMAIN, &self.parts(&[VERSION, role]))
    }

    fn verify(&self, secret: &ClusterSecret, role: u8, tag: &[u8]) -> bool {
        secret.verify(DOMAIN, &self.parts(&[VERSION, role]), tag)
    }

    fn parts<'a>(&'a self, header: &'a [u8; 2]) -> [&'a [u8]; 5] {
        [
            header,
            self.dialer_nonce,
            self.acceptor_nonce,
            self.dialer.as_str().as_bytes(),
            self.acceptor.as_str().as_bytes(),
        ]
    }
}

/// Runs the dialing side toward `peer`. Returns once the stream may carry
/// data: at once after an open hello, after the acceptor authenticated under
/// a secret.
///
/// # Errors
/// `PermissionDenied` when the acceptor answers for another id or its tag
/// does not verify; any I/O error, including the acceptor closing the
/// connection because it refused this hello.
pub(crate) async fn dial(
    sock: &mut (impl AsyncRead + AsyncWrite + Unpin),
    local: &NodeId,
    peer: &NodeId,
    secret: Option<&ClusterSecret>,
) -> io::Result<()> {
    check_id(local)?;
    let id = local.as_str().as_bytes();
    let length = field_header(id)?;
    let Some(secret) = secret else {
        let hello = Hello {
            magic: MAGIC,
            version: VERSION,
            mode: OPEN,
        };
        return write_vectored(sock, &[hello.as_bytes(), length.as_bytes(), id]).await;
    };
    let hello = Hello {
        magic: MAGIC,
        version: VERSION,
        mode: KEYED,
    };
    let dialer_nonce = fresh_nonce()?;
    write_vectored(
        sock,
        &[hello.as_bytes(), length.as_bytes(), id, &dialer_nonce],
    )
    .await?;
    let acceptor = read_id(sock).await?;
    let mut acceptor_nonce = [0; NONCE_LEN];
    sock.read_exact(&mut acceptor_nonce).await?;
    let mut tag = [0; TAG_LEN];
    sock.read_exact(&mut tag).await?;
    if acceptor != *peer {
        return Err(refused("peer answered for another node id"));
    }
    let transcript = Transcript {
        dialer: local,
        acceptor: &acceptor,
        dialer_nonce: &dialer_nonce,
        acceptor_nonce: &acceptor_nonce,
    };
    if !transcript.verify(secret, ACCEPTOR, &tag) {
        return Err(refused("peer failed cluster authentication"));
    }
    write_vectored(sock, &[&transcript.tag(secret, DIALER)]).await
}

/// Runs the accepting side as `local`, returning the dialer's id once its
/// hello (and, under a secret, its answer) is accepted.
///
/// # Errors
/// `InvalidData` for a hello of another version or an unknown mode;
/// `PermissionDenied` for a mode that does not match this endpoint's or an
/// answer that does not verify; any I/O error.
pub(crate) async fn accept(
    sock: &mut (impl AsyncRead + AsyncWrite + Unpin),
    local: &NodeId,
    secret: Option<&ClusterSecret>,
) -> io::Result<NodeId> {
    let mut hello = Hello::new_zeroed();
    sock.read_exact(hello.as_mut_bytes()).await?;
    if hello.magic != MAGIC || hello.version != VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported bulk handshake version",
        ));
    }
    let dialer = read_id(sock).await?;
    let secret = match (hello.mode, secret) {
        (OPEN, None) => return Ok(dialer),
        (KEYED, Some(secret)) => secret,
        (OPEN, Some(_)) => return Err(refused("unauthenticated peer refused")),
        (KEYED, None) => return Err(refused("keyed hello without a local cluster secret")),
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unknown bulk handshake mode",
            ));
        }
    };
    let mut dialer_nonce = [0; NONCE_LEN];
    sock.read_exact(&mut dialer_nonce).await?;
    let acceptor_nonce = fresh_nonce()?;
    let transcript = Transcript {
        dialer: &dialer,
        acceptor: local,
        dialer_nonce: &dialer_nonce,
        acceptor_nonce: &acceptor_nonce,
    };
    let id = local.as_str().as_bytes();
    write_vectored(
        sock,
        &[
            field_header(id)?.as_bytes(),
            id,
            &acceptor_nonce,
            &transcript.tag(secret, ACCEPTOR),
        ],
    )
    .await?;
    let mut tag = [0; TAG_LEN];
    sock.read_exact(&mut tag).await?;
    if !transcript.verify(secret, DIALER, &tag) {
        return Err(refused("peer failed cluster authentication"));
    }
    Ok(dialer)
}

#[cfg(test)]
mod tests;
