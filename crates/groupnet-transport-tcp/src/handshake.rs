//! Length-prefixed handshake fields shared by both planes: the dialing side
//! introduces itself first, because a TCP source address (its port is
//! ephemeral) cannot identify a peer the way a bound UDP source address can.
//!
//! Every field is a typed [`LengthHeader`] followed by its bytes. Node ids
//! obey the one identity bound, [`MAX_NODE_ID_BYTES`] and nonempty, on both
//! the write and the read side; readers check a length before allocating.

use std::io;

use groupnet_core::NodeId;
use groupnet_transport::MAX_NODE_ID_BYTES;
use groupnet_transport::framing::{LengthHeader, write_vectored};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use zerocopy::{FromZeros, IntoBytes};

/// Longest accepted advertised address in an intro.
#[cfg(feature = "msg")]
pub(crate) const MAX_ADDR_LEN: usize = 256;

/// Checks `id` against the identity bound every adapter shares.
///
/// # Errors
/// Returns `InvalidInput` for an empty or overlong id.
pub(crate) fn check_id(id: &NodeId) -> io::Result<()> {
    let len = id.as_str().len();
    if len == 0 || len > MAX_NODE_ID_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "node id outside the handshake identity bound",
        ));
    }
    Ok(())
}

/// The typed length prefix of one field.
pub(crate) fn field_header(bytes: &[u8]) -> io::Result<LengthHeader> {
    LengthHeader::new(bytes.len())
}

/// Sends our node id as one length-prefixed field so the peer can attribute
/// the connection.
pub(crate) async fn write_id(sock: &mut (impl AsyncWrite + Unpin), id: &NodeId) -> io::Result<()> {
    check_id(id)?;
    let id = id.as_str().as_bytes();
    write_vectored(sock, &[field_header(id)?.as_bytes(), id]).await
}

/// Sends the raw msg-plane intro, node id then (possibly empty) listener
/// address, in one vectored write.
#[cfg(feature = "msg")]
pub(crate) async fn write_intro(
    sock: &mut (impl AsyncWrite + Unpin),
    id: &NodeId,
    addr: &str,
) -> io::Result<()> {
    check_id(id)?;
    let id = id.as_str().as_bytes();
    let addr = addr.as_bytes();
    write_vectored(
        sock,
        &[
            field_header(id)?.as_bytes(),
            id,
            field_header(addr)?.as_bytes(),
            addr,
        ],
    )
    .await
}

/// Reads one typed length prefix.
async fn read_header(sock: &mut (impl AsyncRead + Unpin)) -> io::Result<LengthHeader> {
    let mut header = LengthHeader::new_zeroed();
    sock.read_exact(header.as_mut_bytes()).await?;
    Ok(header)
}

/// Reads the body announced by `header`, rejecting it above `max` before
/// allocating.
pub(crate) async fn read_body(
    sock: &mut (impl AsyncRead + Unpin),
    header: LengthHeader,
    max: usize,
) -> io::Result<Vec<u8>> {
    let mut bytes = vec![0; header.length_within(max)?];
    sock.read_exact(&mut bytes).await?;
    Ok(bytes)
}

/// Reads one length-prefixed field of at most `max` bytes.
pub(crate) async fn read_field(
    sock: &mut (impl AsyncRead + Unpin),
    max: usize,
) -> io::Result<Vec<u8>> {
    let header = read_header(sock).await?;
    read_body(sock, header, max).await
}

/// Decodes a node id body: nonempty UTF-8 within the identity bound.
pub(crate) fn decode_id(bytes: Vec<u8>) -> io::Result<NodeId> {
    if bytes.is_empty() || bytes.len() > MAX_NODE_ID_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "handshake id outside the identity bound",
        ));
    }
    String::from_utf8(bytes)
        .map(NodeId::new)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "handshake id not utf-8"))
}

/// Reads the peer's handshake node id.
pub(crate) async fn read_id(sock: &mut (impl AsyncRead + Unpin)) -> io::Result<NodeId> {
    decode_id(read_field(sock, MAX_NODE_ID_BYTES).await?)
}

/// Reads an intro address field: UTF-8 of at most [`MAX_ADDR_LEN`] bytes,
/// empty meaning "nothing dialable to advertise".
#[cfg(feature = "msg")]
pub(crate) async fn read_addr(sock: &mut (impl AsyncRead + Unpin)) -> io::Result<String> {
    String::from_utf8(read_field(sock, MAX_ADDR_LEN).await?)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "intro address not utf-8"))
}

/// Reads the raw msg-plane intro written by [`write_intro`].
#[cfg(feature = "msg")]
pub(crate) async fn read_intro(
    sock: &mut (impl AsyncRead + Unpin),
) -> io::Result<(NodeId, String)> {
    let id = read_id(sock).await?;
    Ok((id, read_addr(sock).await?))
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};

    use super::*;

    /// A connected pair of loopback sockets. The handshake helpers take a
    /// concrete `TcpStream`, so the round trip runs over a real connection
    /// rather than an in-memory duplex.
    async fn pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let (dialed, accepted) = tokio::join!(TcpStream::connect(addr), listener.accept());
        (dialed.expect("connect"), accepted.expect("accept").0)
    }

    /// The dialer's id survives the wire verbatim, in both directions and for
    /// multi-byte UTF-8 — this attribution is all the accepting side gets.
    #[tokio::test]
    async fn id_round_trips_over_a_connection() {
        let (mut dialer, mut acceptor) = pair().await;

        write_id(&mut dialer, &NodeId::new("node-a"))
            .await
            .expect("write");
        assert_eq!(
            read_id(&mut acceptor).await.expect("read"),
            NodeId::new("node-a")
        );

        write_id(&mut acceptor, &NodeId::new("nœud-β"))
            .await
            .expect("write");
        assert_eq!(
            read_id(&mut dialer).await.expect("read"),
            NodeId::new("nœud-β")
        );
    }

    /// The length cap is inclusive and shared with session admission: an id
    /// of exactly `MAX_NODE_ID_BYTES` bytes is still a legal handshake.
    #[tokio::test]
    async fn an_id_at_the_length_cap_is_accepted() {
        let (mut dialer, mut acceptor) = pair().await;
        let long = "x".repeat(MAX_NODE_ID_BYTES);

        write_id(&mut dialer, &NodeId::new(long.clone()))
            .await
            .expect("write");
        assert_eq!(
            read_id(&mut acceptor).await.expect("read"),
            NodeId::new(long)
        );
    }

    /// An id the peer would reject is refused locally before any byte is sent.
    #[tokio::test]
    async fn ids_outside_the_bound_are_never_written() {
        let mut sink = Vec::new();
        for id in [String::new(), "x".repeat(MAX_NODE_ID_BYTES + 1)] {
            let err = write_id(&mut sink, &NodeId::new(id))
                .await
                .expect_err("invalid id refused");
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        }
        assert_eq!(sink, Vec::<u8>::new());
    }

    /// A length prefix past the cap is rejected on the prefix alone — the
    /// reader never allocates the advertised body — and so is an empty id.
    #[tokio::test]
    async fn oversized_or_empty_ids_are_rejected() {
        for len in [MAX_NODE_ID_BYTES + 1, 0] {
            let (mut dialer, mut acceptor) = pair().await;
            let len = u32::try_from(len).expect("fits in u32");
            dialer.write_all(&len.to_be_bytes()).await.expect("write");

            let err = read_id(&mut acceptor).await.expect_err("id rejected");
            assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        }
    }

    /// Non-UTF-8 bytes are a protocol error, not a lossy conversion.
    #[tokio::test]
    async fn a_non_utf8_id_is_rejected() {
        let (mut dialer, mut acceptor) = pair().await;
        dialer.write_all(&2u32.to_be_bytes()).await.expect("write");
        dialer.write_all(&[0xff, 0xfe]).await.expect("write");

        let err = read_id(&mut acceptor)
            .await
            .expect_err("non-utf8 id rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    /// The field layout is a big-endian u32 length then the bytes.
    #[tokio::test]
    async fn id_field_keeps_the_network_layout() {
        let mut wire = Vec::new();
        write_id(&mut wire, &NodeId::new("ab"))
            .await
            .expect("write");
        assert_eq!(wire, [0, 0, 0, 2, b'a', b'b']);
    }

    /// The msg-plane intro round trips, and the empty address — "I have
    /// nothing dialable to advertise" — is a valid value, not a missing field.
    #[cfg(feature = "msg")]
    #[tokio::test]
    async fn intro_round_trips_including_the_empty_advertisement() {
        let (mut dialer, mut acceptor) = pair().await;
        let id = NodeId::new("node-a");

        write_intro(&mut dialer, &id, "127.0.0.1:7000")
            .await
            .expect("write");
        assert_eq!(
            read_intro(&mut acceptor).await.expect("read"),
            (id.clone(), "127.0.0.1:7000".to_owned())
        );

        write_intro(&mut dialer, &id, "").await.expect("write");
        assert_eq!(
            read_intro(&mut acceptor).await.expect("read"),
            (id, String::new())
        );
    }

    /// The intro address has its own cap; overshooting it is an error rather
    /// than an unbounded allocation.
    #[cfg(feature = "msg")]
    #[tokio::test]
    async fn an_oversized_intro_address_is_rejected() {
        let (mut dialer, mut acceptor) = pair().await;
        let len = u32::try_from(MAX_ADDR_LEN + 1).expect("fits in u32");
        dialer.write_all(&len.to_be_bytes()).await.expect("write");

        let err = read_addr(&mut acceptor)
            .await
            .expect_err("oversized address rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
