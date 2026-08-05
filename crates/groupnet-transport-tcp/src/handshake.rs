//! The one-line node-id handshake shared by both planes: the dialing side
//! introduces itself first, because a TCP source address (its port is
//! ephemeral) cannot identify a peer the way a bound UDP source address can.

use std::io;

use groupnet_core::NodeId;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Longest accepted peer id, bounding the handshake allocation.
const MAX_ID_LEN: usize = 1024;

/// Longest accepted advertised address in the msg-plane intro.
#[cfg(feature = "msg")]
const MAX_ADDR_LEN: usize = 256;

/// Sends a length-prefixed UTF-8 string (the msg-plane intro address; empty
/// means "nothing dialable to advertise").
#[cfg(feature = "msg")]
pub(crate) async fn write_str(sock: &mut TcpStream, s: &str) -> io::Result<()> {
    let bytes = s.as_bytes();
    sock.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
    sock.write_all(bytes).await?;
    Ok(())
}

/// Reads a length-prefixed UTF-8 string (the msg-plane intro address).
#[cfg(feature = "msg")]
pub(crate) async fn read_str(sock: &mut TcpStream) -> io::Result<String> {
    let mut len_bytes = [0u8; 4];
    sock.read_exact(&mut len_bytes).await?;
    let len = u32::from_be_bytes(len_bytes) as usize;
    if len > MAX_ADDR_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "intro address too long",
        ));
    }
    let mut bytes = vec![0u8; len];
    sock.read_exact(&mut bytes).await?;
    String::from_utf8(bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "intro address not utf-8"))
}

/// Sends our node id as a length-prefixed handshake so the peer can attribute
/// the connection.
pub(crate) async fn write_id(sock: &mut TcpStream, id: &NodeId) -> io::Result<()> {
    let bytes = id.as_str().as_bytes();
    sock.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
    sock.write_all(bytes).await?;
    Ok(())
}

/// Reads the peer's handshake node id.
pub(crate) async fn read_id(sock: &mut TcpStream) -> io::Result<NodeId> {
    let mut len_bytes = [0u8; 4];
    sock.read_exact(&mut len_bytes).await?;
    let len = u32::from_be_bytes(len_bytes) as usize;
    if len > MAX_ID_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "handshake id too long",
        ));
    }
    let mut id_bytes = vec![0u8; len];
    sock.read_exact(&mut id_bytes).await?;
    let id = std::str::from_utf8(&id_bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "handshake id not utf-8"))?;
    Ok(NodeId::new(id))
}

#[cfg(test)]
mod tests {
    use tokio::net::TcpListener;

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

    /// The length cap is inclusive: an id of exactly `MAX_ID_LEN` bytes is
    /// still a legal handshake.
    #[tokio::test]
    async fn an_id_at_the_length_cap_is_accepted() {
        let (mut dialer, mut acceptor) = pair().await;
        let long = "x".repeat(MAX_ID_LEN);

        write_id(&mut dialer, &NodeId::new(long.clone()))
            .await
            .expect("write");
        assert_eq!(
            read_id(&mut acceptor).await.expect("read"),
            NodeId::new(long)
        );
    }

    /// A length prefix past the cap is rejected on the prefix alone — the
    /// reader never allocates the advertised body.
    #[tokio::test]
    async fn an_oversized_length_prefix_is_rejected() {
        let (mut dialer, mut acceptor) = pair().await;
        let len = u32::try_from(MAX_ID_LEN + 1).expect("fits in u32");
        dialer.write_all(&len.to_be_bytes()).await.expect("write");

        let err = read_id(&mut acceptor)
            .await
            .expect_err("oversized id rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
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

    /// The msg-plane intro address round trips, and the empty string — "I have
    /// nothing dialable to advertise" — is a valid value, not a missing field.
    #[cfg(feature = "msg")]
    #[tokio::test]
    async fn intro_address_round_trips_including_the_empty_advertisement() {
        let (mut dialer, mut acceptor) = pair().await;

        write_str(&mut dialer, "127.0.0.1:7000")
            .await
            .expect("write");
        assert_eq!(
            read_str(&mut acceptor).await.expect("read"),
            "127.0.0.1:7000"
        );

        write_str(&mut dialer, "").await.expect("write");
        assert_eq!(read_str(&mut acceptor).await.expect("read"), "");
    }

    /// The intro address has its own, tighter cap; overshooting it is an error
    /// rather than an unbounded allocation.
    #[cfg(feature = "msg")]
    #[tokio::test]
    async fn an_oversized_intro_address_is_rejected() {
        let (mut dialer, mut acceptor) = pair().await;
        let len = u32::try_from(MAX_ADDR_LEN + 1).expect("fits in u32");
        dialer.write_all(&len.to_be_bytes()).await.expect("write");

        let err = read_str(&mut acceptor)
            .await
            .expect_err("oversized address rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
