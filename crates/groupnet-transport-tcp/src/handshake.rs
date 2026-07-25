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
