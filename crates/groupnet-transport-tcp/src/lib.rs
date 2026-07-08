//! # groupnet-transport-tcp
//!
//! A [`BulkTransport`] over TCP — Groupnet's default data-plane binding for
//! reliable, ordered byte streams (replication, snapshot transfer).
//!
//! A [`tokio::net::TcpStream`] is already `AsyncRead + AsyncWrite`; the only
//! glue is `tokio_util::compat` to present it as the runtime-agnostic
//! `futures-io` stream the trait asks for, plus a one-line node-id handshake so
//! the accepting side can attribute the connection.
//!
//! ## Scaffold simplification
//!
//! Peer addresses come from a static book you register up front (an inbound
//! TCP connection's *source* port is ephemeral, so we identify peers by a
//! handshake, not by address). Production would resolve addresses dynamically.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::RwLock;

use groupnet_core::NodeId;
use groupnet_transport::bulk::BulkTransport;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

const MAX_ID_LEN: usize = 1024;

/// A TCP-backed data-plane transport endpoint.
#[derive(Debug)]
pub struct TcpTransport {
    local: NodeId,
    listener: TcpListener,
    peers: RwLock<HashMap<NodeId, SocketAddr>>,
}

impl TcpTransport {
    /// Binds a listening TCP socket for `local`. Register peers with
    /// [`register_peer`](Self::register_peer) before connecting out.
    ///
    /// # Errors
    /// Propagates any socket bind error.
    pub async fn bind(local: NodeId, addr: impl ToSocketAddrs) -> io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self {
            local,
            listener,
            peers: RwLock::new(HashMap::new()),
        })
    }

    /// This endpoint's local node id.
    #[must_use]
    pub fn local_id(&self) -> &NodeId {
        &self.local
    }

    /// The address the listener is bound to (useful with an ephemeral `:0`).
    ///
    /// # Errors
    /// Propagates any socket error.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Teaches this endpoint that `node` listens at `addr`.
    pub fn register_peer(&self, node: NodeId, addr: SocketAddr) {
        self.peers
            .write()
            .expect("peers lock poisoned")
            .insert(node, addr);
    }
}

impl BulkTransport for TcpTransport {
    type Error = io::Error;
    type Stream = Compat<TcpStream>;

    async fn connect(&self, to: &NodeId) -> io::Result<Self::Stream> {
        // Resolve without holding the lock across the await.
        let addr = self
            .peers
            .read()
            .expect("peers lock poisoned")
            .get(to)
            .copied()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "unknown peer"))?;
        let mut sock = TcpStream::connect(addr).await?;
        write_id(&mut sock, &self.local).await?;
        Ok(sock.compat())
    }

    async fn accept(&self) -> io::Result<(NodeId, Self::Stream)> {
        let (mut sock, _addr) = self.listener.accept().await?;
        let from = read_id(&mut sock).await?;
        Ok((from, sock.compat()))
    }
}

/// Sends our node id as a length-prefixed handshake so the peer can attribute
/// the connection.
async fn write_id(sock: &mut TcpStream, id: &NodeId) -> io::Result<()> {
    let bytes = id.as_str().as_bytes();
    sock.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
    sock.write_all(bytes).await?;
    Ok(())
}

/// Reads the peer's handshake node id.
async fn read_id(sock: &mut TcpStream) -> io::Result<NodeId> {
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
