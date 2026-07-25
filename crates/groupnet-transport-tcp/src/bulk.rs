//! Data-plane streams over TCP: [`TcpBulkTransport`], a [`BulkTransport`] for
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
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

use crate::handshake::{read_id, write_id};

/// A TCP-backed data-plane transport endpoint.
#[derive(Debug)]
pub struct TcpBulkTransport {
    local: NodeId,
    listener: TcpListener,
    peers: RwLock<HashMap<NodeId, SocketAddr>>,
}

impl TcpBulkTransport {
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

impl BulkTransport for TcpBulkTransport {
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
