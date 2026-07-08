//! # groupnet-udp
//!
//! A real [`Transport`] over UDP datagrams — a concrete binding of Groupnet's
//! transport-agnostic trait.
//!
//! UDP is the natural fit for the best-effort, message-oriented contract: one
//! frame per datagram, loss and reorder tolerated, no connection state.
//!
//! ## Scaffold simplifications
//!
//! * **Static address book.** The engine speaks only in [`NodeId`]s, so this
//!   transport maps them to socket addresses via a table you register up front
//!   (and inbound datagrams are attributed by matching their source address). A
//!   production build would gossip addresses or resolve them dynamically.
//! * **One frame per datagram.** A frame must fit in a single UDP packet; very
//!   large clusters could exceed the MTU. Fragmentation / a stream fallback is
//!   future work.
//!
//! [`Transport`]: groupnet_transport::Transport

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use groupnet_core::NodeId;
use groupnet_transport::{Inbound, Transport};
use tokio::net::{ToSocketAddrs, UdpSocket};

/// A UDP-backed transport endpoint.
#[derive(Debug)]
pub struct UdpTransport {
    socket: Arc<UdpSocket>,
    local: NodeId,
    /// NodeId -> where to send. Interior mutability so peers can be registered
    /// after binding (e.g. once ephemeral ports are known).
    peers: RwLock<HashMap<NodeId, SocketAddr>>,
    /// The reverse map, to attribute inbound datagrams to a sender.
    by_addr: RwLock<HashMap<SocketAddr, NodeId>>,
}

impl UdpTransport {
    /// Binds a UDP socket for `local`. Register peers with
    /// [`register_peer`](Self::register_peer) before use.
    ///
    /// # Errors
    /// Propagates any socket bind error.
    pub async fn bind(local: NodeId, bind_addr: impl ToSocketAddrs) -> io::Result<Self> {
        let socket = UdpSocket::bind(bind_addr).await?;
        Ok(Self {
            socket: Arc::new(socket),
            local,
            peers: RwLock::new(HashMap::new()),
            by_addr: RwLock::new(HashMap::new()),
        })
    }

    /// This endpoint's local node id.
    #[must_use]
    pub fn local_id(&self) -> &NodeId {
        &self.local
    }

    /// The address the socket is bound to (useful when binding to an ephemeral
    /// port with `:0`).
    ///
    /// # Errors
    /// Propagates any socket error.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    /// Teaches this endpoint that `node` is reachable at `addr`.
    pub fn register_peer(&self, node: NodeId, addr: SocketAddr) {
        self.peers
            .write()
            .expect("peers lock poisoned")
            .insert(node.clone(), addr);
        self.by_addr
            .write()
            .expect("by_addr lock poisoned")
            .insert(addr, node);
    }
}

impl Transport for UdpTransport {
    type Error = io::Error;

    async fn send(&self, to: &NodeId, msg: &[u8]) -> io::Result<()> {
        // Resolve the address without holding the lock across the await.
        let addr = self
            .peers
            .read()
            .expect("peers lock poisoned")
            .get(to)
            .copied();
        if let Some(addr) = addr {
            // Best-effort: a send error is a drop, which the protocol tolerates.
            let _ = self.socket.send_to(msg, addr).await;
        }
        Ok(())
    }

    async fn recv(&self) -> io::Result<Inbound> {
        let mut buf = vec![0u8; 65_535];
        loop {
            let (n, addr) = self.socket.recv_from(&mut buf).await?;
            let from = self
                .by_addr
                .read()
                .expect("by_addr lock poisoned")
                .get(&addr)
                .cloned();
            if let Some(from) = from {
                buf.truncate(n);
                return Ok(Inbound { from, msg: buf });
            }
            // Datagram from an unregistered address — ignore and keep receiving.
        }
    }
}
