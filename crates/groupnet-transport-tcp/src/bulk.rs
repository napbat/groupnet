//! Data-plane streams over TCP: [`TcpBulkTransport`], a [`BulkTransport`] for
//! reliable, ordered byte streams (replication, snapshot transfer).
//!
//! A [`tokio::net::TcpStream`] is already `AsyncRead + AsyncWrite`; the only
//! glue is `tokio_util::compat` to present it as the runtime-agnostic
//! `futures-io` stream the trait asks for, plus a one-line node-id handshake so
//! the accepting side can attribute the connection.
//!
//! Peer endpoints may be fixed socket addresses or bounded host:port names.
//! Hostnames are resolved afresh on each connection so pod address churn does
//! not change the exact `NodeId` used by the bulk protocol.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::RwLock;

use groupnet_core::NodeId;
use groupnet_transport::bulk::BulkTransport;
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs, lookup_host};
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

use crate::handshake::{read_id, write_id};

/// A TCP-backed data-plane transport endpoint.
#[derive(Debug)]
pub struct TcpBulkTransport {
    local: NodeId,
    listener: TcpListener,
    peers: RwLock<HashMap<NodeId, PeerEndpoint>>,
}

const MAX_HOST_ENDPOINT_BYTES: usize = 320;
const MAX_RESOLVED_ADDRESSES: usize = 8;

#[derive(Clone, Debug)]
enum PeerEndpoint {
    Address(SocketAddr),
    Host(String),
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
    ///
    /// # Panics
    /// If the address book was poisoned by a panic in another thread.
    pub fn register_peer(&self, node: NodeId, addr: SocketAddr) {
        self.peers
            .write()
            .expect("peers lock poisoned")
            .insert(node, PeerEndpoint::Address(addr));
    }

    /// Registers a bounded host:port endpoint, resolved anew on every connect.
    /// The address book is liveness routing only; the given `NodeId` remains the
    /// bulk protocol's exact peer identity.
    ///
    /// # Errors
    /// Rejects an empty, malformed, or overlong host:port endpoint.
    ///
    /// # Panics
    /// If the address book was poisoned by a panic in another thread.
    pub fn register_peer_host(&self, node: NodeId, host: String) -> io::Result<()> {
        let Some((name, port)) = host.rsplit_once(':') else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "host requires port",
            ));
        };
        if host.is_empty()
            || host.len() > MAX_HOST_ENDPOINT_BYTES
            || name.is_empty()
            || port.parse::<u16>().ok().is_none_or(|port| port == 0)
            || host.bytes().any(|byte| {
                byte.is_ascii_whitespace() || matches!(byte, b'/' | b'\\' | b'@' | b'#')
            })
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid host endpoint",
            ));
        }
        self.peers
            .write()
            .expect("peers lock poisoned")
            .insert(node, PeerEndpoint::Host(host));
        Ok(())
    }
}

impl BulkTransport for TcpBulkTransport {
    type Error = io::Error;
    type Stream = Compat<TcpStream>;

    async fn connect(&self, to: &NodeId) -> io::Result<Self::Stream> {
        // Resolve without holding the lock across the await.
        let endpoint = self
            .peers
            .read()
            .expect("peers lock poisoned")
            .get(to)
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "unknown peer"))?;
        let mut sock = match endpoint {
            PeerEndpoint::Address(addr) => TcpStream::connect(addr).await?,
            PeerEndpoint::Host(host) => {
                let addresses = lookup_host(&host)
                    .await?
                    .take(MAX_RESOLVED_ADDRESSES + 1)
                    .collect::<Vec<_>>();
                if addresses.is_empty() || addresses.len() > MAX_RESOLVED_ADDRESSES {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "resolved address count outside bound",
                    ));
                }
                let mut last = None;
                let mut connected = None;
                for address in addresses {
                    match TcpStream::connect(address).await {
                        Ok(stream) => {
                            connected = Some(stream);
                            break;
                        }
                        Err(error) => last = Some(error),
                    }
                }
                connected.ok_or_else(|| {
                    last.unwrap_or_else(|| {
                        io::Error::new(io::ErrorKind::NotFound, "no reachable peer address")
                    })
                })?
            }
        };
        write_id(&mut sock, &self.local).await?;
        Ok(sock.compat())
    }

    async fn accept(&self) -> io::Result<(NodeId, Self::Stream)> {
        let (mut sock, _addr) = self.listener.accept().await?;
        let from = read_id(&mut sock).await?;
        Ok((from, sock.compat()))
    }
}
