//! The address book every [`TcpBulkTransport`](super::TcpBulkTransport) mode
//! dials from: fixed socket addresses or bounded `host:port` names, resolved
//! afresh on each connection.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::RwLock;

use groupnet_core::NodeId;
use tokio::net::{TcpStream, lookup_host};

const MAX_HOST_ENDPOINT_BYTES: usize = 320;
const MAX_RESOLVED_ADDRESSES: usize = 8;

/// Why `register_peer_host` rejected a `host:port` endpoint. Converts into an
/// `InvalidInput` `io::Error` that keeps this error as its inner error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TcpHostEndpointError {
    /// The endpoint is longer than the host-endpoint bound.
    TooLong {
        /// The endpoint's length in bytes.
        len: usize,
        /// The longest endpoint accepted, in bytes.
        max: usize,
    },
    /// The endpoint contains ASCII whitespace or one of `/`, `\`, `@`, `#`.
    ForbiddenByte {
        /// The offending byte.
        byte: u8,
        /// Its offset in the endpoint.
        index: usize,
    },
    /// The endpoint has no `:port` suffix.
    MissingPort,
    /// The host before the final `:` is empty.
    EmptyHost,
    /// The port after the final `:` is not a decimal number from 1 to 65535.
    InvalidPort,
}

impl std::fmt::Display for TcpHostEndpointError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooLong { len, max } => {
                write!(
                    f,
                    "host endpoint is {len} bytes, above the {max}-byte bound"
                )
            }
            Self::ForbiddenByte { byte, index } => write!(
                f,
                "host endpoint byte {byte:#04x} at offset {index} is not allowed"
            ),
            Self::MissingPort => f.write_str("host endpoint requires a :port suffix"),
            Self::EmptyHost => f.write_str("host endpoint has an empty host"),
            Self::InvalidPort => f.write_str("host endpoint port must be 1 to 65535"),
        }
    }
}

impl std::error::Error for TcpHostEndpointError {}

impl From<TcpHostEndpointError> for io::Error {
    fn from(error: TcpHostEndpointError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}

/// Where one peer listens.
#[derive(Clone, Debug)]
pub(super) enum PeerEndpoint {
    Address(SocketAddr),
    Host(String),
}

impl PeerEndpoint {
    /// A checked `host:port` endpoint.
    pub(super) fn host(host: String) -> Result<Self, TcpHostEndpointError> {
        check_host_endpoint(&host)?;
        Ok(Self::Host(host))
    }

    /// Opens a TCP connection with Nagle's algorithm off. A host name is
    /// resolved now; its addresses are tried in order.
    pub(super) async fn open(&self) -> io::Result<TcpStream> {
        let sock = match self {
            Self::Address(addr) => TcpStream::connect(addr).await?,
            Self::Host(host) => {
                let addresses = lookup_host(host)
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
        sock.set_nodelay(true)?;
        Ok(sock)
    }
}

/// The registered peers of one endpoint, each with what its mode dials it
/// with.
#[derive(Debug)]
pub(super) struct Peers<V>(RwLock<HashMap<NodeId, V>>);

impl<V: Clone> Peers<V> {
    pub(super) fn new() -> Self {
        Self(RwLock::new(HashMap::new()))
    }

    /// Records `entry` for `node`, replacing any earlier one.
    ///
    /// # Panics
    /// If the address book was poisoned by a panic in another thread.
    pub(super) fn insert(&self, node: NodeId, entry: V) {
        self.0
            .write()
            .expect("peers lock poisoned")
            .insert(node, entry);
    }

    /// A copy of `node`'s entry, so no lock is held across a dial.
    ///
    /// # Panics
    /// If the address book was poisoned by a panic in another thread.
    pub(super) fn get(&self, node: &NodeId) -> io::Result<V> {
        self.0
            .read()
            .expect("peers lock poisoned")
            .get(node)
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "unknown peer"))
    }
}

/// Checks a `host:port` endpoint for `register_peer_host`.
fn check_host_endpoint(host: &str) -> Result<(), TcpHostEndpointError> {
    if host.len() > MAX_HOST_ENDPOINT_BYTES {
        return Err(TcpHostEndpointError::TooLong {
            len: host.len(),
            max: MAX_HOST_ENDPOINT_BYTES,
        });
    }
    let forbidden =
        |byte: u8| byte.is_ascii_whitespace() || matches!(byte, b'/' | b'\\' | b'@' | b'#');
    if let Some(index) = host.bytes().position(forbidden) {
        return Err(TcpHostEndpointError::ForbiddenByte {
            byte: host.as_bytes()[index],
            index,
        });
    }
    let Some((name, port)) = host.rsplit_once(':') else {
        return Err(TcpHostEndpointError::MissingPort);
    };
    if name.is_empty() {
        return Err(TcpHostEndpointError::EmptyHost);
    }
    if port.parse::<u16>().ok().is_none_or(|port| port == 0) {
        return Err(TcpHostEndpointError::InvalidPort);
    }
    Ok(())
}
