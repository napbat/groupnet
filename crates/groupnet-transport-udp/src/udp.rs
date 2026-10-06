//! The [`UdpTransport`] binding: raw self-attributed datagrams, or optional
//! session-fenced native connectivity over the connection's owned sockets.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use groupnet_core::NodeId;
use groupnet_transport::{Inbound, Transport};
use tokio::net::{ToSocketAddrs, UdpSocket};
use tokio::sync::Mutex;

mod framing;
use framing::{MAX_DATAGRAM, SendBuffer, unframe};

#[cfg(feature = "connectivity")]
use groupnet_transport_punch::{PeerPath, PunchConfig, UdpConnection};

/// Whether a receive error is a transient ICMP response rather than a socket
/// failure.
///
/// Windows reports an ICMP "port unreachable" from an earlier `send_to` as
/// `WSAECONNRESET` on the next `recv_from` of the same unconnected UDP socket.
/// Some other platforms surface the equivalent as `ConnectionRefused`. A seed
/// that has not bound its port yet is normal during rolling or ordered startup,
/// so neither error may permanently stop the transport's receive loop.
fn retryable_recv_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionRefused
    )
}

/// Shared endpoint state behind a single [`Arc`], so every clone of a
/// [`UdpTransport`] observes and performs registrations against the SAME
/// address book. This is what lets one handle be consumed by the node builder
/// while another is kept for out-of-band re-registration (e.g. periodic DNS
/// re-resolution of gossip seeds under pod-IP churn).
#[derive(Debug)]
struct Inner {
    socket: UdpSocket,
    local: NodeId,
    /// `NodeId` -> where to send. Interior mutability so peers can be registered
    /// after binding (e.g. once ephemeral ports are known).
    peers: RwLock<HashMap<NodeId, SocketAddr>>,
    /// The prefix and payload storage shared by all sending clones.
    send_buffer: Mutex<SendBuffer>,
    /// One reusable datagram scratch buffer for the endpoint's receive owner.
    receive_buffer: Mutex<Vec<u8>>,
}

#[derive(Clone, Debug)]
enum Backend {
    Direct(Arc<Inner>),
    #[cfg(feature = "connectivity")]
    Connectivity(UdpConnection),
}

/// A UDP-backed transport endpoint.
///
/// Cheap to [`Clone`]: raw clones share one socket and address book; connected
/// clones share the native connection's sockets, sessions and protocol tasks.
#[derive(Clone, Debug)]
pub struct UdpTransport {
    backend: Backend,
}

impl UdpTransport {
    /// Binds a UDP socket for `local`. Register peers with
    /// [`register_peer`](Self::register_peer) before use.
    ///
    /// # Errors
    /// Propagates any socket bind error or rejects an oversized local identity.
    pub async fn bind(local: NodeId, bind_addr: impl ToSocketAddrs) -> io::Result<Self> {
        let send_buffer = SendBuffer::new(&local)?;
        let socket = UdpSocket::bind(bind_addr).await?;
        Ok(Self {
            backend: Backend::Direct(Arc::new(Inner {
                socket,
                local,
                peers: RwLock::new(HashMap::new()),
                send_buffer: Mutex::new(send_buffer),
                receive_buffer: Mutex::new(vec![0; MAX_DATAGRAM]),
            })),
        })
    }

    /// Binds native connectivity using the connection's actual leased sockets.
    /// Peer admission, candidate checks and relay fallback are session-fenced;
    /// address advertisements and [`register_peer`](Self::register_peer) have no effect.
    ///
    /// # Errors
    /// Propagates invalid configuration, admission and socket errors.
    #[cfg(feature = "connectivity")]
    pub async fn bind_connectivity(config: PunchConfig) -> io::Result<Self> {
        Ok(Self {
            backend: Backend::Connectivity(UdpConnection::bind(config).await?),
        })
    }

    /// This endpoint's local node id.
    #[must_use]
    pub fn local_id(&self) -> &NodeId {
        match &self.backend {
            Backend::Direct(inner) => &inner.local,
            #[cfg(feature = "connectivity")]
            Backend::Connectivity(connection) => connection.local_id(),
        }
    }

    /// The address the socket is bound to (useful when binding to an ephemeral
    /// port with `:0`).
    ///
    /// # Errors
    /// Propagates socket errors, or `NotConnected` after connection shutdown.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        match &self.backend {
            Backend::Direct(inner) => inner.socket.local_addr(),
            #[cfg(feature = "connectivity")]
            Backend::Connectivity(connection) => connection.local_addr(),
        }
    }

    /// Returns registered raw peers, or the connection's configured/live peers.
    ///
    /// # Panics
    /// If the raw address book was poisoned by a panic in another thread.
    #[must_use]
    pub fn known_peers(&self) -> Vec<NodeId> {
        match &self.backend {
            Backend::Direct(inner) => inner
                .peers
                .read()
                .expect("peers lock poisoned")
                .keys()
                .cloned()
                .collect(),
            #[cfg(feature = "connectivity")]
            Backend::Connectivity(connection) => connection.known_peers(),
        }
    }

    /// Returns every leased socket's actual bind address (one in raw mode).
    ///
    /// # Errors
    /// Propagates socket errors, or `NotConnected` after connection shutdown.
    #[cfg(feature = "connectivity")]
    pub fn local_addrs(&self) -> io::Result<Vec<SocketAddr>> {
        match &self.backend {
            Backend::Direct(inner) => Ok(vec![inner.socket.local_addr()?]),
            Backend::Connectivity(connection) => connection.local_addrs(),
        }
    }

    /// Returns advertised native candidates, or the raw socket's bind address.
    /// Relay-only connections return no candidates.
    ///
    /// # Errors
    /// Propagates socket errors, or `NotConnected` after connection shutdown.
    #[cfg(feature = "connectivity")]
    pub fn local_candidates(&self) -> io::Result<Vec<SocketAddr>> {
        match &self.backend {
            Backend::Direct(inner) => Ok(vec![inner.socket.local_addr()?]),
            Backend::Connectivity(connection) => connection.local_candidates(),
        }
    }

    /// Returns the rendezvous-observed primary mapping, if disclosed and live.
    /// Raw endpoints have no rendezvous mapping.
    #[cfg(feature = "connectivity")]
    #[must_use]
    pub fn observed_addr(&self) -> Option<SocketAddr> {
        match &self.backend {
            Backend::Direct(_) => None,
            Backend::Connectivity(connection) => connection.observed_addr(),
        }
    }

    /// Returns the validated live direct address, or a raw registered address.
    ///
    /// # Panics
    /// If the raw address book was poisoned by a panic in another thread.
    #[cfg(feature = "connectivity")]
    #[must_use]
    pub fn direct_addr_to(&self, node: &NodeId) -> Option<SocketAddr> {
        match &self.backend {
            Backend::Direct(inner) => inner
                .peers
                .read()
                .expect("peers lock poisoned")
                .get(node)
                .copied(),
            Backend::Connectivity(connection) => connection.direct_addr_to(node),
        }
    }

    /// Returns the connection's live path, or `Direct` for a raw registered peer.
    /// A raw registration is an address hint, not proof of reachability.
    ///
    /// # Panics
    /// If the raw address book was poisoned by a panic in another thread.
    #[cfg(feature = "connectivity")]
    #[must_use]
    pub fn path_to(&self, node: &NodeId) -> Option<PeerPath> {
        match &self.backend {
            Backend::Direct(_) => self.direct_addr_to(node).map(|_| PeerPath::Direct),
            Backend::Connectivity(connection) => connection.path_to(node),
        }
    }

    /// Returns the connection's shared live session registry, or `None` in raw mode.
    #[cfg(feature = "link")]
    #[must_use]
    pub fn sessions(&self) -> Option<groupnet_transport::admission::SessionRegistry> {
        match &self.backend {
            Backend::Direct(_) => None,
            #[cfg(feature = "connectivity")]
            Backend::Connectivity(connection) => Some(connection.sessions()),
        }
    }

    /// Transfers the live endpoint into a managed link without rebinding.
    /// Connectivity mode retains its session registry, task lifecycle and native
    /// message MTU; raw mode uses the registered peers as static neighbors.
    ///
    /// # Panics
    /// If the raw address book was poisoned by a panic in another thread.
    #[cfg(feature = "link")]
    #[must_use]
    pub fn into_bound_link(self, cost: u32) -> groupnet_transport::link::BoundLink {
        use groupnet_transport::link::{BoundLink, LinkConfig};

        let mut config = LinkConfig::new(self.known_peers());
        config.cost = cost;
        match &self.backend {
            Backend::Direct(_) => BoundLink::new(self, config),
            #[cfg(feature = "connectivity")]
            Backend::Connectivity(connection) => {
                config.mtu = groupnet_transport_punch::MAX_MESSAGE;
                let sessions = connection.sessions();
                let lifecycle = Arc::new(ConnectivityLifecycle(connection.clone()));
                BoundLink::new(self, config)
                    .with_lifecycle(lifecycle)
                    .with_sessions(sessions)
            }
        }
    }

    /// Teaches this endpoint that `node` is reachable at `addr`, replacing any
    /// previous binding for `node`. Callable through any clone — all clones
    /// share one book.
    ///
    /// Has no effect in connectivity mode: only native admission and validated
    /// candidate checks may establish or change a connected peer's path.
    ///
    /// # Panics
    /// If the address book was poisoned by a panic in another thread.
    pub fn register_peer(&self, node: NodeId, addr: SocketAddr) {
        #[cfg(not(feature = "connectivity"))]
        let Backend::Direct(inner) = &self.backend;
        #[cfg(feature = "connectivity")]
        let inner = match &self.backend {
            Backend::Direct(inner) => inner,
            Backend::Connectivity(_) => return,
        };
        inner
            .peers
            .write()
            .expect("peers lock poisoned")
            .insert(node, addr);
    }

    /// Returns the owned native connection when connectivity is enabled.
    ///
    /// Closing this shared resource cancels native I/O and withdraws sessions
    /// from every adapter clone. Raw socket endpoints return `None`; their
    /// socket lifetime is controlled by dropping all owning clones.
    #[cfg(feature = "connectivity")]
    #[must_use]
    pub fn connection(&self) -> Option<&UdpConnection> {
        match &self.backend {
            Backend::Direct(_) => None,
            Backend::Connectivity(connection) => Some(connection),
        }
    }
}

impl Transport for UdpTransport {
    type Error = io::Error;

    fn learn_peer(&self, node: &NodeId, addr: &str) {
        match &self.backend {
            Backend::Direct(_) => {
                // Raw advertisements are hints: register only parseable addresses.
                if let Ok(addr) = addr.parse::<SocketAddr>() {
                    self.register_peer(node.clone(), addr);
                }
            }
            #[cfg(feature = "connectivity")]
            Backend::Connectivity(_) => {}
        }
    }

    async fn send(&self, to: &NodeId, msg: &[u8]) -> io::Result<()> {
        #[cfg(not(feature = "connectivity"))]
        let Backend::Direct(inner) = &self.backend;
        #[cfg(feature = "connectivity")]
        let inner = match &self.backend {
            Backend::Direct(inner) => inner,
            Backend::Connectivity(connection) => return connection.send(to, msg).await,
        };
        // Resolve the address without holding the lock across the await.
        let addr = inner
            .peers
            .read()
            .expect("peers lock poisoned")
            .get(to)
            .copied();
        if let Some(addr) = addr {
            // Hold exclusive buffer ownership through send completion. Reuse
            // its allocation and immutable identity prefix across datagrams.
            let mut buffer = inner.send_buffer.lock().await;
            let datagram = buffer.frame(msg)?;
            // Best-effort: a socket error is a drop, which the protocol tolerates.
            let _ = inner.socket.send_to(datagram, addr).await;
        }
        Ok(())
    }

    #[cfg(feature = "link")]
    async fn send_admitted(
        &self,
        to: &NodeId,
        msg: &[u8],
        session: Option<groupnet_transport::admission::SessionId>,
    ) -> io::Result<()> {
        match &self.backend {
            Backend::Direct(_) => {
                if session.is_none() {
                    self.send(to, msg).await?;
                }
                Ok(())
            }
            #[cfg(feature = "connectivity")]
            Backend::Connectivity(connection) => connection.send_admitted(to, msg, session).await,
        }
    }

    #[cfg(feature = "link")]
    async fn send_owned_admitted(
        &self,
        to: &NodeId,
        msg: Bytes,
        session: Option<groupnet_transport::admission::SessionId>,
    ) -> io::Result<()> {
        match &self.backend {
            Backend::Direct(_) => {
                if session.is_none() {
                    self.send(to, &msg).await?;
                }
                Ok(())
            }
            #[cfg(feature = "connectivity")]
            Backend::Connectivity(connection) => {
                connection.send_owned_admitted(to, msg, session).await
            }
        }
    }

    async fn recv(&self) -> io::Result<Inbound> {
        #[cfg(not(feature = "connectivity"))]
        let Backend::Direct(inner) = &self.backend;
        #[cfg(feature = "connectivity")]
        let inner = match &self.backend {
            Backend::Direct(inner) => inner,
            Backend::Connectivity(connection) => return connection.recv().await,
        };
        let mut buf = inner.receive_buffer.lock().await;
        loop {
            let (n, addr) = match inner.socket.recv_from(buf.as_mut_slice()).await {
                Ok(received) => received,
                Err(error) if retryable_recv_error(&error) => continue,
                Err(error) => return Err(error),
            };
            if let Some((from, msg)) = unframe(&buf[..n]) {
                // Self-attributed: learn where this peer speaks from, so the
                // reverse path works even for a peer nothing told us about
                // (a restart at a fresh address). Only touch the book when
                // the binding actually changed.
                let known = inner
                    .peers
                    .read()
                    .expect("peers lock poisoned")
                    .get(&from)
                    .copied();
                if known != Some(addr) {
                    self.register_peer(from.clone(), addr);
                }
                let msg = Bytes::copy_from_slice(msg);
                return Ok(Inbound { from, msg });
            }
            // Malformed or unattributable datagram — ignore and keep receiving.
        }
    }

    #[cfg(feature = "link")]
    async fn recv_admitted(&self) -> io::Result<groupnet_transport::link::AdmittedInbound> {
        match &self.backend {
            Backend::Direct(_) => Ok(groupnet_transport::link::AdmittedInbound {
                packet: self.recv().await?,
                session: None,
            }),
            #[cfg(feature = "connectivity")]
            Backend::Connectivity(connection) => connection.recv_admitted().await,
        }
    }
}

#[cfg(feature = "connectivity")]
#[derive(Debug)]
struct ConnectivityLifecycle(UdpConnection);

#[cfg(feature = "connectivity")]
impl groupnet_transport::link::LinkLifecycle for ConnectivityLifecycle {
    fn shutdown(&self) {
        self.0.shutdown();
    }

    fn close(&self) -> groupnet_transport::link::LinkFuture<'_, ()> {
        Box::pin(self.0.close())
    }
}

#[cfg(all(test, feature = "connectivity"))]
mod connectivity_tests;

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use groupnet_core::NodeId;
    use groupnet_transport::Transport;

    use super::{UdpTransport, retryable_recv_error};

    fn raw_inner(transport: &UdpTransport) -> &super::Inner {
        match &transport.backend {
            super::Backend::Direct(inner) => inner,
            #[cfg(feature = "connectivity")]
            super::Backend::Connectivity(_) => panic!("expected a raw endpoint"),
        }
    }

    /// Bind a loopback endpoint on an ephemeral port under the given id.
    async fn bind_as(id: &str) -> UdpTransport {
        UdpTransport::bind(NodeId::new(id), "127.0.0.1:0")
            .await
            .expect("bind")
    }

    #[test]
    fn only_transient_icmp_receive_errors_are_retryable() {
        assert!(retryable_recv_error(&std::io::Error::from(
            std::io::ErrorKind::ConnectionReset
        )));
        assert!(retryable_recv_error(&std::io::Error::from(
            std::io::ErrorKind::ConnectionRefused
        )));
        assert!(!retryable_recv_error(&std::io::Error::from(
            std::io::ErrorKind::PermissionDenied
        )));
        assert!(!retryable_recv_error(&std::io::Error::from(
            std::io::ErrorKind::AddrNotAvailable
        )));
    }

    /// An ordered-startup seed can be absent for the first probe. On Windows
    /// that failed send produces `WSAECONNRESET` on `recv_from`; the receiver
    /// must stay alive and accept the peer once it binds.
    #[tokio::test]
    async fn receiver_survives_a_peer_that_binds_after_the_first_probe() {
        let receiver_id = NodeId::new("receiver");
        let sender_id = NodeId::new("delayed-sender");
        let receiver = bind_as(receiver_id.as_str()).await;

        let reservation = std::net::UdpSocket::bind("127.0.0.1:0").expect("reserve port");
        let delayed_addr = reservation.local_addr().expect("reserved address");
        drop(reservation);
        receiver.register_peer(sender_id.clone(), delayed_addr);

        let recv = tokio::spawn({
            let receiver = receiver.clone();
            async move { receiver.recv().await }
        });
        receiver
            .send(&sender_id, b"probe before bind")
            .await
            .expect("initial probe");
        tokio::time::sleep(Duration::from_millis(100)).await;

        let sender = UdpTransport::bind(sender_id.clone(), delayed_addr)
            .await
            .expect("bind delayed sender");
        sender.register_peer(
            receiver_id.clone(),
            receiver.local_addr().expect("receiver address"),
        );
        sender
            .send(&receiver_id, b"peer is now live")
            .await
            .expect("send after bind");

        let inbound = tokio::time::timeout(Duration::from_secs(2), recv)
            .await
            .expect("receiver timed out")
            .expect("receive task panicked")
            .expect("receiver stopped after transient ICMP error");
        assert_eq!(inbound.from, sender_id);
        assert_eq!(inbound.msg.as_ref(), b"peer is now live");
    }

    /// A clone shares the address book: a peer registered through the clone is
    /// reachable when sending through the original handle.
    #[tokio::test]
    async fn clone_shares_address_book() {
        let sender = bind_as("sender").await;
        let receiver = bind_as("receiver").await;
        let sender_id = NodeId::new("sender");
        let receiver_id = NodeId::new("receiver");

        // Register the receiver's address ONLY through a clone; the original
        // must observe it (shared book) to reach the receiver.
        sender
            .clone()
            .register_peer(receiver_id.clone(), receiver.local_addr().expect("addr"));
        // So the receiver can attribute the inbound datagram back to us.
        receiver.register_peer(sender_id.clone(), sender.local_addr().expect("addr"));

        sender.send(&receiver_id, b"hello").await.expect("send");

        let inbound = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .expect("recv timed out")
            .expect("recv");
        assert_eq!(inbound.from, sender_id);
        assert_eq!(inbound.msg, b"hello".to_vec());
    }

    /// The wedge this prefix exists to prevent: a receiver that has NEVER
    /// been told about a sender still attributes its datagram, learns its
    /// address, and can reply — no prior registration in that direction.
    #[tokio::test]
    async fn unknown_sender_is_attributed_and_learned() {
        let sender = bind_as("attr-sender").await;
        let receiver = bind_as("attr-receiver").await;
        let sender_id = NodeId::new("attr-sender");
        let receiver_id = NodeId::new("attr-receiver");

        // ONLY the sender knows where to dial; the receiver's book is empty.
        sender.register_peer(receiver_id.clone(), receiver.local_addr().expect("addr"));
        sender.send(&receiver_id, b"hello").await.expect("send");

        let inbound = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .expect("recv timed out")
            .expect("recv");
        assert_eq!(inbound.from, sender_id, "attributed by the datagram itself");
        assert_eq!(
            inbound.msg,
            b"hello".to_vec(),
            "payload excludes the prefix"
        );

        // The reverse path now works without anyone registering it.
        receiver.send(&sender_id, b"reply").await.expect("send");
        let back = tokio::time::timeout(Duration::from_secs(2), sender.recv())
            .await
            .expect("recv timed out")
            .expect("recv");
        assert_eq!(back.from, receiver_id);
        assert_eq!(back.msg, b"reply".to_vec());
    }

    /// A moved peer (restart at a fresh address) re-teaches the book on its
    /// first datagram, replacing the stale address.
    #[tokio::test]
    async fn a_moved_sender_rebinds_the_book() {
        let receiver = bind_as("move-receiver").await;
        let sender_id = NodeId::new("move-sender");
        let receiver_id = NodeId::new("move-receiver");

        // The receiver holds a STALE address for the sender.
        receiver.register_peer(sender_id.clone(), "127.0.0.1:9".parse().expect("addr"));

        let sender = bind_as("move-sender").await;
        sender.register_peer(receiver_id.clone(), receiver.local_addr().expect("addr"));
        sender.send(&receiver_id, b"i moved").await.expect("send");

        let inbound = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .expect("recv timed out")
            .expect("recv");
        assert_eq!(inbound.from, sender_id);
        assert_eq!(
            raw_inner(&receiver)
                .peers
                .read()
                .expect("peers")
                .get(&sender_id),
            Some(&sender.local_addr().expect("addr")),
            "the book rebound to the sender's live address"
        );
    }

    /// A gossiped advertisement teaches the book exactly like registration —
    /// and garbage is ignored, never an error (an advertisement is a hint).
    #[tokio::test]
    async fn learn_peer_registers_parseable_advertisements() {
        let sender = bind_as("adv-sender").await;
        let receiver = bind_as("adv-receiver").await;
        let sender_id = NodeId::new("adv-sender");
        let receiver_id = NodeId::new("adv-receiver");

        let receiver_addr = receiver.local_addr().expect("addr").to_string();
        sender.learn_peer(&receiver_id, &receiver_addr);
        let sender_addr = sender.local_addr().expect("addr").to_string();
        receiver.learn_peer(&sender_id, &sender_addr);
        sender.learn_peer(&NodeId::new("junk"), "not-an-address");

        sender
            .send(&receiver_id, b"via-gossip")
            .await
            .expect("send");
        let inbound = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .expect("recv timed out")
            .expect("recv");
        assert_eq!(inbound.from, sender_id);
        assert_eq!(inbound.msg, b"via-gossip".to_vec());
    }

    /// Re-registering a node replaces the previous address without retaining
    /// an additional peer entry.
    #[tokio::test]
    async fn reregister_replaces_stale_address() {
        let t = bind_as("local").await;
        let peer = NodeId::new("peer");
        let old: SocketAddr = "127.0.0.1:9001".parse().expect("addr");
        let new: SocketAddr = "127.0.0.1:9002".parse().expect("addr");

        t.register_peer(peer.clone(), old);
        t.register_peer(peer.clone(), new);

        assert_eq!(
            raw_inner(&t).peers.read().expect("peers").get(&peer),
            Some(&new)
        );
        assert_eq!(raw_inner(&t).peers.read().expect("peers").len(), 1);
    }

    /// An inbound datagram from a re-registered address still attributes to the
    /// sender identified by its prefix.
    #[tokio::test]
    async fn inbound_from_new_address_attributes() {
        let receiver = bind_as("receiver").await;
        let sender = bind_as("sender").await;
        let sender_id = NodeId::new("sender");
        let receiver_id = NodeId::new("receiver");

        // Register the sender at a stale address first, then re-resolve to its
        // real one (as the seed re-resolver does on a pod-IP change).
        let stale: SocketAddr = "127.0.0.1:9".parse().expect("addr");
        receiver.register_peer(sender_id.clone(), stale);
        receiver.register_peer(sender_id.clone(), sender.local_addr().expect("addr"));

        sender.register_peer(receiver_id.clone(), receiver.local_addr().expect("addr"));
        sender.send(&receiver_id, b"ping").await.expect("send");

        let inbound = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .expect("recv timed out")
            .expect("recv");
        assert_eq!(inbound.from, sender_id);
        assert_eq!(inbound.msg, b"ping".to_vec());
    }
}

#[cfg(test)]
mod packet_tests;
