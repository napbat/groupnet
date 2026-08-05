//! The [`UdpTransport`] binding: one frame per datagram over a shared
//! socket, peers attributed by source address.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use groupnet_core::NodeId;
use groupnet_transport::{Inbound, Transport};
use tokio::net::{ToSocketAddrs, UdpSocket};

/// Receive-buffer size: the largest possible UDP payload (the length field is
/// 16 bits), so any single datagram is read in one `recv_from` with no
/// truncation.
const MAX_DATAGRAM: usize = 65_535;

/// Longest accepted sender id in a datagram's self-attribution prefix.
const MAX_ID_LEN: usize = 1024;

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

/// Every datagram carries `[u32 sender-id length][sender id][frame]`, so a
/// receiver can attribute — and learn the address of — a peer it has never
/// been told about. Without it, UDP attribution is address-only: a restarted
/// peer at a new address can dial out but nobody will accept its datagrams,
/// and the cluster wedges into one-way visibility.
///
/// The claimed id is trusted exactly as much as a source address was: this
/// is a cluster-internal fabric behind its own network boundary.
fn frame(local: &NodeId, msg: &[u8]) -> Vec<u8> {
    let id = local.as_str().as_bytes();
    let mut out = Vec::with_capacity(4 + id.len() + msg.len());
    out.extend_from_slice(&u32::try_from(id.len()).unwrap_or(u32::MAX).to_le_bytes());
    out.extend_from_slice(id);
    out.extend_from_slice(msg);
    out
}

/// Splits a datagram into `(sender, frame)`, or `None` when the prefix is
/// absent/garbled (a pre-prefix peer, or noise).
fn unframe(datagram: &[u8]) -> Option<(NodeId, &[u8])> {
    let len = usize::try_from(u32::from_le_bytes(datagram.get(0..4)?.try_into().ok()?)).ok()?;
    if len > MAX_ID_LEN {
        return None;
    }
    let id = std::str::from_utf8(datagram.get(4..4 + len)?).ok()?;
    Some((NodeId::new(id), datagram.get(4 + len..)?))
}

/// Shared endpoint state behind a single [`Arc`], so every clone of a
/// [`UdpTransport`] observes and performs registrations against the SAME
/// address book. This is what lets one handle be consumed by the node builder
/// while another is kept for out-of-band re-registration (e.g. periodic DNS
/// re-resolution of gossip seeds under pod-IP churn).
#[derive(Debug)]
struct Inner {
    local: NodeId,
    /// `NodeId` -> where to send. Interior mutability so peers can be registered
    /// after binding (e.g. once ephemeral ports are known).
    peers: RwLock<HashMap<NodeId, SocketAddr>>,
    /// The reverse map, to attribute inbound datagrams to a sender.
    by_addr: RwLock<HashMap<SocketAddr, NodeId>>,
}

/// A UDP-backed transport endpoint.
///
/// Cheap to [`Clone`]: clones share one socket and one address book, so a
/// [`register_peer`](Self::register_peer) through any handle is visible to all.
#[derive(Clone, Debug)]
pub struct UdpTransport {
    socket: Arc<UdpSocket>,
    inner: Arc<Inner>,
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
            inner: Arc::new(Inner {
                local,
                peers: RwLock::new(HashMap::new()),
                by_addr: RwLock::new(HashMap::new()),
            }),
        })
    }

    /// This endpoint's local node id.
    #[must_use]
    pub fn local_id(&self) -> &NodeId {
        &self.inner.local
    }

    /// The address the socket is bound to (useful when binding to an ephemeral
    /// port with `:0`).
    ///
    /// # Errors
    /// Propagates any socket error.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    /// Teaches this endpoint that `node` is reachable at `addr`, replacing any
    /// previous binding for `node`.
    ///
    /// When `node` was previously registered at a *different* address, that
    /// stale reverse (`by_addr`) entry is removed before the new one is
    /// inserted. The reverse map must never retain a dead address: a lingering
    /// entry grows the map without bound and can mis-attribute an inbound
    /// datagram once that address is reused by another node. Callable through
    /// any clone — all clones share one book.
    ///
    /// # Panics
    /// If either half of the address book was poisoned by a panic in another
    /// thread.
    pub fn register_peer(&self, node: NodeId, addr: SocketAddr) {
        // Take both locks (peers before by_addr — the only site that holds
        // both) so the forward and reverse maps update atomically.
        let mut peers = self.inner.peers.write().expect("peers lock poisoned");
        let mut by_addr = self.inner.by_addr.write().expect("by_addr lock poisoned");
        let stale = peers
            .insert(node.clone(), addr)
            .filter(|prev| *prev != addr);
        if let Some(prev) = stale {
            by_addr.remove(&prev);
        }
        by_addr.insert(addr, node);
    }
}

impl Transport for UdpTransport {
    type Error = io::Error;

    fn learn_peer(&self, node: &NodeId, addr: &str) {
        // An advertisement is a hint: register what parses, ignore the rest.
        if let Ok(addr) = addr.parse::<SocketAddr>() {
            self.register_peer(node.clone(), addr);
        }
    }

    async fn send(&self, to: &NodeId, msg: &[u8]) -> io::Result<()> {
        // Resolve the address without holding the lock across the await.
        let addr = self
            .inner
            .peers
            .read()
            .expect("peers lock poisoned")
            .get(to)
            .copied();
        if let Some(addr) = addr {
            // Best-effort: a send error is a drop, which the protocol tolerates.
            let _ = self
                .socket
                .send_to(&frame(&self.inner.local, msg), addr)
                .await;
        }
        Ok(())
    }

    async fn recv(&self) -> io::Result<Inbound> {
        let mut buf = vec![0u8; MAX_DATAGRAM];
        loop {
            let (n, addr) = match self.socket.recv_from(&mut buf).await {
                Ok(received) => received,
                Err(error) if retryable_recv_error(&error) => continue,
                Err(error) => return Err(error),
            };
            if let Some((from, msg)) = unframe(&buf[..n]) {
                // Self-attributed: learn where this peer speaks from, so the
                // reverse path works even for a peer nothing told us about
                // (a restart at a fresh address). Only touch the book when
                // the binding actually changed.
                let known = self
                    .inner
                    .peers
                    .read()
                    .expect("peers lock poisoned")
                    .get(&from)
                    .copied();
                if known != Some(addr) {
                    self.register_peer(from.clone(), addr);
                }
                let msg = msg.to_vec();
                return Ok(Inbound { from, msg });
            }
            // No usable prefix: fall back to address attribution, so a peer
            // still running a pre-prefix build is understood during a roll.
            let from = self
                .inner
                .by_addr
                .read()
                .expect("by_addr lock poisoned")
                .get(&addr)
                .cloned();
            if let Some(from) = from {
                let msg = buf[..n].to_vec();
                return Ok(Inbound { from, msg });
            }
            // Unattributable datagram — ignore and keep receiving.
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use groupnet_core::NodeId;
    use groupnet_transport::Transport;

    use super::{UdpTransport, retryable_recv_error};

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
        assert_eq!(inbound.msg, b"peer is now live");
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
    /// first datagram, and the stale reverse entry does not linger.
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
            receiver.inner.peers.read().expect("peers").get(&sender_id),
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

    /// Re-registering a node at a new address updates BOTH maps and drops the
    /// stale reverse entry.
    #[tokio::test]
    async fn reregister_updates_both_maps_and_drops_stale_reverse() {
        let t = bind_as("local").await;
        let peer = NodeId::new("peer");
        let old: SocketAddr = "127.0.0.1:9001".parse().expect("addr");
        let new: SocketAddr = "127.0.0.1:9002".parse().expect("addr");

        t.register_peer(peer.clone(), old);
        t.register_peer(peer.clone(), new);

        assert_eq!(t.inner.peers.read().expect("peers").get(&peer), Some(&new));
        let by_addr = t.inner.by_addr.read().expect("by_addr");
        assert_eq!(by_addr.get(&new), Some(&peer));
        assert!(
            !by_addr.contains_key(&old),
            "stale reverse entry lingered after re-registration"
        );
    }

    /// An inbound datagram from a RE-REGISTERED (new) address attributes to the
    /// node — the fresh reverse entry resolves, the stale one no longer can.
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
