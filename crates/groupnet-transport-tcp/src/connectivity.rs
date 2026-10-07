//! Native TCP connection binding and physical-path diagnostics.

use super::{Backend, TcpMsgTransport};
use groupnet_core::NodeId;
use groupnet_transport::admission::SessionPeer;
use groupnet_transport_punch::{PeerPath, TcpConnection, TcpPunchConfig};
use std::{io, net::SocketAddr, sync::Arc};
use tokio::sync::watch;

/// Observes the live peers' physical paths of an admitted or native endpoint,
/// from [`TcpMsgTransport::path_changes`].
///
/// A change means a peer was admitted or withdrawn, or a native peer moved
/// between its direct and relay path; [`paths`](Self::paths) reads the current
/// view. Changes that land between two reads are never lost, only coalesced.
/// It reads the endpoint's own session registry and native path state; it
/// keeps no copy of either and does not extend the endpoint's lifetime.
#[derive(Debug)]
pub struct TcpPathChanges<'a> {
    transport: &'a TcpMsgTransport,
    sessions: watch::Receiver<Arc<Vec<SessionPeer>>>,
    native: Option<watch::Receiver<()>>,
}

impl TcpPathChanges<'_> {
    /// Waits until some peer's path may have changed since this watch was
    /// created or last returned.
    ///
    /// # Errors
    /// Returns `NotConnected` once the endpoint has shut down; sticky.
    pub async fn changed(&mut self) -> io::Result<()> {
        let Self {
            transport,
            sessions,
            native,
        } = self;
        let native = async {
            match native {
                Some(native) => native.changed().await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            biased;
            () = transport.closed() => Err(shut_down()),
            changed = sessions.changed() => changed.map_err(|_| shut_down()),
            changed = native => changed.map_err(|_| shut_down()),
        }
    }

    /// The current path of every live peer; empty after shutdown.
    ///
    /// # Panics
    /// If a direct session registry or connection pool lock was poisoned.
    #[must_use]
    pub fn paths(&self) -> Vec<(NodeId, PeerPath)> {
        self.transport
            .known_peers()
            .into_iter()
            .filter_map(|node| {
                let path = self.transport.path_to(&node)?;
                Some((node, path))
            })
            .collect()
    }
}

fn shut_down() -> io::Error {
    io::Error::new(io::ErrorKind::NotConnected, "tcp msg transport shut down")
}

impl TcpMsgTransport {
    /// Binds a native TCP connection with rendezvous admission, retained candidate
    /// sockets, direct-path maintenance, and relay fallback. No raw listener or
    /// pool is created. Clones share the connection and its live-session registry.
    ///
    /// # Errors
    /// Propagates native configuration, bind, authentication, admission, and timeout errors.
    pub async fn bind_connectivity(config: TcpPunchConfig) -> io::Result<Self> {
        let connection = TcpConnection::bind(config).await?;
        let address = connection.local_addr()?;
        Ok(Self {
            backend: Backend::Connectivity {
                connection,
                address,
            },
        })
    }

    /// Returns every retained listener/source bind for a running endpoint.
    /// Direct mode has exactly one listener.
    ///
    /// # Errors
    /// Returns `NotConnected` after shutdown.
    pub fn local_addrs(&self) -> io::Result<Vec<SocketAddr>> {
        match &self.backend {
            Backend::Direct(inner) => {
                ensure_direct_open(inner.tasks.stopped())?;
                Ok(vec![inner.local_addr])
            }
            Backend::Connectivity { connection, .. } => connection.local_addrs(),
        }
    }

    /// Returns locally gathered and explicitly configured native candidates.
    /// Direct mode returns its concrete listener address, or no candidates for
    /// an unspecified listener. Native relay-only mode returns no candidates.
    ///
    /// # Errors
    /// Returns `NotConnected` after shutdown.
    pub fn local_candidates(&self) -> io::Result<Vec<SocketAddr>> {
        match &self.backend {
            Backend::Direct(inner) => {
                ensure_direct_open(inner.tasks.stopped())?;
                Ok(if inner.local_addr.ip().is_unspecified() {
                    Vec::new()
                } else {
                    vec![inner.local_addr]
                })
            }
            Backend::Connectivity { connection, .. } => connection.local_candidates(),
        }
    }

    /// Returns the native registration source observed by rendezvous.
    /// Direct mode and shut-down connections return `None`.
    #[must_use]
    pub fn observed_addr(&self) -> Option<SocketAddr> {
        match &self.backend {
            Backend::Direct(_) => None,
            Backend::Connectivity { connection, .. } => connection.observed_addr(),
        }
    }

    /// Returns a validated native direct endpoint, not a relayed path or hint.
    /// Direct mode returns its registered dial address while running.
    ///
    /// # Panics
    /// If the direct address book lock was poisoned.
    #[must_use]
    pub fn direct_addr_to(&self, node: &NodeId) -> Option<SocketAddr> {
        match &self.backend {
            Backend::Direct(inner) => (!inner.tasks.stopped())
                .then(|| self.peer_addr(node))
                .flatten(),
            Backend::Connectivity { connection, .. } => connection.direct_addr_to(node),
        }
    }

    /// Returns the available native direct or relay path. Direct mode reports
    /// a direct path only for a live admitted identity or an active raw pool entry.
    ///
    /// # Panics
    /// If a direct session registry or connection pool lock was poisoned.
    #[must_use]
    pub fn path_to(&self, node: &NodeId) -> Option<PeerPath> {
        match &self.backend {
            Backend::Direct(inner) => {
                if inner.tasks.stopped() {
                    return None;
                }
                let live = if let Some(managed) = &inner.admission {
                    managed
                        .sessions
                        .subscribe()
                        .borrow()
                        .iter()
                        .any(|peer| &peer.node == node)
                } else {
                    inner
                        .pool
                        .lock()
                        .expect("pool lock poisoned")
                        .conns
                        .contains_key(node)
                };
                live.then_some(PeerPath::Direct)
            }
            Backend::Connectivity { connection, .. } => connection.path_to(node),
        }
    }

    /// Returns live admitted peers for native and admitted modes, or address-book
    /// identities for trusted raw mode. Shut-down endpoints return no peers.
    ///
    /// # Panics
    /// If a direct session registry or address-book lock was poisoned.
    #[must_use]
    pub fn known_peers(&self) -> Vec<NodeId> {
        match &self.backend {
            Backend::Direct(inner) => {
                if inner.tasks.stopped() {
                    return Vec::new();
                }
                if let Some(managed) = &inner.admission {
                    return managed
                        .sessions
                        .subscribe()
                        .borrow()
                        .iter()
                        .map(|peer| peer.node.clone())
                        .collect();
                }
                inner
                    .peers
                    .read()
                    .expect("peers lock poisoned")
                    .keys()
                    .cloned()
                    .collect()
            }
            Backend::Connectivity { connection, .. } => connection.known_peers(),
        }
    }

    /// Watches peer path changes of an admitted or native endpoint, backed by
    /// its live-session registry and, natively, the connection's path state.
    /// Raw trusted endpoints have no admitted peers to observe and return
    /// `None`; use [`sessions`](Self::sessions) for neighbor membership alone.
    #[must_use]
    pub fn path_changes(&self) -> Option<TcpPathChanges<'_>> {
        let sessions = self.sessions()?.subscribe();
        let native = match &self.backend {
            Backend::Direct(_) => None,
            Backend::Connectivity { connection, .. } => Some(connection.path_changes()),
        };
        Some(TcpPathChanges {
            transport: self,
            sessions,
            native,
        })
    }
}

fn ensure_direct_open(stopped: bool) -> io::Result<()> {
    if stopped { Err(shut_down()) } else { Ok(()) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use groupnet_testkit::cluster::eventually_within;
    use groupnet_transport::Transport;
    use groupnet_transport_punch::TcpRendezvous;
    use std::time::Duration;

    const SETTLE: Duration = Duration::from_secs(5);

    #[tokio::test]
    async fn native_hints_cannot_change_paths_and_bound_lifecycle_owns_shared_sessions() {
        let loopback: SocketAddr = "127.0.0.1:0".parse().expect("loopback");
        let relay = TcpRendezvous::bind_open(loopback)
            .await
            .expect("rendezvous");
        let rendezvous = relay.local_addr().expect("rendezvous address");
        let a_id = NodeId::new("adapter-a");
        let b_id = NodeId::new("adapter-b");
        let mut a_config = TcpPunchConfig::open(a_id.clone(), rendezvous);
        a_config.bind = loopback;
        let mut b_config = TcpPunchConfig::open(b_id.clone(), rendezvous);
        b_config.bind = loopback;
        let a = TcpMsgTransport::bind_connectivity(a_config)
            .await
            .expect("a");
        let b = TcpMsgTransport::bind_connectivity(b_config)
            .await
            .expect("b");
        let mut changes = a.path_changes().expect("native path watch");
        let sessions = a.sessions().expect("native registry");
        let live = sessions.subscribe();
        eventually_within("native adapter admission", SETTLE, || {
            live.borrow().iter().any(|peer| peer.node == b_id)
                && b.path_to(&a_id) == Some(PeerPath::Relay)
        })
        .await;
        let generation = live
            .borrow()
            .iter()
            .find(|peer| peer.node == b_id)
            .expect("admitted b")
            .id;
        let fake = NodeId::new("not-admitted");
        let fake_address = "127.0.0.1:9".parse().expect("fake address");
        a.register_peer(fake.clone(), fake_address);
        a.learn_peer(&fake, &fake_address.to_string());
        a.register_peer(b_id.clone(), fake_address);
        assert_eq!(a.peer_addr(&fake), None);
        assert_eq!(a.path_to(&fake), None);
        assert_eq!(a.path_to(&b_id), Some(PeerPath::Relay));
        assert_eq!(a.peer_addr(&b_id), None);
        assert_eq!(a.outbound_connections(), 0);
        assert_eq!(a.known_peers(), vec![b_id.clone()]);
        tokio::time::timeout(SETTLE, changes.changed())
            .await
            .expect("admission notifies the path watch")
            .expect("endpoint running");
        assert_eq!(changes.paths(), vec![(b_id.clone(), PeerPath::Relay)]);

        let bound = a.clone().into_bound_link(7);
        assert!(
            bound
                .sessions
                .as_ref()
                .expect("bound registry")
                .is_active(&b_id, generation)
        );
        a.send_admitted(&b_id, b"still relayed", Some(generation))
            .await
            .expect("send");
        let received = tokio::time::timeout(SETTLE, b.recv_admitted())
            .await
            .expect("receive deadline")
            .expect("receive");
        assert_eq!(received.packet.from, a_id);
        assert_eq!(received.packet.msg.as_ref(), b"still relayed");
        assert!(received.session.is_some());
        tokio::time::timeout(SETTLE, bound.driver.close())
            .await
            .expect("drain native endpoint");
        tokio::time::timeout(SETTLE, a.closed())
            .await
            .expect("native closed state observed");
        assert_eq!(
            changes.changed().await.expect_err("closed watch").kind(),
            io::ErrorKind::NotConnected
        );
        assert_eq!(changes.paths(), Vec::new());
        assert!(live.borrow().is_empty());
        assert!(!sessions.is_active(&b_id, generation));
        assert!(a.local_addrs().is_err());
        assert!(a.send(&b_id, b"closed").await.is_err());
        assert_eq!(a.path_to(&b_id), None);
        let _replacement = tokio::net::TcpListener::bind(a.local_addr())
            .await
            .expect("native listener released");
        b.close().await;
        relay.close().await;
    }

    #[tokio::test]
    async fn direct_path_changes_follow_admitted_sessions_until_shutdown() {
        use crate::{TcpAdmissionConfig, TcpMsgConfig};
        use groupnet_transport::admission::OpenAdmission;

        let raw = TcpMsgTransport::bind(NodeId::new("raw"), "127.0.0.1:0")
            .await
            .expect("raw");
        assert!(raw.path_changes().is_none(), "raw endpoints admit no peers");
        let admitted = |id: &str| {
            TcpMsgTransport::bind_admitted(
                NodeId::new(id),
                "127.0.0.1:0",
                TcpMsgConfig::default(),
                Arc::new(OpenAdmission),
                Vec::new(),
                TcpAdmissionConfig::default(),
            )
        };
        let a = admitted("paths-a").await.expect("a");
        let b = admitted("paths-b").await.expect("b");
        let mut changes = a.path_changes().expect("admitted path watch");
        assert_eq!(changes.paths(), Vec::new());
        b.register_peer(a.local_id().clone(), a.local_addr());
        b.connect_peer(a.local_id()).expect("connect");
        tokio::time::timeout(SETTLE, async {
            while changes.paths() != vec![(b.local_id().clone(), PeerPath::Direct)] {
                changes.changed().await.expect("endpoint running");
            }
        })
        .await
        .expect("admission observed through the path watch");
        a.shutdown();
        assert_eq!(
            changes.changed().await.expect_err("closed watch").kind(),
            io::ErrorKind::NotConnected
        );
        assert_eq!(changes.paths(), Vec::new());
        raw.close().await;
        a.close().await;
        b.close().await;
    }
}
