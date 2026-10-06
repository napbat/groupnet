//! TCP control-plane link configuration and binding.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use groupnet_transport::admission::{AcceptedPeer, Admission, JoinRequest};

use groupnet_core::NodeId;
use groupnet_transport::link::{BoundLink, LinkFuture, LinkProvider, PeerEndpoint};

use crate::{TcpAdmissionConfig, TcpMsgConfig, TcpMsgTransport};

/// A TCP control-plane link with bootstrap endpoints and application admission.
///
/// Available with the `link` feature, which also activates `msg`. Binding owns
/// the listener and all session tasks; link shutdown cancels and drains them.
pub struct TcpLink {
    bind: SocketAddr,
    peers: Vec<NodeId>,
    addresses: Vec<SocketAddr>,
    cost: u32,
    admission: Option<Arc<dyn Admission>>,
    credential: Credential,
    admission_config: TcpAdmissionConfig,
}

impl std::fmt::Debug for TcpLink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpLink")
            .field("bind", &self.bind)
            .field("peers", &self.peers)
            .field("addresses", &self.addresses)
            .field("cost", &self.cost)
            .field("custom_admission", &self.admission.is_some())
            .field("credential", &self.credential)
            .field("admission_config", &self.admission_config)
            .finish()
    }
}

impl TcpLink {
    /// Configures the listener address and directly reachable peer endpoints.
    #[must_use]
    pub fn new(bind: SocketAddr, peers: Vec<PeerEndpoint<SocketAddr>>) -> Self {
        let (peers, addresses) = peers
            .into_iter()
            .map(|peer| (peer.node, peer.address))
            .unzip();
        Self {
            bind,
            peers,
            addresses,
            cost: 1,
            admission: None,
            credential: Credential(Vec::new()),
            admission_config: TcpAdmissionConfig::default(),
        }
    }

    /// Selects the application policy, enabling peers absent from the bootstrap list.
    /// `OpenAdmission` is an explicit unauthenticated membership choice.
    #[must_use]
    pub fn with_admission(mut self, admission: Arc<dyn Admission>) -> Self {
        self.admission = Some(admission);
        self
    }

    /// Supplies bounded opaque credentials presented to the remote admission policy.
    /// Credentials are omitted from debug output. Binding rejects oversized values.
    #[must_use]
    pub fn with_credentials(mut self, credential: Vec<u8>) -> Self {
        self.credential = Credential(credential);
        self
    }

    /// Sets handshake concurrency, established-peer capacity, and handshake deadline.
    #[must_use]
    pub fn with_admission_config(mut self, config: TcpAdmissionConfig) -> Self {
        self.admission_config = config;
        self
    }

    /// Sets the routing cost of this link (default: one).
    #[must_use]
    pub const fn with_cost(mut self, cost: u32) -> Self {
        self.cost = cost;
        self
    }
}

impl LinkProvider for TcpLink {
    fn peers(&self) -> &[NodeId] {
        &self.peers
    }

    fn bind(self: Box<Self>, local: NodeId) -> LinkFuture<'static, io::Result<BoundLink>> {
        Box::pin(async move {
            let Self {
                bind,
                peers,
                addresses,
                cost,
                admission,
                credential,
                admission_config,
            } = *self;
            let policy = admission.unwrap_or_else(|| Arc::new(ConfiguredAdmission(peers.clone())));
            let transport = TcpMsgTransport::bind_admitted(
                local,
                bind,
                TcpMsgConfig::default(),
                policy,
                credential.0,
                admission_config,
            )
            .await?;
            for (node, address) in peers.iter().zip(addresses) {
                transport.register_peer(node.clone(), address);
            }
            transport.bootstrap(peers);
            Ok(transport.into_bound_link(cost))
        })
    }
}

struct Credential(Vec<u8>);

impl std::fmt::Debug for Credential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Credential([redacted])")
    }
}

#[derive(Debug)]
struct ConfiguredAdmission(Vec<NodeId>);

impl Admission for ConfiguredAdmission {
    fn admit<'a>(&'a self, request: JoinRequest<'a>) -> LinkFuture<'a, io::Result<AcceptedPeer>> {
        Box::pin(async move {
            if self.0.contains(request.claimed) {
                Ok(AcceptedPeer {
                    node: request.claimed.clone(),
                })
            } else {
                Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "unconfigured TCP peer",
                ))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn provider_drains_listener_on_close() {
        let reservation = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve port");
        let address = reservation.local_addr().expect("address");
        drop(reservation);
        let peer = NodeId::new("tcp-peer");
        let link = TcpLink::new(
            address,
            vec![PeerEndpoint::new(
                peer,
                "127.0.0.1:12345".parse().expect("peer address"),
            )],
        )
        .with_cost(7);
        let provider: Box<dyn LinkProvider> = Box::new(link);
        let bound = provider.bind(NodeId::new("tcp-local")).await.expect("bind");
        assert!(std::net::TcpListener::bind(address).is_err());
        tokio::time::timeout(std::time::Duration::from_secs(5), bound.driver.close())
            .await
            .expect("drain listener");
        let _replacement = std::net::TcpListener::bind(address).expect("listener released");
    }

    #[tokio::test]
    async fn provider_propagates_listener_bind_errors() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let link = TcpLink::new(listener.local_addr().expect("address"), Vec::new());
        assert!(Box::new(link).bind(NodeId::new("tcp-local")).await.is_err());
    }

    #[tokio::test]
    async fn dropping_unregistered_link_cancels_listener() {
        let reservation = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve port");
        let address = reservation.local_addr().expect("address");
        drop(reservation);
        let bound = Box::new(TcpLink::new(address, Vec::new()))
            .bind(NodeId::new("tcp-local"))
            .await
            .expect("bind");
        drop(bound);
        let _replacement = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                match tokio::net::TcpListener::bind(address).await {
                    Ok(listener) => break listener,
                    Err(error) if error.kind() == io::ErrorKind::AddrInUse => {
                        tokio::task::yield_now().await;
                    }
                    Err(error) => panic!("rebind failed: {error}"),
                }
            }
        })
        .await
        .expect("dropped provider released listener");
    }

    async fn dial_finished(transport: &TcpMsgTransport) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while transport.outbound_connections() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dial attempt drained");
    }

    #[tokio::test]
    async fn static_admission_rejects_unknown_and_bootstrap_recovers_late_listener() {
        use groupnet_transport::Transport;
        use groupnet_transport::admission::OpenAdmission;
        use std::time::Duration;

        let a_id = NodeId::new("static-a");
        let b_id = NodeId::new("static-b");
        let reservation = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve");
        let b_addr = reservation.local_addr().expect("address");
        drop(reservation);
        let a = TcpMsgTransport::bind_admitted(
            a_id.clone(),
            "127.0.0.1:0",
            TcpMsgConfig::default(),
            Arc::new(ConfiguredAdmission(vec![b_id.clone()])),
            Vec::new(),
            TcpAdmissionConfig::default(),
        )
        .await
        .expect("a");
        a.register_peer(b_id.clone(), b_addr);
        a.connect_peer(&b_id)
            .expect("best-effort unavailable bootstrap");
        // Observe the refused attempt before bringing up the configured peer.
        dial_finished(&a).await;
        a.bootstrap(vec![b_id.clone()]);
        let b = TcpMsgTransport::bind_admitted(
            b_id.clone(),
            b_addr,
            TcpMsgConfig::default(),
            Arc::new(ConfiguredAdmission(vec![a_id])),
            Vec::new(),
            TcpAdmissionConfig::default(),
        )
        .await
        .expect("late b");
        tokio::time::timeout(Duration::from_secs(5), async {
            while a
                .sessions()
                .expect("sessions")
                .subscribe()
                .borrow()
                .is_empty()
                || b.sessions()
                    .expect("sessions")
                    .subscribe()
                    .borrow()
                    .is_empty()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("late bootstrap connected");
        b.send(a.local_id(), b"static topology")
            .await
            .expect("send");
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), a.recv())
                .await
                .expect("receive")
                .expect("frame")
                .msg,
            b"static topology"
        );
        let unknown = TcpMsgTransport::bind_admitted(
            NodeId::new("unknown"),
            "127.0.0.1:0",
            TcpMsgConfig::default(),
            Arc::new(OpenAdmission),
            Vec::new(),
            TcpAdmissionConfig::default(),
        )
        .await
        .expect("unknown");
        unknown.register_peer(a.local_id().clone(), a.local_addr());
        unknown.connect_peer(a.local_id()).expect("attempt");
        dial_finished(&unknown).await;
        assert!(
            unknown
                .sessions()
                .expect("sessions")
                .subscribe()
                .borrow()
                .is_empty()
        );
        assert_eq!(
            a.sessions().expect("sessions").subscribe().borrow().len(),
            1
        );
        unknown.close().await;
        a.close().await;
        b.close().await;
    }
}
