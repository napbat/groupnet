//! The managed-network builder validates every policy before binding a link.

use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use groupnet_core::NodeId;
use groupnet_network::{
    BoundLink, LinkProvider, Network, RouterConfig, RouterConfigError, TunnelConfig,
    tunnel::{TunnelLimits, TunnelLimitsError},
};
use groupnet_transport::link::LinkFuture;

use super::credentials;

/// Records whether `bind` ever reached it.
#[derive(Debug)]
struct BindProbe {
    peers: Vec<NodeId>,
    bound: Arc<AtomicBool>,
}

impl LinkProvider for BindProbe {
    fn peers(&self) -> &[NodeId] {
        &self.peers
    }

    fn bind(self: Box<Self>, _local: NodeId) -> LinkFuture<'static, io::Result<BoundLink>> {
        self.bound.store(true, Ordering::SeqCst);
        Box::pin(async { Err(io::Error::other("probe never binds")) })
    }
}

fn probe(peer: &str, bound: &Arc<AtomicBool>) -> BindProbe {
    BindProbe {
        peers: vec![NodeId::new(peer)],
        bound: bound.clone(),
    }
}

#[tokio::test]
async fn builder_lists_deduplicated_peers_in_identity_order() {
    let bound = Arc::new(AtomicBool::new(false));
    let builder = Network::builder().link(probe("peer-b", &bound)).links([
        Box::new(probe("peer-a", &bound)) as Box<dyn LinkProvider>,
        Box::new(probe("peer-b", &bound)),
    ]);
    assert_eq!(
        builder.peers(),
        vec![NodeId::new("peer-a"), NodeId::new("peer-b")]
    );
}

#[tokio::test]
async fn builder_rejects_invalid_router_and_tunnel_policy_before_binding_links() {
    let (identity, _, _) = credentials();
    let bound = Arc::new(AtomicBool::new(false));
    let error = Network::builder()
        .routing(RouterConfig {
            send_timeout: Duration::ZERO,
            ..RouterConfig::default()
        })
        .link(probe("peer", &bound))
        .bind(NodeId::new("local"))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<RouterConfigError>()),
        Some(&RouterConfigError::ZeroSendTimeout)
    );
    let error = Network::builder()
        .link(probe("peer", &bound))
        .tunnels(
            TunnelConfig::new(identity.identity, []).with_limits(TunnelLimits {
                setup_timeout: Duration::ZERO,
                ..TunnelLimits::default()
            }),
        )
        .bind(NodeId::new("local"))
        .await
        .unwrap_err();
    assert_eq!(
        error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<TunnelLimitsError>()),
        Some(&TunnelLimitsError::ZeroSetupTimeout)
    );
    assert!(!bound.load(Ordering::SeqCst), "validation precedes binding");
}
