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
    TunnelConfigError,
    tunnel::{PeerIdentity, TunnelLimits, TunnelLimitsError},
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

/// Binds `config` behind a probe link and returns the typed rejection.
async fn rejected(config: TunnelConfig, routing: RouterConfig) -> TunnelConfigError {
    let bound = Arc::new(AtomicBool::new(false));
    let error = Network::builder()
        .routing(routing)
        .link(probe("peer", &bound))
        .tunnels(config)
        .bind(NodeId::new("local"))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(!bound.load(Ordering::SeqCst), "validation precedes binding");
    error
        .into_inner()
        .and_then(|inner| inner.downcast::<TunnelConfigError>().ok())
        .map(|error| *error)
        .expect("typed tunnel configuration error")
}

#[tokio::test]
async fn builder_rejects_invalid_router_and_tunnel_policy_before_binding_links() {
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
    assert!(!bound.load(Ordering::SeqCst), "validation precedes binding");
    let (identity, _, _) = credentials();
    let limits = TunnelLimits {
        setup_timeout: Duration::ZERO,
        ..TunnelLimits::default()
    };
    let error = rejected(
        TunnelConfig {
            limits,
            ..TunnelConfig::new(identity.identity, [])
        },
        RouterConfig::default(),
    )
    .await;
    assert_eq!(
        error,
        TunnelConfigError::Limits(TunnelLimitsError::ZeroSetupTimeout)
    );
    assert!(std::error::Error::source(&error).is_some());
}

#[tokio::test]
async fn builder_rejects_tunnel_admission_and_envelopes_before_constructing_anything() {
    let pin = |name: &str| PeerIdentity::new(NodeId::new(name), &[1, 2, 3]).unwrap();
    let config = |peers: Vec<PeerIdentity>| TunnelConfig::new(credentials().0.identity, peers);
    assert_eq!(
        rejected(
            config(vec![pin("peer"), pin("other"), pin("peer")]),
            RouterConfig::default()
        )
        .await,
        TunnelConfigError::DuplicatePeer {
            node: NodeId::new("peer")
        }
    );
    assert_eq!(
        rejected(config(vec![pin("")]), RouterConfig::default()).await,
        TunnelConfigError::InvalidPeer {
            node: NodeId::new("")
        }
    );
    let mut crowded = config(vec![pin("a"), pin("b")]);
    crowded.limits.max_peers = std::num::NonZeroUsize::MIN;
    assert_eq!(
        rejected(crowded, RouterConfig::default()).await,
        TunnelConfigError::TooManyPeers
    );
    let small_envelope = RouterConfig {
        max_frame: std::num::NonZeroUsize::new(4096).unwrap(),
        ..RouterConfig::default()
    };
    assert_eq!(
        rejected(config(vec![pin("peer")]), small_envelope).await,
        TunnelConfigError::PayloadAboveRouterFrame {
            node: NodeId::new("peer")
        }
    );
}
