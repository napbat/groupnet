//! TCP control-plane link configuration and binding.

use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::sync::Arc;

use groupnet_core::NodeId;
use groupnet_transport::admission::{AcceptedPeer, Admission, AdmissionError, JoinRequest};
#[cfg(feature = "connectivity")]
use groupnet_transport::link::LinkBindError;
use groupnet_transport::link::{BoundLink, LinkConfig, LinkFuture, LinkProvider, PeerEndpoint};

use crate::{TcpAdmission, TcpAdmissionConfig, TcpMsgConfig, TcpMsgTransport};

/// A TCP control-plane link, registered with a router through [`LinkProvider`].
///
/// Available with the `link` feature, which also activates `msg`. Binding owns
/// the listener and all session tasks; link shutdown cancels and drains them.
///
/// The mode parameter makes each link's settings explicit:
///
/// * [`TcpLink::new`] builds a direct link (`TcpLink<TcpDirect>`, the
///   default) with bootstrap endpoints, application admission, credentials
///   and message tuning;
/// * with `connectivity`, `TcpLink::connectivity` builds a native link
///   (`TcpLink<TcpConnectivity>`) whose admission, credential, discovery and
///   path policy all come from its `TcpPunchConfig`. Direct-only setters do
///   not exist on it, so a setting the native link would ignore cannot be
///   written.
///
/// Both modes are this one link type; no separate link is registered.
///
/// # Validation
///
/// Setters only store their values. Binding is the one validation point: it
/// fails with `InvalidInput` carrying the typed cause
/// ([`LinkConfigError`](groupnet_transport::link::LinkConfigError) for the
/// configured peers, [`TcpMsgConfigError`](crate::TcpMsgConfigError),
/// [`TcpAdmissionError`](crate::TcpAdmissionError), the local
/// [`NodeIdError`](crate::NodeIdError), or for a native link
/// [`LinkBindError`](groupnet_transport::link::LinkBindError) when its
/// configured local identity is not the binding node's, and the native
/// configuration's error) before opening any socket or listener. Preflight
/// with [`LinkConfig::validate_for`], [`TcpMsgConfig::validate`] and
/// [`TcpAdmission::validate`].
#[cfg_attr(
    feature = "connectivity",
    doc = r#"
# Modes

Direct-only settings are rejected at compile time on a native link:

```compile_fail
use groupnet_core::NodeId;
use groupnet_transport_tcp::{NetworkKey, PeerPolicy, TcpLink, TcpPunchConfig};

let config = TcpPunchConfig::new(
    NodeId::new("a"),
    "127.0.0.1:7000".parse().unwrap(),
    Some(NetworkKey::from_bytes([7; 32])),
    PeerPolicy::Static(vec![NodeId::new("b")]),
);
let _ = TcpLink::connectivity(config).with_credentials(b"ignored".to_vec());
```

while both modes share the mode-independent settings:

```
use std::num::NonZeroU32;

use groupnet_core::NodeId;
use groupnet_transport_tcp::{NetworkKey, PeerPolicy, TcpLink, TcpPunchConfig};

let cost = NonZeroU32::new(2).unwrap();
let config = TcpPunchConfig::new(
    NodeId::new("a"),
    "127.0.0.1:7000".parse().unwrap(),
    Some(NetworkKey::from_bytes([7; 32])),
    PeerPolicy::Static(vec![NodeId::new("b")]),
);
let _native = TcpLink::connectivity(config).with_cost(cost);
let _direct = TcpLink::new("127.0.0.1:0".parse().unwrap(), Vec::new())
    .with_credentials(b"token".to_vec())
    .with_cost(cost);
```
"#
)]
#[derive(Debug)]
pub struct TcpLink<M = TcpDirect> {
    mode: M,
    cost: NonZeroU32,
}

/// The direct mode of a [`TcpLink`]: a listener, bootstrap endpoints,
/// application admission and message tuning. Built by [`TcpLink::new`].
pub struct TcpDirect {
    bind: SocketAddr,
    peers: Vec<NodeId>,
    addresses: Vec<SocketAddr>,
    msg_config: TcpMsgConfig,
    admission: Option<Arc<dyn Admission>>,
    credential: Vec<u8>,
    admission_config: TcpAdmissionConfig,
}

impl fmt::Debug for TcpDirect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Neither the credential nor a custom policy (which may hold secrets) is printed.
        f.debug_struct("TcpDirect")
            .field("bind", &self.bind)
            .field("peers", &self.peers)
            .field("addresses", &self.addresses)
            .field("msg_config", &self.msg_config)
            .field("custom_admission", &self.admission.is_some())
            .field("admission_config", &self.admission_config)
            .finish_non_exhaustive()
    }
}

/// The native mode of a [`TcpLink`] (feature `connectivity`): rendezvous
/// admission, candidate traversal and relay fallback, all configured by one
/// `TcpPunchConfig`. Built by `TcpLink::connectivity`.
#[cfg(feature = "connectivity")]
#[derive(Debug)]
pub struct TcpConnectivity {
    config: crate::TcpPunchConfig,
}

impl<M> TcpLink<M> {
    /// Sets the routing cost of this link (default: one).
    #[must_use]
    pub fn with_cost(mut self, cost: NonZeroU32) -> Self {
        self.cost = cost;
        self
    }
}

impl TcpLink<TcpDirect> {
    /// Configures the listener address and directly reachable peer endpoints.
    /// Peers are admitted through a configured-peer allowlist unless
    /// [`with_admission`](Self::with_admission) selects another policy.
    #[must_use]
    pub fn new(bind: SocketAddr, peers: Vec<PeerEndpoint<SocketAddr>>) -> Self {
        let (peers, addresses) = peers
            .into_iter()
            .map(|peer| (peer.node, peer.address))
            .unzip();
        Self {
            mode: TcpDirect {
                bind,
                peers,
                addresses,
                msg_config: TcpMsgConfig::default(),
                admission: None,
                credential: Vec::new(),
                admission_config: TcpAdmissionConfig::default(),
            },
            cost: NonZeroU32::MIN,
        }
    }

    /// Selects the application policy, enabling peers absent from the bootstrap list.
    /// `OpenAdmission` is an explicit unauthenticated membership choice.
    #[must_use]
    pub fn with_admission(mut self, admission: Arc<dyn Admission>) -> Self {
        self.mode.admission = Some(admission);
        self
    }

    /// Supplies opaque credentials presented to the remote admission policy,
    /// at most `MAX_CREDENTIAL_BYTES`. Credentials are omitted from debug output.
    #[must_use]
    pub fn with_credentials(mut self, credential: Vec<u8>) -> Self {
        self.mode.credential = credential;
        self
    }

    /// Sets handshake concurrency, established-peer capacity, and handshake deadline.
    #[must_use]
    pub fn with_admission_config(mut self, config: TcpAdmissionConfig) -> Self {
        self.mode.admission_config = config;
        self
    }

    /// Sets the message transport tuning (idle timeout, pool and queue
    /// capacities, frame limit, advertised address).
    #[must_use]
    pub fn with_msg_config(mut self, config: TcpMsgConfig) -> Self {
        self.mode.msg_config = config;
        self
    }
}

#[cfg(feature = "connectivity")]
impl TcpLink<TcpConnectivity> {
    /// Configures a native TCP connection. `config.local` must match the binding
    /// node identity. Admission, credentials, discovery, and path policy come from
    /// `config`.
    #[must_use]
    pub const fn connectivity(config: crate::TcpPunchConfig) -> Self {
        Self {
            mode: TcpConnectivity { config },
            cost: NonZeroU32::MIN,
        }
    }
}

impl LinkProvider for TcpLink<TcpDirect> {
    fn peers(&self) -> &[NodeId] {
        &self.mode.peers
    }

    fn bind(self: Box<Self>, local: NodeId) -> LinkFuture<'static, io::Result<BoundLink>> {
        Box::pin(async move {
            let Self {
                mode:
                    TcpDirect {
                        bind,
                        peers,
                        addresses,
                        msg_config,
                        admission,
                        credential,
                        admission_config,
                    },
                cost,
            } = *self;
            // Every configured peer is checked before the listener opens;
            // `bind_admitted` then checks the tuning, admission and local id.
            let configured = LinkConfig {
                peers,
                cost,
                ..LinkConfig::default()
            };
            configured.validate_for(&local)?;
            let LinkConfig { peers, .. } = configured;
            let admission = TcpAdmission {
                policy: admission.unwrap_or_else(|| Arc::new(ConfiguredAdmission(peers.clone()))),
                credential,
                config: admission_config,
            };
            let transport =
                TcpMsgTransport::bind_admitted(local, bind, msg_config, admission).await?;
            for (node, address) in peers.iter().zip(addresses) {
                transport.register_peer(node.clone(), address)?;
            }
            transport.bootstrap(peers);
            Ok(transport.into_bound_link(cost))
        })
    }
}

#[cfg(feature = "connectivity")]
impl LinkProvider for TcpLink<TcpConnectivity> {
    fn peers(&self) -> &[NodeId] {
        self.mode.config.peers.static_peers()
    }

    fn bind(self: Box<Self>, local: NodeId) -> LinkFuture<'static, io::Result<BoundLink>> {
        Box::pin(async move {
            let Self {
                mode: TcpConnectivity { config },
                cost,
            } = *self;
            if config.local != local {
                return Err(LinkBindError::IdentityMismatch {
                    configured: config.local,
                    binding: local,
                }
                .into());
            }
            let transport = TcpMsgTransport::bind_connectivity(config).await?;
            Ok(transport.into_bound_link(cost))
        })
    }
}

/// The default direct-link policy: admit exactly the configured peers.
#[derive(Debug)]
struct ConfiguredAdmission(Vec<NodeId>);

impl Admission for ConfiguredAdmission {
    fn admit<'a>(
        &'a self,
        request: JoinRequest<'a>,
    ) -> LinkFuture<'a, Result<AcceptedPeer, AdmissionError>> {
        Box::pin(async move {
            request.validate()?;
            if self.0.contains(request.claimed) {
                Ok(AcceptedPeer::new(request.claimed.clone()))
            } else {
                Err(AdmissionError::UnknownPeer {
                    claimed: request.claimed.clone(),
                })
            }
        })
    }
}

#[cfg(test)]
mod tests;
