//! Router-independent native IPC preparation.

use std::fmt;
use std::io;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;

use groupnet_core::NodeId;
use groupnet_transport::admission::{Admission, AllowedPeers};
use groupnet_transport::link::{BoundLink, LinkConfig, LinkFuture, LinkProvider, PeerEndpoint};

use crate::ipc::Preflight;
use crate::{IpcAddress, IpcAdmission, IpcConfig, IpcTransport, MAX_FRAME};

/// The link MTU: the IPC framing protocol's largest message.
const MTU: NonZeroUsize = NonZeroUsize::new(MAX_FRAME).unwrap();

/// A native IPC link whose peers are admitted at run time.
///
/// The link is always admitted (`docs/client-role.md`, section 2): its
/// configured peers are seeds that it dials while they hold no session, and a
/// peer joins when the [`Admission`] policy accepts its claim. Without
/// [`with_admission`](Self::with_admission), the policy admits exactly the
/// configured peers. [`IpcLink::new`] listens; [`IpcLink::dial_only`] does
/// not, so its node is reachable only over the sessions it opens — a client
/// of the nodes it dials.
///
/// Binding validates everything once, before it opens the listener: the
/// link's peers through [`LinkConfig::validate_for`], the [`IpcConfig`], the
/// credential, the local identity, and every peer as
/// [`IpcTransport::register_peer`] would check it (the 64-byte IPC identity
/// bound, `max_peers` and the platform address). A failure is `InvalidInput`
/// carrying the typed cause
/// ([`LinkConfigError`](groupnet_transport::link::LinkConfigError),
/// [`IpcConfigError`](crate::IpcConfigError),
/// [`IpcAdmissionError`](crate::IpcAdmissionError), the local
/// [`NodeIdError`](crate::NodeIdError), or an
/// [`IpcPeerError`](crate::IpcPeerError), whose `Address` variant wraps the
/// [`IpcAddressError`](crate::IpcAddressError)), and no listener exists.
/// Only opening the listener itself (its own address, an occupied name, a
/// missing Tokio runtime) can still fail.
///
/// Setters only store their values. Preflight with [`IpcConfig::validate`],
/// [`IpcAdmission::validate`] and [`LinkConfig::validate_for`].
pub struct IpcLink {
    bind: Option<IpcAddress>,
    peers: Vec<NodeId>,
    addresses: Vec<IpcAddress>,
    cost: NonZeroU32,
    config: IpcConfig,
    admission: Option<Arc<dyn Admission>>,
    credential: Vec<u8>,
}

impl fmt::Debug for IpcLink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Neither the credential nor a custom policy (which may hold secrets) is printed.
        f.debug_struct("IpcLink")
            .field("bind", &self.bind)
            .field("peers", &self.peers)
            .field("addresses", &self.addresses)
            .field("cost", &self.cost)
            .field("config", &self.config)
            .field("custom_admission", &self.admission.is_some())
            .finish_non_exhaustive()
    }
}

impl IpcLink {
    /// Configures a listener and the native addresses of its seed peers.
    #[must_use]
    pub fn new(bind: IpcAddress, peers: Vec<PeerEndpoint<IpcAddress>>) -> Self {
        Self::with_listener(Some(bind), peers)
    }

    /// Configures a link without a listener that dials `peers`: a client of
    /// the nodes it names. Each peer must admit this node's claim.
    #[must_use]
    pub fn dial_only(peers: Vec<PeerEndpoint<IpcAddress>>) -> Self {
        Self::with_listener(None, peers)
    }

    fn with_listener(bind: Option<IpcAddress>, peers: Vec<PeerEndpoint<IpcAddress>>) -> Self {
        let (peers, addresses) = peers
            .into_iter()
            .map(|peer| (peer.node, peer.address))
            .unzip();
        Self {
            bind,
            peers,
            addresses,
            cost: NonZeroU32::MIN,
            config: IpcConfig::default(),
            admission: None,
            credential: Vec::new(),
        }
    }

    /// Selects the policy that judges every remote claim, admitting peers
    /// absent from the seed list. `OpenAdmission` admits every local peer.
    #[must_use]
    pub fn with_admission(mut self, admission: Arc<dyn Admission>) -> Self {
        self.admission = Some(admission);
        self
    }

    /// Supplies the opaque credential presented to remote policies, at most
    /// `MAX_CREDENTIAL_BYTES`. Credentials are omitted from debug output.
    #[must_use]
    pub fn with_credentials(mut self, credential: Vec<u8>) -> Self {
        self.credential = credential;
        self
    }

    /// Selects listener resource budgets, validated when the link binds.
    #[must_use]
    pub const fn with_config(mut self, config: IpcConfig) -> Self {
        self.config = config;
        self
    }

    /// Sets the routing cost advertised for this link (one by default).
    #[must_use]
    pub const fn with_cost(mut self, cost: NonZeroU32) -> Self {
        self.cost = cost;
        self
    }
}

impl LinkProvider for IpcLink {
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
                config,
                admission,
                credential,
            } = *self;
            let admission = IpcAdmission {
                policy: admission.unwrap_or_else(|| Arc::new(AllowedPeers::new(peers.clone()))),
                credential,
            };
            admission.validate()?;
            let link = LinkConfig {
                peers,
                cost,
                mtu: MTU,
            };
            let preflight = Preflight::for_link(local, config, &link, addresses)?;
            let transport = IpcTransport::start(bind.as_ref(), preflight, Some(admission))?;
            Ok(transport.into_bound_link(cost))
        })
    }
}

impl IpcTransport {
    /// Packages this transport with its workers for registration with a
    /// router.
    ///
    /// An admitted transport routes only through its live sessions: the link
    /// lists no static peers and carries the transport's
    /// [`SessionRegistry`](groupnet_transport::admission::SessionRegistry). A
    /// static transport lists the peers registered so far, which the router
    /// then admits as its only sources.
    #[must_use]
    pub fn into_bound_link(self, cost: NonZeroU32) -> BoundLink {
        let sessions = self.sessions().cloned();
        let peers = if sessions.is_some() {
            Vec::new()
        } else {
            self.registered_peers()
        };
        let config = LinkConfig {
            peers,
            cost,
            mtu: MTU,
        };
        let lifecycle = Arc::new(self.clone());
        let bound = BoundLink::new(self, config).with_lifecycle(lifecycle);
        match sessions {
            Some(sessions) => bound.with_sessions(sessions),
            None => bound,
        }
    }
}
