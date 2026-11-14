//! The client role: a participant of a managed network that joins no group
//! and forwards nothing (`docs/client-role.md`, section 6.2).

use std::fmt;
use std::io;

use groupnet_core::NodeId;
use groupnet_network::tunnel::{TunnelTransport, TunneledStream};
use groupnet_network::{LinkProvider, Router, RouterConfig, TunnelConfig};
use groupnet_streams::{OrderedConfig, UnorderedConfig};
use groupnet_transport::bulk::BulkTransport;

use crate::messaging::{
    Bytes, Frame, MessageContext, MessageId, MessagingConfig, ReceiveHandle, SendOptions,
};
use crate::participant::{Settings, start_dispatcher};
use crate::{Endpoint, Ordered, Participant, Peer, PeerImplementation};

/// A client of one or more hubs: it sends and receives node-addressed
/// messages and opens ordered and unordered sessions, but takes no part in
/// membership.
///
/// A client joins no group, not even the reserved routing group, gossips
/// nothing, runs no peer discovery or address sync, and never forwards transit
/// traffic (its router runs with [`RouterConfig::forwarding`] `false`). It
/// binds the same [`Endpoint`] and [`Peer`] handles as a [`Node`](crate::Node),
/// through the same [`Participant`] machinery. Its links are normally
/// dial-only and admitted by the hub's policy, such as an IPC link built with
/// `IpcLink::dial_only`; the hub is an ordinary `Node` that admits the client
/// through its own link admission.
///
/// Group-addressed messages sent to a client are refused with `NotFound`.
///
/// ```no_run
/// use groupnet_core::NodeId;
/// use groupnet_runtime::{Client, LinkProvider, Messages};
/// use groupnet_runtime::messaging::Bytes;
/// use std::time::Duration;
///
/// # async fn demo(link: impl LinkProvider) -> std::io::Result<()> {
/// let client = Client::builder(NodeId::new("client")).link(link).start().await?;
/// let hub = client.peer(NodeId::new("hub"), Messages::applied(Duration::from_secs(5)))?;
/// hub.send(Bytes::from_static(b"hello")).await?;
/// let (context, reply) = client.recv().await?;
/// context.applied()?;
/// # let _ = reply;
/// client.close().await;
/// # Ok(())
/// # }
/// ```
///
/// Cheap to clone; every clone retains the network's lifetime.
#[derive(Clone)]
pub struct Client {
    participant: Participant,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("id", self.id())
            .finish_non_exhaustive()
    }
}

impl Client {
    /// Starts building a client with the given logical id.
    #[must_use]
    pub fn builder(id: NodeId) -> ClientBuilder {
        ClientBuilder::new(id)
    }

    /// This client's id.
    #[must_use]
    pub fn id(&self) -> &NodeId {
        self.participant.id()
    }

    /// The client's network side, shared with every endpoint and peer it binds.
    #[must_use]
    pub fn participant(&self) -> &Participant {
        &self.participant
    }

    /// The client's router: route inspection and typed adapter registration.
    /// A router clone alone does not retain the managed network's lifetime.
    #[must_use]
    pub fn router(&self) -> &Router {
        self.participant.router()
    }

    /// Accesses tunnel peer admission and revocation.
    ///
    /// # Errors
    /// Returns `Unsupported` for a client without configured TLS tunnels.
    pub fn tunnels(&self) -> io::Result<&TunnelTransport> {
        self.participant.tunnels()
    }

    /// Binds a typed endpoint and immutable options to this client's shared
    /// protocol. See [`Participant::endpoint`].
    ///
    /// # Errors
    /// Returns implementation binding errors or `NotConnected` after close.
    pub fn endpoint<I: PeerImplementation>(&self, implementation: I) -> io::Result<Endpoint<I>> {
        self.participant.endpoint(implementation)
    }

    /// Binds a logical destination, such as a hub. See [`Participant::peer`].
    ///
    /// # Errors
    /// Returns implementation binding errors or `NotConnected` after close.
    pub fn peer<I: PeerImplementation>(
        &self,
        id: NodeId,
        implementation: I,
    ) -> io::Result<Peer<I>> {
        self.participant.peer(id, implementation)
    }

    /// Sends an opaque borrowed buffer to one node, using best-effort delivery.
    /// See [`Participant::send`].
    ///
    /// # Errors
    /// Rejects oversized payloads, shutdown, or local routing enqueue failures.
    pub async fn send(&self, to: &NodeId, payload: impl AsRef<[u8]>) -> io::Result<MessageId> {
        self.participant.send(to, payload).await
    }

    /// Sends an owned/shared buffer with explicit delivery options.
    /// See [`Participant::send_frame`].
    ///
    /// # Errors
    /// Rejects invalid size/options, shutdown, route/queue errors, receiver
    /// rejection, or an acknowledged delivery deadline expiring.
    pub async fn send_frame(
        &self,
        to: &NodeId,
        payload: Bytes,
        options: SendOptions,
    ) -> io::Result<MessageId> {
        self.participant.send_frame(to, payload, options).await
    }

    /// Receives the next node-addressed buffer without copying its payload.
    /// See [`Participant::recv`].
    ///
    /// # Errors
    /// Returns `WouldBlock` while another receive/callback owns the inbox,
    /// or `NotConnected` when messaging or the network closes.
    pub async fn recv(&self) -> io::Result<(MessageContext, Bytes)> {
        self.participant.recv().await
    }

    /// Receives the next node-addressed frame. See [`Participant::recv_frame`].
    ///
    /// # Errors
    /// Returns `WouldBlock` while another receive/callback owns the inbox,
    /// or `NotConnected` when messaging or the network closes.
    pub async fn recv_frame(&self) -> io::Result<Frame> {
        self.participant.recv_frame().await
    }

    /// Registers the exclusive, serial async receiver for node-addressed
    /// buffers. See [`Participant::on_recv`].
    ///
    /// # Errors
    /// Returns `WouldBlock` if the inbox already has a receive owner, or an
    /// error if messaging or the network is closed.
    pub fn on_recv<F, Fut>(&self, callback: F) -> io::Result<ReceiveHandle>
    where
        F: FnMut(MessageContext, Bytes) -> Fut + Send + 'static,
        Fut: Future<Output = io::Result<()>> + Send + 'static,
    {
        self.participant.on_recv(callback)
    }

    /// Registers the exclusive, serial async receiver for node-addressed
    /// frames. See [`Participant::on_frame`].
    ///
    /// # Errors
    /// Returns `WouldBlock` if the inbox already has a receive owner, or an
    /// error if messaging or the network is closed.
    pub fn on_frame<F, Fut>(&self, callback: F) -> io::Result<ReceiveHandle>
    where
        F: FnMut(Frame) -> Fut + Send + 'static,
        Fut: Future<Output = io::Result<()>> + Send + 'static,
    {
        self.participant.on_frame(callback)
    }

    /// Closes connections across all client clones, releases every link and
    /// drains owned network tasks, then waits until the messaging and session
    /// engines have stopped. Endpoints and peers that remain fail with
    /// `NotConnected`.
    pub async fn close(&self) {
        self.participant.close_network().await;
        self.participant.close_engines().await;
    }
}

/// Adapter for code generic over [`BulkTransport`] (a
/// [`DataPlane`](groupnet_transport::bulk::DataPlane) or an RPC client),
/// over the client's default [`Ordered::new`] endpoint, as for
/// [`Node`](crate::Node). Fails with `Unsupported` without configured tunnels
/// and `NotConnected` after close.
impl BulkTransport for Client {
    type Error = io::Error;
    type Stream = TunneledStream;

    async fn connect(&self, to: &NodeId) -> io::Result<Self::Stream> {
        self.endpoint(Ordered::new())?.connect(to).await
    }

    async fn accept(&self) -> io::Result<(NodeId, Self::Stream)> {
        self.endpoint(Ordered::new())?.accept().await
    }
}

/// Configures a [`Client`]'s links, routing limits, tunnels and node-wide
/// messaging and session policies: the settings of a
/// [`NodeBuilder`](crate::NodeBuilder) that apply to a participant with no
/// group.
///
/// No seed, gossip, membership or address-advertisement setting exists here:
/// a client joins no group. [`start`](Self::start) validates every policy once,
/// before it binds any link.
pub struct ClientBuilder {
    id: NodeId,
    settings: Settings,
    routing: RouterConfig,
}

impl fmt::Debug for ClientBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientBuilder")
            .field("id", &self.id)
            .field("settings", &self.settings)
            .field("routing", &self.routing)
            .finish()
    }
}

impl ClientBuilder {
    fn new(id: NodeId) -> Self {
        Self {
            id,
            settings: Settings::default(),
            routing: RouterConfig {
                forwarding: false,
                ..RouterConfig::default()
            },
        }
    }

    /// Registers one configured protocol implementation, normally a dial-only
    /// admitted link to a hub.
    #[must_use]
    pub fn link(mut self, provider: impl LinkProvider) -> Self {
        self.settings.link(provider);
        self
    }

    /// Registers a heterogeneous collection of configured protocol implementations.
    #[must_use]
    pub fn links(mut self, providers: impl IntoIterator<Item = Box<dyn LinkProvider>>) -> Self {
        self.settings.links(providers);
        self
    }

    /// Sets routing limits and the cluster secret. The default is
    /// [`RouterConfig::default`] with `forwarding: false`. Stored as given;
    /// [`start`](Self::start) refuses `forwarding: true`
    /// ([`ClientConfigError::Forwarding`]).
    #[must_use]
    pub fn routing(mut self, config: RouterConfig) -> Self {
        self.routing = config;
        self
    }

    /// Enables pinned, end-to-end TLS streams over routed paths, which ordered
    /// and unordered sessions need.
    #[must_use]
    pub fn tunnels(mut self, config: TunnelConfig) -> Self {
        self.settings.tunnels(config);
        self
    }

    /// Configures message payloads, the inbox and retries.
    /// [`start`](Self::start) rejects an invalid configuration.
    #[must_use]
    pub fn messaging(mut self, config: MessagingConfig) -> Self {
        self.settings.messaging(config);
        self
    }

    /// Configures ordered-stream accept queues and the setup deadline for every
    /// [`Ordered`] endpoint and peer of this client.
    /// [`start`](Self::start) rejects an invalid configuration.
    #[must_use]
    pub fn ordered(mut self, config: OrderedConfig) -> Self {
        self.settings.ordered(config);
        self
    }

    /// Configures unordered-session admission, capacities and timers for every
    /// [`Unordered`](crate::Unordered) endpoint and peer of this client.
    /// [`start`](Self::start) rejects an invalid configuration.
    #[must_use]
    pub fn unordered(mut self, config: UnorderedConfig) -> Self {
        self.settings.unordered(config);
        self
    }

    /// Validates every policy, binds all links and starts routing and the
    /// messaging dispatcher. No group actor, gossip or discovery task starts.
    ///
    /// # Errors
    /// Returns `InvalidInput` before binding anything when a policy fails
    /// validation, wrapping the typed error that names the field: a
    /// [`ClientConfigError`], a
    /// [`MessagingConfigError`](crate::messaging::MessagingConfigError), an
    /// [`OrderedConfigError`](crate::OrderedConfigError) or an
    /// [`UnorderedConfigError`](crate::UnorderedConfigError). Propagates
    /// invalid routing configuration, binding, registration, or security
    /// setup failures.
    ///
    /// # Panics
    /// Requires a Tokio runtime.
    pub async fn start(self) -> io::Result<Client> {
        if self.routing.forwarding {
            return Err(ClientConfigError::Forwarding.into());
        }
        let mut settings = self.settings;
        settings.routing(self.routing);
        let participant = settings.validate()?.bind(self.id).await?;
        // A client has no group inbox: a group-addressed frame finds no member here.
        start_dispatcher(&participant, |frame: Frame| {
            let _ = frame.receipt().reject(io::ErrorKind::NotFound);
        });
        Ok(Client { participant })
    }
}

/// A [`ClientBuilder`] setting that cannot apply to a client.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientConfigError {
    /// [`RouterConfig::forwarding`] is `true`; a client never forwards transit
    /// traffic.
    Forwarding,
}

impl fmt::Display for ClientConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Forwarding => f.write_str(
                "RouterConfig::forwarding must be false for a client, which never forwards",
            ),
        }
    }
}

impl std::error::Error for ClientConfigError {}

impl From<ClientConfigError> for io::Error {
    fn from(error: ClientConfigError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}
