//! Ordered byte streams backed by the node's existing pinned TLS tunnel plane,
//! demultiplexed by an application stream-protocol id.
//!
//! Every ordered stream starts with a setup exchange inside the authenticated
//! tunnel: the connector names a [`StreamProtocolId`], and the acceptor's
//! dispatcher answers whether that protocol's accept queue took the stream.
//! Each protocol id has its own bounded accept queue, so independent consumers
//! on one node never receive each other's streams.

use std::{
    collections::HashMap,
    fmt, io,
    ops::Deref,
    sync::{Arc, Mutex, PoisonError, Weak},
    time::Duration,
};

use futures_util::io::{AsyncReadExt, AsyncWriteExt};
use groupnet_core::NodeId;
use groupnet_network::tunnel::{TunnelTransport, TunneledStream};
use groupnet_transport::{QueueCapacity, bulk::BulkTransport};
use tokio::{
    sync::{Mutex as AsyncMutex, OwnedSemaphorePermit, Semaphore, mpsc},
    time::timeout,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use zerocopy::byteorder::network_endian::U32;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::SessionProtocol;

/// Setup magic: `GNOS`, for groupnet ordered stream.
const MAGIC: [u8; 4] = *b"GNOS";

/// Ordered stream setup codec version. Both ends check it, so a mis-deployed
/// node's setup is rejected rather than misparsed; bump it whenever the
/// request or reply layout changes.
const VERSION: u8 = 1;

const ACCEPTED: u8 = 0;
const UNKNOWN_PROTOCOL: u8 = 1;
const QUEUE_FULL: u8 = 2;

/// The connector's setup request: which protocol's accept queue it targets.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct Request {
    magic: [u8; 4],
    version: u8,
    protocol: U32,
}

/// The acceptor's setup reply: whether the stream was queued.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct Reply {
    magic: [u8; 4],
    version: u8,
    status: u8,
}

/// An application stream-protocol id: which consumer an ordered stream is for.
///
/// Streams connected under one id are accepted only through an endpoint
/// scoped to the same id; [`DEFAULT`](Self::DEFAULT) is the id an unscoped
/// endpoint uses. Every node in a cluster must agree on what each id means.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamProtocolId(u32);

impl StreamProtocolId {
    /// The protocol of unscoped ordered endpoints.
    pub const DEFAULT: Self = Self(0);

    /// An application protocol id. Any value is valid; `0` is
    /// [`DEFAULT`](Self::DEFAULT).
    #[must_use]
    pub const fn new(id: u32) -> Self {
        Self(id)
    }

    /// The id's wire value.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl fmt::Display for StreamProtocolId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "stream protocol {}", self.0)
    }
}

/// Node-wide ordered-stream policy: accept queues and the setup deadline.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrderedConfig {
    /// Streams each protocol's accept queue holds before its acceptor refuses
    /// more; it also bounds setups the acceptor reads concurrently.
    pub accept_queue: QueueCapacity,
    /// Deadline for one stream's setup exchange, on both ends.
    pub setup_timeout: Duration,
}

impl Default for OrderedConfig {
    fn default() -> Self {
        Self {
            accept_queue: QueueCapacity::of(64),
            setup_timeout: Duration::from_secs(10),
        }
    }
}

impl OrderedConfig {
    /// Checks the ordered-stream policy.
    ///
    /// # Errors
    /// [`OrderedConfigError::ZeroSetupTimeout`] for a zero setup deadline.
    pub fn validate(&self) -> Result<(), OrderedConfigError> {
        if self.setup_timeout.is_zero() {
            return Err(OrderedConfigError::ZeroSetupTimeout);
        }
        Ok(())
    }
}

/// A violated [`OrderedConfig`] rule, naming the field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderedConfigError {
    /// `setup_timeout` is zero, so no setup could ever complete.
    ZeroSetupTimeout,
}

impl fmt::Display for OrderedConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroSetupTimeout => f.write_str("ordered setup_timeout is zero"),
        }
    }
}

impl std::error::Error for OrderedConfigError {}

impl From<OrderedConfigError> for io::Error {
    fn from(error: OrderedConfigError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}

/// An [`OrderedConfig`] that passed [`OrderedConfig::validate`], constructible
/// only through [`TryFrom`], so [`OrderedProtocol::with_validated`] never
/// validates twice.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ValidOrderedConfig(OrderedConfig);

impl TryFrom<OrderedConfig> for ValidOrderedConfig {
    type Error = OrderedConfigError;

    fn try_from(config: OrderedConfig) -> Result<Self, Self::Error> {
        config.validate()?;
        Ok(Self(config))
    }
}

impl Deref for ValidOrderedConfig {
    type Target = OrderedConfig;

    fn deref(&self) -> &OrderedConfig {
        &self.0
    }
}

type Queued = (NodeId, TunneledStream);

/// One protocol's isolated accept queue.
#[derive(Debug)]
struct Scope {
    incoming: mpsc::Sender<Queued>,
    accepts: AsyncMutex<mpsc::Receiver<Queued>>,
}

impl Scope {
    fn new(capacity: QueueCapacity) -> Arc<Self> {
        let (incoming, accepts) = mpsc::channel(capacity.get());
        Arc::new(Self {
            incoming,
            accepts: AsyncMutex::new(accepts),
        })
    }
}

#[derive(Debug)]
struct Inner {
    tunnels: TunnelTransport,
    config: OrderedConfig,
    cancel: CancellationToken,
    sessions: Mutex<Vec<CancellationToken>>,
    scopes: Mutex<HashMap<StreamProtocolId, Arc<Scope>>>,
    tasks: TaskTracker,
}

impl Inner {
    fn shutdown(&self) {
        // Registration takes the same lock, so no session can escape a shutdown
        // racing completion of its authenticated handshake.
        let mut sessions = self.sessions.lock().unwrap_or_else(PoisonError::into_inner);
        self.cancel.cancel();
        self.tasks.close();
        for session in sessions.drain(..) {
            session.cancel();
        }
        drop(sessions);
        // Queued streams were never accepted: drop them now so their peers
        // observe the shutdown instead of a stream nobody will read.
        for scope in self
            .scopes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
        {
            if let Ok(mut queue) = scope.accepts.try_lock() {
                queue.close();
                while queue.try_recv().is_ok() {}
            }
        }
    }

    fn scope(&self, protocol: StreamProtocolId) -> Option<Arc<Scope>> {
        self.scopes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&protocol)
            .cloned()
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// A cheap, clone-backed ordered session endpoint for one
/// [`StreamProtocolId`], using pinned TLS tunnels.
///
/// One `OrderedProtocol` owns its tunnel transport's ordered accept queue: its
/// dispatcher reads each inbound stream's setup and routes it to the accept
/// queue of the protocol id the connector named, or refuses it. Derive the
/// endpoints of other protocols with [`scoped`](Self::scoped); never build a
/// second `OrderedProtocol` (or accept raw [`BulkTransport`] streams) on the
/// same tunnel transport, which would compete for inbound streams.
///
/// Scoped endpoints and clones share the dispatcher, shutdown and
/// established-session ownership. Shutting down or dropping the last handle
/// cancels its sessions, setups and blocked calls, not the shared node, tunnel
/// transport or unordered sessions, whose setup channels the ordered queue
/// never sees.
#[derive(Clone, Debug)]
pub struct OrderedProtocol {
    inner: Arc<Inner>,
    protocol: StreamProtocolId,
    scope: Arc<Scope>,
}

impl OrderedProtocol {
    /// The [`StreamProtocolId::DEFAULT`] endpoint over `tunnels` under the
    /// default [`OrderedConfig`].
    ///
    /// # Errors
    /// Requires a running Tokio executor for the setup dispatcher.
    pub fn new(tunnels: TunnelTransport) -> io::Result<Self> {
        Self::with_config(tunnels, OrderedConfig::default())
    }

    /// The [`StreamProtocolId::DEFAULT`] endpoint over `tunnels` under
    /// `config`.
    ///
    /// # Errors
    /// Returns `InvalidInput` wrapping [`OrderedConfigError`] before starting
    /// anything, or an error without a running Tokio executor.
    pub fn with_config(tunnels: TunnelTransport, config: OrderedConfig) -> io::Result<Self> {
        Self::with_validated(tunnels, ValidOrderedConfig::try_from(config)?)
    }

    /// [`with_config`](Self::with_config) for an already validated config.
    ///
    /// # Errors
    /// Requires a running Tokio executor for the setup dispatcher.
    pub fn with_validated(
        tunnels: TunnelTransport,
        config: ValidOrderedConfig,
    ) -> io::Result<Self> {
        tokio::runtime::Handle::try_current()
            .map_err(|_| io::Error::new(io::ErrorKind::NotConnected, "Tokio executor required"))?;
        let config = config.0;
        let scope = Scope::new(config.accept_queue);
        let cancel = tunnels.cancellation();
        let inner = Arc::new(Inner {
            tunnels: tunnels.clone(),
            config,
            cancel: cancel.clone(),
            sessions: Mutex::new(Vec::new()),
            scopes: Mutex::new(HashMap::from([(StreamProtocolId::DEFAULT, scope.clone())])),
            tasks: TaskTracker::new(),
        });
        inner
            .tasks
            .spawn(dispatch(Arc::downgrade(&inner), tunnels, cancel, config));
        Ok(Self {
            inner,
            protocol: StreamProtocolId::DEFAULT,
            scope,
        })
    }

    /// The endpoint for `protocol` on this endpoint's tunnel plane, sharing
    /// its dispatcher, sessions and shutdown.
    ///
    /// The first endpoint for an id opens that id's accept queue: from then on
    /// inbound streams naming it queue (up to
    /// [`accept_queue`](OrderedConfig::accept_queue)) until accepted. Before
    /// that, a connector naming the id is refused. Every call for one id
    /// returns an endpoint over the same queue.
    #[must_use]
    pub fn scoped(&self, protocol: StreamProtocolId) -> Self {
        let scope = self
            .inner
            .scopes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(protocol)
            .or_insert_with(|| Scope::new(self.inner.config.accept_queue))
            .clone();
        Self {
            inner: self.inner.clone(),
            protocol,
            scope,
        }
    }

    /// The stream protocol this endpoint connects and accepts.
    #[must_use]
    pub fn protocol(&self) -> StreamProtocolId {
        self.protocol
    }

    /// Establishes an authenticated, reliable, ordered byte stream to the
    /// remote endpoint scoped to this endpoint's protocol.
    ///
    /// # Errors
    /// Returns tunnel admission, capacity, authentication or shutdown errors,
    /// `TimedOut` past the setup deadline, `ConnectionRefused` when the peer
    /// has no endpoint for this protocol, `WouldBlock` when that endpoint's
    /// accept queue is full, and `InvalidData` for a malformed setup reply.
    pub async fn connect(&self, to: &NodeId, (): ()) -> io::Result<TunneledStream> {
        let setup = async {
            let mut stream = self.inner.tunnels.connect(to).await?;
            let request = Request {
                magic: MAGIC,
                version: VERSION,
                protocol: U32::new(self.protocol.get()),
            };
            let reply = timeout(self.inner.config.setup_timeout, async {
                stream.write_all(request.as_bytes()).await?;
                stream.flush().await?;
                let mut reply = [0; size_of::<Reply>()];
                stream.read_exact(&mut reply).await?;
                io::Result::Ok(reply)
            })
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "ordered setup deadline"))??;
            check_reply(&reply, self.protocol)?;
            io::Result::Ok(stream)
        };
        let stream = tokio::select! {
            biased;
            () = self.inner.cancel.cancelled() => return Err(closed()),
            stream = setup => stream?,
        };
        self.register(&stream)?;
        Ok(stream)
    }

    /// Accepts an ordered stream of this endpoint's protocol and its
    /// authenticated logical peer identity.
    ///
    /// # Errors
    /// Returns state-lock or shutdown errors.
    pub async fn accept(&self) -> io::Result<(NodeId, TunneledStream)> {
        let mut queue = tokio::select! {
            biased;
            () = self.inner.cancel.cancelled() => return Err(closed()),
            queue = self.scope.accepts.lock() => queue,
        };
        let (peer, stream) = tokio::select! {
            biased;
            () = self.inner.cancel.cancelled() => return Err(closed()),
            accepted = queue.recv() => accepted.ok_or_else(closed)?,
        };
        drop(queue);
        self.register(&stream)?;
        Ok((peer, stream))
    }

    /// Cancels this endpoint's calls and streams, for every protocol scope,
    /// without shutting down the node.
    pub fn shutdown(&self) {
        self.inner.shutdown();
    }

    /// Waits for endpoint or node shutdown and the setup dispatcher's exit.
    pub async fn closed(&self) {
        self.inner.cancel.cancelled().await;
        self.inner.tasks.close();
        self.inner.tasks.wait().await;
    }

    fn register(&self, stream: &TunneledStream) -> io::Result<()> {
        let mut sessions = self
            .inner
            .sessions
            .lock()
            .map_err(|_| io::Error::other("ordered session lock poisoned"))?;
        if self.inner.cancel.is_cancelled() {
            return Err(closed());
        }
        // Native tunnel capacity bounds live entries; dropped/revoked streams
        // cancel their token and are removed before any new entry is appended.
        sessions.retain(|session| !session.is_cancelled());
        sessions.push(stream.cancellation());
        Ok(())
    }
}

impl SessionProtocol for OrderedProtocol {
    type Session = TunneledStream;
    type ConnectOptions = ();

    async fn connect(&self, to: &NodeId, options: ()) -> io::Result<Self::Session> {
        Self::connect(self, to, options).await
    }

    async fn accept(&self) -> io::Result<(NodeId, Self::Session)> {
        Self::accept(self).await
    }
}

/// Pulls every inbound ordered stream off the tunnel plane and reads its setup
/// on a bounded number of concurrent tasks, so one slow connector delays only
/// its own stream.
async fn dispatch(
    owner: Weak<Inner>,
    tunnels: TunnelTransport,
    cancel: CancellationToken,
    config: OrderedConfig,
) {
    let setups = Arc::new(Semaphore::new(config.accept_queue.get()));
    loop {
        let permit = tokio::select! {
            biased;
            () = cancel.cancelled() => break,
            permit = setups.clone().acquire_owned() => match permit {
                Ok(permit) => permit,
                Err(_) => break,
            },
        };
        let accepted = tokio::select! {
            biased;
            () = cancel.cancelled() => break,
            accepted = tunnels.accept() => match accepted {
                Ok(accepted) => accepted,
                Err(_) => break,
            },
        };
        let Some(inner) = owner.upgrade() else {
            break;
        };
        let owner = owner.clone();
        let cancel = cancel.clone();
        inner.tasks.spawn(async move {
            tokio::select! {
                biased;
                () = cancel.cancelled() => {},
                _routed = timeout(config.setup_timeout, route(&owner, accepted, permit)) => {},
            }
        });
    }
    cancel.cancel();
}

/// Reads one stream's setup and queues it for the named protocol, answering
/// the connector either way.
async fn route(
    owner: &Weak<Inner>,
    (peer, mut stream): Queued,
    _permit: OwnedSemaphorePermit,
) -> io::Result<()> {
    let mut request = [0; size_of::<Request>()];
    stream.read_exact(&mut request).await?;
    let request =
        Request::read_from_bytes(&request).map_err(|_| invalid("invalid ordered setup request"))?;
    if request.magic != MAGIC || request.version != VERSION {
        return Err(invalid("invalid ordered setup version"));
    }
    let protocol = StreamProtocolId::new(request.protocol.get());
    let scope = owner.upgrade().ok_or_else(closed)?.scope(protocol);
    let reserved = scope.as_ref().map(|scope| scope.incoming.try_reserve());
    let status = match &reserved {
        None => UNKNOWN_PROTOCOL,
        Some(Err(_)) => QUEUE_FULL,
        Some(Ok(_)) => ACCEPTED,
    };
    let reply = Reply {
        magic: MAGIC,
        version: VERSION,
        status,
    };
    stream.write_all(reply.as_bytes()).await?;
    if let Some(Ok(slot)) = reserved {
        stream.flush().await?;
        slot.send((peer, stream));
    } else {
        // A refusal must reach the connector before the drop cancels the
        // session: closing waits for its reliable acknowledgement.
        stream.close().await?;
    }
    Ok(())
}

fn check_reply(reply: &[u8], protocol: StreamProtocolId) -> io::Result<()> {
    let reply = Reply::ref_from_bytes(reply).map_err(|_| invalid("invalid ordered setup reply"))?;
    if reply.magic != MAGIC || reply.version != VERSION {
        return Err(invalid("invalid ordered setup reply version"));
    }
    match reply.status {
        ACCEPTED => Ok(()),
        UNKNOWN_PROTOCOL => Err(io::Error::new(
            io::ErrorKind::ConnectionRefused,
            format!("peer has no ordered endpoint for {protocol}"),
        )),
        QUEUE_FULL => Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            format!("peer's ordered accept queue for {protocol} is full"),
        )),
        _ => Err(invalid("unknown ordered setup status")),
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn closed() -> io::Error {
    io::Error::new(io::ErrorKind::ConnectionAborted, "ordered endpoint closed")
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use zerocopy::IntoBytes;
    use zerocopy::byteorder::network_endian::U32;

    use super::{
        MAGIC, OrderedConfig, OrderedConfigError, Reply, Request, StreamProtocolId, VERSION,
        ValidOrderedConfig, check_reply,
    };

    fn reply(magic: [u8; 4], version: u8, status: u8) -> Vec<u8> {
        Reply {
            magic,
            version,
            status,
        }
        .as_bytes()
        .to_vec()
    }

    #[test]
    fn the_setup_request_is_magic_version_then_a_big_endian_protocol() {
        let request = Request {
            magic: MAGIC,
            version: VERSION,
            protocol: U32::new(0x0102_0304),
        };
        assert_eq!(request.as_bytes(), b"GNOS\x01\x01\x02\x03\x04");
    }

    #[test]
    fn replies_decode_by_status_and_reject_other_versions() {
        let id = StreamProtocolId::new(7);
        assert!(check_reply(&reply(MAGIC, VERSION, 0), id).is_ok());
        assert_eq!(
            check_reply(&reply(MAGIC, VERSION, 1), id)
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::ConnectionRefused
        );
        assert_eq!(
            check_reply(&reply(MAGIC, VERSION, 2), id)
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::WouldBlock
        );
        for bad in [
            reply(MAGIC, VERSION, 3),
            reply(MAGIC, VERSION + 1, 0),
            reply(MAGIC, VERSION - 1, 0),
            reply(*b"GNOX", VERSION, 0),
        ] {
            assert_eq!(
                check_reply(&bad, id).unwrap_err().kind(),
                std::io::ErrorKind::InvalidData
            );
        }
    }

    #[test]
    fn config_validation_rejects_a_zero_setup_deadline_by_field() {
        assert!(ValidOrderedConfig::try_from(OrderedConfig::default()).is_ok());
        let zero = OrderedConfig {
            setup_timeout: Duration::ZERO,
            ..OrderedConfig::default()
        };
        assert_eq!(zero.validate(), Err(OrderedConfigError::ZeroSetupTimeout));
        assert_eq!(
            ValidOrderedConfig::try_from(zero),
            Err(OrderedConfigError::ZeroSetupTimeout)
        );
        let error = std::io::Error::from(OrderedConfigError::ZeroSetupTimeout);
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    }
}
