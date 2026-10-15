//! Router lifecycle, typed links, path-vector learning, and packet forwarding.

mod adapters;
mod bounds;
mod config;
mod packet;
mod pool;
mod protocol;
mod replay;
mod routing;

pub use bounds::{LinkRegistrationError, RoutedSendError};
pub use config::{ReassemblyConfig, ReassemblyConfigError, RouterConfig, RouterConfigError};
pub use packet::PacketBuffer;
pub(crate) use packet::TunnelBuffers;

pub use protocol::{ProtocolId, ProtocolIo};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod admission_tests;

#[cfg(test)]
mod bounds_tests;

#[cfg(test)]
mod outbound_tests;

use routing::{InboundState, drive};

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::sync::{Arc, Mutex, OnceLock, atomic::AtomicU64};
use std::time::Duration;

use bytes::Bytes;
use groupnet_core::NodeId;
use groupnet_transport::admission::{SessionId, SessionRegistry};
use groupnet_transport::link::{BoundLink, LinkConfig, LinkControl};
use groupnet_transport::{Inbound, MAX_NODE_ID_BYTES, Transport, validate_node_id};
use ring::rand::{SecureRandom, SystemRandom};
use tokio::sync::{Mutex as AsyncMutex, mpsc, watch};
use tokio::time::Instant;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::wire::{self, PayloadKind};

/// A local, opaque transport registration identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TransportId(usize);

/// A currently reachable destination and its selected forwarding path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Route {
    /// Final destination, independent of the next physical link.
    pub destination: NodeId,
    /// Adjacent peer which receives the forwarded packet.
    pub next_hop: NodeId,
    /// Local transport registration used by the next hop.
    pub transport: TransportId,
    /// Sum of positive link costs along this path.
    pub cost: u32,
    /// Complete advertised path including this node and the destination.
    pub path: Vec<NodeId>,
}

struct Queued {
    peer: NodeId,
    bytes: Bytes,
    session: Option<SessionId>,
}

struct Link {
    config: LinkConfig,
    control: LinkControl,
    outgoing: mpsc::Sender<Queued>,
    cancel: CancellationToken,
    sessions: Option<SessionRegistry>,
}

impl Link {
    fn admits(&self, peer: &NodeId, session: Option<SessionId>) -> bool {
        match (&self.sessions, session) {
            (Some(registry), Some(id)) => registry.is_active(peer, id),
            (None, None) => self.config.peers.contains(peer),
            _ => false,
        }
    }
}

struct Candidate {
    route: Route,
    updated: Instant,
    session: Option<SessionId>,
}

#[derive(Default)]
struct Table {
    links: HashMap<TransportId, Link>,
    next_link: usize,
    candidates: HashMap<NodeId, HashMap<(TransportId, NodeId), Candidate>>,
}

impl Table {
    fn route(&self, target: &NodeId, ttl: Duration) -> Option<&Route> {
        self.candidate(target, ttl)
            .map(|candidate| &candidate.route)
    }

    fn candidate(&self, target: &NodeId, ttl: Duration) -> Option<&Candidate> {
        self.candidates
            .get(target)?
            .values()
            .filter(|candidate| {
                candidate.updated.elapsed() < ttl
                    && self
                        .links
                        .get(&candidate.route.transport)
                        .is_some_and(|link| {
                            link.admits(&candidate.route.next_hop, candidate.session)
                        })
            })
            .min_by(|a, b| {
                let a = &a.route;
                let b = &b.route;
                (a.cost, &a.path, a.transport).cmp(&(b.cost, &b.path, b.transport))
            })
    }

    fn remove(&mut self, id: TransportId) {
        if let Some(link) = self.links.remove(&id) {
            link.cancel.cancel();
        }
        self.candidates.retain(|_, choices| {
            choices.retain(|(link, _), _| *link != id);
            !choices.is_empty()
        });
    }
}

enum Event {
    Neighbors(TransportId),
    Down(TransportId),
    Announce,
}

/// The tunnel endpoint's synchronous inbound hook. Link workers call it for
/// each locally addressed tunnel packet, so tunnel traffic crosses no router
/// queue or task; implementations must only route to bounded per-session queues.
pub(crate) trait TunnelInbox: Send + Sync + 'static {
    /// Accepts or drops one opaque tunnel packet from its routed origin.
    fn deliver(&self, from: NodeId, payload: Bytes);
}

struct AdvertisedRoute {
    path: Vec<NodeId>,
    frame: Bytes,
}

/// Opaque application-plane packet, separate from coordination and tunnel traffic.
///
/// The router attributes the origin to its trusted fabric, without authenticating
/// application identities or interpreting payloads, receipts, or retry semantics.
#[derive(Debug)]
pub struct ApplicationPacket {
    /// Original routed sender, not the forwarding neighbor.
    pub from: NodeId,
    /// Complete owned opaque application packet bytes.
    pub payload: Bytes,
}

struct Shared {
    local: NodeId,
    config: RouterConfig,
    table: Mutex<Table>,
    inbound: Mutex<InboundState>,
    events: mpsc::Sender<Event>,
    messages: mpsc::Sender<Inbound>,
    tunnel: OnceLock<Arc<dyn TunnelInbox>>,
    protocols: Mutex<protocol::Registry>,
    advertisements: watch::Sender<Arc<Vec<AdvertisedRoute>>>,
    reachable: watch::Sender<Arc<Vec<NodeId>>>,
    cancel: CancellationToken,
    tasks: TaskTracker,
    nonce: [u8; 8],
    sequence: AtomicU64,
    pool: Arc<pool::PacketPool>,
}

struct Handle {
    shared: Arc<Shared>,
    messages: AsyncMutex<mpsc::Receiver<Inbound>>,
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.shared.cancel.cancel();
    }
}

/// A bounded, multi-hop router over any number of heterogeneous adapters.
///
/// Adapters are generic at registration, then driven by typed tasks and bounded
/// channels; each link worker processes its inbound frames inline, and the hot
/// send path does not box futures. Adjacent peers and transit
/// routers are trusted for raw message attribution. Use the tunnel layer for
/// independent end-to-end authentication and confidentiality.
#[derive(Clone)]
pub struct Router {
    inner: Arc<Handle>,
}

impl fmt::Debug for Router {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Router")
            .field("local", self.local_id())
            .finish_non_exhaustive()
    }
}

impl Router {
    /// Starts a routing actor with the default [`RouterConfig`] on the current
    /// Tokio runtime.
    ///
    /// # Errors
    /// Returns `InvalidInput` wrapping the
    /// [`NodeIdError`](crate::NodeIdError) for an invalid local identifier;
    /// rejects unavailable OS randomness.
    /// # Panics
    /// Panics if called outside a Tokio runtime.
    pub fn new(local: NodeId) -> io::Result<Self> {
        Self::with_config(local, RouterConfig::default())
    }

    /// Starts a routing actor with explicit routing policy on the current
    /// Tokio runtime. Nothing starts when validation fails.
    ///
    /// # Errors
    /// Returns `InvalidInput` wrapping a [`RouterConfigError`] or the local
    /// identifier's [`NodeIdError`](crate::NodeIdError); rejects unavailable
    /// OS randomness.
    /// # Panics
    /// Panics if called outside a Tokio runtime.
    pub fn with_config(local: NodeId, config: RouterConfig) -> io::Result<Self> {
        config.validate()?;
        validate_local(&local)?;
        Self::start(local, config)
    }

    /// Starts a router whose identity and policy the caller already validated.
    pub(crate) fn start(local: NodeId, config: RouterConfig) -> io::Result<Self> {
        let mut nonce = [0; 8];
        SystemRandom::new()
            .fill(&mut nonce)
            .map_err(|_| io::Error::other("OS randomness unavailable"))?;
        let (events, receive) = mpsc::channel(config.event_queue.get());
        let (messages, message_rx) = mpsc::channel(config.message_queue.get());
        let (advertisements, _) = watch::channel(Arc::new(Vec::new()));
        let (reachable, _) = watch::channel(Arc::new(Vec::new()));
        let shared = Arc::new(Shared {
            local,
            inbound: Mutex::new(InboundState::new(&config)),
            pool: Arc::new(pool::PacketPool::new(
                config.packet_pool,
                config.max_frame.get(),
            )),
            config,
            table: Mutex::new(Table::default()),
            events,
            messages,
            tunnel: OnceLock::new(),
            advertisements,
            protocols: Mutex::new(protocol::Registry::default()),
            reachable,
            cancel: CancellationToken::new(),
            tasks: TaskTracker::new(),
            nonce,
            sequence: AtomicU64::new(0),
        });
        shared.tasks.spawn(drive(shared.clone(), receive));
        Ok(Self {
            inner: Arc::new(Handle {
                shared,
                messages: AsyncMutex::new(message_rx),
            }),
        })
    }

    /// Watches currently reachable destinations, including newly admitted peers.
    ///
    /// This is the route-readiness notification: wait here for a destination to
    /// appear before sending or connecting, rather than polling
    /// [`route_to`](Self::route_to). It is discovery, not additional link admission.
    #[must_use]
    pub fn reachable(&self) -> watch::Receiver<Arc<Vec<NodeId>>> {
        self.inner.shared.reachable.subscribe()
    }

    /// Registers and starts an adapter. Neighbor lists are explicit link admission.
    ///
    /// # Errors
    /// Returns `InvalidInput` wrapping the link's
    /// [`LinkConfigError`](groupnet_transport::link::LinkConfigError) or a
    /// [`LinkRegistrationError`]; rejects exhausted adapter capacity or a
    /// closed router.
    /// # Panics
    /// Panics if an earlier panic poisoned the internal table or no Tokio runtime exists.
    pub fn add_transport<T: Transport>(
        &self,
        transport: T,
        config: LinkConfig,
    ) -> io::Result<TransportId> {
        self.attach_link(BoundLink::new(transport, config))
            .map_err(|(error, _link)| error)
    }

    /// Registers a bound provider endpoint and owns its complete worker lifecycle.
    /// Failed registration drains the endpoint before returning.
    ///
    /// # Errors
    /// Returns `InvalidInput` wrapping the link's
    /// [`LinkConfigError`](groupnet_transport::link::LinkConfigError) or a
    /// [`LinkRegistrationError`]; rejects exhausted capacity or a closed router.
    /// # Panics
    /// Requires a Tokio runtime; propagates a poisoned routing-state lock.
    pub async fn add_link(&self, link: BoundLink) -> io::Result<TransportId> {
        match self.attach_link(link) {
            Ok(id) => Ok(id),
            Err((error, link)) => {
                link.driver.close().await;
                Err(error)
            }
        }
    }

    fn attach_link(&self, link: BoundLink) -> Result<TransportId, (io::Error, BoundLink)> {
        let shared = &self.inner.shared;
        let config = &link.config;
        if let Err(error) = config.validate_for(&shared.local) {
            return Err((error.into(), link));
        }
        let mtu = config.mtu.get();
        let max_frame = shared.config.max_frame.get();
        let max_routes = shared.config.max_routes.get();
        if config.peers.len() > max_routes {
            let error = LinkRegistrationError::PeersAboveMaxRoutes {
                peers: config.peers.len(),
                max_routes,
            };
            return Err((error.into(), link));
        }
        let min = wire::FRAGMENT + 1;
        if !(min..=max_frame).contains(&mtu) {
            let error = LinkRegistrationError::MtuOutOfRange {
                mtu,
                min,
                max: max_frame,
            };
            return Err((error.into(), link));
        }
        let fragments = max_frame.div_ceil(mtu - wire::FRAGMENT);
        let max_fragments = usize::from(shared.config.reassembly.max_fragments.get());
        if fragments > max_fragments {
            let error = LinkRegistrationError::FragmentsAboveReassembly {
                fragments,
                max_fragments,
            };
            return Err((error.into(), link));
        }
        let mut table = shared.table.lock().expect("router table poisoned");
        if shared.cancel.is_cancelled() {
            return Err((closed(), link));
        }
        if table.links.len() >= shared.config.max_transports.get() {
            return Err((io::Error::other("transport capacity reached"), link));
        }
        let Some(next) = table.next_link.checked_add(1) else {
            return Err((io::Error::other("transport identifier exhausted"), link));
        };
        let id = TransportId(table.next_link);
        table.next_link = next;
        let (send, outgoing) = mpsc::channel(shared.config.link_queue.get());
        let cancel = shared.cancel.child_token();
        let BoundLink {
            config,
            driver,
            sessions,
        } = link;
        let control = driver.control();
        let io = adapters::io(
            shared.clone(),
            id,
            config.peers.clone(),
            sessions.as_ref(),
            config.mtu.get(),
            outgoing,
            cancel.clone(),
        );
        table.links.insert(
            id,
            Link {
                config,
                control,
                outgoing: send,
                cancel: cancel.clone(),
                sessions: sessions.clone(),
            },
        );
        // Register the task under the same lock used by close's shutdown barrier.
        shared.tasks.spawn(driver.run(io));
        if let Some(sessions) = sessions {
            shared.tasks.spawn(adapters::neighbors(
                shared.clone(),
                id,
                sessions.subscribe(),
                cancel,
            ));
        }
        drop(table);
        let _ = shared.events.try_send(Event::Announce);
        Ok(id)
    }

    /// Stops a registered adapter and immediately invalidates routes using it.
    ///
    /// # Panics
    /// Panics if an earlier panic poisoned the internal table.
    pub fn remove_transport(&self, transport: TransportId) {
        self.inner
            .shared
            .table
            .lock()
            .expect("router table poisoned")
            .remove(transport);
        let _ = self.inner.shared.events.try_send(Event::Announce);
    }

    /// This router's logical peer identity.
    #[must_use]
    pub fn local_id(&self) -> &NodeId {
        &self.inner.shared.local
    }

    /// Returns a live selected route, or `None` when the destination is unreachable.
    ///
    /// # Panics
    /// Panics if an earlier panic poisoned the internal table.
    #[must_use]
    pub fn route_to(&self, peer: &NodeId) -> Option<Route> {
        self.inner
            .shared
            .table
            .lock()
            .expect("router table poisoned")
            .route(peer, self.inner.shared.config.route_ttl)
            .cloned()
    }

    /// Forwards an address hint only to links that explicitly admit this peer.
    ///
    /// Learning an address never expands adjacent-peer admission. The controls
    /// retained in the routing table do not keep closed endpoints alive.
    ///
    /// # Panics
    /// Panics if an earlier panic poisoned the internal table.
    pub fn learn_peer(&self, peer: &NodeId, address: &str) {
        let shared = &self.inner.shared;
        if shared.cancel.is_cancelled() {
            return;
        }
        let table = shared.table.lock().expect("router table poisoned");
        for link in table.links.values() {
            let admitted = link.sessions.as_ref().map_or_else(
                || link.config.peers.contains(peer),
                |sessions| {
                    sessions
                        .subscribe()
                        .borrow()
                        .iter()
                        .any(|entry| &entry.node == peer)
                },
            );
            if admitted {
                link.control.learn_peer(peer, address);
            }
        }
    }

    /// Initiates shutdown without waiting; use [`close`](Self::close) to drain tasks.
    pub fn shutdown(&self) {
        self.inner.shared.cancel.cancel();
    }

    /// Whether shared router shutdown has been initiated.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.inner.shared.cancel.is_cancelled()
    }

    /// Waits for shutdown to be initiated, without waiting for tasks to drain.
    pub async fn cancelled(&self) {
        self.inner.shared.cancel.cancelled().await;
    }

    /// Cancels the routing actor/adapters and waits for owned tasks to terminate.
    /// All clones share this shutdown. Pending receivers wake with an error.
    ///
    /// # Panics
    /// Propagates a poisoned routing-state lock.
    pub async fn close(&self) {
        self.inner.shared.cancel.cancel();
        // A concurrent registration must finish spawning its owned worker first.
        drop(
            self.inner
                .shared
                .table
                .lock()
                .expect("router table poisoned"),
        );
        self.inner.shared.tasks.close();
        self.inner.shared.tasks.wait().await;
    }

    pub(crate) fn tunnel_packet_buffer(
        &self,
        to: &NodeId,
        capacity: usize,
    ) -> io::Result<PacketBuffer> {
        PacketBuffer::new(&self.inner.shared, to, capacity, PayloadKind::Tunnel)
    }

    /// Tunnel packet buffers toward `to` that do not keep this router alive.
    pub(crate) fn tunnel_buffers(&self, to: &NodeId) -> TunnelBuffers {
        TunnelBuffers::new(self.inner.shared.clone(), to.clone())
    }

    pub(crate) fn send_tunnel_packet(&self, to: &NodeId, packet: PacketBuffer) -> io::Result<()> {
        self.inner
            .shared
            .send_packet(to, packet, PayloadKind::Tunnel)
    }

    pub(crate) fn send_tunnel_retained(
        &self,
        to: &NodeId,
        packet: PacketBuffer,
    ) -> io::Result<Bytes> {
        let shared = &self.inner.shared;
        if shared.cancel.is_cancelled() {
            return Err(closed());
        }
        let (bytes, offset) = packet.finish(shared, to, PayloadKind::Tunnel)?;
        let retained = bytes.slice(offset..);
        if to == self.local_id() {
            shared.deliver_owned(
                PayloadKind::Tunnel,
                self.local_id().clone(),
                retained.clone(),
            )?;
        } else {
            shared.forward(to, bytes);
        }
        Ok(retained)
    }

    /// The validated largest complete routing envelope.
    pub(crate) fn max_frame(&self) -> usize {
        self.inner.shared.config.max_frame.get()
    }

    /// Installs this router's single tunnel endpoint. Link workers hand it every
    /// locally addressed tunnel packet; unclaimed tunnel traffic is dropped.
    pub(crate) fn claim_tunnels(&self, inbox: Arc<dyn TunnelInbox>) -> io::Result<()> {
        self.inner.shared.tunnel.set(inbox).map_err(|_| {
            io::Error::new(
                io::ErrorKind::AlreadyExists,
                "router already owns a tunnel endpoint",
            )
        })
    }

    /// Exclusively binds a bounded application protocol namespace.
    ///
    /// Registration is shared by clones and released on shutdown or last drop.
    /// Unknown protocol identifiers are discarded at the destination.
    ///
    /// # Errors
    /// Returns `AlreadyExists`, `WouldBlock`, or `NotConnected` for duplicate,
    /// exhausted, or closed registrations.
    pub fn bind_protocol(&self, id: ProtocolId) -> io::Result<ProtocolIo> {
        protocol::bind(self.clone(), id)
    }

    /// Returns a child token cancelled when this router shuts down.
    ///
    /// An endpoint may cancel its token without shutting down the router or another
    /// endpoint. Keep the token alongside a claimed channel to observe shutdown.
    #[must_use]
    pub fn cancellation(&self) -> CancellationToken {
        self.inner.shared.cancel.child_token()
    }
}

impl Transport for Router {
    type Error = io::Error;

    fn learn_peer(&self, peer: &NodeId, address: &str) {
        Self::learn_peer(self, peer, address);
    }

    fn send(&self, to: &NodeId, msg: &[u8]) -> impl Future<Output = io::Result<()>> {
        std::future::ready(self.inner.shared.send(to, msg, PayloadKind::Message))
    }

    fn send_owned_admitted(
        &self,
        to: &NodeId,
        msg: Bytes,
        session: Option<SessionId>,
    ) -> impl Future<Output = io::Result<()>> {
        if session.is_some() {
            return std::future::ready(Ok(()));
        }
        std::future::ready(self.inner.shared.send_owned(to, msg, PayloadKind::Message))
    }

    async fn recv(&self) -> io::Result<Inbound> {
        receive(&self.inner.messages, &self.inner.shared.cancel).await
    }
}

async fn receive(
    receiver: &AsyncMutex<mpsc::Receiver<Inbound>>,
    cancel: &CancellationToken,
) -> io::Result<Inbound> {
    tokio::select! {
        biased;
        () = cancel.cancelled() => Err(closed()),
        message = async { receiver.lock().await.recv().await } => message.ok_or_else(closed),
    }
}

fn closed() -> io::Error {
    io::Error::new(io::ErrorKind::NotConnected, "router closed")
}

/// Rejects a local identity the routing wire cannot carry.
pub(crate) fn validate_local(local: &NodeId) -> io::Result<()> {
    Ok(validate_node_id(local, MAX_NODE_ID_BYTES)?)
}
