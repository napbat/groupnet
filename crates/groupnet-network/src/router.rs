//! Router lifecycle, typed links, path-vector learning, and packet forwarding.

mod adapters;
mod packet;
mod protocol;
mod routing;

pub use crate::wire::ReassemblyConfig;
pub use packet::PacketBuffer;

pub use protocol::{ProtocolId, ProtocolIo};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod admission_tests;

#[cfg(test)]
mod outbound_tests;

use routing::drive;

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

use bytes::Bytes;
use groupnet_core::NodeId;
use groupnet_transport::admission::{SessionId, SessionRegistry};
use groupnet_transport::link::{AdmittedInbound, BoundLink, LinkConfig, LinkControl};
use groupnet_transport::{Inbound, QueueCapacity, Transport};
use ring::rand::{SecureRandom, SystemRandom};
use tokio::sync::{Mutex as AsyncMutex, mpsc, watch};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::wire::{self, PayloadKind};

/// A local, opaque transport registration identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TransportId(usize);

/// Routing and resource policy for one node.
///
/// Queue sizing: one bulk tunnel stream keeps about `2 × window` frames queued
/// (a window of data plus its acknowledgements). Size `link_queue` for the bulk
/// streams sharing a link and `tunnel_queue` for those terminating at this node;
/// the defaults hold two and eight default-window (64-segment) streams. A full
/// link or tunnel queue drops frames, which streams repair as loss.
#[derive(Clone, Debug)]
pub struct RouterConfig {
    /// Whether this node forwards transit traffic between admitted peers.
    /// Enabled by default; set to `false` for an endpoint-only node.
    pub forwarding: bool,
    /// Maximum learned destinations and configured neighbors per transport.
    pub max_routes: usize,
    /// Maximum simultaneous registered transport adapters.
    pub max_transports: usize,
    /// Expiration time for an unrefreshed route.
    pub route_ttl: Duration,
    /// Interval between bounded path-vector announcements.
    pub announce_interval: Duration,
    /// Largest complete routing envelope; must fit the fragment u32 total.
    pub max_frame: usize,
    /// Maximum path length and data hop budget; must fit u8.
    pub max_hops: usize,
    /// Retained incomplete-fragment bounds.
    pub reassembly: ReassemblyConfig,
    /// Maximum exclusive application namespaces; must fit u16 IDs.
    pub max_protocols: usize,
    /// Bounded inbox capacity for each application namespace.
    pub protocol_queue: QueueCapacity,
    /// Bounded router event queue capacity.
    pub event_queue: QueueCapacity,
    /// Bounded coordination inbox capacity.
    pub message_queue: QueueCapacity,
    /// Bounded tunnel inbox capacity shared by this node's tunnel sessions.
    pub tunnel_queue: QueueCapacity,
    /// Bounded outbound queue capacity for each link.
    pub link_queue: QueueCapacity,
    /// Number of recent routed identities retained for loop/replay suppression.
    pub replay_capacity: usize,
    /// Physical-send deadline shared by all fragments of a frame.
    pub send_timeout: Duration,
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            forwarding: true,
            max_routes: 128,
            max_transports: 16,
            route_ttl: Duration::from_secs(6),
            announce_interval: Duration::from_secs(1),
            max_frame: wire::MAX_FRAME,
            max_hops: wire::MAX_HOPS,
            reassembly: ReassemblyConfig::default(),
            max_protocols: 32,
            protocol_queue: QueueCapacity::of(128),
            event_queue: QueueCapacity::of(256),
            message_queue: QueueCapacity::of(64),
            tunnel_queue: QueueCapacity::of(1024),
            link_queue: QueueCapacity::of(256),
            replay_capacity: 4096,
            send_timeout: Duration::from_secs(5),
        }
    }
}

impl RouterConfig {
    /// Validates wire representability and bounded resource policy; queue
    /// capacities carry their own range.
    /// # Errors
    /// Rejects zero bounds, impossible wire bounds, or invalid timers.
    pub fn validate(&self) -> io::Result<()> {
        let now = tokio::time::Instant::now();
        let timers = [
            self.reassembly.timeout,
            self.send_timeout,
            self.announce_interval,
            self.route_ttl,
        ];
        if self.max_routes == 0
            || self.max_transports == 0
            || self.max_frame < wire::MAX_DATA_HEADER
            || u32::try_from(self.max_frame).is_err()
            || self.max_frame > isize::MAX as usize
            || !(2..=usize::from(u8::MAX)).contains(&self.max_hops)
            || self.max_protocols == 0
            || self.max_protocols > usize::from(u16::MAX) + 1
            || self.replay_capacity == 0
            || self.reassembly.max_pending == 0
            || self.reassembly.max_fragments == 0
            || self.reassembly.max_fragments > usize::from(u16::MAX)
            || self.reassembly.timeout.is_zero()
            || self.send_timeout.is_zero()
            || self.announce_interval.is_zero()
            || self.route_ttl <= self.announce_interval
            || timers
                .iter()
                .any(|duration| now.checked_add(*duration).is_none())
        {
            return Err(wire::invalid("invalid router configuration"));
        }
        Ok(())
    }
}

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
    Received {
        link: TransportId,
        packet: AdmittedInbound,
    },
    Neighbors(TransportId),
    Down(TransportId),
    Announce,
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
    events: mpsc::Sender<Event>,
    messages: mpsc::Sender<Inbound>,
    tunnels: mpsc::Sender<Inbound>,
    protocols: Mutex<protocol::Registry>,
    advertisements: watch::Sender<Arc<Vec<AdvertisedRoute>>>,
    reachable: watch::Sender<Arc<Vec<NodeId>>>,
    cancel: CancellationToken,
    tasks: TaskTracker,
    nonce: [u8; 8],
    sequence: AtomicU64,
}

struct Handle {
    shared: Arc<Shared>,
    messages: AsyncMutex<mpsc::Receiver<Inbound>>,
    tunnels: AsyncMutex<mpsc::Receiver<Inbound>>,
    tunnel_claimed: AtomicBool,
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.shared.cancel.cancel();
    }
}

/// A bounded, multi-hop router over any number of heterogeneous adapters.
///
/// Adapters are generic at registration, then driven by typed tasks and bounded
/// channels; the hot send path does not box futures. Adjacent peers and transit
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
    /// Starts a routing actor on the current Tokio runtime.
    ///
    /// # Errors
    /// Rejects invalid identifiers/bounds or unavailable OS randomness.
    /// # Panics
    /// Panics if called outside a Tokio runtime.
    pub fn new(local: NodeId, config: RouterConfig) -> io::Result<Self> {
        config.validate()?;
        if !wire::id_valid(&local) {
            return Err(wire::invalid("invalid router identity"));
        }
        let mut nonce = [0; 8];
        SystemRandom::new()
            .fill(&mut nonce)
            .map_err(|_| io::Error::other("OS randomness unavailable"))?;
        let (events, receive) = mpsc::channel(config.event_queue.get());
        let (messages, message_rx) = mpsc::channel(config.message_queue.get());
        let (tunnels, tunnel_rx) = mpsc::channel(config.tunnel_queue.get());
        let (advertisements, _) = watch::channel(Arc::new(Vec::new()));
        let (reachable, _) = watch::channel(Arc::new(Vec::new()));
        let shared = Arc::new(Shared {
            local,
            config,
            table: Mutex::new(Table::default()),
            events,
            messages,
            tunnels,
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
                tunnels: AsyncMutex::new(tunnel_rx),
                tunnel_claimed: AtomicBool::new(false),
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
    /// Rejects invalid peer IDs/cost/MTU, exhausted adapter capacity, or a closed router.
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
    /// Rejects invalid admission/cost/MTU, exhausted capacity, or a closed router.
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
        if config.cost == 0
            || config.peers.len() > shared.config.max_routes
            || config
                .peers
                .iter()
                .any(|peer| !wire::id_valid(peer) || peer == &shared.local)
            || !(wire::FRAGMENT + 1..=shared.config.max_frame).contains(&config.mtu)
            || shared
                .config
                .max_frame
                .div_ceil(config.mtu.saturating_sub(wire::FRAGMENT).max(1))
                > shared.config.reassembly.max_fragments
        {
            return Err((wire::invalid("invalid link configuration"), link));
        }
        let mut table = shared.table.lock().expect("router table poisoned");
        if shared.cancel.is_cancelled() {
            return Err((closed(), link));
        }
        if table.links.len() >= shared.config.max_transports {
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
            config.mtu,
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

    pub(crate) fn validate_tunnel_payload(&self, to: &NodeId, payload: usize) -> io::Result<()> {
        if !wire::id_valid(to)
            || payload
                > self
                    .inner
                    .shared
                    .config
                    .max_frame
                    .saturating_sub(PayloadKind::Tunnel.header_len(self.local_id(), to))
        {
            return Err(wire::invalid("tunnel payload exceeds router bound"));
        }
        Ok(())
    }

    pub(crate) async fn recv_tunnel(&self) -> io::Result<Inbound> {
        receive(&self.inner.tunnels, &self.inner.shared.cancel).await
    }

    pub(crate) fn claim_tunnels(&self) -> io::Result<()> {
        self.inner
            .tunnel_claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| {
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
