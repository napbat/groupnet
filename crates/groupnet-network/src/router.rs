//! Router lifecycle, typed links, path-vector learning, and packet forwarding.

mod adapters;
mod routing;

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

use groupnet_core::NodeId;
use groupnet_transport::admission::{SessionId, SessionRegistry};
use groupnet_transport::link::{AdmittedInbound, BoundLink, LinkConfig, LinkControl};
use groupnet_transport::{Inbound, Transport};
use ring::rand::{SecureRandom, SystemRandom};
use tokio::sync::{Mutex as AsyncMutex, mpsc, watch};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::wire::{self, PayloadKind};

/// A local, opaque transport registration identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TransportId(usize);

/// Routing and resource policy for one node.
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
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            forwarding: true,
            max_routes: 128,
            max_transports: 16,
            route_ttl: Duration::from_secs(6),
            announce_interval: Duration::from_secs(1),
        }
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
    bytes: Arc<[u8]>,
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
    frame: Arc<[u8]>,
}

struct Shared {
    local: NodeId,
    config: RouterConfig,
    table: Mutex<Table>,
    events: mpsc::Sender<Event>,
    messages: mpsc::Sender<Inbound>,
    tunnels: mpsc::Sender<Inbound>,
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
        if !wire::id_valid(&local)
            || config.max_routes == 0
            || config.max_routes > 4096
            || config.max_transports == 0
            || config.max_transports > 256
            || config.announce_interval.is_zero()
            || config.route_ttl <= config.announce_interval
        {
            return Err(wire::invalid("invalid router configuration"));
        }
        let mut nonce = [0; 8];
        SystemRandom::new()
            .fill(&mut nonce)
            .map_err(|_| io::Error::other("OS randomness unavailable"))?;
        let (events, receive) = mpsc::channel(256);
        let (messages, message_rx) = mpsc::channel(64);
        let (tunnels, tunnel_rx) = mpsc::channel(256);
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
    /// This is discovery, not additional link admission.
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
            || !(128..=wire::MAX_FRAME).contains(&config.mtu)
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
        let (send, outgoing) = mpsc::channel(64);
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

    pub(crate) fn send_tunnel(&self, to: &NodeId, payload: &[u8]) -> io::Result<()> {
        self.inner.shared.send(to, payload, PayloadKind::Tunnel)
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

    pub(crate) fn cancellation(&self) -> CancellationToken {
        self.inner.shared.cancel.clone()
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
