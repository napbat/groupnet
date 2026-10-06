//! **Named seeds**: seed peers addressed by a `host:port` name (a DNS record, or
//! a literal address) instead of a socket address the caller resolved once.
//!
//! A seed's address is a moving target in any orchestrated deployment: a
//! `StatefulSet` peer's headless DNS record is published only after its pod
//! starts, and a rolling restart hands every pod a fresh IP one at a time. A
//! book filled by a single startup resolution goes stale and the cluster wedges
//! (datagrams fly to dead addresses, and a rebooted peer is deaf to us until our
//! datagrams come from an address it knows). So the runtime owns the
//! resolution, off the startup path, for the life of the node:
//!
//! * **First resolution** is attempted at once, then every
//!   [`retry_interval`](NamedSeeds::retry_interval) until it succeeds. After
//!   [`startup_attempts`](NamedSeeds::startup_attempts) failures the seed is
//!   reported [`SeedEvent::Unresolved`] once and from then on retried at the
//!   refresh cadence — a seed is never abandoned.
//! * **Re-resolution** runs every [`refresh_interval`](NamedSeeds::refresh_interval)
//!   once a seed has resolved. A changed address is taught to the transport and
//!   reported [`SeedEvent::Resolved`] with the address it replaced; an unchanged
//!   one is not re-taught. A failed lookup keeps the last address — a DNS blip
//!   must not churn the book.
//!
//! Every named seed joins the node's seed set at [`start`](crate::NodeBuilder::start),
//! so every group contacts it the moment its address is known; until then a
//! send to it is a transport-level drop, which the protocol tolerates. Startup
//! never waits on DNS.
//!
//! Addresses reach admitted links through [`Transport::learn_peer`] — the same
//! path gossiped advertisements take — so address-book bindings (UDP,
//! persistent TCP) work unchanged. Resolution never grants link admission.
//! The lookup itself is a [`SeedResolver`]: [`SystemResolver`] (feature `dns`) in
//! production, any other implementation in tests.

use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::time::Duration;

use groupnet_core::NodeId;
use groupnet_transport::Transport;
use tokio::time::Instant;

/// How long a never-resolved seed waits between attempts by default.
pub const DEFAULT_RETRY_INTERVAL: Duration = Duration::from_secs(1);

/// How many failed attempts a never-resolved seed gets by default before it is
/// reported [`SeedEvent::Unresolved`] and moves to the refresh cadence.
pub const DEFAULT_STARTUP_ATTEMPTS: u32 = 30;

/// How often a resolved seed is re-resolved by default.
pub const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(5);

/// The smallest accepted interval: a zero would busy-spin the resolver task.
const MIN_INTERVAL: Duration = Duration::from_millis(1);

/// A future returned by [`SeedResolver::resolve`].
///
/// Boxed so the trait stays **dyn-compatible**: [`NamedSeeds`] holds an
/// `Arc<dyn SeedResolver>`. One allocation per lookup is noise beside a DNS
/// round trip.
pub type ResolveFuture<'a> = Pin<Box<dyn Future<Output = io::Result<SocketAddr>> + Send + 'a>>;

/// Resolves a seed's `host:port` name to the one socket address to reach it at.
pub trait SeedResolver: Send + Sync + 'static {
    /// Resolve `name`. An `Err` is a failed attempt; the seed keeps its last
    /// address and is retried on schedule.
    fn resolve<'a>(&'a self, name: &'a str) -> ResolveFuture<'a>;
}

/// The operating system's resolver (`getaddrinfo` via Tokio): the first
/// address a name resolves to.
#[cfg(feature = "dns")]
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemResolver;

#[cfg(feature = "dns")]
impl SeedResolver for SystemResolver {
    fn resolve<'a>(&'a self, name: &'a str) -> ResolveFuture<'a> {
        Box::pin(async move {
            tokio::net::lookup_host(name).await?.next().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("{name} resolved to no socket addresses"),
                )
            })
        })
    }
}

/// What the resolver task reports through [`NamedSeeds::on_event`].
#[derive(Debug)]
pub enum SeedEvent {
    /// A seed resolved to an address it did not hold before, and the transport
    /// was taught it. `previous` is `None` on the first resolution and the
    /// replaced address when the seed moved.
    Resolved {
        /// The seed.
        node: NodeId,
        /// Its configured name.
        name: String,
        /// The address now taught to the transport.
        addr: SocketAddr,
        /// The address it replaced, if any.
        previous: Option<SocketAddr>,
    },
    /// A seed failed its whole startup window without ever resolving. Reported
    /// once per seed; resolution continues at the refresh cadence.
    Unresolved {
        /// The seed.
        node: NodeId,
        /// Its configured name.
        name: String,
        /// The last lookup's failure.
        error: io::Error,
    },
}

/// The observer a [`NamedSeeds`] reports to.
type Observer = Arc<dyn Fn(&SeedEvent) + Send + Sync>;

/// A set of named seeds and the policy that resolves them — handed to
/// [`NodeBuilder::named_seeds`](crate::NodeBuilder::named_seeds).
///
/// ```no_run
/// # #[cfg(feature = "dns")]
/// # async fn demo(link: impl groupnet_transport::link::LinkProvider) -> std::io::Result<()> {
/// use groupnet_core::NodeId;
/// use groupnet_runtime::{NamedSeeds, Node, SystemResolver};
///
/// let node = Node::builder(NodeId::new("cache-1"))
///     .link(link)
///     .named_seeds(
///         NamedSeeds::new(SystemResolver)
///             .seed(NodeId::new("cache-0"), "cache-0.cache.svc:7000"),
///     )
///     .start().await?;
/// # let _ = node;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct NamedSeeds {
    seeds: Vec<(NodeId, String)>,
    resolver: Arc<dyn SeedResolver>,
    retry_interval: Duration,
    startup_attempts: u32,
    refresh_interval: Duration,
    observer: Option<Observer>,
}

impl fmt::Debug for NamedSeeds {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NamedSeeds")
            .field("seeds", &self.seeds)
            .field("retry_interval", &self.retry_interval)
            .field("startup_attempts", &self.startup_attempts)
            .field("refresh_interval", &self.refresh_interval)
            .finish_non_exhaustive()
    }
}

impl NamedSeeds {
    /// An empty seed set resolved through `resolver`, with the default policy
    /// ([`DEFAULT_RETRY_INTERVAL`], [`DEFAULT_STARTUP_ATTEMPTS`],
    /// [`DEFAULT_REFRESH_INTERVAL`]).
    #[must_use]
    pub fn new(resolver: impl SeedResolver) -> Self {
        Self {
            seeds: Vec::new(),
            resolver: Arc::new(resolver),
            retry_interval: DEFAULT_RETRY_INTERVAL,
            startup_attempts: DEFAULT_STARTUP_ATTEMPTS,
            refresh_interval: DEFAULT_REFRESH_INTERVAL,
            observer: None,
        }
    }

    /// Adds `node`, reachable at the `host:port` `name`. A seed naming the
    /// local node is ignored at start, so one shared all-replicas list can
    /// configure every replica.
    #[must_use]
    pub fn seed(mut self, node: NodeId, name: impl Into<String>) -> Self {
        self.seeds.push((node, name.into()));
        self
    }

    /// The wait between attempts while a seed has never resolved (floored at
    /// 1 ms).
    #[must_use]
    pub fn retry_interval(mut self, interval: Duration) -> Self {
        self.retry_interval = interval.max(MIN_INTERVAL);
        self
    }

    /// How many failed attempts a never-resolved seed gets before it is
    /// reported [`SeedEvent::Unresolved`] (floored at 1).
    #[must_use]
    pub fn startup_attempts(mut self, attempts: u32) -> Self {
        self.startup_attempts = attempts.max(1);
        self
    }

    /// The re-resolution cadence once a seed has resolved, and the retry
    /// cadence after its startup window (floored at 1 ms).
    #[must_use]
    pub fn refresh_interval(mut self, interval: Duration) -> Self {
        self.refresh_interval = interval.max(MIN_INTERVAL);
        self
    }

    /// Reports every [`SeedEvent`] to `observer` (logging, readiness). Called
    /// on the resolver task; keep it cheap and non-blocking.
    #[must_use]
    pub fn on_event(mut self, observer: impl Fn(&SeedEvent) + Send + Sync + 'static) -> Self {
        self.observer = Some(Arc::new(observer));
        self
    }

    /// The seeds' node ids, in configuration order.
    pub fn nodes(&self) -> impl Iterator<Item = &NodeId> {
        self.seeds.iter().map(|(node, _)| node)
    }

    fn emit(&self, event: &SeedEvent) {
        if let Some(observer) = &self.observer {
            observer(event);
        }
    }
}

/// One seed's resolution state.
struct Slot {
    node: NodeId,
    name: String,
    /// The address last taught to the transport.
    current: Option<SocketAddr>,
    /// Failed attempts before the first success.
    failures: u32,
    /// When the next attempt is due.
    due: Instant,
}

/// Resolves `seeds` for the life of `transport`, teaching each new address via
/// [`Transport::learn_peer`]. Ends when the transport is dropped; the managed
/// node also cancels this future when its router is cancelled. Seeds naming
/// `local` are skipped.
pub(crate) async fn resolve_named_seeds<T: Transport>(
    transport: Weak<T>,
    local: NodeId,
    seeds: NamedSeeds,
) {
    let start = Instant::now();
    let mut slots: Vec<Slot> = seeds
        .seeds
        .iter()
        .filter(|(node, _)| *node != local)
        .map(|(node, name)| Slot {
            node: node.clone(),
            name: name.clone(),
            current: None,
            failures: 0,
            due: start,
        })
        .collect();
    loop {
        let Some(next) = slots.iter().map(|slot| slot.due).min() else {
            return;
        };
        tokio::time::sleep_until(next).await;
        for slot in &mut slots {
            if slot.due > Instant::now() {
                continue;
            }
            let outcome = seeds.resolver.resolve(&slot.name).await;
            let Some(transport) = transport.upgrade() else {
                return;
            };
            slot.due = Instant::now() + attempt(&*transport, slot, outcome, &seeds);
        }
    }
}

/// Applies one lookup's `outcome` to `slot` and returns the wait before its
/// next attempt.
fn attempt<T: Transport>(
    transport: &T,
    slot: &mut Slot,
    outcome: io::Result<SocketAddr>,
    seeds: &NamedSeeds,
) -> Duration {
    match outcome {
        Ok(addr) => {
            if slot.current != Some(addr) {
                transport.learn_peer(&slot.node, &addr.to_string());
                let previous = slot.current.replace(addr);
                seeds.emit(&SeedEvent::Resolved {
                    node: slot.node.clone(),
                    name: slot.name.clone(),
                    addr,
                    previous,
                });
            }
            seeds.refresh_interval
        }
        Err(_) if slot.current.is_some() => seeds.refresh_interval,
        Err(error) => {
            slot.failures = slot.failures.saturating_add(1);
            if slot.failures < seeds.startup_attempts {
                return seeds.retry_interval;
            }
            if slot.failures == seeds.startup_attempts {
                seeds.emit(&SeedEvent::Unresolved {
                    node: slot.node.clone(),
                    name: slot.name.clone(),
                    error,
                });
            }
            seeds.refresh_interval
        }
    }
}

#[cfg(test)]
mod tests;
