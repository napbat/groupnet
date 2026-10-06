//! Protocol-neutral registration and owned link workers.
//!
//! Providers bind typed protocol configuration without knowing about a router.
//! Dynamic dispatch occurs at initialization and worker lifetime boundaries;
//! individual transport send/receive futures remain statically dispatched.

mod worker;

use crate::{Inbound, Transport};
use futures_util::{Sink, Stream};
use groupnet_core::NodeId;
use std::{fmt, future::Future, io, pin::Pin, sync::Arc};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// A future erased once at an initialization or lifecycle boundary.
pub type LinkFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A logical adjacent peer and its protocol-specific physical address.
#[derive(Clone, Debug)]
pub struct PeerEndpoint<A> {
    /// Logical peer identity.
    pub node: NodeId,
    /// Typed physical address.
    pub address: A,
}
impl<A> PeerEndpoint<A> {
    /// Pairs a logical identity and a physical address.
    #[must_use]
    pub fn new(node: NodeId, address: A) -> Self {
        Self { node, address }
    }
}

/// Admission, cost, and frame bounds for one adjacent-peer link.
#[derive(Clone, Debug)]
pub struct LinkConfig {
    /// Admitted adjacent peers; routing rejects other sources.
    pub peers: Vec<NodeId>,
    /// Positive path cost.
    pub cost: u32,
    /// Maximum physical frame size supported by the link.
    pub mtu: usize,
}
impl LinkConfig {
    /// Creates an equal-cost link supporting frames up to 65,000 bytes.
    #[must_use]
    pub fn new(peers: Vec<NodeId>) -> Self {
        Self {
            peers,
            cost: 1,
            mtu: 65_000,
        }
    }
}

/// A configured link implementation, consumed when the network starts.
/// Binding must release partial resources on failure or cancellation.
pub trait LinkProvider: Send + fmt::Debug + 'static {
    /// Explicit adjacent peers, used as initial membership seeds.
    fn peers(&self) -> &[NodeId];
    /// Binds this implementation for the supplied local identity.
    fn bind(self: Box<Self>, local: NodeId) -> LinkFuture<'static, io::Result<BoundLink>>;
}

/// Cleanup of independently spawned adapter tasks, beyond the generic I/O workers.
/// Both operations must be idempotent. A socket-only adapter needs no lifecycle
/// object: dropping its worker releases its socket directly.
pub trait LinkLifecycle: Send + Sync + fmt::Debug + 'static {
    /// Synchronously initiates cancellation of all owned protocol tasks.
    fn shutdown(&self);
    /// Waits for owned protocol tasks and resource cleanup to finish.
    fn close(&self) -> LinkFuture<'_, ()>;
}

#[derive(Debug)]
enum Buffer {
    Shared(Arc<[u8]>),
    Owned(Vec<u8>),
}

/// One physical frame, preserving shared or uniquely owned bytes without copying.
#[derive(Debug)]
pub struct Outbound {
    /// Adjacent recipient.
    pub peer: NodeId,
    /// Deadline shared by every fragment of the original routed frame.
    pub deadline: Instant,
    bytes: Buffer,
}
impl Outbound {
    /// Queues a shared frame without copying its bytes.
    #[must_use]
    pub fn shared(peer: NodeId, bytes: Arc<[u8]>, deadline: Instant) -> Self {
        Self {
            peer,
            bytes: Buffer::Shared(bytes),
            deadline,
        }
    }
    /// Queues a uniquely owned frame without converting/copying its allocation.
    #[must_use]
    pub fn owned(peer: NodeId, bytes: Vec<u8>, deadline: Instant) -> Self {
        Self {
            peer,
            bytes: Buffer::Owned(bytes),
            deadline,
        }
    }
    /// The frame payload.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        match &self.bytes {
            Buffer::Shared(bytes) => bytes,
            Buffer::Owned(bytes) => bytes,
        }
    }
}

/// Router-neutral worker endpoints. Streams/sinks are erased once, not per frame.
/// The incoming sink receives `None` when the transport fails. Implementations
/// must preserve backpressure; the shared workers add no extra message queues.
pub struct LinkIo {
    /// Scheduled physical frames, including any router-generated fragments.
    pub outgoing: Pin<Box<dyn Stream<Item = Outbound> + Send>>,
    /// Incoming frames or a terminal transport failure.
    pub incoming: Pin<Box<dyn Sink<Option<Inbound>, Error = io::Error> + Send>>,
    /// Cancellation owned by the registering network.
    pub cancel: CancellationToken,
    /// Largest accepted incoming physical frame.
    pub mtu: usize,
}
impl fmt::Debug for LinkIo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LinkIo")
            .field("mtu", &self.mtu)
            .finish_non_exhaustive()
    }
}

/// A bound protocol endpoint plus the routing metadata needed to register it.
#[derive(Debug)]
pub struct BoundLink {
    /// Adjacent-peer admission, cost, and frame bounds.
    pub config: LinkConfig,
    /// Owned worker and optional protocol-task lifecycle.
    pub driver: LinkDriver,
}
impl BoundLink {
    /// Prepares statically dispatched workers for any transport implementation.
    #[must_use]
    pub fn new<T: Transport>(transport: T, config: LinkConfig) -> Self {
        Self {
            config,
            driver: LinkDriver {
                worker: Some(Box::new(worker::Typed(transport))),
                lifecycle: None,
            },
        }
    }
    /// Attaches cleanup for an adapter's independently spawned protocol tasks.
    #[must_use]
    pub fn with_lifecycle(mut self, lifecycle: Arc<dyn LinkLifecycle>) -> Self {
        self.driver.lifecycle = Some(lifecycle);
        self
    }
}

/// Single-use, protocol-neutral worker owner. Drop initiates protocol shutdown;
/// [`run`](Self::run) and [`close`](Self::close) also drain protocol tasks.
pub struct LinkDriver {
    worker: Option<Box<dyn worker::Worker>>,
    lifecycle: Option<Arc<dyn LinkLifecycle>>,
}
impl fmt::Debug for LinkDriver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LinkDriver")
            .field("lifecycle", &self.lifecycle)
            .finish_non_exhaustive()
    }
}
impl LinkDriver {
    /// Runs typed I/O until cancellation, input exhaustion, or transport failure,
    /// then cancels and drains the protocol's independently owned tasks.
    ///
    /// # Panics
    /// Only if the internal single-use worker invariant is violated.
    pub fn run(mut self, io: LinkIo) -> LinkFuture<'static, ()> {
        let worker = self.worker.take().expect("single-use link worker");
        Box::pin(async move {
            worker.run(io).await;
            self.close().await;
        })
    }
    /// Cancels and drains an endpoint that could not be registered.
    pub async fn close(self) {
        if let Some(lifecycle) = &self.lifecycle {
            lifecycle.shutdown();
            lifecycle.close().await;
        }
    }
}
impl Drop for LinkDriver {
    fn drop(&mut self) {
        if let Some(lifecycle) = &self.lifecycle {
            lifecycle.shutdown();
        }
    }
}
