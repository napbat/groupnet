//! The in-process data plane: a [`MemBulkNet`] of per-node [`MemBulkTransport`]
//! endpoints wired through [`tokio::io::duplex`] pipes.
//!
//! The sibling of the control plane's [`Network`](crate::Network), one plane
//! down: where that one moves best-effort *datagrams* between endpoints, this
//! one opens reliable, ordered byte *streams* between them. Both are the same
//! shape — a shared fabric hands out per-node endpoints — so a test fixture
//! can hold one of each and hand a node its control-plane endpoint and its
//! data-plane endpoint from the same two lines.
//!
//! A [`tokio::io::DuplexStream`] is already `AsyncRead + AsyncWrite`; the only
//! glue is `tokio_util::compat` to present it as the runtime-agnostic
//! `futures-io` stream [`BulkTransport`] asks for. No sockets, no ports, no
//! handshake — the connector's id travels beside the pipe on the accept queue.
//!
//! ## Deliberate difference from the control plane
//!
//! Connecting to an unregistered id is an **error** here, where sending to one
//! on the control plane is a silent drop. That is not an inconsistency: a
//! best-effort datagram plane owes the caller nothing on delivery, while a
//! stream plane is connection-oriented — a caller that gets a stream back is
//! entitled to assume there is something on the other end of it. Real
//! bindings agree (a UDP `send_to` into the void succeeds; a TCP `connect` to
//! a closed port does not), so code written against the in-process fabric sees
//! the same error surface it will see over TCP.

use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};

use groupnet_core::NodeId;
use groupnet_transport::bulk::BulkTransport;
use tokio::io::DuplexStream;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::mpsc;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

use crate::MemBulkConfig;

/// What a connector puts on a target's accept queue: its own id, and the far
/// half of the pipe it just built.
type Incoming = (NodeId, DuplexStream);

type Queues = Arc<Mutex<HashMap<NodeId, mpsc::Sender<Incoming>>>>;

/// A shared in-process data-plane fabric. Clone it freely; every endpoint
/// created from clones shares one table of accept queues.
#[derive(Clone, Default, Debug)]
pub struct MemBulkNet {
    queues: Queues,
    config: MemBulkConfig,
}

impl MemBulkNet {
    /// Creates an empty data-plane fabric.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a data-plane fabric with bounded accept queues and pipes.
    ///
    /// # Errors
    /// Rejects zero or unsupported queue/pipe capacities.
    pub fn with_config(config: MemBulkConfig) -> io::Result<Self> {
        config.validate()?;
        Ok(Self {
            queues: Queues::default(),
            config,
        })
    }

    /// Creates and registers a data-plane endpoint for `id`.
    ///
    /// Registration installs an **accept queue** under `id`: every
    /// [`connect`](BulkTransport::connect) aimed at `id` builds a pipe and
    /// pushes its far half onto that queue, paired with the connector's id.
    /// [`accept`](BulkTransport::accept) pops them in arrival order. Connecting
    /// waits when the finite accept queue is full, before allocating a pipe.
    /// Each pipe also applies byte backpressure at its configured capacity.
    ///
    /// Re-registering an id **replaces** its queue, mirroring the control
    /// plane's [`Network::endpoint`](crate::Network::endpoint) eviction: new
    /// connections go to the new endpoint, while the evicted one still drains
    /// whatever was already queued and then sees its `accept` fail once the
    /// last sender clone is gone. Streams already open across the old endpoint
    /// are untouched — they are pipes, and own no part of the table.
    ///
    /// # Panics
    /// If the fabric's queue table was poisoned by a panic in another thread.
    #[must_use]
    pub fn endpoint(&self, id: NodeId) -> MemBulkTransport {
        let (tx, rx) = mpsc::channel(self.config.accept_queue);
        self.queues
            .lock()
            .expect("bulk network mutex poisoned")
            .insert(id.clone(), tx);
        MemBulkTransport {
            id,
            queues: self.queues.clone(),
            incoming: AsyncMutex::new(rx),
            pipe_buffer: self.config.pipe_buffer,
        }
    }
}

/// One node's data-plane endpoint on a [`MemBulkNet`].
#[derive(Debug)]
pub struct MemBulkTransport {
    id: NodeId,
    queues: Queues,
    incoming: AsyncMutex<mpsc::Receiver<Incoming>>,
    pipe_buffer: usize,
}

impl MemBulkTransport {
    /// This endpoint's local node id — the one peers see on `accept`.
    #[must_use]
    pub fn local_id(&self) -> &NodeId {
        &self.id
    }
}

impl BulkTransport for MemBulkTransport {
    type Error = io::Error;
    type Stream = Compat<DuplexStream>;

    async fn connect(&self, to: &NodeId) -> io::Result<Self::Stream> {
        let queue = {
            let queues = self.queues.lock().expect("bulk network mutex poisoned");
            queues.get(to).cloned()
        }
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "unknown peer"))?;

        // Acquire accept capacity before allocating either pipe buffer.
        // A displaced endpoint's queue remains distinct from its replacement.
        let permit = queue.reserve().await.map_err(|_| {
            io::Error::new(io::ErrorKind::ConnectionRefused, "peer endpoint dropped")
        })?;
        let (near, far) = tokio::io::duplex(self.pipe_buffer);
        permit.send((self.id.clone(), far));
        Ok(near.compat())
    }

    async fn accept(&self) -> io::Result<(NodeId, Self::Stream)> {
        // The tokio mutex is held across the await intentionally; only the
        // single accept loop ever calls this.
        let mut incoming = self.incoming.lock().await;
        let (from, stream) = incoming
            .recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "endpoint evicted"))?;
        Ok((from, stream.compat()))
    }
}
