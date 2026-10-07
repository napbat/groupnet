//! Exclusive bounded application namespaces, independent of protocol bodies.

use super::{ApplicationPacket, PacketBuffer, Router, closed};
use crate::wire::PayloadKind;
use std::{collections::HashMap, io, sync::Arc};
use tokio::sync::{Mutex as AsyncMutex, mpsc};
use tokio_util::sync::CancellationToken;

/// Application namespace carried in the routing envelope.
pub type ProtocolId = u16;

use bytes::Bytes;

struct Entry {
    generation: u64,
    incoming: mpsc::Sender<ApplicationPacket>,
    cancel: CancellationToken,
}

#[derive(Default)]
pub(super) struct Registry {
    entries: HashMap<ProtocolId, Entry>,
    generation: u64,
}

impl Registry {
    pub(super) fn deliver(&self, id: ProtocolId, packet: ApplicationPacket) -> io::Result<()> {
        let entry = self
            .entries
            .get(&id)
            .filter(|entry| !entry.cancel.is_cancelled())
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotConnected, "unbound application protocol")
            })?;
        entry
            .incoming
            .try_send(packet)
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => {
                    io::Error::new(io::ErrorKind::WouldBlock, "application protocol inbox full")
                }
                mpsc::error::TrySendError::Closed(_) => closed(),
            })
    }
}

struct Endpoint {
    router: Router,
    id: ProtocolId,
    generation: u64,
    incoming: AsyncMutex<mpsc::Receiver<ApplicationPacket>>,
    cancel: CancellationToken,
}

impl Endpoint {
    fn unregister(&self) {
        self.cancel.cancel();
        if let Ok(mut registry) = self.router.inner.shared.protocols.lock()
            && registry
                .entries
                .get(&self.id)
                .is_some_and(|entry| entry.generation == self.generation)
        {
            registry.entries.remove(&self.id);
        }
    }
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        self.unregister();
    }
}

/// Shared opaque application endpoint. Clones consume the same bounded inbox.
/// Endpoint shutdown never shuts down its router or another namespace.
#[derive(Clone)]
pub struct ProtocolIo {
    inner: Arc<Endpoint>,
}

impl std::fmt::Debug for ProtocolIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProtocolIo")
            .field("id", &self.inner.id)
            .finish_non_exhaustive()
    }
}

impl ProtocolIo {
    /// This endpoint's exclusive routing namespace.
    #[must_use]
    pub fn id(&self) -> ProtocolId {
        self.inner.id
    }

    /// Locally enqueues opaque bytes; success is not a remote receipt.
    /// # Errors
    /// Reports shutdown, routing bounds, missing routes, or local backpressure.
    pub fn send(&self, to: &groupnet_core::NodeId, payload: &[u8]) -> io::Result<()> {
        let mut packet = self.packet_buffer(to, payload.len())?;
        packet.extend_from_slice(payload);
        self.send_packet(to, packet)
    }

    /// Largest opaque payload representable for this destination.
    #[must_use]
    pub fn max_payload(&self, to: &groupnet_core::NodeId) -> usize {
        let shared = &self.inner.router.inner.shared;
        shared
            .config
            .max_frame
            .saturating_sub(PayloadKind::Application(self.inner.id).header_len(&shared.local, to))
    }

    /// Allocates an encoding buffer with the exact routing header reserved.
    /// # Errors
    /// Rejects an invalid destination, excessive capacity, or a closed endpoint.
    pub fn packet_buffer(
        &self,
        to: &groupnet_core::NodeId,
        capacity: usize,
    ) -> io::Result<PacketBuffer> {
        if self.inner.cancel.is_cancelled() {
            return Err(closed());
        }
        PacketBuffer::new(
            &self.inner.router.inner.shared,
            to,
            capacity,
            PayloadKind::Application(self.inner.id),
        )
    }

    /// Transfers an encoded packet without copying its protocol bytes.
    /// # Errors
    /// Rejects target/bound mismatches, shutdown, missing routes, or backpressure.
    pub fn send_packet(&self, to: &groupnet_core::NodeId, packet: PacketBuffer) -> io::Result<()> {
        if self.inner.cancel.is_cancelled() {
            return Err(closed());
        }
        self.inner.router.inner.shared.send_packet(
            to,
            packet,
            PayloadKind::Application(self.inner.id),
        )
    }

    /// Transfers owned protocol bytes. Remote sends copy once to prepend routing;
    /// use [`send_packet`](Self::send_packet) to encode with reserved headroom.
    /// # Errors
    /// Reports invalid bounds, shutdown, missing routes, or backpressure.
    pub fn send_owned(&self, to: &groupnet_core::NodeId, payload: Bytes) -> io::Result<()> {
        if self.inner.cancel.is_cancelled() {
            return Err(closed());
        }
        self.inner.router.inner.shared.send_owned(
            to,
            payload,
            PayloadKind::Application(self.inner.id),
        )
    }

    /// Receives one owned opaque packet from this namespace only.
    /// # Errors
    /// Returns `NotConnected` when this endpoint or the router shuts down.
    pub async fn recv(&self) -> io::Result<ApplicationPacket> {
        tokio::select! {
            biased;
            () = self.inner.cancel.cancelled() => Err(closed()),
            packet = async { self.inner.incoming.lock().await.recv().await } => packet.ok_or_else(closed),
        }
    }

    /// A child token cancelled by endpoint or router shutdown.
    #[must_use]
    pub fn cancellation(&self) -> CancellationToken {
        self.inner.cancel.child_token()
    }

    /// Releases this namespace and wakes blocked receivers across all clones.
    pub fn shutdown(&self) {
        self.inner.unregister();
    }

    /// Waits for endpoint or router shutdown.
    pub async fn closed(&self) {
        self.inner.cancel.cancelled().await;
    }
}

pub(super) fn bind(router: Router, id: ProtocolId) -> io::Result<ProtocolIo> {
    let mut registry = router
        .inner
        .shared
        .protocols
        .lock()
        .map_err(|_| io::Error::other("protocol registry poisoned"))?;
    if router.is_closed() {
        return Err(closed());
    }
    if registry
        .entries
        .get(&id)
        .is_some_and(|entry| !entry.cancel.is_cancelled())
    {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "application protocol already bound",
        ));
    }
    registry
        .entries
        .retain(|_, entry| !entry.cancel.is_cancelled());
    if registry.entries.len() >= router.inner.shared.config.max_protocols {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "application namespace capacity reached",
        ));
    }
    let generation = registry
        .generation
        .checked_add(1)
        .ok_or_else(|| io::Error::other("protocol generation exhausted"))?;
    registry.generation = generation;
    let (incoming, receiver) = mpsc::channel(router.inner.shared.config.protocol_queue.get());
    let cancel = router.cancellation();
    registry.entries.insert(
        id,
        Entry {
            generation,
            incoming,
            cancel: cancel.clone(),
        },
    );
    drop(registry);
    Ok(ProtocolIo {
        inner: Arc::new(Endpoint {
            router,
            id,
            generation,
            incoming: AsyncMutex::new(receiver),
            cancel,
        }),
    })
}
