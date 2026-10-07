//! Owned application encoding buffers with router-reserved headroom.

use super::{
    Shared,
    pool::{PacketPool, Recycled},
};
use crate::wire::{self, PayloadKind};
use bytes::Bytes;
use groupnet_core::NodeId;
use std::{fmt, io, sync::Arc};

/// A uniquely owned encoding buffer with reserved routing-envelope headroom.
/// Append protocol headers/body, seal [`payload_mut`](Self::payload_mut) in place,
/// then transfer it through [`ProtocolIo::send_packet`](super::ProtocolIo::send_packet).
/// Its storage comes from, and returns to, the router's bounded packet pool.
#[derive(Debug)]
pub struct PacketBuffer {
    bytes: Vec<u8>,
    headroom: usize,
    target: NodeId,
    kind: PayloadKind,
    pool: Arc<PacketPool>,
}

impl PacketBuffer {
    pub(super) fn new(
        shared: &Shared,
        target: &NodeId,
        capacity: usize,
        kind: PayloadKind,
    ) -> io::Result<Self> {
        let headroom = kind.header_len(&shared.local, target);
        if !wire::id_valid(target) || capacity > shared.config.max_frame.saturating_sub(headroom) {
            return Err(wire::invalid("routed packet capacity exceeds bound"));
        }
        let mut bytes = shared.pool.take(headroom + capacity);
        bytes.resize(headroom, 0);
        Ok(Self {
            bytes,
            headroom,
            target: target.clone(),
            kind,
            pool: shared.pool.clone(),
        })
    }

    /// Appends opaque protocol bytes after the reserved routing header.
    pub fn extend_from_slice(&mut self, bytes: &[u8]) {
        self.bytes.extend_from_slice(bytes);
    }

    /// Reserves space for additional protocol bytes without changing its length.
    pub fn reserve(&mut self, additional: usize) {
        self.bytes.reserve(additional);
    }

    /// Protocol bytes, excluding router headroom.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.bytes[self.headroom..]
    }

    /// Mutable protocol bytes for in-place authenticated encryption.
    pub fn payload_mut(&mut self) -> &mut [u8] {
        &mut self.bytes[self.headroom..]
    }

    /// Number of protocol bytes, excluding router headroom.
    #[must_use]
    pub fn len(&self) -> usize {
        self.bytes.len() - self.headroom
    }

    /// Whether the buffer contains no protocol bytes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(super) fn finish(
        mut self,
        shared: &Shared,
        target: &NodeId,
        kind: PayloadKind,
    ) -> io::Result<(Bytes, usize)> {
        if &self.target != target
            || self.kind != kind
            || self.headroom != kind.header_len(&shared.local, target)
            || self.bytes.len() > shared.config.max_frame
        {
            return Err(wire::invalid(
                "routed packet target, headroom or bound mismatch",
            ));
        }
        wire::stamp(
            &mut self.bytes[..self.headroom],
            kind,
            u8::try_from(shared.config.max_hops).expect("validated hop limit"),
            shared.id(),
            &shared.local,
            target,
        );
        let storage = std::mem::take(&mut self.bytes);
        let bytes = if PacketPool::pooled(storage.capacity()) {
            Bytes::from_owner(Recycled {
                storage,
                pool: self.pool.clone(),
            })
        } else {
            Bytes::from(storage)
        };
        Ok((bytes, self.headroom))
    }
}

impl Drop for PacketBuffer {
    /// An unsent buffer's storage returns to the pool.
    fn drop(&mut self) {
        self.pool.give(std::mem::take(&mut self.bytes));
    }
}

/// Allocates tunnel packet buffers toward one peer without holding a router
/// handle, so a session's stream never keeps a dropped router running.
#[derive(Clone)]
pub(crate) struct TunnelBuffers {
    shared: Arc<Shared>,
    target: NodeId,
}

impl fmt::Debug for TunnelBuffers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TunnelBuffers")
            .field("target", &self.target)
            .finish_non_exhaustive()
    }
}

impl TunnelBuffers {
    pub(super) fn new(shared: Arc<Shared>, target: NodeId) -> Self {
        Self { shared, target }
    }

    /// An empty buffer with room for `capacity` protocol bytes.
    pub(crate) fn get(&self, capacity: usize) -> io::Result<PacketBuffer> {
        PacketBuffer::new(&self.shared, &self.target, capacity, PayloadKind::Tunnel)
    }
}
