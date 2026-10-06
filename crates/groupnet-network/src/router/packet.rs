//! Owned application encoding buffers with router-reserved headroom.

use super::Shared;
use crate::wire::{self, PayloadKind};
use bytes::{Bytes, BytesMut};
use groupnet_core::NodeId;
use std::io;

/// A uniquely owned encoding buffer with reserved routing-envelope headroom.
/// Append protocol headers/body, seal [`payload_mut`](Self::payload_mut) in place,
/// then transfer it through [`ProtocolIo::send_packet`](super::ProtocolIo::send_packet).
#[derive(Debug)]
pub struct PacketBuffer {
    bytes: BytesMut,
    headroom: usize,
    target: NodeId,
    kind: PayloadKind,
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
        let mut bytes = BytesMut::with_capacity(headroom + capacity);
        bytes.resize(headroom, 0);
        Ok(Self {
            bytes,
            headroom,
            target: target.clone(),
            kind,
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

    pub(crate) fn resize_payload(&mut self, length: usize) {
        self.bytes.resize(self.headroom + length, 0);
    }

    pub(crate) fn truncate_payload(&mut self, length: usize) {
        self.bytes.truncate(self.headroom + length);
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
        Ok((self.bytes.freeze(), self.headroom))
    }
}
