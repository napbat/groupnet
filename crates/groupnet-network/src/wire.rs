//! Typed routing envelopes and generation-scoped per-link fragmentation.

use bytes::{Bytes, BytesMut};
use groupnet_core::NodeId;
use groupnet_transport::admission::SessionId;
use std::{
    collections::HashMap,
    io,
    time::{Duration, Instant},
};
use zerocopy::{
    FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned,
    byteorder::network_endian::{U16, U32},
};

pub(crate) const MAX_FRAME: usize = 65_000;
pub(crate) const MAX_HOPS: usize = 16;

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct Prefix {
    magic: [u8; 4],
    kind: u8,
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct DataHeader {
    hops: u8,
    id: [u8; 16],
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct AdvertHeader {
    cost: U32,
    length: u8,
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct FragmentHeader {
    magic: [u8; 4],
    id: [u8; 16],
    index: U16,
    count: U16,
    total: U32,
}

pub(crate) const FRAGMENT: usize = size_of::<FragmentHeader>();
pub(crate) const MAX_DATA_HEADER: usize = size_of::<Prefix>()
    + size_of::<DataHeader>()
    + size_of::<U16>()
    + 2 * (size_of::<u8>() + u8::MAX as usize);

/// Smallest tunnel envelope: one-byte origin and destination identities. With
/// [`MAX_FRAME`] it fixes the largest tunnel packet any default router emits.
pub(crate) const MIN_TUNNEL_HEADER: usize =
    size_of::<Prefix>() + size_of::<DataHeader>() + 2 * (size_of::<u8>() + 1);

pub(crate) fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

pub(crate) fn id_valid(id: &NodeId) -> bool {
    !id.as_str().is_empty() && u8::try_from(id.as_str().len()).is_ok()
}

fn put_id(bytes: &mut Vec<u8>, id: &NodeId) {
    bytes.push(u8::try_from(id.as_str().len()).expect("validated node id"));
    bytes.extend_from_slice(id.as_str().as_bytes());
}

fn take_id(bytes: &mut &[u8]) -> io::Result<NodeId> {
    let (&length, rest) = bytes
        .split_first()
        .ok_or_else(|| invalid("truncated node id"))?;
    if length == 0 {
        return Err(invalid("empty node id"));
    }
    let (part, rest) = rest
        .split_at_checked(usize::from(length))
        .ok_or_else(|| invalid("truncated node id"))?;
    *bytes = rest;
    Ok(NodeId::new(
        std::str::from_utf8(part).map_err(|_| invalid("invalid node id"))?,
    ))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PayloadKind {
    Message,
    Tunnel,
    Application(u16),
}

impl PayloadKind {
    pub(crate) fn header_len(self, from: &NodeId, to: &NodeId) -> usize {
        let namespace = if matches!(self, Self::Application(_)) {
            size_of::<U16>()
        } else {
            0
        };
        size_of::<Prefix>()
            + size_of::<DataHeader>()
            + 2 * size_of::<u8>()
            + namespace
            + from.as_str().len()
            + to.as_str().len()
    }

    fn tag(self) -> u8 {
        match self {
            Self::Message => 1,
            Self::Tunnel => 2,
            Self::Application(_) => 3,
        }
    }
}

pub(crate) enum Frame<'a> {
    Advert {
        cost: u32,
        path: Vec<NodeId>,
    },
    Data {
        kind: PayloadKind,
        hops: u8,
        id: [u8; 16],
        from: NodeId,
        to: NodeId,
        payload: &'a [u8],
    },
}

#[cfg(test)]
pub(crate) fn decode(bytes: &[u8]) -> io::Result<Frame<'_>> {
    decode_bounded(bytes, MAX_FRAME, MAX_HOPS)
}

pub(crate) fn decode_bounded(
    bytes: &[u8],
    max_frame: usize,
    max_hops: usize,
) -> io::Result<Frame<'_>> {
    let (prefix, mut body) =
        Prefix::ref_from_prefix(bytes).map_err(|_| invalid("truncated router frame"))?;
    if bytes.len() > max_frame || prefix.magic != *b"GNR3" {
        return Err(invalid("invalid router frame"));
    }
    if prefix.kind == 0 {
        let (header, rest) =
            AdvertHeader::ref_from_prefix(body).map_err(|_| invalid("truncated advertisement"))?;
        body = rest;
        if header.length == 0 || usize::from(header.length) > max_hops {
            return Err(invalid("invalid route length"));
        }
        let mut path = Vec::with_capacity(usize::from(header.length));
        for _ in 0..header.length {
            let node = take_id(&mut body)?;
            if path.contains(&node) {
                return Err(invalid("loop in route advertisement"));
            }
            path.push(node);
        }
        if !body.is_empty() {
            return Err(invalid("trailing route bytes"));
        }
        Ok(Frame::Advert {
            cost: header.cost.get(),
            path,
        })
    } else {
        let kind = match prefix.kind {
            1 => PayloadKind::Message,
            2 => PayloadKind::Tunnel,
            3 => {
                let (namespace, rest) =
                    U16::ref_from_prefix(body).map_err(|_| invalid("truncated namespace"))?;
                body = rest;
                PayloadKind::Application(namespace.get())
            }
            _ => return Err(invalid("unknown router frame")),
        };
        let (header, rest) =
            DataHeader::ref_from_prefix(body).map_err(|_| invalid("truncated data header"))?;
        body = rest;
        if header.hops == 0 || usize::from(header.hops) > max_hops {
            return Err(invalid("invalid hop limit"));
        }
        let from = take_id(&mut body)?;
        let to = take_id(&mut body)?;
        Ok(Frame::Data {
            kind,
            hops: header.hops,
            id: header.id,
            from,
            to,
            payload: body,
        })
    }
}

pub(crate) fn advert(cost: u32, path: &[NodeId]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(
        Prefix {
            magic: *b"GNR3",
            kind: 0,
        }
        .as_bytes(),
    );
    bytes.extend_from_slice(
        AdvertHeader {
            cost: cost.into(),
            length: u8::try_from(path.len()).expect("bounded route"),
        }
        .as_bytes(),
    );
    for node in path {
        put_id(&mut bytes, node);
    }
    bytes
}

pub(crate) fn stamp(
    bytes: &mut [u8],
    kind: PayloadKind,
    hops: u8,
    id: [u8; 16],
    from: &NodeId,
    to: &NodeId,
) {
    let mut offset = 0;
    let prefix = Prefix {
        magic: *b"GNR3",
        kind: kind.tag(),
    };
    bytes[..size_of::<Prefix>()].copy_from_slice(prefix.as_bytes());
    offset += size_of::<Prefix>();
    if let PayloadKind::Application(namespace) = kind {
        bytes[offset..offset + 2].copy_from_slice(U16::new(namespace).as_bytes());
        offset += 2;
    }
    let header = DataHeader { hops, id };
    bytes[offset..offset + size_of::<DataHeader>()].copy_from_slice(header.as_bytes());
    offset += size_of::<DataHeader>();
    for node in [from, to] {
        bytes[offset] = u8::try_from(node.as_str().len()).expect("validated node id");
        offset += 1;
        bytes[offset..offset + node.as_str().len()].copy_from_slice(node.as_str().as_bytes());
        offset += node.as_str().len();
    }
}

pub(crate) fn decrement_hops(bytes: &mut [u8], kind: PayloadKind) {
    let offset = size_of::<Prefix>()
        + if matches!(kind, PayloadKind::Application(_)) {
            size_of::<U16>()
        } else {
            0
        };
    let (header, _) =
        DataHeader::mut_from_prefix(&mut bytes[offset..]).expect("validated router data header");
    header.hops -= 1;
}

#[cfg(test)]
pub(crate) fn data(
    kind: PayloadKind,
    hops: u8,
    id: [u8; 16],
    from: &NodeId,
    to: &NodeId,
    payload: &[u8],
) -> Vec<u8> {
    let headroom = kind.header_len(from, to);
    let mut bytes = Vec::with_capacity(headroom + payload.len());
    bytes.resize(headroom, 0);
    stamp(&mut bytes, kind, hops, id, from, to);
    bytes.extend_from_slice(payload);
    bytes
}

/// Bounds for retained, generation-scoped fragment assemblies.
#[derive(Clone, Debug)]
pub struct ReassemblyConfig {
    /// Maximum simultaneous incomplete frames across all admitted links.
    pub max_pending: usize,
    /// Maximum fragments retained for one frame; must fit the wire's u16 count.
    pub max_fragments: usize,
    /// Expiration of an incomplete frame.
    pub timeout: Duration,
}

impl Default for ReassemblyConfig {
    fn default() -> Self {
        Self {
            max_pending: 64,
            max_fragments: 1024,
            timeout: Duration::from_secs(3),
        }
    }
}

struct Assembly {
    created: Instant,
    total: usize,
    retained: usize,
    parts: Vec<Option<Bytes>>,
    missing: usize,
}

type AssemblyKey = (usize, Option<SessionId>, NodeId, [u8; 16]);

pub(crate) struct Reassembly {
    pending: HashMap<AssemblyKey, Assembly>,
    config: ReassemblyConfig,
    max_frame: usize,
}

impl Default for Reassembly {
    fn default() -> Self {
        Self::new(ReassemblyConfig::default(), MAX_FRAME)
    }
}

impl Reassembly {
    pub(crate) fn new(config: ReassemblyConfig, max_frame: usize) -> Self {
        Self {
            pending: HashMap::new(),
            config,
            max_frame,
        }
    }

    pub fn receive(
        &mut self,
        link: usize,
        session: Option<SessionId>,
        from: &NodeId,
        bytes: Bytes,
    ) -> Option<Bytes> {
        self.pending
            .retain(|_, entry| entry.created.elapsed() < self.config.timeout);
        if bytes.starts_with(b"GNR3") {
            return (bytes.len() <= self.max_frame).then_some(bytes);
        }
        let (header, body) = FragmentHeader::ref_from_prefix(bytes.as_ref()).ok()?;
        if header.magic != *b"GNF1" || body.is_empty() {
            return None;
        }
        let index = usize::from(header.index.get());
        let count = usize::from(header.count.get());
        let total = usize::try_from(header.total.get()).ok()?;
        if count == 0
            || count > self.config.max_fragments
            || index >= count
            || total > self.max_frame
            || total < count
        {
            return None;
        }
        let key = (link, session, from.clone(), header.id);
        if !self.pending.contains_key(&key) && self.pending.len() >= self.config.max_pending {
            return None;
        }
        let entry = self.pending.entry(key.clone()).or_insert_with(|| Assembly {
            created: Instant::now(),
            total,
            retained: 0,
            parts: (0..count).map(|_| None).collect(),
            missing: count,
        });
        if entry.total != total || entry.parts.len() != count {
            self.pending.remove(&key);
            return None;
        }
        if entry.parts[index].is_none() {
            entry.retained += body.len();
            if entry.retained > total {
                self.pending.remove(&key);
                return None;
            }
            entry.parts[index] = Some(bytes.slice(FRAGMENT..));
            entry.missing -= 1;
        }
        if entry.missing != 0 {
            return None;
        }
        let entry = self.pending.remove(&key)?;
        if entry.retained != total {
            return None;
        }
        let mut result = BytesMut::with_capacity(total);
        for part in entry.parts.into_iter().flatten() {
            result.extend_from_slice(&part);
        }
        Some(result.freeze())
    }
}

pub(crate) struct Fragments {
    bytes: Bytes,
    chunk: usize,
    id: [u8; 16],
    index: u16,
    count: u16,
}

pub(crate) fn fragment(bytes: Bytes, mtu: usize, id: [u8; 16]) -> Fragments {
    let chunk = mtu - FRAGMENT;
    let count = u16::try_from(bytes.len().div_ceil(chunk)).expect("validated frame and MTU");
    Fragments {
        bytes,
        chunk,
        id,
        index: 0,
        count,
    }
}

impl Iterator for Fragments {
    type Item = Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.count {
            return None;
        }
        let offset = usize::from(self.index) * self.chunk;
        let part = &self.bytes[offset..(offset + self.chunk).min(self.bytes.len())];
        let header = FragmentHeader {
            magic: *b"GNF1",
            id: self.id,
            index: self.index.into(),
            count: self.count.into(),
            total: u32::try_from(self.bytes.len())
                .expect("validated frame")
                .into(),
        };
        let mut packet = BytesMut::with_capacity(FRAGMENT + part.len());
        packet.extend_from_slice(header.as_bytes());
        packet.extend_from_slice(part);
        self.index += 1;
        Some(packet.freeze())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_routing_bound_roundtrips_all_namespaces() {
        for length in [1, 255] {
            let from = NodeId::new("f".repeat(length));
            let to = NodeId::new("t".repeat(length));
            for kind in [
                PayloadKind::Message,
                PayloadKind::Tunnel,
                PayloadKind::Application(42),
            ] {
                let payload = vec![7; MAX_FRAME - kind.header_len(&from, &to)];
                let mut envelope = data(kind, 16, [4; 16], &from, &to, &payload);
                assert_eq!(envelope.len(), MAX_FRAME);
                let Frame::Data {
                    kind: decoded,
                    hops,
                    id,
                    from: origin,
                    to: target,
                    payload: body,
                } = decode(&envelope).unwrap()
                else {
                    panic!("expected data");
                };
                assert_eq!(
                    (decoded, hops, id, origin, target, body),
                    (
                        kind,
                        16,
                        [4; 16],
                        from.clone(),
                        to.clone(),
                        payload.as_slice()
                    )
                );
                envelope.push(7);
                assert!(decode(&envelope).is_err());
            }
        }
    }

    #[test]
    fn minimal_tunnel_envelope_uses_one_byte_identities() {
        let one = NodeId::new("a");
        assert_eq!(
            PayloadKind::Tunnel.header_len(&one, &NodeId::new("b")),
            MIN_TUNNEL_HEADER
        );
        assert!(PayloadKind::Tunnel.header_len(&one, &NodeId::new("bb")) > MIN_TUNNEL_HEADER);
    }

    #[test]
    fn rejects_unknown_kinds_versions_and_truncation() {
        let mut envelope = data(
            PayloadKind::Application(42),
            16,
            [4; 16],
            &NodeId::new("a"),
            &NodeId::new("b"),
            b"x",
        );
        for end in 0..envelope.len() - 1 {
            assert!(decode(&envelope[..end]).is_err());
        }
        for version in [b"GNR1", b"GNR2"] {
            envelope[..4].copy_from_slice(version);
            assert!(decode(&envelope).is_err());
        }
        envelope[..4].copy_from_slice(b"GNR3");
        envelope[4] = 4;
        assert!(decode(&envelope).is_err());
    }

    #[test]
    fn reassembly_retains_generation_and_out_of_order_bytes() {
        let from = NodeId::new("a");
        let bytes = Bytes::from(data(
            PayloadKind::Message,
            16,
            [1; 16],
            &from,
            &NodeId::new("b"),
            &[8; 512],
        ));
        let mut frames: Vec<_> = fragment(bytes.clone(), 128, [2; 16]).collect();
        let mut assembly = Reassembly::default();
        let last = frames.pop().unwrap();
        assert!(assembly.receive(0, None, &from, last.clone()).is_none());
        assert!(
            assembly
                .receive(1, None, &from, frames[0].clone())
                .is_none()
        );
        for frame in &frames[..frames.len() - 1] {
            assert!(assembly.receive(0, None, &from, frame.clone()).is_none());
        }
        assert_eq!(
            assembly.receive(0, None, &from, frames.last().unwrap().clone()),
            Some(bytes)
        );
        let direct = Bytes::from(data(
            PayloadKind::Message,
            16,
            [1; 16],
            &from,
            &NodeId::new("b"),
            b"direct",
        ));
        let ptr = direct.as_ptr();
        assert_eq!(
            assembly.receive(0, None, &from, direct).unwrap().as_ptr(),
            ptr
        );
    }

    #[test]
    fn fragment_retention_slices_allocation_and_applies_configured_bounds() {
        let from = NodeId::new("a");
        let frame = Bytes::from(data(
            PayloadKind::Message,
            16,
            [1; 16],
            &from,
            &NodeId::new("b"),
            &[8; 256],
        ));
        let mut parts = fragment(frame.clone(), 128, [2; 16]);
        let first = parts.next().unwrap();
        let ptr = first[FRAGMENT..].as_ptr();
        let mut bounded = Reassembly::new(
            ReassemblyConfig {
                max_pending: 1,
                ..ReassemblyConfig::default()
            },
            MAX_FRAME,
        );
        assert!(bounded.receive(0, None, &from, first).is_none());
        let assembly = bounded.pending.values().next().unwrap();
        assert_eq!(assembly.parts[0].as_ref().unwrap().as_ptr(), ptr);
        let competing = fragment(frame.clone(), 128, [3; 16]).next().unwrap();
        assert!(bounded.receive(0, None, &from, competing).is_none());
        assert_eq!(bounded.pending.len(), 1);
        assert!(
            bounded
                .receive(0, None, &from, parts.next().unwrap())
                .is_none()
        );
        assert_eq!(
            bounded.receive(0, None, &from, parts.next().unwrap()),
            Some(frame.clone())
        );
        let mut tight = Reassembly::new(
            ReassemblyConfig {
                max_fragments: 2,
                ..ReassemblyConfig::default()
            },
            MAX_FRAME,
        );
        assert!(
            tight
                .receive(
                    0,
                    None,
                    &from,
                    fragment(frame, 128, [4; 16]).next().unwrap()
                )
                .is_none()
        );
        assert!(tight.pending.is_empty());
    }
}
