//! Bounded routing envelopes and per-link fragmentation.

use groupnet_core::NodeId;
use std::collections::HashMap;
use std::io;
use std::time::{Duration, Instant};

pub(crate) const MAX_FRAME: usize = 65_000;
pub(crate) const MAX_HOPS: usize = 16;
const FRAGMENT: usize = 28;

pub(crate) fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
pub(crate) fn id_valid(id: &NodeId) -> bool {
    !id.as_str().is_empty() && id.as_str().len() <= 255
}

fn put_id(bytes: &mut Vec<u8>, id: &NodeId) {
    bytes.push(u8::try_from(id.as_str().len()).expect("validated node id"));
    bytes.extend_from_slice(id.as_str().as_bytes());
}

fn take<const N: usize>(bytes: &mut &[u8]) -> io::Result<[u8; N]> {
    let (part, rest) = bytes
        .split_at_checked(N)
        .ok_or_else(|| invalid("truncated frame"))?;
    *bytes = rest;
    part.try_into()
        .map_err(|_| invalid("truncated fixed field"))
}

fn take_id(bytes: &mut &[u8]) -> io::Result<NodeId> {
    let [length] = take(bytes)?;
    if length == 0 {
        return Err(invalid("empty node id"));
    }
    let (part, rest) = bytes
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

pub(crate) fn decode(bytes: &[u8]) -> io::Result<Frame<'_>> {
    if bytes.len() > MAX_FRAME || !bytes.starts_with(b"GNR1") {
        return Err(invalid("invalid router frame"));
    }
    let mut body = &bytes[4..];
    let [kind] = take(&mut body)?;
    if kind == 0 {
        let cost = u32::from_be_bytes(take(&mut body)?);
        let [length] = take(&mut body)?;
        if length == 0 || usize::from(length) > MAX_HOPS {
            return Err(invalid("invalid route length"));
        }
        let mut path = Vec::with_capacity(usize::from(length));
        for _ in 0..length {
            let node = take_id(&mut body)?;
            if path.contains(&node) {
                return Err(invalid("loop in route advertisement"));
            }
            path.push(node);
        }
        if !body.is_empty() {
            return Err(invalid("trailing route bytes"));
        }
        Ok(Frame::Advert { cost, path })
    } else if kind == 1 || kind == 2 {
        let kind = if kind == 1 {
            PayloadKind::Message
        } else {
            PayloadKind::Tunnel
        };
        let [hops] = take(&mut body)?;
        if hops == 0 || usize::from(hops) > MAX_HOPS {
            return Err(invalid("invalid hop limit"));
        }
        let id = take(&mut body)?;
        let from = take_id(&mut body)?;
        let to = take_id(&mut body)?;
        Ok(Frame::Data {
            kind,
            hops,
            id,
            from,
            to,
            payload: body,
        })
    } else {
        Err(invalid("unknown router frame"))
    }
}

pub(crate) fn advert(cost: u32, path: &[NodeId]) -> Vec<u8> {
    let mut bytes = b"GNR1\0".to_vec();
    bytes.extend_from_slice(&cost.to_be_bytes());
    bytes.push(u8::try_from(path.len()).expect("bounded route"));
    for node in path {
        put_id(&mut bytes, node);
    }
    bytes
}

pub(crate) fn data(
    kind: PayloadKind,
    hops: u8,
    id: [u8; 16],
    from: &NodeId,
    to: &NodeId,
    payload: &[u8],
) -> Vec<u8> {
    let mut bytes =
        Vec::with_capacity(24 + from.as_str().len() + to.as_str().len() + payload.len());
    bytes.extend_from_slice(b"GNR1");
    bytes.push(match kind {
        PayloadKind::Message => 1,
        PayloadKind::Tunnel => 2,
    });
    bytes.push(hops);
    bytes.extend_from_slice(&id);
    put_id(&mut bytes, from);
    put_id(&mut bytes, to);
    bytes.extend_from_slice(payload);
    bytes
}

struct Assembly {
    created: Instant,
    total: usize,
    retained: usize,
    parts: Vec<Option<Vec<u8>>>,
}

#[derive(Default)]
pub(crate) struct Reassembly {
    pending: HashMap<(usize, NodeId, [u8; 16]), Assembly>,
}

impl Reassembly {
    pub fn receive(&mut self, link: usize, from: &NodeId, bytes: Vec<u8>) -> Option<Vec<u8>> {
        self.pending
            .retain(|_, entry| entry.created.elapsed() < Duration::from_secs(3));
        if bytes.starts_with(b"GNR1") {
            return (bytes.len() <= MAX_FRAME).then_some(bytes);
        }
        if !bytes.starts_with(b"GNF1") || bytes.len() <= FRAGMENT {
            return None;
        }
        let mut body = &bytes[4..];
        let id = take::<16>(&mut body).ok()?;
        let index = usize::from(u16::from_be_bytes(take(&mut body).ok()?));
        let count = usize::from(u16::from_be_bytes(take(&mut body).ok()?));
        let total = usize::try_from(u32::from_be_bytes(take(&mut body).ok()?)).ok()?;
        if count == 0 || count > 1024 || index >= count || total > MAX_FRAME || total < count {
            return None;
        }
        let key = (link, from.clone(), id);
        if !self.pending.contains_key(&key) && self.pending.len() >= 64 {
            return None;
        }
        let entry = self.pending.entry(key.clone()).or_insert_with(|| Assembly {
            created: Instant::now(),
            total,
            retained: 0,
            parts: (0..count).map(|_| None).collect(),
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
            entry.parts[index] = Some(body.to_vec());
        }
        if entry.parts.iter().any(Option::is_none) {
            return None;
        }
        let entry = self.pending.remove(&key)?;
        if entry.retained != total {
            return None;
        }
        let mut result = Vec::with_capacity(total);
        for part in entry.parts.into_iter().flatten() {
            result.extend_from_slice(&part);
        }
        Some(result)
    }
}

pub(crate) fn fragment(
    bytes: &[u8],
    mtu: usize,
    id: [u8; 16],
) -> impl Iterator<Item = Vec<u8>> + '_ {
    let chunk = mtu - FRAGMENT;
    let count = u16::try_from(bytes.len().div_ceil(chunk)).expect("bounded frame and MTU");
    bytes.chunks(chunk).enumerate().map(move |(index, part)| {
        let mut packet = Vec::with_capacity(FRAGMENT + part.len());
        packet.extend_from_slice(b"GNF1");
        packet.extend_from_slice(&id);
        packet.extend_from_slice(
            &u16::try_from(index)
                .expect("bounded fragment index")
                .to_be_bytes(),
        );
        packet.extend_from_slice(&count.to_be_bytes());
        packet.extend_from_slice(
            &u32::try_from(bytes.len())
                .expect("bounded frame")
                .to_be_bytes(),
        );
        packet.extend_from_slice(part);
        packet
    })
}
